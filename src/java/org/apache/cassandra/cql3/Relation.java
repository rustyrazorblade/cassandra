/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3;

import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.restrictions.SimpleRestriction;
import org.apache.cassandra.cql3.restrictions.SingleRestriction;
import org.apache.cassandra.cql3.restrictions.SubqueryTerms;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.cql3.terms.Terms;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * The parsed version of a {@code SimpleRestriction} as outputed by the CQL parser.
 * {@code Relation.prepare} will be called upon schema binding to create a {@code SimpleRestriction}.
 */
public final class Relation
{
    public static final String FROZEN_MAP_ENTRY_PREDICATES_NOT_SUPPORTED = "Map-entry predicates on frozen map column %s are not supported";

    /**
     * The raw columns'expression.
     */
    private final ColumnsExpression.Raw rawExpressions;

    /**
     * The relation operator
     */
    private final Operator operator;

    /**
     * The raw terms.
     */
    private final Terms.Raw rawTerms;

    /**
     * The raw selectable on the left-hand side of a HAVING predicate (e.g. {@code SUM(v)}), or {@code null}
     * for a column-based relation.  HAVING predicates on aggregate functions are parsed and carried here,
     * but they are gated at prepare time; {@link #toRestriction} does not support them yet.
     */
    private final Selectable.Raw rawSelectable;

    /**
     * The raw inner query of an IN-subquery ({@code pk IN (SELECT ...)}), or {@code null} for a normal
     * relation.  Research POC, gated by {@link CassandraRelevantProperties#CQL_SUBQUERY_ENABLED}.
     */
    private final SelectStatement.RawStatement rawSubquery;

    private Relation(ColumnsExpression.Raw rawExpressions, Operator operator, Terms.Raw rawTerms)
    {
        this.rawExpressions = rawExpressions;
        this.operator = operator;
        this.rawTerms = rawTerms;
        this.rawSelectable = null;
        this.rawSubquery = null;
    }

    private Relation(Selectable.Raw rawSelectable, Operator operator, Terms.Raw rawTerms)
    {
        this.rawExpressions = null;
        this.operator = operator;
        this.rawTerms = rawTerms;
        this.rawSelectable = rawSelectable;
        this.rawSubquery = null;
    }

    private Relation(ColumnsExpression.Raw rawExpressions, Operator operator, SelectStatement.RawStatement rawSubquery)
    {
        this.rawExpressions = rawExpressions;
        this.operator = operator;
        this.rawTerms = null;
        this.rawSelectable = null;
        this.rawSubquery = rawSubquery;
    }

    public Operator operator()
    {
        return operator;
    }

    /**
     * Creates a relation for a single column (e.g. {@code columnA = ?} ).
     *
     * @param identifier the column identifier to which the relation applies
     * @param operator the relation operator
     * @param rawTerm the term to which the column values must be compared
     * @return a relation for a single column.
     */
    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for a single column (e.g. {@code columnA IN ?} ).
     *
     * @param identifier the column identifier to which the relation applies
     * @param operator the relation operator
     * @param rawTerms the terms to which the column values must be compared
     * @return a relation for a single column.
     */
    public static Relation singleColumn(ColumnIdentifier identifier, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() != Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), operator, rawTerms);
    }

    /**
     * Creates a relation for a single column whose IN value list is an uncorrelated subquery
     * (e.g. {@code pk IN (SELECT one_col FROM t2)}).  Research POC.
     *
     * @param identifier the column identifier to which the relation applies (must be a single-column
     *                   partition key at prepare time)
     * @param subquery the raw inner SELECT statement
     * @return a relation whose IN values come from a subquery.
     */
    public static Relation singleColumnSubquery(ColumnIdentifier identifier, SelectStatement.RawStatement subquery)
    {
        return new Relation(ColumnsExpression.Raw.singleColumn(identifier), Operator.IN, subquery);
    }

    /**
     * @return {@code true} if this relation is an IN-subquery ({@code pk IN (SELECT ...)}).
     */
    public boolean isSubquery()
    {
        return rawSubquery != null;
    }

    /**
     * @return {@code true} if this relation's value terms contain any bind markers.  Used to reject
     * bind markers inside an IN-subquery, which is uncorrelated and resolved with no bound values.
     */
    boolean containsBindMarkers()
    {
        return rawTerms != null && rawTerms.containsBindMarkers();
    }

    /**
     * Creates a relation whose left-hand side is a function applied to columns (e.g. {@code SUM(v) > 10}).
     *
     * <p>This is used for HAVING predicates on aggregate functions.  The relation is carried through
     * parsing, but it is not yet convertible to a restriction; HAVING is gated at prepare time.</p>
     *
     * @param rawSelectable the raw selectable (function) on the left-hand side
     * @param operator the relation operator
     * @param rawTerm the term to which the function result is compared
     * @return a relation with a function on the left-hand side.
     */
    public static Relation function(Selectable.Raw rawSelectable, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(rawSelectable, operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for a map element (e.g. {@code columnA[?] = ?}).
     *
     * @param identifier the map column identifier
     * @param rawKey the map element key (we do not support list elements in relations yet)
     * @param operator the relation operator
     * @param rawTerm the term to which the map element must be compared
     * @return a relation for a map element.
     */
    @VisibleForTesting
    static Relation mapElement(ColumnIdentifier identifier, Term.Raw rawKey, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.collectionElement(identifier, rawKey), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for multiple columns (e.g. {@code (columnA, columnB) = (?, ?)}).
     *
     * @param identifiers the columns identifiers
     * @param operator the relation operator
     * @param rawTerm the term (tuple) to which the multiple columns must be compared
     * @return a relation for multiple columns.
     */
    public static Relation multiColumn(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.multiColumn(identifiers), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for multiple columns (e.g. {@code (columnA, columnB) = (?, ?)}).
     *
     * @param identifiers the columns identifiers
     * @param operator the relation operator
     * @param rawTerms the terms (tuples) to which the multiple columns must be compared
     * @return a relation for multiple columns.
     */
    public static Relation multiColumn(List<ColumnIdentifier> identifiers, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() != Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.multiColumn(identifiers), operator, rawTerms);
    }

    /**
     * Creates a relation for token expression (e.g. {@code token(columnA, columnB) = ?} ).
     *
     * @param identifiers the column identifiers for the partition columns
     * @param operator the relation operator
     * @param rawTerm the terms to which the token value must be compared
     * @return a relation for a token expression.
     */
    public static Relation token(List<ColumnIdentifier> identifiers, Operator operator, Term.Raw rawTerm)
    {
        assert operator.kind() == Operator.Kind.BINARY;
        return new Relation(ColumnsExpression.Raw.token(identifiers), operator, Terms.Raw.of(rawTerm));
    }

    /**
     * Creates a relation for token expression (e.g. {@code token(columnA, columnB) = ?} ).
     *
     * @param identifiers the column identifiers for the partition columns
     * @param operator the relation operator
     * @param rawTerms the terms to which the token value must be compared
     * @return a relation for a token expression.
     */
    public static Relation token(List<ColumnIdentifier> identifiers, Operator operator, Terms.Raw rawTerms)
    {
        assert operator.kind() == Operator.Kind.TERNARY;
        return new Relation(ColumnsExpression.Raw.token(identifiers), operator, rawTerms);
    }

    /**
     * Checks if this relation is a token relation (e.g. <pre>token(a) = token(1)</pre>).
     *
     * @return <code>true</code> if this relation is a token relation, <code>false</code> otherwise.
     */
    public boolean onToken()
    {
        return rawExpressions != null && rawExpressions.kind() == ColumnsExpression.Kind.TOKEN;
    }

    /**
     * Converts this <code>Relation</code> into a <code>Restriction</code>.
     *
     * @param table the table metadata
     * @param boundNames the variables specification where to collect the bind variables
     * @return the <code>Restriction</code> corresponding to this <code>Relation</code>
     * @throws InvalidRequestException if this <code>Relation</code> is not valid
     */
    public SingleRestriction toRestriction(ClientState state, TableMetadata table, VariableSpecifications boundNames, Object owner, boolean allowFiltering)
    {
        if (rawSubquery != null)
            return toSubqueryRestriction(state, table, allowFiltering);

        if (rawSelectable != null)
            throw invalidRequest("Functions on the left-hand side of a predicate are not yet supported: %s", this);

        ColumnsExpression columnsExpression = rawExpressions.prepare(table);

        if (operator == Operator.NEQ && columnsExpression.kind() == ColumnsExpression.Kind.TOKEN)
            throw invalidRequest("Unsupported '!=' relation: %s", this);

        // TODO support restrictions on list elements as we do in conditions, then we can probably move below validations
        //  to ElementExpression prepare/validateColumns
        if (columnsExpression.isMapElementExpression())
        {
            ColumnMetadata column = columnsExpression.firstColumn();
            AbstractType<?> baseType = column.type.unwrap();
            checkFalse(baseType instanceof ListType, "Indexes on list entries (%s[index] = value) are not supported.", column.name);
            checkTrue(baseType instanceof MapType, "Column %s cannot be used as a map", column.name);

            if (column.isClusteringColumn() && baseType.isCollection() && !column.type.isMultiCell())
                throw invalidRequest(FROZEN_MAP_ENTRY_PREDICATES_NOT_SUPPORTED, column.name);

            columnsExpression.collectMarkerSpecification(boundNames, owner);
        }

        operator.validateFor(columnsExpression);

        ColumnSpecification receiver = columnsExpression.columnSpecification();
        if (!operator.appliesToColumnValues())
            receiver = ((CollectionType<?>) receiver.type).makeCollectionReceiver(receiver, operator.appliesToMapKeys());

        Terms terms = rawTerms.prepare(table.keyspace, receiver);
        terms.collectMarkerSpecification(boundNames, owner);

        // An IN restriction with only one element is the same as an EQ restriction
        if (operator.isIN() && terms.containsSingleTerm())
            return new SimpleRestriction(columnsExpression, Operator.EQ, terms, allowFiltering);

        return new SimpleRestriction(columnsExpression, operator, terms, allowFiltering);
    }

    /**
     * Converts an IN-subquery relation ({@code pk IN (SELECT one_col FROM t2 [WHERE ...])}) into a
     * restriction.  Research POC, gated by {@link CassandraRelevantProperties#CQL_SUBQUERY_ENABLED}.
     *
     * <p>Runs all prepare-time rejections, then prepares the inner statement standalone and wraps it
     * in a {@link SubqueryTerms}.  The inner is uncorrelated: it references no outer column, carries no
     * bind marker, and is resolved once at the coordinator before the outer read runs.  The resulting
     * restriction is an ordinary {@link SimpleRestriction} on an IN operator; everything from the
     * partition-key planner onward is byte-for-byte the same as a literal IN list.</p>
     */
    private SingleRestriction toSubqueryRestriction(ClientState state, TableMetadata table, boolean allowFiltering)
    {
        // FLAG: check first, before any other work, so a disabled cluster fails with the flag message.
        if (!CassandraRelevantProperties.CQL_SUBQUERY_ENABLED.getBoolean())
            throw invalidRequest("IN-subqueries are not enabled. Set -Dcassandra.cql.subquery.enabled=true to use them.");

        // M1: only IN is supported.  The grammar only wires the subquery into the IN branch, never
        // NOT IN, but check anyway so a future grammar change cannot open a hole silently.
        if (operator != Operator.IN)
            throw invalidRequest("Only the IN operator supports a subquery; %s does not.", operator);

        ColumnsExpression columnsExpression = rawExpressions.prepare(table);
        ColumnMetadata column = columnsExpression.firstColumn();

        // H5: the outer column must be a single-column partition key.  Reject every other column kind
        // with a specific message so the user knows why.
        if (column.isClusteringColumn())
            throw invalidRequest("IN-subqueries are not supported on clustering column %s; " +
                                 "only a single-column partition key is supported.", column.name);
        if (!column.isPartitionKey())
            throw invalidRequest("IN-subqueries are not supported on non-partition-key column %s; " +
                                 "only a single-column partition key is supported. This also covers " +
                                 "regular, static, and indexed columns.", column.name);
        if (table.partitionKeyColumns().size() > 1)
            throw invalidRequest("IN-subqueries are not supported on component %s of a composite " +
                                 "partition key; only a single-column partition key is supported.", column.name);

        // H3: one level only.  Reject a subquery whose own WHERE clause contains another subquery.
        for (Relation innerRelation : rawSubquery.whereClause.relations)
        {
            if (innerRelation.isSubquery())
                throw invalidRequest("Nested IN-subqueries are not supported; a subquery cannot contain another subquery.");
        }

        // The inner query is uncorrelated and is resolved with no bound values, so it cannot contain
        // bind markers.  Reject them before preparing to avoid a confusing marker-index failure.
        for (Relation innerRelation : rawSubquery.whereClause.relations)
        {
            if (innerRelation.containsBindMarkers())
                throw invalidRequest("IN-subqueries cannot contain bind markers.");
        }
        if ((rawSubquery.limit != null && rawSubquery.limit.containsBindMarker())
            || (rawSubquery.perPartitionLimit != null && rawSubquery.perPartitionLimit.containsBindMarker()))
            throw invalidRequest("IN-subqueries cannot contain bind markers.");

        // H2: reject aggregating, GROUP BY, and DISTINCT inner queries, each with its own message.
        // These would each change the shape or cardinality of the inner result in ways the partition
        // key IN list cannot use.  Check GROUP BY and DISTINCT before preparing (they are on the raw
        // parameters), and the aggregate case after preparing (it is derived during prepare).
        if (!rawSubquery.parameters.groups.isEmpty())
            throw invalidRequest("IN-subqueries cannot use GROUP BY.");
        if (rawSubquery.parameters.isDistinct)
            throw invalidRequest("IN-subqueries cannot use DISTINCT.");

        // Prepare the inner statement standalone.  Inherit the outer table's keyspace when the inner
        // is not itself qualified, so an unqualified inner table resolves against the same keyspace.
        if (!rawSubquery.isFullyQualified())
            rawSubquery.setKeyspace(table.keyspace);
        SelectStatement inner = rawSubquery.prepare(state);

        if (inner.hasAggregation())
            throw invalidRequest("IN-subqueries cannot use aggregate functions.");

        // The inner query must project exactly one column, and its type must match the outer partition
        // key column type.  TYPE: compare the AbstractType with equals(), not the CQL type string.
        List<ColumnSpecification> projected = inner.getSelection().getResultMetadata().names;
        if (projected.size() != 1)
            throw invalidRequest("IN-subqueries must project exactly one column, but the subquery projects %d.", projected.size());

        AbstractType<?> innerType = projected.get(0).type;
        AbstractType<?> outerType = column.type;
        if (!innerType.equals(outerType))
            throw invalidRequest("Type mismatch in IN-subquery: partition key %s is of type %s but the subquery projects type %s.",
                                 column.name, outerType.asCQL3Type(), innerType.asCQL3Type());

        return new SimpleRestriction(columnsExpression, Operator.IN, new SubqueryTerms(inner), allowFiltering);
    }

    public ColumnIdentifier column()
    {
        if (rawExpressions == null)
            throw invalidRequest("Relation %s does not apply to a single column", this);
        return rawExpressions.identifiers().get(0);
    }

    /**
     * Renames an identifier in this Relation, if applicable.
     * @param from the old identifier
     * @param to the new identifier
     * @return this object, if the old identifier is not in the set of entities that this relation covers; otherwise
     *         a new Relation with "from" replaced by "to" is returned.
     */
    public Relation renameIdentifier(ColumnIdentifier from, ColumnIdentifier to)
    {
        if (rawExpressions == null || rawSubquery != null)
            return this;
        return new Relation(rawExpressions.renameIdentifier(from, to), operator, rawTerms);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Relation relation = (Relation) o;
        return Objects.equals(rawExpressions, relation.rawExpressions)
            && Objects.equals(rawSelectable, relation.rawSelectable)
            && operator == relation.operator
            && Objects.equals(rawTerms, relation.rawTerms)
            && Objects.equals(rawSubquery, relation.rawSubquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rawExpressions, rawSelectable, operator, rawTerms, rawSubquery);
    }

    /**
     * Returns a CQL representation of this relation.
     *
     * @return a CQL representation of this relation
     */
    public String toCQLString()
    {
        if (rawSubquery != null)
            return rawExpressions + " IN (" + rawSubquery + ')';
        if (rawSelectable != null)
            return rawSelectable + " " + operator + " " + rawTerms;
        return operator.buildCQLString(rawExpressions, rawTerms);
    }

    @Override
    public String toString()
    {
        return toCQLString();
    }
}
