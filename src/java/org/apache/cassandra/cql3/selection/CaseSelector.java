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
package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A {@code Selector} for CASE expressions.
 * <p>Every branch is a comparison {@code left op right} plus a result.  {@link #getOutput} evaluates
 * the branches in order and returns the result of the first branch whose comparison is true.  If no
 * branch matches, the ELSE result is returned, or null when there is no ELSE.</p>
 * <p>The simple form ({@code CASE operand WHEN value ...}) is normalized to {@code operand = value}
 * comparisons before it reaches this selector.</p>
 */
final class CaseSelector extends Selector
{
    /**
     * A runtime branch: a comparison and the result selector to use when the comparison is true.
     */
    private static final class Branch
    {
        final Selector left;
        final Operator operator;
        final Selector right;
        final AbstractType<?> comparisonType;
        final Selector result;

        Branch(Selector left, Operator operator, Selector right, AbstractType<?> comparisonType, Selector result)
        {
            this.left = left;
            this.operator = operator;
            this.right = right;
            this.comparisonType = comparisonType;
            this.result = result;
        }
    }

    /**
     * The factory-side representation of a branch, holding child factories.
     */
    static final class BranchFactory
    {
        final Factory left;
        final Operator operator;
        final Factory right;
        final AbstractType<?> comparisonType;
        final Factory result;

        BranchFactory(Factory left, Operator operator, Factory right, AbstractType<?> comparisonType, Factory result)
        {
            this.left = left;
            this.operator = operator;
            this.right = right;
            this.comparisonType = comparisonType;
            this.result = result;
        }
    }

    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            AbstractType<?> resultType = readType(metadata, in);
            int size = in.readUnsignedVInt32();
            List<Branch> branches = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                AbstractType<?> comparisonType = readType(metadata, in);
                Operator operator = Operator.readFromUnsignedVInt(in);
                Selector left = serializer.deserialize(in, version, metadata);
                Selector right = serializer.deserialize(in, version, metadata);
                Selector result = serializer.deserialize(in, version, metadata);
                branches.add(new Branch(left, operator, right, comparisonType, result));
            }

            boolean hasElse = in.readBoolean();
            Selector elseResult = hasElse ? serializer.deserialize(in, version, metadata) : null;

            return new CaseSelector(resultType, branches, elseResult);
        }
    };

    private final AbstractType<?> resultType;
    private final List<Branch> branches;
    private final Selector elseResult; // may be null

    public static Factory newFactory(AbstractType<?> resultType,
                                     List<BranchFactory> branchFactories,
                                     Factory elseFactory,
                                     String columnName)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return columnName;
            }

            protected AbstractType<?> getReturnType()
            {
                return resultType;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
                for (BranchFactory branch : branchFactories)
                {
                    branch.left.addColumnMapping(tmpMapping, resultsColumn);
                    branch.right.addColumnMapping(tmpMapping, resultsColumn);
                    branch.result.addColumnMapping(tmpMapping, resultsColumn);
                }
                if (elseFactory != null)
                    elseFactory.addColumnMapping(tmpMapping, resultsColumn);

                if (tmpMapping.getMappings().get(resultsColumn).isEmpty())
                    mapping.addMapping(resultsColumn, (ColumnMetadata) null);
                else
                    mapping.addMapping(resultsColumn, tmpMapping.getMappings().values());
            }

            public void addFunctionsTo(List<Function> functions)
            {
                for (BranchFactory branch : branchFactories)
                {
                    branch.left.addFunctionsTo(functions);
                    branch.right.addFunctionsTo(functions);
                    branch.result.addFunctionsTo(functions);
                }
                if (elseFactory != null)
                    elseFactory.addFunctionsTo(functions);
            }

            public Selector newInstance(QueryOptions options)
            {
                List<Branch> branches = new ArrayList<>(branchFactories.size());
                for (BranchFactory branch : branchFactories)
                    branches.add(new Branch(branch.left.newInstance(options),
                                            branch.operator,
                                            branch.right.newInstance(options),
                                            branch.comparisonType,
                                            branch.result.newInstance(options)));

                Selector elseSelector = elseFactory == null ? null : elseFactory.newInstance(options);
                return new CaseSelector(resultType, branches, elseSelector);
            }

            public boolean isAggregateSelectorFactory()
            {
                for (BranchFactory branch : branchFactories)
                {
                    if (branch.left.isAggregateSelectorFactory()
                        || branch.right.isAggregateSelectorFactory()
                        || branch.result.isAggregateSelectorFactory())
                        return true;
                }
                return elseFactory != null && elseFactory.isAggregateSelectorFactory();
            }

            @Override
            public boolean areAllFetchedColumnsKnown()
            {
                for (BranchFactory branch : branchFactories)
                {
                    if (!branch.left.areAllFetchedColumnsKnown()
                        || !branch.right.areAllFetchedColumnsKnown()
                        || !branch.result.areAllFetchedColumnsKnown())
                        return false;
                }
                return elseFactory == null || elseFactory.areAllFetchedColumnsKnown();
            }

            @Override
            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                for (BranchFactory branch : branchFactories)
                {
                    branch.left.addFetchedColumns(builder);
                    branch.right.addFetchedColumns(builder);
                    branch.result.addFetchedColumns(builder);
                }
                if (elseFactory != null)
                    elseFactory.addFetchedColumns(builder);
            }
        };
    }

    private CaseSelector(AbstractType<?> resultType, List<Branch> branches, Selector elseResult)
    {
        super(Kind.CASE_SELECTOR);
        this.resultType = resultType;
        this.branches = branches;
        this.elseResult = elseResult;
    }

    @Override
    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        for (Branch branch : branches)
        {
            branch.left.addFetchedColumns(builder);
            branch.right.addFetchedColumns(builder);
            branch.result.addFetchedColumns(builder);
        }
        if (elseResult != null)
            elseResult.addFetchedColumns(builder);
    }

    @Override
    public void addInput(InputRow input)
    {
        // All children are fed so that every referenced column is fetched, even in branches that
        // do not match.
        for (Branch branch : branches)
        {
            branch.left.addInput(input);
            branch.right.addInput(input);
            branch.result.addInput(input);
        }
        if (elseResult != null)
            elseResult.addInput(input);
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        for (Branch branch : branches)
        {
            ByteBuffer left = branch.left.getOutput(protocolVersion);
            ByteBuffer right = branch.right.getOutput(protocolVersion);
            if (matches(branch.operator, branch.comparisonType, left, right))
                return branch.result.getOutput(protocolVersion);
        }
        return elseResult == null ? null : elseResult.getOutput(protocolVersion);
    }

    // A comparison with a null operand is never true, so such a branch does not match.
    private static boolean matches(Operator operator, AbstractType<?> type, ByteBuffer left, ByteBuffer right)
    {
        if (left == null || right == null)
            return false;

        int cmp = type.compareForCQL(left, right);
        switch (operator)
        {
            case EQ: return cmp == 0;
            case NEQ: return cmp != 0;
            case LT: return cmp < 0;
            case LTE: return cmp <= 0;
            case GT: return cmp > 0;
            case GTE: return cmp >= 0;
            default: throw invalidRequest("Unsupported operator %s in a CASE expression", operator);
        }
    }

    @Override
    public void reset()
    {
        for (Branch branch : branches)
        {
            branch.left.reset();
            branch.right.reset();
            branch.result.reset();
        }
        if (elseResult != null)
            elseResult.reset();
    }

    @Override
    public boolean isTerminal()
    {
        for (Branch branch : branches)
        {
            if (!branch.left.isTerminal() || !branch.right.isTerminal() || !branch.result.isTerminal())
                return false;
        }
        return elseResult == null || elseResult.isTerminal();
    }

    @Override
    public void prepare(FunctionContext context)
    {
        super.prepare(context);
        for (Branch branch : branches)
        {
            branch.left.prepare(context);
            branch.right.prepare(context);
            branch.result.prepare(context);
        }
        if (elseResult != null)
            elseResult.prepare(context);
    }

    @Override
    public void validateForGroupBy()
    {
        throw invalidRequest("CASE expressions are not supported in the GROUP BY clause.");
    }

    @Override
    public AbstractType<?> getType()
    {
        return resultType;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("CASE");
        for (Branch branch : branches)
            sb.append(" WHEN ").append(branch.left).append(' ').append(branch.operator)
              .append(' ').append(branch.right).append(" THEN ").append(branch.result);
        if (elseResult != null)
            sb.append(" ELSE ").append(elseResult);
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof CaseSelector))
            return false;

        CaseSelector s = (CaseSelector) o;
        if (!Objects.equal(resultType, s.resultType) || !Objects.equal(elseResult, s.elseResult))
            return false;
        if (branches.size() != s.branches.size())
            return false;
        for (int i = 0; i < branches.size(); i++)
        {
            Branch a = branches.get(i);
            Branch b = s.branches.get(i);
            if (a.operator != b.operator
                || !Objects.equal(a.comparisonType, b.comparisonType)
                || !Objects.equal(a.left, b.left)
                || !Objects.equal(a.right, b.right)
                || !Objects.equal(a.result, b.result))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int h = Objects.hashCode(resultType, elseResult);
        for (Branch branch : branches)
            h = 31 * h + Objects.hashCode(branch.operator, branch.comparisonType, branch.left, branch.right, branch.result);
        return h;
    }

    @Override
    protected int serializedSize(int version)
    {
        int size = sizeOf(resultType) + TypeSizes.sizeofUnsignedVInt(branches.size());
        for (Branch branch : branches)
        {
            size += sizeOf(branch.comparisonType);
            size += TypeSizes.sizeofUnsignedVInt(branch.operator.getValue());
            size += serializer.serializedSize(branch.left, version);
            size += serializer.serializedSize(branch.right, version);
            size += serializer.serializedSize(branch.result, version);
        }
        size += TypeSizes.sizeof(elseResult != null);
        if (elseResult != null)
            size += serializer.serializedSize(elseResult, version);
        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, resultType);
        out.writeUnsignedVInt32(branches.size());
        for (Branch branch : branches)
        {
            writeType(out, branch.comparisonType);
            branch.operator.writeToUnsignedVInt(out);
            serializer.serialize(branch.left, out, version);
            serializer.serialize(branch.right, out, version);
            serializer.serialize(branch.result, out, version);
        }
        out.writeBoolean(elseResult != null);
        if (elseResult != null)
            serializer.serialize(elseResult, out, version);
    }
}
