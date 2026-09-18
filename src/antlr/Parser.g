/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

parser grammar Parser;

options {
    language = Java;
}

@parser::members {
    // Preserve the ANTLR 3 recovery behavior: report the first syntax error,
    // then stop recovering.  This instance initializer overrides the default
    // DefaultErrorStrategy for every CqlParser instance.
    {
        _errHandler = new CqlErrorStrategy();
    }

    protected final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

    // enables parsing txn specific syntax when true
    protected boolean isParsingTxn = false;
    // tracks whether a txn has conditional updates
    protected boolean isTxnConditional = false;

    protected List<RowDataReference.Raw> references;

    private Token statementBeginMarker;

    public static final Set<String> reservedTypeNames = new HashSet<String>()
    {{
        add("byte");
        add("complex");
        add("enum");
        add("date");
        add("interval");
        add("macaddr");
        add("bitstring");
    }};

    public Marker.Raw newBindVariables(ColumnIdentifier name)
    {
        Marker.Raw marker = new Marker.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public InMarker.Raw newINBindVariables(ColumnIdentifier name)
    {
        InMarker.Raw marker = new InMarker.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Json.Marker newJsonBindVariables(ColumnIdentifier name)
    {
        Json.Marker marker = new Json.Marker(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public RowDataReference.Raw newRowDataReference(Selectable.RawIdentifier tuple, Selectable.Raw selectable)
    {
        if (!isParsingTxn)
            throw new SyntaxException("Cannot create a row data reference unless parsing a transaction");

        if (references == null)
            references = new ArrayList<>();

        RowDataReference.Raw reference = RowDataReference.Raw.fromSelectable(tuple, selectable);
        references.add(reference);
        return reference;
    }

    protected void addRecognitionError(String msg)
    {
        // Route grammar-level semantic errors through the ANTLR 4 error-listener
        // mechanism so they are collected by the same ErrorCollector as syntax errors.
        notifyErrorListeners(msg);
    }

    public Map<String, String> convertPropertyMap(Maps.Literal map)
    {
        if (map == null || map.entries == null || map.entries.isEmpty())
            return Collections.<String, String>emptyMap();

        Map<String, String> res = new HashMap<>(map.entries.size());

        for (Pair<Term.Raw, Term.Raw> entry : map.entries)
        {
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have null
            // entries in there. Just skip those, a proper error will be thrown in the end.
            if (entry.left == null || entry.right == null)
                break;

            if (!(entry.left instanceof Constants.Literal))
            {
                String msg = "Invalid property name: " + entry.left;
                if (entry.left instanceof Marker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }
            if (!(entry.right instanceof Constants.Literal))
            {
                String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
                if (entry.right instanceof Marker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }

            if (res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText()) != null)
            {
                addRecognitionError(String.format("Multiple definition for property " + ((Constants.Literal)entry.left).getRawText()));
            }
        }

        return res;
    }

    public void addRawUpdate(UpdateStatement.OperationCollector collector, ColumnIdentifier key, Operation.RawUpdate update)
    {
        if (collector.conflictsWithExistingUpdate(key, update))
            addRecognitionError("Multiple incompatible setting of column " + key);
        if (collector.conflictsWithExistingSubstitution(key))
            addRecognitionError("Normal and reference operations for " + key);

        collector.addRawUpdate(key, update);
    }

    public void addRawReferenceOperation(UpdateStatement.OperationCollector collector, ColumnIdentifier key, ReferenceOperation.Raw update)
    {
        if (collector.conflictsWithExistingUpdate(key))
            addRecognitionError("Multiple incompatible setting of column " + key);
        if (collector.conflictsWithExistingSubstitution(key))
            addRecognitionError("Normal and reference operations for " + key);

        collector.addRawReferenceOperation(key, update);
    }

    public Set<Permission> filterPermissions(Set<Permission> permissions, IResource resource)
    {
        if (resource == null)
            return Collections.emptySet();
        Set<Permission> filtered = new HashSet<>(permissions);
        filtered.retainAll(resource.applicablePermissions());
        if (filtered.isEmpty())
            addRecognitionError("Resource type " + resource.getClass().getSimpleName() +
                                    " does not support any of the requested permissions");

        return filtered;
    }

    public String canonicalizeObjectName(String s, boolean enforcePattern)
    {
        // these two conditions are here because technically they are valid
        // ObjectNames, but we want to restrict their use without adding unnecessary
        // work to JMXResource construction as that also happens on hotter code paths
        if ("".equals(s))
            addRecognitionError("Empty JMX object name supplied");

        if ("*:*".equals(s))
            addRecognitionError("Please use ALL MBEANS instead of wildcard pattern");

        try
        {
            javax.management.ObjectName objectName = javax.management.ObjectName.getInstance(s);
            if (enforcePattern && !objectName.isPattern())
                addRecognitionError("Plural form used, but non-pattern JMX object name specified (" + s + ")");
            return objectName.getCanonicalName();
        }
        catch (javax.management.MalformedObjectNameException e)
        {
          addRecognitionError(s + " is not a valid JMX object name");
          return s;
        }
    }

    public Token stmtBegins()
    {
        statementBeginMarker = _input.LT(1);
        return statementBeginMarker;
    }

    public StatementSource stmtSrc()
    {
        StatementSource stmtSrc = StatementSource.create(statementBeginMarker);
        statementBeginMarker = null;
        return stmtSrc;
    }
}

/** STATEMENTS **/

cqlStatement returns [CQLStatement.Raw stmt]
    @after{ if ($stmt != null) $stmt.setBindVariables(bindVariables); }
    : st1= selectStatement                 { $stmt = $st1.expr; }
    | st2= insertStatement                 { $stmt = $st2.expr; }
    | st3= updateStatement                 { $stmt = $st3.expr; }
    | st4= batchStatement                  { $stmt = $st4.expr; }
    | st5= deleteStatement                 { $stmt = $st5.expr; }
    | st6= useStatement                    { $stmt = $st6.stmt; }
    | st7= truncateStatement               { $stmt = $st7.stmt; }
    | st8= createKeyspaceStatement         { $stmt = $st8.stmt; }
    | st9= createTableStatement            { $stmt = $st9.stmt; }
    | st10=createIndexStatement            { $stmt = $st10.stmt; }
    | st11=dropKeyspaceStatement           { $stmt = $st11.stmt; }
    | st12=dropTableStatement              { $stmt = $st12.stmt; }
    | st13=dropIndexStatement              { $stmt = $st13.stmt; }
    | st14=alterTableStatement             { $stmt = $st14.stmt; }
    | st15=alterKeyspaceStatement          { $stmt = $st15.stmt; }
    | st16=grantPermissionsStatement       { $stmt = $st16.stmt; }
    | st17=revokePermissionsStatement      { $stmt = $st17.stmt; }
    | st18=listPermissionsStatement        { $stmt = $st18.stmt; }
    | st19=createUserStatement             { $stmt = $st19.stmt; }
    | st20=alterUserStatement              { $stmt = $st20.stmt; }
    | st21=dropUserStatement               { $stmt = $st21.stmt; }
    | st22=listUsersStatement              { $stmt = $st22.stmt; }
    | st23=createTriggerStatement          { $stmt = $st23.stmt; }
    | st24=dropTriggerStatement            { $stmt = $st24.stmt; }
    | st25=createTypeStatement             { $stmt = $st25.stmt; }
    | st26=alterTypeStatement              { $stmt = $st26.stmt; }
    | st27=dropTypeStatement               { $stmt = $st27.stmt; }
    | st28=createFunctionStatement         { $stmt = $st28.stmt; }
    | st29=dropFunctionStatement           { $stmt = $st29.stmt; }
    | st30=createAggregateStatement        { $stmt = $st30.stmt; }
    | st31=dropAggregateStatement          { $stmt = $st31.stmt; }
    | st32=createRoleStatement             { $stmt = $st32.stmt; }
    | st33=alterRoleStatement              { $stmt = $st33.stmt; }
    | st34=dropRoleStatement               { $stmt = $st34.stmt; }
    | st35=listRolesStatement              { $stmt = $st35.stmt; }
    | st36=grantRoleStatement              { $stmt = $st36.stmt; }
    | st37=revokeRoleStatement             { $stmt = $st37.stmt; }
    | st38=createMaterializedViewStatement { $stmt = $st38.stmt; }
    | st39=dropMaterializedViewStatement   { $stmt = $st39.stmt; }
    | st40=alterMaterializedViewStatement  { $stmt = $st40.stmt; }
    | st41=describeStatement               { $stmt = $st41.stmt; }
    | st42=addIdentityStatement            { $stmt = $st42.stmt; }
    | st43=dropIdentityStatement           { $stmt = $st43.stmt; }
    | st44=listSuperUsersStatement         { $stmt = $st44.stmt; }
    | st45=copyTableStatement              { $stmt = $st45.stmt; }
    | st46=batchTxnStatement               { $stmt = $st46.expr; }
    | st47=letStatement                    { $stmt = $st47.expr; }
    | st48=commentOnKeyspaceStatement      { $stmt = $st48.stmt; }
    | st49=securityLabelOnKeyspaceStatement { $stmt = $st49.stmt; }
    | st50=commentOnTableStatement         { $stmt = $st50.stmt; }
    | st51=securityLabelOnTableStatement   { $stmt = $st51.stmt; }
    | st52=commentOnColumnStatement        { $stmt = $st52.stmt; }
    | st53=securityLabelOnColumnStatement  { $stmt = $st53.stmt; }
    | st54=commentOnUserTypeStatement      { $stmt = $st54.stmt; }
    | st55=securityLabelOnUserTypeStatement    { $stmt = $st55.stmt; }
    | st56=commentOnUserTypeFieldStatement     { $stmt = $st56.stmt; }
    | st57=securityLabelOnUserTypeFieldStatement   { $stmt = $st57.stmt; }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [UseStatement stmt]
    : K_USE ks=keyspaceName { $stmt = new UseStatement($ks.id); }
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement.RawStatement expr]
    @init {
        Term.Raw limit = null;
        Term.Raw perPartitionLimit = null;
        List<Ordering.Raw> orderings = new ArrayList<>();
        List<Selectable.Raw> groups = new ArrayList<>();
        boolean allowFiltering = false;
        boolean isJson = false;
        SelectOptions options = new SelectOptions();
        SelectStatement.RawJoin joinRaw = null;
        stmtBegins();
    }
    : K_SELECT
        // json is a valid column name. By consequence, we need to resolve the ambiguity for "json - json"
      ( K_JSON { isJson = true; } )? sclause=selectClause
      K_FROM cf=columnFamilyName
        // Research POC (CQL_JOIN_ENABLED): a single INNER JOIN of two tables on one equi-predicate.
      ( K_JOIN jt=columnFamilyName K_ON l1=cident '.' l2=cident '=' r1=cident '.' r2=cident
        { joinRaw = new SelectStatement.RawJoin($jt.name, $l1.id, $l2.id, $r1.id, $r2.id); } )?
      ( K_WHERE wclause=whereClause )?
      ( K_GROUP K_BY groupByClause[groups] ( ',' groupByClause[groups] )* )?
      ( K_HAVING hv=havingClause )?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_PER K_PARTITION K_LIMIT rows=intValue { perPartitionLimit = $rows.raw; } )?
      ( K_LIMIT rows=intValue { limit = $rows.raw; } )?
      ( K_ALLOW K_FILTERING  { allowFiltering = true; } )?
      ( K_WITH properties[options] )?
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
                                                                             groups,
                                                                             $sclause.isDistinct,
                                                                             allowFiltering,
                                                                             isJson,
                                                                             null);
          WhereClause where = $wclause.ctx == null ? WhereClause.empty() : $wclause.clause.build();
          WhereClause having = $hv.ctx == null ? WhereClause.empty() : $hv.clause.build();
          $expr = new SelectStatement.RawStatement($cf.name, params, $sclause.selectorsList, where, having, limit, perPartitionLimit, stmtSrc(), options, joinRaw);
      }
    ;
    
/**
 * ex. LET x = (SELECT * FROM <table> WHERE k=1 AND c=2)
 * ex. LET y = (SELECT * FROM <table> WHERE k=1 LIMIT 1)
 */
letStatement returns [SelectStatement.RawStatement expr]
    @init {
        Term.Raw limit = null;
    }
    : K_LET txnVar=IDENT '='
      '(' { stmtBegins(); } K_SELECT assignments=letSelectors K_FROM cf=columnFamilyName K_WHERE wclause=whereClause ( K_LIMIT rows=intValue { limit = $rows.raw; } )? ')'
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(Collections.emptyList(), Collections.emptyList(), false, false, false, $txnVar.text);
          WhereClause where = $wclause.ctx == null ? WhereClause.empty() : $wclause.clause.build();

          $expr = new SelectStatement.RawStatement($cf.name, params, $assignments.expr, where, WhereClause.empty(), limit, null, stmtSrc(), SelectOptions.EMPTY, null);
      }
    ;
    
letSelectors returns [List<RawSelector> expr]
    : t1=letSelector { $expr = new ArrayList<RawSelector>(); $expr.add($t1.s); } (',' tN=letSelector { $expr.add($tN.s); })*
    | '*' { $expr = Collections.<RawSelector>emptyList();}
    ;
    
letSelector returns [RawSelector s]
    @init{ ColumnIdentifier alias = null; }
    : us=unaliasedSelector { $s = new RawSelector($us.s, alias); }
    ;

selectClause returns [boolean isDistinct, List<RawSelector> selectorsList]
    @init{ $isDistinct = false; }
    // distinct is a valid column name. By consequence, we need to resolve the ambiguity for "distinct - distinct"
    : K_DISTINCT s=selectors { $isDistinct = true; $selectorsList = $s.expr; }
    | s=selectors { $selectorsList = $s.expr; }
    ;

selectors returns [List<RawSelector> expr]
    : t1=selector { $expr = new ArrayList<RawSelector>(); $expr.add($t1.s); } (',' tN=selector { $expr.add($tN.s); })*
    | '*' { $expr = Collections.<RawSelector>emptyList();}
    ;

selector returns [RawSelector s]
    @init{ ColumnIdentifier alias = null; }
    : us=unaliasedSelector (K_AS c=noncol_ident { alias = $c.id; })? { $s = new RawSelector($us.s, alias); }
    ;

unaliasedSelector returns [Selectable.Raw s]
    : a=selectionAddition {$s = $a.s;}
    ;

selectionAddition returns [Selectable.Raw s]
    :   l=selectionMultiplication   {$s = $l.s;}
        ( '+' r=selectionMultiplication {$s = Selectable.WithFunction.Raw.newOperation('+', $s, $r.s);}
        | '-' r=selectionMultiplication {$s = Selectable.WithFunction.Raw.newOperation('-', $s, $r.s);}
        )*
    ;

selectionMultiplication returns [Selectable.Raw s]
    :   l=selectionGroup   {$s = $l.s;}
        ( '*' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('*', $s, $r.s);}
        | '/' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('/', $s, $r.s);}
        | '%' r=selectionGroup {$s = Selectable.WithFunction.Raw.newOperation('%', $s, $r.s);}
        )*
    ;

selectionGroup returns [Selectable.Raw s]
    : f=selectionGroupWithField { $s=$f.s; }
    | g=selectionGroupWithoutField { $s=$g.s; }
    | '-' gsub=selectionGroup {$s = Selectable.WithFunction.Raw.newNegation($gsub.s);}
    ;

selectionGroupWithField returns [Selectable.Raw s]
    : g=selectionGroupWithoutField m=selectorModifier[$g.s] {$s = $m.s;}
    ;

selectorModifier[Selectable.Raw receiver] returns [Selectable.Raw s]
    : f=fieldSelectorModifier[receiver] m=selectorModifier[$f.s] { $s = $m.s; }
    | '[' ss=collectionSubSelection[receiver] ']' m=selectorModifier[$ss.s] { $s = $m.s; }
    | { $s = receiver; }
    ;

fieldSelectorModifier[Selectable.Raw receiver] returns [Selectable.Raw s]
    : '.' fi=fident { $s = new Selectable.WithFieldSelection.Raw(receiver, $fi.id); }
    ;

collectionSubSelection [Selectable.Raw receiver] returns [Selectable.Raw s]
    @init { boolean isSlice=false; }
    : ( t1=term ( { isSlice=true; } RANGE (t2=term)? )?
      | RANGE { isSlice=true; } t2=term
      ) {
          $s = isSlice
             ? new Selectable.WithSliceSelection.Raw(receiver, $t1.raw, $t2.raw)
             : new Selectable.WithElementSelection.Raw(receiver, $t1.raw);
      }
     ;

selectionGroupWithoutField returns [Selectable.Raw s]
    @init { Selectable.Raw tmp = null; }
    @after { $s = tmp; }
    : sn=simpleUnaliasedSelector  { tmp=$sn.s; }
    | h=selectionTypeHint { tmp=$h.s; }
    | t=selectionTupleOrNestedSelector { tmp=$t.s; }
    | l=selectionList { tmp=$l.s; }
    | m=selectionMapOrSet { tmp=$m.s; }
    // UDTs are equivalent to maps from the syntax point of view, so the final decision will be done in Selectable.WithMapOrUdt
    ;

selectionTypeHint returns [Selectable.Raw s]
    : '(' ct=comparatorType ')' a=selectionGroupWithoutField { $s = new Selectable.WithTypeHint.Raw($ct.t, $a.s); }
    ;

selectionList returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); }
    @after { $s = new Selectable.WithArrayLiteral.Raw(l); }
    : '[' ( t1=unaliasedSelector { l.add($t1.s); } ( ',' tn=unaliasedSelector { l.add($tn.s); } )* )? ']'
    ;

selectionMapOrSet returns [Selectable.Raw s]
    : '{' t1=unaliasedSelector ( m=selectionMap[$t1.s] { $s = $m.s; } | st=selectionSet[$t1.s] { $s = $st.s; }) '}'
    | '{' '}' { $s = new Selectable.WithSet.Raw(Collections.emptyList());}
    ;

selectionMap [Selectable.Raw k1] returns [Selectable.Raw s]
    @init { List<Pair<Selectable.Raw, Selectable.Raw>> m = new ArrayList<>(); }
    @after { $s = new Selectable.WithMapOrUdt.Raw(m); }
      : ':' v1=unaliasedSelector   { m.add(Pair.create(k1, $v1.s)); } ( ',' kn=unaliasedSelector ':' vn=unaliasedSelector { m.add(Pair.create($kn.s, $vn.s)); } )*
      ;

selectionSet [Selectable.Raw t1] returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); l.add(t1); }
    @after { $s = new Selectable.WithSet.Raw(l); }
      : ( ',' tn=unaliasedSelector { l.add($tn.s); } )*
      ;

selectionTupleOrNestedSelector returns [Selectable.Raw s]
    @init { List<Selectable.Raw> l = new ArrayList<>(); }
    @after { $s = new Selectable.BetweenParenthesesOrWithTuple.Raw(l); }
    : '(' t1=unaliasedSelector { l.add($t1.s); } (',' tn=unaliasedSelector { l.add($tn.s); } )* ')'
    ;

/*
 * A single selection. The core of it is selecting a column, but we also allow any term and function, as well as
 * sub-element selection for UDT.
 */
simpleUnaliasedSelector returns [Selectable.Raw s]
    : w=windowFunction                           { $s = $w.s; }
    | e=caseExpression                           { $s = $e.s; }
    | c=sident                                   { $s = $c.id; }
    | l=selectionLiteral                         { $s = new Selectable.WithTerm.Raw($l.raw); }
    | f=selectionFunction                        { $s = $f.s; }
    ;

/*
 * A window function.  Only ROW_NUMBER() with a single ORDER BY column is supported:
 *   ROW_NUMBER() OVER (ORDER BY col [ASC|DESC])
 * The ORDER BY column reuses the same single-column ordering the top-level ORDER BY uses.
 * The grammar is unconditional; the feature flag is enforced at prepare time.  ANTLR 4
 * adaptive prediction keeps this distinct from a generic function call named row_number,
 * which the two productions share up to the OVER keyword.
 */
windowFunction returns [Selectable.Raw s]
    @init {
        Ordering.Direction direction = Ordering.Direction.ASC;
    }
    : K_ROW_NUMBER '(' ')' K_OVER '(' K_ORDER K_BY c=cident (K_ASC | K_DESC { direction = Ordering.Direction.DESC; })? ')'
    {
        Ordering.Raw ordering = new Ordering.Raw(new Ordering.Raw.SingleColumn($c.id), direction);
        $s = new Selectable.WindowFunction.Raw(ordering);
    }
    ;

/*
 * A CASE expression.  Two forms exist:
 *   simple:   CASE operand WHEN v1 THEN r1 [WHEN v2 THEN r2 ...] [ELSE d] END
 *   searched: CASE WHEN cond1 THEN r1 [WHEN cond2 THEN r2 ...] [ELSE d] END
 * A condition is a comparison "lhs op rhs" between two selectables.
 * The grammar uses one unified when-condition: an unaliasedSelector with an optional
 * comparison tail.  ANTLR cannot decide simple-vs-searched from the optional leading
 * operand, so we parse permissively here and disambiguate at prepare time.
 */
caseExpression returns [Selectable.Raw s]
    @init {
        Selectable.CaseExpression.Raw.Builder builder = new Selectable.CaseExpression.Raw.Builder();
    }
    @after { $s = builder.build(); }
    : K_CASE ( op=unaliasedSelector { builder.setOperand($op.s); } )?
      ( K_WHEN w=whenCondition[builder] K_THEN r=unaliasedSelector { builder.addWhen($w.left, $w.op, $w.right, $r.s); } )+
      ( K_ELSE e=unaliasedSelector { builder.setElse($e.s); } )?
      K_END
    ;

whenCondition[Selectable.CaseExpression.Raw.Builder builder] returns [Selectable.Raw left, Operator op, Selectable.Raw right]
    : l=unaliasedSelector { $left = $l.s; $op = null; $right = null; }
      ( t=relationType rr=unaliasedSelector { $op = $t.op; $right = $rr.s; } )?
    ;

selectionFunction returns [Selectable.Raw s]
    : K_COUNT        '(' '*' ')'                                    { $s = Selectable.WithFunction.Raw.newCountRowsFunction(); }
    | K_MAXWRITETIME '(' c=sident m=selectorModifier[$c.id] ')'          { $s = new Selectable.WritetimeOrTTL.Raw($c.id, $m.s, Selectable.WritetimeOrTTL.Kind.MAX_WRITE_TIME); }
    | K_WRITETIME    '(' c=sident m=selectorModifier[$c.id] ')'          { $s = new Selectable.WritetimeOrTTL.Raw($c.id, $m.s, Selectable.WritetimeOrTTL.Kind.WRITE_TIME); }
    | K_TTL          '(' c=sident m=selectorModifier[$c.id] ')'          { $s = new Selectable.WritetimeOrTTL.Raw($c.id, $m.s, Selectable.WritetimeOrTTL.Kind.TTL); }
    | K_CAST         '(' sn=unaliasedSelector K_AS t=native_type ')' { $s = new Selectable.WithCast.Raw($sn.s, $t.t);}
    | f=functionName args=selectionFunctionArgs                      { $s = new Selectable.WithFunction.Raw($f.s, $args.a); }
    ;

selectionLiteral returns [Term.Raw raw]
    : c=constant  { $raw = $c.literal; }
    | K_NULL      { $raw = Constants.NULL_LITERAL; }
    | m=marker    { $raw = $m.raw; }
    ;

marker returns [Term.Raw raw]
    : ':' id=noncol_ident  { $raw = newBindVariables($id.id); }
    | QMARK                { $raw = newBindVariables(null); }
    ;

selectionFunctionArgs returns [List<Selectable.Raw> a]
    @init{ $a = new ArrayList<>(); }
    : '(' (s1=unaliasedSelector { $a.add($s1.s); }
          ( ',' sn=unaliasedSelector { $a.add($sn.s); } )*)?
      ')'
    ;

sident returns [Selectable.RawIdentifier id]
    : t=IDENT              { $id = Selectable.RawIdentifier.forUnquoted($t.text); }
    | t=QUOTED_NAME        { $id = Selectable.RawIdentifier.forQuoted($t.text); }
    | k=unreserved_keyword { $id = Selectable.RawIdentifier.forUnquoted($k.str); }
    ;

whereClause returns [WhereClause.Builder clause]
    @init{ $clause = new WhereClause.Builder(); }
    : relationOrExpression[$clause] (K_AND relationOrExpression[$clause])*
    ;

relationOrExpression [WhereClause.Builder clause]
    : relation[$clause]
    | customIndexExpression[$clause]
    ;

customIndexExpression [WhereClause.Builder clause]
    @init{QualifiedName name = new QualifiedName();}
    : 'expr(' idxName[name] ',' t=term ')' { clause.add(new CustomIndexExpression(name, $t.raw));}
    ;

/**
 * HAVING predicates.  Unlike WHERE, the left-hand side may be an aggregate function call
 * (e.g. HAVING SUM(v) > 10) as well as a plain column.  This reuses the existing
 * selectionFunction, relationType and term productions the SELECT clause already uses;
 * selectionFunction accepts a function applied to columns.  It does not introduce a new
 * expression grammar.  The grammar is unconditional; the feature flag is enforced at prepare
 * time in SelectStatement.
 */
havingClause returns [WhereClause.Builder clause]
    @init{ $clause = new WhereClause.Builder(); }
    : havingRelation[$clause] (K_AND havingRelation[$clause])*
    ;

havingRelation[WhereClause.Builder clause]
    : s=selectionFunction type=relationType t=term { $clause.add(Relation.function($s.s, $type.op, $t.raw)); }
    | name=cident type=relationType t=term { $clause.add(Relation.singleColumn($name.id, $type.op, $t.raw)); }
    ;

orderByClause[List<Ordering.Raw> orderings]
    @init{
        Ordering.Direction direction = Ordering.Direction.ASC;
    }
    : c=cident (K_ANN K_OF t=term)? (K_ASC | K_DESC { direction = Ordering.Direction.DESC; })?
    {
        Ordering.Raw.Expression expr = ($t.ctx == null)
            ? new Ordering.Raw.SingleColumn($c.id)
            : new Ordering.Raw.Ann($c.id, $t.raw);
        orderings.add(new Ordering.Raw(expr, direction));
    }
    ;

groupByClause[List<Selectable.Raw> groups]
    : s=unaliasedSelector { groups.add($s.s); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [ModificationStatement.Parsed expr]
    @init {
        stmtBegins();
    }
    : K_INSERT K_INTO cf=columnFamilyName
        ( st1=normalInsertStatement[$cf.name] { $expr = $st1.expr; }
        | K_JSON st2=jsonInsertStatement[$cf.name] { $expr = $st2.expr; })
    ;

normalInsertStatement [QualifiedName qn] returns [UpdateStatement.ParsedInsert expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<ColumnIdentifier> columnNames  = new ArrayList<>();
        List<Term.Raw> values = new ArrayList<>();
        boolean ifNotExists = false;
    }
    : '(' c1=cident { columnNames.add($c1.id); }  ( ',' cn=cident { columnNames.add($cn.id); } )* ')'
      K_VALUES
      '(' insertValue[values] ( ',' insertValue[values] )* ')'
      ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsert(qn, attrs, columnNames, values, ifNotExists, stmtSrc(), isParsingTxn);
      }
    ;

insertValue[List<Term.Raw> values]
    : t=term { values.add($t.raw); }
    | {isParsingTxn}? dr=rowDataReference { values.add(new ReferenceValue.Substitution.Raw($dr.rawRef)); }
    ;

jsonInsertStatement [QualifiedName qn] returns [UpdateStatement.ParsedInsertJson expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        boolean ifNotExists = false;
        boolean defaultUnset = false;
    }
    : val=jsonValue
      ( K_DEFAULT ( K_NULL | ( { defaultUnset = true; } K_UNSET) ) )?
      ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsertJson(qn, attrs, $val.raw, defaultUnset, ifNotExists, stmtSrc(), isParsingTxn);
      }
    ;

jsonValue returns [Json.Raw raw]
    : s=STRING_LITERAL { $raw = new Json.Literal($s.text); }
    | ':' id=noncol_ident     { $raw = newJsonBindVariables($id.id); }
    | QMARK            { $raw = newJsonBindVariables(null); }
    ;

usingClause[Attributes.Raw attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[Attributes.Raw attrs]
    : K_TIMESTAMP ts=intValue { attrs.timestamp = $ts.raw; }
    | K_TTL t=intValue { attrs.timeToLive = $t.raw; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 * [IF (EXISTS | name = value, ...)];
 */
updateStatement returns [UpdateStatement.ParsedUpdate expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        UpdateStatement.OperationCollector operations = new UpdateStatement.OperationCollector();
        boolean ifExists = false;
        stmtBegins();
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          $expr = new UpdateStatement.ParsedUpdate($cf.name,
                                                   attrs,
                                                   operations,
                                                   $wclause.clause.build(),
                                                   $conditions.ctx == null ? Collections.<ColumnCondition.Raw>emptyList() : $conditions.conditions,
                                                   ifExists,
                                                   isParsingTxn,
                                                   stmtSrc());
     }
    ;

updateConditions returns [List<ColumnCondition.Raw> conditions]
    @init { $conditions = new ArrayList<ColumnCondition.Raw>(); }
    : c1=columnCondition { $conditions.add($c1.condition);} ( K_AND cn=columnCondition { $conditions.add($cn.condition); })*
    ;

/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [DeleteStatement.Parsed expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
        boolean ifExists = false;
        stmtBegins();
    }
    : K_DELETE ( dels=deleteSelection { columnDeletions = $dels.operations; } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          $expr = new DeleteStatement.Parsed($cf.name,
                                             attrs,
                                             columnDeletions,
                                             $wclause.clause.build(),
                                             $conditions.ctx == null ? Collections.<ColumnCondition.Raw>emptyList() : $conditions.conditions,
                                             ifExists,
                                             stmtSrc(),
                                             isParsingTxn);
      }
    ;

deleteSelection returns [List<Operation.RawDeletion> operations]
    : { $operations = new ArrayList<Operation.RawDeletion>(); }
          t1=deleteOp { $operations.add($t1.op); }
          (',' tN=deleteOp { $operations.add($tN.op); })*
    ;

deleteOp returns [Operation.RawDeletion op]
    : c=cident                { $op = new Operation.ColumnDeletion($c.id); }
    | c=cident '[' t=term ']' { $op = new Operation.ElementDeletion($c.id, $t.raw); }
    | c=cident '.' field=fident { $op = new Operation.FieldDeletion($c.id, $field.id); }
    ;

usingClauseDelete[Attributes.Raw attrs]
    : K_USING K_TIMESTAMP ts=intValue { attrs.timestamp = $ts.raw; }
    ;

/**
 * BEGIN BATCH
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement.Parsed expr]
    @init {
        BatchStatement.Type type = BatchStatement.Type.LOGGED;
        List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
        Attributes.Raw attrs = new Attributes.Raw();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = BatchStatement.Type.UNLOGGED; } | K_COUNTER { type = BatchStatement.Type.COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.add($s.statement); } )*
      K_APPLY K_BATCH
      {
          $expr = new BatchStatement.Parsed(type, attrs, statements);
      }
    ;

batchStatementObjective returns [ModificationStatement.Parsed statement]
    : i=insertStatement  { $statement = $i.expr; }
    | u=updateStatement  { $statement = $u.expr; }
    | d=deleteStatement  { $statement = $d.expr; }
    ;

/**
 * ex. conditional update returning pre-update values
 *
 * BEGIN TRANSACTION
 *   LET row1 = (SELECT * FROM <table> WHERE k=1 AND c=2);
 *   LET row2 = (SELECT * FROM <table> WHERE k=2 AND c=2);
 *   SELECT row1.v, row2.v;
 *   IF row1.v = 3 AND row2.v = 4 THEN
 *     UPDATE <table> SET v = row1.v + 1 WHERE k = 1 AND c = 2;
 *   END IF
 * COMMIT TRANSACTION
 *
 * ex. read-only transaction
 * 
 * BEGIN TRANSACTION
 *   SELECT * FROM <table> WHERE k=1 AND c=2;
 * COMMIT TRANSACTION
 *
 * ex. write-only transaction
 * 
 * BEGIN TRANSACTION
 *   INSERT INTO <table> (k, c, v) VALUES (0, 0, 1);
 * COMMIT TRANSACTION
 */
batchTxnStatement returns [TransactionStatement.Parsed expr]
    @init {
        isParsingTxn = true;
        List<SelectStatement.RawStatement> assignments = new ArrayList<>();
        SelectStatement.RawStatement select = null;
        List<RowDataReference.Raw> returning = null;
        List<ModificationStatement.Parsed> updates = new ArrayList<>();
    }
    : K_BEGIN K_TRANSACTION
      ( let=letStatement ';' { assignments.add($let.expr); })*
      ( ( s=selectStatement ';' { select = $s.expr; }) | ( K_SELECT drs=rowDataReferences ';' { returning = $drs.refs; }) )?
      ( K_IF conditions=txnConditions K_THEN { isTxnConditional = true; } )?
      ( upd=batchStatementObjective ';' { updates.add($upd.statement); } )*
      ( {!isTxnConditional}? (K_COMMIT K_TRANSACTION) | {isTxnConditional}? (K_END K_IF K_COMMIT K_TRANSACTION))
    {
        $expr = new TransactionStatement.Parsed(assignments, select, returning, updates, $conditions.ctx == null ? null : $conditions.conditions, references);
    }
    ;
    finally { isParsingTxn = false; }

rowDataReferences returns [List<RowDataReference.Raw> refs]
    : r1=rowDataReference { $refs = new ArrayList<RowDataReference.Raw>(); $refs.add($r1.rawRef); } (',' rN=rowDataReference { $refs.add($rN.rawRef); })*
    ;

rowDataReference returns [RowDataReference.Raw rawRef]
    @init { Selectable.RawIdentifier tuple = null; Selectable.Raw selectable = null; }
    @after { $rawRef = newRowDataReference(tuple, selectable); }
    : t=sident ('.' s=referenceSelection)? { tuple = $t.id; selectable = $s.ctx == null ? null : $s.s; }
    ;

referenceSelection returns [Selectable.Raw s]
    : g=referenceSelectionWithoutField m=selectorModifier[$g.s] {$s = $m.s;}
    ;

referenceSelectionWithoutField returns [Selectable.Raw s]
    @init { Selectable.Raw tmp = null; }
    @after { $s = tmp; }
    : sn=sident  { tmp=$sn.id; }
    | h=selectionTypeHint { tmp=$h.s; }
    | t=selectionTupleOrNestedSelector { tmp=$t.s; }
    | l=selectionList { tmp=$l.s; }
    | m=selectionMapOrSet { tmp=$m.s; }
    // UDTs are equivalent to maps from the syntax point of view, so the final decision will be done in Selectable.WithMapOrUdt
    ;

txnConditions returns [List<ConditionStatement.Raw> conditions]
    @init { $conditions = new ArrayList<ConditionStatement.Raw>(); }
    : txnColumnCondition[$conditions] ( K_AND txnColumnCondition[$conditions] )*
    ;

txnConditionKind returns [ConditionStatement.Kind op]
    : '='  { $op = ConditionStatement.Kind.EQ; }
    | '<'  { $op = ConditionStatement.Kind.LT; }
    | '<=' { $op = ConditionStatement.Kind.LTE; }
    | '>'  { $op = ConditionStatement.Kind.GT; }
    | '>=' { $op = ConditionStatement.Kind.GTE; }
    | '!=' { $op = ConditionStatement.Kind.NEQ; }
    ;

txnColumnCondition[List<ConditionStatement.Raw> conditions]
    : lhs=rowDataReference
      ( 
        K_IS 
        (
            K_NOT K_NULL { conditions.add(new ConditionStatement.Raw($lhs.rawRef, ConditionStatement.Kind.IS_NOT_NULL, null)); }
            | K_NULL { conditions.add(new ConditionStatement.Raw($lhs.rawRef, ConditionStatement.Kind.IS_NULL, null)); }
        )
        | op=txnConditionKind t=term { conditions.add(new ConditionStatement.Raw($lhs.rawRef, $op.op, $t.raw)); }
      )
    | lhsTerm=term op=txnConditionKind rhs=rowDataReference { conditions.add(new ConditionStatement.Raw($lhsTerm.raw, $op.op, $rhs.rawRef)); }
    ;

createAggregateStatement returns [CreateAggregateStatement.Raw stmt]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        List<CQL3Type.Raw> argTypes = new ArrayList<>();
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      K_AGGREGATE
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          v=comparatorType { argTypes.add($v.t); }
          ( ',' v=comparatorType { argTypes.add($v.t); } )*
        )?
      ')'
      K_SFUNC sfunc = allowedFunctionName
      K_STYPE stype = comparatorType
      (
        K_FINALFUNC ffunc = allowedFunctionName
      )?
      (
        K_INITCOND ival = term
      )?
      { $stmt = new CreateAggregateStatement.Raw($fn.s, argTypes, $stype.t, $sfunc.s, $ffunc.ctx == null ? null : $ffunc.s, $ival.ctx == null ? null : $ival.raw, orReplace, ifNotExists); }
    ;

dropAggregateStatement returns [DropAggregateStatement.Raw stmt]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean argsSpecified = false;
    }
    : K_DROP K_AGGREGATE
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argTypes.add($v.t); }
            ( ',' v=comparatorType { argTypes.add($v.t); } )*
          )?
        ')'
        { argsSpecified = true; }
      )?
      { $stmt = new DropAggregateStatement.Raw($fn.s, argTypes, argsSpecified, ifExists); }
    ;

createFunctionStatement returns [CreateFunctionStatement.Raw stmt]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        List<ColumnIdentifier> argNames = new ArrayList<>();
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean calledOnNullInput = false;
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      K_FUNCTION
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          k=noncol_ident v=comparatorType { argNames.add($k.id); argTypes.add($v.t); }
          ( ',' k=noncol_ident v=comparatorType { argNames.add($k.id); argTypes.add($v.t); } )*
        )?
      ')'
      ( (K_RETURNS K_NULL) | (K_CALLED { calledOnNullInput=true; })) K_ON K_NULL K_INPUT
      K_RETURNS returnType = comparatorType
      K_LANGUAGE language = IDENT
      K_AS body = STRING_LITERAL
      { $stmt = new CreateFunctionStatement.Raw(
          $fn.s, argNames, argTypes, $returnType.t, calledOnNullInput, LocalizeString.toLowerCaseLocalized($language.text), $body.text, orReplace, ifNotExists);
      }
    ;

dropFunctionStatement returns [DropFunctionStatement.Raw stmt]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argTypes = new ArrayList<>();
        boolean argsSpecified = false;
    }
    : K_DROP K_FUNCTION
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argTypes.add($v.t); }
            ( ',' v=comparatorType { argTypes.add($v.t); } )*
          )?
        ')'
        { argsSpecified = true; }
      )?
      { $stmt = new DropFunctionStatement.Raw($fn.s, argTypes, argsSpecified, ifExists); }
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [CreateKeyspaceStatement.Raw stmt]
    @init {
        KeyspaceAttributes attrs = new KeyspaceAttributes();
        boolean ifNotExists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? ks=keyspaceName
      K_WITH properties[attrs] { $stmt = new CreateKeyspaceStatement.Raw($ks.id, attrs, ifNotExists); }
    ;

/**
 * CREATE TABLE [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [CreateTableStatement.Raw stmt]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      cf=columnFamilyName { $stmt = new CreateTableStatement.Raw($cf.name, ifNotExists); }
      tableDefinition[$stmt]
    ;

tableDefinition[CreateTableStatement.Raw stmt]
    : '(' tableColumns[stmt] ( ',' tableColumns[stmt]? )* ')'
      ( K_WITH tableProperty[stmt] ( K_AND tableProperty[stmt] )*)?
    ;

tableColumns[CreateTableStatement.Raw stmt]
    @init { boolean isStatic = false; boolean isNotNull = false; }
    : k=ident v=comparatorType (K_STATIC { isStatic = true; })? (K_NOT K_NULL { isNotNull = true; })? (mask=columnMask)? (constraints=columnConstraints)? { $stmt.addColumn($k.id, $v.t, isStatic, isNotNull, $mask.ctx == null ? null : $mask.mask, $constraints.ctx == null ? null : $constraints.constraints); }
        (K_PRIMARY K_KEY { $stmt.setPartitionKeyColumn($k.id); })?
    | K_PRIMARY K_KEY '(' tablePartitionKey[stmt] (',' c=ident { $stmt.markClusteringColumn($c.id); } )* ')'
    ;

columnConstraints returns [ColumnConstraints.Raw constraints]
    @init {
        boolean isStatic = false;
        List constraintsList = new ArrayList();
    }
    : K_CHECK cc=columnConstraint { constraintsList.add($cc.constraint); } (K_AND cc=columnConstraint { constraintsList.add($cc.constraint); })* { $constraints = new ColumnConstraints.Raw(constraintsList); }
    ;

columnConstraint returns [ColumnConstraint constraint]
    @init { List<String> arguments = new ArrayList<>(); }
    : K_NOT K_NULL
    {
        $constraint = new UnaryFunctionColumnConstraint.Raw("NOT_NULL").prepare();
    }
    | funcName=ident columnConstraintsArguments[arguments] (op=relationType t=value)?
    {
        if ($op.ctx != null && $t.ctx != null)
        {
            $constraint = new FunctionColumnConstraint.Raw($funcName.id, arguments, $op.op, $t.raw.getText()).prepare();
        }
        else
        {
            $constraint = new UnaryFunctionColumnConstraint.Raw($funcName.id, arguments).prepare();
        }
    }
    | k=ident op=relationType t=value
    {
        $constraint = new ScalarColumnConstraint.Raw($k.id, $op.op, $t.raw.getText()).prepare();
    }
    | funcName=ident
    {
         $constraint = new UnaryFunctionColumnConstraint.Raw($funcName.id).prepare();
    }
    ;

columnMask returns [ColumnMask.Raw mask]
    @init { List<Term.Raw> arguments = new ArrayList<>(); }
    : K_MASKED K_WITH name=functionName columnMaskArguments[arguments] { $mask = new ColumnMask.Raw($name.s, arguments); }
    | K_MASKED K_WITH K_DEFAULT { $mask = new ColumnMask.Raw(FunctionName.nativeFunction("mask_default"), arguments); }
    ;

columnMaskArguments[List<Term.Raw> arguments]
    : '('  ')' | '(' c=term { arguments.add($c.raw); } (',' cn=term { arguments.add($cn.raw); })* ')'
    ;

columnConstraintsArguments[List<String> arguments]
    : '('  ')'
    | '(' c=term { try { arguments.add($c.raw.toString()); } catch (Throwable t) { throw new SyntaxException("Constraint function parameters need to be strings."); }; } (',' cn=term { try { arguments.add($cn.raw.toString()); } catch (Throwable t) { throw new SyntaxException("Constraint function parameters need to be strings."); }; })* ')'
    | '(' ci=ident { if (true) throw new SyntaxException("Constraint function parameters need to be strings."); } (',' cni=ident)* ')'
    ;

tablePartitionKey[CreateTableStatement.Raw stmt]
    @init {List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>();}
    @after{ $stmt.setPartitionKeyColumns(l); }
    : k1=ident { l.add($k1.id);}
    | '(' k1=ident { l.add($k1.id); } ( ',' kn=ident { l.add($kn.id); } )* ')'
    ;

tableProperty[CreateTableStatement.Raw stmt]
    : property[stmt.attrs]
    | K_COMPACT K_STORAGE { $stmt.setCompactStorage(); }
    | K_CLUSTERING K_ORDER K_BY '(' tableClusteringOrder[stmt] (',' tableClusteringOrder[stmt])* ')'
    ;

tableClusteringOrder[CreateTableStatement.Raw stmt]
    @init{ boolean ascending = true; }
    : k=ident (K_ASC | K_DESC { ascending = false; } ) { $stmt.extendClusteringOrder($k.id, ascending); }
    ;

/**
 * CREATE TABLE [IF NOT EXISTS] <NEW_TABLE> LIKE <OLD_TABLE> WITH <property> = <value> AND ...;
 */
copyTableStatement returns  [CopyTableStatement.Raw stmt]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      newCf=columnFamilyName K_LIKE oldCf=columnFamilyName
      { $stmt = new CopyTableStatement.Raw($newCf.name, $oldCf.name, ifNotExists); }
      ( K_WITH propertyOrOption[$stmt] ( K_AND propertyOrOption[$stmt] )*)?
    ;

propertyOrOption[CopyTableStatement.Raw stmt]
    : likeOption[stmt]
    | property[stmt.attrs]
    ;

likeOption[CopyTableStatement.Raw stmt]
    : K_INDEXES {$stmt.addLikeOption(CopyTableStatement.CreateLikeOption.INDEXES);}
    | K_COMMENTS {$stmt.addLikeOption(CopyTableStatement.CreateLikeOption.COMMENTS);}
    | K_SECURITY K_LABELS {$stmt.addLikeOption(CopyTableStatement.CreateLikeOption.SECURITY_LABELS);}
    ;

/**
 * CREATE TYPE foo (
 *    <name1> <type1>,
 *    <name2> <type2>,
 *    ....
 * )
 */
createTypeStatement returns [CreateTypeStatement.Raw stmt]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
         tn=userTypeName { $stmt = new CreateTypeStatement.Raw($tn.name, ifNotExists); }
         '(' typeColumns[$stmt] ( ',' typeColumns[$stmt]? )* ')'
    ;

typeColumns[CreateTypeStatement.Raw stmt]
    : k=fident v=comparatorType { $stmt.addField($k.id, $v.t); }
    ;

/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [CreateIndexStatement.Raw stmt]
    @init {
        IndexAttributes props = new IndexAttributes();
        boolean ifNotExists = false;
        QualifiedName name = new QualifiedName();
        List<IndexTarget.Raw> targets = new ArrayList<>();
    }
    : K_CREATE (K_CUSTOM { props.isCustom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        (idxName[name])? K_ON cf=columnFamilyName '(' (indexIdent[targets] (',' indexIdent[targets])*)? ')'
        (K_USING cls=STRING_LITERAL { props.customClass = $cls.text; })?
        (K_WITH properties[props])?
      { $stmt = new CreateIndexStatement.Raw($cf.name, name, targets, props, ifNotExists); }
    ;

indexIdent [List<IndexTarget.Raw> targets]
    : c=cident                   { $targets.add(IndexTarget.Raw.simpleIndexOn($c.id)); }
    | K_VALUES '(' c=cident ')'  { $targets.add(IndexTarget.Raw.valuesOf($c.id)); }
    | K_KEYS '(' c=cident ')'    { $targets.add(IndexTarget.Raw.keysOf($c.id)); }
    | K_ENTRIES '(' c=cident ')' { $targets.add(IndexTarget.Raw.keysAndValuesOf($c.id)); }
    | K_FULL '(' c=cident ')'    { $targets.add(IndexTarget.Raw.fullCollection($c.id)); }
    ;

/**
 * CREATE MATERIALIZED VIEW <viewName> AS
 *  SELECT <columns>
 *  FROM <CF>
 *  WHERE <pkColumns> IS NOT NULL
 *  PRIMARY KEY (<pkColumns>)
 *  WITH <property> = <value> AND ...;
 */
createMaterializedViewStatement returns [CreateViewStatement.Raw stmt]
    @init {
        boolean ifNotExists = false;
    }
    : K_CREATE K_MATERIALIZED K_VIEW (K_IF K_NOT K_EXISTS { ifNotExists = true; })? cf=columnFamilyName K_AS
        K_SELECT sclause=selectors K_FROM basecf=columnFamilyName
        (K_WHERE wclause=whereClause)?
        {
             WhereClause where = $wclause.ctx == null ? WhereClause.empty() : $wclause.clause.build();
             $stmt = new CreateViewStatement.Raw($basecf.name, $cf.name, $sclause.expr, where, ifNotExists);
        }
        viewPrimaryKey[$stmt]
        ( K_WITH viewProperty[$stmt] ( K_AND viewProperty[$stmt] )*)?
    ;

viewPrimaryKey[CreateViewStatement.Raw stmt]
    : K_PRIMARY K_KEY '(' viewPartitionKey[stmt] (',' c=ident { $stmt.markClusteringColumn($c.id); } )* ')'
    ;

viewPartitionKey[CreateViewStatement.Raw stmt]
    @init {List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>();}
    @after{ $stmt.setPartitionKeyColumns(l); }
    : k1=ident { l.add($k1.id);}
    | '(' k1=ident { l.add($k1.id); } ( ',' kn=ident { l.add($kn.id); } )* ')'
    ;

viewProperty[CreateViewStatement.Raw stmt]
    : property[stmt.attrs]
    | K_COMPACT K_STORAGE { if (true) throw new SyntaxException("COMPACT STORAGE tables are not allowed starting with version 4.0"); }
    | K_CLUSTERING K_ORDER K_BY '(' viewClusteringOrder[stmt] (',' viewClusteringOrder[stmt])* ')'
    ;

viewClusteringOrder[CreateViewStatement.Raw stmt]
    @init{ boolean ascending = true; }
    : k=ident (K_ASC | K_DESC { ascending = false; } ) { $stmt.extendClusteringOrder($k.id, ascending); }
    ;

/**
 * CREATE TRIGGER triggerName ON columnFamily USING 'triggerClass';
 */
createTriggerStatement returns [CreateTriggerStatement.Raw stmt]
    @init {
        boolean ifNotExists = false;
    }
    : K_CREATE K_TRIGGER (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? (name=ident)
        K_ON cf=columnFamilyName K_USING cls=STRING_LITERAL
      { $stmt = new CreateTriggerStatement.Raw($cf.name, $name.id.toString(), $cls.text, ifNotExists); }
    ;

/**
 * DROP TRIGGER [IF EXISTS] triggerName ON columnFamily;
 */
dropTriggerStatement returns [DropTriggerStatement.Raw stmt]
     @init { boolean ifExists = false; }
    : K_DROP K_TRIGGER (K_IF K_EXISTS { ifExists = true; } )? (name=ident) K_ON cf=columnFamilyName
      { $stmt = new DropTriggerStatement.Raw($cf.name, $name.id.toString(), ifExists); }
    ;

/**
 * ALTER KEYSPACE [IF EXISTS] <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement returns [AlterKeyspaceStatement.Raw stmt]
    @init {
     KeyspaceAttributes attrs = new KeyspaceAttributes();
     boolean ifExists = false;
    }
    : K_ALTER K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName
        K_WITH properties[attrs] { $stmt = new AlterKeyspaceStatement.Raw($ks.id, attrs, ifExists); }
    ;

/**
 * ALTER TABLE <table> ALTER <column> TYPE <newtype>;
 * ALTER TABLE [IF EXISTS] <table> ALTER [IF EXISTS] <column> MASKED WITH <maskFunction>);
 * ALTER TABLE [IF EXISTS] <table> ALTER [IF EXISTS] <column> DROP MASKED;
 * ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] <column> <newtype> <maskFunction>; | ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] (<column> <newtype> <maskFunction>, <column1> <newtype1>  <maskFunction1>..... <column n> <newtype n>  <maskFunction n>)
 * ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] <column>; | ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] ( <column>,<column1>.....<column n>)
 * ALTER TABLE [IF EXISTS] <table> RENAME [IF EXISTS] <column> TO <column>;
 * ALTER TABLE [IF EXISTS] <table> WITH <property> = <value>;
 */
alterTableStatement returns [AlterTableStatement.Raw stmt]
    @init { boolean ifExists = false; ColumnMask.Raw addMask = null; ColumnConstraints.Raw addCons = null; }
    : K_ALTER K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )?
      cf=columnFamilyName { $stmt = new AlterTableStatement.Raw($cf.name, ifExists); }
      (
        K_ALTER id=cident K_TYPE v=comparatorType { $stmt.alter($id.id, $v.t); }

      | K_ALTER ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )? id=cident
              ( mask=columnMask { $stmt.mask($id.id, $mask.mask); }
              | constraints=columnConstraints { $stmt.constraint($id.id, $constraints.constraints); }
              | K_DROP K_MASKED { $stmt.mask($id.id, null); }
              | K_DROP K_CHECK { $stmt.constraint($id.id, null); })

      | K_ADD ( K_IF K_NOT K_EXISTS { $stmt.ifColumnNotExists(true); } )?
              (        aid=ident  v=comparatorType  b=isStaticColumn (m=columnMask { addMask = $m.mask; })? (c=columnConstraints { addCons = $c.constraints; })? { $stmt.add($aid.id,  $v.t,  $b.isStaticCol, addMask, addCons); addMask=null; addCons=null; }
               | ('('  id1=ident v1=comparatorType b1=isStaticColumn (m1=columnMask { addMask = $m1.mask; })? (c=columnConstraints { addCons = $c.constraints; })? { $stmt.add($id1.id, $v1.t, $b1.isStaticCol, addMask, addCons); addMask=null; addCons=null; }
                 ( ',' idn=ident vn=comparatorType bn=isStaticColumn (mn=columnMask { addMask = $mn.mask; })? (cn=columnConstraints { addCons = $cn.constraints; })? { $stmt.add($idn.id, $vn.t, $bn.isStaticCol, addMask, addCons); addMask=null; addCons=null; } )* ')') )

      | K_DROP ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )?
               (       did=ident { $stmt.drop($did.id);  }
               | ('('  id1=ident { $stmt.drop($id1.id); }
                 ( ',' idn=ident { $stmt.drop($idn.id); } )* ')') )
               ( K_USING K_TIMESTAMP t=INTEGER { $stmt.timestamp(Long.parseLong(Constants.Literal.integer($t.text).getText())); } )?

      | K_RENAME ( K_IF K_EXISTS { $stmt.ifColumnExists(true); } )?
               (        id1=ident K_TO toId1=ident { $stmt.rename($id1.id, $toId1.id); }
                ( K_AND idn=ident K_TO toIdn=ident { $stmt.rename($idn.id, $toIdn.id); } )* )

      | K_DROP K_COMPACT K_STORAGE { $stmt.dropCompactStorage(); }

      | K_WITH properties[$stmt.attrs] { $stmt.attrs(); }
      )
    ;

isStaticColumn returns [boolean isStaticCol]
    @init { boolean isStatic = false; }
    : (K_STATIC { isStatic=true; })? { $isStaticCol = isStatic; }
    ;

alterMaterializedViewStatement returns [AlterViewStatement.Raw stmt]
    @init {
        TableAttributes attrs = new TableAttributes();
        boolean ifExists = false;
    }
    : K_ALTER K_MATERIALIZED K_VIEW (K_IF K_EXISTS { ifExists = true; } )? name=columnFamilyName
          K_WITH properties[attrs]
    {
        $stmt = new AlterViewStatement.Raw($name.name, attrs, ifExists);
    }
    ;


/**
 * ALTER TYPE [IF EXISTS] <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE [IF EXISTS] <name> ADD [IF NOT EXISTS]<field> <newtype>;
 * ALTER TYPE [IF EXISTS] <name> RENAME [IF EXISTS] <field> TO <newtype> AND ...;
 */
alterTypeStatement returns [AlterTypeStatement.Raw stmt]
    @init {
        boolean ifExists = false;
    }
    : K_ALTER K_TYPE (K_IF K_EXISTS { ifExists = true; } )? name=userTypeName { $stmt = new AlterTypeStatement.Raw($name.name, ifExists); }
      (
        K_ALTER   f=fident K_TYPE v=comparatorType { $stmt.alter($f.id, $v.t); }

      | K_ADD (K_IF K_NOT K_EXISTS { $stmt.ifFieldNotExists(true); } )?     f=fident v=comparatorType        { $stmt.add($f.id, $v.t); }

      | K_RENAME (K_IF K_EXISTS { $stmt.ifFieldExists(true); } )? f1=fident K_TO toF1=fident        { $stmt.rename($f1.id, $toF1.id); }
         ( K_AND fn=fident K_TO toFn=fident        { $stmt.rename($fn.id, $toFn.id); } )*
      )
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [DropKeyspaceStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName { $stmt = new DropKeyspaceStatement.Raw($ks.id, ifExists); }
    ;

/**
 * COMMENT ON KEYSPACE <keyspace> IS <comment>;
 */
commentOnKeyspaceStatement returns [CommentOnKeyspaceStatement.Raw stmt]
    : K_COMMENT K_ON K_KEYSPACE ks=keyspaceName K_IS (comment=STRING_LITERAL | K_NULL) { $stmt = new CommentOnKeyspaceStatement.Raw($ks.id, $comment != null ? $comment.text : null); }
    ;

/**
 * SECURITY LABEL [FOR <provider>] ON KEYSPACE <keyspace> IS <label>;
 */
securityLabelOnKeyspaceStatement returns [SecurityLabelOnKeyspaceStatement.Raw stmt]
    @init { String provider = null; }
    : K_SECURITY K_LABEL (K_FOR prov=noncol_ident { provider = $prov.id.toString(); })? K_ON K_KEYSPACE ks=keyspaceName K_IS (label=STRING_LITERAL | K_NULL) { $stmt = new SecurityLabelOnKeyspaceStatement.Raw($ks.id, $label != null ? $label.text : null, provider); }
    ;

/**
 * COMMENT ON TABLE <table> IS <comment>;
 */
commentOnTableStatement returns [CommentOnTableStatement.Raw stmt]
    : K_COMMENT K_ON K_COLUMNFAMILY cf=columnFamilyName K_IS (comment=STRING_LITERAL | K_NULL) { $stmt = new CommentOnTableStatement.Raw($cf.name, $comment != null ? $comment.text : null); }
    ;

/**
 * SECURITY LABEL [FOR <provider>] ON TABLE <table> IS <label>;
 */
securityLabelOnTableStatement returns [SecurityLabelOnTableStatement.Raw stmt]
    @init { String provider = null; }
    : K_SECURITY K_LABEL (K_FOR prov=noncol_ident { provider = $prov.id.toString(); })? K_ON K_COLUMNFAMILY cf=columnFamilyName K_IS (label=STRING_LITERAL | K_NULL) { $stmt = new SecurityLabelOnTableStatement.Raw($cf.name, $label != null ? $label.text : null, provider); }
    ;

/**
 * COMMENT ON COLUMN <table>.<column> IS <comment>;
 * COMMENT ON COLUMN <keyspace>.<table>.<column> IS <comment>;
 */
commentOnColumnStatement returns [CommentOnColumnStatement.Raw stmt]
    : K_COMMENT K_ON K_COLUMN columnRef=columnReference K_IS (comment=STRING_LITERAL | K_NULL)
      { $stmt = new CommentOnColumnStatement.Raw($columnRef.table, $columnRef.column, $comment != null ? $comment.text : null); }
    ;

/**
 * SECURITY LABEL [FOR <provider>] ON COLUMN <table>.<column> IS <label>;
 * SECURITY LABEL [FOR <provider>] ON COLUMN <keyspace>.<table>.<column> IS <label>;
 */
securityLabelOnColumnStatement returns [SecurityLabelOnColumnStatement.Raw stmt]
    @init { String provider = null; }
    : K_SECURITY K_LABEL (K_FOR prov=noncol_ident { provider = $prov.id.toString(); })? K_ON K_COLUMN columnRef=columnReference K_IS (label=STRING_LITERAL | K_NULL)
      { $stmt = new SecurityLabelOnColumnStatement.Raw($columnRef.table, $columnRef.column, $label != null ? $label.text : null, provider); }
    ;

/**
 * COMMENT ON TYPE <type> IS <comment>;
 */
commentOnUserTypeStatement returns [CommentOnUserTypeStatement.Raw stmt]
    : K_COMMENT K_ON K_TYPE tn=userTypeName K_IS (comment=STRING_LITERAL | K_NULL) { $stmt = new CommentOnUserTypeStatement.Raw($tn.name, $comment != null ? $comment.text : null); }
    ;

/**
 * SECURITY LABEL [FOR <provider>] ON TYPE <type> IS <label>;
 */
securityLabelOnUserTypeStatement returns [SecurityLabelOnUserTypeStatement.Raw stmt]
    @init { String provider = null; }
    : K_SECURITY K_LABEL (K_FOR prov=noncol_ident { provider = $prov.id.toString(); })? K_ON K_TYPE tn=userTypeName K_IS (label=STRING_LITERAL | K_NULL) { $stmt = new SecurityLabelOnUserTypeStatement.Raw($tn.name, $label != null ? $label.text : null, provider); }
    ;

/**
 * COMMENT ON FIELD <type>.<field> IS <comment>;
 * COMMENT ON FIELD <keyspace>.<type>.<field> IS <comment>;
 */
commentOnUserTypeFieldStatement returns [CommentOnUserTypeFieldStatement.Raw stmt]
    : K_COMMENT K_ON K_FIELD typeFieldRef=typeFieldReference K_IS (comment=STRING_LITERAL | K_NULL)
      { $stmt = new CommentOnUserTypeFieldStatement.Raw($typeFieldRef.typeName, $typeFieldRef.field, $comment != null ? $comment.text : null); }
    ;

/**
 * SECURITY LABEL [FOR <provider>] ON FIELD <type>.<field> IS <label>;
 * SECURITY LABEL [FOR <provider>] ON FIELD <keyspace>.<type>.<field> IS <label>;
 */
securityLabelOnUserTypeFieldStatement returns [SecurityLabelOnUserTypeFieldStatement.Raw stmt]
    @init { String provider = null; }
    : K_SECURITY K_LABEL (K_FOR prov=noncol_ident { provider = $prov.id.toString(); })? K_ON K_FIELD typeFieldRef=typeFieldReference K_IS (label=STRING_LITERAL | K_NULL)
      { $stmt = new SecurityLabelOnUserTypeFieldStatement.Raw($typeFieldRef.typeName, $typeFieldRef.field, $label != null ? $label.text : null, provider); }
    ;

/**
 * DROP TABLE [IF EXISTS] <table>;
 */
dropTableStatement returns [DropTableStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )? name=columnFamilyName { $stmt = new DropTableStatement.Raw($name.name, ifExists); }
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement returns [DropTypeStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_TYPE (K_IF K_EXISTS { ifExists = true; } )? name=userTypeName { $stmt = new DropTypeStatement.Raw($name.name, ifExists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { ifExists = true; } )? index=indexName
      { $stmt = new DropIndexStatement.Raw($index.name, ifExists); }
    ;

/**
 * DROP MATERIALIZED VIEW [IF EXISTS] <view_name>
 */
dropMaterializedViewStatement returns [DropViewStatement.Raw stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_MATERIALIZED K_VIEW (K_IF K_EXISTS { ifExists = true; } )? cf=columnFamilyName
      { $stmt = new DropViewStatement.Raw($cf.name, ifExists); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [TruncateStatement stmt]
    : K_TRUNCATE (K_COLUMNFAMILY)? cf=columnFamilyName { $stmt = new TruncateStatement($cf.name); }
    ;

/**
 * GRANT <permission>[, <permission>]* | ALL ON <resource> TO <rolename>
 */
grantPermissionsStatement returns [GrantPermissionsStatement stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          grantee=userOrRoleName
      { $stmt = new GrantPermissionsStatement(filterPermissions($permissionOrAll.perms, $resource.res), $resource.res, $grantee.name); }
    ;

/**
 * REVOKE <permission>[, <permission>]* | ALL ON <resource> FROM <rolename>
 */
revokePermissionsStatement returns [RevokePermissionsStatement stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          revokee=userOrRoleName
      { $stmt = new RevokePermissionsStatement(filterPermissions($permissionOrAll.perms, $resource.res), $resource.res, $revokee.name); }
    ;

/**
 * GRANT ROLE <rolename> TO <grantee>
 */
grantRoleStatement returns [GrantRoleStatement stmt]
    : K_GRANT
          role=userOrRoleName
      K_TO
          grantee=userOrRoleName
      { $stmt = new GrantRoleStatement($role.name, $grantee.name); }
    ;

/**
 * REVOKE ROLE <rolename> FROM <revokee>
 */
revokeRoleStatement returns [RevokeRoleStatement stmt]
    : K_REVOKE
          role=userOrRoleName
      K_FROM
          revokee=userOrRoleName
      { $stmt = new RevokeRoleStatement($role.name, $revokee.name); }
    ;

listPermissionsStatement returns [ListPermissionsStatement stmt]
    @init {
        IResource resource = null;
        boolean recursive = true;
        RoleName grantee = new RoleName();
    }
    : K_LIST
          permissionOrAll
      ( K_ON resource { resource = $resource.res; } )?
      ( K_OF roleName[grantee] )?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListPermissionsStatement($permissionOrAll.perms, resource, grantee, recursive); }
    ;

permission returns [Permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE | K_DESCRIBE | K_EXECUTE | K_UNMASK | K_SELECT_MASKED)
    { $perm = Permission.valueOf(LocalizeString.toUpperCaseLocalized($p.text)); }
    ;

permissionOrAll returns [Set<Permission> perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = Permission.ALL; }
    | p=permission ( K_PERMISSION )? { $perms = EnumSet.of($p.perm); } ( ',' p=permission ( K_PERMISSION )? { $perms.add($p.perm); } )*
    ;

resource returns [IResource res]
    : d=dataResource { $res = $d.res; }
    | r=roleResource { $res = $r.res; }
    | f=functionResource { $res = $f.res; }
    | j=jmxResource { $res = $j.res; }
    ;

dataResource returns [DataResource res]
    : K_ALL K_KEYSPACES { $res = DataResource.root(); }
    | K_KEYSPACE ks = keyspaceName { $res = DataResource.keyspace($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName { $res = DataResource.table($cf.name.getKeyspace(), $cf.name.getName()); }
    | K_ALL K_TABLES K_IN K_KEYSPACE ks = keyspaceName { $res = DataResource.allTables($ks.id); }
    ;

jmxResource returns [JMXResource res]
    : K_ALL K_MBEANS { $res = JMXResource.root(); }
    // when a bean name (or pattern) is supplied, validate that it's a legal ObjectName
    // also, just to be picky, if the "MBEANS" form is used, only allow a pattern style names
    | K_MBEAN mbean { $res = JMXResource.mbean(canonicalizeObjectName($mbean.text, false)); }
    | K_MBEANS mbean { $res = JMXResource.mbean(canonicalizeObjectName($mbean.text, true)); }
    ;

roleResource returns [RoleResource res]
    : K_ALL K_ROLES { $res = RoleResource.root(); }
    | K_ROLE role = userOrRoleName { $res = RoleResource.role($role.name.getName()); }
    ;

functionResource returns [FunctionResource res]
    @init {
        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
    }
    : K_ALL K_FUNCTIONS { $res = FunctionResource.root(); }
    | K_ALL K_FUNCTIONS K_IN K_KEYSPACE ks = keyspaceName { $res = FunctionResource.keyspace($ks.id); }
    // Arg types are mandatory for DCL statements on Functions
    | K_FUNCTION fn=functionName
      (
        '('
          (
            v=comparatorType { argsTypes.add($v.t); }
            ( ',' v=comparatorType { argsTypes.add($v.t); } )*
          )?
        ')'
      )
      { $res = FunctionResource.functionFromCql($fn.s.keyspace, $fn.s.name, argsTypes); }
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [CreateRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        opts.setOption(IRoleManager.Option.LOGIN, true);
        boolean superuser = false;
        boolean ifNotExists = false;
        RoleName name = new RoleName();
    }
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS { ifNotExists = true; })? u=username { name.setName($u.text, true); }
      ( K_WITH userPassword[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { opts.setOption(IRoleManager.Option.SUPERUSER, superuser);
        if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
        {
           throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
        }
        $stmt = new CreateRoleStatement(name, opts, DCPermissions.all(), CIDRPermissions.all(), ifNotExists);
      }
    ;

/**
 * ALTER USER [IF EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [AlterRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        RoleName name = new RoleName();
        boolean ifExists = false;
    }
    : K_ALTER K_USER (K_IF K_EXISTS { ifExists = true; })? u=username { name.setName($u.text, true); }
      ( K_WITH userPassword[opts] )?
      ( K_SUPERUSER { opts.setOption(IRoleManager.Option.SUPERUSER, true); }
        | K_NOSUPERUSER { opts.setOption(IRoleManager.Option.SUPERUSER, false); } ) ?
      {
         if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
         {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
         }
         if (opts.getPassword().isPresent() && opts.isGeneratedPassword())
         {
            throw new SyntaxException("Options 'password' and 'generated password' are mutually exclusive");
         }
         if (opts.getHashedPassword().isPresent() && opts.isGeneratedPassword())
         {
            throw new SyntaxException("Options 'hashed password' and 'generated password' are mutually exclusive");
         }
         $stmt = new AlterRoleStatement(name, opts, null, null, ifExists);
      }
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement returns [DropRoleStatement stmt]
    @init {
        boolean ifExists = false;
        RoleName name = new RoleName();
    }
    : K_DROP K_USER (K_IF K_EXISTS { ifExists = true; })? u=username { name.setName($u.text, true); $stmt = new DropRoleStatement(name, ifExists); }
    ;
/**
 * ADD IDENTITY [IF NOT EXISTS] <identity> TO ROLE <role>
 */
addIdentityStatement returns [AddIdentityStatement stmt]
    @init {
        String identity = null;
        String role = null;
        boolean ifNotExists = false;
    }
    : K_ADD K_IDENTITY (K_IF K_NOT K_EXISTS { ifNotExists = true; })? u=identity { identity= $u.text; } K_TO K_ROLE r=identity { role=$r.text; $stmt = new AddIdentityStatement(identity, role, ifNotExists); }
    ;

/**
 * DROP IDENTITY [IF EXISTS] <identity>
 */
 dropIdentityStatement returns [DropIdentityStatement stmt]
      @init {
          boolean ifExists = false;
          String identity = null;
      }
      : K_DROP K_IDENTITY (K_IF K_EXISTS { ifExists = true; })? u=identity { identity= $u.text; $stmt = new DropIdentityStatement(identity, ifExists);}
      ;

/**
 * LIST USERS
 */
listUsersStatement returns [ListRolesStatement stmt]
    : K_LIST K_USERS { $stmt = new ListUsersStatement(); }
    ;

/**
 * CREATE [GENERATED] ROLE [IF NOT EXISTS] <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  GENERATED PASSWORD
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 *  ACCESS TO ALL DATACENTERS
 *  ACCESS TO DATACENTERS { dcPermission (, dcPermission)* }
 *  ACCESS FROM ALL CIDRS
 *  ACCESS FROM CIDRS { cidrPermission (, cidrPermission)* }
 */
createRoleStatement returns [CreateRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        DCPermissions.Builder dcperms = DCPermissions.builder();
        CIDRPermissions.Builder cidrperms = CIDRPermissions.builder();
        boolean ifNotExists = false;
        boolean isGeneratedName = false;
    }
    : K_CREATE (K_GENERATED { isGeneratedName = true; })? K_ROLE (K_IF K_NOT K_EXISTS { ifNotExists = true; })? (name=userOrRoleName)?
      ( K_WITH roleOptions[opts, dcperms, cidrperms] )?
      {
        // set defaults if they weren't explictly supplied
        if (!opts.getLogin().isPresent())
        {
            opts.setOption(IRoleManager.Option.LOGIN, false);
        }
        if (!opts.getSuperuser().isPresent())
        {
            opts.setOption(IRoleManager.Option.SUPERUSER, false);
        }
        if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
        {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
        }
        if (opts.getPassword().isPresent() && opts.isGeneratedPassword())
        {
            throw new SyntaxException("Options 'password' and 'generated password' are mutually exclusive");
        }
        if (opts.getHashedPassword().isPresent() && opts.isGeneratedPassword())
        {
           throw new SyntaxException("Options 'hashed password' and 'generated password' are mutually exclusive");
        }
        if (isGeneratedName)
        {
           if ($name.ctx != null)
           {
               throw new SyntaxException("Name can not be specified together with GENERATED keyword.");
           }
           if (ifNotExists)
           {
               throw new SyntaxException("GENERATED keyword for role creation can not be used together with IF NOT EXISTS.");
           }
           opts.setOption(IRoleManager.Option.GENERATED_NAME, true);
        }
        $stmt = new CreateRoleStatement($name.ctx == null ? null : $name.name, opts, dcperms.build(), cidrperms.build(), ifNotExists);
      }
    ;

/**
 * ALTER ROLE [IF EXISTS] <rolename> [ [WITH] option [ [AND] option ]* ]
 *
 * where option can be:
 *  PASSWORD = '<password>'
 *  SUPERUSER = (true|false)
 *  LOGIN = (true|false)
 *  OPTIONS = { 'k1':'v1', 'k2':'v2'}
 *  ACCESS TO ALL DATACENTERS
 *  ACCESS TO DATACENTERS { dcPermission (, dcPermission)* }
 *  ACCESS FROM ALL CIDRS
 *  ACCESS FROM CIDRS { cidrPermission (, cidrPermission)* }
 */
alterRoleStatement returns [AlterRoleStatement stmt]
    @init {
        RoleOptions opts = new RoleOptions();
        DCPermissions.Builder dcperms = DCPermissions.builder();
        CIDRPermissions.Builder cidrperms = CIDRPermissions.builder();
        boolean ifExists = false;
    }
    : K_ALTER K_ROLE (K_IF K_EXISTS { ifExists = true; })? name=userOrRoleName
      ( K_WITH roleOptions[opts, dcperms, cidrperms] )?
      {
         if (opts.getPassword().isPresent() && opts.getHashedPassword().isPresent())
         {
            throw new SyntaxException("Options 'password' and 'hashed password' are mutually exclusive");
         }
         if (opts.getPassword().isPresent() && opts.isGeneratedPassword())
         {
            throw new SyntaxException("Options 'password' and 'generated password' are mutually exclusive");
         }
         if (opts.getHashedPassword().isPresent() && opts.isGeneratedPassword())
         {
            throw new SyntaxException("Options 'hashed password' and 'generated password' are mutually exclusive");
         }
         $stmt = new AlterRoleStatement($name.name, opts, dcperms.isModified() ? dcperms.build() : null, cidrperms.isModified() ? cidrperms.build() : null, ifExists);
      }
    ;

/**
 * DROP ROLE [IF EXISTS] <rolename>
 */
dropRoleStatement returns [DropRoleStatement stmt]
    @init {
        boolean ifExists = false;
    }
    : K_DROP K_ROLE (K_IF K_EXISTS { ifExists = true; })? name=userOrRoleName
      { $stmt = new DropRoleStatement($name.name, ifExists); }
    ;

/**
 * LIST ROLES [OF <rolename>] [NORECURSIVE]
 */
listRolesStatement returns [ListRolesStatement stmt]
    @init {
        boolean recursive = true;
        RoleName grantee = new RoleName();
    }
    : K_LIST K_ROLES
      ( K_OF roleName[grantee])?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListRolesStatement(grantee, recursive); }
    ;

/**
 * LIST SUPERUSERS
 */
listSuperUsersStatement returns [ListSuperUsersStatement stmt]
    @init {
    }
    : K_LIST K_SUPERUSERS { $stmt = new ListSuperUsersStatement(); }
    ;

roleOptions[RoleOptions opts, DCPermissions.Builder dcperms, CIDRPermissions.Builder cidrperms]
    : roleOption[opts, dcperms, cidrperms] (K_AND roleOption[opts, dcperms, cidrperms])*
    ;

roleOption[RoleOptions opts, DCPermissions.Builder dcperms, CIDRPermissions.Builder cidrperms]
    :  K_PASSWORD '=' v=STRING_LITERAL { opts.setOption(IRoleManager.Option.PASSWORD, $v.text); }
    |  K_GENERATED K_PASSWORD { opts.setOption(IRoleManager.Option.GENERATED_PASSWORD, Boolean.TRUE); } 
    |  K_HASHED K_PASSWORD '=' v=STRING_LITERAL { opts.setOption(IRoleManager.Option.HASHED_PASSWORD, $v.text); }
    |  K_OPTIONS '=' m=fullMapLiteral { opts.setOption(IRoleManager.Option.OPTIONS, convertPropertyMap($m.map)); }
    |  K_SUPERUSER '=' b=BOOLEAN { opts.setOption(IRoleManager.Option.SUPERUSER, Boolean.valueOf($b.text)); }
    |  K_LOGIN '=' b=BOOLEAN { opts.setOption(IRoleManager.Option.LOGIN, Boolean.valueOf($b.text)); }
    |  K_ACCESS K_TO K_ALL K_DATACENTERS { dcperms.all(); }
    |  K_ACCESS K_TO K_DATACENTERS '{' dcPermission[dcperms] (',' dcPermission[dcperms])* '}'
    |  K_ACCESS K_FROM K_ALL K_CIDRS { cidrperms.all(); }
    |  K_ACCESS K_FROM K_CIDRS '{' cidrPermission[cidrperms] (',' cidrPermission[cidrperms])* '}'
    ;

dcPermission[DCPermissions.Builder builder]
    : dc=STRING_LITERAL { builder.add($dc.text); }
    ;

cidrPermission[CIDRPermissions.Builder builder]
    : cidr=STRING_LITERAL { builder.add($cidr.text); }
    ;

// for backwards compatibility in CREATE/ALTER USER, this has no '='
userPassword[RoleOptions opts]
    :  K_PASSWORD v=STRING_LITERAL { opts.setOption(IRoleManager.Option.PASSWORD, $v.text); }
    |  K_HASHED K_PASSWORD v=STRING_LITERAL { opts.setOption(IRoleManager.Option.HASHED_PASSWORD, $v.text); }
    |  K_GENERATED K_PASSWORD { opts.setOption(IRoleManager.Option.GENERATED_PASSWORD, Boolean.TRUE); }
    ;

/**
 * DESCRIBE statement(s)
 *
 * Must be in sync with the javadoc for org.apache.cassandra.cql3.statements.DescribeStatement and the
 * cqlsh syntax definition in for cqlsh_describe_cmd_syntax_rules pylib/cqlshlib/cqlshhandling.py.
 */
describeStatement returns [DescribeStatement stmt]
    @init {
        boolean fullSchema = false;
        boolean pending = false;
        boolean config = false;
        boolean only = false;
        QualifiedName gen = new QualifiedName();
    }
    : ( K_DESCRIBE | K_DESC )
    ( K_CLUSTER                                   { $stmt = DescribeStatement.cluster(); }
    | (K_FULL { fullSchema=true; })? K_SCHEMA     { $stmt = DescribeStatement.schema(fullSchema); }
    | K_KEYSPACES                                 { $stmt = DescribeStatement.keyspaces(); }
    | (K_ONLY { only=true; })? K_KEYSPACE ( ks=keyspaceName )?
                                                  { $stmt = DescribeStatement.keyspace($ks.ctx == null ? null : $ks.id, only); }
    | K_TABLES                                    { $stmt = DescribeStatement.tables(); }
    | K_COLUMNFAMILY cf=columnFamilyName          { $stmt = DescribeStatement.table($cf.name.getKeyspace(), $cf.name.getName()); }
    | K_INDEX idx=columnFamilyName                { $stmt = DescribeStatement.index($idx.name.getKeyspace(), $idx.name.getName()); }
    | K_MATERIALIZED K_VIEW view=columnFamilyName { $stmt = DescribeStatement.view($view.name.getKeyspace(), $view.name.getName()); }
    | K_TYPES                                     { $stmt = DescribeStatement.types(); }
    | K_TYPE tn=userTypeName                      { $stmt = DescribeStatement.type($tn.name.getKeyspace(), $tn.name.getStringTypeName()); }
    | K_FUNCTIONS                                 { $stmt = DescribeStatement.functions(); }
    | K_FUNCTION fn=functionName                  { $stmt = DescribeStatement.function($fn.s.keyspace, $fn.s.name); }
    | K_AGGREGATES                                { $stmt = DescribeStatement.aggregates(); }
    | K_AGGREGATE ag=functionName                 { $stmt = DescribeStatement.aggregate($ag.s.keyspace, $ag.s.name); }
    | ( ( ksT=IDENT                       { gen.setKeyspace($ksT.text, false);}
          | ksT=QUOTED_NAME                 { gen.setKeyspace($ksT.text, true);}
          | ksK=unreserved_keyword          { gen.setKeyspace($ksK.str, false);} ) '.' )?
        ( tT=IDENT                          { gen.setName($tT.text, false);}
        | tT=QUOTED_NAME                    { gen.setName($tT.text, true);}
        | tK=unreserved_keyword             { gen.setName($tK.str, false);} )
                                                    { $stmt = DescribeStatement.generic(gen.getKeyspace(), gen.getName()); }
    )
    ( K_WITH K_INTERNALS { $stmt.withInternalDetails(); } )?
    ;

/** DEFINITIONS **/

// Like ident, but for case where we take a column name that can be the legacy super column empty name. Importantly,
// this should not be used in DDL statements, as we don't want to let users create such column.
cident returns [ColumnIdentifier id]
    : EMPTY_QUOTED_NAME    { $id = ColumnIdentifier.getInterned("", true); }
    | t=ident              { $id = $t.id; }
    ;

ident returns [ColumnIdentifier id]
    : t=IDENT              { $id = ColumnIdentifier.getInterned($t.text, false); }
    | t=QUOTED_NAME        { $id = ColumnIdentifier.getInterned($t.text, true); }
    | k=unreserved_keyword { $id = ColumnIdentifier.getInterned($k.str, false); }
    ;

fident returns [FieldIdentifier id]
    : t=IDENT              { $id = FieldIdentifier.forUnquoted($t.text); }
    | t=QUOTED_NAME        { $id = FieldIdentifier.forQuoted($t.text); }
    | k=unreserved_keyword { $id = FieldIdentifier.forUnquoted($k.str); }
    ;

// Identifiers that do not refer to columns
noncol_ident returns [ColumnIdentifier id]
    : t=IDENT              { $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME        { $id = new ColumnIdentifier($t.text, true); }
    | k=unreserved_keyword { $id = new ColumnIdentifier($k.str, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [String id]
    @init { QualifiedName name = new QualifiedName(); }
    : ksName[name] { $id = name.getKeyspace(); }
    ;

indexName returns [QualifiedName name]
    @init { $name = new QualifiedName(); }
    : (ksName[$name] '.')? idxName[$name]
    ;

indexNames returns [Set<QualifiedName> names]
    @init { $names = new HashSet<QualifiedName>(); }
    : '{' ( t1=indexName { $names.add($t1.name); } ( ',' tn=indexName { $names.add($tn.name); } )* )? '}'
    ;

columnFamilyName returns [QualifiedName name]
    @init { $name = new QualifiedName(); }
    : (ksName[$name] '.')? cfName[$name]
    ;

columnReference returns [QualifiedName table, ColumnIdentifier column]
    @init { $table = new QualifiedName(); }
    : cfName[$table] '.' col=cident
      { $column = $col.id; }
    | ksName[$table] '.' cfName[$table] '.' col=cident
      { $column = $col.id; }
    ;

typeFieldReference returns [UTName typeName, FieldIdentifier field]
    : ut=non_type_ident '.' fld=fident
      { $typeName = new UTName(null, $ut.id); $field = $fld.id; }
    | ks=noncol_ident '.' ut=non_type_ident '.' fld=fident
      { $typeName = new UTName($ks.id, $ut.id); $field = $fld.id; }
    ;


userTypeName returns [UTName name]
    : (ks=noncol_ident '.')? ut=non_type_ident { $name = new UTName($ks.ctx == null ? null : $ks.id, $ut.id); }
    ;

userOrRoleName returns [RoleName name]
    @init { RoleName role = new RoleName(); }
    : roleName[role] {$name = role;}
    ;

ksName[QualifiedName name]
    : t=IDENT              { $name.setKeyspace($t.text, false);}
    | t=QUOTED_NAME        { $name.setKeyspace($t.text, true);}
    | k=unreserved_keyword { $name.setKeyspace($k.str, false);}
    | QMARK {addRecognitionError("Bind variables cannot be used for keyspace names");}
    ;

cfName[QualifiedName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | t=QUOTED_NAME        { $name.setName($t.text, true); }
    | k=unreserved_keyword { $name.setName($k.str, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for table names");}
    ;

idxName[QualifiedName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | t=QUOTED_NAME        { $name.setName($t.text, true);}
    | k=unreserved_keyword { $name.setName($k.str, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for index names");}
    ;

roleName[RoleName name]
    : t=IDENT              { $name.setName($t.text, false); }
    | s=STRING_LITERAL     { $name.setName($s.text, true); }
    | t=QUOTED_NAME        { $name.setName($t.text, true); }
    | k=unreserved_keyword { $name.setName($k.str, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for role names");}
    ;

constant returns [Constants.Literal literal]
    : t=STRING_LITERAL { $literal = Constants.Literal.string($t.text); }
    | t=INTEGER        { $literal = Constants.Literal.integer($t.text); }
    | t=FLOAT          { $literal = Constants.Literal.floatingPoint($t.text); }
    | t=BOOLEAN        { $literal = Constants.Literal.bool($t.text); }
    | t=DURATION       { $literal = Constants.Literal.duration($t.text);}
    | t=UUID           { $literal = Constants.Literal.uuid($t.text); }
    | t=HEXNUMBER      { $literal = Constants.Literal.hex($t.text); }
    | ((K_POSITIVE_NAN | K_NEGATIVE_NAN) { $literal = Constants.Literal.floatingPoint("NaN"); }
        | K_POSITIVE_INFINITY  { $literal = Constants.Literal.floatingPoint("Infinity"); }
        | K_NEGATIVE_INFINITY { $literal = Constants.Literal.floatingPoint("-Infinity"); })
    ;

fullMapLiteral returns [Maps.Literal map]
    @init { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>();}
    @after{ $map = new Maps.Literal(m); }
    : '{' ( k1=term ':' v1=term { m.add(Pair.create($k1.raw, $v1.raw)); } ( ',' kn=term ':' vn=term { m.add(Pair.create($kn.raw, $vn.raw)); } )* )?
      '}'
    ;

setOrMapLiteral[Term.Raw t] returns [Term.Raw raw]
    : m=mapLiteral[t] { $raw=$m.raw; }
    | s=setLiteral[t] { $raw=$s.raw; }
    ;

setLiteral[Term.Raw t] returns [Term.Raw raw]
    @init { List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); }
    @after { $raw = new Sets.Literal(s); }
    : ( ',' tn=term { s.add($tn.raw); } )*
    ;

mapLiteral[Term.Raw k] returns [Term.Raw raw]
    @init { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); }
    @after { $raw = new Maps.Literal(m); }
    : ':' v=term {  m.add(Pair.create(k, $v.raw)); } ( ',' kn=term ':' vn=term { m.add(Pair.create($kn.raw, $vn.raw)); } )*
    ;

collectionLiteral returns [Term.Raw raw]
    : l=listLiteral { $raw = $l.raw; }
    | '{' t=term v=setOrMapLiteral[$t.raw] { $raw = $v.raw; } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $raw = new Sets.Literal(Collections.<Term.Raw>emptyList()); }
    ;

listLiteral returns [Term.Raw raw]
    @init {List<Term.Raw> l = new ArrayList<Term.Raw>();}
    @after {$raw = new ArrayLiteral(l);}
    : '[' ( t1=term { l.add($t1.raw); } ( ',' tn=term { l.add($tn.raw); } )* )? ']' { $raw = new ArrayLiteral(l); }
    ;

usertypeLiteral returns [UserTypes.Literal ut]
    @init{ Map<FieldIdentifier, Term.Raw> m = new HashMap<>(); }
    @after{ $ut = new UserTypes.Literal(m); }
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' k1=fident ':' v1=term { m.put($k1.id, $v1.raw); } ( ',' kn=fident ':' vn=term { m.put($kn.id, $vn.raw); } )* '}'
    ;

tupleLiteral returns [Tuples.Literal tt]
    @init{ List<Term.Raw> l = new ArrayList<Term.Raw>(); }
    @after{ $tt = new Tuples.Literal(l); }
    : '(' t1=term { l.add($t1.raw); } ( ',' tn=term { l.add($tn.raw); } )* ')'
    ;

value returns [Term.Raw raw]
    : c=constant           { $raw = $c.literal; }
    | l=collectionLiteral  { $raw = $l.raw; }
    | u=usertypeLiteral    { $raw = $u.ut; }
    | t=tupleLiteral       { $raw = $t.tt; }
    | K_NULL               { $raw = Constants.NULL_LITERAL; }
    | m=marker             { $raw = $m.raw; }
    ;

intValue returns [Term.Raw raw]
    : t=INTEGER { $raw = Constants.Literal.integer($t.text); }
    | m=marker  { $raw = $m.raw; }
    ;

functionName returns [FunctionName s]
     // antlr might try to recover and give a null for f. It will still error out in the end, but FunctionName
     // wouldn't be happy with that so we should bypass this for now or we'll have a weird user-facing error
    : (ks=keyspaceName '.')? f=allowedFunctionName   { $s = $f.s == null ? null : new FunctionName($ks.ctx == null ? null : $ks.id, $f.s); }
    ;

allowedFunctionName returns [String s]
    : f=IDENT                       { $s = LocalizeString.toLowerCaseLocalized($f.text); }
    | f=QUOTED_NAME                 { $s = $f.text; }
    | u=unreserved_function_keyword { $s = $u.str; }
    | K_TOKEN                       { $s = "token"; }
    | K_COUNT                       { $s = "count"; }
    ;

function returns [Term.Raw t]
    : f=functionName '(' ')'                   { $t = new FunctionCall.Raw($f.s, Collections.<Term.Raw>emptyList()); }
    | f=functionName '(' args=functionArgs ')' { $t = new FunctionCall.Raw($f.s, $args.args); }
    ;

functionArgs returns [List<Term.Raw> args]
    @init{ $args = new ArrayList<Term.Raw>(); }
    : t1=term {$args.add($t1.raw); } ( ',' tn=term { $args.add($tn.raw); } )*
    ;

term returns [Term.Raw raw]
    : t=termAddition                          { $raw = $t.raw; }
    ;

termAddition returns [Term.Raw raw]
    :   l=termMultiplication   {$raw = $l.raw;}
        ( '+' r=termMultiplication {$raw = FunctionCall.Raw.newOperation('+', $raw, $r.raw);}
        | '-' r=termMultiplication {$raw = FunctionCall.Raw.newOperation('-', $raw, $r.raw);}
        )*
    ;

termMultiplication returns [Term.Raw raw]
    :   l=termGroup   {$raw = $l.raw;}
        ( '*' r=termGroup {$raw = FunctionCall.Raw.newOperation('*', $raw, $r.raw);}
        | '/' r=termGroup {$raw = FunctionCall.Raw.newOperation('/', $raw, $r.raw);}
        | '%' r=termGroup {$raw = FunctionCall.Raw.newOperation('%', $raw, $r.raw);}
        )*
    ;

termGroup returns [Term.Raw raw]
    : t=simpleTerm              { $raw = $t.raw; }
    | '-'  t=simpleTerm         { $raw = FunctionCall.Raw.newNegation($t.raw); }
    ;

simpleTerm returns [Term.Raw raw]
    : v=value                                        { $raw = $v.raw; }
    | f=function                                     { $raw = $f.t; }
    | '(' c=comparatorType ')' t=simpleTerm          { $raw = new TypeCast($c.t, $t.raw); }
    | K_CAST '(' t=simpleTerm K_AS n=native_type ')' { $raw = FunctionCall.Raw.newCast($t.raw, $n.t); }
    ;

columnOperation[UpdateStatement.OperationCollector operations]
    : key=cident columnOperationDifferentiator[operations, $key.id]
    ;

columnOperationDifferentiator[UpdateStatement.OperationCollector operations, ColumnIdentifier key]
    : '=' normalColumnOperation[operations, key]
    | shorthandColumnOperation[operations, key]
    | '[' k=term ']' collectionColumnOperation[operations, key, $k.raw]
    | '.' field=fident udtColumnOperation[operations, key, $field.id]
    ;

normalColumnOperation[UpdateStatement.OperationCollector operations, ColumnIdentifier key]
    : t=term ('+' c=cident )?
      {
          if ($c.ctx == null)
          {
              addRawUpdate(operations, key, new Operation.SetValue($t.raw));
          }
          else
          {
              if (!key.equals($c.id))
                  addRecognitionError("Only expressions of the form X = <value> + X are supported.");
              addRawUpdate(operations, key, new Operation.Prepend($t.raw));
          }
      }
    | c=cident sig=('+' | '-') t=term
      {
          if (!key.equals($c.id))
              addRecognitionError("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          addRawUpdate(operations, key, $sig.text.equals("+") ? new Operation.Addition($t.raw) : new Operation.Substraction($t.raw));
      }
    | c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (!key.equals($c.id))
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              addRecognitionError("Only expressions of the form X = X " + ($i.text.charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
          addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer($i.text)));
      }
     | {isParsingTxn}? r=rowDataReference
       {
           addRawReferenceOperation(operations, key, new ReferenceOperation.Raw(new Operation.SetValue($r.rawRef), key, new ReferenceValue.Substitution.Raw($r.rawRef)));
       }
    ;

shorthandColumnOperation[UpdateStatement.OperationCollector operations, ColumnIdentifier key]
    : sig=('+=' | '-=')
      (
          t=term
          {
              addRawUpdate(operations, key, $sig.text.equals("+=") ? new Operation.Addition($t.raw) : new Operation.Substraction($t.raw));
          }
          | {isParsingTxn}? dr=rowDataReference
            {
                ReferenceValue.Raw right = new ReferenceValue.Substitution.Raw($dr.rawRef);
                Operation.RawUpdate operation = $sig.text.equals("+=") ? new Operation.Addition($dr.rawRef) : new Operation.Substraction($dr.rawRef);
                addRawReferenceOperation(operations, key, new ReferenceOperation.Raw(operation, key, right));
            }
      )
    ;

collectionColumnOperation[UpdateStatement.OperationCollector operations, ColumnIdentifier key, Term.Raw k]
    : '='
      (
          t=term
          {
              addRawUpdate(operations, key, new Operation.SetElement(k, $t.raw));
          }
          | {isParsingTxn}? dr=rowDataReference
            {
                ReferenceValue.Raw right = new ReferenceValue.Substitution.Raw($dr.rawRef);
                addRawReferenceOperation(operations, key, new ReferenceOperation.Raw(new Operation.SetElement(k, $dr.rawRef), key, right));
            }
      )
    ;

udtColumnOperation[UpdateStatement.OperationCollector operations, ColumnIdentifier key, FieldIdentifier field]
    : '='
      (
          t=term
          {
              addRawUpdate(operations, key, new Operation.SetField(field, $t.raw));
          }
          | {isParsingTxn}? dr=rowDataReference
            {
                ReferenceValue.Raw right = new ReferenceValue.Substitution.Raw($dr.rawRef);
                addRawReferenceOperation(operations, key, new ReferenceOperation.Raw(new Operation.SetField(field, $dr.rawRef), key, right));
            }
      )
    ;

columnCondition returns [ColumnCondition.Raw condition]
    // Note: we'll reject duplicates later
    : column=cident
        ( op=relationType t=term       { $condition = ColumnCondition.Raw.simpleCondition($column.id, $op.op, Terms.Raw.of($t.raw)); }
        | K_CONTAINS (K_KEY)? t=term   { $condition = ColumnCondition.Raw.simpleCondition($column.id, $K_KEY != null ? Operator.CONTAINS_KEY : Operator.CONTAINS, Terms.Raw.of($t.raw)); }
        | K_IN v=singleColumnInValues  { $condition = ColumnCondition.Raw.simpleCondition($column.id, Operator.IN, $v.raws); }
        | '[' element=term ']'
            ( op=relationType t=term      { $condition = ColumnCondition.Raw.collectionElementCondition($column.id, $element.raw, $op.op, Terms.Raw.of($t.raw)); }
            | K_IN v=singleColumnInValues { $condition = ColumnCondition.Raw.collectionElementCondition($column.id, $element.raw, Operator.IN, $v.raws); }
            )
        | '.' field=fident
            ( op=relationType t=term      { $condition = ColumnCondition.Raw.udtFieldCondition($column.id, $field.id, $op.op, Terms.Raw.of($t.raw)); }
            | K_IN v=singleColumnInValues { $condition = ColumnCondition.Raw.udtFieldCondition($column.id, $field.id, Operator.IN, $v.raws); }
            )
        )
    ;

properties[PropertyDefinitions props]
    : property[props] (K_AND property[props])*
    ;

indexProperty returns [String s]
    : 'included_indexes' { $s = "included_indexes"; }
    | 'excluded_indexes' { $s = "excluded_indexes"; }
    ;

property[PropertyDefinitions props]
    : k=noncol_ident '=' simple=propertyValue { try { $props.addProperty($k.id.toString(), $simple.str); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
    | k=noncol_ident '=' map=fullMapLiteral { try { $props.addProperty($k.id.toString(), convertPropertyMap($map.map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
    | s=indexProperty '=' names=indexNames { try { $props.addProperty($s.s, $names.names); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
    ;

propertyValue returns [String str]
    : c=constant           { $str = $c.literal.getRawText(); }
    | u=unreserved_keyword { $str = $u.str; }
    ;

singleColumnBetweenValues returns [Terms.Raw raws]
    @init { List<Term.Raw> list = new ArrayList<>(); }
    @after { $raws = Terms.Raw.of(list); }
    : t1=term { list.add($t1.raw); } K_AND t2=term { list.add($t2.raw); }
    ;

relationType returns [Operator op]
    : '='  { $op = Operator.EQ; }
    | '<'  { $op = Operator.LT; }
    | '<=' { $op = Operator.LTE; }
    | '>'  { $op = Operator.GT; }
    | '>=' { $op = Operator.GTE; }
    | '!=' { $op = Operator.NEQ; }
    ;

relation[WhereClause.Builder clauses]
    : name=cident
           ( type=relationType t=term { $clauses.add(Relation.singleColumn($name.id, $type.op, $t.raw)); }
           | K_BETWEEN betweenValues=singleColumnBetweenValues { $clauses.add(Relation.singleColumn($name.id, Operator.BETWEEN, $betweenValues.raws)); }
           | K_LIKE t=term { $clauses.add(Relation.singleColumn($name.id, Operator.LIKE, $t.raw)); }
           | K_IS K_NOT K_NULL { $clauses.add(Relation.singleColumn($name.id, Operator.IS_NOT, Constants.NULL_LITERAL)); }
           | K_IN '(' { Token subqueryMarker = statementBeginMarker; } inSub=selectStatement ')'
                 {
                     // The nested selectStatement runs stmtBegins()/stmtSrc(), which clobber the shared
                     // statementBeginMarker.  Restore the outer statement's marker so its source tracking
                     // stays correct.  Research POC (uncorrelated IN-subquery on the partition key).
                     statementBeginMarker = subqueryMarker;
                     $clauses.add(Relation.singleColumnSubquery($name.id, $inSub.expr));
                 }
           | rtInOperator=inOperator inValue=singleColumnInValues { $clauses.add(Relation.singleColumn($name.id, $rtInOperator.o, $inValue.raws)); }
           | rtContainsOperator=containsOperator t=term { $clauses.add(Relation.singleColumn($name.id, $rtContainsOperator.o, $t.raw)); }
           )
    | K_TOKEN l=tupleOfIdentifiers
        ( type=relationType t=term { $clauses.add(Relation.token($l.ids, $type.op, $t.raw)); }
        | K_BETWEEN betweenValues=singleColumnBetweenValues { $clauses.add(Relation.token($l.ids, Operator.BETWEEN, $betweenValues.raws)); }
        )
    | name=cident '[' key=term ']' type=relationType t=term { $clauses.add(Relation.mapElement($name.id, $key.raw, $type.op, $t.raw)); }
    | ids=tupleOfIdentifiers
        ( rt=inOperator mcInValue=multiColumnInValues { $clauses.add(Relation.multiColumn($ids.ids, $rt.o, $mcInValue.raws)); }
        | type=relationType v=multiColumnValue {$clauses.add(Relation.multiColumn($ids.ids, $type.op, $v.raw)); }
        | K_BETWEEN t1=multiColumnValue K_AND t2=multiColumnValue { $clauses.add(Relation.multiColumn($ids.ids, Operator.BETWEEN, Terms.Raw.of(List.of($t1.raw, $t2.raw)))); }
        )
    | '(' relation[$clauses] ')'
    ;

containsOperator returns [Operator o]
    : K_CONTAINS { $o = Operator.CONTAINS; } (K_KEY { $o = Operator.CONTAINS_KEY; })?
    | K_NOT K_CONTAINS { $o = Operator.NOT_CONTAINS; } (K_KEY { $o = Operator.NOT_CONTAINS_KEY; })?
    ;

inOperator returns [Operator o]
    : K_IN { $o = Operator.IN; }
    | K_NOT K_IN { $o = Operator.NOT_IN; }
    ;

inMarker returns [Terms.Raw raws]
    : QMARK { $raws = newINBindVariables(null); }
    | ':' name=noncol_ident { $raws = newINBindVariables($name.id); }
    ;

tupleOfIdentifiers returns [List<ColumnIdentifier> ids]
    @init { $ids = new ArrayList<ColumnIdentifier>(); }
    : '(' n1=cident { $ids.add($n1.id); } (',' ni=cident { $ids.add($ni.id); })* ')'
    ;

singleColumnInValues returns [Terms.Raw raws]
    : t=terms     { $raws = $t.raws;}
    | m=inMarker  { $raws = $m.raws;}
    ;

terms returns [Terms.Raw raws]
    @init { List<Term.Raw> list = new ArrayList<>(); }
    @after { $raws = Terms.Raw.of(list); }
    : '(' ( t1 = term { list.add($t1.raw); } (',' ti=term { list.add($ti.raw); })* )? ')'
    ;

multiColumnValue returns [Term.Raw raw]
    : l=tupleLiteral { $raw = $l.tt; } /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
    | m=marker       { $raw = $m.raw; } /* (a, b, c) >= ? */
    ;

multiColumnInValues returns [Terms.Raw raws]
    : '(' ')'                    { $raws = Terms.Raw.of();}  /* (a, b, c) IN () */
    | m=inMarker                 { $raws = $m.raws; }              /* (a, b, c) IN ? */
    | tl=tupleOfTupleLiterals    { $raws = $tl.literals; }             /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
    | tm=tupleOfMarkersForTuples { $raws = $tm.markers; }             /* (a, b, c) IN (?, ?, ...) */
    ;

tupleOfTupleLiterals returns [Terms.Raw literals]
    @init { List<Term.Raw> list = new ArrayList<>(); }
    @after { $literals = Terms.Raw.of(list); }
    : '(' t1=tupleLiteral { list.add($t1.tt); } (',' ti=tupleLiteral { list.add($ti.tt); })* ')'
    ;

tupleOfMarkersForTuples returns [Terms.Raw markers]
    @init { List<Term.Raw> list = new ArrayList<>(); }
    @after { $markers = Terms.Raw.of(list); }
    : '(' m1=marker { list.add($m1.raw); } (',' mi=marker { list.add($mi.raw); })* ')'
    ;

comparatorType returns [CQL3Type.Raw t]
    : n=native_type     { $t = CQL3Type.Raw.from($n.t); }
    | c=collection_type { $t = $c.pt; }
    | tt=tuple_type     { $t = $tt.t; }
    | vc=vector_type    { $t = $vc.vt; }
    | id=userTypeName   { $t = CQL3Type.Raw.userType($id.name); }
    | K_FROZEN '<' f=comparatorType '>'
      {
        try {
            $t = $f.t.freeze();
        } catch (InvalidRequestException e) {
            addRecognitionError(e.getMessage());
        }
      }
    | s=STRING_LITERAL
      {
        try {
            $t = CQL3Type.Raw.from(new CQL3Type.Custom($s.text));
        } catch (SyntaxException e) {
            addRecognitionError("Cannot parse type " + $s.text + ": " + e.getMessage());
        } catch (ConfigurationException e) {
            addRecognitionError("Error setting type " + $s.text + ": " + e.getMessage());
        }
      }
    ;

native_type returns [CQL3Type t]
    : K_ASCII     { $t = CQL3Type.Native.ASCII; }
    | K_BIGINT    { $t = CQL3Type.Native.BIGINT; }
    | K_BLOB      { $t = CQL3Type.Native.BLOB; }
    | K_BOOLEAN   { $t = CQL3Type.Native.BOOLEAN; }
    | K_COUNTER   { $t = CQL3Type.Native.COUNTER; }
    | K_DECIMAL   { $t = CQL3Type.Native.DECIMAL; }
    | K_DOUBLE    { $t = CQL3Type.Native.DOUBLE; }
    | K_DURATION  { $t = CQL3Type.Native.DURATION; }
    | K_FLOAT     { $t = CQL3Type.Native.FLOAT; }
    | K_INET      { $t = CQL3Type.Native.INET;}
    | K_INT       { $t = CQL3Type.Native.INT; }
    | K_SMALLINT  { $t = CQL3Type.Native.SMALLINT; }
    | K_TEXT      { $t = CQL3Type.Native.TEXT; }
    | K_TIMESTAMP { $t = CQL3Type.Native.TIMESTAMP; }
    | K_TINYINT   { $t = CQL3Type.Native.TINYINT; }
    | K_UUID      { $t = CQL3Type.Native.UUID; }
    | K_VARCHAR   { $t = CQL3Type.Native.VARCHAR; }
    | K_VARINT    { $t = CQL3Type.Native.VARINT; }
    | K_TIMEUUID  { $t = CQL3Type.Native.TIMEUUID; }
    | K_DATE      { $t = CQL3Type.Native.DATE; }
    | K_TIME      { $t = CQL3Type.Native.TIME; }
    ;

collection_type returns [CQL3Type.Raw pt]
    : K_MAP  '<' t1=comparatorType ',' t2=comparatorType '>'
        {
            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
            if ($t1.t != null && $t2.t != null)
                $pt = CQL3Type.Raw.map($t1.t, $t2.t);
        }
    | K_LIST '<' t=comparatorType '>'
        { if ($t.t != null) $pt = CQL3Type.Raw.list($t.t); }
    | K_SET  '<' t=comparatorType '>'
        { if ($t.t != null) $pt = CQL3Type.Raw.set($t.t); }
    ;

tuple_type returns [CQL3Type.Raw t]
    @init {List<CQL3Type.Raw> types = new ArrayList<>();}
    @after {$t = CQL3Type.Raw.tuple(types);}
    : K_TUPLE '<' t1=comparatorType { types.add($t1.t); } (',' tn=comparatorType { types.add($tn.t); })* '>'
    ;

vector_type returns [CQL3Type.Raw vt]
    : K_VECTOR '<' t1=comparatorType ','  d=INTEGER '>'
        { $vt = CQL3Type.Raw.vector($t1.t, Integer.parseInt($d.text)); }
    ;

username
    : IDENT
    | STRING_LITERAL
    | unreserved_keyword
    | QUOTED_NAME { addRecognitionError("Quoted strings are are not supported for user names and USER is deprecated, please use ROLE");}
    ;

identity
    : IDENT
    | STRING_LITERAL
    | unreserved_keyword
    | QUOTED_NAME { addRecognitionError("Quoted strings are are not supported for identity");}
    ;

mbean
    : STRING_LITERAL
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident returns [ColumnIdentifier id]
    : t=IDENT                    { if (reservedTypeNames.contains($t.text)) addRecognitionError("Invalid (reserved) user type name " + $t.text); $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME              { $id = new ColumnIdentifier($t.text, true); }
    | k=basic_unreserved_keyword { $id = new ColumnIdentifier($k.str, false); }
    | kk=K_KEY                   { $id = new ColumnIdentifier($kk.text, false); }
    ;

unreserved_keyword returns [String str]
    : u=unreserved_function_keyword     { $str = $u.str; }
    | k=(K_TTL | K_COUNT | K_WRITETIME | K_MAXWRITETIME | K_KEY | K_CAST | K_JSON | K_DISTINCT) { $str = $k.text; }
    ;

unreserved_function_keyword returns [String str]
    : u=basic_unreserved_keyword { $str = $u.str; }
    | t=native_type              { $str = $t.t.toString(); }
    ;

basic_unreserved_keyword returns [String str]
    : k=( K_KEYS
        | K_AS
        | K_CLUSTER
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TABLES
        | K_TYPE
        | K_TYPES
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_PERMISSION
        | K_PERMISSIONS
        | K_KEYSPACES
        | K_ALL
        | K_USER
        | K_USERS
        | K_ROLE
        | K_ROLES
        | K_IDENTITY
        | K_SUPERUSER
        | K_SUPERUSERS
        | K_NOSUPERUSER
        | K_LOGIN
        | K_NOLOGIN
        | K_OPTIONS
        | K_PASSWORD
        | K_GENERATED
        | K_HASHED
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_CONTAINS
        | K_INTERNALS
        | K_ONLY
        | K_STATIC
        | K_FROZEN
        | K_FOR
        | K_TUPLE
        | K_FUNCTION
        | K_FUNCTIONS
        | K_AGGREGATE
        | K_AGGREGATES
        | K_SFUNC
        | K_STYPE
        | K_FINALFUNC
        | K_INITCOND
        | K_RETURNS
        | K_LANGUAGE
        | K_CALLED
        | K_INPUT
        | K_LIKE
        | K_PER
        | K_PARTITION
        | K_GROUP
        | K_HAVING
        | K_DATACENTERS
        | K_CIDRS
        | K_ACCESS
        | K_DEFAULT
        | K_MBEAN
        | K_MBEANS
        | K_REPLACE
        | K_UNSET
        | K_MASKED
        | K_UNMASK
        | K_SELECT_MASKED
        | K_VECTOR
        | K_ANN
        | K_BETWEEN
        | K_CHECK
        | K_INDEXES
        | K_COMMIT
        | K_END
        | K_LET
        | K_THEN
        | K_CASE
        | K_WHEN
        | K_ELSE
        | K_OVER
        | K_ROW_NUMBER
        | K_TRANSACTION
        | K_COMMENT
        | K_COMMENTS
        | K_SECURITY
        | K_LABEL
        | K_LABELS
        | K_FIELD
        | K_COLUMN
        | K_JOIN
        ) { $str = $k.text; }
    ;
