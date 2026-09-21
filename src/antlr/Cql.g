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

grammar Cql;

options {
    language = Java;
}

import Parser,Lexer;

tokens {
    K_SELECT,
    K_LET,
    K_FROM,
    K_AS,
    K_WHERE,
    K_AND,
    K_KEY,
    K_KEYS,
    K_ENTRIES,
    K_FULL,
    K_INSERT,
    K_UPDATE,
    K_WITH,
    K_LIMIT,
    K_PER,
    K_PARTITION,
    K_USING,
    K_USE,
    K_DISTINCT,
    K_COUNT,
    K_SET,
    K_BEGIN,
    K_UNLOGGED,
    K_BATCH,
    K_APPLY,
    K_COMMIT,
    K_TRUNCATE,
    K_DELETE,
    K_TRANSACTION,
    K_IN,
    K_CREATE,
    K_SCHEMA,
    K_KEYSPACE,
    K_KEYSPACES,
    K_COLUMNFAMILY,
    K_COLUMN,
    K_TABLES,
    K_MATERIALIZED,
    K_VIEW,
    K_INDEX,
    K_INDEXES,
    K_CUSTOM,
    K_ON,
    K_TO,
    K_DROP,
    K_PRIMARY,
    K_INTO,
    K_VALUES,
    K_TIMESTAMP,
    K_TTL,
    K_CAST,
    K_ALTER,
    K_RENAME,
    K_ADD,
    K_TYPE,
    K_TYPES,
    K_COMPACT,
    K_STORAGE,
    K_ORDER,
    K_BY,
    K_ASC,
    K_DESC,
    K_ALLOW,
    K_FILTERING,
    K_IF,
    K_THEN,
    K_END,
    K_IS,
    K_CONTAINS,
    K_BETWEEN,
    K_GROUP,
    K_CLUSTER,
    K_INTERNALS,
    K_ONLY,
    K_CHECK,
    K_GRANT,
    K_ALL,
    K_PERMISSION,
    K_PERMISSIONS,
    K_OF,
    K_REVOKE,
    K_MODIFY,
    K_AUTHORIZE,
    K_DESCRIBE,
    K_EXECUTE,
    K_NORECURSIVE,
    K_MBEAN,
    K_MBEANS,
    K_USER,
    K_USERS,
    K_ROLE,
    K_ROLES,
    K_SUPERUSERS,
    K_SUPERUSER,
    K_NOSUPERUSER,
    K_GENERATED,
    K_PASSWORD,
    K_HASHED,
    K_LOGIN,
    K_NOLOGIN,
    K_OPTIONS,
    K_ACCESS,
    K_DATACENTERS,
    K_CIDRS,
    K_IDENTITY,
    K_CLUSTERING,
    K_ASCII,
    K_BIGINT,
    K_BLOB,
    K_BOOLEAN,
    K_COUNTER,
    K_DECIMAL,
    K_DOUBLE,
    K_DURATION,
    K_FLOAT,
    K_INET,
    K_INT,
    K_SMALLINT,
    K_TINYINT,
    K_TEXT,
    K_UUID,
    K_VARCHAR,
    K_VARINT,
    K_TIMEUUID,
    K_TOKEN,
    K_WRITETIME,
    K_MAXWRITETIME,
    K_DATE,
    K_TIME,
    K_NULL,
    K_NOT,
    K_EXISTS,
    K_MAP,
    K_LIST,
    K_POSITIVE_NAN,
    K_POSITIVE_INFINITY,
    K_TUPLE,
    K_TRIGGER,
    K_STATIC,
    K_FROZEN,
    K_FOR,
    K_FIELD,
    K_FUNCTION,
    K_FUNCTIONS,
    K_AGGREGATE,
    K_AGGREGATES,
    K_SFUNC,
    K_STYPE,
    K_FINALFUNC,
    K_INITCOND,
    K_RETURNS,
    K_CALLED,
    K_INPUT,
    K_LANGUAGE,
    K_OR,
    K_REPLACE,
    K_JSON,
    K_DEFAULT,
    K_UNSET,
    K_LIKE,
    K_MASKED,
    K_UNMASK,
    K_SELECT_MASKED,
    K_VECTOR,
    K_ANN,
    K_COMMENT,
    K_COMMENTS,
    K_SECURITY,
    K_LABEL,
    K_LABELS,
    // Token types whose lexer rules were removed by the lexer-ATN restructuring.  The rules were
    // merged into fewer, broader rules to shrink the lexer config-set churn; nextToken() below
    // reclassifies the merged tokens back to these types so the parser is unchanged.
    BOOLEAN,
    INTEGER,
    FLOAT,
    DURATION,
    QUOTED_NAME,
    EMPTY_QUOTED_NAME
}

@lexer::members {
    // Keyword classification.  The lexer no longer defines a rule per keyword; instead it lexes
    // an IDENT and this map re-labels it with the matching K_* token type.  This removes ~170
    // keyword rules from the lexer ATN so identifier lexing does not weigh every keyword at each
    // character.  Case folding matches the old case-insensitive keyword rules.  Quoted names and
    // the '-' prefixed NaN/Infinity keywords keep their own rules and are never re-labeled.
    private static final java.util.Map<String, Integer> KEYWORDS = buildKeywords();

    private static java.util.Map<String, Integer> buildKeywords() {
        java.util.Map<String, Integer> m = new java.util.HashMap<>(512);
        m.put("access", CqlParser.K_ACCESS);
        m.put("add", CqlParser.K_ADD);
        m.put("aggregate", CqlParser.K_AGGREGATE);
        m.put("aggregates", CqlParser.K_AGGREGATES);
        m.put("all", CqlParser.K_ALL);
        m.put("allow", CqlParser.K_ALLOW);
        m.put("alter", CqlParser.K_ALTER);
        m.put("and", CqlParser.K_AND);
        m.put("ann", CqlParser.K_ANN);
        m.put("apply", CqlParser.K_APPLY);
        m.put("as", CqlParser.K_AS);
        m.put("asc", CqlParser.K_ASC);
        m.put("ascii", CqlParser.K_ASCII);
        m.put("authorize", CqlParser.K_AUTHORIZE);
        m.put("batch", CqlParser.K_BATCH);
        m.put("begin", CqlParser.K_BEGIN);
        m.put("between", CqlParser.K_BETWEEN);
        m.put("bigint", CqlParser.K_BIGINT);
        m.put("blob", CqlParser.K_BLOB);
        m.put("boolean", CqlParser.K_BOOLEAN);
        m.put("by", CqlParser.K_BY);
        m.put("called", CqlParser.K_CALLED);
        m.put("cast", CqlParser.K_CAST);
        m.put("check", CqlParser.K_CHECK);
        m.put("cidrs", CqlParser.K_CIDRS);
        m.put("cluster", CqlParser.K_CLUSTER);
        m.put("clustering", CqlParser.K_CLUSTERING);
        m.put("column", CqlParser.K_COLUMN);
        m.put("columnfamilies", CqlParser.K_TABLES);
        m.put("columnfamily", CqlParser.K_COLUMNFAMILY);
        m.put("comment", CqlParser.K_COMMENT);
        m.put("comments", CqlParser.K_COMMENTS);
        m.put("commit", CqlParser.K_COMMIT);
        m.put("compact", CqlParser.K_COMPACT);
        m.put("contains", CqlParser.K_CONTAINS);
        m.put("count", CqlParser.K_COUNT);
        m.put("counter", CqlParser.K_COUNTER);
        m.put("create", CqlParser.K_CREATE);
        m.put("custom", CqlParser.K_CUSTOM);
        m.put("datacenters", CqlParser.K_DATACENTERS);
        m.put("date", CqlParser.K_DATE);
        m.put("decimal", CqlParser.K_DECIMAL);
        m.put("default", CqlParser.K_DEFAULT);
        m.put("delete", CqlParser.K_DELETE);
        m.put("desc", CqlParser.K_DESC);
        m.put("describe", CqlParser.K_DESCRIBE);
        m.put("distinct", CqlParser.K_DISTINCT);
        m.put("double", CqlParser.K_DOUBLE);
        m.put("drop", CqlParser.K_DROP);
        m.put("duration", CqlParser.K_DURATION);
        m.put("end", CqlParser.K_END);
        m.put("entries", CqlParser.K_ENTRIES);
        m.put("execute", CqlParser.K_EXECUTE);
        m.put("exists", CqlParser.K_EXISTS);
        m.put("false", CqlParser.BOOLEAN);
        m.put("field", CqlParser.K_FIELD);
        m.put("filtering", CqlParser.K_FILTERING);
        m.put("finalfunc", CqlParser.K_FINALFUNC);
        m.put("float", CqlParser.K_FLOAT);
        m.put("for", CqlParser.K_FOR);
        m.put("from", CqlParser.K_FROM);
        m.put("frozen", CqlParser.K_FROZEN);
        m.put("full", CqlParser.K_FULL);
        m.put("function", CqlParser.K_FUNCTION);
        m.put("functions", CqlParser.K_FUNCTIONS);
        m.put("generated", CqlParser.K_GENERATED);
        m.put("grant", CqlParser.K_GRANT);
        m.put("group", CqlParser.K_GROUP);
        m.put("hashed", CqlParser.K_HASHED);
        m.put("identity", CqlParser.K_IDENTITY);
        m.put("if", CqlParser.K_IF);
        m.put("in", CqlParser.K_IN);
        m.put("index", CqlParser.K_INDEX);
        m.put("indexes", CqlParser.K_INDEXES);
        m.put("inet", CqlParser.K_INET);
        m.put("infinity", CqlParser.K_POSITIVE_INFINITY);
        m.put("initcond", CqlParser.K_INITCOND);
        m.put("input", CqlParser.K_INPUT);
        m.put("insert", CqlParser.K_INSERT);
        m.put("int", CqlParser.K_INT);
        m.put("internals", CqlParser.K_INTERNALS);
        m.put("into", CqlParser.K_INTO);
        m.put("is", CqlParser.K_IS);
        m.put("json", CqlParser.K_JSON);
        m.put("key", CqlParser.K_KEY);
        m.put("keys", CqlParser.K_KEYS);
        m.put("keyspace", CqlParser.K_KEYSPACE);
        m.put("keyspaces", CqlParser.K_KEYSPACES);
        m.put("label", CqlParser.K_LABEL);
        m.put("labels", CqlParser.K_LABELS);
        m.put("language", CqlParser.K_LANGUAGE);
        m.put("let", CqlParser.K_LET);
        m.put("like", CqlParser.K_LIKE);
        m.put("limit", CqlParser.K_LIMIT);
        m.put("list", CqlParser.K_LIST);
        m.put("login", CqlParser.K_LOGIN);
        m.put("map", CqlParser.K_MAP);
        m.put("masked", CqlParser.K_MASKED);
        m.put("materialized", CqlParser.K_MATERIALIZED);
        m.put("maxwritetime", CqlParser.K_MAXWRITETIME);
        m.put("mbean", CqlParser.K_MBEAN);
        m.put("mbeans", CqlParser.K_MBEANS);
        m.put("modify", CqlParser.K_MODIFY);
        m.put("nan", CqlParser.K_POSITIVE_NAN);
        m.put("nologin", CqlParser.K_NOLOGIN);
        m.put("norecursive", CqlParser.K_NORECURSIVE);
        m.put("nosuperuser", CqlParser.K_NOSUPERUSER);
        m.put("not", CqlParser.K_NOT);
        m.put("null", CqlParser.K_NULL);
        m.put("of", CqlParser.K_OF);
        m.put("on", CqlParser.K_ON);
        m.put("only", CqlParser.K_ONLY);
        m.put("options", CqlParser.K_OPTIONS);
        m.put("or", CqlParser.K_OR);
        m.put("order", CqlParser.K_ORDER);
        m.put("partition", CqlParser.K_PARTITION);
        m.put("password", CqlParser.K_PASSWORD);
        m.put("per", CqlParser.K_PER);
        m.put("permission", CqlParser.K_PERMISSION);
        m.put("permissions", CqlParser.K_PERMISSIONS);
        m.put("primary", CqlParser.K_PRIMARY);
        m.put("rename", CqlParser.K_RENAME);
        m.put("replace", CqlParser.K_REPLACE);
        m.put("returns", CqlParser.K_RETURNS);
        m.put("revoke", CqlParser.K_REVOKE);
        m.put("role", CqlParser.K_ROLE);
        m.put("roles", CqlParser.K_ROLES);
        m.put("schema", CqlParser.K_SCHEMA);
        m.put("security", CqlParser.K_SECURITY);
        m.put("select", CqlParser.K_SELECT);
        m.put("select_masked", CqlParser.K_SELECT_MASKED);
        m.put("set", CqlParser.K_SET);
        m.put("sfunc", CqlParser.K_SFUNC);
        m.put("smallint", CqlParser.K_SMALLINT);
        m.put("static", CqlParser.K_STATIC);
        m.put("storage", CqlParser.K_STORAGE);
        m.put("stype", CqlParser.K_STYPE);
        m.put("superuser", CqlParser.K_SUPERUSER);
        m.put("superusers", CqlParser.K_SUPERUSERS);
        m.put("table", CqlParser.K_COLUMNFAMILY);
        m.put("tables", CqlParser.K_TABLES);
        m.put("text", CqlParser.K_TEXT);
        m.put("then", CqlParser.K_THEN);
        m.put("time", CqlParser.K_TIME);
        m.put("timestamp", CqlParser.K_TIMESTAMP);
        m.put("timeuuid", CqlParser.K_TIMEUUID);
        m.put("tinyint", CqlParser.K_TINYINT);
        m.put("to", CqlParser.K_TO);
        m.put("token", CqlParser.K_TOKEN);
        m.put("transaction", CqlParser.K_TRANSACTION);
        m.put("trigger", CqlParser.K_TRIGGER);
        m.put("true", CqlParser.BOOLEAN);
        m.put("truncate", CqlParser.K_TRUNCATE);
        m.put("ttl", CqlParser.K_TTL);
        m.put("tuple", CqlParser.K_TUPLE);
        m.put("type", CqlParser.K_TYPE);
        m.put("types", CqlParser.K_TYPES);
        m.put("unlogged", CqlParser.K_UNLOGGED);
        m.put("unmask", CqlParser.K_UNMASK);
        m.put("unset", CqlParser.K_UNSET);
        m.put("update", CqlParser.K_UPDATE);
        m.put("use", CqlParser.K_USE);
        m.put("user", CqlParser.K_USER);
        m.put("users", CqlParser.K_USERS);
        m.put("using", CqlParser.K_USING);
        m.put("uuid", CqlParser.K_UUID);
        m.put("values", CqlParser.K_VALUES);
        m.put("varchar", CqlParser.K_VARCHAR);
        m.put("varint", CqlParser.K_VARINT);
        m.put("vector", CqlParser.K_VECTOR);
        m.put("view", CqlParser.K_VIEW);
        m.put("where", CqlParser.K_WHERE);
        m.put("with", CqlParser.K_WITH);
        m.put("writetime", CqlParser.K_WRITETIME);
        return m;
    }

    // Allocation-free keyword classification.
    // The keyword set is packed once into an open-addressed hash table of primitive arrays.
    // At lex time we read the matched identifier's characters straight from the input stream,
    // fold their case in place into a reused per-lexer buffer, and probe the table.  This never
    // allocates a String, never calls toLowerCase, and never boxes an int on the hot path.
    private static final int KW_TABLE_SIZE = 512;      // power of two; load factor stays well under 0.5
    private static final int KW_MASK = KW_TABLE_SIZE - 1;
    private static final int KW_MAX_LEN = 32;          // no CQL keyword is longer; used as a fast reject
    private static final char[][] KW_KEYS = new char[KW_TABLE_SIZE][];
    private static final int[] KW_VALS = new int[KW_TABLE_SIZE];

    static {
        for (java.util.Map.Entry<String, Integer> e : KEYWORDS.entrySet()) {
            char[] key = e.getKey().toCharArray();     // keys are already lower case
            int slot = kwHash(key, key.length) & KW_MASK;
            while (KW_KEYS[slot] != null)
                slot = (slot + 1) & KW_MASK;           // linear probe to the next free slot
            KW_KEYS[slot] = key;
            KW_VALS[slot] = e.getValue().intValue();
        }
    }

    // FNV-1a over case-folded characters.  Folding only touches ASCII A-Z, which is all a
    // keyword can contain; keys were lower case already, so the same hash matches at lookup.
    private static int kwHash(char[] a, int len) {
        int h = 0x811c9dc5;
        for (int i = 0; i < len; i++) {
            char c = a[i];
            if (c >= 'A' && c <= 'Z') c += 32;
            h = (h ^ c) * 0x01000193;
        }
        return h;
    }

    // Reused across every token lexed by this lexer instance; allocated once, never per token.
    private final char[] kwBuf = new char[KW_MAX_LEN];

    @Override
    public org.antlr.v4.runtime.Token nextToken() {
        org.antlr.v4.runtime.Token t = super.nextToken();
        if (t instanceof org.antlr.v4.runtime.WritableToken) {
            int type = t.getType();
            if (type == IDENT) {
                int kw = classifyKeyword(t.getStartIndex(), t.getStopIndex());
                if (kw != -1)
                    ((org.antlr.v4.runtime.WritableToken) t).setType(kw);
            }
            else if (type == NUMBER) {
                // One merged NUMBER rule; recover the original INTEGER / FLOAT / DURATION type.
                ((org.antlr.v4.runtime.WritableToken) t).setType(classifyNumber(t.getStartIndex(), t.getStopIndex()));
            }
            else if (type == DURATION_P) {
                // ISO 8601 'P'-leading durations always were DURATION.
                ((org.antlr.v4.runtime.WritableToken) t).setType(CqlParser.DURATION);
            }
            else if (type == QUOTED) {
                // One merged QUOTED rule; a two-character "" is the empty quoted name, anything
                // longer is a quoted name.  The rule's action already unescaped the longer case.
                int rawLen = t.getStopIndex() - t.getStartIndex() + 1;
                ((org.antlr.v4.runtime.WritableToken) t).setType(rawLen == 2 ? CqlParser.EMPTY_QUOTED_NAME : CqlParser.QUOTED_NAME);
            }
        }
        return t;
    }

    // Classifies a NUMBER token as INTEGER, FLOAT or DURATION by looking at the first character
    // after the leading '-'? DIGIT+ run.  Reads straight from the input stream, bounded by the
    // token span so it never inspects a character past the token, and allocates nothing.
    private int classifyNumber(int start, int stop) {
        int len = stop - start + 1;
        org.antlr.v4.runtime.CharStream in = getInputStream();
        int save = in.index();
        in.seek(start);
        int i = 0;
        if ((char) in.LA(1) == '-')
            i = 1;                                     // skip the optional sign
        while (i < len) {                              // skip the leading digit run
            char c = (char) in.LA(i + 1);
            if (c < '0' || c > '9')
                break;
            i++;
        }
        int type;
        if (i >= len) {
            type = CqlParser.INTEGER;                  // pure '-'? DIGIT+
        } else {
            char c = (char) in.LA(i + 1);              // first char after the digit run
            if (c == '.' || c == 'e' || c == 'E')
                type = CqlParser.FLOAT;                // took the dot branch or an exponent
            else
                type = CqlParser.DURATION;             // a duration unit letter
        }
        in.seek(save);                                 // restore the lexer's read position
        return type;
    }

    // Returns the K_* token type for the identifier at [start, stop] if it is a keyword, else -1.
    // Reads the characters directly from the input stream with no String allocation.
    private int classifyKeyword(int start, int stop) {
        int len = stop - start + 1;
        if (len <= 0 || len > KW_MAX_LEN)
            return -1;                                 // too long to be a keyword

        org.antlr.v4.runtime.CharStream in = getInputStream();
        int save = in.index();
        in.seek(start);
        int h = 0x811c9dc5;
        for (int i = 0; i < len; i++) {
            char c = (char) in.LA(i + 1);
            if (c >= 'A' && c <= 'Z') c += 32;         // fold case in place
            kwBuf[i] = c;
            h = (h ^ c) * 0x01000193;
        }
        in.seek(save);                                 // restore the lexer's read position

        for (int slot = h & KW_MASK; KW_KEYS[slot] != null; slot = (slot + 1) & KW_MASK) {
            char[] key = KW_KEYS[slot];
            if (key.length != len)
                continue;
            int i = 0;
            while (i < len && key[i] == kwBuf[i])
                i++;
            if (i == len)
                return KW_VALS[slot];
        }
        return -1;
    }
}


@header {
    package org.apache.cassandra.cql3;

    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashMap;
    import java.util.HashSet;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.*;
    import org.apache.cassandra.cql3.conditions.*;
    import org.apache.cassandra.cql3.constraints.*;
    import org.apache.cassandra.cql3.functions.*;
    import org.apache.cassandra.cql3.functions.masking.*;
    import org.apache.cassandra.cql3.restrictions.CustomIndexExpression;
    import org.apache.cassandra.cql3.selection.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.statements.schema.*;
    import org.apache.cassandra.cql3.terms.*;
    import org.apache.cassandra.cql3.transactions.*;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.schema.ColumnMetadata;
    import org.apache.cassandra.utils.Pair;
    import org.apache.cassandra.utils.LocalizeString;
}

query returns [CQLStatement.Raw stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = $st.stmt; }
    ;
