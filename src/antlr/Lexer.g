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

lexer grammar Lexer;

// Keywords are not lexed as individual rules.  The lexer matches an IDENT and the
// nextToken() override in Cql.g reclassifies it to the matching K_* token type using an
// allocation-free keyword table.  This keeps ~170 keyword rules out of the lexer ATN, so
// identifier lexing does not weigh every keyword at each character.
// When adding a new reserved keyword, add an entry to o.a.c.cql3.ReservedKeywords, to
// pylib/cqlshlib/cqlhandling.py::cql_keywords_reserved, and to the tokens{} block in Cql.g.
// When adding a new unreserved keyword, add an entry to unreserved keywords in Parser.g.

// The '-' prefixed NaN/Infinity keywords keep dedicated rules: they start with '-', not a
// letter, so they are never lexed as IDENT and cannot be reclassified.
K_NEGATIVE_NAN: '-' N A N;
K_NEGATIVE_INFINITY: '-' I N F I N I T Y;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

STRING_LITERAL
    : (
        /* pg-style string literal */
        ( '$$' .*? '$$' )
        |
        /* conventional quoted string literal */
        ( '\'' ( ~'\'' | '\'' '\'' )* '\'' )
      )
      {
          String raw = getText();
          if (!raw.isEmpty() && raw.charAt(0) == '$')
              // pg-style: strip the leading and trailing '$$'
              setText(raw.substring(2, raw.length() - 2));
          else
              // conventional: strip the surrounding quotes and unescape doubled quotes
              setText(raw.substring(1, raw.length() - 1).replace("''", "'"));
      }
    ;

/*
 * One rule for both quoted names.  It replaces QUOTED_NAME (one-or-more inner) and
 * EMPTY_QUOTED_NAME ("").  nextToken() in Cql.g looks at the raw length: a two-character "" is
 * EMPTY_QUOTED_NAME, anything longer is QUOTED_NAME.  The action unescapes only the longer case,
 * leaving the empty name as the literal "" the old EMPTY_QUOTED_NAME rule produced.
 */
QUOTED
    : '"' ( ~'"' | '"' '"' )* '"'
      { if (getText().length() > 2) setText(getText().substring(1, getText().length() - 1).replace("\"\"", "\"")); }
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

fragment EXPONENT
    : E ('+' | '-')? DIGIT+
    ;

fragment DURATION_ISO_8601_PERIOD_DESIGNATORS
    : '-'? 'P' DIGIT+ 'Y' (DIGIT+ 'M')? (DIGIT+ 'D')?
    | '-'? 'P' DIGIT+ 'M' (DIGIT+ 'D')?
    | '-'? 'P' DIGIT+ 'D'
    ;

fragment DURATION_ISO_8601_TIME_DESIGNATORS
    : 'T' DIGIT+ 'H' (DIGIT+ 'M')? (DIGIT+ 'S')?
    | 'T' DIGIT+ 'M' (DIGIT+ 'S')?
    | 'T' DIGIT+ 'S'
    ;

fragment DURATION_ISO_8601_WEEK_PERIOD_DESIGNATOR
    : '-'? 'P' DIGIT+ 'W'
    ;

fragment DURATION_UNIT
    : Y
    | M O
    | W
    | D
    | H
    | M
    | S
    | M S
    | U S
    | '\u00B5' S
    | N S
    ;

QMARK
    : '?'
    ;

RANGE
    : '..'
    ;

/*
 * NUMBER is one rule covering integers, floats and digit-leading durations.  It used to be three
 * rules (INTEGER, FLOAT, and the digit-leading DURATION alternative); merging them keeps a single
 * '-'? DIGIT+ prefix in the lexer ATN instead of three overlapping ones, which cuts the config-set
 * churn while the lexer weighs a number.  nextToken() in Cql.g reclassifies each NUMBER back to
 * INTEGER, FLOAT or DURATION so the parser sees the same token types as before.
 *
 * The optional tail decides the kind:
 *   - the dot branch (a float) uses the same predicate the old FLOAT rule used.  It overlaps
 *     RANGE ('..'): the dot is taken only when the next character is not a dot, or when a third
 *     dot follows.  So "3." is a float, "1..3" is INTEGER RANGE INTEGER, and "0...3." is
 *     FLOAT '0.' RANGE '..' FLOAT '3.'.
 *   - the EXPONENT branch (a float) covers "1e3" with no dot.
 *   - the DURATION_UNIT branch covers "1y2mo3d".
 */
NUMBER
    : '-'? DIGIT+
      ( { _input.LA(1) == '.' && (_input.LA(2) != '.' || _input.LA(3) == '.') }? '.' DIGIT* EXPONENT?
      | EXPONENT
      | DURATION_UNIT (DIGIT+ DURATION_UNIT)*
      )?
    ;

/*
 * ISO 8601 'P'-leading durations.  Split off from the old DURATION rule and left-factored on the
 * shared '-'? 'P' prefix.  nextToken() reclassifies DURATION_P to DURATION.  The accepted strings
 * and their spans are unchanged.
 */
DURATION_P
    : '-'? 'P' DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT // ISO 8601 "alternative format"
    | '-'? 'P' DURATION_ISO_8601_TIME_DESIGNATORS
    | DURATION_ISO_8601_WEEK_PERIOD_DESIGNATOR
    | DURATION_ISO_8601_PERIOD_DESIGNATORS DURATION_ISO_8601_TIME_DESIGNATORS?
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
    ;

HEXNUMBER
    : '0' X HEX*
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ -> channel(HIDDEN)
    ;

COMMENT
    : ('--' | '//') ~('\n'|'\r')* ('\n'|'\r')? -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;
