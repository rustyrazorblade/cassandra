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

import java.util.BitSet;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

/**
 * Listener used to collect the syntax errors emitted by the Lexer and Parser.
 *
 * <p>In ANTLR 4 both the lexer and the parser report every error, syntactic or
 * grammar-level (via {@code notifyErrorListeners}), through the single
 * {@link ANTLRErrorListener#syntaxError} method.  This interface narrows
 * {@link ANTLRErrorListener} to that one method and gives the prediction
 * callbacks no-op defaults, so a collector only needs to implement
 * {@code syntaxError}.</p>
 */
public interface ErrorListener extends ANTLRErrorListener
{
    /**
     * {@inheritDoc}
     *
     * Invoked when a syntax error occurs.
     */
    @Override
    void syntaxError(Recognizer<?, ?> recognizer,
                     Object offendingSymbol,
                     int line,
                     int charPositionInLine,
                     String msg,
                     RecognitionException e);

    @Override
    default void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex, int stopIndex,
                                 boolean exact, BitSet ambigAlts, ATNConfigSet configs) {}

    @Override
    default void reportAttemptingFullContext(Parser recognizer, DFA dfa, int startIndex, int stopIndex,
                                             BitSet conflictingAlts, ATNConfigSet configs) {}

    @Override
    default void reportContextSensitivity(Parser recognizer, DFA dfa, int startIndex, int stopIndex,
                                          int prediction, ATNConfigSet configs) {}
}
