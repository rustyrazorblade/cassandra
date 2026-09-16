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

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import org.apache.cassandra.exceptions.SyntaxException;

/**
 * Helper class to encapsulate common code that calls one of the generated methods in {@code CqlParser}.
 */
public final class CQLFragmentParser
{
    /**
     * Error listener for the fast SLL pass.  It throws on the first error so the pass aborts and the
     * caller falls back to the LL pass.  It catches errors that the {@link BailErrorStrategy} does not:
     * lexer errors and grammar-level semantic errors, which the parser reports through
     * {@code notifyErrorListeners} instead of the error strategy.
     */
    private static final BaseErrorListener BAIL_ON_FIRST_ERROR = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                                int charPositionInLine, String msg, RecognitionException e)
        {
            throw new ParseCancellationException(msg, e);
        }
    };

    @FunctionalInterface
    public interface CQLParserFunction<R>
    {
        R parse(CqlParser parser) throws RecognitionException;
    }

    public static <R> R parseAny(CQLParserFunction<R> parserFunction, String input, String meaning)
    {
        try
        {
            return parseAnyUnhandled(parserFunction, input);
        }
        // In ANTLR 4 RecognitionException is a RuntimeException, so it must be caught
        // before the general RuntimeException clause; otherwise that clause hides it.
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed " + meaning + ": " + e.getMessage());
        }
        catch (RuntimeException re)
        {
            throw new SyntaxException(String.format("Failed parsing %s: [%s] reason: %s %s",
                                                    meaning,
                                                    input,
                                                    re.getClass().getSimpleName(),
                                                    re.getMessage()));
        }
    }

    /**
     * Just call a parser method in {@link CqlParser} - does not do any error handling.
     *
     * <p>This uses ANTLR 4's recommended two-stage parsing.  The first pass runs {@link
     * PredictionMode#SLL} with a {@link BailErrorStrategy}; that is the fast path and avoids the
     * expensive full-context (LL) prediction that ANTLR 4 otherwise performs on ambiguous decisions
     * (measured as a large regression on the SELECT parse path versus the ANTLR 3 LL(*) parser).  If
     * the SLL pass reports any error, the input is re-parsed from scratch with {@link
     * PredictionMode#LL} and the {@link CqlErrorStrategy}.  SLL and LL accept the same language, so
     * valid input takes only the fast path and produces the same result the LL pass would; ambiguous
     * or invalid input pays the LL cost exactly as before, with identical error reporting.</p>
     */
    public static <R> R parseAnyUnhandled(CQLParserFunction<R> parserFunction, String input) throws RecognitionException
    {
        try
        {
            // Fast path: SLL prediction, bail on the first error.  No ErrorCollector is attached, so a
            // failed SLL pass leaves no partial state; the LL fallback below rebuilds everything.
            return parse(parserFunction, input, PredictionMode.SLL, null);
        }
        catch (ParseCancellationException e)
        {
            // The SLL pass hit an error (a genuine syntax error, or an ambiguity SLL could not
            // resolve).  Re-parse from scratch with full LL prediction and the CQL error strategy so
            // error reporting is byte-identical to the single-stage LL parser.
            return parse(parserFunction, input, PredictionMode.LL, new ErrorCollector(input));
        }
    }

    /**
     * Build a fresh lexer, token stream, and parser, then run {@code parserFunction} once.
     *
     * @param errorCollector when non-null, the LL pass: the collector receives lexer and parser
     *                       errors and {@link CqlErrorStrategy} governs recovery, and the first
     *                       collected error is thrown after the parse.  When null, the SLL fast pass:
     *                       a {@link BailErrorStrategy} plus {@link #BAIL_ON_FIRST_ERROR} abort on the
     *                       first error of any kind.
     */
    private static <R> R parse(CQLParserFunction<R> parserFunction, String input,
                               PredictionMode mode, ErrorCollector errorCollector) throws RecognitionException
    {
        CharStream stream = CharStreams.fromString(input);
        CqlLexer lexer = new CqlLexer(stream);
        // Drop the default ConsoleErrorListener so errors go only to our collector,
        // matching the ANTLR 3 behavior (no console output).
        lexer.removeErrorListeners();

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.removeErrorListeners();
        parser.getInterpreter().setPredictionMode(mode);

        if (errorCollector != null)
        {
            lexer.addErrorListener(errorCollector);
            parser.addErrorListener(errorCollector);
        }
        else
        {
            // Fast SLL pass: throw on the first error so parseAnyUnhandled falls back to the LL pass.
            // BAIL_ON_FIRST_ERROR catches lexer errors and grammar-level semantic errors (reported
            // through notifyErrorListeners); BailErrorStrategy catches recognition/prediction errors.
            lexer.addErrorListener(BAIL_ON_FIRST_ERROR);
            parser.addErrorListener(BAIL_ON_FIRST_ERROR);
            parser.setErrorHandler(new BailErrorStrategy());
        }

        // Parse the query string to a statement instance
        R r = parserFunction.parse(parser);

        // The errorCollector has queue up any errors that the lexer and parser may have encountered
        // along the way, if necessary, we turn the last error into exceptions here.
        if (errorCollector != null)
            errorCollector.throwFirstSyntaxError();

        return r;
    }
}
