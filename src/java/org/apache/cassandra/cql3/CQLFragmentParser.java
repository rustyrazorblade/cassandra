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

import java.util.concurrent.TimeUnit;

import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.Clock;

/**
 * Helper class to encapsulate common code that calls one of the generated methods in {@code CqlParser}.
 */
public final class CQLFragmentParser
{
    private static final Logger logger = LoggerFactory.getLogger(CQLFragmentParser.class);

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
        // The parse-time budget hard stop. It is a ParseCancellationException (a RuntimeException),
        // so it must be caught before the general RuntimeException clause; that clause would
        // otherwise turn it into an opaque message. Surface it with a clear reason instead.
        catch (ParseTimeBudgetExceededException e)
        {
            throw budgetExceeded(meaning, e);
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
     * Fail-fast variant of {@link #parseAny}.  It runs the same two-stage parse, but the LL fallback
     * bails on the first error instead of recovering and reporting.  Use it only for a closed fragment
     * where recovery across multiple errors has no value, and where the exact ANTLR 3 error wording is
     * not guarded by the differential tests; the CQL type fragment is the current example.  Valid input
     * is unaffected: it takes the SLL fast path, and the LL fallback runs only after an SLL error.
     */
    public static <R> R parseAnyFailFast(CQLParserFunction<R> parserFunction, String input, String meaning)
    {
        try
        {
            return parseAnyUnhandled(parserFunction, input, true);
        }
        catch (ParseTimeBudgetExceededException e)
        {
            throw budgetExceeded(meaning, e);
        }
        catch (RecognitionException e)
        {
            throw new SyntaxException("Invalid or malformed " + meaning + ": " + e.getMessage());
        }
        // The fail-fast LL pass bails through a ParseCancellationException.  BailErrorStrategy wraps
        // the underlying recognition error; unwrap it for a clear message.  This clause must precede
        // the general RuntimeException clause, and follow the ParseTimeBudgetExceededException subtype.
        catch (ParseCancellationException e)
        {
            Throwable cause = e.getCause();
            // Fall back through cause message, then exception message, then a fixed string, so a
            // null-on-null pair never renders the literal "null" in the reported error.
            String detail = cause != null && cause.getMessage() != null ? cause.getMessage()
                          : e.getMessage() != null ? e.getMessage()
                          : "invalid syntax";
            throw new SyntaxException("Invalid or malformed " + meaning + ": " + detail);
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
        return parseAnyUnhandled(parserFunction, input, false);
    }

    /**
     * Two-stage parse with an optional fail-fast LL pass.
     *
     * <p>When {@code failFast} is false (the default query path) the LL fallback uses an {@link
     * ErrorCollector} and {@link CqlErrorStrategy}: it recovers, collects the errors, and reports the
     * first one with wording byte-identical to the ANTLR 3 parser.  The differential tests guard that
     * wording.</p>
     *
     * <p>When {@code failFast} is true the LL fallback instead runs with no ErrorCollector, so a {@link
     * BailErrorStrategy} aborts on the first error with no recovery.  The SLL fast path is unchanged, so
     * valid input still takes it and produces the same result; fail-fast changes behavior for malformed
     * input alone.</p>
     */
    public static <R> R parseAnyUnhandled(CQLParserFunction<R> parserFunction, String input, boolean failFast) throws RecognitionException
    {
        // Read the budget once, then share the same deadline across both passes and the lexer.  The
        // total parse time is then bounded by the budget, no matter how many passes run.
        Deadline deadline = parseDeadline();
        try
        {
            // Fast path: SLL prediction, bail on the first error.  No ErrorCollector is attached, so a
            // failed SLL pass leaves no partial state; the LL fallback below rebuilds everything.
            return parse(parserFunction, input, PredictionMode.SLL, null, deadline);
        }
        catch (ParseTimeBudgetExceededException e)
        {
            // The SLL pass exhausted the shared budget.  The LL pass is more expensive and starts with
            // the same, already-blown deadline, so it would fail on its first prediction.  Skip it and
            // fail fast; parseAny turns this into a clear SyntaxException.
            throw e;
        }
        catch (ParseCancellationException e)
        {
            // The SLL pass hit an error (a genuine syntax error, or an ambiguity SLL could not
            // resolve).  Re-parse from scratch with full LL prediction.  In fail-fast mode the LL pass
            // uses no ErrorCollector, so BailErrorStrategy aborts on the first error; otherwise it uses
            // the ErrorCollector so reporting is byte-identical to the single-stage LL parser.  The LL
            // pass reuses the SAME deadline, so its extra cost is bounded by the shared budget.
            ErrorCollector collector = failFast ? null : new ErrorCollector(input);
            return parse(parserFunction, input, PredictionMode.LL, collector, deadline);
        }
    }

    /**
     * Increment the trip metric, log at DEBUG (never the raw input, which is adversarial and may be
     * large), and build the reported error, which names the configured cap.
     */
    private static SyntaxException budgetExceeded(String meaning, ParseTimeBudgetExceededException e)
    {
        QueryProcessor.metrics.parseTimeBudgetExceeded.inc();
        logger.debug("CQL parse-time budget of {} ms exceeded while parsing {}", e.budgetMs, meaning);
        return new SyntaxException("Parsing " + meaning + " exceeded the CQL parse-time budget of "
                                   + e.budgetMs + " ms");
    }

    /**
     * Read the configured parse-time budget and turn it into an absolute deadline.  A budget of 0 or
     * less disables the cap.  A very large budget can overflow the nanosecond clock; that saturates to
     * the far future so the cap stays enabled but effectively never trips, rather than wrapping to a
     * value that would silently disable it or trip immediately.
     */
    private static Deadline parseDeadline()
    {
        long budgetMs = CassandraRelevantProperties.CQL_PARSE_TIME_BUDGET_MS.getLong();
        if (budgetMs <= 0)
            return Deadline.DISABLED;

        long budgetNanos = TimeUnit.MILLISECONDS.toNanos(budgetMs); // saturates on overflow
        long deadlineNanos;
        try
        {
            deadlineNanos = Math.addExact(Clock.Global.nanoTime(), budgetNanos);
        }
        catch (ArithmeticException overflow)
        {
            deadlineNanos = Long.MAX_VALUE;
        }
        return new Deadline(true, deadlineNanos, budgetMs);
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
                               PredictionMode mode, ErrorCollector errorCollector,
                               Deadline deadline) throws RecognitionException
    {
        CharStream stream = CharStreams.fromString(input);
        CqlLexer lexer = new CqlLexer(stream);
        // Drop the default ConsoleErrorListener so errors go only to our collector,
        // matching the ANTLR 3 behavior (no console output).
        lexer.removeErrorListeners();

        if (deadline.enabled)
        {
            // The parser deadline only guards ALL(*) prediction, but our profiling puts the ANTLR 4
            // lexer ATN at the majority of parse CPU, so an input that blows up lexing would bypass a
            // parser-only cap.  Guard the lexer with the same shared deadline.  It reuses the lexer's
            // ATN, per-decision DFA cache, and shared context cache, so it changes only the deadline
            // check, not the tokens the lexer produces.
            LexerATNSimulator lexerInterp = lexer.getInterpreter();
            lexer.setInterpreter(new DeadlineLexerATNSimulator(lexer, lexerInterp.atn, lexerInterp.decisionToDFA,
                                                               lexerInterp.getSharedContextCache(), deadline));
        }

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.removeErrorListeners();

        ParserATNSimulator interpreter = parser.getInterpreter();
        if (deadline.enabled)
        {
            // Replace the prediction engine with one that aborts once the shared deadline passes.  It
            // reuses the parser's ATN, per-decision DFA cache, and shared context cache, so it changes
            // only the deadline check, not the language the parser accepts.
            interpreter = new DeadlineParserATNSimulator(parser, interpreter.atn, interpreter.decisionToDFA,
                                                         interpreter.getSharedContextCache(), deadline);
            parser.setInterpreter(interpreter);
        }
        interpreter.setPredictionMode(mode);

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

        // Skip a trailing statement terminator ';'.  A statement fragment entry point, such as
        // createTableStatement, stops at the end of the statement and does not consume the ';'.  The
        // query rule matches "(';')* EOF", but a bare statement fragment does not, so CreateTableStatement.parse
        // (and AccordKeyspace at class init) passes CQL that ends in ';'.  Consume the terminator so a
        // well-formed statement is not rejected as extraneous input.  A dangling operator such as "1 +"
        // on a bare term is still rejected below, because '+' is not ';'.
        while (tokenStream.LA(1) != Token.EOF && ";".equals(tokenStream.LT(1).getText()))
            tokenStream.consume();

        // Require the parser to consume the whole input.  The bare fragment entry points, such as
        // term and comparatorType, otherwise stop at the first complete construct and silently ignore
        // a dangling operator; for example, the term "1 +" returns 1 and leaves "+" unconsumed.  The
        // query rule already matches a trailing EOF, so its callers reach this point with LA(1) at EOF
        // and pass the check unchanged.
        if (tokenStream.LA(1) != Token.EOF)
        {
            if (errorCollector == null)
            {
                // Fast SLL pass: signal failure the same way every other SLL error does, so
                // parseAnyUnhandled falls back to the LL pass for identical error reporting.
                throw new ParseCancellationException("extraneous input past end of input");
            }

            // LL pass: route the error through the collector so it surfaces as a SyntaxException,
            // consistent with the other parser errors.  It is thrown by throwFirstSyntaxError below.
            Token offending = tokenStream.LT(1);
            parser.notifyErrorListeners(offending, "extraneous input '" + offending.getText() + '\'', null);
        }

        // The errorCollector has queue up any errors that the lexer and parser may have encountered
        // along the way, if necessary, we turn the last error into exceptions here.
        if (errorCollector != null)
            errorCollector.throwFirstSyntaxError();

        return r;
    }

    /**
     * A {@link ParserATNSimulator} that enforces a wall-clock deadline on ALL(*) prediction.  The
     * ALL(*) cost of an adversarial input lives in {@link #adaptivePredict}, so the deadline is
     * checked there.  When the deadline passes the prediction throws {@link
     * ParseTimeBudgetExceededException}.
     */
    private static final class DeadlineParserATNSimulator extends ParserATNSimulator
    {
        private final Deadline deadline;

        DeadlineParserATNSimulator(Parser parser, ATN atn, DFA[] decisionToDFA,
                                   PredictionContextCache sharedContextCache, Deadline deadline)
        {
            super(parser, atn, decisionToDFA, sharedContextCache);
            this.deadline = deadline;
        }

        @Override
        public int adaptivePredict(TokenStream input, int decision, ParserRuleContext outerContext)
        {
            // Checked once per prediction decision, so the parse can overshoot the budget by at most
            // one decision's cost before it aborts.
            if (deadline.exceeded())
                throw new ParseTimeBudgetExceededException(deadline.budgetMs);
            return super.adaptivePredict(input, decision, outerContext);
        }
    }

    /**
     * A {@link LexerATNSimulator} that enforces the same wall-clock deadline on tokenizing.  Lexing an
     * adversarial input drives {@link #match}, so the deadline is checked there.  When the deadline
     * passes the lexer throws {@link ParseTimeBudgetExceededException}.
     */
    private static final class DeadlineLexerATNSimulator extends LexerATNSimulator
    {
        private final Deadline deadline;

        DeadlineLexerATNSimulator(Lexer recog, ATN atn, DFA[] decisionToDFA,
                                  PredictionContextCache sharedContextCache, Deadline deadline)
        {
            super(recog, atn, decisionToDFA, sharedContextCache);
            this.deadline = deadline;
        }

        @Override
        public int match(CharStream input, int mode)
        {
            // Checked once per token, so lexing can overshoot the budget by at most one token before it
            // aborts.
            if (deadline.exceeded())
                throw new ParseTimeBudgetExceededException(deadline.budgetMs);
            return super.match(input, mode);
        }
    }

    /**
     * An absolute parse deadline shared across the lexer and both parse passes.  {@link #DISABLED} means
     * the cap is off.  {@link #budgetMs} is the configured budget, carried for the reported message.
     */
    private static final class Deadline
    {
        static final Deadline DISABLED = new Deadline(false, 0L, 0L);

        final boolean enabled;
        final long deadlineNanos;
        final long budgetMs;

        Deadline(boolean enabled, long deadlineNanos, long budgetMs)
        {
            this.enabled = enabled;
            this.deadlineNanos = deadlineNanos;
            this.budgetMs = budgetMs;
        }

        boolean exceeded()
        {
            // now and the deadline share the same clock origin, so the direct comparison is correct for
            // any origin (including a negative one).  A budget that overflowed saturates the deadline to
            // Long.MAX_VALUE, which this comparison never exceeds, so the cap effectively never trips.
            return enabled && Clock.Global.nanoTime() > deadlineNanos;
        }
    }

    /**
     * Thrown when a parse exceeds the configured parse-time budget.  It extends {@link
     * ParseCancellationException} so the SLL pass routes it the same way as any other prediction
     * abort; {@link #parseAnyUnhandled} then recognizes the subtype and fails fast.
     */
    private static final class ParseTimeBudgetExceededException extends ParseCancellationException
    {
        final long budgetMs;

        ParseTimeBudgetExceededException(long budgetMs)
        {
            super("CQL parse-time budget exceeded");
            this.budgetMs = budgetMs;
        }
    }
}
