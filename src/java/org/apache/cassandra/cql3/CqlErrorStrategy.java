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

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;

/**
 * Error-recovery strategy that preserves the behavior of the ANTLR 3 CQL parser.
 *
 * The ANTLR 3 grammar overrode {@code recover()} and
 * {@code recoverFromMismatchedToken()} to stop recovery on the first syntax
 * error, because the parse result is ignored anyway.  ANTLR 4 recovers by
 * default: it deletes or inserts tokens, continues the parse, and can report
 * several errors for a single mistake.
 *
 * This strategy restores the ANTLR 3 behavior.  It reports the first syntax
 * error, then stops recovering.  It does not consume tokens to resynchronize
 * and it does not perform single-token deletion or insertion.
 */
public final class CqlErrorStrategy extends DefaultErrorStrategy
{
    /**
     * ANTLR 3 {@code recover()} was a no-op.  Do not consume tokens to
     * resynchronize after an error.
     */
    @Override
    public void recover(Parser recognizer, RecognitionException e)
    {
    }

    /**
     * ANTLR 3 {@code recoverFromMismatchedToken()} re-threw instead of deleting
     * or inserting a token.  Throw so the enclosing rule reports one error and
     * stops.
     */
    @Override
    public Token recoverInline(Parser recognizer) throws RecognitionException
    {
        throw new InputMismatchException(recognizer);
    }

    /**
     * ANTLR 3 had no pre-emptive resynchronization step.  Detect errors only at
     * a token match, exactly as the ANTLR 3 parser did.
     */
    @Override
    public void sync(Parser recognizer)
    {
    }
}
