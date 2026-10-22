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

package org.apache.cassandra.db.cursorreads;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

/**
 * Runs every test of a subclass once per sstable format, BIG and BTI.  The format is selected
 * before each test and restored after it, so the class runs the same under any harness config.
 */
@RunWith(Parameterized.class)
public abstract class SSTableFormatParameterizedTester extends CursorReadDifferentialTester
{
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return Arrays.asList(new Object[][]{ { "big" }, { "bti" } });
    }

    private SSTableFormat<?, ?> formatBeforeTest;

    @Before
    public void selectParameterFormat()
    {
        formatBeforeTest = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(format);
    }

    @After
    public void restoreParameterFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(formatBeforeTest);
    }

    /** The sstable format this run selects. */
    protected String formatName()
    {
        return format;
    }

    protected boolean isBti()
    {
        return "bti".equals(format);
    }
}
