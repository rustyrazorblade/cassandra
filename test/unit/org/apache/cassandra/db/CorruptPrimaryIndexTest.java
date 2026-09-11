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

package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CorruptPrimaryIndexTest extends CQLTester.InMemory
{
    protected ListenableFileSystem.PathFilter isCurrentTableIndexFile(String keyspace, String endsWith)
    {
        return path -> {
            if (!path.getFileName().toString().endsWith(endsWith))
                return false;
            Descriptor desc = Descriptor.fromFile(new File(path));
            if (!desc.ksname.equals(keyspace) || !desc.cfname.equals(currentTable()))
                return false;
            return true;
        };
    }

    @Test
    public void primaryIndexDiskCorruptionIsDetected()
    {
        // Set listener early, before the file is opened; mmap access can not be listened to, so need to observe the open, which happens on flush
        if (BtiFormat.isSelected())
        {
            fs.onPostRead(isCurrentTableIndexFile(keyspace(), "Partitions.db"), (path, channel, position, dst, read) -> {
                // simulate bit rot by having 1 byte change...
                // first read should be in the footer -- give it an invalid root position
                // TODO: Change this to modify something in a more undetectable position in the file when checksumming is implemented
                dst.put(2, Byte.MAX_VALUE);
            });
        }
        else
            throw Util.testMustBeImplementedForSSTableFormat();

        createTable("CREATE TABLE %s (id int PRIMARY KEY, value int)");
        execute("INSERT INTO %s (id, value) VALUES (?, ?)", 0, 0);
        flush();

        assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE id=?", 0)).isInstanceOf(CorruptSSTableException.class);
    }
}
