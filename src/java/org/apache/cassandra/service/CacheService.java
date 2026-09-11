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
package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.cache.AutoSavingCache.CacheSerializer;
import org.apache.cassandra.cache.CacheProvider;
import org.apache.cassandra.cache.CaffeineCache;
import org.apache.cassandra.cache.CounterCacheKey;
import org.apache.cassandra.cache.ICache;
import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;

public class CacheService implements CacheServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public enum CacheType
    {
        ROW_CACHE("RowCache"),
        COUNTER_CACHE("CounterCache");

        private final String name;

        CacheType(String typeName)
        {
            name = typeName;
        }

        public String toString()
        {
            return name;
        }
    }

    public final static CacheService instance = new CacheService();

    public final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache;
    public final AutoSavingCache<CounterCacheKey, ClockAndCount> counterCache;

    private CacheService()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);

        rowCache = initRowCache();
        counterCache = initCounterCache();
    }

    /**
     * @return initialized row cache
     */
    private AutoSavingCache<RowCacheKey, IRowCacheEntry> initRowCache()
    {
        logger.info("Initializing row cache with capacity of {} MiBs", DatabaseDescriptor.getRowCacheSizeInMiB());

        CacheProvider<RowCacheKey, IRowCacheEntry> cacheProvider;
        String cacheProviderClassName = DatabaseDescriptor.getRowCacheSizeInMiB() > 0
                                        ? DatabaseDescriptor.getRowCacheClassName() : "org.apache.cassandra.cache.NopCacheProvider";
        try
        {
            Class<? extends CacheProvider> cacheProviderClass =
                FBUtilities.classForNameWithoutInitialization(cacheProviderClassName, "row cache provider", CacheProvider.class);
            @SuppressWarnings("unchecked")
            CacheProvider<RowCacheKey, IRowCacheEntry> typedCacheProvider = cacheProviderClass.newInstance();
            cacheProvider = typedCacheProvider;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot find configured row cache provider class " + DatabaseDescriptor.getRowCacheClassName());
        }

        // cache object
        ICache<RowCacheKey, IRowCacheEntry> rc = cacheProvider.create();
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = new AutoSavingCache<>(rc, CacheType.ROW_CACHE, new RowCacheSerializer());

        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        rowCache.scheduleSaving(DatabaseDescriptor.getRowCacheSavePeriod(), rowCacheKeysToSave);

        return rowCache;
    }

    private AutoSavingCache<CounterCacheKey, ClockAndCount> initCounterCache()
    {
        logger.info("Initializing counter cache with capacity of {} MiBs", DatabaseDescriptor.getCounterCacheSizeInMiB());

        long capacity = DatabaseDescriptor.getCounterCacheSizeInMiB() * 1024 * 1024;

        AutoSavingCache<CounterCacheKey, ClockAndCount> cache =
            new AutoSavingCache<>(CaffeineCache.create(capacity),
                                  CacheType.COUNTER_CACHE,
                                  new CounterCacheSerializer());

        int keysToSave = DatabaseDescriptor.getCounterCacheKeysToSave();

        logger.info("Scheduling counter cache save to every {} seconds (going to save {} keys).",
                    DatabaseDescriptor.getCounterCacheSavePeriod(),
                    keysToSave == Integer.MAX_VALUE ? "all" : keysToSave);

        cache.scheduleSaving(DatabaseDescriptor.getCounterCacheSavePeriod(), keysToSave);

        return cache;
    }


    public int getRowCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getRowCacheSavePeriod();
    }

    public void setRowCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("RowCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setRowCacheSavePeriod(seconds);
        rowCache.scheduleSaving(seconds, DatabaseDescriptor.getRowCacheKeysToSave());
    }

    public int getCounterCacheSavePeriodInSeconds()
    {
        return DatabaseDescriptor.getCounterCacheSavePeriod();
    }

    public void setCounterCacheSavePeriodInSeconds(int seconds)
    {
        if (seconds < 0)
            throw new RuntimeException("CounterCacheSavePeriodInSeconds must be non-negative.");

        DatabaseDescriptor.setCounterCacheSavePeriod(seconds);
        counterCache.scheduleSaving(seconds, DatabaseDescriptor.getCounterCacheKeysToSave());
    }

    public int getRowCacheKeysToSave()
    {
        return DatabaseDescriptor.getRowCacheKeysToSave();
    }

    public void setRowCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("RowCacheKeysToSave must be non-negative.");
        DatabaseDescriptor.setRowCacheKeysToSave(count);
        rowCache.scheduleSaving(getRowCacheSavePeriodInSeconds(), count);
    }

    public int getCounterCacheKeysToSave()
    {
        return DatabaseDescriptor.getCounterCacheKeysToSave();
    }

    public void setCounterCacheKeysToSave(int count)
    {
        if (count < 0)
            throw new RuntimeException("CounterCacheKeysToSave must be non-negative.");
        DatabaseDescriptor.setCounterCacheKeysToSave(count);
        counterCache.scheduleSaving(getCounterCacheSavePeriodInSeconds(), count);
    }

    public void invalidateRowCache()
    {
        rowCache.clear();
    }

    public void invalidateRowCacheForCf(TableMetadata tableMetadata)
    {
        Iterator<RowCacheKey> rowCacheIterator = rowCache.keyIterator();
        while (rowCacheIterator.hasNext())
        {
            RowCacheKey key = rowCacheIterator.next();
            if (key.sameTable(tableMetadata))
                rowCacheIterator.remove();
        }
    }

    public void invalidateCounterCacheForCf(TableMetadata tableMetadata)
    {
        Iterator<CounterCacheKey> counterCacheIterator = counterCache.keyIterator();
        while (counterCacheIterator.hasNext())
        {
            CounterCacheKey key = counterCacheIterator.next();
            if (key.sameTable(tableMetadata))
                counterCacheIterator.remove();
        }
    }

    public void invalidateCounterCache()
    {
        counterCache.clear();
    }




    public void setRowCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        rowCache.setCapacity(capacity * 1024 * 1024);
    }


    public void setCounterCacheCapacityInMB(long capacity)
    {
        if (capacity < 0)
            throw new RuntimeException("capacity should not be negative.");

        counterCache.setCapacity(capacity * 1024 * 1024);
    }

    public void saveCaches() throws ExecutionException, InterruptedException
    {
        List<Future<?>> futures = new ArrayList<>(2);
        logger.debug("submitting cache saves");

        futures.add(rowCache.submitWrite(DatabaseDescriptor.getRowCacheKeysToSave()));
        futures.add(counterCache.submitWrite(DatabaseDescriptor.getCounterCacheKeysToSave()));

        FBUtilities.waitOnFutures(futures);
        logger.debug("cache saves completed");
    }

    public static class CounterCacheSerializer extends CacheSerializer<CounterCacheKey, ClockAndCount>
    {
        public void serialize(CounterCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException
        {
            assert(cfs.metadata().isCounter());
            writeCFS(out, cfs);
            key.write(out);
        }

        public Future<Pair<CounterCacheKey, ClockAndCount>> deserialize(DataInputPlus in) throws IOException
        {
            //Keyspace and CF name are deserialized by AutoSaving cache and used to fetch the CFS provided as a
            //parameter so they aren't deserialized here, even though they are serialized by this serializer
            ColumnFamilyStore cfs = readCFS(in);
            if (cfs == null)
                return null;
            final CounterCacheKey cacheKey = CounterCacheKey.read(cfs.metadata(), in);
            if (!cfs.metadata().isCounter() || !cfs.isCounterCacheEnabled())
                return null;

            return Stage.READ.submit(() -> {
                ByteBuffer value = cacheKey.readCounterValue(cfs);
                return value == null
                     ? null
                     : Pair.create(cacheKey, CounterContext.instance().getLocalClockAndCount(value));
            });
        }
    }

    public static class RowCacheSerializer extends CacheSerializer<RowCacheKey, IRowCacheEntry>
    {
        public void serialize(RowCacheKey key, DataOutputPlus out, ColumnFamilyStore cfs) throws IOException
        {
            assert(!cfs.isIndex());//Shouldn't have row cache entries for indexes
            writeCFS(out, cfs);
            ByteArrayUtil.writeWithLength(key.key, out);
        }

        public Future<Pair<RowCacheKey, IRowCacheEntry>> deserialize(DataInputPlus in) throws IOException
        {
            //Keyspace and CF name are deserialized by AutoSaving cache and used to fetch the CFS provided as a
            //parameter so they aren't deserialized here, even though they are serialized by this serializer
            ColumnFamilyStore cfs = readCFS(in);
            final ByteBuffer buffer = ByteBufferUtil.readWithLength(in);
            if (cfs == null  || !cfs.isRowCacheEnabled())
                return null;
            final int rowsToCache = cfs.metadata().params.caching.rowsPerPartitionToCache();
            assert(!cfs.isIndex());//Shouldn't have row cache entries for indexes

            return Stage.READ.submit(() -> {
                DecoratedKey key = cfs.decorateKey(buffer);
                long nowInSec = FBUtilities.nowInSeconds();
                SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, key);
                try (ReadExecutionController controller = cmd.executionController(); UnfilteredRowIterator iter = cmd.queryMemtableAndDisk(cfs, controller))
                {
                    CachedPartition toCache = CachedBTreePartition.create(DataLimits.cqlLimits(rowsToCache).filter(iter, nowInSec, true), nowInSec);
                    return Pair.create(new RowCacheKey(cfs.metadata(), key), toCache);
                }
            });
        }
    }

}
