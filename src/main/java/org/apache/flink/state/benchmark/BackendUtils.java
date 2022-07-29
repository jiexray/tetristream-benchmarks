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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.compaction.CompactionScheduler;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.rocksdb.ColumnFamilyOptions;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.state.benchmark.StateBenchmarkConstants.recoveryDirName;

/**
 * Utils to create keyed state backend.
 */
public class BackendUtils {
    public static RocksDBKeyedStateBackend<Long> createRocksDBKeyedStateBackend(
            File rootDir,
            File dbPathFile,
            List<KeyedStateHandle> restoreStateHandles,
            KeyGroupRange keyGroupRange,
            int maxParallelism) throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        RocksDBResourceContainer resourceContainer = new RocksDBResourceContainer();
        resourceContainer.getDbOptions().setCreateIfMissing(true);

        if (keyGroupRange == null) {
            maxParallelism = 2;
            keyGroupRange = new KeyGroupRange(0, 1);
        } else {
            Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() <= maxParallelism);
        }

        ColumnFamilyOptions columnOptions = new ColumnFamilyOptions();
        ExecutionConfig executionConfig = new ExecutionConfig();
        RocksDBKeyedStateBackendBuilder<Long> builder = new RocksDBKeyedStateBackendBuilder<>(
                "Test",
                Thread.currentThread().getContextClassLoader(),
                dbPathFile,
                resourceContainer,
                stateName -> PredefinedOptions.DEFAULT.createColumnOptions(),
                null,
                LongSerializer.INSTANCE,
                maxParallelism,
                keyGroupRange,
                executionConfig,
                new LocalRecoveryConfig(false, new LocalRecoveryDirectoryProviderImpl(recoveryBaseDir, new JobID(), new JobVertexID(), 0)),
                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                restoreStateHandles == null ? Collections.emptyList() : restoreStateHandles,
                AbstractStateBackend.getCompressionDecorator(executionConfig),
                new CloseableRegistry());
        try {
            builder.setEnableIncrementalCheckpointing(true);
            return builder.build();
        } catch (Exception e) {
            IOUtils.closeQuietly(columnOptions);
            IOUtils.closeQuietly(resourceContainer);
            throw e;
        }
    }

    public static RocksDBKeyedStateBackend<Long> createRocksDBKeyedStateBackendEnableFileIngestion(
            File rootDir,
            File dbPathFile,
            List<KeyedStateHandle> restoreStateHandles,
            KeyGroupRange keyGroupRange,
            int maxParallelism,
            @Nullable CompactionScheduler compactionScheduler) throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        RocksDBResourceContainer resourceContainer = new RocksDBResourceContainer();
        resourceContainer.getDbOptions().setCreateIfMissing(true);

        if (keyGroupRange == null) {
            maxParallelism = 2;
            keyGroupRange = new KeyGroupRange(0, 1);
        } else {
            Preconditions.checkArgument(keyGroupRange.getNumberOfKeyGroups() <= maxParallelism);
        }

        ColumnFamilyOptions columnOptions = new ColumnFamilyOptions();
        ExecutionConfig executionConfig = new ExecutionConfig();
        RocksDBKeyedStateBackendBuilder<Long> builder = new RocksDBKeyedStateBackendBuilder<>(
                "Test",
                Thread.currentThread().getContextClassLoader(),
                dbPathFile,
                resourceContainer,
                stateName -> PredefinedOptions.COMPACTION_DISABLED.createColumnOptions(),
                null,
                LongSerializer.INSTANCE,
                maxParallelism,
                keyGroupRange,
                executionConfig,
                new LocalRecoveryConfig(false, new LocalRecoveryDirectoryProviderImpl(recoveryBaseDir, new JobID(), new JobVertexID(), 0)),
                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                restoreStateHandles == null ? Collections.emptyList() : restoreStateHandles,
                AbstractStateBackend.getCompressionDecorator(executionConfig),
                new CloseableRegistry());
        builder.setEnableIncrementalCheckpointing(true);
        builder.setEnableRestoreWithRescalingByFileIngestion(true);
        if (compactionScheduler == null) {
            builder.setCompactionScheduler(CompactionScheduler.getDummyCompactionScheduler());
        } else {
            builder.setCompactionScheduler(compactionScheduler);
        }
        builder.setParallelizedL0Compaction(true);

        try {
            return builder.build();
        } catch (Exception e) {
            IOUtils.closeQuietly(columnOptions);
            IOUtils.closeQuietly(resourceContainer);
            throw e;
        }
    }

    static HeapKeyedStateBackend<Long> createHeapKeyedStateBackend(File rootDir) throws IOException {
        File recoveryBaseDir = prepareDirectory(recoveryDirName, rootDir);
        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        int numberOfKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();
        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
        HeapKeyedStateBackendBuilder<Long> backendBuilder = new HeapKeyedStateBackendBuilder<>(
                null,
                new LongSerializer(),
                Thread.currentThread().getContextClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                Collections.emptyList(),
                AbstractStateBackend.getCompressionDecorator(executionConfig),
                new LocalRecoveryConfig(false, new LocalRecoveryDirectoryProviderImpl(recoveryBaseDir, new JobID(), new JobVertexID(), 0)),
                priorityQueueSetFactory,
                false,
                new CloseableRegistry()
        );
        return backendBuilder.build();
    }

    public static File prepareDirectory(String prefix, File parentDir) throws IOException {
        File target = File.createTempFile(prefix, "", parentDir);
        if (target.exists() && !target.delete()) {
            throw new IOException("Target dir {" + target.getAbsolutePath() + "} exists but failed to clean it up");
        } else if (!target.mkdirs()) {
            throw new IOException("Failed to create target directory: " + target.getAbsolutePath());
        }
        return target;
    }

    public static KeyedStateHandle deserializeKeyedStateHandle(File keyedStateHandleFile) throws IOException, ClassNotFoundException {
        try (FileInputStream fileIn = new FileInputStream(keyedStateHandleFile);
             ObjectInputStream objectIn = new ObjectInputStream(fileIn)) {

            KeyedStateHandle keyedStateHandle = (KeyedStateHandle) objectIn.readObject();
            Preconditions.checkArgument(keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle);

            return keyedStateHandle;
        } catch (Exception e) {
            throw e;
        }
    }
}
