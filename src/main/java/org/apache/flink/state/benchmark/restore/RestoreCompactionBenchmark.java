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

package org.apache.flink.state.benchmark.restore;

import cn.nju.edu.StateInitializerConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.compaction.CompactionScheduler;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.rocksdb.CompactionRunner;
import org.apache.flink.state.benchmark.BackendUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static cn.nju.edu.StateInitializerConstants.MB;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.dbDirName;

@BenchmarkMode(Mode.AverageTime)
public class RestoreCompactionBenchmark extends RestoreBenchmarkBase {
    @Param({"LIST", "MAP"})
    private static RestoreBenchmarkBase.StateType stateType;

    private static boolean finishKeyElimination;
    private static CompactionScheduler compactionScheduler;

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RestoreCompactionBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUpPerTrial() throws Exception {
        switch (stateType) {
            case MAP:
                prepareDummyKeyedStateBackends(new File(StateInitializerConstants.getMapStateInitializerRootDir()));
                break;
            case LIST:
                prepareDummyKeyedStateBackends(new File(StateInitializerConstants.getListStateInitializerRootDir()));
                break;
            default:
                throw new UnsupportedOperationException("Unknown state type " + stateType);
        }
    }

    @Setup(Level.Invocation)
    public void setupPerInvocation() throws IOException {
        switch (mode) {
            case FILE_INGESTION:
                dbPath = BackendUtils.prepareDirectory(dbDirName, rootDir);
                tmpKeyedStateHandles = new ArrayList<>(keyedStateHandles);

                List<KeyGroupRange> keyGroupRanges = StateInitializerConstants.splitKeyGroupRanges(parallelism);
                KeyGroupRange startKeyGroupRange = keyGroupRanges.get(0);
                KeyGroupRange endKeyGroupRange = keyGroupRanges.get(keyGroupRanges.size() - 1);
                int leftBound = (startKeyGroupRange.getStartKeyGroup() + startKeyGroupRange.getEndKeyGroup()) / 2;
                int rightBound = (endKeyGroupRange.getStartKeyGroup() + endKeyGroupRange.getEndKeyGroup()) / 2;

                Configuration config = new Configuration();
                config.set(ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL, Long.MAX_VALUE);
                config.set(ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE, "Level");

                compactionScheduler = new CompactionScheduler(config);
                compactionScheduler.setRescaling(true);

                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackendEnableFileIngestion(
                        rootDir,
                        dbPath,
                        tmpKeyedStateHandles,
                        new KeyGroupRange(leftBound, rightBound),
                        maxParallelism,
                        compactionScheduler);
                finishKeyElimination = false;
                compactionScheduler.finishRestore(() -> {finishKeyElimination = true; return true;});
                Preconditions.checkArgument(compactionScheduler.getSchedulerState() == CompactionScheduler.SchedulerState.OnKeyElimination);

                break;
            case BASIC:
            default:
        }

    }

    @TearDown(Level.Invocation)
    public void tearDownPerInvocation() throws IOException {
        switch (mode) {
            case FILE_INGESTION:
                keyedStateBackend.dispose();
                FileUtils.deleteDirectory(dbPath);
            case BASIC:
            default:
        }
    }

    @Benchmark
    @Warmup(iterations = 2)
    @Measurement(iterations = 2)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void compactionForKeyElimination() throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                while (!finishKeyElimination) {
                    try (CompactionRunner compactionJob = compactionScheduler.pickCompactionFromTask()) {
                        if (compactionJob != null) {
                            compactionJob.runCompaction();
                        } else {
                            break;
                        }
                    }
                }
                break;
            case BASIC:
            default:
        }
    }

}
