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
import org.openjdk.jmh.annotations.Fork;
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
import java.util.concurrent.TimeUnit;

import static cn.nju.edu.StateInitializerConstants.MB;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ENABLE_TRIVIAL_MOVE_AFTER_FILE_INGESTION;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.dbDirName;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
@Fork(value = 1, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"})
public class CompactionToEndBenchmark extends RestoreBenchmarkBase {
    @Param({"LIST", "MAP"})
    private static StateType type;

    @Param({"true", "false"})
    private static boolean trivialMove;

    private static CompactionScheduler compactionScheduler;

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + CompactionToEndBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUpPerTrial() throws Exception {
        switch (type) {
            case MAP:
                prepareDummyKeyedStateBackends(new File(StateInitializerConstants.getMapStateInitializerRootDir()));
                break;
            case LIST:
                prepareDummyKeyedStateBackends(new File(StateInitializerConstants.getListStateInitializerRootDir()));
                break;
            default:
                throw new UnsupportedOperationException("Unknown state type " + type);
        }
    }

    @Setup(Level.Invocation)
    public void setupPerInvocation() throws IOException {
        switch (mode) {
            case FILE_INGESTION:
                dbPath = BackendUtils.prepareDirectory(dbDirName, rootDir);
                tmpKeyedStateHandles = new ArrayList<>(keyedStateHandles);

                Configuration config = new Configuration();
                config.set(ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL, Long.MAX_VALUE);
                config.set(ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE, "Level");
                config.set(ENABLE_TRIVIAL_MOVE_AFTER_FILE_INGESTION, trivialMove);

                compactionScheduler = new CompactionScheduler(config);
                compactionScheduler.setRescaling(true);

                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackendEnableFileIngestion(
                        rootDir,
                        dbPath,
                        tmpKeyedStateHandles,
                        new KeyGroupRange(0, maxParallelism - 1),
                        maxParallelism,
                        compactionScheduler);
                doEmptyKeyElimination(compactionScheduler);

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
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void compactionToEnd() throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                if (trivialMove) {
                    while (true) {
                        try (CompactionRunner compactionJob = compactionScheduler.pickCompactionFromTask()) {
                            if (compactionJob != null) {
                                if (trivialMove) {
                                    Preconditions.checkArgument(compactionJob.isTrivialMove());
                                }
                                compactionJob.runCompaction();
                            } else {
                                break;
                            }
                        }
                    }
                }

                while (true) {
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
