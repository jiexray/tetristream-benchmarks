package org.apache.flink.state.benchmark.restore;

import cn.nju.edu.StateInitializerConstants;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.compaction.CompactionScheduler;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.rocksdb.CompactionRunner;
import org.apache.flink.util.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.nju.edu.StateInitializerConstants.MB;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ENABLE_TRIVIAL_MOVE_AFTER_FILE_INGESTION;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE;
import static org.apache.flink.contrib.streaming.state.RocksDBOptions.ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@BenchmarkMode(Throughput)
@Warmup(iterations = 30)
@Measurement(iterations = 30)
@Fork(value = 1, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"})
public class MapStateAfterCompactionBenchmark extends RestoreBenchmarkBase {
    @Param({"0", "30000", "60000", "120000", "180000", "-1"})
    private static long compactionDuration;

    private MapState<Long, byte[]> mapState;
    private static CompactionScheduler compactionScheduler;
    private Map<Long, byte[]> dummyMaps;

    protected final File initializerRootDir = new File(StateInitializerConstants.getMapStateInitializerRootDir());

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + MapStateAfterCompactionBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUpPerTrial() throws Exception {
        keyIndex = new AtomicInteger();
        int mapKeyCount = StateInitializerConstants.mapKeyCount;
        List<Long> mapKeys = StateInitializerConstants.mapKeys;

        switch (mode) {
            case FILE_INGESTION:

                Configuration config = new Configuration();
                config.set(ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL, Long.MAX_VALUE);
                config.set(ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE, "Level");
                config.set(ENABLE_TRIVIAL_MOVE_AFTER_FILE_INGESTION, true);

                compactionScheduler = new CompactionScheduler(config);
                compactionScheduler.setRescaling(false);

                createKeyedStateBackend(initializerRootDir, compactionScheduler);

                mapState = keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>("mapState", Long.class, byte[].class));

                doEmptyKeyElimination(compactionScheduler);

                final long targetCompactionFinishedTimestamp;
                long compactionStartTimestamp = System.currentTimeMillis();
                if (compactionDuration == -1L) {
                    targetCompactionFinishedTimestamp = Long.MAX_VALUE;
                } else {
                    targetCompactionFinishedTimestamp = System.currentTimeMillis() + compactionDuration;
                }

                boolean finishTrivialMove = false;
                while (System.currentTimeMillis() < targetCompactionFinishedTimestamp) {
                    try (CompactionRunner compactionJob = compactionScheduler.pickCompactionFromTask()) {
                        if (compactionJob != null) {
                            compactionJob.runCompaction();
                        } else {
                            if (!finishTrivialMove) {
                                finishTrivialMove = true;
                            } else {
                                break;
                            }
                        }
                    }
                }

                break;
            case BASIC:
                if (compactionDuration != 0) {
                    return;
                }
                createKeyedStateBackend(initializerRootDir);

                mapState = keyedStateBackend.getPartitionedState(
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE,
                        new MapStateDescriptor<>("mapState", Long.class, byte[].class));

            default:
        }

        dummyMaps = new HashMap<>(mapKeyCount);
        for (int i = 0; i < mapKeyCount; ++i) {
            byte[] values = new byte[100];
            random.nextBytes(values);
            dummyMaps.put(mapKeys.get(i), values);
        }
    }

    @Benchmark
    public byte[] mapGet(KeyValue keyValue) throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                return mapState.get(keyValue.mapKey);
            case BASIC:
                if (compactionDuration == 0) {
                    keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                    return mapState.get(keyValue.mapKey);
                }
            default:
                return null;
        }
    }

    @Benchmark
    @OperationsPerInvocation(StateInitializerConstants.mapKeyCount)
    public void mapKeys(KeyValue keyValue, Blackhole bh) throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                for (Long key : mapState.keys()) {
                    bh.consume(key);
                }
                break;
            case BASIC:
                if (compactionDuration == 0) {
                    keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                    for (Long key : mapState.keys()) {
                        bh.consume(key);
                    }
                }
                break;
            default:
        }
    }

    @Benchmark
    @OperationsPerInvocation(StateInitializerConstants.mapKeyCount)
    public void mapValues(KeyValue keyValue, Blackhole bh) throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                for (byte[] value : mapState.values()) {
                    bh.consume(value);
                }
                break;
            case BASIC:
                if (compactionDuration == 0) {
                    keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                    for (byte[] value : mapState.values()) {
                        bh.consume(value);
                    }
                }
                break;
            default:
        }
    }

    @Benchmark
    @OperationsPerInvocation(StateInitializerConstants.mapKeyCount)
    public void mapEntries(KeyValue keyValue, Blackhole bh) throws Exception {
        switch (mode) {
            case FILE_INGESTION:
                keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                Iterable<Map.Entry<Long, byte[]>> iterable = mapState.entries();
                if (iterable != null) {
                    for (Map.Entry<Long, byte[]> entry : mapState.entries()) {
                        bh.consume(entry.getKey());
                        bh.consume(entry.getValue());
                    }
                }
                break;
            case BASIC:
                if (compactionDuration == 0) {
                    keyedStateBackend.setCurrentKey(keyValue.setUpKey);
                    for (Map.Entry<Long, byte[]> entry : mapState.entries()) {
                        bh.consume(entry.getKey());
                        bh.consume(entry.getValue());
                    }
                }
                break;
            default:
        }
    }
}
