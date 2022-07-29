package org.apache.flink.state.benchmark.restore;

import cn.nju.edu.StateInitializerConstants;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.compaction.CompactionScheduler;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.rocksdb.CompactionRunner;
import org.apache.flink.state.benchmark.BackendUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.nju.edu.StateInitializerConstants.MB;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.state.benchmark.BackendUtils.deserializeKeyedStateHandle;
import static org.apache.flink.state.benchmark.BackendUtils.prepareDirectory;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.dbDirName;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.rootDirName;
import static org.openjdk.jmh.annotations.Scope.Thread;

@Fork(value = 3, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"})
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class RestoreBenchmarkBase {
    protected RocksDBKeyedStateBackend<Long> keyedStateBackend;
    protected List<KeyedStateHandle> keyedStateHandles;
    protected List<KeyedStateHandle> tmpKeyedStateHandles;
    protected final static ThreadLocalRandom random = ThreadLocalRandom.current();
    protected final static int maxParallelism = StateInitializerConstants.maxParallelism;

    protected File rootDir;
    protected File dbPath;

    @Param({"512", "2048", "4096", "8192", "16384"})
    protected static long size;

    @Param({"2", "3"})
    protected static int parallelism;

    @Param({"FILE_INGESTION", "BASIC"})
    protected static RestoreMode mode;

    protected void prepareDummyKeyedStateBackends(File initializerRootDir) throws IOException, ClassNotFoundException {
        Preconditions.checkArgument(initializerRootDir.exists(), "State initialization file directory is not exist!");

        rootDir = prepareDirectory(rootDirName, null);
        keyedStateHandles = new ArrayList<>();

        List<KeyGroupRange> keyGroupRanges = StateInitializerConstants.splitKeyGroupRanges(parallelism);

        for (int parallelism = 0; parallelism < keyGroupRanges.size(); parallelism++) {
            String keyedStateHandleFileName = String.format(
                    StateInitializerConstants.keyedStateHandleFileNameFormat,
                    size,
                    RestoreBenchmarkBase.parallelism,
                    parallelism);
            File keyedStateHandleFile = new File(initializerRootDir, keyedStateHandleFileName);
            Preconditions.checkArgument(keyedStateHandleFile.exists(), "KeyedStateHandle file " + keyedStateHandleFileName + " does not exist");
            KeyedStateHandle restoredDummyStateHandle = deserializeKeyedStateHandle(keyedStateHandleFile);

            keyedStateHandles.add(restoredDummyStateHandle);
        }
    }

    protected void createKeyedStateBackend(File initializerRootDir, CompactionScheduler compactionScheduler) throws IOException, ClassNotFoundException {
        prepareDummyKeyedStateBackends(initializerRootDir);

        dbPath = BackendUtils.prepareDirectory(dbDirName, rootDir);
        tmpKeyedStateHandles = new ArrayList<>(keyedStateHandles);
        switch (mode) {
            case BASIC:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackend(rootDir, dbPath, tmpKeyedStateHandles, new KeyGroupRange(0, maxParallelism - 1), maxParallelism);
                break;
            case FILE_INGESTION:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackendEnableFileIngestion(rootDir, dbPath, tmpKeyedStateHandles, new KeyGroupRange(0, maxParallelism - 1), maxParallelism, compactionScheduler);
                break;
        }
    }

    protected void createKeyedStateBackend(File initializerRootDir) throws IOException, ClassNotFoundException {
        createKeyedStateBackend(initializerRootDir, null);
    }

    protected static void doEmptyKeyElimination(CompactionScheduler compactionScheduler) {
        compactionScheduler.finishRestore(() -> true);
        Preconditions.checkArgument(compactionScheduler.getSchedulerState() == CompactionScheduler.SchedulerState.OnKeyElimination);

        CompactionRunner compactionJob = compactionScheduler.pickCompactionFromTask();
        Preconditions.checkArgument(compactionJob == null, "must no compaction job for key elimination");
        Preconditions.checkArgument(compactionScheduler.getSchedulerState() == CompactionScheduler.SchedulerState.Running);
    }

    static AtomicInteger keyIndex;

    private static int getCurrentIndex() {
        int currentIndex = keyIndex.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            keyIndex.set(0);
        }
        return currentIndex;
    }

    @State(Scope.Thread)
    public static class KeyValue {
        @Setup(Level.Invocation)
        public void kvSetup() {
            int currentIndex = getCurrentIndex();
            setUpKey = random.nextInt(setupKeyCount);
            newKey = newKeys.get(currentIndex % newKeyCount);

            mapKey = mapKeys.get(currentIndex % mapKeyCount);
            mapValue = new byte[100];
            listValue = Collections.singletonList(random.nextLong());
            random.nextBytes(mapValue);
            randomValue = random.nextLong();
        }

        int setupKeyCount = StateInitializerConstants.getEstimatedSetupKeyCount(size, parallelism, StateInitializerConstants.StateType.Map);
        int newKeyCount = StateInitializerConstants.newKeyCount;
        List<Long> newKeys = StateInitializerConstants.getNewKeys(size, parallelism, StateInitializerConstants.StateType.Map);
        long setUpKey;
        long newKey;

        int mapKeyCount = StateInitializerConstants.mapKeyCount;
        List<Long> mapKeys = StateInitializerConstants.mapKeys;
        long mapKey;
        byte[] mapValue;
        List<Long> listValue;
        Long randomValue;
    }

    @TearDown(Level.Trial)
    public void tearDownPerTrial() throws IOException {
        if (keyedStateBackend != null) {
            keyedStateBackend.dispose();
        }
        if (rootDir != null && rootDir.exists()) {
            FileUtils.deleteDirectory(rootDir);
        }
    }

    public enum RestoreMode {
        BASIC,
        FILE_INGESTION
    }

    public enum StateType {
        LIST,
        MAP
    }
}
