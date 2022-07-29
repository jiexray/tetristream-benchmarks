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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
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
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static cn.nju.edu.StateInitializerConstants.MB;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.state.benchmark.BackendUtils.deserializeKeyedStateHandle;
import static org.apache.flink.state.benchmark.BackendUtils.prepareDirectory;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.dbDirName;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.rootDirName;
import static org.openjdk.jmh.annotations.Scope.Thread;

@Fork(value = 1, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"})
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.AverageTime)
public class RestoreOneParallelism {
    @Param({"LIST", "MAP"})
    private static RestoreBenchmarkBase.StateType stateType;
    protected RocksDBKeyedStateBackend<Long> keyedStateBackend;
    protected KeyedStateHandle keyedStateHandle;
    protected List<KeyedStateHandle> tmpKeyedStateHandles;
    protected final static ThreadLocalRandom random = ThreadLocalRandom.current();
    protected final static int maxParallelism = StateInitializerConstants.maxParallelism;

    protected File rootDir;
    protected File dbPath;

    private static String tmpDBRootName = "tmpDBs";
    private static File tmpDBRoot;
    private static String tmpRestoreDBPathFormat = "tmpDB_%d";

    protected KeyGroupRange newKeyGroupRange;

    @Param({"512", "2048", "4096", "8192", "16384"})
    protected static long size;

    @Param({"FILE_INGESTION", "BASIC"})
    protected static RestoreBenchmarkBase.RestoreMode mode;

    @Param({"2", "4"})
    protected static double sliceFactor;

    protected void prepareDummyKeyedStateBackends(File initializerRootDir) throws IOException, ClassNotFoundException {
        Preconditions.checkArgument(initializerRootDir.exists(), "State initialization file directory is not exist!");

        rootDir = prepareDirectory(rootDirName, null);

        String keyedStateHandleFileName = String.format(
                StateInitializerConstants.keyedStateHandleFileNameFormat,
                size,
                1,
                0);
        File keyedStateHandleFile = new File(initializerRootDir, keyedStateHandleFileName);
        Preconditions.checkArgument(keyedStateHandleFile.exists(), "KeyedStateHandle file " + keyedStateHandleFileName + " does not exist");
        keyedStateHandle = deserializeKeyedStateHandle(keyedStateHandleFile);
        System.out.println("KeyGroup " + 0 + ", state handle size: " + keyedStateHandle.getStateSize() / MB + " MB");
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
    public void setupPerInvocation() throws Exception {
        dbPath = prepareDirectory(dbDirName, rootDir);
        tmpDBRoot = prepareDirectory(tmpDBRootName, rootDir);
        tmpKeyedStateHandles = new ArrayList<>();
        CloseableRegistry closeableRegistry = new CloseableRegistry();

        int idx = 1;
        Preconditions.checkArgument(keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle);
        IncrementalRemoteKeyedStateHandle remoteStateHandle = (IncrementalRemoteKeyedStateHandle) keyedStateHandle;
        File tmpDbPath = prepareDirectory(String.format(tmpRestoreDBPathFormat, idx), tmpDBRoot);

        try (RocksDBStateDownloader rocksDBStateDownloader =
                     new RocksDBStateDownloader(1)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    remoteStateHandle, tmpDbPath.toPath(), closeableRegistry);
        }

        tmpKeyedStateHandles.add(new IncrementalLocalKeyedStateHandle(
                remoteStateHandle.getBackendIdentifier(),
                remoteStateHandle.getCheckpointId(),
                new DirectoryStateHandle(tmpDbPath.toPath()),
                remoteStateHandle.getKeyGroupRange(),
                remoteStateHandle.getMetaStateHandle(),
                remoteStateHandle.getSharedState().keySet()));
        closeableRegistry.close();

        int startKeyGroup = random.nextInt((int) ((maxParallelism - 1) * (1.0 / sliceFactor)));
        int endKeyGroup = Math.min(maxParallelism - 1, startKeyGroup + (int) ((maxParallelism - 1) * (1.0 - 1.0 / sliceFactor)));
        newKeyGroupRange = new KeyGroupRange(startKeyGroup, endKeyGroup);
    }

    @TearDown(Level.Invocation)
    public void tearDownPerInvocation() throws IOException {
        keyedStateBackend.dispose();
        FileUtils.deleteDirectory(dbPath);
        FileUtils.deleteDirectory(tmpDBRoot);
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

    @Benchmark
    @Warmup(iterations = 2)
    @Measurement(iterations = 2)
    @OutputTimeUnit(MILLISECONDS)
    public void restore() throws Exception {
        switch (mode) {
            case BASIC:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackend(rootDir, dbPath, tmpKeyedStateHandles, newKeyGroupRange, maxParallelism);
                break;
            case FILE_INGESTION:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackendEnableFileIngestion(rootDir, dbPath, tmpKeyedStateHandles, newKeyGroupRange, maxParallelism, null);
                break;
            default:
                throw new Exception("Do not support restore mode: " + mode);
        }
    }
}
