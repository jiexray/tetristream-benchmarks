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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cn.nju.edu.StateInitializerConstants.MB;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.dbDirName;

@BenchmarkMode(Mode.AverageTime)
@Fork(value = 3, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"})
public class RestoreBenchmark extends RestoreBenchmarkBase {
    @Param({"LIST", "MAP"})
    private static RestoreBenchmarkBase.StateType stateType;

    private static String tmpDBRootName = "tmpDBs";
    private static File tmpDBRoot;
    private static String tmpRestoreDBPathFormat = "tmpDB_%d";

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RestoreBenchmark.class.getSimpleName() + ".*")
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
    public void setupPerInvocation() throws Exception {
        dbPath = BackendUtils.prepareDirectory(dbDirName, rootDir);
        tmpDBRoot = BackendUtils.prepareDirectory(tmpDBRootName, rootDir);
        tmpKeyedStateHandles = new ArrayList<>();
        CloseableRegistry closeableRegistry = new CloseableRegistry();
        List<File> tmpDbPaths = new ArrayList<>();

        int idx = 1;
        for (KeyedStateHandle stateHandle : keyedStateHandles) {
            Preconditions.checkArgument(stateHandle instanceof IncrementalRemoteKeyedStateHandle);
            IncrementalRemoteKeyedStateHandle remoteStateHandle = (IncrementalRemoteKeyedStateHandle) stateHandle;
            File tmpDbPath = BackendUtils.prepareDirectory(String.format(tmpRestoreDBPathFormat, idx), tmpDBRoot);
            tmpDbPaths.add(tmpDbPath);

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
        }
        closeableRegistry.close();

        for (File tmpDbPath : tmpDbPaths) {
            for (java.nio.file.Path file : FileUtils.listDirectory(tmpDbPath.toPath())) {
                try (FileOutputStream fos = new FileOutputStream(file.getFileName().toFile(), true)) {
                    FileDescriptor fd = fos.getFD();
                    fos.flush();
                    fd.sync();
                }
            }
        }
    }

    // -----------------------------------------------
    // Teardown at each trial and invocation
    // -----------------------------------------------

    @TearDown(Level.Invocation)
    public void tearDownPerInvocation() throws IOException {
        keyedStateBackend.dispose();
        FileUtils.deleteDirectory(dbPath);
        FileUtils.deleteDirectory(tmpDBRoot);
    }

    @Benchmark
    @Warmup(iterations = 5)
    @Measurement(iterations = 5)
    @OutputTimeUnit(MILLISECONDS)
    public void restore() throws Exception {
        switch (mode) {
            case BASIC:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackend(rootDir, dbPath, tmpKeyedStateHandles, new KeyGroupRange(0, maxParallelism - 1), maxParallelism);
                break;
            case FILE_INGESTION:
                keyedStateBackend = BackendUtils.createRocksDBKeyedStateBackendEnableFileIngestion(rootDir, dbPath, tmpKeyedStateHandles, new KeyGroupRange(0, maxParallelism - 1), maxParallelism, null);
                break;
            default:
                throw new Exception("Do not support restore mode: " + mode);
        }
    }
}
