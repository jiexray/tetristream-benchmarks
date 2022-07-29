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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.FileUtils;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.nju.edu.StateInitializerConstants.MB;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@BenchmarkMode(Throughput)
@Warmup(iterations = 30)
@Measurement(iterations = 30)
public class ListStateRestoreBenchmark extends RestoreBenchmarkBase {
    private ListState<Long> listState;
    private List<Long> dummyLists;

    protected final File initializerRootDir = new File(StateInitializerConstants.getListStateInitializerRootDir());

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ListStateRestoreBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUpPerTrial() throws Exception {
        int listValueCount = StateInitializerConstants.listValueCount;

        createKeyedStateBackend(initializerRootDir);

        listState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new ListStateDescriptor<>("listState", Long.class));

        keyIndex = new AtomicInteger();

        java.nio.file.Path[] dbFiles = FileUtils.listDirectory(new File(dbPath, "db").toPath());
        long stateSize = 0L;
        for (java.nio.file.Path file : dbFiles) {
            stateSize += file.toFile().length();
        }
        System.out.println("restored db size: " + stateSize / MB + " MB");

        dummyLists = new ArrayList<>(listValueCount);
        for (int i = 0; i < listValueCount; ++i) {
            dummyLists.add(random.nextLong());
        }
    }

    @Benchmark
    public void listUpdate(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.update(keyValue.listValue);
    }

    @Benchmark
    public void listAdd(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.add(keyValue.randomValue);
    }

    @Benchmark
    public void listAddAll(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        listState.addAll(dummyLists);
    }
}
