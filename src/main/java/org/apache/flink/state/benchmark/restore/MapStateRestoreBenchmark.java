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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.nju.edu.StateInitializerConstants.MB;
import static org.openjdk.jmh.annotations.Mode.Throughput;

@BenchmarkMode(Throughput)
@Warmup(iterations = 30)
@Measurement(iterations = 30)
public class MapStateRestoreBenchmark extends RestoreBenchmarkBase {
    private MapState<Long, byte[]> mapState;
    private Map<Long, byte[]> dummyMaps;

    protected final File initializerRootDir = new File(StateInitializerConstants.getMapStateInitializerRootDir());

    public static void main(String args[]) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + MapStateRestoreBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup(Level.Trial)
    public void setUpPerTrial() throws Exception {
        int mapKeyCount = StateInitializerConstants.mapKeyCount;
        List<Long> mapKeys = StateInitializerConstants.mapKeys;

        createKeyedStateBackend(initializerRootDir);

        mapState = keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE,
                VoidNamespaceSerializer.INSTANCE,
                new MapStateDescriptor<>("mapState", Long.class, byte[].class));

        keyIndex = new AtomicInteger();

        dummyMaps = new HashMap<>(mapKeyCount);
        for (int i = 0; i < mapKeyCount; ++i) {
            byte[] values = new byte[100];
            random.nextBytes(values);
            dummyMaps.put(mapKeys.get(i), values);
        }
    }

    @Benchmark
    public void mapUpdate(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public void mapAdd(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        mapState.put(keyValue.mapKey, keyValue.mapValue);
    }

    @Benchmark
    public void mapPutAll(KeyValue keyValue) throws Exception {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        mapState.putAll(dummyMaps);
    }
}
