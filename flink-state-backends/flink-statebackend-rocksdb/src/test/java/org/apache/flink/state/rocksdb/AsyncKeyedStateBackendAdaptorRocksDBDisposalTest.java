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

package org.apache.flink.state.rocksdb;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces the RocksDB working-directory leak described in FLINK-40037: when a v1 {@link
 * RocksDBKeyedStateBackend} is wrapped in {@link AsyncKeyedStateBackendAdaptor}, disposing the
 * adaptor must delete the wrapped backend's local instance base path. Before FLINK-40037 was fixed,
 * the adaptor's {@code dispose()} was an empty no-op, so {@link RocksDBKeyedStateBackend#dispose()}
 * — the sole caller of {@code cleanInstanceBasePath()} — was never reached, and the directory
 * leaked.
 */
class AsyncKeyedStateBackendAdaptorRocksDBDisposalTest {

    @Test
    void disposeAdaptorDeletesRocksDBInstanceBasePath(@TempDir File tempDir) throws Exception {
        File instanceBasePath = new File(tempDir, "rocksdb-instance");
        RocksDBKeyedStateBackend<String> rocksBackend =
                RocksDBTestUtils.builderForTestDefaults(instanceBasePath, StringSerializer.INSTANCE)
                        .build();

        assertThat(instanceBasePath)
                .as("RocksDB backend should create its instance base path on build")
                .exists()
                .isDirectory();

        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(rocksBackend);

        adaptor.dispose();

        assertThat(instanceBasePath)
                .as(
                        "adaptor.dispose() must delegate to RocksDBKeyedStateBackend.dispose(),"
                                + " which calls cleanInstanceBasePath() and deletes the local"
                                + " working directory (FLINK-40037)")
                .doesNotExist();
    }
}
