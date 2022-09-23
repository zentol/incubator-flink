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

package org.apache.flink.runtime.rpc.grpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.core.classloading.SubmoduleClassLoader;
import org.apache.flink.runtime.rpc.CleanupOnCloseRpcSystem;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcSystemLoader;
import org.apache.flink.runtime.rpc.exceptions.RpcLoaderException;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ServiceLoader;
import java.util.UUID;

/**
 * Loader for the gRPC-based RPC system.
 *
 * <p>This loader expects the flink-rpc-grpc jar to be accessible via {@link
 * ClassLoader#getResource(String)}. It will extract the jar into a temporary directory and create a
 * new {@link SubmoduleClassLoader} to load the rpc system from that jar.
 */
public class GRpcSystemLoader implements RpcSystemLoader {

    static final String HINT_USAGE =
            "mvn clean package -pl flink-rpc/flink-rpc-grpc,flink-rpc/flink-rpc-grpc-loader -DskipTests";

    @Override
    public int getLoadPriority() {
        return 0;
    }

    @Override
    public RpcSystem loadRpcSystem(Configuration config) {
        try {
            final ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

            final Path tmpDirectory = Paths.get(ConfigurationUtils.parseTempDirectories(config)[0]);
            Files.createDirectories(tmpDirectory);
            final Path tempFile =
                    Files.createFile(
                            tmpDirectory.resolve("flink-rpc-grpc_" + UUID.randomUUID() + ".jar"));

            final InputStream resourceStream =
                    flinkClassLoader.getResourceAsStream("flink-rpc-grpc.jar");
            if (resourceStream == null) {
                throw new RpcLoaderException(
                        String.format(
                                "gRPC RPC system could not be found. If this happened while running a test in the IDE, "
                                        + "run '%s' on the command-line, "
                                        + "or add a test dependency on the flink-rpc-grpc-loader test-jar.",
                                HINT_USAGE));
            }

            IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));

            final SubmoduleClassLoader submoduleClassLoader =
                    new SubmoduleClassLoader(
                            new URL[] {tempFile.toUri().toURL()}, flinkClassLoader);

            return new CleanupOnCloseRpcSystem(
                    ServiceLoader.load(RpcSystem.class, submoduleClassLoader).iterator().next(),
                    submoduleClassLoader,
                    tempFile);
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize RPC system.", e);
        }
    }
}
