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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/** A gRPC-based {@link RpcSystem}. */
public class GRpcSystem implements RpcSystem {
    @Override
    public RpcServiceBuilder localServiceBuilder(Configuration configuration) {
        return new GRpcServiceBuilder(configuration);
    }

    @Override
    public RpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return new GRpcServiceBuilder(configuration, externalAddress, externalPortRange);
    }

    @Override
    public String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config) {
        return hostname + ":" + port + "@" + endpointName;
    }

    @Override
    public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) {
        return InetSocketAddress.createUnresolved(
                url.substring(0, url.indexOf(":")),
                Integer.parseInt(url.substring(url.indexOf(":") + 1, url.indexOf("@"))));
    }

    @Override
    public long getMaximumMessageSizeInBytes(Configuration config) {
        return Long.MAX_VALUE;
    }

    private static class GRpcServiceBuilder implements RpcServiceBuilder {

        private final Configuration configuration;
        @Nullable private final String externalAddress;
        private final Iterator<Integer> externalPortRange;

        @Nullable private String componentName;
        private String bindAddress = NetUtils.getWildcardIPAddress();
        @Nullable private Integer bindPort = null;

        @Nullable
        private Function<String, ScheduledExecutorService> scheduledExecutorServiceFactory = null;

        public GRpcServiceBuilder(Configuration configuration) {
            this.configuration = configuration;
            this.bindPort = 0;
            this.externalAddress = null;
            this.externalPortRange = Collections.emptyIterator();
        }

        public GRpcServiceBuilder(
                Configuration configuration,
                @Nullable String externalAddress,
                @Nullable String externalPortRange) {
            this.configuration = configuration;
            this.externalAddress =
                    externalAddress == null
                            ? InetAddress.getLoopbackAddress().getHostAddress()
                            : externalAddress;
            try {
                this.externalPortRange =
                        externalPortRange != null
                                ? NetUtils.getPortRangeFromString(externalPortRange)
                                : Collections.emptyIterator();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid port range definition: " + externalPortRange);
            }
        }

        @Override
        public RpcServiceBuilder withComponentName(String name) {
            this.componentName = Preconditions.checkNotNull(name);
            return this;
        }

        @Override
        public RpcServiceBuilder withBindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        @Override
        public RpcServiceBuilder withBindPort(int bindPort) {
            this.bindPort = bindPort;
            return this;
        }

        @Override
        public RpcServiceBuilder withExecutorConfiguration(
                FixedThreadPoolExecutorConfiguration executorConfiguration) {
            scheduledExecutorServiceFactory =
                    componentName ->
                            Executors.newScheduledThreadPool(
                                    executorConfiguration.getMaxNumThreads(),
                                    new ExecutorThreadFactory.Builder()
                                            .setPoolName("flink-" + componentName)
                                            .setExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                            .setThreadPriority(
                                                    executorConfiguration.getThreadPriority())
                                            .build());
            return this;
        }

        @Override
        public RpcServiceBuilder withExecutorConfiguration(
                ForkJoinExecutorConfiguration executorConfiguration) {

            int parallelism =
                    Math.min(
                            Math.max(
                                    (int)
                                            Math.ceil(
                                                    Runtime.getRuntime().availableProcessors()
                                                            * executorConfiguration
                                                                    .getParallelismFactor()),
                                    executorConfiguration.getMinParallelism()),
                            executorConfiguration.getMaxParallelism());

            scheduledExecutorServiceFactory =
                    componentName ->
                            Executors.newScheduledThreadPool(
                                    parallelism,
                                    new ExecutorThreadFactory.Builder()
                                            .setPoolName("flink-" + componentName)
                                            .setExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                            .build());
            return this;
        }

        @Override
        public RpcService createAndStart() throws IOException {
            if (componentName == null) {
                componentName = "Flink";
            }
            // Preconditions.checkNotNull(componentName);

            if (scheduledExecutorServiceFactory == null) {
                final double parallelismFactor =
                        configuration.getDouble(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR);
                final int minParallelism =
                        configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN);
                final int maxParallelism =
                        configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX);

                withExecutorConfiguration(
                        new RpcSystem.ForkJoinExecutorConfiguration(
                                parallelismFactor, minParallelism, maxParallelism));
            }

            Preconditions.checkArgument(bindPort != null || externalPortRange.hasNext());

            return new GRpcService(
                    configuration,
                    componentName,
                    bindAddress,
                    externalAddress,
                    bindPort,
                    externalPortRange,
                    scheduledExecutorServiceFactory.apply(componentName),
                    GRpcSystem.class.getClassLoader());
        }
    }
}
