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

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GRpcServer implements RpcServer {

    private final ScheduledExecutorService mainThread;
    private final Server server;

    public GRpcServer(RpcEndpoint endpoint, ClassLoader flinkClassLoader) throws IOException {
        this.mainThread =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("flink-rpc-server-" + endpoint.getEndpointId()));

        this.server =
                NettyServerBuilder.forPort(0)
                        .addService(
                                new ServerGrpc.ServerImpl(mainThread, endpoint, flinkClassLoader))
                        .build()
                        .start();
    }

    @Override
    public String getAddress() {
        return "localhost:" + server.getPort();
    }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public void runAsync(Runnable runnable) {
        mainThread.submit(runnable);
    }

    @Override
    public <V> CompletableFuture<V> callAsync(Callable<V> callable, Duration callTimeout) {
        return FutureUtils.orTimeout(
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return callable.call();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        mainThread),
                callTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void scheduleRunAsync(Runnable runnable, long delay) {
        mainThread.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return null;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
        mainThread.shutdownNow();
        server.shutdownNow();
    }
}
