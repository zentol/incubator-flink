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

import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GRpcServer implements RpcServer {

    private final ScheduledExecutorService mainThread;

    private final MainThreadValidatorUtil mainThreadValidator;
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private final String address;
    private final String hostName;
    private final RpcEndpoint rpcEndpoint;

    public GRpcServer(
            ScheduledExecutorService mainThread,
            String address,
            String hostName,
            RpcEndpoint rpcEndpoint)
            throws IOException {
        this.mainThread = mainThread;
        this.address = address;
        this.hostName = hostName;
        this.rpcEndpoint = rpcEndpoint;
        this.mainThreadValidator = new MainThreadValidatorUtil(rpcEndpoint);
        mainThread.submit(mainThreadValidator::enterMainThread);
    }

    public String getEndpointId() {
        return rpcEndpoint.getEndpointId();
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostName;
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
        return terminationFuture;
    }

    @Override
    public void start() {
        mainThread.submit(
                () -> {
                    try {
                        rpcEndpoint.internalCallOnStart();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void stop() {
        mainThread.submit(
                () -> FutureUtils.forward(rpcEndpoint.internalCallOnStop(), terminationFuture));
    }
}
