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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GRpcService implements RpcService {

    private final ScheduledExecutorService mainThread;
    private final Server server;
    private final ServerGrpc.ServerImpl service;

    private final String bindAddress;
    @Nullable private final String externalAddress;
    @Nullable private final Integer externalPort;

    private final Duration rpcTimeout;
    private final ClassLoader flinkClassLoader;
    private final boolean captureAskCallStack;

    public GRpcService(
            Configuration configuration,
            String componentName,
            String bindAddress,
            @Nullable String externalAddress,
            @Nullable Integer bindPort,
            Iterator<Integer> externalPortRange,
            ExecutorService executorService,
            ClassLoader flinkClassLoader)
            throws IOException {
        this.flinkClassLoader = flinkClassLoader;
        this.mainThread =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("flink-grpc-service-" + componentName));

        this.service = new ServerGrpc.ServerImpl(mainThread, flinkClassLoader);

        this.bindAddress = bindAddress;
        this.externalAddress = externalAddress;

        this.rpcTimeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION);
        this.captureAskCallStack = configuration.get(AkkaOptions.CAPTURE_ASK_CALLSTACK);

        final Tuple2<Integer, Server> externalPortAndServer =
                startServerOnOpenPort(bindAddress, bindPort, externalPortRange, service);
        this.externalPort = externalPortAndServer.f0;
        this.server = externalPortAndServer.f1;
    }

    private static Tuple2<Integer, Server> startServerOnOpenPort(
            String bindAddress,
            Integer bindPort,
            Iterator<Integer> externalPortRange,
            ServerGrpc.ServerImpl service)
            throws IOException {
        boolean firstAttempt = true;
        while (firstAttempt || externalPortRange.hasNext()) {
            final int externalPortCandidate = externalPortRange.next();
            final int bindPortCandidate = bindPort != null ? bindPort : externalPortCandidate;

            try {
                final Server serverCandidate =
                        NettyServerBuilder.forAddress(
                                        new InetSocketAddress(bindAddress, bindPortCandidate))
                                .addService(service)
                                .build()
                                .start();

                return Tuple2.of(externalPortCandidate, serverCandidate);
            } catch (IOException e) {
                if ((e.getCause() instanceof BindException)) {
                    // retry with next port
                    firstAttempt = false;
                } else {
                    throw e;
                }
            }
        }
        throw new BindException("Could not start RPC server on any port. " + externalPortRange);
    }

    @Override
    public String getAddress() {
        return externalAddress != null ? externalAddress : bindAddress;
    }

    @Override
    public int getPort() {
        return externalPort != null ? externalPort : server.getPort();
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        String actualAddress = address.substring(0, address.indexOf("@"));
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(actualAddress).usePlaintext().build();

        ServerGrpc.ServerFutureStub serverFutureStub = ServerGrpc.newStub(channel);

        GRpcGateway<?> invocationHandler =
                new GRpcGateway<>(
                        null,
                        address,
                        getAddress(),
                        address.substring(address.indexOf("@") + 1),
                        captureAskCallStack,
                        rpcTimeout,
                        false,
                        true,
                        flinkClassLoader,
                        serverFutureStub);

        @SuppressWarnings("unchecked")
        C rpcServer =
                (C)
                        Proxy.newProxyInstance(
                                GRpcService.class.getClassLoader(),
                                new Class<?>[] {clazz},
                                invocationHandler);

        return CompletableFuture.completedFuture(rpcServer);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        String actualAddress = address.substring(0, address.indexOf("@"));
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget(actualAddress).usePlaintext().build();

        ServerGrpc.ServerFutureStub serverFutureStub = ServerGrpc.newStub(channel);

        GRpcGateway<F> invocationHandler =
                new GRpcGateway<>(
                        fencingToken,
                        address,
                        getAddress(),
                        address.substring(address.indexOf("@") + 1),
                        captureAskCallStack,
                        rpcTimeout,
                        false,
                        true,
                        flinkClassLoader,
                        serverFutureStub);

        @SuppressWarnings("unchecked")
        C rpcServer =
                (C)
                        Proxy.newProxyInstance(
                                GRpcService.class.getClassLoader(),
                                new Class<?>[] {clazz},
                                invocationHandler);

        return CompletableFuture.completedFuture(rpcServer);
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        try {
            GRpcServer gRpcServer =
                    new GRpcServer(
                            mainThread,
                            getAddress() + ":" + getPort() + "@" + rpcEndpoint.getEndpointId(),
                            "localhost",
                            rpcEndpoint);
            service.addTarget(rpcEndpoint);
            return gRpcServer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        service.removeTarget(((GRpcServer) server).getEndpointId());
        server.stop();
    }

    @Override
    public CompletableFuture<Void> stopService() {
        server.shutdownNow();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return new ScheduledExecutorServiceAdapter(mainThread);
    }
}
