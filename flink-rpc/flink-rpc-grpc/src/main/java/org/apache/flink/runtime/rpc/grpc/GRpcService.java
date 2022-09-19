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
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.grpc.messages.RemoteRequestWithID;
import org.apache.flink.runtime.rpc.grpc.messages.RemoteResponseWithID;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * A gRPC-based {@link RpcService}. Acts as the main server receiving requests which are then
 * dispatched to the correct {@link RpcEndpoint}. Also provides the main thread.
 */
public class GRpcService implements RpcService, BindableService {

    private static final Logger LOG = LoggerFactory.getLogger(GRpcService.class);

    private final ScheduledExecutorService mainThread;
    private final Server server;

    private final String bindAddress;
    @Nullable private final String externalAddress;
    @Nullable private final Integer externalPort;

    private final Duration rpcTimeout;
    private final ScheduledExecutorService executorService;
    private final ClassLoader flinkClassLoader;
    private final boolean captureAskCallStack;
    private final Map<String, GRpcServer> targets = new ConcurrentHashMap<>();

    public GRpcService(
            Configuration configuration,
            String componentName,
            String bindAddress,
            @Nullable String externalAddress,
            @Nullable Integer bindPort,
            Iterator<Integer> externalPortRange,
            ScheduledExecutorService executorService,
            ClassLoader flinkClassLoader)
            throws IOException {
        this.executorService = executorService;
        this.flinkClassLoader = flinkClassLoader;
        this.mainThread =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory.Builder()
                                .setPoolName("flink-grpc-service-" + componentName)
                                .setExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                .build());

        this.bindAddress = bindAddress;
        this.externalAddress = externalAddress;

        this.rpcTimeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION);
        this.captureAskCallStack = configuration.get(AkkaOptions.CAPTURE_ASK_CALLSTACK);

        if (bindPort != null) {
            this.server =
                    startServerOnOpenPort(
                            bindAddress, Collections.singleton(bindPort).iterator(), this);
        } else {
            this.server = startServerOnOpenPort(bindAddress, externalPortRange, this);
        }
        this.externalPort =
                bindPort != null && externalPortRange.hasNext()
                        ? externalPortRange.next()
                        : this.server.getPort();
    }

    private static Server startServerOnOpenPort(
            String bindAddress, Iterator<Integer> bindPorts, BindableService service)
            throws IOException {

        while (bindPorts.hasNext()) {
            try {
                return NettyServerBuilder.forAddress(
                                new InetSocketAddress(bindAddress, bindPorts.next()))
                        .addService(service)
                        .maxInboundMessageSize(1024 * 1024 * 128) // 128 mb
                        .build()
                        .start();
            } catch (IOException e) {
                if (!(e.getCause() instanceof BindException)) {
                    throw e;
                }
            }
        }
        throw new BindException("Could not start RPC server on any port.");
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
    @SuppressWarnings("unchecked")
    public <C extends RpcGateway> C getSelfGateway(
            Class<C> selfGatewayType, RpcEndpoint rpcEndpoint, RpcServer rpcServer) {
        try {
            if (rpcEndpoint instanceof FencedRpcEndpoint) {
                Serializable fencingToken = ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken();
                return (C)
                        connect(
                                        rpcServer.getAddress(),
                                        fencingToken,
                                        (Class<FencedRpcGateway<Serializable>>) selfGatewayType)
                                .get();
            } else {
                return connect(rpcServer.getAddress(), selfGatewayType).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        return internalConnect(address, null, clazz);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return internalConnect(address, fencingToken, clazz);
    }

    private <F extends Serializable, C> CompletableFuture<C> internalConnect(
            String address, @Nullable F fencingToken, Class<C> clazz) {
        final String actualAddress = address.substring(0, address.indexOf("@"));
        final ManagedChannel channel =
                ManagedChannelBuilder.forTarget(actualAddress).usePlaintext().build();

        final GRpcGateway<F> invocationHandler =
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
                        channel);

        @SuppressWarnings("unchecked")
        final C rpcServer =
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
                            rpcEndpoint,
                            flinkClassLoader);
            targets.put(rpcEndpoint.getEndpointId(), gRpcServer);
            return gRpcServer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        targets.remove(((GRpcServer) server).getEndpointId());
        server.stop();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        mainThread.execute(
                () -> {
                    ArrayList<RpcServer> rpcEndpoints = new ArrayList<>(targets.values());
                    targets.clear();
                    FutureUtils.forward(
                            FutureUtils.waitForAll(
                                    rpcEndpoints.stream()
                                            .map(
                                                    server -> {
                                                        server.stop();
                                                        return server.getTerminationFuture();
                                                    })
                                            .collect(Collectors.toList())),
                            terminationFuture);
                });
        return terminationFuture
                .thenRun(executorService::shutdownNow)
                .thenRun(server::shutdown)
                .thenRun(mainThread::shutdown);
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return new ScheduledExecutorServiceAdapter(executorService);
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GRpcServerSpec.createService("Server", this::setupConnection);
    }

    private StreamObserver<byte[]> setupConnection(StreamObserver<byte[]> responseObserver) {
        return new StreamObserver<byte[]>() {
            @Override
            public void onNext(byte[] bytes) {
                try {
                    RemoteRequestWithID<Serializable> request =
                            InstantiationUtil.deserializeObject(bytes, flinkClassLoader);

                    switch (request.getType()) {
                        case TELL:
                            handleRpcInvocation(request);
                            break;
                        case ASK:
                            CompletableFuture<?> resp = handleRpcInvocation(request);

                            resp.thenAccept(
                                            s -> {
                                                try {
                                                    responseObserver.onNext(
                                                            InstantiationUtil.serializeObject(
                                                                    new RemoteResponseWithID<>(
                                                                            (Serializable) s,
                                                                            request.getId())));
                                                } catch (IOException e) {
                                                    LOG.error(
                                                            "Failed to serialize RPC response.", e);
                                                    responseObserver.onError(
                                                            new StatusException(
                                                                    Status.INTERNAL.withDescription(
                                                                            "Could not serialize response.")));
                                                }
                                            })
                                    .exceptionally(
                                            e -> {
                                                try {
                                                    responseObserver.onNext(
                                                            InstantiationUtil.serializeObject(
                                                                    new RemoteResponseWithID<>(
                                                                            new SerializedThrowable(
                                                                                    ExceptionUtils
                                                                                            .stripCompletionException(
                                                                                                    e)),
                                                                            request.getId())));
                                                } catch (IOException ex) {
                                                    responseObserver.onError(
                                                            new StatusException(
                                                                    Status.INTERNAL.withDescription(
                                                                            "Could not serialize exception.")));
                                                }
                                                return null;
                                            });
                            break;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    /**
     * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
     * method with the provided method arguments. If the method has a return value, it is returned
     * to the sender of the call.
     */
    private CompletableFuture<?> handleRpcInvocation(RemoteFencedMessage<?, RpcInvocation> message)
            throws RpcConnectionException {

        final RpcInvocation rpcInvocation = message.getPayload();

        final GRpcServer rpcServer = targets.get(rpcInvocation.getTarget());
        if (rpcServer == null) {
            return FutureUtils.completedExceptionally(
                    new RecipientUnreachableException(
                            "unknown", rpcInvocation.getTarget(), rpcInvocation.toString()));
        }

        return rpcServer.handleRpcInvocation(message.getFencingToken(), message.getPayload());
    }
}
