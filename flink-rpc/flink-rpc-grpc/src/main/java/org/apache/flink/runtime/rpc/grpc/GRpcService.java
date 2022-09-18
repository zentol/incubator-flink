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
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.InstantiationUtil;
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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.rpc.grpc.ClassLoadingUtils.runWithContextClassLoader;

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
    private final Map<String, RpcEndpoint> targets = new ConcurrentHashMap<>();

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
                            rpcEndpoint);
            addTarget(rpcEndpoint);
            return gRpcServer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        removeTarget(((GRpcServer) server).getEndpointId());
        server.stop();
    }

    @Override
    public CompletableFuture<Void> stopService() {
        CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        mainThread.execute(
                () -> {
                    ArrayList<RpcEndpoint> rpcEndpoints = new ArrayList<>(targets.values());
                    targets.clear();
                    FutureUtils.forward(
                            FutureUtils.waitForAll(
                                    rpcEndpoints.stream()
                                            .map(RpcEndpoint::closeAsync)
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
        return GRpcServerSpec.createService("Server", this::tell, this::ask);
    }

    public void addTarget(RpcEndpoint rpcEndpoint) {
        targets.put(rpcEndpoint.getEndpointId(), rpcEndpoint);
    }

    public void removeTarget(String rpcEndpoint) {
        targets.remove(rpcEndpoint);
    }

    private void tell(byte[] request, StreamObserver<Void> responseObserver) {
        try {
            RemoteFencedMessage<?, RpcInvocation> o =
                    InstantiationUtil.deserializeObject(request, flinkClassLoader);

            System.out.println("tell " + o);

            handleRpcInvocation(o);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    private void ask(byte[] request, StreamObserver<byte[]> responseObserver) {
        try {
            RemoteFencedMessage<?, RpcInvocation> o =
                    InstantiationUtil.deserializeObject(request, flinkClassLoader);
            System.out.println("ask " + o);

            CompletableFuture<?> resp = handleRpcInvocation(o);

            resp.thenAccept(
                            s -> {
                                try {
                                    responseObserver.onNext(InstantiationUtil.serializeObject(s));
                                    responseObserver.onCompleted();
                                } catch (IOException e) {
                                    // bruh why do we need to set BOTH?
                                    responseObserver.onError(
                                            new StatusException(Status.INTERNAL.withCause(e))
                                                    .initCause(e));
                                }
                            })
                    .exceptionally(
                            e -> {
                                // bruh why do we need to set BOTH?
                                responseObserver.onError(
                                        new StatusException(Status.INTERNAL.withCause(e))
                                                .initCause(e));
                                return null;
                            });
        } catch (Exception e) {
            // bruh why do we need to set BOTH?
            responseObserver.onError(
                    new StatusException(Status.INTERNAL.withCause(e)).initCause(e));
        }
    }

    /**
     * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
     * method with the provided method arguments. If the method has a return value, it is returned
     * to the sender of the call.
     */
    private CompletableFuture<?> handleRpcInvocation(RemoteFencedMessage<?, RpcInvocation> message)
            throws RpcConnectionException {

        final RpcInvocation rpcInvocation = message.getPayload();

        final RpcEndpoint rpcEndpoint = targets.get(rpcInvocation.getTarget());
        if (rpcEndpoint == null) {
            return FutureUtils.completedExceptionally(
                    new RecipientUnreachableException(
                            "unknown", rpcInvocation.getTarget(), rpcInvocation.toString()));
        }

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            final Serializable expectedFencingToken =
                    ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken();

            if (expectedFencingToken == null) {
                LOG.debug(
                        "Fencing token not set: Ignoring message {} because the fencing token is null.",
                        message);

                return FutureUtils.completedExceptionally(
                        new FencingTokenException(
                                String.format(
                                        "Fencing token not set: Ignoring message %s sent to %s because the fencing token is null.",
                                        message, rpcEndpoint.getAddress())));
            } else {
                final Serializable fencingToken = message.getFencingToken();

                if (!Objects.equals(expectedFencingToken, fencingToken)) {
                    LOG.debug(
                            "Fencing token mismatch: Ignoring message {} because the fencing token {} did "
                                    + "not match the expected fencing token {}.",
                            message,
                            fencingToken,
                            expectedFencingToken);

                    return FutureUtils.completedExceptionally(
                            new FencingTokenException(
                                    "Fencing token mismatch: Ignoring message "
                                            + message
                                            + " because the fencing token "
                                            + fencingToken
                                            + " did not match the expected fencing token "
                                            + expectedFencingToken
                                            + '.'));
                }
            }
        }

        final Method rpcMethod;

        try {
            String methodName = rpcInvocation.getMethodName();
            Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

            rpcMethod = rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
        } catch (final NoSuchMethodException e) {
            LOG.error("Could not find rpc method for rpc invocation.", e);

            throw new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
        }

        try {
            // this supports declaration of anonymous classes
            rpcMethod.setAccessible(true);

            if (rpcMethod.getReturnType().equals(Void.TYPE)) {
                // No return value to send back
                mainThread.execute(
                        () -> {
                            try {
                                runWithContextClassLoader(
                                        () ->
                                                rpcMethod.invoke(
                                                        rpcEndpoint, rpcInvocation.getArgs()),
                                        flinkClassLoader);
                            } catch (ReflectiveOperationException e) {
                                throw new RuntimeException(e);
                            }
                        });
                return CompletableFuture.completedFuture(null);
            } else {
                final CompletableFuture<Object> result = new CompletableFuture<>();
                mainThread.execute(
                        () -> {
                            try {
                                runWithContextClassLoader(
                                        () -> {
                                            Object rpcResult =
                                                    rpcMethod.invoke(
                                                            rpcEndpoint, rpcInvocation.getArgs());
                                            if (rpcResult instanceof CompletableFuture) {
                                                FutureUtils.forward(
                                                        (CompletableFuture<Object>) rpcResult,
                                                        result);
                                            } else {
                                                result.complete(rpcResult);
                                            }
                                        },
                                        flinkClassLoader);
                            } catch (ReflectiveOperationException e) {
                                LOG.debug(
                                        "Reporting back error thrown in remote procedure {}",
                                        rpcMethod,
                                        e);
                                result.completeExceptionally(e);
                            }
                        });

                final String methodName = rpcMethod.getName();
                final boolean isLocalRpcInvocation = rpcMethod.getAnnotation(Local.class) != null;

                return result;
            }
        } catch (Throwable e) {
            LOG.error("Error while executing remote procedure call {}.", rpcMethod, e);
            // tell the sender about the failure
            throw e;
        }
    }
}
