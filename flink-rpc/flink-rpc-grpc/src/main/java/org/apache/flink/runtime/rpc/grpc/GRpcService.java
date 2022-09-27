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
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RemoteRequestWithID;
import org.apache.flink.runtime.rpc.messages.RemoteResponseWithID;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.BiFunctionWithException;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A gRPC-based {@link RpcService}. Acts as the main server receiving requests which are then
 * dispatched to the correct {@link RpcEndpoint}.
 */
public class GRpcService implements RpcService, BindableService {

    private static final Logger LOG = LoggerFactory.getLogger(GRpcService.class);

    private final Server server;
    private final Server localServer;

    private final String bindAddress;
    @Nullable private final String externalAddress;
    @Nullable private final Integer externalPort;

    private final Duration rpcTimeout;
    private final Configuration configuration;
    private final ScheduledExecutorService executorService;
    private final ClassLoader flinkClassLoader;
    private final boolean captureAskCallStack;
    private final Map<String, GRpcServer> targets = new ConcurrentHashMap<>();
    private final GRpcServerSpec serverSpec;

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
        this.configuration = configuration;
        this.executorService = executorService;
        this.flinkClassLoader = flinkClassLoader;

        this.bindAddress = bindAddress;
        this.externalAddress = externalAddress;

        this.rpcTimeout = configuration.get(AkkaOptions.ASK_TIMEOUT_DURATION);
        this.captureAskCallStack = configuration.get(AkkaOptions.CAPTURE_ASK_CALLSTACK);

        this.serverSpec = new GRpcServerSpec(flinkClassLoader);

        if (bindPort != null) {
            this.server =
                    startServerOnOpenPort(
                            bindAddress,
                            Collections.singleton(bindPort).iterator(),
                            this,
                            configuration);
        } else {
            this.server =
                    startServerOnOpenPort(bindAddress, externalPortRange, this, configuration);
        }
        this.externalPort =
                bindPort != null && externalPortRange.hasNext()
                        ? externalPortRange.next()
                        : this.server.getPort();
        this.localServer =
                InProcessServerBuilder.forName(getAddress() + ":" + getPort())
                        .addService(this)
                        .build()
                        .start();
    }

    private static Server startServerOnOpenPort(
            String bindAddress,
            Iterator<Integer> bindPorts,
            BindableService service,
            Configuration configuration)
            throws IOException {

        final SslContext sslContext;
        try {
            SslContextBuilder nettySslContextBuilder =
                    SSLUtils.createInternalNettySSLContext(configuration, false);
            sslContext =
                    nettySslContextBuilder != null
                            ? GrpcSslContexts.configure(nettySslContextBuilder).build()
                            : null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        while (bindPorts.hasNext()) {
            int bindPort = bindPorts.next();
            try {

                return NettyServerBuilder.forAddress(new InetSocketAddress(bindAddress, bindPort))
                        .addService(service)
                        .withOption(ChannelOption.ALLOCATOR, new UnpooledByteBufAllocator(true))
                        .sslContext(sslContext)
                        .maxInboundMessageSize(1024 * 1024 * 128) // 128 mb
                        .build()
                        .start();
            } catch (IOException e) {
                if (!(e.getCause() instanceof BindException)) {
                    throw e;
                }
                LOG.debug("Could not start RPC server on port {}.", bindPort, e);
            }
        }
        throw new BindException("Could not start RPC server on any port.");
    }

    @Override
    public String getAddress() {
        return externalAddress != null
                ? externalAddress
                : InetAddress.getLoopbackAddress().getHostAddress();
    }

    @Override
    public int getPort() {
        return externalPort != null ? externalPort : server.getPort();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType, RpcServer rpcServer) {
        final RpcEndpoint rpcEndpoint = ((GRpcServer) rpcServer).getRpcEndpoint();
        if (selfGatewayType.isInstance(rpcEndpoint)) {
            try {
                if (rpcEndpoint instanceof FencedRpcEndpoint) {
                    Serializable fencingToken =
                            ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken();
                    return (C)
                            internalConnect(
                                            rpcServer.getAddress(),
                                            fencingToken,
                                            (Class<FencedRpcGateway<Serializable>>) selfGatewayType,
                                            InProcessChannelBuilder::forName,
                                            serverSpec::prepareLocalConnection)
                                    .get();
                } else {
                    return internalConnect(
                                    rpcServer.getAddress(),
                                    null,
                                    selfGatewayType,
                                    InProcessChannelBuilder::forName,
                                    serverSpec::prepareLocalConnection)
                            .get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException(
                    "RpcEndpoint does not implement the RpcGateway interface of type "
                            + selfGatewayType
                            + '.');
        }
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        return internalConnect(
                address, null, clazz, forNetty(configuration), serverSpec::prepareConnection);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return internalConnect(
                address,
                Preconditions.checkNotNull(fencingToken),
                clazz,
                forNetty(configuration),
                serverSpec::prepareConnection);
    }

    private static Function<String, ManagedChannelBuilder<?>> forNetty(
            Configuration configuration) {

        final SslContext sslContext;
        try {
            SslContextBuilder nettySslContextBuilder =
                    SSLUtils.createInternalNettySSLContext(configuration, true);
            sslContext =
                    nettySslContextBuilder != null
                            ? GrpcSslContexts.configure(nettySslContextBuilder).build()
                            : null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return address -> {
            NettyChannelBuilder nettyChannelBuilder =
                    NettyChannelBuilder.forTarget(address)
                            .sslContext(sslContext)
                            .withOption(
                                    ChannelOption.ALLOCATOR, new UnpooledByteBufAllocator(true));
            if (sslContext == null) {
                // need to explicitly opt-in to use plain text
                nettyChannelBuilder.usePlaintext();
            }
            return nettyChannelBuilder;
        };
    }

    private final Map<Integer, ManagedChannel> channelIndex = new ConcurrentHashMap<>();

    private <F extends Serializable, C> CompletableFuture<C> internalConnect(
            String address,
            @Nullable F fencingToken,
            Class<C> clazz,
            Function<String, ManagedChannelBuilder<?>> channelBuilder,
            Function<Channel, ClientCall<RemoteRequestWithID, RemoteResponseWithID>> callFunction) {
        if (!address.contains("@")) {
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException("Invalid address: " + address));
        }
        final String actualAddress = address.substring(0, address.indexOf("@"));

        final int channelId =
                (getAddress() + ":" + getPort()).hashCode() + actualAddress.hashCode();

        final ManagedChannel channel = channelBuilder.apply(actualAddress).build();
        channelIndex.put(channelId, channel);

        final CompletableFuture<Void> connectFuture = new CompletableFuture<>();
        waitUntilConnectionEstablished(
                getScheduledExecutor(),
                channel,
                channel.getState(true),
                address,
                connectFuture,
                Duration.ZERO);

        return ClassLoadingUtils.guardCompletionWithContextClassLoader(
                connectFuture.thenApply(
                        ignored -> {
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
                                            channel,
                                            callFunction);

                            gateways.add(invocationHandler);

                            return createProxy(clazz, invocationHandler);
                        }),
                flinkClassLoader);
    }

    @SuppressWarnings("unchecked")
    private <C> C createProxy(Class<?> clazz, InvocationHandler invocationHandler) {
        return (C)
                Proxy.newProxyInstance(flinkClassLoader, new Class<?>[] {clazz}, invocationHandler);
    }

    private final Collection<GRpcGateway<?>> gateways = new ConcurrentLinkedDeque<>();

    private static void waitUntilConnectionEstablished(
            ScheduledExecutor scheduledExecutor,
            ManagedChannel channel,
            ConnectivityState state,
            String address,
            CompletableFuture<Void> connectFuture,
            Duration delay) {
        if (channel.getState(true) == ConnectivityState.READY) {
            connectFuture.complete(null);
            return;
        }
        scheduledExecutor.schedule(
                () ->
                        channel.notifyWhenStateChanged(
                                state,
                                () -> {
                                    switch (channel.getState(true)) {
                                        case READY:
                                            connectFuture.complete(null);
                                            break;
                                        case TRANSIENT_FAILURE:
                                            channel.shutdownNow();
                                            connectFuture.completeExceptionally(
                                                    new RpcConnectionException(
                                                            "Failed to connect to " + address));
                                            break;
                                        case SHUTDOWN:
                                            connectFuture.completeExceptionally(
                                                    new RpcConnectionException(
                                                            "Channel was shut down unexpectedly."));
                                            break;
                                        case IDLE:
                                        case CONNECTING:
                                            waitUntilConnectionEstablished(
                                                    scheduledExecutor,
                                                    channel,
                                                    state,
                                                    address,
                                                    connectFuture,
                                                    Duration.ofMillis(50));
                                    }
                                }),
                delay.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        final String endpointAddress =
                getAddress() + ":" + getPort() + "@" + rpcEndpoint.getEndpointId();
        LOG.info("Starting RPC server {}.", endpointAddress);
        try {
            GRpcServer gRpcServer =
                    new GRpcServer(endpointAddress, getAddress(), rpcEndpoint, flinkClassLoader);
            targets.put(rpcEndpoint.getEndpointId(), gRpcServer);
            return gRpcServer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        LOG.info("Stopping RPC server {}.", server.getAddress());
        server.stop();
        server.getTerminationFuture()
                .thenRun(() -> targets.remove(((GRpcServer) server).getEndpointId()));
    }

    private final AtomicReference<CompletableFuture<Void>> terminationFuture =
            new AtomicReference<>();

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (terminationFuture.compareAndSet(null, new CompletableFuture<>())) {
            final CompletableFuture<Void> shutdownStart = new CompletableFuture<>();

            FutureUtils.forward(
                    shutdownStart
                            .thenCompose(
                                    ignored ->
                                            FutureUtils.waitForAll(
                                                    targets.values().stream()
                                                            .map(
                                                                    server -> {
                                                                        server.stop();
                                                                        return server
                                                                                .getTerminationFuture();
                                                                    })
                                                            .collect(Collectors.toList())))
                            .thenRun(
                                    () -> {
                                        gateways.forEach(GRpcGateway::close);
                                        channelIndex.values().stream()
                                                .filter(c -> !c.isShutdown() || !c.isTerminated())
                                                .forEach(ManagedChannel::shutdown);
                                        server.shutdown();
                                        localServer.shutdown();
                                    })
                            .thenRunAsync(
                                    () -> {
                                        LOG.debug(
                                                "IsCurrentThreadInterrupted:"
                                                        + Thread.currentThread().isInterrupted());
                                        LOG.debug("Closing channels");
                                        for (ManagedChannel value : channelIndex.values()) {
                                            awaitShutdown(
                                                    value::awaitTermination, value::isTerminated);
                                        }
                                        awaitShutdown(
                                                server::awaitTermination, server::isTerminated);
                                        if (!server.isTerminated()) {
                                            LOG.debug("server did not terminate in time.");
                                            server.shutdownNow();
                                        }
                                        awaitShutdown(
                                                localServer::awaitTermination,
                                                localServer::isTerminated);
                                        LOG.debug(
                                                "Closed channels {}",
                                                Thread.currentThread().isInterrupted());
                                    },
                                    executorService)
                            .thenRun(channelIndex::clear)
                            .thenRun(targets::clear)
                            .thenRun(executorService::shutdown),
                    terminationFuture.get());

            shutdownStart.complete(null);
        }
        return terminationFuture.get();
    }

    static void awaitShutdown(
            BiFunctionWithException<Long, TimeUnit, Boolean, InterruptedException> value,
            Supplier<Boolean> terminatedCheck) {
        LOG.debug("Thread {} closing resource.", Thread.currentThread());
        try {
            value.apply(10L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.info("Thread {} interrupted.", Thread.currentThread(), e);
            Thread.currentThread().interrupt();
        }
        if (!terminatedCheck.get()) {
            LOG.debug("Channel did not shut down.");
        }
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return new ScheduledExecutorServiceAdapter(executorService);
    }

    @Override
    public ServerServiceDefinition bindService() {
        return serverSpec.createService("Server", this::setupConnection);
    }

    private StreamObserver<RemoteRequestWithID> setupConnection(
            StreamObserver<RemoteResponseWithID> responseObserver) {

        return new StreamObserver<RemoteRequestWithID>() {

            private final Object lock = new Object();

            @GuardedBy("lock")
            private boolean isStopped = false;

            @Override
            public void onNext(RemoteRequestWithID request) {
                LOG.trace(
                        "Received {} #{} to '{}:{}@{} (RPC={})",
                        request.getType().name().toLowerCase(Locale.ROOT),
                        request.getId(),
                        getAddress(),
                        getPort(),
                        request.getTarget(),
                        request.getPayload());
                try {
                    switch (request.getType()) {
                        case TELL:
                            handleRpcInvocation(request.getTarget(), request);
                            break;
                        case ASK:
                            CompletableFuture<?> resp =
                                    handleRpcInvocation(request.getTarget(), request);

                            resp.thenAccept(
                                            s -> {
                                                synchronized (lock) {
                                                    if (!isStopped) {
                                                        responseObserver.onNext(
                                                                new RemoteResponseWithID(
                                                                        (Serializable) s,
                                                                        request.getId()));
                                                    } else {
                                                        LOG.debug(
                                                                "Dropping RPC ({}) result because server was stopped.",
                                                                request.getPayload());
                                                    }
                                                }
                                            })
                                    .exceptionally(
                                            e -> {
                                                synchronized (lock) {
                                                    if (e.getCause()
                                                                    instanceof
                                                                    RecipientUnreachableException
                                                            || e.getCause()
                                                                    instanceof
                                                                    RejectedExecutionException) {
                                                        LOG.debug(
                                                                "RPC ({}) failed{}.",
                                                                request.getPayload(),
                                                                isStopped
                                                                        ? " while server was stopped."
                                                                        : "",
                                                                e);
                                                    } else {
                                                        LOG.error(
                                                                "RPC ({}) failed{}.",
                                                                request.getPayload(),
                                                                isStopped
                                                                        ? " while server was stopped."
                                                                        : "",
                                                                e);
                                                    }
                                                    if (!isStopped) {
                                                        responseObserver.onNext(
                                                                new RemoteResponseWithID(
                                                                        new SerializedThrowable(
                                                                                ExceptionUtils
                                                                                        .stripCompletionException(
                                                                                                e)),
                                                                        request.getId()));
                                                    }
                                                    return null;
                                                }
                                            });
                            break;
                    }

                } catch (Exception e) {
                    synchronized (lock) {
                        LOG.error("Fatal RPC failed.", e);
                        responseObserver.onError(e);
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                LOG.debug("Server ({}:{}) side onError", getAddress(), getPort(), t);
                synchronized (lock) {
                    responseObserver.onError(t);
                }
            }

            @Override
            public void onCompleted() {
                LOG.debug("Server ({}:{}) side onCompleted", getAddress(), getPort());
                synchronized (lock) {
                    isStopped = true;
                    responseObserver.onCompleted();
                }
            }
        };
    }

    /**
     * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
     * method with the provided method arguments. If the method has a return value, it is returned
     * to the sender of the call.
     */
    private CompletableFuture<?> handleRpcInvocation(
            String target, RemoteFencedMessage<?, ? extends RpcInvocation> message)
            throws RpcConnectionException {

        final RpcInvocation rpcInvocation = message.getPayload();

        GRpcServer rpcServer = targets.get(target);
        if (rpcServer == null) {
            // check for wildcard endpoint name
            if (target.endsWith("*")) {
                String targetWithoutWildcard = target.substring(0, target.length() - 1);

                Optional<String> first =
                        targets.keySet().stream()
                                .filter(t -> t.startsWith(targetWithoutWildcard))
                                .findFirst();

                if (first.isPresent()) {
                    rpcServer = targets.get(first.get());
                } else {
                    return FutureUtils.completedExceptionally(
                            new RecipientUnreachableException(
                                    "unknown", target, rpcInvocation.toString()));
                }
            } else {
                return FutureUtils.completedExceptionally(
                        new RecipientUnreachableException(
                                "unknown", target, rpcInvocation.toString()));
            }
        }

        return rpcServer.handleRpcInvocation(message.getFencingToken(), message.getPayload());
    }
}
