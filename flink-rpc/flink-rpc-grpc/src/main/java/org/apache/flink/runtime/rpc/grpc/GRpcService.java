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
import org.apache.flink.runtime.rpc.grpc.connection.ClientConnection;
import org.apache.flink.runtime.rpc.grpc.connection.Connection;
import org.apache.flink.runtime.rpc.grpc.connection.ServerConnection;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.messages.Type;
import org.apache.flink.runtime.rpc.messages.grpc.Message;
import org.apache.flink.runtime.rpc.messages.grpc.Request;
import org.apache.flink.util.Preconditions;
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
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

    @Nullable private final String externalAddress;
    @Nullable private final Integer externalPort;
    private final AtomicLong atomicLong = new AtomicLong();

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
                InProcessServerBuilder.forName(getInternalAddress() + ":" + getPort())
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
        return externalAddress != null ? externalAddress : "";
    }

    public String getInternalAddress() {
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
                return internalConnectAndCreateGateway(
                                rpcServer.getAddress(), getFencingTaken(rpcServer), selfGatewayType)
                        .get();
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

    private static Serializable getFencingTaken(RpcServer rpcServer) {
        final RpcEndpoint rpcEndpoint = ((GRpcServer) rpcServer).getRpcEndpoint();

        return rpcEndpoint instanceof FencedRpcEndpoint
                ? ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken()
                : null;
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        return internalConnectAndCreateGateway(address, null, clazz);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return internalConnectAndCreateGateway(
                address, Preconditions.checkNotNull(fencingToken), clazz);
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
                            .maxInboundMessageSize(1024 * 1024 * 128) // 128 mb
                            .withOption(
                                    ChannelOption.ALLOCATOR, new UnpooledByteBufAllocator(true));
            if (sslContext == null) {
                // need to explicitly opt-in to use plain text
                nettyChannelBuilder.usePlaintext();
            }
            return nettyChannelBuilder;
        };
    }

    private CompletableFuture<Connection> createConnection(
            String address,
            boolean isLocal,
            Function<String, ManagedChannelBuilder<?>> channelBuilder,
            Function<Channel, ClientCall<Message<?>, Message<?>>> callFunction) {
        final String actualAddress = address.substring(0, address.indexOf("@"));

        final String pseudoAddress = actualAddress + (isLocal ? "(local)" : "(remote)");

        final CompletableFuture<Connection> connectionFuture;
        Connection serverConnection = connectionsAsServer.get(pseudoAddress);
        if (serverConnection == null) {
            connectionFuture =
                    connectionsAsClient.computeIfAbsent(
                            pseudoAddress,
                            ignored -> {
                                final ManagedChannel channel =
                                        channelBuilder.apply(actualAddress).build();

                                final CompletableFuture<Void> connectFuture =
                                        new CompletableFuture<>();
                                waitUntilConnectionEstablished(
                                        getScheduledExecutor(),
                                        channel,
                                        channel.getState(true),
                                        address,
                                        connectFuture,
                                        Duration.ZERO);

                                return connectFuture.thenApply(
                                        i ->
                                                new ClientConnection(
                                                        channel,
                                                        callFunction.apply(channel),
                                                        getInternalAddress() + ":" + getPort(),
                                                        this::handleRpcInvocation,
                                                        () ->
                                                                connectionsAsClient.remove(
                                                                        pseudoAddress)));
                            });
        } else {
            connectionFuture = CompletableFuture.completedFuture(serverConnection);
        }

        return connectionFuture;
    }

    private <F extends Serializable, C> CompletableFuture<Connection> internalConnect(
            String address) {

        if (!address.contains("@")) {
            return FutureUtils.completedExceptionally(
                    new IllegalArgumentException("Invalid address: " + address));
        }
        final String actualAddress = address.substring(0, address.indexOf("@"));
        final String target = address.substring(address.indexOf("@") + 1);

        LOG.debug(
                "Connect request for {} at rpc service {}; mapped to {}@{}",
                address,
                getInternalAddress() + ":" + getPort(),
                actualAddress,
                target);

        // check if target runs in this rpc service, and if so use local channel
        final Function<String, ManagedChannelBuilder<?>> channelBuilder;
        final Function<Channel, ClientCall<Message<?>, Message<?>>> callFunction;
        final boolean isLocal;
        if (!AkkaOptions.isForceRpcInvocationSerializationEnabled(configuration)
                && actualAddress.equals(this.getInternalAddress() + ":" + this.getPort())
                && resolveTarget(target).isPresent()) {
            LOG.debug("Creating local connection");
            isLocal = true;
            channelBuilder = InProcessChannelBuilder::forName;
            callFunction = serverSpec::prepareLocalConnection;
        } else {
            LOG.debug("Creating remote connection");
            isLocal = false;
            channelBuilder = forNetty(configuration);
            callFunction = serverSpec::prepareConnection;
        }

        return createConnection(address, isLocal, channelBuilder, callFunction);
    }

    private <F extends Serializable, C> CompletableFuture<C> internalConnectAndCreateGateway(
            String address, @Nullable F fencingToken, Class<C> clazz) {

        final CompletableFuture<Connection> connectionFuture = internalConnect(address);

        final String target = address.substring(address.indexOf("@") + 1);

        return ClassLoadingUtils.guardCompletionWithContextClassLoader(
                connectionFuture.thenApply(
                        conn -> {
                            final GRpcGateway<F> invocationHandler =
                                    new GRpcGateway<>(
                                            fencingToken,
                                            address,
                                            getInternalAddress(),
                                            target,
                                            captureAskCallStack,
                                            rpcTimeout,
                                            true,
                                            flinkClassLoader,
                                            conn,
                                            atomicLong);

                            return createProxy(clazz, invocationHandler);
                        }),
                flinkClassLoader);
    }

    @SuppressWarnings("unchecked")
    private <C> C createProxy(Class<?> clazz, InvocationHandler invocationHandler) {
        return (C)
                Proxy.newProxyInstance(flinkClassLoader, new Class<?>[] {clazz}, invocationHandler);
    }

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
                getInternalAddress() + ":" + getPort() + "@" + rpcEndpoint.getEndpointId();
        LOG.info("Starting RPC server {}.", endpointAddress);
        try {
            GRpcServer gRpcServer =
                    new GRpcServer(
                            endpointAddress, getInternalAddress(), rpcEndpoint, flinkClassLoader);
            targets.put(rpcEndpoint.getEndpointId(), gRpcServer);
            return gRpcServer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        if (shutdownComplete.get()) {
            return;
        }
        LOG.info("Stopping RPC server {}.", server.getAddress());
        GRpcServer grpcServer = (GRpcServer) server;
        server.getTerminationFuture()
                .thenRun(() -> targets.remove(grpcServer.getEndpointId()))
                .thenRun(() -> LOG.info("Stopped RPC server {}.", server.getAddress()));
        internalConnect(server.getAddress())
                .thenAccept(
                        connection ->
                                connection.tell(
                                        new Request(
                                                getFencingTaken(server),
                                                null,
                                                grpcServer.getEndpointId(),
                                                -1,
                                                Type.TELL)));
    }

    private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);
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
                                                                        stopServer(server);
                                                                        return server
                                                                                .getTerminationFuture();
                                                                    })
                                                            .collect(Collectors.toList())))
                            .thenCompose(
                                    ignored -> {
                                        FutureUtils.ConjunctFuture<Void> voidConjunctFuture =
                                                FutureUtils.completeAll(
                                                        connectionsAsClient.values().stream()
                                                                .filter(f -> f.isDone())
                                                                .map(
                                                                        x ->
                                                                                x.thenAccept(
                                                                                                Connection
                                                                                                        ::close)
                                                                                        .handle(
                                                                                                (v,
                                                                                                        e) ->
                                                                                                        null))
                                                                .collect(Collectors.toList()));
                                        connectionsAsServer.values().forEach(Connection::close);
                                        server.shutdown();
                                        localServer.shutdown();
                                        return voidConjunctFuture;
                                    })
                            .thenRunAsync(
                                    () -> {
                                        LOG.debug(
                                                "IsCurrentThreadInterrupted:"
                                                        + Thread.currentThread().isInterrupted());
                                        LOG.debug("Closing channels");
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
                            .thenRun(targets::clear)
                            .thenRun(executorService::shutdown)
                            .thenRun(() -> shutdownComplete.set(true)),
                    terminationFuture.get());

            shutdownStart.complete(null);
        }
        return terminationFuture.get();
    }

    public static void awaitShutdown(
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
        return serverSpec.createService("Server", new Handler());
    }

    private final Map<String, CompletableFuture<Connection>> connectionsAsClient =
            new ConcurrentHashMap<>();
    private final Map<String, Connection> connectionsAsServer = new ConcurrentHashMap<>();

    private class Handler implements ServerCallHandler<Message<?>, Message<?>> {

        @Override
        public ServerCall.Listener<Message<?>> startCall(
                ServerCall<Message<?>, Message<?>> call, Metadata headers) {

            final String clientAddress = headers.get(GRpcServerSpec.HEADER_CLIENT_ADDRESS);
            final ServerConnection serverConnection =
                    new ServerConnection(
                            call,
                            getInternalAddress() + ":" + getPort(),
                            GRpcService.this::handleRpcInvocation,
                            () -> connectionsAsServer.remove(clientAddress));
            // TODO: handle duplicate connections
            connectionsAsServer.put(clientAddress, serverConnection);
            return serverConnection;
        }
    }

    private Optional<GRpcServer> resolveTarget(String target) {
        if (target.endsWith("*")) {
            String targetWithoutWildcard = target.substring(0, target.length() - 1);

            return targets.keySet().stream()
                    .filter(t -> t.startsWith(targetWithoutWildcard))
                    .map(targets::get)
                    .findFirst();
        } else {
            return Optional.ofNullable(targets.get(target));
        }
    }

    /**
     * Handle rpc invocations by looking up the rpc method on the rpc endpoint and calling this
     * method with the provided method arguments. If the method has a return value, it is returned
     * to the sender of the call.
     */
    private CompletableFuture<?> handleRpcInvocation(String target, Request message)
            throws RpcConnectionException {

        final RpcInvocation rpcInvocation = message.getPayload();

        GRpcServer rpcServer = targets.get(target);
        if (rpcServer == null) {
            if (rpcInvocation == null) {
                // shutdown signal; server already shut down
                return CompletableFuture.completedFuture(null);
            }
            // check for wildcard endpoint name
            Optional<GRpcServer> gRpcServer = resolveTarget(target);
            if (gRpcServer.isPresent()) {
                rpcServer = gRpcServer.get();
            } else {
                return FutureUtils.completedExceptionally(
                        new RecipientUnreachableException(
                                "unknown", target, rpcInvocation.toString()));
            }
        }

        return rpcServer.handleRpcInvocation(message.getFencingToken(), message.getPayload());
    }
}
