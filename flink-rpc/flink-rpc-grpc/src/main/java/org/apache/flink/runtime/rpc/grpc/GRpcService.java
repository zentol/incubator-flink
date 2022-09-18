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
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
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

        final Tuple2<Integer, Server> externalPortAndServer =
                startServerOnOpenPort(bindAddress, bindPort, externalPortRange, this);
        this.externalPort = externalPortAndServer.f0;
        this.server = externalPortAndServer.f1;
    }

    private static Tuple2<Integer, Server> startServerOnOpenPort(
            String bindAddress,
            Integer bindPort,
            Iterator<Integer> externalPortRange,
            BindableService service)
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
        executorService.shutdownNow();
        server.shutdownNow();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return new ScheduledExecutorServiceAdapter(executorService);
    }

    static final MethodDescriptor<byte[], Void> METHOD_TELL =
            MethodDescriptor.<byte[], Void>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "tell"))
                    .setRequestMarshaller(new Serde())
                    .setResponseMarshaller(new FailingSerde())
                    .build();

    static final MethodDescriptor<byte[], byte[]> METHOD_ASK =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "ask"))
                    .setRequestMarshaller(new Serde())
                    .setResponseMarshaller(new Serde())
                    .build();

    @Override
    public ServerServiceDefinition bindService() {
        return ServerServiceDefinition.builder(
                        ServiceDescriptor.newBuilder("Server")
                                .addMethod(METHOD_TELL)
                                .addMethod(METHOD_ASK)
                                .build())
                .addMethod(METHOD_TELL, asyncUnaryCall(this::tell))
                .addMethod(METHOD_ASK, asyncUnaryCall(this::ask))
                .build();
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
        } catch (Exception e) {
            responseObserver.onError(e);
        }
        responseObserver.onCompleted();
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
                                } catch (IOException e) {
                                    // bruh why do we need to set BOTH?
                                    responseObserver.onError(
                                            new StatusException(Status.INTERNAL.withCause(e))
                                                    .initCause(e));
                                }
                                responseObserver.onCompleted();
                            })
                    .exceptionally(
                            e -> {
                                // bruh why do we need to set BOTH?
                                responseObserver.onError(
                                        new StatusException(Status.INTERNAL.withCause(e))
                                                .initCause(e));
                                responseObserver.onCompleted();
                                return null;
                            });
        } catch (Exception e) {
            // bruh why do we need to set BOTH?
            responseObserver.onError(
                    new StatusException(Status.INTERNAL.withCause(e)).initCause(e));
            responseObserver.onCompleted();
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

    private static class FailingSerde implements MethodDescriptor.Marshaller<Void> {

        @Override
        public InputStream stream(Void value) {
            // should never be called
            throw new RuntimeException("Serialization should never be attempted for this call.");
        }

        @Override
        public Void parse(InputStream stream) {
            // should never be called
            throw new RuntimeException("Serialization should never be attempted for this call.");
        }
    }

    private static class Serde implements MethodDescriptor.Marshaller<byte[]> {

        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                byte[] bytes = new byte[stream.available()];
                IOUtils.readFully(stream, bytes, 0, stream.available());
                return bytes;
            } catch (IOException e) {
                throw new RuntimeException();
            }
        }
    }
}
