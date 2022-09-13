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

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.RemoteFencedMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.concurrent.FutureUtils;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static org.apache.flink.runtime.rpc.grpc.ClassLoadingUtils.runWithContextClassLoader;

/** */
public final class ServerGrpc {

    private ServerGrpc() {}

    private static final MethodDescriptor<byte[], Void> METHOD_TELL =
            MethodDescriptor.<byte[], Void>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "tell"))
                    .setRequestMarshaller(new Serde())
                    .setResponseMarshaller(new FailingSerde())
                    .build();

    private static final MethodDescriptor<byte[], byte[]> METHOD_ASK =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "ask"))
                    .setRequestMarshaller(new Serde())
                    .setResponseMarshaller(new Serde())
                    .build();

    private static final int METHODID_TELL = 0;
    private static final int METHODID_ASK = 1;

    private static final ServiceDescriptor SERVICE_DESCRIPTOR =
            ServiceDescriptor.newBuilder("Server")
                    .addMethod(METHOD_TELL)
                    .addMethod(METHOD_ASK)
                    .build();

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

    public static ServerFutureStub newStub(Channel channel) {
        return new ServerFutureStub(channel, CallOptions.DEFAULT);
    }

    /** TODO: replicate AkkaRpcActor behavior */
    public static class ServerImpl implements BindableService {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private final Executor mainThread;
        private final Map<String, RpcEndpoint> targets = new ConcurrentHashMap<>();

        private final ClassLoader flinkClassLoader;

        public ServerImpl(Executor executor, ClassLoader flinkClassLoader) {
            this.mainThread = executor;
            this.flinkClassLoader = flinkClassLoader;
        }

        public void addTarget(RpcEndpoint rpcEndpoint) {
            targets.put(rpcEndpoint.getEndpointId(), rpcEndpoint);
        }

        public void removeTarget(String rpcEndpoint) {
            targets.remove(rpcEndpoint);
        }

        public void tell(byte[] request, StreamObserver<Void> responseObserver) {
            try {
                RemoteFencedMessage<?, RpcInvocation> o =
                        InstantiationUtil.deserializeObject(
                                request, ServerGrpc.class.getClassLoader());

                System.out.println("tell " + o);

                handleRpcInvocation(o);
            } catch (Exception e) {
                responseObserver.onError(e);
            }
            responseObserver.onCompleted();
        }

        public void ask(byte[] request, StreamObserver<byte[]> responseObserver) {
            try {
                RemoteFencedMessage<?, RpcInvocation> o =
                        InstantiationUtil.deserializeObject(
                                request, ServerGrpc.class.getClassLoader());
                System.out.println("ask " + o);

                CompletableFuture<?> resp = handleRpcInvocation(o);

                resp.thenAccept(
                                s -> {
                                    try {
                                        responseObserver.onNext(
                                                InstantiationUtil.serializeObject(s));
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
         * method with the provided method arguments. If the method has a return value, it is
         * returned to the sender of the call.
         */
        private CompletableFuture<?> handleRpcInvocation(
                RemoteFencedMessage<?, RpcInvocation> message) throws RpcConnectionException {

            final RpcInvocation rpcInvocation = message.getPayload();

            final RpcEndpoint rpcEndpoint = targets.get(rpcInvocation.getTarget());

            if (rpcEndpoint instanceof FencedRpcEndpoint) {
                final Serializable expectedFencingToken =
                        ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken();

                if (expectedFencingToken == null) {
                    log.debug(
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
                        log.debug(
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

                rpcMethod = lookupRpcMethod(methodName, parameterTypes, rpcEndpoint);
            } catch (final NoSuchMethodException e) {
                log.error("Could not find rpc method for rpc invocation.", e);

                throw new RpcConnectionException(
                        "Could not find rpc method for rpc invocation.", e);
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
                                                                rpcEndpoint,
                                                                rpcInvocation.getArgs());
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
                                    log.debug(
                                            "Reporting back error thrown in remote procedure {}",
                                            rpcMethod,
                                            e);
                                    result.completeExceptionally(e);
                                }
                            });

                    final String methodName = rpcMethod.getName();
                    final boolean isLocalRpcInvocation =
                            rpcMethod.getAnnotation(Local.class) != null;

                    return result;
                }
            } catch (Throwable e) {
                log.error("Error while executing remote procedure call {}.", rpcMethod, e);
                // tell the sender about the failure
                throw e;
            }
        }

        /**
         * Look up the rpc method on the given {@link RpcEndpoint} instance.
         *
         * @param methodName Name of the method
         * @param parameterTypes Parameter types of the method
         * @return Method of the rpc endpoint
         * @throws NoSuchMethodException Thrown if the method with the given name and parameter
         *     types cannot be found at the rpc endpoint
         */
        private Method lookupRpcMethod(
                final String methodName, final Class<?>[] parameterTypes, Object rpcEndpoint)
                throws NoSuchMethodException {
            return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
        }

        @Override
        public final ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder(SERVICE_DESCRIPTOR)
                    .addMethod(
                            METHOD_TELL, asyncUnaryCall(new MethodHandlers<>(this, METHODID_TELL)))
                    .addMethod(METHOD_ASK, asyncUnaryCall(new MethodHandlers<>(this, METHODID_ASK)))
                    .build();
        }
    }

    public static final class ServerFutureStub {
        private final Channel channel;
        private final CallOptions callOptions;

        private ServerFutureStub(Channel channel, CallOptions callOptions) {
            this.channel = channel;
            this.callOptions = callOptions;
        }

        public void tell(byte[] request) {
            ClientCall<byte[], Void> call = channel.newCall(METHOD_TELL, callOptions);

            call.start(
                    new ClientCall.Listener<Void>() {
                        @Override
                        public void onClose(Status status, Metadata trailers) {
                            // TODO: handle errors
                        }
                    },
                    new Metadata());

            call.sendMessage(request);
            call.halfClose();
        }

        public CompletableFuture<byte[]> ask(byte[] request) {
            ClientCall<byte[], byte[]> call = channel.newCall(METHOD_ASK, callOptions);

            CompletableFuture<byte[]> response = new CompletableFuture<>();
            call.start(
                    new ClientCall.Listener<byte[]>() {
                        @Override
                        public void onMessage(byte[] message) {
                            System.out.println("onMessage");
                            response.complete(message);
                        }

                        @Override
                        public void onClose(Status status, Metadata trailers) {
                            // TODO: handle errors
                            System.out.println("onClose: " + status);

                            if (!status.isOk()) {
                                response.completeExceptionally(status.asException());
                            }
                        }
                    },
                    new Metadata());

            call.request(1);
            call.sendMessage(request);
            call.halfClose();

            return response;
        }
    }

    private static final class MethodHandlers<Req, Resp>
            implements ServerCalls.UnaryMethod<Req, Resp> {
        private final ServerImpl serviceImpl;
        private final int methodId;

        MethodHandlers(ServerImpl serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void invoke(Req request, StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_TELL:
                    serviceImpl.tell((byte[]) request, (StreamObserver<Void>) responseObserver);
                    break;
                case METHODID_ASK:
                    serviceImpl.ask((byte[]) request, (StreamObserver<byte[]>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }
    }
}
