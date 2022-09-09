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

import org.apache.flink.util.IOUtils;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

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
            throw new RuntimeException();
        }

        @Override
        public Void parse(InputStream stream) {
            // should never be called
            throw new RuntimeException();
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
    public static class ServerImplBase implements BindableService {

        /** */
        public void tell(byte[] request, StreamObserver<Void> responseObserver) {
            // responseObserver.onCompleted();
            // asyncUnimplementedUnaryCall(METHOD_TELL, responseObserver);

            System.out.println("tell " + Arrays.toString(request));
            responseObserver.onCompleted();
        }

        /** */
        public void ask(byte[] request, StreamObserver<byte[]> responseObserver) {
            // asyncUnimplementedUnaryCall(METHOD_ASK, responseObserver);
            System.out.println("ask " + Arrays.toString(request));
            responseObserver.onNext(new byte[] {1, 2, 3, 4});
            responseObserver.onCompleted();
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

        protected ServerFutureStub build(Channel channel, CallOptions callOptions) {
            return new ServerFutureStub(channel, callOptions);
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
                        }

                        @Override
                        public void onHeaders(Metadata headers) {
                            System.out.println("headers");
                        }

                        @Override
                        public void onReady() {
                            System.out.println("ready");
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
        private final ServerImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(ServerImplBase serviceImpl, int methodId) {
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
