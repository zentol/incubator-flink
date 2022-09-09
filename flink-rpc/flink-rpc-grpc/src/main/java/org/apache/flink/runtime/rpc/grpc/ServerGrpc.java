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

import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/** */
public final class ServerGrpc {

    private ServerGrpc() {}

    public static final String SERVICE_NAME = "Server";

    // Static method descriptors that strictly reflect the proto.
    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<ServerOuterClass.Payload, ServerOuterClass.Empty>
            METHOD_TELL =
                    MethodDescriptor.<ServerOuterClass.Payload, ServerOuterClass.Empty>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName("Server", "tell"))
                            .setRequestMarshaller(
                                    ProtoUtils.marshaller(
                                            ServerOuterClass.Payload.getDefaultInstance()))
                            .setResponseMarshaller(
                                    ProtoUtils.marshaller(
                                            ServerOuterClass.Empty.getDefaultInstance()))
                            .build();

    @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
    public static final io.grpc.MethodDescriptor<ServerOuterClass.Payload, ServerOuterClass.Payload>
            METHOD_ASK =
                    MethodDescriptor
                            .<ServerOuterClass.Payload, ServerOuterClass.Payload>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName("Server", "ask"))
                            .setRequestMarshaller(
                                    ProtoUtils.marshaller(
                                            ServerOuterClass.Payload.getDefaultInstance()))
                            .setResponseMarshaller(
                                    ProtoUtils.marshaller(
                                            ServerOuterClass.Payload.getDefaultInstance()))
                            .build();

    /** Creates a new async stub that supports all call types for the service */
    public static ServerStub newStub(io.grpc.Channel channel) {
        return new ServerStub(channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the
     * service
     */
    public static ServerBlockingStub newBlockingStub(io.grpc.Channel channel) {
        return new ServerBlockingStub(channel);
    }

    /** Creates a new ListenableFuture-style stub that supports unary calls on the service */
    public static ServerFutureStub newFutureStub(io.grpc.Channel channel) {
        return new ServerFutureStub(channel);
    }

    /** */
    public abstract static class ServerImplBase implements io.grpc.BindableService {

        /** */
        public void tell(
                ServerOuterClass.Payload request,
                StreamObserver<ServerOuterClass.Empty> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_TELL, responseObserver);
        }

        /** */
        public void ask(
                ServerOuterClass.Payload request,
                io.grpc.stub.StreamObserver<ServerOuterClass.Payload> responseObserver) {
            asyncUnimplementedUnaryCall(METHOD_ASK, responseObserver);
        }

        @Override
        public final ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder(getServiceDescriptor())
                    .addMethod(
                            METHOD_TELL, asyncUnaryCall(new MethodHandlers<>(this, METHODID_TELL)))
                    .addMethod(METHOD_ASK, asyncUnaryCall(new MethodHandlers<>(this, METHODID_ASK)))
                    .build();
        }
    }

    /** */
    public static final class ServerStub extends io.grpc.stub.AbstractStub<ServerStub> {
        private ServerStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ServerStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected ServerStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new ServerStub(channel, callOptions);
        }

        /** */
        public void tell(
                ServerOuterClass.Payload request,
                StreamObserver<ServerOuterClass.Empty> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_TELL, getCallOptions()), request, responseObserver);
        }

        /** */
        public void ask(
                ServerOuterClass.Payload request,
                io.grpc.stub.StreamObserver<ServerOuterClass.Payload> responseObserver) {
            asyncUnaryCall(
                    getChannel().newCall(METHOD_ASK, getCallOptions()), request, responseObserver);
        }
    }

    /** */
    public static final class ServerBlockingStub
            extends io.grpc.stub.AbstractStub<ServerBlockingStub> {
        private ServerBlockingStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ServerBlockingStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected ServerBlockingStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new ServerBlockingStub(channel, callOptions);
        }

        /** */
        public ServerOuterClass.Empty tell(ServerOuterClass.Payload request) {
            return blockingUnaryCall(getChannel(), METHOD_TELL, getCallOptions(), request);
        }

        /** */
        public ServerOuterClass.Payload ask(ServerOuterClass.Payload request) {
            return blockingUnaryCall(getChannel(), METHOD_ASK, getCallOptions(), request);
        }
    }

    /** */
    public static final class ServerFutureStub extends io.grpc.stub.AbstractStub<ServerFutureStub> {
        private ServerFutureStub(io.grpc.Channel channel) {
            super(channel);
        }

        private ServerFutureStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected ServerFutureStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new ServerFutureStub(channel, callOptions);
        }

        /** */
        public com.google.common.util.concurrent.ListenableFuture<ServerOuterClass.Empty> tell(
                ServerOuterClass.Payload request) {
            return futureUnaryCall(getChannel().newCall(METHOD_TELL, getCallOptions()), request);
        }

        /** */
        public com.google.common.util.concurrent.ListenableFuture<ServerOuterClass.Payload> ask(
                ServerOuterClass.Payload request) {
            return futureUnaryCall(getChannel().newCall(METHOD_ASK, getCallOptions()), request);
        }
    }

    private static final int METHODID_TELL = 0;
    private static final int METHODID_ASK = 1;

    private static final class MethodHandlers<Req, Resp>
            implements io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
                    io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
                    io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
                    io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final ServerImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(ServerImplBase serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_TELL:
                    serviceImpl.tell(
                            (ServerOuterClass.Payload) request,
                            (StreamObserver<ServerOuterClass.Empty>) responseObserver);
                    break;
                case METHODID_ASK:
                    serviceImpl.ask(
                            (ServerOuterClass.Payload) request,
                            (io.grpc.stub.StreamObserver<ServerOuterClass.Payload>)
                                    responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
                io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                default:
                    throw new AssertionError();
            }
        }
    }

    private static final class ServerDescriptorSupplier
            implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
        @Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return ServerOuterClass.getDescriptor();
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (ServerGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor =
                            result =
                                    io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                                            .setSchemaDescriptor(new ServerDescriptorSupplier())
                                            .addMethod(METHOD_TELL)
                                            .addMethod(METHOD_ASK)
                                            .build();
                }
            }
        }
        return result;
    }
}
