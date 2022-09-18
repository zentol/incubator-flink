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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ServerCalls;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;

/** This class contains the gRPC spec for the transport layer. */
public class GRpcServerSpec {
    private static final MethodDescriptor<byte[], Void> METHOD_TELL =
            MethodDescriptor.<byte[], Void>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "tell"))
                    .setRequestMarshaller(new ByteArrayMarshaller())
                    .setResponseMarshaller(new FailingMarshaller())
                    .build();

    private static final MethodDescriptor<byte[], byte[]> METHOD_ASK =
            MethodDescriptor.<byte[], byte[]>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(generateFullMethodName("Server", "ask"))
                    .setRequestMarshaller(new ByteArrayMarshaller())
                    .setResponseMarshaller(new ByteArrayMarshaller())
                    .build();

    static ServerServiceDefinition createService(
            final String serverName,
            final ServerCalls.UnaryMethod<byte[], Void> tellFunction,
            final ServerCalls.UnaryMethod<byte[], byte[]> askFunction) {
        return ServerServiceDefinition.builder(
                        ServiceDescriptor.newBuilder(serverName)
                                .addMethod(METHOD_TELL)
                                .addMethod(METHOD_ASK)
                                .build())
                .addMethod(METHOD_TELL, asyncUnaryCall(tellFunction))
                .addMethod(METHOD_ASK, asyncUnaryCall(askFunction))
                .build();
    }

    static ClientCall<byte[], Void> prepareTell(Channel channel) {
        return channel.newCall(GRpcServerSpec.METHOD_TELL, CallOptions.DEFAULT);
    }

    static ClientCall<byte[], byte[]> prepareAsk(Channel channel) {
        return channel.newCall(GRpcServerSpec.METHOD_ASK, CallOptions.DEFAULT);
    }
}
