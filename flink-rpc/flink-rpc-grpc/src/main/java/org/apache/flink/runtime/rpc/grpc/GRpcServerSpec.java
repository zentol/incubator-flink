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

import org.apache.flink.runtime.rpc.grpc.marshalling.LocalObjectMarshaller;
import org.apache.flink.runtime.rpc.grpc.marshalling.ObjectMarshaller;
import org.apache.flink.runtime.rpc.messages.grpc.Message;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/** This class contains the gRPC spec for the transport layer. */
public class GRpcServerSpec {

    public static final Metadata.Key<String> HEADER_CLIENT_ADDRESS =
            Metadata.Key.of("sender", Metadata.ASCII_STRING_MARSHALLER);

    private final MethodDescriptor<Message<?>, Message<?>> methodSetupConnectionLocal;

    private final MethodDescriptor<Message<?>, Message<?>> methodSetupConnection;

    public GRpcServerSpec(ClassLoader flinkClassLoader) {
        methodSetupConnectionLocal =
                MethodDescriptor.<Message<?>, Message<?>>newBuilder()
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setFullMethodName(generateFullMethodName("Server", "setupLocalConnection"))
                        .setRequestMarshaller(new LocalObjectMarshaller<>())
                        .setResponseMarshaller(new LocalObjectMarshaller<>())
                        .build();
        methodSetupConnection =
                MethodDescriptor.<Message<?>, Message<?>>newBuilder()
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setFullMethodName(generateFullMethodName("Server", "setupConnection"))
                        .setRequestMarshaller(new ObjectMarshaller<>(flinkClassLoader))
                        .setResponseMarshaller(new ObjectMarshaller<>(flinkClassLoader))
                        .build();
    }

    public ServerServiceDefinition createService(
            final String serverName, final ServerCallHandler<Message<?>, Message<?>> ltrFunction) {
        return ServerServiceDefinition.builder(
                        ServiceDescriptor.newBuilder(serverName)
                                .addMethod(methodSetupConnection)
                                .addMethod(methodSetupConnectionLocal)
                                .build())
                .addMethod(methodSetupConnection, ltrFunction)
                .addMethod(methodSetupConnectionLocal, ltrFunction)
                .build();
    }

    public ClientCall<Message<?>, Message<?>> prepareLocalConnection(Channel channel) {
        return channel.newCall(methodSetupConnectionLocal, CallOptions.DEFAULT);
    }

    public ClientCall<Message<?>, Message<?>> prepareConnection(Channel channel) {
        return channel.newCall(methodSetupConnection, CallOptions.DEFAULT);
    }
}
