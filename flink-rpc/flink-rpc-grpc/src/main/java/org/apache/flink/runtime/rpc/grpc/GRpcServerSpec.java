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
import org.apache.flink.runtime.rpc.messages.RemoteRequestWithID;
import org.apache.flink.runtime.rpc.messages.RemoteResponseWithID;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ServerCalls;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

/** This class contains the gRPC spec for the transport layer. */
class GRpcServerSpec {

    private final MethodDescriptor<RemoteRequestWithID, RemoteResponseWithID>
            methodSetupConnectionLocal;

    private final MethodDescriptor<RemoteRequestWithID, RemoteResponseWithID> methodSetupConnection;

    public GRpcServerSpec(ClassLoader flinkClassLoader) {
        methodSetupConnectionLocal =
                MethodDescriptor.<RemoteRequestWithID, RemoteResponseWithID>newBuilder()
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setFullMethodName(generateFullMethodName("Server", "setupLocalConnection"))
                        .setRequestMarshaller(new LocalObjectMarshaller<>())
                        .setResponseMarshaller(new LocalObjectMarshaller<>())
                        .build();
        methodSetupConnection =
                MethodDescriptor.<RemoteRequestWithID, RemoteResponseWithID>newBuilder()
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setFullMethodName(generateFullMethodName("Server", "setupConnection"))
                        .setRequestMarshaller(new ObjectMarshaller<>(flinkClassLoader))
                        .setResponseMarshaller(new ObjectMarshaller<>(flinkClassLoader))
                        .build();
    }

    public ServerServiceDefinition createService(
            final String serverName,
            final ServerCalls.BidiStreamingMethod<RemoteRequestWithID, RemoteResponseWithID>
                    ltrFunction) {
        return ServerServiceDefinition.builder(
                        ServiceDescriptor.newBuilder(serverName)
                                .addMethod(methodSetupConnection)
                                .addMethod(methodSetupConnectionLocal)
                                .build())
                .addMethod(methodSetupConnection, asyncBidiStreamingCall(ltrFunction))
                .addMethod(methodSetupConnectionLocal, asyncBidiStreamingCall(ltrFunction))
                .build();
    }

    public ClientCall<RemoteRequestWithID, RemoteResponseWithID> prepareLocalConnection(
            Channel channel) {
        return channel.newCall(methodSetupConnectionLocal, CallOptions.DEFAULT);
    }

    public ClientCall<RemoteRequestWithID, RemoteResponseWithID> prepareConnection(
            Channel channel) {
        return channel.newCall(methodSetupConnection, CallOptions.DEFAULT);
    }
}
