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

package org.apache.flink.runtime.rpc.grpc.connection;

import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.grpc.GRpcServerSpec;
import org.apache.flink.runtime.rpc.grpc.GRpcService;
import org.apache.flink.runtime.rpc.messages.grpc.Message;
import org.apache.flink.runtime.rpc.messages.grpc.Request;
import org.apache.flink.util.function.BiFunctionWithException;

import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;

import java.util.concurrent.CompletableFuture;

public class ClientConnection extends ClientCall.Listener<Message<?>> implements Connection {

    private final ManagedChannel channel;
    private final ClientCall<Message<?>, Message<?>> call;
    private final Runnable cleanup;
    private final ConnectionHandler connectionHandler;

    public ClientConnection(
            ManagedChannel channel,
            ClientCall<Message<?>, Message<?>> call,
            String localAddress,
            BiFunctionWithException<String, Request, CompletableFuture<?>, RpcConnectionException>
                    invocationHandler,
            Runnable cleanup) {
        this.channel = channel;
        this.call = call;
        this.cleanup = cleanup;
        Metadata metadata = new Metadata();
        metadata.put(GRpcServerSpec.HEADER_CLIENT_ADDRESS, localAddress);
        this.call.start(this, metadata);
        this.call.request(1);

        this.connectionHandler =
                new ConnectionHandler(
                        localAddress, invocationHandler, call::sendMessage, () -> call.request(1));
    }

    @Override
    public void tell(Message<?> message) {
        connectionHandler.tell(message);
    }

    @Override
    public CompletableFuture<Object> ask(Message<?> message) {
        return connectionHandler.ask(message);
    }

    @Override
    public void onMessage(Message message) {
        connectionHandler.handleRequest(message);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        try {
            call.halfClose();
        } catch (IllegalStateException ise) {
            // just means the call was already closed/cancelled
        }
        return connectionHandler
                .closeAsync()
                .thenRun(channel::shutdown)
                .thenRun(
                        () ->
                                GRpcService.awaitShutdown(
                                        channel::awaitTermination, channel::isTerminated))
                .thenRun(cleanup);
    }
}
