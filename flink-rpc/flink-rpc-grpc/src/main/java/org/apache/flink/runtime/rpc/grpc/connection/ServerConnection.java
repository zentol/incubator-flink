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
import org.apache.flink.runtime.rpc.messages.grpc.Message;
import org.apache.flink.runtime.rpc.messages.grpc.Request;
import org.apache.flink.util.function.BiFunctionWithException;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.Status;

import java.util.concurrent.CompletableFuture;

public class ServerConnection extends ServerCall.Listener<Message<?>> implements Connection {

    private final ServerCall<Message<?>, Message<?>> call;
    private final ConnectionHandler connectionHandler;
    private final Runnable cleanup;

    public ServerConnection(
            ServerCall<Message<?>, Message<?>> call,
            String localAddress,
            BiFunctionWithException<String, Request, CompletableFuture<?>, RpcConnectionException>
                    invocationHandler,
            Runnable cleanup) {
        this.call = call;
        this.cleanup = cleanup;
        this.call.sendHeaders(new Metadata());
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
    public void onMessage(Message<?> message) {
        connectionHandler.handleRequest(message);
    }

    @Override
    public void onHalfClose() {
        close();
    }

    @Override
    public void close() {
        try {
            call.close(Status.OK, new Metadata());
        } catch (IllegalStateException ise) {
            // just means the call was already closed/cancelled
        }
        connectionHandler.close();
        cleanup.run();
    }
}
