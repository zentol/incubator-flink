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

import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.grpc.Message;
import org.apache.flink.runtime.rpc.messages.grpc.Request;
import org.apache.flink.runtime.rpc.messages.grpc.Response;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

class ConnectionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHandler.class);

    private final String localAddress;
    private final BiFunctionWithException<
                    String, Request, CompletableFuture<?>, RpcConnectionException>
            invocationHandler;
    private final Consumer<Message<?>> messageSender;
    private final Runnable callRequester;

    private final Map<Long, CompletableFuture<Object>> pendingResponses = new ConcurrentHashMap<>();
    private final Object lock = new Object();

    @GuardedBy("lock")
    private boolean isStopped = false;

    public ConnectionHandler(
            String localAddress,
            BiFunctionWithException<String, Request, CompletableFuture<?>, RpcConnectionException>
                    invocationHandler,
            Consumer<Message<?>> messageSender,
            Runnable callRequester) {
        this.localAddress = localAddress;
        this.invocationHandler = invocationHandler;
        this.messageSender = messageSender;
        this.callRequester = callRequester;
    }

    public void tell(Message<?> message) {
        synchronized (lock) {
            messageSender.accept(message);
        }
    }

    public CompletableFuture<Object> ask(Message<?> message) {
        CompletableFuture<Object> responseFuture = new CompletableFuture<>();
        pendingResponses.put(message.getId(), responseFuture);
        tell(message);
        synchronized (lock) {
            callRequester.run();
        }
        return responseFuture;
    }

    void handleRequest(Message<?> message) {
        if (message instanceof Request) {
            handleRequest((Request) message);
        } else if (message instanceof Response) {
            handleResponse((Response) message);
        } else {
            System.out.println("get fuckkkkked");
            throw new RuntimeException("get fucked");
        }
        synchronized (lock) {
            callRequester.run();
        }
    }

    private void handleRequest(Request message) {
        LOG.trace(
                "Received {} request #{} to '{}@{} (RPC={})",
                message.getType().name().toLowerCase(Locale.ROOT),
                message.getId(),
                localAddress,
                message.getTarget(),
                message.getPayload());

        try {
            switch (message.getType()) {
                case TELL:
                    invocationHandler.apply(message.getTarget(), message);
                    break;
                case ASK:
                    CompletableFuture<?> resp =
                            invocationHandler.apply(message.getTarget(), message);

                    resp.thenAccept(
                                    s -> {
                                        synchronized (lock) {
                                            if (!isStopped) {
                                                LOG.trace("Sending response {}.", s);
                                                tell(
                                                        new Response(
                                                                (Serializable) s,
                                                                0,
                                                                message.getId()));
                                            } else {
                                                LOG.debug(
                                                        "Dropping RPC ({}) result because server was stopped.",
                                                        message.getPayload());
                                            }
                                        }
                                    })
                            .exceptionally(
                                    e -> {
                                        synchronized (lock) {
                                            if (e.getCause()
                                                            instanceof RecipientUnreachableException
                                                    || e.getCause()
                                                            instanceof RejectedExecutionException) {
                                                LOG.debug(
                                                        "RPC ({}) failed{}.",
                                                        message.getPayload(),
                                                        isStopped
                                                                ? " while server was stopped."
                                                                : "",
                                                        e);
                                            } else {
                                                LOG.debug(
                                                        "RPC ({}) failed{}.",
                                                        message.getPayload(),
                                                        isStopped
                                                                ? " while server was stopped."
                                                                : "",
                                                        e);
                                            }
                                            if (!isStopped) {
                                                tell(
                                                        new Response(
                                                                new SerializedThrowable(
                                                                        ExceptionUtils
                                                                                .stripCompletionException(
                                                                                        e)),
                                                                1,
                                                                message.getId()));
                                            }
                                            return null;
                                        }
                                    });
                    break;
            }
        } catch (Exception e) {
            LOG.error("Fatal RPC failed.", e);
        }
    }

    private void handleResponse(Response response) {
        LOG.trace(
                "Received response #{} for request #{} to '{} (Response={})",
                response.getId(),
                response.getRequestId(),
                localAddress,
                response.getPayload());

        CompletableFuture<Object> responseFuture = pendingResponses.remove(response.getRequestId());
        if (responseFuture != null) {
            responseFuture.complete(response.getPayload());
        } else {
            System.out.println("Unexpected response with ID " + response.getId());
        }
    }

    public CompletableFuture<Void> closeAsync() {
        return FutureUtils.combineAll(pendingResponses.values())
                .thenApply(x -> null)
                .thenRun(
                        () -> {
                            synchronized (lock) {
                                isStopped = true;
                            }
                        });
         */
        /* option 2: fail remaining futures; generally sound?
         */
        pendingResponses.forEach(
                (key, future) ->
                        future.completeExceptionally(
                                new TimeoutException("rpc service is being closed.")));
        synchronized (lock) {
            isStopped = true;
        }
        return CompletableFuture.completedFuture(null);
        /* option 3: ignore pending responses; hacky?
        synchronized (lock) {
            isStopped = true;
        }
        return CompletableFuture.completedFuture(null);
        */
    }
}
