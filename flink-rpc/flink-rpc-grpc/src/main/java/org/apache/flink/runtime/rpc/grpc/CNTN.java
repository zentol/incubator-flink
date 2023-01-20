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

import org.apache.flink.runtime.rpc.messages.RemoteRequestWithID;
import org.apache.flink.runtime.rpc.messages.RemoteResponseWithID;
import org.apache.flink.util.concurrent.FutureUtils;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class CNTN {

    private static final Logger LOG = LoggerFactory.getLogger(CNTN.class);

    private final ClientCall<RemoteRequestWithID, RemoteResponseWithID> call;
    private final Map<Long, CompletableFuture<Object>> pendingResponses;

    public CNTN(
            ClientCall<RemoteRequestWithID, RemoteResponseWithID> call,
            Map<Long, CompletableFuture<Object>> pendingResponses) {
        this.pendingResponses = pendingResponses;
        this.call = call;
        this.call.start(new ClientCall.Listener<RemoteResponseWithID>() {}, new Metadata());
    }

    public void tell(RemoteRequestWithID req) {
        call.sendMessage(req);
    }

    public CompletableFuture<Object> ask(RemoteRequestWithID req) {
        long id = req.getId();

        CompletableFuture<Object> response = new CompletableFuture<>();
        response.thenAccept(
                resp -> LOG.trace("Received ask #{} response {} (RPC={})", id, resp, resp));

        pendingResponses.put(id, response);
        call.sendMessage(req);
        return response;
    }

    public CompletableFuture<Void> closeAsync() {
        try {
            call.halfClose();
        } catch (IllegalStateException ise) {
            // just means the call was already closed/cancelled
        }
        return FutureUtils.combineAll(pendingResponses.values()).thenApply(x -> null);
    }

    void complete(RemoteResponseWithID response) {
        LOG.trace("Receiver response #{} {}", response.getId(), response);
        CompletableFuture<Object> responseFuture = pendingResponses.remove(response.getId());
        if (responseFuture != null) {
            responseFuture.complete(response.getPayload());
        } else {
            System.out.println("Unexpected response with ID " + response.getId());
        }
    }
}
