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

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class GRpcService implements RpcService {
    @Override
    public String getAddress() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();

        ServerGrpc.ServerFutureStub serverFutureStub = ServerGrpc.newStub(channel);

        GRpcGateway invocationHandler =
                new GRpcGateway(
                        address,
                        "localhost",
                        true,
                        Duration.ofSeconds(5),
                        false,
                        true,
                        GRpcService.class.getClassLoader(),
                        serverFutureStub);

        @SuppressWarnings("unchecked")
        C rpcServer =
                (C)
                        Proxy.newProxyInstance(
                                GRpcService.class.getClassLoader(),
                                new Class<?>[] {clazz},
                                invocationHandler);

        return CompletableFuture.completedFuture(rpcServer);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return null;
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        try {
            return new GRpcServer(rpcEndpoint, GRpcService.class.getClassLoader());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stopServer(RpcServer server) {
        server.stop();
    }

    @Override
    public CompletableFuture<Void> stopService() {
        return null;
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return null;
    }
}
