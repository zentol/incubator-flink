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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

class ServerGrpcTest {

    @Test
    void test() throws IOException, ExecutionException, InterruptedException {
        Server server =
                ServerBuilder.forPort(9000)
                        .addService(new ServerGrpc.ServerImplBase())
                        .build()
                        .start();

        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:" + 9000).usePlaintext().build();

        ServerGrpc.ServerFutureStub serverFutureStub = ServerGrpc.newStub(channel);

        // serverFutureStub.tell(new byte[] {1, 2, 3});
        byte[] bytes = serverFutureStub.ask(new byte[] {1}).get();
        System.out.println("resp " + Arrays.toString(bytes));

        channel.shutdownNow();
        server.shutdownNow();
    }
}
