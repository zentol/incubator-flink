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

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

class ServerGrpcTest {

    private static final ClassLoader flinkClassLoader = ServerGrpcTest.class.getClassLoader();

    /*  @Test
    void test() throws IOException, ExecutionException, InterruptedException {
        Server server =
                NettyServerBuilder.forPort(9000)
                        .addService(
                                new ServerGrpc.ServerImpl(
                                        Executors.directExecutor(), new Object(), flinkClassLoader))
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

    @Test
    void test2() throws IOException, ExecutionException, InterruptedException {
        Server server =
                NettyServerBuilder.forPort(9000)
                        .addService(
                                new ServerGrpc.ServerImpl(
                                        Executors.directExecutor(), new Object(), flinkClassLoader))
                        .build()
                        .start();

        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("localhost:" + 9000).usePlaintext().build();

        ServerGrpc.ServerFutureStub serverFutureStub = ServerGrpc.newStub(channel);

        GRpcGateway invocationHandler =
                new GRpcGateway(
                        "localhost:9000",
                        "localhost",
                        address.substring(address.indexOf("@") + 1),
                        true,
                        Duration.ofSeconds(5),
                        false,
                        true,
                        ServerGrpcTest.class.getClassLoader(),
                        serverFutureStub);

        @SuppressWarnings("unchecked")
        TestGateway rpcServer =
                (TestGateway)
                        Proxy.newProxyInstance(
                                flinkClassLoader,
                                Collections.singleton(TestGateway.class).toArray(new Class<?>[1]),
                                invocationHandler);

        System.out.println(rpcServer.getCount().get());
    }*/

    @Test
    void test3() throws Exception {
        final GRpcService rpcService = new GRpcService(flinkClassLoader);

        try (TestEndpoint testEndpoint = new TestEndpoint(rpcService);
                TestEndpoint2 testEndpoint2 = new TestEndpoint2(rpcService)) {
            testEndpoint.start();
            testEndpoint2.start();

            System.out.println(
                    rpcService
                            .connect(testEndpoint.getAddress(), TestGateway.class)
                            .get()
                            .getCount()
                            .get());

            System.out.println(
                    rpcService
                            .connect(testEndpoint2.getAddress(), TestGateway2.class)
                            .get()
                            .getCount2()
                            .get());
        }
    }

    public interface TestGateway extends RpcGateway {
        CompletableFuture<Integer> getCount();
    }

    public static class TestEndpoint extends RpcEndpoint implements TestGateway {

        protected TestEndpoint(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String getAddress() {
            return rpcServer.getAddress();
        }

        @Override
        public String getHostname() {
            return null;
        }

        @Override
        public CompletableFuture<Integer> getCount() {
            return CompletableFuture.completedFuture(4);
        }

        @Override
        protected void onStart() throws Exception {
            System.out.println("start");
        }

        @Override
        protected CompletableFuture<Void> onStop() {
            System.out.println("stop");
            return CompletableFuture.completedFuture(null);
        }
    }

    public interface TestGateway2 extends RpcGateway {
        CompletableFuture<Integer> getCount2();
    }

    public static class TestEndpoint2 extends RpcEndpoint implements TestGateway2 {

        protected TestEndpoint2(RpcService rpcService) {
            super(rpcService);
        }

        @Override
        public String getAddress() {
            return rpcServer.getAddress();
        }

        @Override
        public String getHostname() {
            return null;
        }

        @Override
        public CompletableFuture<Integer> getCount2() {
            return CompletableFuture.completedFuture(2);
        }
    }
}
