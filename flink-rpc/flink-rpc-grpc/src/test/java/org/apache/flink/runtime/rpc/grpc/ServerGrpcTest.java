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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;

import org.junit.jupiter.api.Test;

import java.net.BindException;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServerGrpcTest {

    @Test
    void testPortConflictsResolved() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            final RpcService rpcService1 =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000-9000")
                            .withComponentName("test")
                            .createAndStart();

            final RpcService rpcService2 =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000-9000")
                            .withComponentName("test")
                            .createAndStart();

            rpcService1.stopService().get();
            rpcService2.stopService().get();
        }
    }

    @Test
    void testPortConflictsDetected() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            final RpcService rpcService1 =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000-9000")
                            .withComponentName("test")
                            .createAndStart();

            try {
                assertThatThrownBy(
                                () ->
                                        rpcSystem
                                                .remoteServiceBuilder(
                                                        new Configuration(),
                                                        null,
                                                        String.valueOf(rpcService1.getPort()))
                                                .withComponentName("test")
                                                .createAndStart())
                        .isInstanceOf(BindException.class);
            } finally {
                rpcService1.stopService();
            }
        }
    }

    @Test
    void test3() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            final RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000,8001")
                            .withComponentName("test")
                            .createAndStart();

            try {
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
            } finally {
                rpcService.stopService().get();
            }
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
