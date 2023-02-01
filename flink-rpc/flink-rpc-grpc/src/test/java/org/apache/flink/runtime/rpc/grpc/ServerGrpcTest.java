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

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.net.BindException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class ServerGrpcTest {

    @Test
    void testSelfGateway() throws Exception {
        Configuration configuration = new Configuration();
        // we explicitly test that messages on self gateways do not go through serialization
        configuration.set(AkkaOptions.FORCE_RPC_INVOCATION_SERIALIZATION, false);

        try (RpcSystem rpcSystem = new GRpcSystem()) {
            final RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(configuration, null, "8000,8001")
                            .withComponentName("test")
                            .createAndStart();

            try {
                final Serializable object = new Serializable() {};
                try (TestEndpoint3 testEndpoint = new TestEndpoint3(rpcService, object)) {
                    testEndpoint.start();

                    final TestGateway3 selfGateway =
                            testEndpoint.getSelfGateway(TestGateway3.class);

                    assertThat(selfGateway.getCount2().get()).isSameAs(object);
                }
            } finally {
                rpcService.closeAsync().get();
            }
        }
    }

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

            rpcService1.closeAsync().get();
            rpcService2.closeAsync().get();
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
                rpcService1.closeAsync();
            }
        }
    }

    @Test
    void testErrorsReturned() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            final RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000,8001")
                            .withComponentName("test")
                            .createAndStart();

            try {
                try (FailingEndpoint testEndpoint = new FailingEndpoint(rpcService)) {
                    testEndpoint.start();

                    assertThatThrownBy(
                                    () ->
                                            rpcService
                                                    .connect(
                                                            testEndpoint.getAddress(),
                                                            TestGateway.class)
                                                    .get()
                                                    .getCount()
                                                    .get())
                            .isInstanceOf(ExecutionException.class)
                            .hasCauseInstanceOf(RuntimeException.class);
                }
            } finally {
                rpcService.closeAsync().get();
            }
        }
    }

    @Test
    void test3() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            try (RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000,8001")
                            .withComponentName("test")
                            .createAndStart()) {

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
        }
    }

    @Test
    void testWildcard() throws Exception {
        try (RpcSystem rpcSystem = new GRpcSystem()) {
            try (RpcService rpcService =
                    rpcSystem
                            .remoteServiceBuilder(new Configuration(), null, "8000,8001")
                            .withComponentName("test")
                            .createAndStart()) {

                try (TestEndpoint testEndpoint = new TestEndpoint(rpcService); ) {
                    testEndpoint.start();

                    String address = testEndpoint.getAddress();
                    String wildcardAddress = address.substring(0, address.length() - 1) + "*";

                    System.out.println(
                            rpcService
                                    .connect(wildcardAddress, TestGateway.class)
                                    .get()
                                    .getCount()
                                    .get());
                }
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

    public interface TestGateway3 extends RpcGateway {
        CompletableFuture<Serializable> getCount2();
    }

    public static class TestEndpoint3 extends RpcEndpoint implements TestGateway3 {

        private final Serializable object;

        protected TestEndpoint3(RpcService rpcService, Serializable object) {
            super(rpcService);
            this.object = object;
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
        public CompletableFuture<Serializable> getCount2() {
            return CompletableFuture.completedFuture(object);
        }
    }

    public static class FailingEndpoint extends RpcEndpoint implements TestGateway {

        protected FailingEndpoint(RpcService rpcService) {
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
            return FutureUtils.completedExceptionally(new RuntimeException());
        }
    }
}
