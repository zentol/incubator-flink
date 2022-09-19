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

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.rpc.grpc.ClassLoadingUtils.runWithContextClassLoader;

/**
 * A gRPC-based {@link RpcServer}. This class is relatively small because the actual server work is
 * handled by the {@link GRpcService} because we can't add additional endpoints (==gRPC services) on
 * the fly.
 */
public class GRpcServer implements RpcServer {

    private static final Logger LOG = LoggerFactory.getLogger(GRpcServer.class);

    private final ScheduledExecutorService mainThread;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private final String address;
    private final String hostName;
    private final RpcEndpoint rpcEndpoint;
    private final ClassLoader flinkClassLoader;

    public GRpcServer(
            ScheduledExecutorService mainThread,
            String address,
            String hostName,
            RpcEndpoint rpcEndpoint,
            ClassLoader flinkClassLoader)
            throws IOException {
        this.mainThread = mainThread;
        this.address = address;
        this.hostName = hostName;
        this.rpcEndpoint = rpcEndpoint;
        this.flinkClassLoader = flinkClassLoader;
        final MainThreadValidatorUtil mainThreadValidator =
                new MainThreadValidatorUtil(rpcEndpoint);
        mainThread.submit(mainThreadValidator::enterMainThread);
    }

    public String getEndpointId() {
        return rpcEndpoint.getEndpointId();
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostName;
    }

    @Override
    public void runAsync(Runnable runnable) {
        try {
            mainThread.submit(runnable);
        } catch (RejectedExecutionException e) {
            // ignore; means something was scheduled while the endpoint was already shut down
        }
    }

    @Override
    public <V> CompletableFuture<V> callAsync(Callable<V> callable, Duration callTimeout) {
        return FutureUtils.orTimeout(
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return callable.call();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        mainThread),
                callTimeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void scheduleRunAsync(Runnable runnable, long delay) {
        mainThread.schedule(runnable, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    private final AtomicReference<TernaryBoolean> isRunning =
            new AtomicReference<>(TernaryBoolean.UNDEFINED);

    @Override
    public void start() {
        mainThread.submit(
                () -> {
                    try {
                        isRunning.set(TernaryBoolean.TRUE);
                        rpcEndpoint.internalCallOnStart();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void stop() {
        if (isRunning.compareAndSet(TernaryBoolean.TRUE, TernaryBoolean.FALSE)) {
            mainThread.submit(
                    () -> {
                        FutureUtils.forward(rpcEndpoint.internalCallOnStop(), terminationFuture);
                        terminationFuture.thenRun(mainThread::shutdownNow);
                    });
        } else {
            terminationFuture.complete(null);
        }
    }

    public CompletableFuture<?> handleRpcInvocation(
            final Serializable fencingToken, final RpcInvocation rpcInvocation)
            throws RpcConnectionException {
        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            final Serializable expectedFencingToken =
                    ((FencedRpcEndpoint<?>) rpcEndpoint).getFencingToken();

            if (expectedFencingToken == null) {
                LOG.debug(
                        "Fencing token not set: Ignoring message {} because the fencing token is null.",
                        rpcInvocation);

                return FutureUtils.completedExceptionally(
                        new FencingTokenException(
                                String.format(
                                        "Fencing token not set: Ignoring message %s sent to %s because the fencing token is null.",
                                        rpcInvocation, rpcEndpoint.getAddress())));
            } else {

                if (!Objects.equals(expectedFencingToken, fencingToken)) {
                    LOG.debug(
                            "Fencing token mismatch: Ignoring message {} because the fencing token {} did "
                                    + "not match the expected fencing token {}.",
                            rpcInvocation,
                            fencingToken,
                            expectedFencingToken);

                    return FutureUtils.completedExceptionally(
                            new FencingTokenException(
                                    "Fencing token mismatch: Ignoring message "
                                            + rpcInvocation
                                            + " because the fencing token "
                                            + fencingToken
                                            + " did not match the expected fencing token "
                                            + expectedFencingToken
                                            + '.'));
                }
            }
        }

        final Method rpcMethod;

        try {
            String methodName = rpcInvocation.getMethodName();
            Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

            rpcMethod = rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
        } catch (final NoSuchMethodException e) {
            LOG.error("Could not find rpc method for rpc invocation.", e);

            throw new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
        }

        try {
            // this supports declaration of anonymous classes
            rpcMethod.setAccessible(true);

            if (rpcMethod.getReturnType().equals(Void.TYPE)) {
                // No return value to send back
                mainThread.execute(
                        () -> {
                            try {
                                runWithContextClassLoader(
                                        () ->
                                                rpcMethod.invoke(
                                                        rpcEndpoint, rpcInvocation.getArgs()),
                                        flinkClassLoader);
                            } catch (ReflectiveOperationException e) {
                                throw new RuntimeException(e);
                            }
                        });
                return CompletableFuture.completedFuture(null);
            } else {
                final CompletableFuture<Object> result = new CompletableFuture<>();
                mainThread.execute(
                        () -> {
                            try {
                                runWithContextClassLoader(
                                        () -> {
                                            Object rpcResult =
                                                    rpcMethod.invoke(
                                                            rpcEndpoint, rpcInvocation.getArgs());
                                            if (rpcResult instanceof CompletableFuture) {
                                                FutureUtils.forward(
                                                        (CompletableFuture<Object>) rpcResult,
                                                        result);
                                            } else {
                                                result.complete(rpcResult);
                                            }
                                        },
                                        flinkClassLoader);
                            } catch (ReflectiveOperationException e) {
                                LOG.debug(
                                        "Reporting back error thrown in remote procedure {}",
                                        rpcMethod,
                                        e);
                                result.completeExceptionally(e);
                            }
                        });

                final String methodName = rpcMethod.getName();
                final boolean isLocalRpcInvocation = rpcMethod.getAnnotation(Local.class) != null;

                return result;
            }
        } catch (Throwable e) {
            LOG.error("Error while executing remote procedure call {}.", rpcMethod, e);
            // tell the sender about the failure
            throw e;
        }
    }
}
