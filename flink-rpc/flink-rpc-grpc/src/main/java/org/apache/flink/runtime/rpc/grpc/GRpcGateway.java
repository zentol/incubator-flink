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
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcGatewayUtils;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.grpc.connection.Connection;
import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.runtime.rpc.messages.Type;
import org.apache.flink.runtime.rpc.messages.grpc.Request;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A gRPC-based {@link RpcGateway}. Submits requests against a {@link GRpcService} and processes the
 * responses.
 *
 * <p>Fencing is always used, with a sentinel value if no fencing token was provided by Flink.
 */
public class GRpcGateway<F extends Serializable>
        implements RpcGateway, InvocationHandler, FencedRpcGateway<F> {

    private static final Logger LOG = LoggerFactory.getLogger(GRpcGateway.class);

    @Nullable private final F fencingToken;
    private final String address;
    private final String hostname;

    private final String endpointId;
    private final boolean captureAskCallStack;
    private final Duration timeout;

    protected final boolean isLocal;
    protected final boolean forceRpcInvocationSerialization;

    private final ClassLoader flinkClassLoader;

    private final Connection connection;
    private final AtomicLong atomicLong;

    public GRpcGateway(
            @Nullable F fencingToken,
            String address,
            String hostname,
            String endpointId,
            boolean captureAskCallStack,
            Duration timeout,
            boolean isLocal,
            boolean forceRpcInvocationSerialization,
            ClassLoader flinkClassLoader,
            Connection connection,
            AtomicLong atomicLong) {
        this.fencingToken = fencingToken;
        this.address = address;
        this.hostname = hostname;
        this.endpointId = endpointId;
        this.captureAskCallStack = captureAskCallStack;
        this.timeout = timeout;
        this.isLocal = isLocal;
        this.forceRpcInvocationSerialization = forceRpcInvocationSerialization;
        this.flinkClassLoader = flinkClassLoader;
        this.connection = connection;
        this.atomicLong = atomicLong;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Class<?> declaringClass = method.getDeclaringClass();

        Object result;

        if (declaringClass.equals(Object.class)
                || declaringClass.equals(RpcGateway.class)
                || declaringClass.equals(FencedRpcGateway.class)
                || declaringClass.equals(AutoCloseableAsync.class)
                || declaringClass.equals(AutoCloseable.class)
                || declaringClass.equals(Closeable.class)) {
            result = method.invoke(this, args);
        } else {
            result = invokeRpc(method, args);
        }

        return result;
    }

    /**
     * Invokes a RPC method by sending the RPC invocation details to the rpc endpoint.
     *
     * @param method to call
     * @param args of the method call
     * @return result of the RPC; the result future is completed with a {@link TimeoutException} if
     *     the requests times out; if the recipient is not reachable, then the result future is
     *     completed with a {@link RecipientUnreachableException}.
     * @throws Exception if the RPC invocation fails
     */
    private Object invokeRpc(Method method, Object[] args) throws Exception {
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        final boolean isLocalRpcInvocation = method.getAnnotation(Local.class) != null;
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        Duration futureTimeout =
                RpcGatewayUtils.extractRpcTimeout(parameterAnnotations, args, timeout);

        if (isLocalRpcInvocation) {
            throw new RuntimeException("Local RPCs are not supported.");
        }

        final RemoteRpcInvocation rpcInvocation =
                new RemoteRpcInvocation(
                        method.getDeclaringClass().getSimpleName(),
                        methodName,
                        parameterTypes,
                        args);

        Class<?> returnType = method.getReturnType();

        final Object result;

        if (Objects.equals(returnType, Void.TYPE)) {
            tell(rpcInvocation);

            result = null;
        } else {
            // Capture the call stack. It is significantly faster to do that via an exception than
            // via Thread.getStackTrace(), because exceptions lazily initialize the stack trace,
            // initially only
            // capture a lightweight native pointer, and convert that into the stack trace lazily
            // when needed.
            final Throwable callStackCapture = captureAskCallStack ? new Throwable() : null;

            // execute an asynchronous call
            final CompletableFuture<?> resultFuture = ask(rpcInvocation, futureTimeout);

            final CompletableFuture<Object> completableFuture = new CompletableFuture<>();
            resultFuture.whenComplete(
                    (resultValue, failure) -> {
                        if (failure != null) {
                            completableFuture.completeExceptionally(
                                    resolveTimeoutException(
                                            ExceptionUtils.stripCompletionException(failure),
                                            callStackCapture,
                                            address,
                                            rpcInvocation));
                        } else {
                            if (resultValue instanceof SerializedThrowable) {
                                completableFuture.completeExceptionally(
                                        ((SerializedThrowable) resultValue)
                                                .deserializeError(flinkClassLoader));
                            } else {
                                completableFuture.complete(resultValue);
                            }
                        }
                    });

            if (Objects.equals(returnType, CompletableFuture.class)) {
                result = completableFuture;
            } else {
                try {
                    result = completableFuture.get(futureTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (ExecutionException ee) {
                    throw new RpcException(
                            "Failure while obtaining synchronous RPC result.",
                            ExceptionUtils.stripExecutionException(ee));
                }
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    /**
     * Sends the message to the RPC endpoint.
     *
     * @param message to send to the RPC endpoint.
     */
    private void tell(RemoteRpcInvocation message) {
        final long id = atomicLong.getAndIncrement();

        LOG.trace("Sending tell #{} to '{}' (RPC={})", id, address, message);

        ClassLoadingUtils.runWithContextClassLoader(
                () -> {
                    connection.tell(new Request(fencingToken, message, endpointId, id, Type.TELL));
                },
                flinkClassLoader);
    }

    /**
     * Sends the message to the RPC endpoint and returns a future containing its response.
     *
     * @param message to send to the RPC endpoint
     * @param timeout time to wait until the response future is failed with a {@link
     *     TimeoutException}
     * @return Response future
     */
    private CompletableFuture<?> ask(RemoteRpcInvocation message, Duration timeout) {
        final long id = atomicLong.getAndIncrement();

        LOG.trace("Sending ask #{} to '{}' (RPC={})", id, address, message);

        CompletableFuture<Object> response = new CompletableFuture<>();
        response.thenAccept(
                resp -> LOG.trace("Received ask #{} response {} (RPC={})", id, resp, message));

        ClassLoadingUtils.runWithContextClassLoader(
                () ->
                        FutureUtils.forward(
                                connection.ask(
                                        new Request(
                                                fencingToken, message, endpointId, id, Type.ASK)),
                                response),
                flinkClassLoader);

        return ClassLoadingUtils.guardCompletionWithContextClassLoader(
                FutureUtils.orTimeout(response, timeout.toMillis(), TimeUnit.MILLISECONDS),
                flinkClassLoader);
    }

    static Throwable resolveTimeoutException(
            Throwable exception,
            @Nullable Throwable callStackCapture,
            String recipient,
            RpcInvocation rpcInvocation) {
        if (!(exception instanceof TimeoutException)) {
            return exception;
        }

        final Exception newException;

        if (exception.getClass().isAssignableFrom(RecipientUnreachableException.class)) {
            newException = (Exception) exception;
        } else {
            newException =
                    new TimeoutException(
                            String.format(
                                    "Invocation of [%s] at recipient [%s] timed out. This is usually caused by: 1) Akka failed sending "
                                            + "the message silently, due to problems like oversized payload or serialization failures. "
                                            + "In that case, you should find detailed error information in the logs. 2) The recipient needs "
                                            + "more time for responding, due to problems like slow machines or network jitters. In that case, you can try to increase %s.",
                                    rpcInvocation,
                                    recipient,
                                    AkkaOptions.ASK_TIMEOUT_DURATION.key()));
        }

        newException.initCause(exception);

        if (callStackCapture != null) {
            // remove the stack frames coming from the proxy interface invocation
            final StackTraceElement[] stackTrace = callStackCapture.getStackTrace();
            newException.setStackTrace(Arrays.copyOfRange(stackTrace, 3, stackTrace.length));
        }

        return newException;
    }

    @Override
    public F getFencingToken() {
        return fencingToken;
    }
}
