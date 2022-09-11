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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GRpcGateway<F extends Serializable>
        implements RpcGateway, InvocationHandler, FencedRpcGateway<F> {

    @Nullable private F fencingToken;
    private final String address;
    private final String hostname;

    private final String endpointId;
    private final boolean captureAskCallStack;
    private final Duration timeout;

    protected final boolean isLocal;
    protected final boolean forceRpcInvocationSerialization;

    private final ClassLoader flinkClassLoader;

    private final ServerGrpc.ServerFutureStub server;

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
            ServerGrpc.ServerFutureStub server) {
        this.fencingToken = fencingToken;
        this.address = address;
        this.hostname = hostname;
        this.endpointId = endpointId;
        this.captureAskCallStack = captureAskCallStack;
        this.timeout = timeout;
        this.isLocal = isLocal;
        this.forceRpcInvocationSerialization = forceRpcInvocationSerialization;
        this.flinkClassLoader = flinkClassLoader;
        this.server = server;
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
                || declaringClass.equals(FencedRpcGateway.class)) {
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
        Duration futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

        final RpcInvocation rpcInvocation =
                createRpcInvocationMessage(
                        method.getDeclaringClass().getSimpleName(),
                        methodName,
                        isLocalRpcInvocation,
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
            final CompletableFuture<?> resultFuture =
                    ask(rpcInvocation, futureTimeout)
                            .thenApply(
                                    resultValue ->
                                            deserializeValueIfNeeded(
                                                    resultValue, method, flinkClassLoader));

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
                            completableFuture.complete(resultValue);
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

    /**
     * Create the RpcInvocation message for the given RPC.
     *
     * @param declaringClassName of the RPC
     * @param methodName of the RPC
     * @param isLocalRpcInvocation whether the RPC must be sent as a local message
     * @param parameterTypes of the RPC
     * @param args of the RPC
     * @return RpcInvocation message which encapsulates the RPC details
     * @throws IOException if we cannot serialize the RPC invocation parameters
     */
    private RpcInvocation createRpcInvocationMessage(
            final String declaringClassName,
            final String methodName,
            final boolean isLocalRpcInvocation,
            final Class<?>[] parameterTypes,
            final Object[] args)
            throws IOException {
        final RpcInvocation rpcInvocation;

        if (isLocal && (!forceRpcInvocationSerialization || isLocalRpcInvocation)) {
            rpcInvocation =
                    new LocalRpcInvocation(
                            endpointId, declaringClassName, methodName, parameterTypes, args);
        } else {
            rpcInvocation =
                    new RemoteRpcInvocation(
                            endpointId, declaringClassName, methodName, parameterTypes, args);
        }

        return rpcInvocation;
    }

    // ------------------------------------------------------------------------
    //  Helper methods
    // ------------------------------------------------------------------------

    /**
     * Extracts the {@link RpcTimeout} annotated rpc timeout value from the list of given method
     * arguments. If no {@link RpcTimeout} annotated parameter could be found, then the default
     * timeout is returned.
     *
     * @param parameterAnnotations Parameter annotations
     * @param args Array of arguments
     * @param defaultTimeout Default timeout to return if no {@link RpcTimeout} annotated parameter
     *     has been found
     * @return Timeout extracted from the array of arguments or the default timeout
     */
    private static Duration extractRpcTimeout(
            Annotation[][] parameterAnnotations, Object[] args, Duration defaultTimeout) {
        if (args != null) {
            Preconditions.checkArgument(parameterAnnotations.length == args.length);

            for (int i = 0; i < parameterAnnotations.length; i++) {
                if (isRpcTimeout(parameterAnnotations[i])) {
                    if (args[i] instanceof Time) {
                        return TimeUtils.toDuration((Time) args[i]);
                    } else if (args[i] instanceof Duration) {
                        return (Duration) args[i];
                    } else {
                        throw new RuntimeException(
                                "The rpc timeout parameter must be of type "
                                        + Time.class.getName()
                                        + " or "
                                        + Duration.class.getName()
                                        + ". The type "
                                        + args[i].getClass().getName()
                                        + " is not supported.");
                    }
                }
            }
        }

        return defaultTimeout;
    }

    /**
     * Checks whether any of the annotations is of type {@link RpcTimeout}.
     *
     * @param annotations Array of annotations
     * @return True if {@link RpcTimeout} was found; otherwise false
     */
    private static boolean isRpcTimeout(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(RpcTimeout.class)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Sends the message to the RPC endpoint.
     *
     * @param message to send to the RPC endpoint.
     */
    protected void tell(Object message) throws IOException {
        server.tell(InstantiationUtil.serializeObject(message));
    }

    /**
     * Sends the message to the RPC endpoint and returns a future containing its response.
     *
     * @param message to send to the RPC endpoint
     * @param timeout time to wait until the response future is failed with a {@link
     *     TimeoutException}
     * @return Response future
     */
    protected CompletableFuture<?> ask(Object message, Duration timeout) throws IOException {
        CompletableFuture<byte[]> ask =
                FutureUtils.orTimeout(
                        server.ask(InstantiationUtil.serializeObject(message)),
                        timeout.toMillis(),
                        TimeUnit.MILLISECONDS);

        return ClassLoadingUtils.guardCompletionWithContextClassLoader(ask, flinkClassLoader);
    }

    private static Object deserializeValueIfNeeded(
            Object o, Method method, ClassLoader flinkClassLoader) {
        if (o instanceof byte[]) {
            try {
                return InstantiationUtil.deserializeObject((byte[]) o, flinkClassLoader);
            } catch (IOException | ClassNotFoundException e) {
                throw new CompletionException(
                        new RpcException(
                                "Could not deserialize the serialized payload of RPC method : "
                                        + method.getName(),
                                e));
            }
        } else {
            return o;
        }
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

        // TODO: add exception type
        if (false /*AkkaRpcServiceUtils.isRecipientTerminatedException(exception)*/) {
            newException =
                    new RecipientUnreachableException(
                            "unknown", recipient, rpcInvocation.toString());
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
