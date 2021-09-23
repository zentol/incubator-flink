/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/** TODO: Add javadoc. */
public interface Handler<
        T, R extends RequestBody, P extends ResponseBody, M extends MessageParameters> {

    /**
     * This method is called for every incoming request and returns a {@link CompletableFuture}
     * containing a the response.
     *
     * <p>Implementations may decide whether to throw {@link RestHandlerException}s or fail the
     * returned {@link CompletableFuture} with a {@link RestHandlerException}.
     *
     * <p>Failing the future with another exception type or throwing unchecked exceptions is
     * regarded as an implementation error as it does not allow us to provide a meaningful HTTP
     * status code. In this case a {@link HttpResponseStatus#INTERNAL_SERVER_ERROR} will be
     * returned.
     *
     * @param request request that should be handled
     * @param gateway leader gateway
     * @return future containing a handler response
     * @throws RestHandlerException if the handling failed
     */
    CompletableFuture<P> handleRequest(@Nonnull HandlerRequest<R, M> request, @Nonnull T gateway)
            throws RestHandlerException;
}
