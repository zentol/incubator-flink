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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Simple container for the request to a handler, that contains the {@link RequestBody} and
 * path/query parameters.
 *
 * @param <R> type of the contained request body
 * @param <M> type of the contained message parameters
 */
public class HandlerRequest<R extends RequestBody, M extends MessageParameters> {

    private final R requestBody;
    private final Collection<File> uploadedFiles;
    private final Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>>
            pathParameters;
    private final Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>>
            queryParameters;

    public static <R extends RequestBody, M extends MessageParameters>
            HandlerRequest<R, M> fromRequest(
                    R requestBody,
                    M messageParameters,
                    Map<String, String> receivedPathParameters,
                    Map<String, List<String>> receivedQueryParameters,
                    Collection<File> uploadedFiles)
                    throws HandlerRequestException {

        Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>> pathParameters =
                resolveParameters(
                        messageParameters.getPathParameters(),
                        receivedPathParameters,
                        x -> true,
                        x -> x);

        Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>> queryParameters =
                resolveParameters(
                        messageParameters.getQueryParameters(),
                        receivedQueryParameters,
                        x -> !x.isEmpty(),
                        x -> String.join(";"));

        return new HandlerRequest<R, M>(
                requestBody, pathParameters, queryParameters, uploadedFiles);
    }

    @VisibleForTesting
    public static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> empty() {
        return new HandlerRequest(
                EmptyRequestBody.getInstance(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }

    @VisibleForTesting
    public static <R extends RequestBody, M extends MessageParameters> HandlerRequest<R, M> create(
            R requestBody, M messageParameters) throws HandlerRequestException {
        return create(requestBody, messageParameters, Collections.emptyList());
    }

    @VisibleForTesting
    public static <R extends RequestBody, M extends MessageParameters> HandlerRequest<R, M> create(
            R requestBody, M messageParameters, Collection<File> uploadedFiles)
            throws HandlerRequestException {
        return new HandlerRequest(
                requestBody,
                resolveParameters(messageParameters.getPathParameters()),
                resolveParameters(messageParameters.getQueryParameters()),
                uploadedFiles);
    }

    private HandlerRequest(
            R requestBody,
            Map<Class<? extends MessagePathParameter<?>>, MessagePathParameter<?>> pathParameters,
            Map<Class<? extends MessageQueryParameter<?>>, MessageQueryParameter<?>>
                    queryParameters,
            Collection<File> uploadedFiles) {
        this.requestBody = requestBody;
        this.pathParameters = pathParameters;
        this.queryParameters = queryParameters;
        this.uploadedFiles = uploadedFiles;
    }

    static <M extends MessageParameter<?>, T> Map<Class<? extends M>, M> resolveParameters(
            Collection<M> messageParameters) throws HandlerRequestException {

        final Map<Class<? extends M>, M> pathParameters = new HashMap<>(2);

        for (M pathParameter : messageParameters) {
            if (pathParameter.isResolved()) {
                @SuppressWarnings("unchecked")
                Class<M> clazz = (Class<M>) pathParameter.getClass();
                pathParameters.put(clazz, pathParameter);
            }
        }

        return pathParameters;
    }

    static <M extends MessageParameter<?>, T> Map<Class<? extends M>, M> resolveParameters(
            Collection<M> messageParameters,
            Map<String, T> receivedPathParameters,
            Predicate<T> predicate,
            Function<T, String> processor)
            throws HandlerRequestException {

        final Map<Class<? extends M>, M> pathParameters = new HashMap<>(2);

        for (M pathParameter : messageParameters) {
            T value = receivedPathParameters.get(pathParameter.getKey());

            if (value != null && predicate.test(value)) {
                final String apply = processor.apply(value);
                try {
                    pathParameter.resolveFromString(apply);
                } catch (Exception e) {
                    throw new HandlerRequestException(
                            "Cannot resolve path parameter ("
                                    + pathParameter.getKey()
                                    + ") from value \""
                                    + value
                                    + "\".");
                }
            }
            if (pathParameter.isResolved()) {
                @SuppressWarnings("unchecked")
                Class<M> clazz = (Class<M>) pathParameter.getClass();
                pathParameters.put(clazz, pathParameter);
            }
        }

        return pathParameters;
    }

    public HandlerRequest(
            R requestBody,
            M messageParameters,
            Map<String, String> receivedPathParameters,
            Map<String, List<String>> receivedQueryParameters)
            throws HandlerRequestException {
        this(
                requestBody,
                messageParameters,
                receivedPathParameters,
                receivedQueryParameters,
                Collections.emptyList());
    }

    /**
     * Returns the request body.
     *
     * @return request body
     */
    public R getRequestBody() {
        return requestBody;
    }

    /**
     * Returns the value of the {@link MessagePathParameter} for the given class.
     *
     * @param parameterClass class of the parameter
     * @param <X> the value type that the parameter contains
     * @param <PP> type of the path parameter
     * @return path parameter value for the given class
     * @throws IllegalStateException if no value is defined for the given parameter class
     */
    public <X, PP extends MessagePathParameter<X>> X getPathParameter(Class<PP> parameterClass) {
        @SuppressWarnings("unchecked")
        PP pathParameter = (PP) pathParameters.get(parameterClass);
        Preconditions.checkState(
                pathParameter != null, "No parameter could be found for the given class.");
        return pathParameter.getValue();
    }

    /**
     * Returns the value of the {@link MessageQueryParameter} for the given class.
     *
     * @param parameterClass class of the parameter
     * @param <X> the value type that the parameter contains
     * @param <QP> type of the query parameter
     * @return query parameter value for the given class, or an empty list if no parameter value
     *     exists for the given class
     */
    public <X, QP extends MessageQueryParameter<X>> List<X> getQueryParameter(
            Class<QP> parameterClass) {
        @SuppressWarnings("unchecked")
        QP queryParameter = (QP) queryParameters.get(parameterClass);
        if (queryParameter == null) {
            return Collections.emptyList();
        } else {
            return queryParameter.getValue();
        }
    }

    @Nonnull
    public Collection<File> getUploadedFiles() {
        return uploadedFiles;
    }
}
