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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.types.Either;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** TODO: Add javadoc. */
public class Rx<T> {
    public HttpResponseStatus getResponseCode() {
        return returnCode.getResponseStatus();
    }

    public boolean isSuccess() {
        return response.isLeft();
    }

    public T getContent() {
        return response.left();
    }

    public RestHandlerException getError() {
        return response.right();
    }

    public RxT getReturnCode() {
        return returnCode;
    }

    private final RxT returnCode;

    private final Either<T, RestHandlerException> response;

    private Rx(RxT returnCode, Either<T, RestHandlerException> response) {
        this.returnCode = returnCode;
        this.response = response;
    }

    public static <T> Rx<T> success(RxT responseStatus, T content) {
        return new Rx<>(responseStatus, Either.Left(content));
    }

    public static <T> Rx<T> failure(RestHandlerException error) {
        return new Rx<>(error.getHttpResponseStatus(), Either.Right(error));
    }
}
