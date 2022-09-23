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

package org.apache.flink.runtime.rpc.grpc.marshalling;

import io.grpc.MethodDescriptor;

import java.io.InputStream;

/**
 * A {@link MethodDescriptor.Marshaller} for passing objects without serialization. This marshaller
 * (ab)uses that input streams returned by {@link #stream(Object)} are passed directly to the
 * receiving marshaller's {@link #parse(InputStream)} method. By wrapping the input object in a
 * pseudo {@link InputStream} we can pass objects in memory without serialization.
 *
 * <p>Note: While this seems sketchy it is also what the built-in gRPC protobuf marshaller does.
 */
public class LocalObjectMarshaller<T> implements MethodDescriptor.Marshaller<T> {

    @Override
    public InputStream stream(T value) {
        return new LocalInputStream<>(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(InputStream stream) {
        if (stream instanceof LocalInputStream) {
            return ((LocalInputStream<T>) stream).getValue();
        }
        throw new RuntimeException(
                "The input value was not marshalled by a " + getClass().getSimpleName());
    }

    private static final class LocalInputStream<T> extends InputStream {

        private final T value;

        public LocalInputStream(T value) {
            this.value = value;
        }

        @Override
        public int read() {
            return 0;
        }

        public T getValue() {
            return value;
        }
    }
}
