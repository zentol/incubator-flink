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

import org.apache.flink.util.InstantiationUtil;

import io.grpc.MethodDescriptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/** A {@link MethodDescriptor.Marshaller} for objects using Java serialization. */
public class ObjectMarshaller<T> implements MethodDescriptor.Marshaller<T> {

    private final ClassLoader flinkClassLoader;

    public ObjectMarshaller(ClassLoader flinkClassLoader) {
        this.flinkClassLoader = flinkClassLoader;
    }

    @Override
    public InputStream stream(T value) {
        try {
            // this could be more efficient via Drainable; not sure how that interacts with
            // KnownLength
            return new ByteArrayInputStream(InstantiationUtil.serializeObject(value));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(InputStream stream) {
        try {
            return InstantiationUtil.deserializeObject(stream, flinkClassLoader);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
