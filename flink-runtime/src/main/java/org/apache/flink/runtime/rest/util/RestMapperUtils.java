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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.Version;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.NullSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.NumberSerializers;

import java.io.IOException;

/** This class contains utilities for mapping requests and responses to/from JSON. */
public class RestMapperUtils {
    private static final ObjectMapper objectMapper;

    static {
        objectMapper = JacksonMapperFactory.createObjectMapper();
        objectMapper.enable(
                DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,
                DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        final SimpleModule module =
                new SimpleModule("customDoubleSerializers", Version.unknownVersion());
        module.addSerializer(Double.class, new SpecDoubleSerializer(Double.class));
        module.addSerializer(Double.TYPE, new SpecDoubleSerializer(Double.TYPE));

        objectMapper.registerModule(module);
    }

    /**
     * Returns a preconfigured {@link ObjectMapper}.
     *
     * @return preconfigured object mapper
     */
    public static ObjectMapper getStrictObjectMapper() {
        return objectMapper;
    }

    private static class SpecDoubleSerializer extends JsonSerializer<Double> {
        private final NumberSerializers.DoubleSerializer doubleSerializer;

        public SpecDoubleSerializer(Class<Double> doubleClass) {
            doubleSerializer = new NumberSerializers.DoubleSerializer(doubleClass);
        }

        @Override
        public void serialize(
                Double value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
                throws IOException {
            if (Double.isNaN(value)
                    || value == Double.POSITIVE_INFINITY
                    || value == Double.NEGATIVE_INFINITY) {
                NullSerializer.instance.serialize(null, jsonGenerator, serializerProvider);
            } else {
                doubleSerializer.serialize(value, jsonGenerator, serializerProvider);
            }
        }
    }
}
