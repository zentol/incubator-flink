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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.util.function.TriConsumer;

import com.esotericsoftware.kryo.Serializer;
import com.twitter.chill.java.ArraysAsListSerializer;
import com.twitter.chill.java.BitSetSerializer;
import com.twitter.chill.java.InetSocketAddressSerializer;
import com.twitter.chill.java.LocaleSerializer;
import com.twitter.chill.java.RegexSerializer;
import com.twitter.chill.java.SimpleDateFormatSerializer;
import com.twitter.chill.java.SqlDateSerializer;
import com.twitter.chill.java.SqlTimeSerializer;
import com.twitter.chill.java.TimestampSerializer;
import com.twitter.chill.java.URISerializer;
import com.twitter.chill.java.UUIDSerializer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Registers all chill serializers used for Java types.
 *
 * <p>All registrations use a hard-coded ID which were determined at commit
 * 18f176ce86900fd4e932c73f3d138912355c6880.
 *
 * <p>DO NOT add more serializers, as this would break backwards-compatibility.
 */
public class FlinkChillPackageRegistrar implements ChillSerializerRegistrar {

    private static final int MAX_REGISTRATION_ID = 84;

    @Override
    public int getMaxRegistrationId() {
        return MAX_REGISTRATION_ID;
    }

    @Override
    public void registerSerializers(TriConsumer<Class<?>, Serializer<?>, Integer> kryo) {
        //noinspection ArraysAsListWithZeroOrOneArgument
        kryo.accept(Arrays.asList("").getClass(), new ArraysAsListSerializer(), 73);
        kryo.accept(BitSet.class, new BitSetSerializer(), 74);
        kryo.accept(PriorityQueue.class, new PriorityQueueSerializer(), 75);
        kryo.accept(Pattern.class, new RegexSerializer(), 76);
        kryo.accept(Date.class, new SqlDateSerializer(), 77);
        kryo.accept(Time.class, new SqlTimeSerializer(), 78);
        kryo.accept(Timestamp.class, new TimestampSerializer(), 79);
        kryo.accept(URI.class, new URISerializer(), 80);
        kryo.accept(InetSocketAddress.class, new InetSocketAddressSerializer(), 81);
        kryo.accept(UUID.class, new UUIDSerializer(), 82);
        kryo.accept(Locale.class, new LocaleSerializer(), 83);
        kryo.accept(SimpleDateFormat.class, new SimpleDateFormatSerializer(), MAX_REGISTRATION_ID);
    }
}
