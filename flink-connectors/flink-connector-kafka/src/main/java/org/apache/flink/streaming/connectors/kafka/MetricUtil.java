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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

/** Utilities for Kafka metrics. */
@Internal
public class MetricUtil {
    /** Returns the appropriate metric to capture the number of read bytes. */
    public static Metric getKakfaByteInMetric(Map<MetricName, ? extends Metric> metrics) {
        return metrics.entrySet().stream()
                .filter(
                        e ->
                                e.getKey().group().equals("consumer-fetch-manager-metrics")
                                        && e.getKey().name().equals("bytes-consumed-total"))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(
                        () -> new IllegalStateException("Cannot find bytes-consumed-total metric"));
    }

    /** Ensures that the counter has the same value as the given Kafka metric. */
    public static void sync(Metric from, Counter to) {
        to.inc(((Number) from.metricValue()).longValue() - to.getCount());
    }
}
