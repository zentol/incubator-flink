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

package org.apache.flink.metrics.reporter;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** A mock implementation that makes all reported metrics available for tests. */
public class TestReporter implements MetricReporter {
    private Map<String, Metric> metrics = new HashMap<>();
    private static final ThreadLocal<TestReporter> REPORTERS =
            ThreadLocal.withInitial(TestReporter::new);

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {
        metrics.clear();
        REPORTERS.remove();
    }

    public static TestReporter getInstance() {
        return REPORTERS.get();
    }

    public Map<String, Metric> getMetrics() {
        return metrics;
    }

    public Map<String, Metric> getMetrics(String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        return metrics.entrySet().stream()
                .filter(m -> pattern.matcher(m.getKey()).find())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Metric getMetric(String patternString) {
        Pattern pattern = Pattern.compile(patternString);
        return metrics.entrySet().stream()
                .filter(m -> pattern.matcher(m.getKey()).find())
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "No metric found in " + metrics.keySet()));
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        metrics.put(group.getMetricIdentifier(metricName), metric);
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        metrics.remove(group.getMetricIdentifier(metricName));
    }

    /** The factory for the {@link TestReporter}. */
    public static class Factory implements MetricReporterFactory {
        @Override
        public MetricReporter createMetricReporter(Properties properties) {
            return REPORTERS.get();
        }
    }
}
