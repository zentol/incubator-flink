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

package org.apache.flink.metrics.datadog;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import com.datadog.api.client.ApiClient;
import com.datadog.api.client.ApiException;
import com.datadog.api.client.v2.api.MetricsApi;
import com.datadog.api.client.v2.model.MetricIntakeType;
import com.datadog.api.client.v2.model.MetricPayload;
import com.datadog.api.client.v2.model.MetricPoint;
import com.datadog.api.client.v2.model.MetricResource;
import com.datadog.api.client.v2.model.MetricSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metric Reporter for Datadog.
 *
 * <p>Variables in metrics scope will be sent to Datadog as tags.
 */
public class DatadogHttpReporter implements MetricReporter, Scheduled {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatadogHttpReporter.class);
    private static final String HOST_VARIABLE = "<host>";

    // Both Flink's Gauge and Meter values are taken as gauge in Datadog
    private final Map<Gauge, DGauge> gauges = new ConcurrentHashMap<>();
    private final Map<Counter, DCounter> counters = new ConcurrentHashMap<>();
    private final Map<Meter, DMeter> meters = new ConcurrentHashMap<>();
    private final Map<Histogram, DHistogram> histograms = new ConcurrentHashMap<>();

    private DatadogHttpClient client;
    private List<String> configTags;
    private int maxMetricsPerRequestValue;

    private final Clock clock = () -> System.currentTimeMillis() / 1000L;

    public static final String API_KEY = "apikey";
    public static final String PROXY_HOST = "proxyHost";
    public static final String PROXY_PORT = "proxyPort";
    public static final String DATA_CENTER = "dataCenter";
    public static final String TAGS = "tags";
    public static final String MAX_METRICS_PER_REQUEST = "maxMetricsPerRequest";
    private String apiKey;
    private DataCenter dataCenter;

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name = group.getMetricIdentifier(metricName);

        List<String> tags = new ArrayList<>(configTags);
        tags.addAll(getTagsFromMetricGroup(group));
        String host = getHostFromMetricGroup(group);

        switch (metric.getMetricType()) {
            case COUNTER:
                Counter c = (Counter) metric;
                counters.put(c, new DCounter(c, name, host, tags, clock));
                break;
            case GAUGE:
                Gauge g = (Gauge) metric;
                gauges.put(g, new DGauge(g, name, host, tags, clock));
                break;
            case METER:
                Meter m = (Meter) metric;
                // Only consider rate
                meters.put(m, new DMeter(m, name, host, tags, clock));
                break;
            case HISTOGRAM:
                Histogram h = (Histogram) metric;
                histograms.put(h, new DHistogram(h, name, host, tags, clock));
                break;
            default:
                LOGGER.warn(
                        "Cannot add unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        switch (metric.getMetricType()) {
            case COUNTER:
                counters.remove(metric);
                break;
            case GAUGE:
                gauges.remove(metric);
                break;
            case METER:
                meters.remove(metric);
                break;
            case HISTOGRAM:
                histograms.remove(metric);
                break;
            default:
                LOGGER.warn(
                        "Cannot remove unknown metric type {}. This indicates that the reporter "
                                + "does not support this metric type.",
                        metric.getClass().getName());
        }
    }

    @Override
    public void open(MetricConfig config) {
        apiKey = config.getString(API_KEY, null);
        String proxyHost = config.getString(PROXY_HOST, null);
        Integer proxyPort = config.getInteger(PROXY_PORT, 8080);
        String rawDataCenter = config.getString(DATA_CENTER, "US");
        maxMetricsPerRequestValue = config.getInteger(MAX_METRICS_PER_REQUEST, 2000);
        dataCenter = DataCenter.valueOf(rawDataCenter);
        String tags = config.getString(TAGS, "");

        client = new DatadogHttpClient(apiKey, proxyHost, proxyPort, dataCenter, true);

        configTags = getTagsFromConfig(tags);

        LOGGER.info(
                "Configured DatadogHttpReporter with {tags={}, proxyHost={}, proxyPort={}, dataCenter={}, maxMetricsPerRequest={}",
                tags,
                proxyHost,
                proxyPort,
                dataCenter,
                maxMetricsPerRequestValue);
    }

    @Override
    public void close() {
        client.close();
        LOGGER.info("Shut down DatadogHttpReporter");
    }

    @Override
    public void report() {
        ApiClient defaultClient = ApiClient.getDefaultApiClient();
        defaultClient.setApiKey(this.apiKey);
        final Map<String, String> serverVariables = new HashMap<>();
        serverVariables.put("site", "datadoghq." + dataCenter.getDomain());
        defaultClient.setServerVariables(serverVariables);
        MetricsApi apiInstance = new MetricsApi(defaultClient);

        final MetricPayload metricName =
                new MetricPayload()
                        .series(
                                Collections.singletonList(
                                        new MetricSeries()
                                                .metric("metricName")
                                                .type(MetricIntakeType.GAUGE)
                                                .points(
                                                        Collections.singletonList(
                                                                new MetricPoint().value(0.0)))
                                                .tags(Collections.emptyList())
                                                .resources(
                                                        Collections.singletonList(
                                                                new MetricResource()
                                                                        .type("host")
                                                                        .name("localhost")))));

        try {
            apiInstance.submitMetrics(metricName);
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }

        final List<MetricSeries> request = new ArrayList<>();

        final long timestamp = clock.getUnixEpochTimestamp();

        addGaugesAndUnregisterOnException(request, timestamp);
        counters.values().stream()
                .map(
                        counter ->
                                counter.getBaseMetricSeries()
                                        .points(
                                                Collections.singletonList(
                                                        new MetricPoint()
                                                                .value(counter.getMetricValue())
                                                                .timestamp(timestamp))))
                .forEach(request::add);
        counters.values().stream()
                .map(
                        counter ->
                                counter.getBaseMetricSeries()
                                        .points(
                                                Collections.singletonList(
                                                        new MetricPoint()
                                                                .value(counter.getMetricValue())
                                                                .timestamp(timestamp))))
                .forEach(request::add);
        meters.values().stream()
                .map(
                        counter ->
                                counter.getBaseMetricSeries()
                                        .points(
                                                Collections.singletonList(
                                                        new MetricPoint()
                                                                .value(counter.getMetricValue())
                                                                .timestamp(timestamp))))
                .forEach(request::add);
        histograms.values().forEach(histogram -> histogram.addTo(request, timestamp));

        int totalMetrics = request.size();
        int fromIndex = 0;
        while (fromIndex < totalMetrics) {
            int toIndex = Math.min(fromIndex + maxMetricsPerRequestValue, totalMetrics);
            try {
                final MetricPayload metricPayload = new MetricPayload();
                metricPayload.series(request.subList(fromIndex, toIndex));
                apiInstance.submitMetrics(metricName);
                // TODO: fix ack chunk.getSeries().forEach(DMetric::ackReport);
                LOGGER.debug("Reported series with size {}.", metricPayload.getSeries().size());
            } catch (Exception e) {
                LOGGER.warn("Failed reporting metrics to Datadog.", e);
            }
            fromIndex = toIndex;
        }
    }

    private void addGaugesAndUnregisterOnException(List<MetricSeries> request, long timestamp) {
        List<Gauge> gaugesToRemove = new ArrayList<>();
        for (Map.Entry<Gauge, DGauge> entry : gauges.entrySet()) {
            DGauge g = entry.getValue();
            try {
                // Will throw exception if the Gauge is not of Number type
                // Flink uses Gauge to store many types other than Number
                request.add(
                        g.getBaseMetricSeries()
                                .points(
                                        Collections.singletonList(
                                                new MetricPoint()
                                                        .value((double) g.getMetricValue())
                                                        .timestamp(timestamp))));
            } catch (ClassCastException e) {
                LOGGER.info(
                        "The metric {} will not be reported because only number types are supported by this reporter.",
                        g.getBaseMetricSeries().getMetric());
                gaugesToRemove.add(entry.getKey());
            } catch (Exception e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "The metric {} will not be reported because it threw an exception.",
                            g.getBaseMetricSeries().getMetric(),
                            e);
                } else {
                    LOGGER.info(
                            "The metric {} will not be reported because it threw an exception.",
                            g.getBaseMetricSeries().getMetric());
                }
                gaugesToRemove.add(entry.getKey());
            }
        }
        gaugesToRemove.forEach(gauges::remove);
    }

    /** Get config tags from config 'metrics.reporter.dghttp.tags'. */
    private List<String> getTagsFromConfig(String str) {
        return Arrays.asList(str.split(","));
    }

    /** Get tags from MetricGroup#getAllVariables(), excluding 'host'. */
    private List<String> getTagsFromMetricGroup(MetricGroup metricGroup) {
        List<String> tags = new ArrayList<>();

        for (Map.Entry<String, String> entry : metricGroup.getAllVariables().entrySet()) {
            if (!entry.getKey().equals(HOST_VARIABLE)) {
                tags.add(getVariableName(entry.getKey()) + ":" + entry.getValue());
            }
        }

        return tags;
    }

    private String getHostFromMetricGroup(MetricGroup metricGroup) {
        return metricGroup.getAllVariables().get(HOST_VARIABLE);
    }

    /** Removes leading and trailing angle brackets. */
    private String getVariableName(String str) {
        return str.substring(1, str.length() - 1);
    }
}
