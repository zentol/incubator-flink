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

package org.apache.flink.metrics.datadog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import com.datadog.api.client.v2.model.MetricPoint;
import com.datadog.api.client.v2.model.MetricSeries;

import java.util.Collections;
import java.util.List;

/**
 * Maps histograms to datadog gauges.
 *
 * <p>Note: We cannot map them to datadog histograms because the HTTP API does not support them.
 */
public class DHistogram {
    @VisibleForTesting static final String SUFFIX_AVG = ".avg";
    @VisibleForTesting static final String SUFFIX_COUNT = ".count";
    @VisibleForTesting static final String SUFFIX_MEDIAN = ".median";
    @VisibleForTesting static final String SUFFIX_95_PERCENTILE = ".95percentile";
    @VisibleForTesting static final String SUFFIX_MIN = ".min";
    @VisibleForTesting static final String SUFFIX_MAX = ".max";

    private final Histogram histogram;

    private final MetricSeries metaDataAvg;
    private final MetricSeries metaDataCount;
    private final MetricSeries metaDataMedian;
    private final MetricSeries metaData95Percentile;
    private final MetricSeries metaDataMin;
    private final MetricSeries metaDataMax;

    public DHistogram(
            Histogram histogram, String metricName, String host, List<String> tags, Clock clock) {
        this.histogram = histogram;
        this.metaDataAvg =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_AVG, host, tags, clock);
        this.metaDataCount =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_COUNT, host, tags, clock);
        this.metaDataMedian =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MEDIAN, host, tags, clock);
        this.metaData95Percentile =
                new MetricMetaData(
                        MetricType.gauge, metricName + SUFFIX_95_PERCENTILE, host, tags, clock);
        this.metaDataMin =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MIN, host, tags, clock);
        this.metaDataMax =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MAX, host, tags, clock);
    }

    public void addTo(List<MetricSeries> series, long timestamp) {
        final HistogramStatistics statistics = histogram.getStatistics();

        // this selection is based on
        // https://docs.datadoghq.com/developers/metrics/types/?tab=histogram
        // we only exclude 'sum' (which is optional), because we cannot compute it
        // the semantics for count are also slightly different, because we don't reset it after a
        // report

        series.add(
                metaDataAvg.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value(statistics.getMean())
                                        .timestamp(timestamp))));
        series.add(
                metaDataCount.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value((double) histogram.getCount())
                                        .timestamp(timestamp))));
        series.add(
                metaDataMedian.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value(statistics.getQuantile(.5))
                                        .timestamp(timestamp))));
        series.add(
                metaData95Percentile.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value(statistics.getQuantile(.95))
                                        .timestamp(timestamp))));
        series.add(
                metaDataMin.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value((double) statistics.getMin())
                                        .timestamp(timestamp))));
        series.add(
                metaDataMax.points(
                        Collections.singletonList(
                                new MetricPoint()
                                        .value((double) statistics.getMax())
                                        .timestamp(timestamp))));
    }
}
