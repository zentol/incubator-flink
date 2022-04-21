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
package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for {@link MetricsFilterParameter}. */
public class MetricsFilterParameterTest extends TestLogger {

    private MetricsFilterParameter metricsFilterParameter;

    @Before
    public void setUp() {
        metricsFilterParameter = new MetricsFilterParameter();
    }

    @Test
    public void testIsOptionalParameter() {
        assertFalse(metricsFilterParameter.isMandatory());
    }

    @Test
    public void testConversions() {
        assertThat(metricsFilterParameter.convertValueToString("test"), equalTo("test"));
        assertThat(metricsFilterParameter.convertStringToValue("test"), equalTo("test"));
    }
}
