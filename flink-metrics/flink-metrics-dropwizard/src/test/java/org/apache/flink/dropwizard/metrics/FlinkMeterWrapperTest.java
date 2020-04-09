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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestMeter;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


/**
 * Tests for the FlinkMeterWrapper.
 */
public class FlinkMeterWrapperTest {

	private static final double DELTA = 0.0001;

	@Test
	public void testWrapper() {
		Meter meter = new TestMeter();

		FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
		assertEquals(0, wrapper.getMeanRate(), DELTA);
		assertEquals(5, wrapper.getOneMinuteRate(), DELTA);
		assertEquals(0, wrapper.getFiveMinuteRate(), DELTA);
		assertEquals(0, wrapper.getFifteenMinuteRate(), DELTA);
		assertEquals(100L, wrapper.getCount());
	}

	@Test
	public void testMarkOneEvent() {
		Counter backingCounter = new SimpleCounter();
		Meter meter = new MeterView(backingCounter);

		FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
		wrapper.mark();

		assertThat(backingCounter.getCount(), is(1L));
	}

	@Test
	public void testMarkSeveralEvents() {
		Counter backingCounter = new SimpleCounter();
		Meter meter = new MeterView(backingCounter);

		FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
		wrapper.mark(5);

		assertThat(backingCounter.getCount(), is(5L));
	}
}
