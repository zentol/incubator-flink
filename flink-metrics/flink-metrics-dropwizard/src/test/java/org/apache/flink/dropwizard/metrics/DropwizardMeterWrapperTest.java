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

import org.apache.flink.api.common.time.Time;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the DropwizardMeterWrapper.
 */
public class DropwizardMeterWrapperTest {

	@Test
	public void testWrapper() {
		SettableClock clock = new SettableClock();
		com.codahale.metrics.Meter dropwizardMeter = new Meter(clock);
		dropwizardMeter.mark(100L);
		// nano seconds
		clock.setCurrentTime(Time.minutes(1).toMilliseconds() * 1_000_000);

		DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);

		// dropwizard meters use an exponential moving average
		assertEquals(7.996993086896951, wrapper.getRate(), 0.00001);
		assertEquals(100L, wrapper.getCount());
	}

	@Test
	public void testMarkEvent() {
		com.codahale.metrics.Meter dropwizardMeter = new Meter();
		DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);
		wrapper.markEvent();

		assertThat(dropwizardMeter.getCount(), is(1L));
	}

	@Test
	public void testMarkEventN() {
		com.codahale.metrics.Meter dropwizardMeter = new Meter();
		DropwizardMeterWrapper wrapper = new DropwizardMeterWrapper(dropwizardMeter);
		wrapper.markEvent(10L);

		assertThat(dropwizardMeter.getCount(), is(10L));
	}

	private static class SettableClock extends Clock {

		private long currentTime = 0;

		void setCurrentTime(long time) {
			currentTime = time;
		}

		@Override
		public long getTick() {
			return currentTime;
		}
	}
}
