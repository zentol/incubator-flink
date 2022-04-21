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
package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.util.clock.ManualClock;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** The unit test class for {@link ComponentClosingUtils}. */
public class ComponentClosingUtilsTest {
    private ManualClock clock;

    @Before
    public void setup() {
        clock = new ManualClock();
        ComponentClosingUtils.setClock(clock);
    }

    @Test
    public void testTryShutdownExecutorElegantlyWithoutForcefulShutdown() {
        MockExecutorService executor = new MockExecutorService(0);
        assertTrue(
                ComponentClosingUtils.tryShutdownExecutorElegantly(executor, Duration.ofDays(1)));
        assertEquals(0, executor.forcefullyShutdownCount);
    }

    @Test
    public void testTryShutdownExecutorElegantlyWithForcefulShutdown() {
        MockExecutorService executor = new MockExecutorService(5);
        assertFalse(
                ComponentClosingUtils.tryShutdownExecutorElegantly(executor, Duration.ofDays(1)));
        assertEquals(1, executor.forcefullyShutdownCount);
    }

    @Test
    public void testTryShutdownExecutorElegantlyTimeoutWithForcefulShutdown() {
        MockExecutorService executor = new MockExecutorService(5);
        executor.timeoutAfterNumForcefulShutdown(clock, 0);
        assertFalse(
                ComponentClosingUtils.tryShutdownExecutorElegantly(executor, Duration.ofDays(1)));
        assertEquals(1, executor.forcefullyShutdownCount);
    }

    @Test
    public void testTryShutdownExecutorElegantlyInterruptedWithForcefulShutdown() {
        MockExecutorService executor = new MockExecutorService(5);
        executor.interruptAfterNumForcefulShutdown(0);
        assertFalse(
                ComponentClosingUtils.tryShutdownExecutorElegantly(executor, Duration.ofDays(1)));
        assertEquals(1, executor.forcefullyShutdownCount);
    }

    @Test
    public void testShutdownExecutorForcefully() {
        MockExecutorService executor = new MockExecutorService(5);
        assertTrue(
                ComponentClosingUtils.shutdownExecutorForcefully(
                        executor, Duration.ofDays(1), false));
        assertEquals(5, executor.forcefullyShutdownCount);
    }

    @Test
    public void testShutdownExecutorForcefullyReachesTimeout() {
        MockExecutorService executor = new MockExecutorService(5);
        executor.timeoutAfterNumForcefulShutdown(clock, 1);
        assertFalse(
                ComponentClosingUtils.shutdownExecutorForcefully(
                        executor, Duration.ofDays(1), false));
        assertEquals(1, executor.forcefullyShutdownCount);
    }

    @Test
    public void testShutdownExecutorForcefullyNotInterruptable() {
        MockExecutorService executor = new MockExecutorService(5);
        executor.interruptAfterNumForcefulShutdown(1);
        assertTrue(
                ComponentClosingUtils.shutdownExecutorForcefully(
                        executor, Duration.ofDays(1), false));
        assertEquals(5, executor.forcefullyShutdownCount);
    }

    @Test
    public void testShutdownExecutorForcefullyInterruptable() {
        MockExecutorService executor = new MockExecutorService(5);
        executor.interruptAfterNumForcefulShutdown(1);
        assertFalse(
                ComponentClosingUtils.shutdownExecutorForcefully(
                        executor, Duration.ofDays(1), true));
        assertEquals(1, executor.forcefullyShutdownCount);
    }

    // ============== private class for testing ===============

    /** An executor class that behaves in an orchestrated way. */
    private static final class MockExecutorService
            extends ManuallyTriggeredScheduledExecutorService {
        private final int numRequiredForcefullyShutdown;
        private ManualClock clock;
        private int forcefullyShutdownCount;
        private int interruptAfterNumForcefulShutdown = Integer.MAX_VALUE;
        private int timeoutAfterNumForcefulShutdown = Integer.MAX_VALUE;

        private MockExecutorService(int numRequiredForcefullyShutdown) {
            this.numRequiredForcefullyShutdown = numRequiredForcefullyShutdown;
            forcefullyShutdownCount = 0;
        }

        @Override
        public @NotNull List<Runnable> shutdownNow() {
            forcefullyShutdownCount++;
            return super.shutdownNow();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (forcefullyShutdownCount < numRequiredForcefullyShutdown) {
                if (forcefullyShutdownCount >= timeoutAfterNumForcefulShutdown) {
                    clock.advanceTime(Duration.ofDays(100));
                }
                if (forcefullyShutdownCount >= interruptAfterNumForcefulShutdown) {
                    throw new InterruptedException();
                }
            }
            return super.awaitTermination(timeout, unit) && reachedForcefulShutdownCount();
        }

        @Override
        public boolean isTerminated() {
            return super.isTerminated() && reachedForcefulShutdownCount();
        }

        public void interruptAfterNumForcefulShutdown(int interruptAfterNumForcefulShutdown) {
            this.interruptAfterNumForcefulShutdown = interruptAfterNumForcefulShutdown;
        }

        public void timeoutAfterNumForcefulShutdown(
                ManualClock clock, int timeoutAfterNumForcefulShutdown) {
            this.clock = clock;
            this.timeoutAfterNumForcefulShutdown = timeoutAfterNumForcefulShutdown;
        }

        private boolean reachedForcefulShutdownCount() {
            return forcefullyShutdownCount >= numRequiredForcefullyShutdown;
        }
    }
}
