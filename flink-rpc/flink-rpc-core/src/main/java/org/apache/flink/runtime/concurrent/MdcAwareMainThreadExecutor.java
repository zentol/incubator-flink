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

package org.apache.flink.runtime.concurrent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.MdcUtils.wrapCallable;
import static org.apache.flink.util.MdcUtils.wrapRunnable;

/**
 * Wraps {@link ComponentMainThreadExecutor} so that all executions use the specified {@link
 * org.slf4j.MDC}.
 */
public class MdcAwareMainThreadExecutor implements ComponentMainThreadExecutor {
    private final ComponentMainThreadExecutor delegate;
    private final Map<String, String> contextData;

    public MdcAwareMainThreadExecutor(
            ComponentMainThreadExecutor delegate, Map<String, String> contextData) {
        this.delegate = delegate;
        this.contextData = contextData;
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(wrapRunnable(contextData, command));
    }

    @Override
    public void assertRunningInMainThread() {
        delegate.assertRunningInMainThread();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate.schedule(wrapRunnable(contextData, command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate.schedule(wrapCallable(contextData, callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        return delegate.scheduleAtFixedRate(
                wrapRunnable(contextData, command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return delegate.scheduleWithFixedDelay(
                wrapRunnable(contextData, command), initialDelay, delay, unit);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
