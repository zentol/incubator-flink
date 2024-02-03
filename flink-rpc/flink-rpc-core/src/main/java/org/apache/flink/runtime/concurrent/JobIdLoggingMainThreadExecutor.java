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

import org.apache.flink.api.common.JobID;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.MdcUtils.wrapCallable;
import static org.apache.flink.util.MdcUtils.wrapRunnable;

/** Logs {@link JobID} by adding it to {@link org.slf4j.MDC}. */
public class JobIdLoggingMainThreadExecutor implements ComponentMainThreadExecutor {
    private final ComponentMainThreadExecutor delegate;
    private final JobID jobID;

    private JobIdLoggingMainThreadExecutor(ComponentMainThreadExecutor delegate, JobID jobID) {
        this.delegate = delegate;
        this.jobID = jobID;
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(wrapRunnable(jobID, command));
    }

    @Override
    public void assertRunningInMainThread() {
        delegate.assertRunningInMainThread();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate.schedule(wrapRunnable(jobID, command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate.schedule(wrapCallable(jobID, callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        return delegate.scheduleAtFixedRate(
                wrapRunnable(jobID, command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return delegate.scheduleWithFixedDelay(
                wrapRunnable(jobID, command), initialDelay, delay, unit);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Wrap the given {@link ComponentMainThreadExecutor} so that the given {@link JobID} is added
     * before it executes any submitted commands and removed afterward.
     */
    public static ComponentMainThreadExecutor scopeToJob(
            JobID jobID, ComponentMainThreadExecutor e) {
        return new JobIdLoggingMainThreadExecutor(e, jobID);
    }
}
