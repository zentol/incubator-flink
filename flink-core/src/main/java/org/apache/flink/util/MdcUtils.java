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

package org.apache.flink.util;

import org.apache.flink.api.common.JobID;

import org.slf4j.MDC;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkArgument;

// todo: tests

/** Utility class to manage common Flink attributes in {@link MDC} (only {@link JobID} ATM). */
public class MdcUtils {

    public static final String JOB_ID = "flink-job-id";

    /** Add the given {@link JobID} to {@link MDC}. */
    public static void addJobID(JobID jobID) {
        MDC.put(JOB_ID, jobID.toHexString());
    }

    /** Remove any {@link JobID} from {@link MDC}. */
    public static void removeJobID() {
        // todo: restore original jid if present
        MDC.remove(JOB_ID);
    }

    /**
     * Wrap the given {@link Runnable} so that the given {@link JobID} is added before its execution
     * and removed afterward.
     */
    public static Runnable wrapRunnable(JobID jobID, Runnable command) {
        return () -> {
            addJobID(jobID);
            try {
                command.run();
            } finally {
                removeJobID();
            }
        };
    }

    /**
     * Wrap the given {@link Callable} so that the given {@link JobID} is added before its execution
     * and removed afterward.
     */
    public static <T> Callable<T> wrapCallable(JobID jobID, Callable<T> command) {
        return () -> {
            addJobID(jobID);
            try {
                return command.call();
            } finally {
                removeJobID();
            }
        };
    }

    /**
     * Wrap the given {@link Executor} so that the given {@link JobID} is added before it executes
     * any submitted commands and removed afterward.
     */
    public static Executor wrapExecutor(JobID jobID, Executor executor) {
        checkArgument(!(executor instanceof JobIdLoggingExecutor));
        return new JobIdLoggingExecutor<>(executor, jobID);
    }

    /**
     * Wrap the given {@link ExecutorService} so that the given {@link JobID} is added before it
     * executes any submitted commands and removed afterward.
     */
    public static ExecutorService wrapExecutorService(JobID jobID, ExecutorService delegate) {
        checkArgument(!(delegate instanceof JobIdLoggingExecutorService));
        return new JobIdLoggingExecutorService<>(delegate, jobID);
    }

    /**
     * Wrap the given {@link ScheduledExecutorService} so that the given {@link JobID} is added
     * before it executes any submitted commands and removed afterward.
     */
    public static ScheduledExecutorService wrapScheduledExecutorService(
            JobID jobID, ScheduledExecutorService ses) {
        checkArgument(!(ses instanceof JobIdLoggingScheduledExecutorService));
        return new JobIdLoggingScheduledExecutorService(ses, jobID);
    }
}
