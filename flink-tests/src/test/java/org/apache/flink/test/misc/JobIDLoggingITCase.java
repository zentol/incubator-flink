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

package org.apache.flink.test.misc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.TestLogger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/**
 * Tests adding of {@link JobID} to logs (via {@link org.slf4j.MDC}) in the most important cases.
 */
public class JobIDLoggingITCase extends TestLogger {
    private static final Logger logger = LoggerFactory.getLogger(JobIDLoggingITCase.class);

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void testJobIDLogging() throws Exception {
        LoggerContext ctx = LoggerContext.getContext(false);
        org.apache.logging.log4j.core.config.Configuration originalConfiguration =
                ctx.getConfiguration();
        ListAppender appender = new ListAppender("list-appender");
        JobID jobID = null;
        try {
            setupLogging(appender, ctx, Level.DEBUG);
            jobID = runJob();
        } finally {
            if (jobID != null) {
                miniClusterResource.getClusterClient().cancel(jobID).get();
            }
            restoreLogging(ctx, originalConfiguration);
            appender.stop();
        }

        assertJobIDPresent(jobID, appender.getEvents());
    }

    private static void assertJobIDPresent(JobID jobID, List<LogEvent> events) {
        // NOTE: most of the assertions are empirical, such as
        // - which classes are important
        // - how many messages to expect
        // - which log patterns to ignore
        Map<String, List<LogEvent>> eventsByLogger =
                events.stream().collect(Collectors.groupingBy(LogEvent::getLoggerName));

        assertJobIDPresent(eventsByLogger, CheckpointCoordinator.class, jobID, 3);
        assertJobIDPresent(eventsByLogger, StreamTask.class, jobID, 6);
        assertJobIDPresent(eventsByLogger, Task.class, jobID, 13);
        assertJobIDPresent(
                eventsByLogger,
                // this class is private
                "org.apache.flink.streaming.runtime.tasks.AsyncCheckpointRunnable",
                jobID,
                1);
        assertJobIDPresent(eventsByLogger, ExecutionGraph.class, jobID, 10);
        assertJobIDPresent(
                eventsByLogger,
                JobMaster.class,
                jobID,
                15,
                "Registration at ResourceManager.*",
                "Registration with ResourceManager.*",
                "Resolved ResourceManager address.*");
        long totalMessagesWithJobID =
                events.stream()
                        .filter(e -> e.getContextData().containsKey(MdcUtils.JOB_ID))
                        .count();
        assertTrue(
                "at least " + .75f + " of all events are expected to have job id",
                (float) totalMessagesWithJobID / events.size() > .75f);
    }

    private static void assertJobIDPresent(
            Map<String, List<LogEvent>> map,
            Class<?> aClass,
            JobID jobid,
            int expectedLogMessages,
            String... ignPatterns) {
        assertJobIDPresent(map, aClass.getName(), jobid, expectedLogMessages, ignPatterns);
    }

    private static void assertJobIDPresent(
            Map<String, List<LogEvent>> map,
            String loggerName,
            JobID jobid,
            int expectedLogMessages,
            String... ignPatterns) {
        final List<LogEvent> events = map.getOrDefault(loggerName, Collections.emptyList());
        checkState(
                events.size() >= expectedLogMessages,
                "Too few log events recorded for %s (%s) - this must be a bug in the test code",
                loggerName,
                events.size());

        final List<LogEvent> eventsWithMissingJobId = new ArrayList<>();
        final List<LogEvent> eventsWithWrongJobId = new ArrayList<>();
        final List<LogEvent> ignoredEvents = new ArrayList<>();
        final List<Pattern> ignorePatterns =
                Arrays.stream(ignPatterns).map(Pattern::compile).collect(Collectors.toList());

        for (LogEvent e : events) {
            if (e.getContextData().containsKey(MdcUtils.JOB_ID)) {
                if (!Objects.equals(
                        e.getContextData().getValue(MdcUtils.JOB_ID), jobid.toHexString())) {
                    eventsWithWrongJobId.add(e);
                }
            } else if (matchesAny(ignorePatterns, e.getMessage().getFormattedMessage())) {
                ignoredEvents.add(e);
            } else {
                eventsWithMissingJobId.add(e);
            }
        }
        logger.debug(
                "checked events for {}:\n  {};\n  ignored: {},\n  wrong job id: {},\n  missing job id: {}",
                loggerName,
                events,
                ignoredEvents,
                eventsWithWrongJobId,
                eventsWithMissingJobId);
        assertTrue(
                "events with a wrong Job ID recorded for "
                        + loggerName
                        + ": "
                        + eventsWithWrongJobId,
                eventsWithWrongJobId.isEmpty());
        assertTrue(
                "too many events without Job ID recorded for "
                        + loggerName
                        + ": "
                        + eventsWithMissingJobId,
                eventsWithMissingJobId.isEmpty());
    }

    private static boolean matchesAny(List<Pattern> patternStream, String message) {
        return patternStream.stream().anyMatch(p -> p.matcher(message).matches());
    }

    private static void restoreLogging(
            LoggerContext ctx,
            org.apache.logging.log4j.core.config.Configuration originalConfiguration) {
        ctx.setConfigLocation(originalConfiguration.getConfigurationSource().getURI());
    }

    private static void setupLogging(ListAppender appender, LoggerContext ctx, Level level) {
        LoggerConfig root = ctx.getConfiguration().getRootLogger();
        root.getAppenders().keySet().forEach(root::removeAppender);
        appender.start();
        root.setLevel(level); // ORDER MATTERS!
        ctx.getRootLogger().addAppender(appender);
    }

    private static JobID runJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE).addSink(new DiscardingSink<>());
        JobID jobId =
                miniClusterResource
                        .getClusterClient()
                        .submitJob(env.getStreamGraph().getJobGraph())
                        .get();
        CommonTestUtils.waitForAllTaskRunning(miniClusterResource.getMiniCluster(), jobId, true);
        miniClusterResource.getMiniCluster().triggerCheckpoint(jobId).get();
        return jobId;
    }
}
