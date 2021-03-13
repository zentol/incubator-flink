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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;

/** A collection of safe drop-in replacements for existing {@link ComponentMetricGroup}s. */
public class UnregisteredMetricGroups {

    private static final JobManagerMetaInfo JOB_MANAGER_META_INFO =
            new JobManagerMetaInfo("UnregisteredHost");
    private static final TaskManagerMetaInfo TASK_MANAGER_META_INFO =
            new TaskManagerMetaInfo("UnregisteredHost", "0");
    private static final JobMetaInfo JOB_META_INFO =
            new JobMetaInfo(new JobID(0, 0), "UnregisteredJob");
    private static final TaskMetaInfo TASK_META_INFO =
            new TaskMetaInfo(
                    new JobVertexID(0, 0), new ExecutionAttemptID(), "UnregisteredTask", 0, 0);
    private static final OperatorMetaInfo OPERATOR_META_INFO =
            new OperatorMetaInfo(new OperatorID(0, 0), "UnregisteredOperator");

    private UnregisteredMetricGroups() {}

    public static ProcessMetricGroup createUnregisteredProcessMetricGroup() {
        return new UnregisteredProcessMetricGroup();
    }

    public static ResourceManagerMetricGroup createUnregisteredResourceManagerMetricGroup() {
        return new UnregisteredResourceManagerMetricGroup();
    }

    public static SlotManagerMetricGroup createUnregisteredSlotManagerMetricGroup() {
        return new UnregisteredSlotManagerMetricGroup();
    }

    public static JobManagerMetricGroup createUnregisteredJobManagerMetricGroup() {
        return new UnregisteredJobManagerMetricGroup();
    }

    public static JobManagerJobMetricGroup createUnregisteredJobManagerJobMetricGroup() {
        return new UnregisteredJobManagerJobMetricGroup();
    }

    public static TaskManagerMetricGroup createUnregisteredTaskManagerMetricGroup() {
        return new UnregisteredTaskManagerMetricGroup();
    }

    public static TaskManagerJobMetricGroup createUnregisteredTaskManagerJobMetricGroup() {
        return new UnregisteredTaskManagerJobMetricGroup();
    }

    public static TaskMetricGroup createUnregisteredTaskMetricGroup() {
        return new UnregisteredTaskMetricGroup();
    }

    public static OperatorMetricGroup createUnregisteredOperatorMetricGroup() {
        return new UnregisteredOperatorMetricGroup();
    }

    /** A safe drop-in replacement for {@link ProcessMetricGroup ProcessMetricGroups}. */
    public static class UnregisteredProcessMetricGroup extends ProcessMetricGroup {
        private static final String UNREGISTERED_HOST = "UnregisteredHost";

        public UnregisteredProcessMetricGroup() {
            super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
        }
    }

    /**
     * A safe drop-in replacement for {@link ResourceManagerMetricGroup
     * ResourceManagerMetricGroups}.
     */
    public static class UnregisteredResourceManagerMetricGroup extends ResourceManagerMetricGroup {
        private static final String UNREGISTERED_HOST = "UnregisteredHost";

        UnregisteredResourceManagerMetricGroup() {
            super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
        }
    }

    /** A safe drop-in replacement for {@link SlotManagerMetricGroup SlotManagerMetricGroups}. */
    public static class UnregisteredSlotManagerMetricGroup extends SlotManagerMetricGroup {
        private static final String UNREGISTERED_HOST = "UnregisteredHost";

        UnregisteredSlotManagerMetricGroup() {
            super(NoOpMetricRegistry.INSTANCE, UNREGISTERED_HOST);
        }
    }

    /** A safe drop-in replacement for {@link JobManagerMetricGroup}s. */
    public static class UnregisteredJobManagerMetricGroup extends JobManagerMetricGroup {

        private UnregisteredJobManagerMetricGroup() {
            super(NoOpMetricRegistry.INSTANCE, JOB_MANAGER_META_INFO);
        }

        @Override
        public JobManagerJobMetricGroup addJob(JobID jobId, String jobName) {
            return createUnregisteredJobManagerJobMetricGroup();
        }
    }

    /** A safe drop-in replacement for {@link JobManagerJobMetricGroup}s. */
    public static class UnregisteredJobManagerJobMetricGroup extends JobManagerJobMetricGroup {

        protected UnregisteredJobManagerJobMetricGroup() {
            super(
                    NoOpMetricRegistry.INSTANCE,
                    new UnregisteredJobManagerMetricGroup(),
                    JOB_MANAGER_META_INFO,
                    JOB_META_INFO);
        }
    }

    /** A safe drop-in replacement for {@link TaskManagerMetricGroup}s. */
    public static class UnregisteredTaskManagerMetricGroup extends TaskManagerMetricGroup {

        protected UnregisteredTaskManagerMetricGroup() {
            super(NoOpMetricRegistry.INSTANCE, TASK_MANAGER_META_INFO);
        }

        @Override
        public TaskMetricGroup addTaskForJob(
                final JobID jobId,
                final String jobName,
                final JobVertexID jobVertexId,
                final ExecutionAttemptID executionAttemptId,
                final String taskName,
                final int subtaskIndex,
                final int attemptNumber) {
            return createUnregisteredTaskMetricGroup();
        }
    }

    /** A safe drop-in replacement for {@link TaskManagerJobMetricGroup}s. */
    public static class UnregisteredTaskManagerJobMetricGroup extends TaskManagerJobMetricGroup {

        public UnregisteredTaskManagerJobMetricGroup() {
            super(
                    NoOpMetricRegistry.INSTANCE,
                    new UnregisteredTaskManagerMetricGroup(),
                    TASK_MANAGER_META_INFO,
                    JOB_META_INFO);
        }

        @Override
        public TaskMetricGroup addTask(
                final JobVertexID jobVertexId,
                final ExecutionAttemptID executionAttemptID,
                final String taskName,
                final int subtaskIndex,
                final int attemptNumber) {
            return createUnregisteredTaskMetricGroup();
        }
    }

    /** A safe drop-in replacement for {@link TaskMetricGroup}s. */
    public static class UnregisteredTaskMetricGroup extends TaskMetricGroup {

        protected UnregisteredTaskMetricGroup() {
            super(
                    NoOpMetricRegistry.INSTANCE,
                    new UnregisteredTaskManagerJobMetricGroup(),
                    TASK_MANAGER_META_INFO,
                    JOB_META_INFO,
                    TASK_META_INFO);
        }

        @Override
        public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String name) {
            return createUnregisteredOperatorMetricGroup();
        }
    }

    /** A safe drop-in replacement for {@link OperatorMetricGroup}s. */
    public static class UnregisteredOperatorMetricGroup extends OperatorMetricGroup {

        protected UnregisteredOperatorMetricGroup() {
            super(
                    NoOpMetricRegistry.INSTANCE,
                    new UnregisteredTaskMetricGroup(),
                    TASK_MANAGER_META_INFO,
                    JOB_META_INFO,
                    TASK_META_INFO,
                    OPERATOR_META_INFO);
        }
    }
}
