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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/** Metadata for a task. */
public class TaskMetaInfo {

    /**
     * The execution Id uniquely identifying the executed task represented by this metrics group.
     */
    private final ExecutionAttemptID executionId;

    private final JobVertexID vertexId;
    private final String taskName;
    private final int subtaskIndex;
    private final int attemptNumber;

    public TaskMetaInfo(
            JobVertexID vertexId,
            ExecutionAttemptID executionId,
            String taskName,
            int subtaskIndex,
            int attemptNumber) {
        this.executionId = executionId;
        this.vertexId = vertexId;
        this.taskName = taskName;
        this.subtaskIndex = subtaskIndex;
        this.attemptNumber = attemptNumber;
    }

    public ExecutionAttemptID getExecutionId() {
        return executionId;
    }

    public JobVertexID getVertexId() {
        return vertexId;
    }

    public String getTaskName() {
        return taskName;
    }

    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }
}
