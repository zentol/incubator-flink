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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a Flink runtime Task.
 *
 * <p>Contains extra logic for adding operators.
 */
@Internal
public class TaskMetricGroup extends ComponentMetricGroup<TaskManagerJobMetricGroup> {

    private final Map<String, OperatorMetricGroup> operators = new HashMap<>();

    static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 80;

    private final TaskIOMetricGroup ioMetrics;

    private final TaskManagerMetaInfo taskManagerMetaInfo;
    private final JobMetaInfo jobMetaInfo;
    private final TaskMetaInfo taskMetaInfo;

    // ------------------------------------------------------------------------

    public TaskMetricGroup(
            MetricRegistry registry,
            TaskManagerJobMetricGroup parent,
            TaskManagerMetaInfo taskManagerMetaInfo,
            JobMetaInfo jobMetaInfo,
            TaskMetaInfo taskMetaInfo) {
        super(
                registry,
                registry.getScopeFormats()
                        .getTaskFormat()
                        .formatScope(taskManagerMetaInfo, jobMetaInfo, taskMetaInfo),
                parent);
        this.taskManagerMetaInfo = taskManagerMetaInfo;
        this.jobMetaInfo = jobMetaInfo;
        this.taskMetaInfo = taskMetaInfo;

        this.ioMetrics = new TaskIOMetricGroup(this);
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    public final TaskManagerJobMetricGroup parent() {
        return parent;
    }

    /**
     * Returns the TaskIOMetricGroup for this task.
     *
     * @return TaskIOMetricGroup for this task.
     */
    public TaskIOMetricGroup getIOMetricGroup() {
        return ioMetrics;
    }

    @Override
    protected QueryScopeInfo.TaskQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.TaskQueryScopeInfo(
                jobMetaInfo.getJobId().toString(),
                String.valueOf(taskMetaInfo.getVertexId()),
                taskMetaInfo.getSubtaskIndex());
    }

    // ------------------------------------------------------------------------
    //  operators and cleanup
    // ------------------------------------------------------------------------

    public OperatorMetricGroup getOrAddOperator(String operatorName) {
        return getOrAddOperator(
                OperatorID.fromJobVertexID(taskMetaInfo.getVertexId()), operatorName);
    }

    public OperatorMetricGroup getOrAddOperator(OperatorID operatorID, String operatorName) {
        final String truncatedOperatorName;
        if (operatorName != null && operatorName.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
            LOG.warn(
                    "The operator name {} exceeded the {} characters length limit and was truncated.",
                    operatorName,
                    METRICS_OPERATOR_NAME_MAX_LENGTH);
            truncatedOperatorName = operatorName.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
        } else {
            truncatedOperatorName = operatorName;
        }

        // unique OperatorIDs only exist in streaming, so we have to rely on the name for batch
        // operators
        final String key = operatorID + truncatedOperatorName;

        synchronized (this) {
            return operators.computeIfAbsent(
                    key,
                    operator ->
                            new OperatorMetricGroup(
                                    this.registry,
                                    this,
                                    taskManagerMetaInfo,
                                    jobMetaInfo,
                                    taskMetaInfo,
                                    new OperatorMetaInfo(operatorID, truncatedOperatorName)));
        }
    }

    @Override
    public void close() {
        super.close();

        parent.removeTaskMetricGroup(taskMetaInfo.getExecutionId());
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_TASK_VERTEX_ID, taskMetaInfo.getVertexId().toString());
        variables.put(ScopeFormat.SCOPE_TASK_NAME, taskMetaInfo.getTaskName());
        variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_ID, taskMetaInfo.getExecutionId().toString());
        variables.put(
                ScopeFormat.SCOPE_TASK_ATTEMPT_NUM,
                String.valueOf(taskMetaInfo.getAttemptNumber()));
        variables.put(
                ScopeFormat.SCOPE_TASK_SUBTASK_INDEX,
                String.valueOf(taskMetaInfo.getSubtaskIndex()));
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return operators.values();
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "task";
    }
}
