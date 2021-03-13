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

package org.apache.flink.runtime.metrics.dump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Container for scope related information as required by the MetricQueryService. */
public abstract class QueryScopeInfo {
    /**
     * Categories to be returned by {@link QueryScopeInfo#getCategory()} to avoid instanceof checks.
     */
    public static final byte INFO_CATEGORY_JM = 0;

    public static final byte INFO_CATEGORY_TM = 1;
    public static final byte INFO_CATEGORY_JOB = 2;
    public static final byte INFO_CATEGORY_TASK = 3;
    public static final byte INFO_CATEGORY_OPERATOR = 4;

    /** The remaining scope not covered by specific fields. */
    public final String scope;

    private QueryScopeInfo(String scope) {
        this.scope = scope;
    }

    /**
     * Create a copy of this QueryScopeInfo and append the given scope.
     *
     * @param userScope scope to append
     * @return modified copy of this QueryScopeInfo
     */
    public abstract QueryScopeInfo copy(String userScope);

    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(scope);
        out.writeByte(getCategory());
        writeVariablesTo(out);
    }

    public static QueryScopeInfo readFrom(DataInput dis) throws IOException {
        String scope = dis.readUTF();
        byte cat = dis.readByte();

        switch (cat) {
            case INFO_CATEGORY_JM:
                return QueryScopeInfo.JobManagerQueryScopeInfo.readFrom(dis, scope);
            case INFO_CATEGORY_TM:
                return QueryScopeInfo.TaskManagerQueryScopeInfo.readFrom(dis, scope);
            case INFO_CATEGORY_JOB:
                return QueryScopeInfo.JobQueryScopeInfo.readFrom(dis, scope);
            case INFO_CATEGORY_TASK:
                return QueryScopeInfo.TaskQueryScopeInfo.readFrom(dis, scope);
            case INFO_CATEGORY_OPERATOR:
                return QueryScopeInfo.OperatorQueryScopeInfo.readFrom(dis, scope);
            default:
                throw new IOException("Unknown scope category: " + cat);
        }
    }

    protected abstract void writeVariablesTo(DataOutput out) throws IOException;

    /**
     * Returns the category for this QueryScopeInfo.
     *
     * @return category
     */
    public abstract byte getCategory();

    @Override
    public String toString() {
        return "QueryScopeInfo{"
                + "scope='"
                + scope
                + '\''
                + ", category='"
                + getCategory()
                + '\''
                + '}';
    }

    protected String concatScopes(String additionalScope) {
        return scope.isEmpty() ? additionalScope : scope + "." + additionalScope;
    }

    /** Container for the job manager scope. Stores no additional information. */
    public static class JobManagerQueryScopeInfo extends QueryScopeInfo {
        public JobManagerQueryScopeInfo() {
            super("");
        }

        public JobManagerQueryScopeInfo(String scope) {
            super(scope);
        }

        @Override
        public JobManagerQueryScopeInfo copy(String additionalScope) {
            return new JobManagerQueryScopeInfo(concatScopes(additionalScope));
        }

        @Override
        protected void writeVariablesTo(DataOutput out) throws IOException {}

        static JobManagerQueryScopeInfo readFrom(DataInput dis, String scope) {
            return new JobManagerQueryScopeInfo(scope);
        }

        @Override
        public byte getCategory() {
            return INFO_CATEGORY_JM;
        }
    }

    /** Container for the task manager scope. Stores the ID of the task manager. */
    public static class TaskManagerQueryScopeInfo extends QueryScopeInfo {
        public final String taskManagerID;

        public TaskManagerQueryScopeInfo(String taskManagerId) {
            this(taskManagerId, "");
        }

        public TaskManagerQueryScopeInfo(String taskManagerId, String scope) {
            super(scope);
            this.taskManagerID = taskManagerId;
        }

        @Override
        public TaskManagerQueryScopeInfo copy(String additionalScope) {
            return new TaskManagerQueryScopeInfo(this.taskManagerID, concatScopes(additionalScope));
        }

        @Override
        protected void writeVariablesTo(DataOutput out) throws IOException {
            out.writeUTF(taskManagerID);
        }

        static TaskManagerQueryScopeInfo readFrom(DataInput dis, String scope) throws IOException {
            String tmID = dis.readUTF();
            return new QueryScopeInfo.TaskManagerQueryScopeInfo(tmID, scope);
        }

        @Override
        public byte getCategory() {
            return INFO_CATEGORY_TM;
        }
    }

    /** Container for the job scope. Stores the ID of the job. */
    public static class JobQueryScopeInfo extends QueryScopeInfo {
        public final String jobID;

        public JobQueryScopeInfo(String jobID) {
            this(jobID, "");
        }

        public JobQueryScopeInfo(String jobID, String scope) {
            super(scope);
            this.jobID = jobID;
        }

        @Override
        public JobQueryScopeInfo copy(String additionalScope) {
            return new JobQueryScopeInfo(this.jobID, concatScopes(additionalScope));
        }

        @Override
        protected void writeVariablesTo(DataOutput out) throws IOException {
            out.writeUTF(jobID);
        }

        static JobQueryScopeInfo readFrom(DataInput dis, String scope) throws IOException {
            final String jobId = dis.readUTF();
            return new QueryScopeInfo.JobQueryScopeInfo(jobId, scope);
        }

        @Override
        public byte getCategory() {
            return INFO_CATEGORY_JOB;
        }
    }

    /** Container for the task scope. Stores the ID of the job/vertex and subtask index. */
    public static class TaskQueryScopeInfo extends QueryScopeInfo {
        public final String jobID;
        public final String vertexID;
        public final int subtaskIndex;

        public TaskQueryScopeInfo(String jobID, String vertexid, int subtaskIndex) {
            this(jobID, vertexid, subtaskIndex, "");
        }

        public TaskQueryScopeInfo(String jobID, String vertexid, int subtaskIndex, String scope) {
            super(scope);
            this.jobID = jobID;
            this.vertexID = vertexid;
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public TaskQueryScopeInfo copy(String additionalScope) {
            return new TaskQueryScopeInfo(
                    this.jobID, this.vertexID, this.subtaskIndex, concatScopes(additionalScope));
        }

        @Override
        protected void writeVariablesTo(DataOutput out) throws IOException {
            out.writeUTF(jobID);
            out.writeUTF(vertexID);
            out.writeInt(subtaskIndex);
        }

        static TaskQueryScopeInfo readFrom(DataInput dis, String scope) throws IOException {
            final String jobID = dis.readUTF();
            final String vertexID = dis.readUTF();
            final int subtaskIndex = dis.readInt();
            return new QueryScopeInfo.TaskQueryScopeInfo(jobID, vertexID, subtaskIndex, scope);
        }

        @Override
        public byte getCategory() {
            return INFO_CATEGORY_TASK;
        }
    }

    /**
     * Container for the operator scope. Stores the ID of the job/vertex, the subtask index and the
     * name of the operator.
     */
    public static class OperatorQueryScopeInfo extends QueryScopeInfo {
        public final String jobID;
        public final String vertexID;
        public final int subtaskIndex;
        public final String operatorName;

        public OperatorQueryScopeInfo(
                String jobID, String vertexid, int subtaskIndex, String operatorName) {
            this(jobID, vertexid, subtaskIndex, operatorName, "");
        }

        public OperatorQueryScopeInfo(
                String jobID,
                String vertexid,
                int subtaskIndex,
                String operatorName,
                String scope) {
            super(scope);
            this.jobID = jobID;
            this.vertexID = vertexid;
            this.subtaskIndex = subtaskIndex;
            this.operatorName = operatorName;
        }

        @Override
        public OperatorQueryScopeInfo copy(String additionalScope) {
            return new OperatorQueryScopeInfo(
                    this.jobID,
                    this.vertexID,
                    this.subtaskIndex,
                    this.operatorName,
                    concatScopes(additionalScope));
        }

        @Override
        protected void writeVariablesTo(DataOutput out) throws IOException {
            out.writeUTF(jobID);
            out.writeUTF(vertexID);
            out.writeInt(subtaskIndex);
            out.writeUTF(operatorName);
        }

        static OperatorQueryScopeInfo readFrom(DataInput dis, String scope) throws IOException {
            final String jobID = dis.readUTF();
            final String vertexID = dis.readUTF();
            final int subtaskIndex = dis.readInt();
            String operatorName = dis.readUTF();
            return new QueryScopeInfo.OperatorQueryScopeInfo(
                    jobID, vertexID, subtaskIndex, operatorName, scope);
        }

        @Override
        public byte getCategory() {
            return INFO_CATEGORY_OPERATOR;
        }
    }
}
