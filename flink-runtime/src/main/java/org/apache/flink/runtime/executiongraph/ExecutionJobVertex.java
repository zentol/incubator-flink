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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer to the {@link
 * JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It contains an {@link
 * DefaultExecutionVertex} for each parallel instance of that operation.
 */
public interface ExecutionJobVertex
        extends AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

    List<OperatorIDPair> getOperatorIDs();

    void setMaxParallelism(int maxParallelismDerived);

    InternalExecutionGraphAccessor getGraph();

    JobVertex getJobVertex();

    boolean isMaxParallelismConfigured();

    JobID getJobId();

    IntermediateResult[] getProducedDataSets();

    InputSplitAssigner getSplitAssigner();

    SlotSharingGroup getSlotSharingGroup();

    @Override
    ExecutionVertex[] getTaskVertices();

    @Nullable
    CoLocationGroup getCoLocationGroup();

    List<IntermediateResult> getInputs();

    Collection<OperatorCoordinatorHolder> getOperatorCoordinators();

    Either<SerializedValue<TaskInformation>, PermanentBlobKey> getTaskInformationOrBlobKey()
            throws IOException;

    void connectToPredecessors(Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets)
            throws JobException;

    void cancel();

    CompletableFuture<Void> cancelWithFuture();

    CompletableFuture<Void> suspend();

    void fail(Throwable t);

    /**
     * A utility function that computes an "aggregated" state for the vertex.
     *
     * <p>This state is not used anywhere in the coordination, but can be used for display in
     * dashboards to as a summary for how the particular parallel operation represented by this
     * ExecutionJobVertex is currently behaving.
     *
     * <p>For example, if at least one parallel task is failed, the aggregate state is failed. If
     * not, and at least one parallel task is cancelling (or cancelled), the aggregate state is
     * cancelling (or cancelled). If all tasks are finished, the aggregate state is finished, and so
     * on.
     *
     * @param verticesPerState The number of vertices in each state (indexed by the ordinal of the
     *     ExecutionState values).
     * @param parallelism The parallelism of the ExecutionJobVertex
     * @return The aggregate state of this ExecutionJobVertex.
     */
    static ExecutionState getAggregateJobVertexState(int[] verticesPerState, int parallelism) {
        if (verticesPerState == null || verticesPerState.length != ExecutionState.values().length) {
            throw new IllegalArgumentException(
                    "Must provide an array as large as there are execution states.");
        }

        if (verticesPerState[ExecutionState.FAILED.ordinal()] > 0) {
            return ExecutionState.FAILED;
        }
        if (verticesPerState[ExecutionState.CANCELING.ordinal()] > 0) {
            return ExecutionState.CANCELING;
        } else if (verticesPerState[ExecutionState.CANCELED.ordinal()] > 0) {
            return ExecutionState.CANCELED;
        } else if (verticesPerState[ExecutionState.RUNNING.ordinal()] > 0) {
            return ExecutionState.RUNNING;
        } else if (verticesPerState[ExecutionState.FINISHED.ordinal()] > 0) {
            return verticesPerState[ExecutionState.FINISHED.ordinal()] == parallelism
                    ? ExecutionState.FINISHED
                    : ExecutionState.RUNNING;
        } else {
            // all else collapses under created
            return ExecutionState.CREATED;
        }
    }
}
