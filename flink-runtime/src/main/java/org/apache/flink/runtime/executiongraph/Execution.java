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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single
 * execution of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 *
 * <p>In several points of the code, we need to deal with possible concurrent state changes and
 * actions. For example, while the call to deploy a task (send it to the TaskManager) happens, the
 * task gets cancelled.
 *
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to
 * running) such that it is guaranteed that any "cancel command" will only pick up after deployment
 * is done and that the "cancel command" call will never overtake the deploying call.
 *
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their
 * locking behavior, it may even result in distributed deadlocks (unless carefully avoided). We
 * therefore use atomic state updates and occasional double-checking to ensure that the state after
 * a completed call is as expected, and trigger correcting actions if it is not. Many actions are
 * also idempotent (like canceling).
 */
public interface Execution
        extends AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {
    ExecutionVertex getVertex();

    @Nullable
    AllocationID getAssignedAllocationID();

    CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture();

    LogicalSlot getAssignedResource();

    Optional<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptor(
            IntermediateResultPartitionID id);

    boolean tryAssignResource(LogicalSlot logicalSlot);

    InputSplit getNextInputSplit();

    boolean isFinished();

    @Nullable
    JobManagerTaskRestore getTaskRestore();

    void setInitialState(@Nullable JobManagerTaskRestore taskRestore);

    CompletableFuture<?> getReleaseFuture();

    CompletableFuture<Execution> registerProducedPartitions(
            TaskManagerLocation location, boolean notifyPartitionDataAvailable);

    void deploy() throws JobException;

    void cancel();

    CompletableFuture<?> suspend();

    void notifyCheckpointComplete(long checkpointId, long timestamp);

    void notifyCheckpointAborted(long abortCheckpointId, long timestamp);

    void triggerCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions);

    void triggerSynchronousSavepoint(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            boolean advanceToEndOfEventTime);

    CompletableFuture<Acknowledge> sendOperatorEvent(
            OperatorID operatorId, SerializedValue<OperatorEvent> event);

    @VisibleForTesting
    CompletableFuture<Collection<TaskManagerLocation>> calculatePreferredLocations(
            LocationPreferenceConstraint locationPreferenceConstraint);

    void transitionState(ExecutionState targetState);

    String getVertexWithAttempt();

    void setAccumulators(Map<String, Accumulator<?, ?>> userAccumulators);

    Map<String, Accumulator<?, ?>> getUserAccumulators();
}
