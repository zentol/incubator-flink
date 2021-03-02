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
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The ExecutionVertex is a parallel subtask of the execution. It may be executed once, or several
 * times, each of which time it spawns an {@link Execution}.
 */
public interface ExecutionVertex
        extends AccessExecutionVertex, Archiveable<ArchivedExecutionVertex> {
    JobID getJobId();

    ExecutionJobVertex getJobVertex();

    JobVertexID getJobvertexId();

    String getTaskName();

    int getTotalNumberOfParallelSubtasks();

    int getMaxParallelism();

    /**
     * Returns the current execution for this execution vertex.
     *
     * @return current execution
     */
    Execution getCurrentExecutionAttempt();

    ResourceProfile getResourceProfile();

    ExecutionVertexID getID();

    int getNumberOfInputs();

    ExecutionEdge[] getInputEdges(int input);

    ExecutionEdge[][] getAllInputEdges();

    InputSplit getNextInputSplit(String host);

    CompletableFuture<TaskManagerLocation> getCurrentTaskManagerLocationFuture();

    LogicalSlot getCurrentAssignedResource();

    ArchivedExecution getLatestPriorExecution();

    TaskManagerLocation getLatestPriorLocation();

    AllocationID getLatestPriorAllocation();

    InternalExecutionGraphAccessor getExecutionAccessor();

    Map<IntermediateResultPartitionID, IntermediateResultPartition> getProducedPartitions();

    void connectSource(
            int inputNumber, IntermediateResult source, JobEdge edge, int consumerNumber);

    Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocations();

    Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnState();

    Optional<TaskManagerLocation> getPreferredLocationBasedOnState();

    Collection<CompletableFuture<TaskManagerLocation>> getPreferredLocationsBasedOnInputs();

    void resetForNewExecution();

    void tryAssignResource(LogicalSlot slot);

    void deploy() throws JobException;

    @VisibleForTesting
    void deployToSlot(LogicalSlot slot) throws JobException;

    CompletableFuture<?> cancel();

    CompletableFuture<?> suspend();

    void fail(Throwable t);

    void markFailed(Throwable t);
}
