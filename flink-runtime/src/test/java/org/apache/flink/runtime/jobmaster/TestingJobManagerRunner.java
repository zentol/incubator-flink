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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherTest;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.SerializedValue;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of the {@link JobManagerRunner}.
 */
public class TestingJobManagerRunner implements JobManagerRunner {

	private final JobID jobId;

	private final boolean blockingTermination;

	private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

	private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture;

	private final CompletableFuture<Void> terminationFuture;

	public TestingJobManagerRunner(JobID jobId) {
		this(jobId, false);
	}

	public TestingJobManagerRunner(JobID jobId, boolean blockingTermination) {
		this.jobId = jobId;
		this.blockingTermination = blockingTermination;
		this.resultFuture = new CompletableFuture<>();
		this.jobMasterGatewayFuture = new CompletableFuture<>();
		this.terminationFuture = new CompletableFuture<>();

		terminationFuture.whenComplete((ignored, ignoredThrowable) -> resultFuture.completeExceptionally(new JobNotFinishedException(jobId)));
	}

	@Override
	public void start() throws Exception {
		jobMasterGatewayFuture.complete(new MockRunningJobMasterGateway());
	}

	@Override
	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		return jobMasterGatewayFuture;
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return resultFuture;
	}

	@Override
	public JobID getJobID() {
		return jobId;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (!blockingTermination) {
			terminationFuture.complete(null);
		}

		return terminationFuture;
	}

	public void completeResultFuture(ArchivedExecutionGraph archivedExecutionGraph) {
		resultFuture.complete(archivedExecutionGraph);
	}

	public void completeResultFutureExceptionally(Exception e) {
		resultFuture.completeExceptionally(e);
	}

	public void completeTerminationFuture() {
		terminationFuture.complete(null);
	}

	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	/**
	 * This mock gateway always returns a RUNNING job status.
	 * Needed for {@link DispatcherTest#testBlockingJobManagerRunner()}
	 */
	private static class MockRunningJobMasterGateway implements JobMasterGateway {

		@Override
		public CompletableFuture<Acknowledge> cancel(Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState) {
			return null;
		}

		@Override
		public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			JobVertexID vertexID,
			ExecutionAttemptID executionAttempt) {
			return null;
		}

		@Override
		public CompletableFuture<ExecutionState> requestPartitionState(
			IntermediateDataSetID intermediateResultId,
			ResultPartitionID partitionId) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			ResultPartitionID partitionID,
			Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> disconnectTaskManager(
			ResourceID resourceID,
			Exception cause) {
			return null;
		}

		@Override
		public void disconnectResourceManager(
			ResourceManagerId resourceManagerId,
			Exception cause) {

		}

		@Override
		public CompletableFuture<Collection<SlotOffer>> offerSlots(
			ResourceID taskManagerId,
			Collection<SlotOffer> slots,
			Time timeout) {
			return null;
		}

		@Override
		public void failSlot(
			ResourceID taskManagerId,
			AllocationID allocationId,
			Exception cause) {

		}

		@Override
		public CompletableFuture<RegistrationResponse> registerTaskManager(
			String taskManagerRpcAddress,
			UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
			Time timeout) {
			return null;
		}

		@Override
		public void heartbeatFromTaskManager(
			ResourceID resourceID,
			TaskExecutorToJobManagerHeartbeatPayload payload) {

		}

		@Override
		public void heartbeatFromResourceManager(ResourceID resourceID) {

		}

		@Override
		public CompletableFuture<JobDetails> requestJobDetails(
			Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
			return CompletableFuture.completedFuture(JobStatus.RUNNING);
		}

		@Override
		public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<String> triggerSavepoint(
			String targetDirectory,
			boolean cancelJob, Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<String> stopWithSavepoint(
			String targetDirectory,
			boolean advanceToEndOfEventTime, Time timeout) {
			return null;
		}

		@Override
		public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
			JobVertexID jobVertexId) {
			return null;
		}

		@Override
		public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {

		}

		@Override
		public CompletableFuture<Object> updateGlobalAggregate(
			String aggregateName,
			Object aggregand, byte[] serializedAggregationFunction) {
			return null;
		}

		@Override
		public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
			OperatorID operatorId,
			SerializedValue<CoordinationRequest> serializedRequest,
			Time timeout) {
			return null;
		}

		@Override
		public void acknowledgeCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			CheckpointMetrics checkpointMetrics,
			TaskStateSnapshot subtaskState) {

		}

		@Override
		public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {

		}

		@Override
		public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
			ExecutionAttemptID task,
			OperatorID operatorID,
			SerializedValue<OperatorEvent> event) {
			return null;
		}

		@Override
		public CompletableFuture<KvStateLocation> requestKvStateLocation(
			JobID jobId, String registrationName) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> notifyKvStateRegistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName,
			KvStateID kvStateId,
			InetSocketAddress kvStateServerAddress) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
			JobID jobId,
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName) {
			return null;
		}

		@Override
		public JobMasterId getFencingToken() {
			return null;
		}

		@Override
		public String getAddress() {
			return null;
		}

		@Override
		public String getHostname() {
			return null;
		}
	}
}
