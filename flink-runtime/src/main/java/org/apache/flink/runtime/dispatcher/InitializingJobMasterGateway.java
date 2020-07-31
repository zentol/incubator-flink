package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
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

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * This gateway will be used internally by the Dispatcher while the JobMaster is still initializing.
 */
@Internal
public class InitializingJobMasterGateway implements JobMasterGateway {
	private final CompletableFuture<JobManagerRunner> initializingJobManager;
	private final JobDetails jobDetails = null; //TODO

	public InitializingJobMasterGateway(CompletableFuture<JobManagerRunner> initializingJobManager) {
		this.initializingJobManager = initializingJobManager;
	}

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		initializingJobManager.cancel(true);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(
		Time timeout) {
		// TODO field is null
		return CompletableFuture.completedFuture(jobDetails);
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		if (!initializingJobManager.isDone()) {
			return CompletableFuture.completedFuture(JobStatus.INITIALIZING);
		}

		if (initializingJobManager.isCancelled()) {
			return CompletableFuture.completedFuture(JobStatus.CANCELLING);
		}
		// we expect the Dispatcher to look up this job in the archive on an exception.
		return FutureUtils.completedExceptionally(new RuntimeException("Unknown job status"));
	}

	// ----------------- All operations below are not supported ----------------- //

	private static final String EXCEPTION_MESSAGE = "This operation is not supported on an initializing JobManager";

	private static void throwUnsupportedException() {
		throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
	}

	private static <T> CompletableFuture<T> getUnsupportedExceptionFuture() {
		// This error happens when the JobManager is still initializing. We are not able to
		// process this request, because we can not forward it to the JM yet.
		return FutureUtils.completedExceptionally(new UnsupportedOperationException(EXCEPTION_MESSAGE));
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(
		Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
		JobVertexID vertexID,
		ExecutionAttemptID executionAttempt) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(
		IntermediateDataSetID intermediateResultId,
		ResultPartitionID partitionId) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
		ResultPartitionID partitionID,
		Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(
		ResourceID resourceID,
		Exception cause) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public void disconnectResourceManager(
		ResourceManagerId resourceManagerId,
		Exception cause) {
		throwUnsupportedException();
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
		ResourceID taskManagerId,
		Collection<SlotOffer> slots,
		Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public void failSlot(
		ResourceID taskManagerId,
		AllocationID allocationId,
		Exception cause) {
		throwUnsupportedException();
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(
		String taskManagerRpcAddress,
		UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
		Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public void heartbeatFromTaskManager(
		ResourceID resourceID,
		TaskExecutorToJobManagerHeartbeatPayload payload) {
		throwUnsupportedException();
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID resourceID) {
		throwUnsupportedException();
	}


	@Override
	public CompletableFuture<String> triggerSavepoint(
		@Nullable String targetDirectory,
		boolean cancelJob, Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
		@Nullable String targetDirectory,
		boolean advanceToEndOfEventTime, Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
		JobVertexID jobVertexId) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public void notifyAllocationFailure(AllocationID allocationID, Exception cause) {
		throwUnsupportedException();
	}

	@Override
	public CompletableFuture<Object> updateGlobalAggregate(
		String aggregateName,
		Object aggregand, byte[] serializedAggregationFunction) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
		OperatorID operatorId,
		SerializedValue<CoordinationRequest> serializedRequest,
		Time timeout) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public void acknowledgeCheckpoint(
		JobID jobID,
		ExecutionAttemptID executionAttemptID,
		long checkpointId,
		CheckpointMetrics checkpointMetrics,
		TaskStateSnapshot subtaskState) {
		throwUnsupportedException();
	}

	@Override
	public void declineCheckpoint(DeclineCheckpoint declineCheckpoint) {
		throwUnsupportedException();
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
		ExecutionAttemptID task,
		OperatorID operatorID,
		SerializedValue<OperatorEvent> event) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(
		JobID jobId, String registrationName) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName,
		KvStateID kvStateId,
		InetSocketAddress kvStateServerAddress) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
		JobID jobId,
		JobVertexID jobVertexId,
		KeyGroupRange keyGroupRange,
		String registrationName) {
		return getUnsupportedExceptionFuture();
	}

	@Override
	public JobMasterId getFencingToken() {
		throwUnsupportedException();
		return null;
	}

	@Override
	public String getAddress() {
		throwUnsupportedException();
		return null;
	}

	@Override
	public String getHostname() {
		throwUnsupportedException();
		return null;
	}
}
