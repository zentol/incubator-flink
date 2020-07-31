package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class FailedInInitializationExecutionGraph extends ArchivedExecutionGraph {
	public FailedInInitializationExecutionGraph(
		JobID jobID,
		String jobName,
		Map<JobVertexID, ArchivedExecutionJobVertex> tasks,
		List<ArchivedExecutionJobVertex> verticesInCreationOrder,
		long[] stateTimestamps,
		JobStatus state,
		@Nullable ErrorInfo failureCause,
		String jsonPlan,
		StringifiedAccumulatorResult[] archivedUserAccumulators,
		Map<String, SerializedValue<OptionalFailure<Object>>> serializedUserAccumulators,
		ArchivedExecutionConfig executionConfig,
		boolean isStoppable,
		@Nullable CheckpointCoordinatorConfiguration jobCheckpointingConfiguration,
		@Nullable CheckpointStatsSnapshot checkpointStatsSnapshot,
		@Nullable String stateBackendName) {
		super(
			jobID,
			jobName,
			tasks,
			verticesInCreationOrder,
			stateTimestamps,
			state,
			failureCause,
			jsonPlan,
			archivedUserAccumulators,
			serializedUserAccumulators,
			executionConfig,
			isStoppable,
			jobCheckpointingConfiguration,
			checkpointStatsSnapshot,
			stateBackendName);
	}
}
