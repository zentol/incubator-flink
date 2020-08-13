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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.InitializingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * This gateway will be used internally by the Dispatcher while the JobMaster is still initializing.
 */
@Internal
public class InitializingJobMasterGatewayImpl implements InitializingJobMasterGateway {
	private static final Logger LOG = LoggerFactory.getLogger(InitializingJobMasterGatewayImpl.class);

	private final CompletableFuture<JobManagerRunner> initializingJobManager;
	private final JobID jobId;
	private final String jobName;
	private final int jobNumTasks;
	private final CompletableFuture<Void> initializationErrorHandlingFuture;

	public InitializingJobMasterGatewayImpl(
		CompletableFuture<JobManagerRunner> initializingJobManager,
		CompletableFuture<Void> initializationErrorHandlingFuture,
		JobGraph jobGraph) {
		this.initializingJobManager = initializingJobManager;
		this.initializationErrorHandlingFuture = initializationErrorHandlingFuture;
		jobId = jobGraph.getJobID();
		jobName = jobGraph.getName();
		jobNumTasks = jobGraph.getVerticesAsArray().length;
	}

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		// cancel the JM initialization future & wait for the error handling to be completed
		return CompletableFuture.supplyAsync(() -> {
			initializingJobManager.cancel(true);
			LOG.debug("JM future cancelled");
			return Acknowledge.get();
		}).thenCombine(initializationErrorHandlingFuture, (ign, ore) -> Acknowledge.get());
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(
		Time timeout) {
		int[] tasksPerState = new int[ExecutionState.values().length];
		JobStatus status = JobStatus.INITIALIZING;
		if (initializingJobManager.isCancelled()) {
			status = JobStatus.CANCELED;
		}
		return CompletableFuture.completedFuture(new JobDetails(jobId, jobName, 0, 0, 0, status, 0, tasksPerState, jobNumTasks));
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		return CompletableFuture.completedFuture(JobStatus.INITIALIZING);
	}
}
