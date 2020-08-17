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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

// give factory into DispatcherJob that can create a runner
// move start dispatcher runner here

// move gateway ackquisiton here

//( move all dispatcher methods here)

// consider job as running when it gets leadership

// delegate the three method of InitializingJMGateway to this class
// add isRunning() (jm runner is running & gateway has been retrieved)
// then you can get the jobmastergateway (the actual gateway)

// happy case: request ArchivedExecutionGraph from JM on success, otherwise, FailedArchiveExecGraph
// AutoCloseableAsync call Dispatcher.removeJob()
// CompletableFuture<ArchivedExecutionGraph> getResultFuture();

// -------------------- new DispatcherJob -------------------------- //

/**
 * TODO.
 */
public final class DispatcherJob implements AutoCloseableAsync {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
	private final CompletableFuture<ArchivedExecutionGraph> jobResultFuture;

	private final JobGraph jobGraph;
	private final long initializationTimestamp;

	private JobStatus jobStatus = JobStatus.INITIALIZING;

	// if this future is != null, the job has been cancelled. Cancellation completes with this future.
	@Nullable
	private CompletableFuture<Acknowledge> cancellationFuture = null;

	private enum SubmissionType {
		INITIAL, RECOVERY
	}

	static DispatcherJob createForSubmission(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture, JobGraph jobGraph, long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobGraph, initializationTimestamp, SubmissionType.INITIAL);
	}

	static DispatcherJob createForRecovery(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture, JobGraph jobGraph, long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobGraph, initializationTimestamp, SubmissionType.RECOVERY);
		// TODO handle HA case
	}

	private DispatcherJob(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		JobGraph jobGraph, long initializationTimestamp, SubmissionType submissionType) {
		this.jobManagerRunnerFuture = jobManagerRunnerFuture;
		this.jobGraph = jobGraph;
		this.initializationTimestamp = initializationTimestamp;
		this.jobResultFuture = new CompletableFuture<>();

		FutureUtils.assertNoException(this.jobManagerRunnerFuture.handle((jobManagerRunner, throwable) -> {
			// this gets called when the JM has been initialized
			log.info("jm runner handle called", throwable);
			if (throwable == null) {
				if (cancellationFuture != null) {
					Preconditions.checkState(jobStatus == JobStatus.CANCELLING);
					log.warn("JobManager initialization has been cancelled for {}. Stopping JobManager.", jobGraph.getJobID());

					CompletableFuture<Acknowledge> cancelJobFuture = jobManagerRunner.getJobMasterGateway().thenCompose(
						gw -> gw.cancel(Time.seconds(60))); // TODO set better timeout
					cancelJobFuture.whenComplete((ack, cancelThrowable) -> {
						jobStatus = JobStatus.CANCELED;
					});
					// forward our cancellation future
					FutureUtils.forward(cancelJobFuture, cancellationFuture);
					// the JobManager will complete the job result future with state CANCELLED.
					FutureUtils.forward(jobManagerRunner.getResultFuture(), jobResultFuture);
				} else {
					jobStatus = JobStatus.RUNNING; // this status should never be exposed from the DispatcherJob. Only used internally for tracking runnin state
					FutureUtils.forward(jobManagerRunner.getResultFuture(), jobResultFuture);
				}
			} else { // failure during initialization
				jobStatus = JobStatus.FAILED;
				if (submissionType == SubmissionType.RECOVERY) {
					jobResultFuture.completeExceptionally(throwable);
				} else {
					jobResultFuture.complete(ArchivedExecutionGraph.createFromFailedInit(
						jobGraph,
						throwable,
						jobStatus,
						initializationTimestamp));
				}
			}
			return null;
		}));
	}

	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return jobResultFuture;
	}

	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		if (isRunning()) {
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobDetails(timeout));
		} else {
			int[] tasksPerState = new int[ExecutionState.values().length];
			return CompletableFuture.completedFuture(new JobDetails(jobGraph.getJobID(),
				jobGraph.getName(),
				initializationTimestamp,
				0,
				0,
				jobStatus,
				0,
				tasksPerState,
				jobGraph.getVerticesAsArray().length));
		}
	}

	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		if (isRunning()) {
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
		} else {
			return cancelInternal();
		}
	}

	private CompletableFuture<Acknowledge> cancelInternal() {
		log.debug("cancelInternal");
		jobStatus = JobStatus.CANCELLING;
		this.cancellationFuture = new CompletableFuture<>();
		return cancellationFuture;
	}

	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		if (isRunning()) {
			log.debug("requestJobStatus: gateway");
			return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobStatus(timeout));
		} else {
			log.debug("requestJobStatus: self");
			return CompletableFuture.completedFuture(jobStatus);
		}
	}

	public boolean isRunning() {
		// todo revisit definition of isRunning. alternative definition: once gateway is available = when is leader
		return jobStatus == JobStatus.RUNNING;
	}

	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		Preconditions.checkState(isRunning(), "JobMaster Gateway is not available during initialization");
		try {
			return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
		} catch (Throwable e) {
			throw new IllegalStateException("JobMaster gateway is not available", e);
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		log.debug("closeAsync: running=" + isRunning() + " isDone=" + jobManagerRunnerFuture.isDone());
		switch(jobStatus) {
			case RUNNING:
				return jobManagerRunnerFuture.thenAccept(AutoCloseableAsync::closeAsync);
			case CANCELED:
			case FAILED:
				return CompletableFuture.completedFuture(null);
			case INITIALIZING:
				return cancelInternal().thenApply(ack -> null);
			default:
				throw new IllegalStateException("Job is in a status that is not covered by the dispatcher job: " + jobStatus);

		}
		/*if (isRunning()) {
			return jobManagerRunnerFuture.thenAccept(AutoCloseableAsync::closeAsync);
		} else if (jobManagerRunnerFuture.isDone()) {
			// if this precondition fails, then an assumption in the jobManagerRunnerFuture.handle() is wrong
			// we assume that is !isRunning() && jobManagerRunnerFuture.isDone == error during initialization.
			Preconditions.checkState(jobManagerRunnerFuture.isCompletedExceptionally(), "Expecting error during initialization");
			return CompletableFuture.completedFuture(null);
		} else {
			return cancelInternal().thenApply(ack -> null);
		} */
	}

}
