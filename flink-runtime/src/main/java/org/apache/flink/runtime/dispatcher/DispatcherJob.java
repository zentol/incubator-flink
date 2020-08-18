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
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction used by the {@link Dispatcher} to manage JobManagers, in
 * particular during initialization.
 * While a job is initializing, the JobMasterGateway is not available. A small subset
 * of the methods of the JobMasterGateway necessary during initialization are provided
 * by this class (job details, cancel).
 */
public final class DispatcherJob implements AutoCloseableAsync {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
	private final CompletableFuture<ArchivedExecutionGraph> jobResultFuture;

	private final Object lock = new Object();

	private final JobGraph jobGraph;
	private final long initializationTimestamp;

	// internal field to track job status during initialization. Is not updated anymore after
	// job is initialized, cancelled or failed.
	@GuardedBy("lock")
	private JobStatus jobStatus = JobStatus.INITIALIZING;

	@Nullable
	@GuardedBy("lock")
	private CompletableFuture<Acknowledge> cancellationFuture = null;

	private enum SubmissionType {
		INITIAL, RECOVERY
	}

	static DispatcherJob createForSubmission(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture, JobGraph jobGraph, long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobGraph, initializationTimestamp, SubmissionType.INITIAL);
	}

	static DispatcherJob createForRecovery(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture, JobGraph jobGraph, long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobGraph, initializationTimestamp, SubmissionType.RECOVERY);
	}

	private DispatcherJob(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		JobGraph jobGraph, long initializationTimestamp, SubmissionType submissionType) {
		this.jobManagerRunnerFuture = jobManagerRunnerFuture;
		this.jobGraph = jobGraph;
		this.initializationTimestamp = initializationTimestamp;
		this.jobResultFuture = new CompletableFuture<>();

		FutureUtils.assertNoException(this.jobManagerRunnerFuture.handle((jobManagerRunner, throwable) -> {
			// JM has been initialized, or the initialization failed
			synchronized (lock) {
				if (throwable == null) {
					if (cancellationFuture != null) {
						Preconditions.checkState(jobStatus == JobStatus.CANCELLING);
						log.warn(
							"JobManager initialization has been cancelled for {}. Stopping JobManager.",
							jobGraph.getJobID());

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
			}
			return null;
		}));
	}

	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return jobResultFuture;
	}

	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		synchronized (lock) {
			if (isRunning()) {
				return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobDetails(
					timeout));
			} else {
				int[] tasksPerState = new int[ExecutionState.values().length];
				return CompletableFuture.completedFuture(new JobDetails(
					jobGraph.getJobID(),
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
	}

	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		synchronized (lock) {
			if (isRunning()) {
				return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.cancel(
					timeout));
			} else {
				return cancelInternal();
			}
		}
	}

	private CompletableFuture<Acknowledge> cancelInternal() {
		Preconditions.checkState(Thread.holdsLock(lock));
		jobStatus = JobStatus.CANCELLING;
		this.cancellationFuture = new CompletableFuture<>();
		return cancellationFuture;
	}

	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		synchronized (lock) {
			if (isRunning()) {
				return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJobStatus(timeout));
			} else {
				return CompletableFuture.completedFuture(jobStatus);
			}
		}
	}

	public boolean isRunning() {
		// todo revisit definition of isRunning. alternative definition: once gateway is available = when is leader
		return jobStatus == JobStatus.RUNNING;
	}

	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		synchronized (lock) {
			Preconditions.checkState(
				isRunning(),
				"JobMaster Gateway is not available during initialization");
			try {
				return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
			} catch (Throwable e) {
				throw new IllegalStateException("JobMaster gateway is not available", e);
			}
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			switch (jobStatus) {
				case RUNNING:
					return jobManagerRunnerFuture.thenAccept(AutoCloseableAsync::closeAsync);
				case CANCELED:
				case FAILED:
					return CompletableFuture.completedFuture(null);
				case INITIALIZING:
					return cancelInternal().thenApply(ack -> null);
				default:
					throw new IllegalStateException(
						"Job is in a status that is not covered by the DispatcherJob: "
							+ jobStatus);
			}
		}
	}
}
