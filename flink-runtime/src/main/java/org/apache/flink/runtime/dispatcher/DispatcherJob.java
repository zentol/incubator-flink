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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.InitializingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.Preconditions;

import akka.remote.artery.SystemMessageDelivery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

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
public class DispatcherJob implements AutoCloseableAsync {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
	private final CompletableFuture<ArchivedExecutionGraph> jobResultFuture;
	// job is running when this completes
	private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture;

	private final JobGraph jobGraph;
	private final long initializationTimestamp;

	// if this future is != null, the job has been cancelled. Cancellation completes with this future.
	@Nullable
	private CompletableFuture<Acknowledge> cancellationFuture = null;

	static DispatcherJob createForSubmission(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture, JobGraph jobGraph, long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobGraph, initializationTimestamp);
	}

	static DispatcherJob createForRecovery(CompletableFuture<JobManagerRunner> bla) {
		return null;
	}

	private DispatcherJob(CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
		JobGraph jobGraph, long initializationTimestamp) {
		this.jobManagerRunnerFuture = jobManagerRunnerFuture;
		this.jobGraph = jobGraph;
		this.initializationTimestamp = initializationTimestamp;
		jobResultFuture = new CompletableFuture<>();
		jobMasterGatewayFuture = new CompletableFuture<>();
		jobManagerRunnerFuture.handle((jobManagerRunner, throwable) -> {
			// this gets called when the JM has been initialized
			if (throwable == null) {
				if (cancellationFuture != null) {
					log.warn("JobManager initialization has been cancelled for {}. Stopping JobManager.", jobGraph.getJobID());
					// todo consider exposing a method in the JobManagerRunner to properly forward an exception
					cancellationFuture
						.thenRun(() -> initializationFailedWith(null, JobStatus.CANCELED))
						.thenCombine(jobManagerRunner.closeAsync(), (ign, ore) -> Acknowledge.get());

				} else {
					// request JobMaster gateway
					jobManagerRunner
						.getJobMasterGateway()
						.thenAccept(jobMasterGatewayFuture::complete);
					// forward result future
					jobManagerRunner.getResultFuture().handle(getJobManagerResultHandler());
				}
			} else {
				initializationFailedWith(throwable, JobStatus.FAILED);
			}
			return null;
		});
	}

	private BiFunction<ArchivedExecutionGraph, Throwable, Void> getJobManagerResultHandler() {
		return ((archivedExecutionGraph, throwable) -> {
			Preconditions.checkState(!jobResultFuture.isDone(), "The job can not finish twice.");
			if (throwable == null) {
				jobResultFuture.complete(archivedExecutionGraph);
			} else {
				jobResultFuture.completeExceptionally(throwable);
			}
			return null;
		});
	}

	private void initializationFailedWith(Throwable throwable, JobStatus status) {
		jobResultFuture.complete(ArchivedExecutionGraph.createFromFailedInit(jobGraph, throwable, status, initializationTimestamp));
	}

	public CompletableFuture<ArchivedExecutionGraph> getResultFuture() {
		return jobResultFuture;
	}

	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		if (isRunning()) {
			return getJobMasterGateway().requestJobDetails(timeout);
		} else {
			int[] tasksPerState = new int[ExecutionState.values().length];
			final JobStatus status = cancellationFuture != null ? JobStatus.CANCELED : JobStatus.INITIALIZING;
			return CompletableFuture.completedFuture(new JobDetails(jobGraph.getJobID(),
				jobGraph.getName(),
				initializationTimestamp,
				0,
				0,
				status,
				0,
				tasksPerState,
				jobGraph.getVerticesAsArray().length));
		}
	}

	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		if (isRunning()) {
			return getJobMasterGateway().cancel(timeout);
		} else {
			return cancelInternal();
		}
	}

	private CompletableFuture<Acknowledge> cancelInternal() {
		return this.cancellationFuture = new CompletableFuture<>();
	}

	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		if (isRunning()) {
			return getJobMasterGateway().requestJobStatus(timeout);
		} else {
			return CompletableFuture.completedFuture(cancellationFuture != null ? JobStatus.CANCELED : JobStatus.INITIALIZING);
		}
	}

	public boolean isRunning() {
		return jobMasterGatewayFuture.isDone();
	}

	public JobMasterGateway getJobMasterGateway() {
		Preconditions.checkState(isRunning(), "JobMaster Gateway is not available during initialization");
		try {
			return jobMasterGatewayFuture.get();
		} catch (Throwable e) {
			throw new IllegalStateException("JobMaster gateway is not available", e);
		}
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (isRunning()) {
			return jobManagerRunnerFuture.thenAccept(AutoCloseableAsync::closeAsync);
		} else {
			return cancelInternal().thenApply(ack -> null);
		}
	}
}
