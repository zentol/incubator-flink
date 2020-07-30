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

import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class DispatcherJob {
	private final CompletableFuture<JobManagerRunner> initializingJobManager;

	private ErrorInfo failure = null;
	private static final Logger LOG = LoggerFactory.getLogger(DispatcherJob.class);

	public DispatcherJob(JobGraph jobGraph, Dispatcher dispatcher) {
		LOG.info("Defining future");
		initializingJobManager = dispatcher.createJobManagerRunner(jobGraph)
			.thenApplyAsync(FunctionUtils.uncheckedFunction((runner) -> {
				LOG.info("Starting jm runner:");
				JobManagerRunner r = dispatcher.startJobManagerRunner(runner);
				LOG.info("started jm");
				return r;
			}), dispatcher.getDispatcherExecutor());
		initializingJobManager.whenCompleteAsync((ignored, throwable) -> {
			if (throwable != null) {
				// error during initialization
				//dispatcher.onJobManagerInitFailure(jobGraph.getJobID());
				this.failure = new ErrorInfo(throwable, System.currentTimeMillis());
				LOG.info("Error in initialization recorded");
			}
		});
		/*initializingJobManager.whenCompleteAsync((jobManagerRunner, initThrowable) -> {
			LOG.info("jm init finished");
			// JM init has finished
			if (initThrowable != null) {
				// initialization failed
				failure = new ErrorInfo(initThrowable, System.currentTimeMillis());
				dispatcher.onJobManagerInitFailure(jobGraph.getJobID());
			} else {
				// register error handler
				jobManagerRunnerFuture.whenCompleteAsync((ignore, runnerThrowable) -> {
					if (runnerThrowable != null) {
						// at any point in the JobManager's life, there was an error.
						dispatcher.onJobManagerFailure(jobGraph.getJobID());
						// TODO: there could be a scenario where the remove call happens before the item is added to the Map in the Dispatcher
					}
				}, dispatcher.getDispatcherExecutor());
			}
		}); */
		LOG.info("ctor done");
	}

	public boolean isInitializing() {
		return !initializingJobManager.isDone();
	}

	public boolean isFailed() {
		return failure != null;
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		LOG.info("getJobManagerRunnerFuture");
		Preconditions.checkState(!isInitializing(), "Expecting initialized JobManager");
		return initializingJobManager;
	}

	public void cancelInitialization() {
		initializingJobManager.cancel(true);
	}
}
