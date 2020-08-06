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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class DispatcherJob {
	private final CompletableFuture<JobManagerRunner> initializingJobManager;

	// as long as this gateway is set, the job is initializing
	private CompletableFuture<JobMasterGateway> initializingJobMasterGateway;

	private static final Logger LOG = LoggerFactory.getLogger(DispatcherJob.class);

	public DispatcherJob(JobGraph jobGraph, Dispatcher dispatcher) {
		LOG.info("Defining future");
		long jobManagerInitializationStarted = System.currentTimeMillis();
		initializingJobManager = dispatcher.createJobManagerRunner(jobGraph)
			.thenApplyAsync(FunctionUtils.uncheckedFunction((runner) -> {
				LOG.info("Starting jm runner:");
				JobManagerRunner r = dispatcher.startJobManagerRunner(runner);
				LOG.info("started jm");
				initializingJobMasterGateway = null;
				return r;
			}), dispatcher.getRpcService().getExecutor()); // execute in separate pool to avoid blocking the Dispatcher
		initializingJobManager.whenCompleteAsync((ignored, throwable) -> {
			if (throwable != null) {
				// error during initialization
				dispatcher.onJobManagerInitFailure(
					jobGraph,
					throwable,
					jobManagerInitializationStarted);
				LOG.info("Error in initialization recorded");
			}
		}, dispatcher.getDispatcherExecutor()); // execute in main thread to avoid concurrency issues
		initializingJobMasterGateway = CompletableFuture.supplyAsync(() -> new InitializingJobMasterGateway(initializingJobManager, jobGraph),
			dispatcher.getRpcService().getExecutor());

		LOG.info("ctor done");
	}

	public boolean isInitializing() {
		return initializingJobMasterGateway != null;
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		LOG.info("getJobManagerRunnerFuture");
		return initializingJobManager;
	}

	/**
	 * Returns a fake JobMasterGateway that acts as an initializing JobMaster.
	 */
	public CompletableFuture<JobMasterGateway> getInitializingJobMasterGatewayFuture() {
		return initializingJobMasterGateway;
	}
}
