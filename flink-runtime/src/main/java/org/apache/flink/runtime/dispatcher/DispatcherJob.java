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

import org.apache.flink.runtime.jobmaster.InitializingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class DispatcherJob {
	/**
	 * This future returns the JobManagerRunner. The future might get cancelled during initialization.
	 * The job is then considered cancelled.
	 */
	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;

	/**
	 * During initialization, this future is set to a "fake" JobManagerGateway that
	 * has some methods available. Once the initialization is done, this future is set to null.
	 */
	@Nullable
	private CompletableFuture<InitializingJobMasterGateway> initializingJobMasterGatewayFuture;

	public DispatcherJob(
		CompletableFuture<JobManagerRunner> initializingJobManager,
		CompletableFuture<InitializingJobMasterGateway> initializingJobMasterGateway) {
		this.jobManagerRunnerFuture = initializingJobManager;
		this.initializingJobMasterGatewayFuture = initializingJobMasterGateway;
	}

	public boolean isInitializing() {
		return initializingJobMasterGatewayFuture != null;
	}

	public CompletableFuture<InitializingJobMasterGateway> getInitializingJobMasterGatewayFuture() {
		return initializingJobMasterGatewayFuture;
	}

	public void setInitializingJobMasterGatewayFuture(CompletableFuture<InitializingJobMasterGateway> initializingJobMasterGateway) {
		this.initializingJobMasterGatewayFuture = initializingJobMasterGateway;
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		return jobManagerRunnerFuture;
	}
}
