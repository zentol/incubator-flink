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

import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class DispatcherJob {
	private CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;

	// as long as this future is set, the job is initializing
	private CompletableFuture<JobMasterGateway> initializingJobMasterGatewayFuture;
	private boolean cancelled = false;

	public boolean isInitializing() {
		return initializingJobMasterGatewayFuture != null;
	}

	public CompletableFuture<JobMasterGateway> getInitializingJobMasterGatewayFuture() {
		return initializingJobMasterGatewayFuture;
	}

	public void setInitializingJobMasterGatewayFuture(CompletableFuture<JobMasterGateway> initializingJobMasterGateway) {
		this.initializingJobMasterGatewayFuture = initializingJobMasterGateway;
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		return jobManagerRunnerFuture;
	}

	public void setInitializingJobManagerRunnerFuture(CompletableFuture<JobManagerRunner> initializingJobManager) {
		this.jobManagerRunnerFuture = initializingJobManager;
	}

	public void setCancelled(boolean val) {
		this.cancelled = val;
	}

	public boolean isCancelled() {
		return cancelled;
	}
}
