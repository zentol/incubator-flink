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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;

/**
 * Test for the {@link DispatcherJob} class.
 */
public class DispatcherJobTest extends TestLogger {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private static final Time TIMEOUT = Time.seconds(10L);
	private static final JobID TEST_JOB_ID = new JobID();

	@Test
	public void testInitialSubmissionError() throws ExecutionException, InterruptedException {
		TestContext testContext = createDispatcherJob();
		DispatcherJob dispatcherJob = testContext.dispatcherJob;

		Assert.assertThat(dispatcherJob.isRunning(), is(false));
		Assert.assertThat(dispatcherJob.requestJobStatus(TIMEOUT).get(), is(JobStatus.INITIALIZING));
		Assert.assertThat(dispatcherJob.requestJobDetails(TIMEOUT).get().getStatus(), is(JobStatus.INITIALIZING));
		Assert.assertThat(dispatcherJob.getResultFuture().isDone(), is(false));

		// now fail
		testContext.jobManagerRunnerCompletableFuture.completeExceptionally(new RuntimeException("Artificial test failure"));

		Assert.assertThat(dispatcherJob.isRunning(), is(false));
		Assert.assertThat(dispatcherJob.requestJobStatus(TIMEOUT).get(), is(JobStatus.FAILED));
		Assert.assertThat(dispatcherJob.requestJobDetails(TIMEOUT).get().getStatus(), is(JobStatus.FAILED));
		Assert.assertThat(dispatcherJob.getResultFuture().isDone(), is(true));
		ArchivedExecutionGraph aeg = dispatcherJob.getResultFuture().get();
		Assert.assertThat(ExceptionUtils.findThrowableSerializedAware(
			aeg.getFailureInfo().getException(), RuntimeException.class, ClassLoader.getSystemClassLoader()).isPresent(),
			is(true));
	}

	@Test
	public void testCloseWhileInitializing() throws Exception {
		TestContext testContext = createDispatcherJob();
		DispatcherJob dispatcherJob = testContext.dispatcherJob;

		Assert.assertThat(dispatcherJob.requestJobStatus(TIMEOUT).get(), is(JobStatus.INITIALIZING));

		CompletableFuture<Void> closeFuture = dispatcherJob.closeAsync();

		// finish initialization, so that we can cancel the job

		// create a jobmanager runner with a mocked JobMaster gateway, that cancels right away.
		TestingJobManagerRunner jobManagerRunner =
			new TestingJobManagerRunner(testContext.jobGraph.getJobID(), false);
		TestingJobMasterGateway mockRunningJobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setCancelFunction(() -> CompletableFuture.completedFuture(Acknowledge.get())).build();
		jobManagerRunner.getJobMasterGateway().complete(mockRunningJobMasterGateway);

		// complete JobManager runner future to indicate to the DispatcherJob that the Runner has been initialized
		testContext.jobManagerRunnerCompletableFuture.complete(jobManagerRunner);

		// this future should now complete (because we were able to cancel the job)
		closeFuture.get();
	}

	private TestContext createDispatcherJob() {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		TestContext ctx = new TestContext();

		ctx.jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);
		ctx.jobManagerRunnerCompletableFuture = new CompletableFuture<>();
		ctx.dispatcherJob = DispatcherJob.createForSubmission(ctx.jobManagerRunnerCompletableFuture, ctx.jobGraph, 1337);

		return ctx;
	}

	private static class TestContext {
		public CompletableFuture<JobManagerRunner> jobManagerRunnerCompletableFuture;
		public DispatcherJob dispatcherJob;
		public JobGraph jobGraph;
	}
}
