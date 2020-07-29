package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.function.FunctionUtils;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class InitializingJob {

	private final JobGraph jobGraph;

	private InitializingJob(JobGraph jobGraph) {
		this.jobGraph = jobGraph;
	}

	public static InitializingJob createFrom(JobGraph jobGraph) {
		return new InitializingJob(jobGraph);
	}

	/**
	 * Starts the jobManager initialization
	 */
	public void start() {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

		return jobManagerRunnerFuture
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
					}
				},
				getMainThreadExecutor());
	}
}
