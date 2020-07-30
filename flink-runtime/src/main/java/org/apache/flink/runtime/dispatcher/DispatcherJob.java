package org.apache.flink.runtime.dispatcher;

import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import java.util.concurrent.CompletableFuture;

/**
 * Representation of a job while the JobManager is initializing, managed by the {@link Dispatcher}.
 */
public class DispatcherJob {
	private final CompletableFuture<CompletableFuture<JobManagerRunner>> initializingJobManager;
	private CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;

	private ErrorInfo failure = null;

	public DispatcherJob(JobGraph jobGraph, Dispatcher dispatcher) {
		initializingJobManager = CompletableFuture.supplyAsync(() -> {
			// initialize JM
			return dispatcher.createJobManagerRunner(jobGraph).thenApply(FunctionUtils.uncheckedFunction(dispatcher::startJobManagerRunner));
		}, dispatcher.getDispatcherExecutor());
		initializingJobManager.whenCompleteAsync((jobManagerRunner, initThrowable) -> {
			// JM init has finished
			if (initThrowable != null) {
				// initialization failed
				failure = new ErrorInfo(initThrowable, System.currentTimeMillis());
				dispatcher.onJobManagerInitFailure(jobGraph.getJobID());
			} else {
				jobManagerRunnerFuture = jobManagerRunner;
				// register error handler
				jobManagerRunnerFuture.whenCompleteAsync((ignore, runnerThrowable) -> {
					if (runnerThrowable != null) {
						// at any point in the JobManager's life, there was an error.
						dispatcher.onJobManagerFailure(jobGraph.getJobID());
						// TODO: there could be a scenario where the remove call happens before the item is added to the Map in the Dispatcher
					}
				}, dispatcher.getDispatcherExecutor());
			}
		});
	}

	public boolean isInitializing() {
		return !initializingJobManager.isDone();
	}

	public boolean isFailed() {
		return failure != null;
	}

	public CompletableFuture<JobManagerRunner> getJobManagerRunnerFuture() {
		Preconditions.checkState(!isInitializing(), "Expecting initialized JobManager");
		return jobManagerRunnerFuture;
	}

	public void cancelInitialization() {
		initializingJobManager.cancel(true);
	}
}
