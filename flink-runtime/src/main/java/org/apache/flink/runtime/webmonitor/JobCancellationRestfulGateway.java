/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.concurrent.CompletableFuture;

/** TODO: Add javadoc. */
public interface JobCancellationRestfulGateway {

    /**
     * Cancel the given job.
     *
     * @param jobId identifying the job to cancel
     * @param timeout of the operation
     * @return A future acknowledge if the cancellation succeeded
     */
    CompletableFuture<Acknowledge> cancelJob(JobID jobId, @RpcTimeout Time timeout);
}
