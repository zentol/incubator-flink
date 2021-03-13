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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;

/** TODO: Add javadoc. */
public class JobMetaInfo {

    /** The ID of the job represented by this metrics group. */
    private final JobID jobId;

    /** The name of the job represented by this metrics group. */
    @Nullable private final String jobName;

    public JobMetaInfo(JobID jobId, @Nullable String jobName) {
        this.jobId = jobId;
        this.jobName = jobName;
    }

    public JobID getJobId() {
        return jobId;
    }

    @Nullable
    public String getJobName() {
        return jobName;
    }
}
