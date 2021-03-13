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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;

import java.util.Map;

/**
 * Special abstract {@link org.apache.flink.metrics.MetricGroup} representing everything belonging
 * to a specific job.
 *
 * @param <C> The type of the parent ComponentMetricGroup.
 */
@Internal
public abstract class JobMetricGroup<C extends ComponentMetricGroup<C>>
        extends ComponentMetricGroup<C> {

    protected final JobMetaInfo jobMetaInfo;

    // ------------------------------------------------------------------------

    protected JobMetricGroup(
            MetricRegistry registry, C parent, JobMetaInfo jobMetaInfo, String[] scope) {
        super(registry, scope, parent);

        this.jobMetaInfo = jobMetaInfo;
    }

    @Override
    protected QueryScopeInfo.JobQueryScopeInfo createQueryServiceMetricInfo(
            CharacterFilter filter) {
        return new QueryScopeInfo.JobQueryScopeInfo(this.jobMetaInfo.getJobId().toString());
    }

    // ------------------------------------------------------------------------
    //  Component Metric Group Specifics
    // ------------------------------------------------------------------------

    @Override
    protected void putVariables(Map<String, String> variables) {
        variables.put(ScopeFormat.SCOPE_JOB_ID, this.jobMetaInfo.getJobId().toString());
        variables.put(ScopeFormat.SCOPE_JOB_NAME, this.jobMetaInfo.getJobName());
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "job";
    }
}
