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
package org.apache.flink.runtime.state.changelog.inmemory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageFactory;

/** An {@link StateChangelogStorageFactory} for creating {@link InMemoryStateChangelogStorage}. */
public class InMemoryStateChangelogStorageFactory implements StateChangelogStorageFactory {

    public static String identifier = "memory";

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public StateChangelogStorage<?> createStorage(
            Configuration configuration, TaskManagerJobMetricGroup metricGroup) {
        return new InMemoryStateChangelogStorage();
    }
}
