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
package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;

import java.util.Optional;

/** Factory for {@link JobMasterPartitionTracker}. */
@FunctionalInterface
public interface PartitionTrackerFactory {

    /**
     * Creates a new PartitionTracker.
     *
     * @param taskExecutorGatewayLookup lookup function to access task executor gateways
     * @return created PartitionTracker
     */
    JobMasterPartitionTracker create(TaskExecutorGatewayLookup taskExecutorGatewayLookup);

    /** Lookup function for {@link TaskExecutorGateway}. */
    @FunctionalInterface
    interface TaskExecutorGatewayLookup {

        /**
         * Returns a {@link TaskExecutorGateway} corresponding to the given ResourceID.
         *
         * @param taskExecutorId id of the task executor to look up.
         * @return optional task executor gateway
         */
        Optional<TaskExecutorGateway> lookup(ResourceID taskExecutorId);
    }
}
