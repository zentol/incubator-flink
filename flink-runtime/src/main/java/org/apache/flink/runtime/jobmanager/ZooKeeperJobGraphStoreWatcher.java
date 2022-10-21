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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.flink.shaded.curator5.org.apache.curator.utils.ZKPaths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link JobGraphStoreWatcher} implementation for ZooKeeper.
 *
 * <p>Each job graph creates ZNode:
 *
 * <pre>
 * +----O /flink/jobgraphs/&lt;job-id&gt; 1 [persistent]
 * .
 * .
 * .
 * +----O /flink/jobgraphs/&lt;job-id&gt; N [persistent]
 * </pre>
 *
 * <p>The root path is watched to detect concurrent modifications in corner situations where
 * multiple instances operate concurrently. The job manager acts as a {@link
 * JobGraphStore.JobGraphListener} to react to such situations.
 */
public class ZooKeeperJobGraphStoreWatcher implements JobGraphStoreWatcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperJobGraphStoreWatcher.class);

    /**
     * Cache to monitor all children. This is used to detect races with other instances working on
     * the same state.
     */
    private final CuratorCache pathCache;

    private JobGraphStore.JobGraphListener jobGraphListener;

    private volatile boolean running;

    public ZooKeeperJobGraphStoreWatcher(CuratorCache pathCache) {
        this.pathCache = checkNotNull(pathCache);
        this.pathCache.listenable().addListener(createListener());
        running = false;
    }

    @Override
    public void start(JobGraphStore.JobGraphListener jobGraphListener) throws Exception {
        this.jobGraphListener = checkNotNull(jobGraphListener);
        running = true;
        pathCache.start();
    }

    @Override
    public void stop() throws Exception {
        if (!running) {
            return;
        }
        running = false;

        LOG.info("Stopping ZooKeeperJobGraphStoreWatcher ");
        pathCache.close();
    }

    private CuratorCacheListener createListener() {
        return CuratorCacheListener.builder()
                .forInitialized(() -> LOG.info("JobGraphsPathCacheListener initialized"))
                .forCreates(
                        newNode -> {
                            JobID jobId = fromEvent(newNode);

                            LOG.debug("Received CHILD_ADDED event notification for job {}", jobId);

                            jobGraphListener.onAddedJobGraph(jobId);
                        })
                .forDeletes(
                        deletedNode -> {
                            JobID jobId = fromEvent(deletedNode);

                            LOG.debug(
                                    "Received CHILD_REMOVED event notification for job {}", jobId);

                            jobGraphListener.onRemovedJobGraph(jobId);
                        })
                .build();
    }

    /** Returns a JobID for the event's path. */
    private static JobID fromEvent(ChildData event) {
        return JobID.fromHexString(ZKPaths.getNodeFromPath(event.getPath()));
    }
}
