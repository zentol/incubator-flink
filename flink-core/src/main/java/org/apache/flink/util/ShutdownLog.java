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

package org.apache.flink.util;

import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** A utility for logging the shutdown of components. */
public class ShutdownLog {
    /**
     * Logs the beginning and end of the shutdown procedure for the given component.
     *
     * <p>This method accepts a {@link Supplier} instead of a {@link CompletableFuture} because the
     * latter usually required implies the shutdown to already have begun.
     *
     * @param log Logger of owning component
     * @param component component that will be shut down
     * @param shutdownTrigger component shutdown trigger
     * @return termination future of the component
     */
    public static <C> CompletableFuture<C> logShutdown(
            Logger log, String component, Supplier<CompletableFuture<C>> shutdownTrigger) {
        log.debug("Starting shutdown of {}.", component);
        return FutureUtils.logCompletion(log, "shutdown of " + component, shutdownTrigger.get());
    }
}
