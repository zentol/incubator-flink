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
package org.apache.flink.runtime.blob;

import java.io.IOException;
import java.net.InetSocketAddress;

/** No-op {@link TaskExecutorBlobService} implementation for testing purposes. */
public class NoOpTaskExecutorBlobService implements TaskExecutorBlobService {
    public static final NoOpTaskExecutorBlobService INSTANCE = new NoOpTaskExecutorBlobService();

    private NoOpTaskExecutorBlobService() {}

    @Override
    public TransientBlobService getTransientBlobService() {
        return NoOpTransientBlobService.INSTANCE;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public JobPermanentBlobService getPermanentBlobService() {
        return NoOpJobPermanentBlobService.INSTANCE;
    }

    @Override
    public void setBlobServerAddress(InetSocketAddress blobServerAddress) {
        // noop
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
