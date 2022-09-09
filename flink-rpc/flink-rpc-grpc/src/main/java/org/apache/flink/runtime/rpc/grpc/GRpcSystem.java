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

package org.apache.flink.runtime.rpc.grpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class GRpcSystem implements RpcSystem {
    @Override
    public RpcServiceBuilder localServiceBuilder(Configuration configuration) {
        return null;
    }

    @Override
    public RpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return null;
    }

    @Override
    public String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {
        return null;
    }

    @Override
    public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) throws Exception {
        return null;
    }

    @Override
    public long getMaximumMessageSizeInBytes(Configuration config) {
        return Long.MAX_VALUE;
    }
}
