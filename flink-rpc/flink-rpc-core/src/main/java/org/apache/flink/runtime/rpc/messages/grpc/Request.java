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

package org.apache.flink.runtime.rpc.messages.grpc;

import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.messages.Type;

import java.io.Serializable;

public class Request extends Message<RemoteRpcInvocation> {

    private final String target;
    private final Type type;
    private final Serializable fencingToken;

    public Request(
            Serializable fencingToken,
            RemoteRpcInvocation payload,
            String target,
            long id,
            Type type) {
        super(payload, id);
        this.target = target;
        this.type = type;
        this.fencingToken = fencingToken;
    }

    public String getTarget() {
        return target;
    }

    public Type getType() {
        return type;
    }

    public Serializable getFencingToken() {
        return fencingToken;
    }
}
