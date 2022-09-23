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

package org.apache.flink.runtime.rpc.messages;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A {@link RemoteFencedMessage} with an ID for matching the resulting response to the request. */
public class RemoteRequestWithID extends RemoteFencedMessage<Serializable, RemoteRpcInvocation> {

    private final long id;
    private final String target;
    private final Type type;

    public RemoteRequestWithID(
            @Nullable Serializable fencingToken,
            RemoteRpcInvocation payload,
            String target,
            long id,
            Type type) {
        super(fencingToken, payload);
        this.target = target;
        this.id = id;
        this.type = type;
    }

    public long getId() {
        return id;
    }

    public String getTarget() {
        return target;
    }

    public Type getType() {
        return type;
    }
}
