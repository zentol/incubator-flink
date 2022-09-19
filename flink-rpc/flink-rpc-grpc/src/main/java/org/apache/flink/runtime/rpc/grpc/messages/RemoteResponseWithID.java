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

package org.apache.flink.runtime.rpc.grpc.messages;

import java.io.Serializable;

/** A response with an ID for matching the response to the original request. */
public class RemoteResponseWithID<P extends Serializable> implements Serializable {
    private final P payload;
    private final long id;

    public RemoteResponseWithID(P payload, long id) {
        this.payload = payload;
        this.id = id;
    }

    public P getPayload() {
        return payload;
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return payload.toString();
    }
}
