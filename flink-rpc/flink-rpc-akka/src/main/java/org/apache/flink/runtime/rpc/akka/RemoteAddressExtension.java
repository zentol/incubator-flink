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
package org.apache.flink.runtime.rpc.akka;

import akka.actor.AbstractExtensionId;
import akka.actor.Address;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;

/**
 * {@link akka.actor.ActorSystem} {@link Extension} used to obtain the {@link Address} on which the
 * given ActorSystem is listening.
 */
public class RemoteAddressExtension
        extends AbstractExtensionId<RemoteAddressExtension.RemoteAddressExtensionImpl> {

    @Override
    public RemoteAddressExtensionImpl createExtension(ExtendedActorSystem system) {
        return new RemoteAddressExtensionImpl(system);
    }

    /** Actual {@link Extension} implementation. */
    public static class RemoteAddressExtensionImpl implements Extension {

        private final Address address;

        public RemoteAddressExtensionImpl(ExtendedActorSystem system) {
            address = system.provider().getDefaultAddress();
        }

        public Address getAddress() {
            return address;
        }
    }
}
