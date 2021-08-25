/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link FlinkChillPackageRegistrar}. */
public class FlinkChillPackageRegistrarTest extends TestLogger {

    private static final FlinkChillPackageRegistrar registrar = new FlinkChillPackageRegistrar();

    @Test
    public void testGetNextRegistrationId() {
        assertThat(registrar.getMaxRegistrationId(), equalTo(84));
    }

    @Test
    public void testRegistrationIdsInExpectedRange() {
        final List<Integer> expectedRegistrationIds =
                IntStream.range(73, 85).boxed().collect(Collectors.toList());
        final List<Integer> actualRegistrationIds = new ArrayList<>();

        registrar.registerSerializers(
                (type, serializer, registrationId) -> actualRegistrationIds.add(registrationId));

        assertThat(actualRegistrationIds, equalTo(expectedRegistrationIds));
    }
}
