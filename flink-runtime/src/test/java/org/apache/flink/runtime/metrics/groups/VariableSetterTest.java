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

package org.apache.flink.runtime.metrics.groups;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

/** Tests for {@link VariableSetter}. */
public class VariableSetterTest {
    @Test
    public void testPut() {
        final Map<String, String> backingMap = new HashMap<>();

        final VariableSetter setter = new VariableSetter(backingMap, Collections.emptySet());

        setter.put("some_key", "some_value");
        assertThat(backingMap.get("some_key"), is("some_value"));
    }

    @Test
    public void testPutExclusion() {
        final Map<String, String> backingMap = new HashMap<>();
        final Set<String> exclusions = Collections.singleton("excluded");

        final VariableSetter setter = new VariableSetter(backingMap, exclusions);

        // exclusion should be applied to key
        setter.put("excluded", "some_value");
        assertThat(backingMap, not(hasKey("excluded")));

        // exclusions should not be applied to values
        setter.put("some_key", "excluded");
        assertThat(backingMap, hasEntry("some_key", "excluded"));
    }
}
