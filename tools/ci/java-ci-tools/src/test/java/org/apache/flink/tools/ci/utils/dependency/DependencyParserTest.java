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

package org.apache.flink.tools.ci.utils.dependency;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DependencyParserTest {

    private static Stream<String> getTestDependencyTree() {
        return Stream.of(
                "[INFO] --- maven-dependency-plugin:3.2.0:tree (default-cli) @ m1 ---",
                "[INFO] org.example:m1:jar:1.1",
                "[INFO] +- external:dependency1:jar:2.1:compile",
                "[INFO] |  +- external:dependency2:jar:2.2:compile (optional)",
                "[INFO] |  |  \\- external:dependency3:jar:2.3:compile",
                "[INFO] |  +- external:dependency4:jar:2.4:compile",
                "[INFO]",
                "[INFO] --- maven-dependency-plugin:3.2.0:tree (default-cli) @ m2 ---",
                "[INFO] org.example:m2:jar:1.2",
                "[INFO] +- org.example:m1:jar:1.1:compile",
                "[INFO] |  +- external:dependency4:jar:2.4:compile");
    }

    @Test
    void testTreeParsing() {
        final Map<String, Collection<Dependency>> result =
                DependencyParser.parseDependencyTree(getTestDependencyTree()).asMap();

        assertThat(result).containsOnlyKeys("m1", "m2");
        assertThat(result.get("m1"))
                .containsExactlyInAnyOrder(
                        Dependency.create("external", "dependency1", "2.1", false),
                        Dependency.create("external", "dependency2", "2.2", true),
                        Dependency.create("external", "dependency3", "2.3", true),
                        Dependency.create("external", "dependency4", "2.4", false));
        assertThat(result.get("m2"))
                .containsExactlyInAnyOrder(
                        Dependency.create("org.example", "m1", "1.1", false),
                        Dependency.create("external", "dependency4", "2.4", false));
    }

    @Test
    void testLineParsing() {
        assertThat(
                        DependencyParser.parseDependency(
                                "[INFO] +- external:dependency1:jar:2.1:compile"))
                .hasValue(Dependency.create("external", "dependency1", "2.1", false));
        assertThat(
                        DependencyParser.parseDependency(
                                "[INFO] +- external:dependency1:jar:2.1:compile (optional)"))
                .hasValue(Dependency.create("external", "dependency1", "2.1", true));
        assertThat(
                        DependencyParser.parseDependency(
                                "[INFO] +- external:dependency1:jar:some_classifier:2.1:compile (optional)"))
                .hasValue(Dependency.create("external", "dependency1", "2.1", true));
        assertThat(
                        DependencyParser.parseDependency(
                                "[INFO] +- external:dependency1:pom:some_classifier:2.1:compile (optional)"))
                .hasValue(Dependency.create("external", "dependency1", "2.1", true));
    }
}
