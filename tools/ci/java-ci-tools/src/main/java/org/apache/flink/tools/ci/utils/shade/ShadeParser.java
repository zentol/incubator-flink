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

package org.apache.flink.tools.ci.utils.shade;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Utils for parsing the shade-plugin output. */
public final class ShadeParser {

    // pattern for maven shade plugin
    private static final Pattern SHADE_NEXT_MODULE_PATTERN =
            Pattern.compile(
                    ".*:shade \\((shade-flink|shade-dist|default)\\) @ ([^ _]+)(_[0-9.]+)? --.*");
    private static final Pattern SHADE_INCLUDE_MODULE_PATTERN =
            Pattern.compile(
                    ".*Including "
                            + "(?<groupId>.*?):"
                            + "(?<artifactId>.*?):"
                            + "(?<type>.*?):"
                            + "(?:(?<classifier>.*?):)?"
                            + "(?<version>.*?)"
                            + " in the shaded jar");

    /** Returns a map MODULE -> INCLUDED_DEPENDENCY. */
    public static Multimap<String, IncludedDependency> parseModulesFromBuildResult(File buildResult)
            throws IOException {
        Multimap<String, IncludedDependency> result = ArrayListMultimap.create();

        try (Stream<String> lines = Files.lines(buildResult.toPath())) {
            String currentShadeModule = null;
            for (String line : (Iterable<String>) lines::iterator) {
                Matcher nextShadeModuleMatcher = SHADE_NEXT_MODULE_PATTERN.matcher(line);
                if (nextShadeModuleMatcher.find()) {
                    currentShadeModule = nextShadeModuleMatcher.group(2);
                }

                if (currentShadeModule != null) {
                    Matcher includeMatcher = SHADE_INCLUDE_MODULE_PATTERN.matcher(line);
                    if (includeMatcher.find()) {
                        String groupId = includeMatcher.group("groupId");
                        String artifactId = includeMatcher.group("artifactId");
                        String version = includeMatcher.group("version");
                        result.put(
                                currentShadeModule,
                                IncludedDependency.create(groupId, artifactId, version));
                    }
                }

                if (line.contains("Replacing original artifact with shaded artifact")) {
                    currentShadeModule = null;
                }
            }
        }
        return result;
    }

    private ShadeParser() {}
}
