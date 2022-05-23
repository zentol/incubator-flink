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

import org.apache.flink.annotation.VisibleForTesting;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Optional;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** TODO: Add javadoc. */
public class DependencyParser {

    private static final Pattern DEPENDENCY_NEXT_MODULE_PATTERN =
            Pattern.compile(".*:tree .* @ ([^ _]+)(_[0-9.]+)? --.*");
    private static final Pattern DEPENDENCY_LIST_START_PATTERN =
            Pattern.compile(".*The following files have been resolved:.*");
    private static final Pattern DEPENDENCY_LIST_ITEM_PATTERN =
            Pattern.compile(
                    ".* +"
                            + "(?<groupId>.*?):"
                            + "(?<artifactId>.*?):"
                            + "(?<type>.*?):"
                            + "(?:(?<classifier>.*?):)?"
                            + "(?<version>.*?):"
                            + "(?<scope>[^ ]*)"
                            + "(?<optional> \\(optional\\))?");
    private static final Pattern DEPENDENCY_NOISE_PATTERN = Pattern.compile("\\[WARNING\\].*");

    /** Returns a map MODULE -> DECLARED_DEPENDENCIES. */
    public static Multimap<String, Dependency> parseDependencyTree(File buildResult)
            throws IOException {
        try (Stream<String> lines = Files.lines(buildResult.toPath())) {
            return parseDependencyTree(
                    lines.filter(line -> !DEPENDENCY_NOISE_PATTERN.matcher(line).matches()));
        }
    }

    @VisibleForTesting
    static Multimap<String, Dependency> parseDependencyTree(Stream<String> lines) {
        Multimap<String, Dependency> result = ArrayListMultimap.create();

        String currentModule = null;

        final Iterator<String> iterator = lines.iterator();

        while (iterator.hasNext()) {
            Matcher moduleMatcher = DEPENDENCY_NEXT_MODULE_PATTERN.matcher(iterator.next());
            while (!moduleMatcher.find()) {
                if (iterator.hasNext()) {
                    moduleMatcher = DEPENDENCY_NEXT_MODULE_PATTERN.matcher(iterator.next());
                } else {
                    return result;
                }
            }
            currentModule = moduleMatcher.group(1);

            if (currentModule == null || !iterator.hasNext()) {
                throw new IllegalStateException();
            }

            // discard one line
            iterator.next();

            if (!iterator.hasNext()) {
                throw new IllegalStateException();
            }

            final Stack<Dependency> parentStack = new Stack<>();
            final Stack<Integer> treeDepthStack = new Stack<>();
            String line = iterator.next();
            Optional<Dependency> parsedDependency = parseDependency(line);
            while (parsedDependency.isPresent()) {
                int treeDepth = getLevel(line);

                while (!treeDepthStack.isEmpty() && treeDepth <= treeDepthStack.peek()) {
                    parentStack.pop();
                    treeDepthStack.pop();
                }

                final Dependency dependency = parsedDependency.get();

                final Dependency dependencyWithInheritedOptionalFlag =
                        dependency.isOptional()
                                ? dependency
                                : dependency.withOptional(
                                        !parentStack.isEmpty() && parentStack.peek().isOptional());

                if (treeDepthStack.isEmpty() || treeDepth > treeDepthStack.peek()) {
                    treeDepthStack.push(treeDepth);
                    parentStack.push(dependencyWithInheritedOptionalFlag);
                }

                result.put(currentModule, dependencyWithInheritedOptionalFlag);
                if (iterator.hasNext()) {
                    line = iterator.next();
                    parsedDependency = parseDependency(line);
                } else {
                    parsedDependency = Optional.empty();
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    static Optional<Dependency> parseDependency(String line) {
        Matcher dependencyMatcher = DEPENDENCY_LIST_ITEM_PATTERN.matcher(line);
        if (!dependencyMatcher.find()) {
            return Optional.empty();
        }

        return Optional.of(
                Dependency.create(
                        dependencyMatcher.group("groupId"),
                        dependencyMatcher.group("artifactId"),
                        dependencyMatcher.group("version"),
                        dependencyMatcher.group("optional") != null));
    }

    private static int getLevel(String line) {
        final int level = line.indexOf('+');
        if (level != -1) {
            return level;
        }
        return line.indexOf('\\');
    }
}
