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

package org.apache.flink.tools.ci.optional;

import org.apache.flink.tools.ci.utils.dependency.Dependency;
import org.apache.flink.tools.ci.utils.dependency.DependencyParser;
import org.apache.flink.tools.ci.utils.shade.IncludedDependency;
import org.apache.flink.tools.ci.utils.shade.ShadeParser;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;

/**
 * Verifies that all dependencies bundled with the shade-plugin are marked as optional in the pom.
 * This ensures compatibility with later maven versions and in general simplifies dependency
 * management as transitivity is no longer dependent on the shade-plugin.
 */
public class ShadeOptionalChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ShadeOptionalChecker.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println(
                    "Usage: ShadeOptionalChecker <pathShadeBuildOutput> <pathMavenDependencyOutput>");
            System.exit(1);
        }

        final Path shadeOutputPath = Paths.get(args[0]);
        final Path dependencyOutputPath = Paths.get(args[1]);

        final Multimap<String, IncludedDependency> bundledDependenciesByModule =
                ShadeParser.parseModulesFromBuildResult(shadeOutputPath.toFile());
        final Multimap<String, Dependency> dependenciesByModule =
                DependencyParser.parseDependencyTree(dependencyOutputPath.toFile());

        final Multimap<String, IncludedDependency> violations = ArrayListMultimap.create();

        for (String module : bundledDependenciesByModule.keySet()) {
            LOG.debug("Checking module '{}'.", module);
            if (!dependenciesByModule.containsKey(module)) {
                throw new IllegalStateException(
                        String.format(
                                "Module %s listed by shade-plugin, but not dependency-plugin.",
                                module));
            }

            Collection<IncludedDependency> bundledDependencies =
                    bundledDependenciesByModule.get(module);

            if (bundledDependencies.isEmpty()) {
                LOG.debug("\tModule is not bundling any dependencies.");
                continue;
            }

            for (IncludedDependency bundledDependency : bundledDependencies) {
                LOG.debug("\tChecking dependency '{}'.", bundledDependency);

                final Optional<Dependency> matchingOptionalDeclaration =
                        dependenciesByModule.get(module).stream()
                                .filter(
                                        dependency ->
                                                bundledDependency
                                                        .getGroupId()
                                                        .equals(dependency.getGroupId()))
                                .filter(
                                        dependency ->
                                                bundledDependency
                                                        .getArtifactId()
                                                        .equals(dependency.getArtifactId()))
                                .filter(dependency -> dependency.isOptional())
                                .findAny();
                if (!matchingOptionalDeclaration.isPresent()) {
                    violations.put(module, bundledDependency);
                }
            }
            if (!violations.containsKey(module)) {
                LOG.info("OK: {}", module);
            }
        }

        if (!violations.isEmpty()) {
            LOG.error(
                    "{} modules bundle in total {} dependencies without them being marked as optional in the pom:",
                    violations.keySet().size(),
                    violations.size());

            for (String moduleWithViolations : violations.keySet()) {
                final Collection<IncludedDependency> dependencyViolations =
                        violations.get(moduleWithViolations);
                LOG.error(
                        "\tModule {} ({} violation{}):",
                        moduleWithViolations,
                        dependencyViolations.size(),
                        dependencyViolations.size() == 1 ? "" : "s");
                for (IncludedDependency dependencyViolation : dependencyViolations) {
                    LOG.error("\t\t{}", dependencyViolation);
                }
            }

            System.exit(1);
        }
    }
}
