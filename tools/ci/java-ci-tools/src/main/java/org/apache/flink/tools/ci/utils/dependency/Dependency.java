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

import com.google.common.base.Preconditions;

import java.util.Objects;

/** Represents a declared dependency. */
public final class Dependency {

    private final String groupId;
    private final String artifactId;
    private final String version;
    private final boolean isOptional;

    private Dependency(String groupId, String artifactId, String version, boolean isOptional) {
        this.groupId = Preconditions.checkNotNull(groupId);
        this.artifactId = Preconditions.checkNotNull(artifactId);
        this.version = Preconditions.checkNotNull(version);
        this.isOptional = isOptional;
    }

    public static Dependency create(
            String groupId, String artifactId, String version, boolean isOptional) {
        return new Dependency(groupId, artifactId, version, isOptional);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getArtifactId() {
        return artifactId;
    }

    public String getVersion() {
        return version;
    }

    public boolean isOptional() {
        return isOptional;
    }

    Dependency withOptional(boolean isOptional) {
        return new Dependency(groupId, artifactId, version, isOptional);
    }

    @Override
    public String toString() {
        return groupId + ":" + artifactId + ":" + version + (isOptional ? " (optional)" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Dependency that = (Dependency) o;
        return isOptional == that.isOptional
                && Objects.equals(groupId, that.groupId)
                && Objects.equals(artifactId, that.artifactId)
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, artifactId, version, isOptional);
    }
}
