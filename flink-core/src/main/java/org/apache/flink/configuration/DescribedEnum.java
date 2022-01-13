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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Describe enum constants used in {@link ConfigOption}s.
 *
 * <p>For enums used as config options, this interface can be implemented to provide a description
 * for each enum constant. This will be used when generating documentation for config options to
 * include a list of available values alongside their respective descriptions.
 */
@PublicEvolving
public interface DescribedEnum {

    /** Returns the description for the enum constant. */
    String getDescription();
}
