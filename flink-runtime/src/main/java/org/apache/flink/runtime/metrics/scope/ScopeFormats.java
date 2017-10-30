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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;

/**
 * A container for component scope formats.
 */
public final class ScopeFormats {

	private final JobManagerScopeFormat jobManagerFormat;
	private final JobManagerJobScopeFormat jobManagerJobFormat;
	private final TaskManagerScopeFormat taskManagerFormat;
	private final TaskManagerJobScopeFormat taskManagerJobFormat;
	private final TaskScopeFormat taskFormat;
	private final OperatorScopeFormat operatorFormat;

	// ------------------------------------------------------------------------

	/**
	 * Creates all scope formats, based on the given scope format strings.
	 */
	private ScopeFormats(
			String jobManagerFormat,
			String jobManagerJobFormat,
			String taskManagerFormat,
			String taskManagerJobFormat,
			String taskFormat,
			String operatorFormat,
			boolean shortIds) {
		this.jobManagerFormat = new JobManagerScopeFormat(jobManagerFormat);
		this.jobManagerJobFormat = new JobManagerJobScopeFormat(jobManagerJobFormat, this.jobManagerFormat, shortIds);
		this.taskManagerFormat = new TaskManagerScopeFormat(taskManagerFormat, shortIds);
		this.taskManagerJobFormat = new TaskManagerJobScopeFormat(taskManagerJobFormat, this.taskManagerFormat, shortIds);
		this.taskFormat = new TaskScopeFormat(taskFormat, this.taskManagerJobFormat, shortIds);
		this.operatorFormat = new OperatorScopeFormat(operatorFormat, this.taskFormat, shortIds);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public JobManagerScopeFormat getJobManagerFormat() {
		return this.jobManagerFormat;
	}

	public TaskManagerScopeFormat getTaskManagerFormat() {
		return this.taskManagerFormat;
	}

	public TaskManagerJobScopeFormat getTaskManagerJobFormat() {
		return this.taskManagerJobFormat;
	}

	public JobManagerJobScopeFormat getJobManagerJobFormat() {
		return this.jobManagerJobFormat;
	}

	public TaskScopeFormat getTaskFormat() {
		return this.taskFormat;
	}

	public OperatorScopeFormat getOperatorFormat() {
		return this.operatorFormat;
	}

	// ------------------------------------------------------------------------
	//  Parsing from Config
	// ------------------------------------------------------------------------

	/**
	 * Creates the scope formats as defined in the given configuration.
	 *
	 * @param config The configuration that defines the formats
	 * @return The ScopeFormats parsed from the configuration
	 */
	public static ScopeFormats fromConfig(Configuration config) {
		boolean shortIds = config.getBoolean(MetricOptions.SCOPE_SHORT_IDS);
		String jmFormat = config.getString(MetricOptions.SCOPE_NAMING_JM);
		String jmJobFormat = config.getString(MetricOptions.SCOPE_NAMING_JM_JOB);
		String tmFormat = config.getString(MetricOptions.SCOPE_NAMING_TM);
		String tmJobFormat = config.getString(MetricOptions.SCOPE_NAMING_TM_JOB);
		String taskFormat = config.getString(MetricOptions.SCOPE_NAMING_TASK);
		String operatorFormat = config.getString(MetricOptions.SCOPE_NAMING_OPERATOR);

		return new ScopeFormats(jmFormat, jmJobFormat, tmFormat, tmJobFormat, taskFormat, operatorFormat, shortIds);
	}
}
