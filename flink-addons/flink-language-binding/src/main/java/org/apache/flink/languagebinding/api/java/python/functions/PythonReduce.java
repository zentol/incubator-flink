/**
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
package org.apache.flink.languagebinding.api.java.python.functions;

import org.apache.flink.api.java.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import java.io.IOException;

/**
 * Reduce-function that uses an external python function.
 * @param <IN>
 */
public class PythonReduce<IN> extends ReduceFunction<IN> {
	private final String operator;
	private PythonStreamer streamer;
	private String metaInformation;

	public PythonReduce(String operator) {
		this.operator = operator;
	}

	public PythonReduce(String operator, String metaInformation) {
		this(operator);
		this.metaInformation = metaInformation;
	}

	/**
	 * Opens this function.
	 *
	 * @param ignored
	 * @throws IOException
	 */
	@Override
	public void open(Configuration ignored) throws IOException {
		streamer = metaInformation != null
				? new PythonStreamer(this, operator, metaInformation)
				: new PythonStreamer(this, operator);
		streamer.open();
	}

	/**
	 * Calls the external python function.
	 * @param value1 function input
	 * @param value2 function input
	 * @return function output
	 * @throws IOException
	 */
	@Override
	public final IN reduce(IN value1, IN value2) throws IOException {
		try {
			return (IN) streamer.stream(value1, value2);
		} catch (IOException ioe) {
			if (ioe.getMessage().startsWith("Stream closed")) {
				throw new IOException(
						"The python process has prematurely terminated (or may have never started).", ioe);
			}
			throw ioe;
		}
	}

	/**
	 * Closes this function.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws Exception {
		streamer.close();
	}
}
