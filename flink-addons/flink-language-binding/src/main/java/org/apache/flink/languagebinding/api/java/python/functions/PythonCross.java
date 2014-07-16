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

import org.apache.flink.api.java.functions.CrossFunction;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import org.apache.flink.types.TypeInformation;
import java.io.IOException;

/**
 * Cross-function that uses an external python function.
 * @param <IN1>
 * @param <IN2>
 * @param <OUT>
 */
public class PythonCross<IN1, IN2, OUT> extends CrossFunction<IN1, IN2, OUT> implements ResultTypeQueryable {
	private final String operator;
	private PythonStreamer streamer;
	private final Object typeInformation;
	private String metaInformation;

	public PythonCross(String operator, Object typeInformation) {
		this.operator = operator;
		this.typeInformation = typeInformation;
	}

	public PythonCross(String operator, Object typeInformation, String metaInformation) {
		this(operator, typeInformation);
		this.metaInformation = metaInformation;
	}

	/**
	 Opens this function.
	 @param ignored ignored
	 @throws IOException 
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
	 * @param first function input
	 * @param second function input
	 * @return OUT function output
	 * @throws IOException
	 */
	@Override
	public final OUT cross(IN1 first, IN2 second) throws Exception {
		try {
			return (OUT) streamer.streamWithGroups(first, second);
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
	public void close() throws IOException {
		streamer.close();
	}

	@Override
	public TypeInformation getProducedType() {
		return getForObject(typeInformation);
	}
}
