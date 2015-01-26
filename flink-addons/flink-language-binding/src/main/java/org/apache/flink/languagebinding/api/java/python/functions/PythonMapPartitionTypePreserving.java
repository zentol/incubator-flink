/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.languagebinding.api.java.python.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.languagebinding.api.java.python.streaming.PythonStreamer;
import java.io.IOException;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.usePython3;
import org.apache.flink.util.Collector;

/**
 * Multi-purpose class, usable by all operations using a python script with one input source and identical in-/output
 * types.
 *
 * @param <IN>
 */
public class PythonMapPartitionTypePreserving<IN> extends RichMapPartitionFunction<IN, IN> {
	private final PythonStreamer streamer;

	public PythonMapPartitionTypePreserving(int id, byte[] operator, String metaInformation) {
		streamer = new PythonStreamer(this, id, operator, metaInformation, usePython3);
	}

	/**
	 * Opens this function.
	 *
	 * @param config configuration
	 * @throws IOException
	 */
	@Override
	public void open(Configuration config) throws IOException {
		streamer.open();
		streamer.sendBroadCastVariables(config);
	}

	@Override
	public void mapPartition(Iterable<IN> values, Collector<IN> out) throws Exception {
		streamer.streamBufferWithoutGroups(values.iterator(), out);
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
}
