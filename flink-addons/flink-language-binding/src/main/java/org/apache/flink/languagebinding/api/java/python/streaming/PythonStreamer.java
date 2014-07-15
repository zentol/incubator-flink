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
package org.apache.flink.languagebinding.api.java.python.streaming;

import org.apache.flink.api.common.functions.AbstractFunction;
import static org.apache.flink.languagebinding.api.java.python.PythonExecutor.FLINK_EXECUTOR_ID;
import static org.apache.flink.languagebinding.api.java.python.PythonExecutor.FLINK_PLAN_ID;
import static org.apache.flink.languagebinding.api.java.python.PythonExecutor.FLINK_PYTHON_ID;
import static org.apache.flink.languagebinding.api.java.python.PythonExecutor.FLINK_USER_ID;
import org.apache.flink.languagebinding.api.java.streaming.Converter;
//import org.apache.flink.languagebinding.api.java.streaming.StreamPrinter;
import org.apache.flink.languagebinding.api.java.streaming.Streamer;
import java.io.IOException;

/**
 * This streamer is used by functions with two input types to send/receive data to/from an external python process.
 *
 * Type(flag) conversion table (Java -> PB -> Python)
 * bool	  -> bool         -> bool
 * byte	  -> int32(byte)  -> int
 * short  -> int32(short) -> int
 * int	  -> int32        -> int
 * long   -> int64        -> long
 * float  -> float        -> float
 * double -> double       -> floatt
 * string -> string       -> string
 *   ?    -> string       -> string
 *
 * Type(flag) conversion table (Python -> PB -> Java)
 * bool   -> bool   -> bool
 * int    -> int32  -> int
 * long   -> int64  -> long
 * float  -> float  -> float
 * float  -> float  -> float
 * string -> string -> string
 *   ?    -> string -> string
 */
public class PythonStreamer extends Streamer {
	private final String operator;
	private AbstractFunction function;
	private Process process;
	private String metaInformation;
	private Converter inConverter1;
	private Converter inConverter2;
	private Converter outConverter;

	public PythonStreamer(AbstractFunction function, String operator, String metaInformation) {
		this.function = function;
		this.operator = operator;
		this.metaInformation = metaInformation;
	}

	public PythonStreamer(AbstractFunction function, String operator,
			Converter inConverter1,
			Converter outConverter) {
		this(function, operator, null);
		this.inConverter1 = inConverter1;
		this.outConverter = outConverter;
	}

	public PythonStreamer(AbstractFunction function, String operator,
			Converter inConverter1,
			Converter inConverter2,
			Converter outConverter) {
		this(function, operator, null);
		this.inConverter1 = inConverter1;
		this.inConverter2 = inConverter2;
		this.outConverter = outConverter;
	}

	/**
	 * Opens this streamer and starts the python script.
	 * @throws IOException 
	 */
	@Override
	public void open() throws IOException {
		ProcessBuilder pb = new ProcessBuilder();

		function.getRuntimeContext().getDistributedCache()
				.getFile(FLINK_USER_ID).getAbsolutePath();
		String tmpPlanPath = function.getRuntimeContext().getDistributedCache()
				.getFile(FLINK_PLAN_ID).getAbsolutePath();
		function.getRuntimeContext().getDistributedCache()
				.getFile(FLINK_PYTHON_ID).getAbsolutePath();
		String executorPath = function.getRuntimeContext().getDistributedCache()
				.getFile(FLINK_EXECUTOR_ID).getAbsolutePath();

		String planFileName = tmpPlanPath.substring(tmpPlanPath.lastIndexOf('/') + 1, tmpPlanPath.lastIndexOf('.'));

		if (metaInformation == null) {//hybrid mode
			pb.command("python", executorPath, "1", operator);
		} else {//plan mode
			String[] frag = metaInformation.split("\\|");
			StringBuilder sb = new StringBuilder();
			if (frag[0].contains("__main__")) {
				sb.append("from ");
				sb.append(planFileName);
				sb.append(" import ");
				sb.append(frag[1]);
			} else {
				sb.append("import ");
				sb.append(planFileName);
			}
			pb.command("python", executorPath, "0", operator, sb.toString());
		}
		process = pb.start();
		sender = new RawSender(function, process.getOutputStream());
		receiver = new RawReceiver(function, process.getInputStream());
		//new StreamPrinter(process.getErrorStream()).start();
	}

	@Override
	public void close() throws IOException {
		super.close();
	}
}
