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
package org.apache.flink.languagebinding.api.java.python.streaming;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON_EXECUTOR_NAME;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON_DC_ID;
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.FLINK_TMP_DATA_DIR;
import org.apache.flink.languagebinding.api.java.common.streaming.StreamPrinter;
import org.apache.flink.languagebinding.api.java.common.streaming.Streamer;

/**
 * This streamer is used by functions to send/receive data to/from an external python process.
 */
public class PythonStreamer extends Streamer {
	private final byte[] operator;
	private Process process;
	private final String metaInformation;
	private final int id;
	private final boolean usePython3;

	private String inputFilePath;
	private String outputFilePath;

	public PythonStreamer(AbstractRichFunction function, int id, byte[] operator, String metaInformation, boolean p3) {
		super(function);
		this.operator = operator;
		this.metaInformation = metaInformation;
		this.id = id;
		this.usePython3 = p3;
	}

	/**
	 * Starts the python script.
	 *
	 * @throws IOException
	 */
	@Override
	public void setupProcess() throws IOException {
		startPython();
	}

	private void startPython() throws IOException {
		DatagramSocket s = new DatagramSocket(0);

		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "input";

		sender.open(inputFilePath);
		receiver.open(outputFilePath);

		ProcessBuilder pb = new ProcessBuilder();

		String path = function.getRuntimeContext().getDistributedCache().getFile(FLINK_PYTHON_DC_ID).getAbsolutePath();
		String executorPath = path + FLINK_PYTHON_EXECUTOR_NAME;
		String[] frag = metaInformation.split("\\|");
		StringBuilder importString = new StringBuilder();
		if (frag[0].contains("__main__")) {
			importString.append("from ");
			importString.append(FLINK_PYTHON_PLAN_NAME.substring(1, FLINK_PYTHON_PLAN_NAME.length() - 3));
			importString.append(" import ");
			importString.append(frag[1]);
		} else {
			importString.append("import ");
			importString.append(FLINK_PYTHON_PLAN_NAME.substring(1, FLINK_PYTHON_PLAN_NAME.length() - 3));
		}
		if (usePython3) {
			pb.command(
					"python3", "-O", "-B",
					executorPath, "" + s.getLocalPort(), importString.toString(),
					inputFilePath, outputFilePath, "" + socket.getLocalPort());
		} else {
			pb.command("python", "-O", "-B",
					executorPath, "" + s.getLocalPort(), importString.toString(),
					inputFilePath, outputFilePath, "" + socket.getLocalPort());
		}

		process = pb.start();
		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		byte[] executorPort = new byte[4];
		s.receive(new DatagramPacket(executorPort, 0, 4));
		if (getInt(executorPort, 0) == -2) {
			try { //wait before terminating to ensure that the complete error message is printed
				Thread.sleep(2000);
			} catch (InterruptedException ex) {
			}
			throw new RuntimeException(
					"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely. Check log-files for details.");
		}
		byte[] opSize = new byte[4];
		putInt(opSize, 0, operator.length);
		int exPort = getInt(executorPort, 0);
		s.send(new DatagramPacket(opSize, 0, 4, host, exPort));
		s.send(new DatagramPacket(operator, 0, operator.length, host, exPort));

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}

		try {
			process.exitValue();
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely. Check log-files for details.");
		} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
		}
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	@Override
	public void close() throws IOException {
		try {
			super.close();
		} catch (Exception e) {
			LOG.error("Exception occurred while closing Streamer. :" + e.getMessage());
		}
		try {
			process.exitValue();
		} catch (IllegalThreadStateException ise) { //process still active
			process.destroy();
		}
	}
}
