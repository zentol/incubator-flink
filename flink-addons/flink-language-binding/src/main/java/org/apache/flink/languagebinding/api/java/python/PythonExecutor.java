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
package org.apache.flink.languagebinding.api.java.python;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.PrintingOutputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.CrossOperator.CrossProjection;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.CrossOperator.ProjectCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.JoinProjection;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.operators.ProjectOperator.Projection;
import org.apache.flink.api.java.operators.SingleInputUdfOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.TwoInputUdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.api.java.typeutils.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
//CHECKSTYLE.OFF: AvoidStarImport - enum/function import
import static org.apache.flink.languagebinding.api.java.python.PythonExecutor.OperationInfo.*;
import org.apache.flink.languagebinding.api.java.python.functions.*;
//CHECKSTYLE.ON: AvoidStarImport
import org.apache.flink.languagebinding.api.java.python.streaming.RawReceiver;
import org.apache.flink.languagebinding.api.java.python.streaming.RawSender;
import org.apache.flink.languagebinding.api.java.streaming.Receiver;
import static org.apache.flink.languagebinding.api.java.streaming.Receiver.createTuple;
import org.apache.flink.languagebinding.api.java.streaming.Sender;
import org.apache.flink.languagebinding.api.java.streaming.StreamPrinter;
import org.apache.flink.runtime.filecache.FileCache;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

/**
 This class allows the execution of a Flink plan written in python.
 */
public class PythonExecutor {
	private static Sender sender;
	private static Receiver receiver;
	private static Process process;
	private static HashMap<Integer, Object> sets;
	private static ExecutionEnvironment env;
	private static String tmpPackagePath = null;
	private static String tmpPlanPath = null;

	public static final byte BYTE = new Integer(1).byteValue();
	public static final int SHORT = new Integer(1).shortValue();
	public static final int INT = 1;
	public static final long LONG = 1L;
	public static final String STRING = "type";
	public static final double FLOAT = 1.5F;
	public static final double DOUBLE = 1.5D;
	public static final boolean BOOLEAN = true;

	public static String FLINK_HDFS_PATH;
	public static final String FLINK_PYTHON_ID = "flink";
	public static final String FLINK_PYTHON_LOCAL_PATH = "/resources/python";
	public static final String FLINK_PYTHON_PLAN_NAME = "/plan.py";
	public static final String FLINK_PYTHON_EXECUTOR_NAME = "/executor.py";

	public static String FLINK_DIR = System.getenv("FLINK_ROOT_DIR");

	/**
	 Entry point for the execution of a python plan.
	 @param args 
	 [0] = local path to user package (can be omitted)
	 [1] = local path to python script containing the plan
	 [X] = additional parameters passed to the plan
	 @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		FLINK_DIR = FLINK_DIR.substring(0, FLINK_DIR.length() - 7);

		env = ExecutionEnvironment.getExecutionEnvironment();
		boolean singleArgument = !FileSystem.get(new URI(args[0])).getFileStatus(new Path(args[0])).isDir();
		if (singleArgument) {
			prepareLocalUserFile(args[0]);
		} else {
			prepareLocalUserFile(args[0], args[1]);
		}
		open(Arrays.copyOfRange(args, singleArgument ? 1 : 2, args.length));
		receivePlan();
		prepareDistributedFiles();
		env.execute();
		close();
	}

	private static void prepareLocalUserFile(String... filePaths) throws IOException, URISyntaxException {
		for (int x = 0; x < filePaths.length; x++) {

			String filePath = filePaths[x];
			if (filePath.endsWith("/")) {
				filePath = filePath.substring(0, filePath.length() - 1);
			}

			FileSystem fs;

			if (x + 1 == filePaths.length) {
				tmpPlanPath = FLINK_DIR + FLINK_PYTHON_LOCAL_PATH + FLINK_PYTHON_PLAN_NAME;
				fs = FileSystem.get(new URI(tmpPlanPath));
				if (fs.exists(new Path(tmpPlanPath))) {
					fs.delete(new Path(tmpPlanPath), false);
				}
				FileCache.copy(new Path(filePath), new Path(tmpPlanPath), false);
			} else {
				tmpPackagePath = FLINK_DIR + FLINK_PYTHON_LOCAL_PATH
						+ filePath.substring(filePath.lastIndexOf('/'));
				fs = FileSystem.get(new URI(tmpPackagePath));
				if (fs.exists(new Path(tmpPackagePath))) {
					fs.delete(new Path(tmpPackagePath), true);
				}
				FileCache.copy(new Path(filePath), new Path(tmpPackagePath), false);
			}
		}
	}

	private static void open(String[] args) throws IOException {
		sets = new HashMap();
		StringBuilder argsBuilder = new StringBuilder();
		for (String arg : args) {
			argsBuilder.append(" ").append(arg);
		}
		process = Runtime.getRuntime().exec("python -B " + tmpPlanPath + argsBuilder.toString());
		sender = new RawSender(null, process.getOutputStream());
		receiver = new RawReceiver(null, process.getInputStream());
		new StreamPrinter(process.getErrorStream()).start();
	}

	private static void prepareDistributedFiles() throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(FLINK_HDFS_PATH));

		if (fs.exists(new Path(FLINK_HDFS_PATH))) {
			fs.delete(new Path(FLINK_HDFS_PATH), true);
		}

		FileCache.copy(new Path(FLINK_DIR + FLINK_PYTHON_LOCAL_PATH), new Path(FLINK_HDFS_PATH), false);
		env.registerCachedFile(FLINK_HDFS_PATH, FLINK_PYTHON_ID);
	}

	private static void close() throws IOException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(new URI(FLINK_HDFS_PATH));
		hdfs.delete(new Path(FLINK_HDFS_PATH), true);

		FileSystem local = FileSystem.getLocalFileSystem();
		if (tmpPackagePath != null) {
			local.delete(new Path(tmpPackagePath), true);
		}
		local.delete(new Path(tmpPlanPath), true);

		try {
			sender.close();
		} catch (NullPointerException npe) {
		}
		try {
			receiver.close();
		} catch (NullPointerException npe) {
		}
	}

	//====Plan==========================================================================================================
	private static void receivePlan() throws IOException {
		receiveParameters();
		receiveSources();
		receiveSets();
		receiveSinks();
		receiveBroadcast();
	}

	//====Environment===================================================================================================
	/**
	 This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP,
		MODE
	}

	private static void receiveParameters() throws IOException {
		Integer count = (Integer) receiver.receiveRecord();

		for (int x = 0; x < count; x++) {
			Tuple value = (Tuple) receiver.receiveRecord();
			switch (Parameters.valueOf(((String) value.getField(0)).toUpperCase())) {
				case DOP:
					Integer dop = (Integer) value.getField(1);
					env.setDegreeOfParallelism(dop);
					break;
				case MODE:
					FLINK_HDFS_PATH = (Boolean) value.getField(1) ? "file:/tmp/flink" : "hdfs:/tmp/flink";
					break;
			}
		}
		if (env.getDegreeOfParallelism() < 0) {
			env.setDegreeOfParallelism(1);
		}
	}

	//====Sources=======================================================================================================
	/**
	 This enum contains the identifiers for all supported InputFormats.
	 */
	private enum InputFormats {
		CSV,
		JDBC,
		TEXT,
		VALUE
	}

	private static void receiveSources() throws IOException {
		Integer sourceCount = (Integer) receiver.receiveRecord();
		for (int x = 0; x < sourceCount; x++) {
			String identifier = (String) receiver.receiveRecord();
			switch (InputFormats.valueOf(identifier.toUpperCase())) {
				case CSV:
					createCsvSource();
					break;
				case JDBC:
					createJDBCSource();
					break;
				case TEXT:
					createTextSource();
					break;
				case VALUE:
					createValueSource();
					break;
			}
		}
	}

	private static void createCsvSource() throws IOException {
		int id = (Integer) receiver.receiveRecord();
		Tuple args = (Tuple) receiver.receiveRecord();
		Tuple t = createTuple(args.getArity() - 3);
		Class[] classes = new Class[t.getArity()];
		for (int x = 0; x < args.getArity() - 3; x++) {
			t.setField(args.getField(x + 3), x);
			classes[x] = t.getField(x).getClass();
		}
		DataSet<Tuple> set = env.createInput(
				new CsvInputFormat(new Path((String) args.getField(0)), (String) args.getField(1),
						((String) args.getField(2)).charAt(0), classes), getForObject(t));
		sets.put(id, set);
	}

	private static void createJDBCSource() throws IOException {
		int id = (Integer) receiver.receiveRecord();
		Tuple args = (Tuple) receiver.receiveRecord();

		DataSet<Tuple> set;
		if (args.getArity() == 3) {
			set = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
					.setDrivername((String) args.getField(0))
					.setDBUrl((String) args.getField(1))
					.setQuery((String) args.getField(2))
					.finish());
			sets.put(id, set);
		}
		if (args.getArity() == 5) {
			set = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
					.setDrivername((String) args.getField(0))
					.setDBUrl((String) args.getField(1))
					.setQuery((String) args.getField(2))
					.setUsername((String) args.getField(3))
					.setPassword((String) args.getField(4))
					.finish());
			sets.put(id, set);
		}
	}

	private static void createTextSource() throws IOException {
		int id = (Integer) receiver.receiveRecord();
		Tuple args = (Tuple) receiver.receiveRecord();
		Path path = new Path((String) args.getField(0));
		DataSet<String> set = env.createInput(new TextInputFormat(path), STRING_TYPE_INFO);
		sets.put(id, set);
	}

	private static void createValueSource() throws IOException {
		int id = (Integer) receiver.receiveRecord();
		int valueCount = (Integer) receiver.receiveRecord();
		Object[] values = new Object[valueCount];
		for (int x = 0; x < valueCount; x++) {
			values[x] = receiver.receiveRecord();
		}
		sets.put(id, env.fromElements(values));
	}

	//====Sinks=========================================================================================================
	/**
	 This enum contains the identifiers for all supported OutputFormats.
	 */
	private enum OutputFormats {
		CSV,
		JDBC,
		PRINT,
		TEXT
	}

	private static void receiveSinks() throws IOException {
		Integer sinkCount = (Integer) receiver.receiveRecord();
		for (int x = 0; x < sinkCount; x++) {
			int parentID = (Integer) receiver.receiveRecord();
			String identifier = (String) receiver.receiveRecord();
			Tuple args;
			switch (OutputFormats.valueOf(identifier.toUpperCase())) {
				case CSV:
					args = (Tuple) receiver.receiveRecord();
					createCsvSink(parentID, args);
					break;
				case JDBC:
					args = (Tuple) receiver.receiveRecord();
					createJDBCSink(parentID, args);
					break;
				case PRINT:
					createPrintSink(parentID);
					break;
				case TEXT:
					args = (Tuple) receiver.receiveRecord();
					createTextSink(parentID, args);
					break;
			}
		}
	}

	private static void createCsvSink(int id, Tuple args) {
		CsvOutputFormat out = new CsvOutputFormat(
				new Path((String) args.getField(0)),
				(String) args.getField(1),
				(String) args.getField(2));
		switch ((Integer) args.getField(3)) {
			case 0:
				out.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE);
				break;
			case 1:
				out.setWriteMode(FileSystem.WriteMode.OVERWRITE);
				break;
		}
		((DataSet) sets.get(id)).output(out);
	}

	private static void createJDBCSink(int id, Tuple args) {
		switch (args.getArity()) {
			case 3:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.finish());
				break;
			case 4:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setBatchInterval((Integer) args.getField(3))
						.finish());
				break;
			case 5:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setUsername((String) args.getField(3))
						.setPassword((String) args.getField(4))
						.finish());
				break;
			case 6:
				((DataSet) sets.get(id)).output(JDBCOutputFormat.buildJDBCOutputFormat()
						.setDrivername((String) args.getField(0))
						.setDBUrl((String) args.getField(1))
						.setQuery((String) args.getField(2))
						.setUsername((String) args.getField(3))
						.setPassword((String) args.getField(4))
						.setBatchInterval((Integer) args.getField(5))
						.finish());
				break;
		}
	}

	private static void createPrintSink(int id) {
		((DataSet) sets.get(id)).output(new PrintingOutputFormat());
	}

	private static void createTextSink(int id, Tuple args) {
		((DataSet) sets.get(id)).output(new TextOutputFormat(new Path((String) args.getField(0))));
	}

	//====Operations====================================================================================================
	/**
	 This enum contains the identifiers for all supported DataSet operations.
	 */
	private enum Operations {
		COGROUP,
		CROSS,
		CROSS_H,
		CROSS_T,
		FILTER,
		FLATMAP,
		GROUPBY,
		GROUPREDUCE,
		JOIN,
		JOIN_H,
		JOIN_T,
		MAP,
		PROJECTION,
		REDUCE,
		SORT,
		UNION
		//aggregate
		//iterate
		//withParameters (cast to UdfOperator)
		//withBroadCastSet (cast to UdfOperator)
	}

	/**
	 General purpose container for all information related to operations.
	 */
	protected static class OperationInfo {
		protected static final int INFO_MODE_FULL_PRJ = -1;
		protected static final int INFO_MODE_FULL = 0;
		protected static final int INFO_MODE_NOKEY = 1;
		protected static final int INFO_MODE_OPTYPE = 2;
		protected static final int INFO_MODE_OP = 3;
		protected static final int INFO_MODE_GRP = 4;
		protected static final int INFO_MODE_SORT = 5;
		protected static final int INFO_MODE_UNION = 6;
		protected static final int INFO_MODE_PROJECT = 7;
		protected static final int INFO_MODE_NOKEY_PRJ = 8;

		protected int parentID;
		protected int otherID;
		protected int childID;

		protected Tuple keys1;
		protected Tuple keys2;

		protected Tuple projectionKeys1;
		protected Tuple projectionKeys2;

		protected Object types;
		protected String operator;
		protected String meta;

		protected int field;
		protected int order;

		protected OperationInfo(int id, int mode) throws IOException {
			parentID = id;
			childID = (Integer) receiver.receiveRecord();
			switch (mode) {
				case INFO_MODE_FULL_PRJ:
					keys1 = (Tuple) receiver.receiveRecord();
					keys2 = (Tuple) receiver.receiveRecord();
					otherID = (Integer) receiver.receiveRecord();
					types = receiver.receiveRecord();
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					projectionKeys1 = (Tuple) receiver.receiveRecord();
					projectionKeys2 = (Tuple) receiver.receiveRecord();
					break;
				case INFO_MODE_NOKEY_PRJ:
					otherID = (Integer) receiver.receiveRecord();
					types = receiver.receiveRecord();
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					projectionKeys1 = (Tuple) receiver.receiveRecord();
					projectionKeys2 = (Tuple) receiver.receiveRecord();
					break;
				case INFO_MODE_FULL:
					keys1 = (Tuple) receiver.receiveRecord();
					keys2 = (Tuple) receiver.receiveRecord();
					otherID = (Integer) receiver.receiveRecord();
					types = receiver.receiveRecord();
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					break;
				case INFO_MODE_NOKEY:
					otherID = (Integer) receiver.receiveRecord();
					types = receiver.receiveRecord();
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					break;
				case INFO_MODE_OPTYPE:
					types = receiver.receiveRecord();
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					break;
				case INFO_MODE_OP:
					operator = (String) receiver.receiveRecord();
					meta = (String) receiver.receiveRecord();
					break;
				case INFO_MODE_GRP:
					keys1 = (Tuple) receiver.receiveRecord();
					break;
				case INFO_MODE_SORT:
					field = (Integer) receiver.receiveRecord();
					order = (Integer) receiver.receiveRecord();
					break;
				case INFO_MODE_UNION:
					otherID = (Integer) receiver.receiveRecord();
					break;
				case INFO_MODE_PROJECT:
					keys1 = (Tuple) receiver.receiveRecord();
					types = receiver.receiveRecord();
					break;
			}
		}
	}

	private static void receiveSets() throws IOException {
		Integer setCount = (Integer) receiver.receiveRecord();
		for (int x = 0; x < setCount; x++) {
			String identifier = (String) receiver.receiveRecord();
			int id = (Integer) receiver.receiveRecord();
			switch (Operations.valueOf(identifier.toUpperCase())) {
				case COGROUP:
					createCoGroupOperation(new OperationInfo(id, INFO_MODE_FULL));
					break;
				case CROSS:
					createCrossOperation(0, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case CROSS_H:
					createCrossOperation(1, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case CROSS_T:
					createCrossOperation(2, new OperationInfo(id, INFO_MODE_NOKEY_PRJ));
					break;
				case FILTER:
					createFilterOperation(new OperationInfo(id, INFO_MODE_OP));
					break;
				case FLATMAP:
					createFlatMapOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case GROUPREDUCE:
					createGroupReduceOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case JOIN:
					createJoinOperation(0, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case JOIN_H:
					createJoinOperation(1, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case JOIN_T:
					createJoinOperation(2, new OperationInfo(id, INFO_MODE_FULL_PRJ));
					break;
				case MAP:
					createMapOperation(new OperationInfo(id, INFO_MODE_OPTYPE));
					break;
				case PROJECTION:
					createProjectOperation(new OperationInfo(id, INFO_MODE_PROJECT));
					break;
				case REDUCE:
					createReduceOperation(new OperationInfo(id, INFO_MODE_OP));
					break;
				case GROUPBY:
					createGroupOperation(new OperationInfo(id, INFO_MODE_GRP));
					break;
				case SORT:
					createSortOperation(new OperationInfo(id, INFO_MODE_SORT));
					break;
				case UNION:
					createUnionOperation(new OperationInfo(id, INFO_MODE_UNION));
					break;
			}
		}
	}

	private static void createCoGroupOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		int keyCount = info.keys1.getArity();
		int firstKey = (Integer) info.keys1.getField(0);
		int secondKey = (Integer) info.keys2.getField(0);
		int[] firstKeys = new int[keyCount - 1];
		int[] secondKeys = new int[keyCount - 1];
		for (int x = 0; x < keyCount - 1; x++) {
			firstKeys[x] = (Integer) info.keys1.getField(x + 1);
			secondKeys[x] = (Integer) info.keys2.getField(x + 1);
		}
		sets.put(info.childID, op1.coGroup(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys)
				.with(new PythonCoGroup(info.operator, info.types, info.meta)));
	}

	private static void createCrossOperation(int mode, OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		DefaultCross defaultResult = null;
		switch (mode) {
			case 0:
				defaultResult = op1.cross(op2);
				break;
			case 1:
				defaultResult = op1.crossWithHuge(op2);
				break;
			case 2:
				defaultResult = op1.crossWithTiny(op2);
				break;
			default:
				throw new IllegalArgumentException("Invalid Cross mode specified: " + mode);
		}

		if (info.projectionKeys1 == null & info.projectionKeys2 == null) {
			sets.put(info.childID, defaultResult.with(new PythonCross(info.operator, info.types, info.meta)));
		} else {
			CrossProjection projectionResult = null;
			if (info.projectionKeys1 != null) {
				int[] projectionKeys = new int[info.projectionKeys1.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys1.getField(x);
				}
				projectionResult = defaultResult.projectFirst(projectionKeys);
			} else {
				projectionResult = defaultResult.projectFirst();
			}

			if (info.projectionKeys2 != null) {
				int[] projectionKeys = new int[info.projectionKeys2.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys2.getField(x);
				}
				projectionResult = projectionResult.projectSecond(projectionKeys);
			} else {
				projectionResult = projectionResult.projectSecond();
			}

			ProjectCross pc;
			Tuple types = (Tuple) info.types;
			switch (types.getArity()) {
				case 1:
					pc = projectionResult.types(
							types.getField(0).getClass());
					break;
				case 2:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass());
					break;
				case 3:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
					break;
				case 4:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass());
					break;
				case 5:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass());
					break;
				case 6:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
					break;
				case 7:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass());
					break;
				case 8:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass());
					break;
				case 9:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass());
					break;
				case 10:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass());
					break;
				case 11:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
					break;
				case 12:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass());
					break;
				case 13:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass());
					break;
				case 14:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
					break;
				case 15:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass());
					break;
				case 17:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass());
					break;
				case 18:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
					break;
				case 19:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass());
					break;
				case 20:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass());
					break;
				case 21:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
					break;
				case 22:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass());
					break;
				case 23:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass());
					break;
				case 24:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
					break;
				case 25:
					pc = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
							types.getField(24).getClass());
					break;
				default:
					throw new IllegalArgumentException("Tuple size not supported");
			}
			sets.put(info.childID, pc);
		}

	}

	private static void createFilterOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.filter(new PythonFilter(info.operator, info.meta)));
	}

	private static void createFlatMapOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.flatMap(new PythonFlatMap(info.operator, info.types, info.meta)));
	}

	private static void createGroupOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		int[] fields = new int[info.keys1.getArity()];
		for (int x = 0; x < fields.length; x++) {
			fields[x] = (Integer) info.keys1.getField(x);
		}
		sets.put(info.childID, op1.groupBy(fields));
	}

	private static void createGroupReduceOperation(OperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.childID, ((DataSet) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.childID, ((SortedGrouping) op1)
					.reduceGroup(new PythonGroupReduce(info.operator, info.types, info.meta)));
		}
	}

	private static void createJoinOperation(int mode, OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		int keyCount = info.keys1.getArity();
		int firstKey = (Integer) info.keys1.getField(0);
		int secondKey = (Integer) info.keys2.getField(0);
		int[] firstKeys = new int[keyCount - 1];
		int[] secondKeys = new int[keyCount - 1];
		for (int x = 0; x < keyCount - 1; x++) {
			firstKeys[x] = (Integer) info.keys1.getField(x + 1);
			secondKeys[x] = (Integer) info.keys2.getField(x + 1);
		}

		DefaultJoin defaultResult = null;
		switch (mode) {
			case 0:
				defaultResult = op1.join(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			case 1:
				defaultResult = op1.joinWithHuge(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			case 2:
				defaultResult = op1.joinWithTiny(op2).where(firstKey, firstKeys).equalTo(secondKey, secondKeys);
				break;
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}

		if (info.projectionKeys1 == null & info.projectionKeys2 == null) {
			sets.put(info.childID, defaultResult.with(new PythonJoin(info.operator, info.types, info.meta)));
		} else {
			JoinProjection projectionResult = null;
			if (info.projectionKeys1 != null) {
				int[] projectionKeys = new int[info.projectionKeys1.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys1.getField(x);
				}
				projectionResult = defaultResult.projectFirst(projectionKeys);
			} else {
				projectionResult = defaultResult.projectFirst();
			}

			if (info.projectionKeys2 != null) {
				int[] projectionKeys = new int[info.projectionKeys2.getArity()];
				for (int x = 0; x < projectionKeys.length; x++) {
					projectionKeys[x] = (Integer) info.projectionKeys2.getField(x);
				}
				projectionResult = projectionResult.projectSecond(projectionKeys);
			} else {
				projectionResult = projectionResult.projectSecond();
			}

			ProjectJoin pj;
			Tuple types = (Tuple) info.types;
			switch (types.getArity()) {
				case 1:
					pj = projectionResult.types(
							types.getField(0).getClass());
					break;
				case 2:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass());
					break;
				case 3:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
					break;
				case 4:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass());
					break;
				case 5:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass());
					break;
				case 6:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
					break;
				case 7:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass());
					break;
				case 8:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass());
					break;
				case 9:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass());
					break;
				case 10:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass());
					break;
				case 11:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
					break;
				case 12:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass());
					break;
				case 13:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass());
					break;
				case 14:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
					break;
				case 15:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass());
					break;
				case 17:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass());
					break;
				case 18:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
					break;
				case 19:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass());
					break;
				case 20:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass());
					break;
				case 21:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
					break;
				case 22:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass());
					break;
				case 23:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass());
					break;
				case 24:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
					break;
				case 25:
					pj = projectionResult.types(
							types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
							types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
							types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
							types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
							types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
							types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
							types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
							types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
							types.getField(24).getClass());
					break;
				default:
					throw new IllegalArgumentException("Tuple size not supported");
			}
			sets.put(info.childID, pj);
		}
	}

	private static void createMapOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.childID, op1.map(new PythonMap(info.operator, info.types, info.meta)));
	}

	private static void createProjectOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		Tuple types = (Tuple) info.types;
		int[] fields = new int[info.keys1.getArity()];
		for (int x = 0; x < fields.length; x++) {
			fields[x] = (Integer) info.keys1.getField(x);
		}
		Projection p = op1.project(fields);
		ProjectOperator po;
		switch (types.getArity()) {
			case 1:
				po = p.types(types.getField(0).getClass());
				break;
			case 2:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass());
				break;
			case 3:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass());
				break;
			case 4:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass());
				break;
			case 5:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass());
				break;
			case 6:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass());
				break;
			case 7:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass());
				break;
			case 8:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass());
				break;
			case 9:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass());
				break;
			case 10:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass());
				break;
			case 11:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass());
				break;
			case 12:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass());
				break;
			case 13:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass());
				break;
			case 14:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass());
				break;
			case 15:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass());
				break;
			case 17:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass());
				break;
			case 18:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass());
				break;
			case 19:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass());
				break;
			case 20:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass());
				break;
			case 21:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass());
				break;
			case 22:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass());
				break;
			case 23:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass());
				break;
			case 24:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass());
				break;
			case 25:
				po = p.types(types.getField(0).getClass(), types.getField(1).getClass(), types.getField(2).getClass(),
						types.getField(3).getClass(), types.getField(4).getClass(), types.getField(5).getClass(),
						types.getField(6).getClass(), types.getField(7).getClass(), types.getField(8).getClass(),
						types.getField(9).getClass(), types.getField(10).getClass(), types.getField(11).getClass(),
						types.getField(12).getClass(), types.getField(13).getClass(), types.getField(14).getClass(),
						types.getField(15).getClass(), types.getField(16).getClass(), types.getField(17).getClass(),
						types.getField(18).getClass(), types.getField(19).getClass(), types.getField(20).getClass(),
						types.getField(21).getClass(), types.getField(22).getClass(), types.getField(23).getClass(),
						types.getField(24).getClass());
				break;
			default:
				throw new IllegalArgumentException("Tuple size not supported");
		}
		sets.put(info.childID, po);
	}

	private static void createReduceOperation(OperationInfo info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.childID, ((DataSet) op1).reduce(new PythonReduce(info.operator, info.meta)));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1).reduce(new PythonReduce(info.operator, info.meta)));
		}
	}

	private static void createSortOperation(OperationInfo info) {
		Grouping op1 = (Grouping) sets.get(info.parentID);
		Order o;
		switch (info.order) {
			case 0:
				o = Order.NONE;
				break;
			case 1:
				o = Order.ASCENDING;
				break;
			case 2:
				o = Order.DESCENDING;
				break;
			case 3:
				o = Order.ANY;
				break;
			default:
				o = Order.NONE;
				break;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.childID, ((UnsortedGrouping) op1).sortGroup(info.field, o));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.childID, ((SortedGrouping) op1).sortGroup(info.field, o));
		}
	}

	private static void createUnionOperation(OperationInfo info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.childID, op1.union(op2));
	}

	//====BroadCastVariables============================================================================================
	private static void receiveBroadcast() throws IOException {
		Integer broadcastCount = (Integer) receiver.receiveRecord();
		for (int x = 0; x < broadcastCount; x++) {
			int parentID = (Integer) receiver.receiveRecord();
			int otherID = (Integer) receiver.receiveRecord();
			String name = (String) receiver.receiveRecord();
			DataSet op1 = (DataSet) sets.get(parentID);
			DataSet op2 = (DataSet) sets.get(otherID);

			if (op1 instanceof SingleInputUdfOperator) {
				((SingleInputUdfOperator) op1).withBroadcastSet(op2, name);
			} else if (op1 instanceof TwoInputUdfOperator) {
				((TwoInputUdfOperator) op1).withBroadcastSet(op2, name);
			}
		}
	}
}
