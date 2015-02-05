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
package org.apache.flink.languagebinding.api.java.python;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.operators.CoGroupPythonOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.common.PlanBinder;
import org.apache.flink.languagebinding.api.java.common.OperationInfo;
import org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.PythonOperationInfo;
//CHECKSTYLE.OFF: AvoidStarImport - enum/function import
import static org.apache.flink.languagebinding.api.java.python.PythonPlanBinder.PythonOperationInfo.*;
import org.apache.flink.languagebinding.api.java.python.functions.*;
//CHECKSTYLE.ON: AvoidStarImport
import org.apache.flink.languagebinding.api.java.common.streaming.Receiver;
import org.apache.flink.languagebinding.api.java.common.streaming.StreamPrinter;
import org.apache.flink.runtime.filecache.FileCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class allows the execution of a Flink plan written in python.
 */
public class PythonPlanBinder extends PlanBinder<PythonOperationInfo> {
	static final Logger LOG = LoggerFactory.getLogger(PythonPlanBinder.class);

	public static final String ARGUMENT_PYTHON_2 = "2";
	public static final String ARGUMENT_PYTHON_3 = "3";

	public static final String FLINK_PYTHON_DC_ID = "flink";
	public static final String FLINK_PYTHON_PLAN_NAME = "/plan.py";
	public static final String FLINK_PYTHON_EXECUTOR_NAME = "/executor.py";

	private static final String FLINK_PYTHON_FILE_PATH = System.getProperty("java.io.tmpdir") + "/flink_plan";
	protected static final String FLINK_PYTHON_REL_LOCAL_PATH = "/resources/python";
	protected static final String FLINK_DIR = System.getenv("FLINK_ROOT_DIR");
	protected static String FULL_PATH;

	private Process process;

	public static boolean usePython3 = false;

	/**
	 * Entry point for the execution of a python plan.
	 *
	 * @param args planPath[ package1[ packageX[ - parameter1[ parameterX]]]]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: ./bin/pyflink<2/3>.sh <pathToScript>[ <pathToPackage1>[ <pathToPackageX]][ - <parameter1>[ <parameterX>]]");
			return;
		}
		usePython3 = args[0].equals(ARGUMENT_PYTHON_3);
		PythonPlanBinder binder = new PythonPlanBinder();
		binder.runPlan(Arrays.copyOfRange(args, 1, args.length));
	}

	public PythonPlanBinder() throws IOException {
		FULL_PATH = FLINK_DIR != null
				? FLINK_DIR.substring(0, FLINK_DIR.length() - 7) + FLINK_PYTHON_REL_LOCAL_PATH //command-line
				: FileSystem.getLocalFileSystem().getWorkingDirectory().toString() //testing
				+ "/src/main/python/org/apache/flink/languagebinding/api/python";
	}

	protected void runPlan(String[] args) throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
		FLINK_HDFS_PATH = env instanceof LocalEnvironment 
				? "file:/tmp/flink"  //local mode
				: "hdfs:/tmp/flink"; //cluster mode

		int split = 0;
		for (int x = 0; x < args.length; x++) {
			if (args[x].compareTo("-") == 0) {
				split = x;
			}
		}
		try {
			prepareFiles(Arrays.copyOfRange(args, 0, split == 0 ? 1 : split));
			startPython(Arrays.copyOfRange(args, split == 0 ? args.length : split + 1, args.length));
			receivePlan();
			distributeFiles(env);
			env.execute();
			close();
		} catch (Exception e) {
			close();
			throw e;
		}
	}

	//=====Setup========================================================================================================
	/**
	 * Copies all files to a common directory (FLINK_PYTHON_FILE_PATH). This allows us to distribute it as one big
	 * package, and resolves PYTHONPATH issues.
	 *
	 * @param filePaths
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	private void prepareFiles(String... filePaths) throws IOException, URISyntaxException {
		//Flink python package
		String tempFilePath = FLINK_PYTHON_FILE_PATH;
		clearPath(tempFilePath);
		FileCache.copy(new Path(FULL_PATH), new Path(tempFilePath), false);

		//plan file		
		copyFile(filePaths[0], FLINK_PYTHON_PLAN_NAME);

		//additional files/folders
		for (int x = 1; x < filePaths.length; x++) {
			copyFile(filePaths[x], null);
		}
	}

	private static void clearPath(String path) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(new URI(path));
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true);
		}
	}

	private static void copyFile(String path, String name) throws IOException, URISyntaxException {
		if (path.endsWith("/")) {
			path = path.substring(0, path.length() - 1);
		}
		String identifier = name == null ? path.substring(path.lastIndexOf("/")) : name;
		String tmpFilePath = FLINK_PYTHON_FILE_PATH + "/" + identifier;
		clearPath(tmpFilePath);
		Path p = new Path(path);
		FileCache.copy(p.makeQualified(FileSystem.get(p.toUri())), new Path(tmpFilePath), true);
	}

	private static void distributeFiles(ExecutionEnvironment env) throws IOException, URISyntaxException {
		clearPath(FLINK_HDFS_PATH);
		FileCache.copy(new Path(FLINK_PYTHON_FILE_PATH), new Path(FLINK_HDFS_PATH), true);
		env.registerCachedFile(FLINK_HDFS_PATH, FLINK_PYTHON_DC_ID);
		clearPath(FLINK_PYTHON_FILE_PATH);
	}

	private void startPython(String[] args) throws IOException {
		sets = new HashMap();
		StringBuilder argsBuilder = new StringBuilder();
		for (String arg : args) {
			argsBuilder.append(" ").append(arg);
		}
		receiver = new Receiver(null);
		receiver.open(null);
		if (usePython3) {
			process = Runtime.getRuntime().exec("python3 -B " + FLINK_PYTHON_FILE_PATH + FLINK_PYTHON_PLAN_NAME + argsBuilder.toString());
		} else {
			process = Runtime.getRuntime().exec("python -B " + FLINK_PYTHON_FILE_PATH + FLINK_PYTHON_PLAN_NAME + argsBuilder.toString());
		}
		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		try {
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}

		try {
			int value = process.exitValue();
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details.");
			}
		} catch (IllegalThreadStateException ise) {
		}
	}

	private void close() {
		try { //prevent throwing exception so that previous exceptions aren't hidden.
			FileSystem hdfs = FileSystem.get(new URI(FLINK_HDFS_PATH));
			hdfs.delete(new Path(FLINK_HDFS_PATH), true);

			FileSystem local = FileSystem.getLocalFileSystem();
			local.delete(new Path(FLINK_PYTHON_FILE_PATH), true);
			local.delete(new Path(FLINK_TMP_DATA_DIR), true);
			receiver.close();
		} catch (NullPointerException npe) {
		} catch (IOException ioe) {
			LOG.error("PythonAPI file cleanup failed. " + ioe.getMessage());
		} catch (URISyntaxException use) { // can't occur
		}
		try {
			process.exitValue();
		} catch (NullPointerException npe) { //exception occurred before process was started
		} catch (IllegalThreadStateException ise) { //process still active
			process.destroy();
		}
	}

	//=====Plan Binding=================================================================================================
	protected class PythonOperationInfo extends OperationInfo {
		protected byte[] operator;
		protected String meta;
		protected boolean combine;
		protected byte[] combineOperator;
		protected String name;

		private int[] tupleToIntArray(Tuple tuple) {
			int[] keys = new int[tuple.getArity()];
			for (int y = 0; y < tuple.getArity(); y++) {
				keys[y] = (Integer) tuple.getField(y);
			}
			return keys;
		}

		protected PythonOperationInfo(Operations mode) throws IOException {
			Object tmpType;
			parentID = (Integer) receiver.getNormalizedRecord();
			setID = (Integer) receiver.getNormalizedRecord();
			switch (mode) {
				case COGROUP:
					otherID = (Integer) receiver.getNormalizedRecord();
					keys1 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					keys2 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					operator = (byte[]) receiver.getRecord();
					meta = (String) receiver.getRecord();
					tmpType = receiver.getRecord();
					types = tmpType == null ? null : getForObject(tmpType);
					name = (String) receiver.getRecord();
					break;
				case CROSS:
				case CROSS_H:
				case CROSS_T:
					otherID = (Integer) receiver.getNormalizedRecord();
					operator = (byte[]) receiver.getRecord();
					meta = (String) receiver.getRecord();
					tmpType = receiver.getRecord();
					types = tmpType == null ? null : getForObject(tmpType);
					int cProjectCount = (Integer) receiver.getNormalizedRecord();
					projections = new ProjectionEntry[cProjectCount];
					for (int x = 0; x < cProjectCount; x++) {
						String side = (String) receiver.getRecord();
						int[] keys = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
						projections[x] = new ProjectionEntry(ProjectionSide.valueOf(side.toUpperCase()), keys);
					}
					name = (String) receiver.getRecord();
					break;
				case REDUCE:
				case GROUPREDUCE:
					operator = (byte[]) receiver.getRecord();
					combineOperator = (byte[]) receiver.getRecord();
					meta = (String) receiver.getRecord();
					tmpType = receiver.getRecord();
					types = tmpType == null ? null : getForObject(tmpType);
					combine = (Boolean) receiver.getRecord();
					name = (String) receiver.getRecord();
					break;
				case JOIN:
				case JOIN_H:
				case JOIN_T:
					keys1 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					keys2 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					otherID = (Integer) receiver.getNormalizedRecord();
					operator = (byte[]) receiver.getRecord();
					meta = (String) receiver.getRecord();
					tmpType = receiver.getRecord();
					types = tmpType == null ? null : getForObject(tmpType);
					int jProjectCount = (Integer) receiver.getNormalizedRecord();
					projections = new ProjectionEntry[jProjectCount];
					for (int x = 0; x < jProjectCount; x++) {
						String side = (String) receiver.getRecord();
						int[] keys = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
						projections[x] = new ProjectionEntry(ProjectionSide.valueOf(side.toUpperCase()), keys);
					}
					name = (String) receiver.getRecord();
					break;
				case MAPPARTITION:
				case FLATMAP:
				case MAP:
				case FILTER:
					operator = (byte[]) receiver.getRecord();
					meta = (String) receiver.getRecord();
					tmpType = receiver.getRecord();
					types = tmpType == null ? null : getForObject(tmpType);
					name = (String) receiver.getRecord();
					break;
				case PROJECTION:
					keys1 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					break;
				case GROUPBY:
					keys1 = tupleToIntArray((Tuple) receiver.getNormalizedRecord());
					break;
				case SORT:
					field = (Integer) receiver.getNormalizedRecord();
					order = (Integer) receiver.getNormalizedRecord();
					break;
				case UNION:
					otherID = (Integer) receiver.getNormalizedRecord();
					break;
				default:
					throw new UnsupportedOperationException("This operation is not implemented in the Python API: " + mode);
			}
		}
	}

	@Override
	protected PythonOperationInfo createOperationInfo(String identifier) throws IOException {
		return new PythonOperationInfo(Operations.valueOf(identifier.toUpperCase()));
	}

	@Override
	protected DataSet applyCoGroupOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, PythonOperationInfo info) {
		return new CoGroupPythonOperator(
				op1,
				op2,
				new Keys.ExpressionKeys(firstKeys, op1.getType()),
				new Keys.ExpressionKeys(secondKeys, op2.getType()),
				new PythonCoGroup(info.setID, info.operator, info.types, info.meta),
				getForObject(info.types), info.name);
	}

	@Override
	protected DataSet applyCrossOperation(DataSet op1, DataSet op2, int mode, PythonOperationInfo info) {
		switch (mode) {
			case 0:
				return op1.cross(op2).name("PythonCrossPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			case 1:
				return op1.crossWithHuge(op2).name("PythonCrossPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			case 2:
				return op1.crossWithTiny(op2).name("PythonCrossPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			default:
				throw new IllegalArgumentException("Invalid Cross mode specified: " + mode);
		}
	}

	@Override
	protected DataSet applyFilterOperation(DataSet op1, PythonOperationInfo info) {
		return op1.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
	}

	@Override
	protected DataSet applyFlatMapOperation(DataSet op1, PythonOperationInfo info) {
		return op1.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
	}

	@Override
	protected DataSet applyGroupReduceOperation(DataSet op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID, info.combineOperator, info.meta))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		}
	}

	@Override
	protected DataSet applyGroupReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID, info.combineOperator, info.meta))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		}
	}

	@Override
	protected DataSet applyGroupReduceOperation(SortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID, info.combineOperator, info.meta))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonGroupReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		}
	}

	@Override
	protected DataSet applyJoinOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, int mode, PythonOperationInfo info) {
		switch (mode) {
			case 0:
				return op1.join(op2).where(firstKeys).equalTo(secondKeys).name("PythonJoinPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			case 1:
				return op1.joinWithHuge(op2).where(firstKeys).equalTo(secondKeys).name("PythonJoinPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			case 2:
				return op1.joinWithTiny(op2).where(firstKeys).equalTo(secondKeys).name("PythonJoinPreStep")
						.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
			default:
				throw new IllegalArgumentException("Invalid join mode specified.");
		}
	}

	@Override
	protected DataSet applyMapOperation(DataSet op1, PythonOperationInfo info) {
		return op1.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
	}

	@Override
	protected DataSet applyMapPartitionOperation(DataSet op1, PythonOperationInfo info) {
		return op1.mapPartition(new PythonMapPartition(info.setID, info.operator, info.types, info.meta)).name(info.name);
	}

	@Override
	protected DataSet applyReduceOperation(DataSet op1, PythonOperationInfo info) {
		return op1.reduceGroup(new PythonCombineIdentity())
				.setCombinable(false).name("PythonReducePreStep")
				.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
				.name(info.name);
	}

	@Override
	protected DataSet applyReduceOperation(UnsortedGrouping op1, PythonOperationInfo info) {
		if (info.combine) {
			return op1.reduceGroup(new PythonCombineIdentity(info.setID, info.combineOperator, info.meta))
					.setCombinable(true).name("PythonCombine")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		} else {
			return op1.reduceGroup(new PythonCombineIdentity())
					.setCombinable(false).name("PythonReducePreStep")
					.mapPartition(new PythonMapPartition(info.setID * -1, info.operator, info.types, info.meta))
					.name(info.name);
		}
	}
}
