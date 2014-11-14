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
package org.apache.flink.languagebinding.api.java.common;

import java.io.IOException;
import java.util.HashMap;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.CrossOperator.ProjectCross;
import org.apache.flink.api.java.operators.Grouping;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UdfOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.languagebinding.api.java.common.OperationInfo.ProjectionEntry;
import org.apache.flink.languagebinding.api.java.common.streaming.Receiver;

/**
 * Generic class to construct a Flink plan based on external data.
 *
 * @param <INFO>
 */
public abstract class PlanBinder<INFO extends OperationInfo> {
	public static final boolean BOOLEAN = true;
	public static final byte BYTE = new Integer(1).byteValue();
	public static final double DOUBLE = 1.5;
	public static final double FLOAT = 1.5F;
	public static final int INT = 1;
	public static final long LONG = 1L;
	public static final int SHORT = new Integer(1).shortValue();
	public static final String STRING = "type";

	public static final String PLANBINDER_CONFIG_BCVAR_COUNT = "PLANBINDER_BCVAR_COUNT";
	public static final String PLANBINDER_CONFIG_BCVAR_NAME_PREFIX = "PLANBINDER_BCVAR_";

	protected static String FLINK_HDFS_PATH = "hdfs:/tmp";
	public static final String FLINK_TMP_DATA_DIR = System.getProperty("java.io.tmpdir") + "/flink_data";

	public static void setLocalMode() {
		FLINK_HDFS_PATH = System.getProperty("java.io.tmpdir") + "/flink";
	}

	protected HashMap<Integer, Object> sets;
	public static ExecutionEnvironment env;
	protected Receiver receiver;

	public static final int MAPPED_FILE_SIZE = 1024 * 1024 * 64;

	public static int ID = 0;

	//====Plan==========================================================================================================
	protected void receivePlan() throws IOException {
		receiveParameters();
		receiveOperations();
	}

	//====Environment===================================================================================================
	/**
	 * This enum contains the identifiers for all supported environment parameters.
	 */
	private enum Parameters {
		DOP,
		MODE
	}

	private void receiveParameters() throws IOException {
		Integer parameterCount = (Integer) receiver.getNormalizedRecord();

		for (int x = 0; x < parameterCount; x++) {
			Tuple value = (Tuple) receiver.getNormalizedRecord();
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

	//====Operations====================================================================================================
	/**
	 * This enum contains the identifiers for all supported DataSet operations.
	 */
	protected enum Operations {
		SOURCE_CSV, SOURCE_TEXT, SOURCE_VALUE, SINK_CSV, SINK_TEXT, SINK_PRINT,
		COGROUP, CROSS, CROSS_H, CROSS_T, FILTER, FLATMAP, GROUPREDUCE, JOIN, JOIN_H, JOIN_T, MAP, REDUCE, MAPPARTITION,
		PROJECTION, SORT, UNION, FIRST, DISTINCT, GROUPBY, REBALANCE,
		BROADCAST
	}

	protected void receiveOperations() throws IOException {
		Integer operationCount = (Integer) receiver.getNormalizedRecord();
		for (int x = 0; x < operationCount; x++) {
			String identifier = (String) receiver.getRecord();
			switch (Operations.valueOf(identifier.toUpperCase())) {
				case SOURCE_CSV:
					createCsvSource();
					break;
				case SOURCE_TEXT:
					createTextSource();
					break;
				case SOURCE_VALUE:
					createValueSource();
					break;
				case SINK_CSV:
					createCsvSink();
					break;
				case SINK_TEXT:
					createTextSink();
					break;
				case SINK_PRINT:
					createPrintSink();
					break;
				case BROADCAST:
					createBroadcastVariable();
					break;
				case COGROUP:
					createCoGroupOperation(createOperationInfo(identifier));
					break;
				case CROSS:
					createCrossOperation(0, createOperationInfo(identifier));
					break;
				case CROSS_H:
					createCrossOperation(1, createOperationInfo(identifier));
					break;
				case CROSS_T:
					createCrossOperation(2, createOperationInfo(identifier));
					break;
				case DISTINCT:
					createDistinctOperation(createOperationInfo(identifier));
				case FILTER:
					createFilterOperation(createOperationInfo(identifier));
					break;
				case FIRST:
					createFirstOperation(createOperationInfo(identifier));
					break;
				case FLATMAP:
					createFlatMapOperation(createOperationInfo(identifier));
					break;
				case GROUPREDUCE:
					createGroupReduceOperation(createOperationInfo(identifier));
					break;
				case JOIN:
					createJoinOperation(0, createOperationInfo(identifier));
					break;
				case JOIN_H:
					createJoinOperation(1, createOperationInfo(identifier));
					break;
				case JOIN_T:
					createJoinOperation(2, createOperationInfo(identifier));
					break;
				case MAP:
					createMapOperation(createOperationInfo(identifier));
					break;
				case MAPPARTITION:
					createMapPartitionOperation(createOperationInfo(identifier));
					break;
				case PROJECTION:
					createProjectOperation(createOperationInfo(identifier));
					break;
				case REBALANCE:
					createRebalanceOperation(createOperationInfo(identifier));
				case REDUCE:
					createReduceOperation(createOperationInfo(identifier));
					break;
				case GROUPBY:
					createGroupOperation(createOperationInfo(identifier));
					break;
				case SORT:
					createSortOperation(createOperationInfo(identifier));
					break;
				case UNION:
					createUnionOperation(createOperationInfo(identifier));
					break;
			}
		}
	}

	private void createCsvSource() throws IOException {
		int id = (Integer) receiver.getNormalizedRecord();
		String path = (String) receiver.getRecord();
		String fieldDelimiter = (String) receiver.getRecord();
		String lineDelimiter = (String) receiver.getRecord();
		Tuple types = (Tuple) receiver.getRecord();

		Class[] classes = new Class[types.getArity()];
		for (int x = 0; x < types.getArity(); x++) {
			classes[x] = types.getField(x).getClass();
		}
		sets.put(id, env.createInput(new CsvInputFormat(new Path(path), lineDelimiter, fieldDelimiter.charAt(0), classes), getForObject(types)).name("CsvSource"));
	}

	private void createTextSource() throws IOException {
		int id = (Integer) receiver.getNormalizedRecord();
		String path = (String) receiver.getRecord();
		sets.put(id, env.readTextFile(path).name("TextSource"));
	}

	private void createValueSource() throws IOException {
		int id = (Integer) receiver.getNormalizedRecord();
		int valueCount = (Integer) receiver.getNormalizedRecord();
		Object[] values = new Object[valueCount];
		for (int x = 0; x < valueCount; x++) {
			values[x] = receiver.getRecord();
		}
		sets.put(id, env.fromElements(values).name("ValueSource"));
	}

	private void createCsvSink() throws IOException {
		int parentID = (Integer) receiver.getNormalizedRecord();
		String path = (String) receiver.getRecord();
		String fieldDelimiter = (String) receiver.getRecord();
		String lineDelimiter = (String) receiver.getRecord();
		WriteMode writeMode = ((Integer) receiver.getNormalizedRecord()) == 1
				? WriteMode.OVERWRITE
				: WriteMode.NO_OVERWRITE;
		DataSet parent = (DataSet) sets.get(parentID);
		parent.writeAsCsv(path, lineDelimiter, fieldDelimiter, writeMode).name("CsvSink");
	}

	private void createTextSink() throws IOException {
		int parentID = (Integer) receiver.getNormalizedRecord();
		String path = (String) receiver.getRecord();
		WriteMode writeMode = ((Integer) receiver.getNormalizedRecord()) == 1
				? WriteMode.OVERWRITE
				: WriteMode.NO_OVERWRITE;
		DataSet parent = (DataSet) sets.get(parentID);
		parent.writeAsText(path, writeMode).name("TextSink");
	}

	private void createPrintSink() throws IOException {
		int parentID = (Integer) receiver.getNormalizedRecord();
		DataSet parent = (DataSet) sets.get(parentID);
		parent.print().name("PrintSink");
	}

	private void createBroadcastVariable() throws IOException {
		int parentID = (Integer) receiver.getNormalizedRecord();
		int otherID = (Integer) receiver.getNormalizedRecord();
		String name = (String) receiver.getRecord();
		UdfOperator op1 = (UdfOperator) sets.get(parentID);
		DataSet op2 = (DataSet) sets.get(otherID);

		op1.withBroadcastSet(op2, name);
		Configuration c = ((UdfOperator) op1).getParameters();

		if (c == null) {
			c = new Configuration();
		}

		int count = c.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);
		c.setInteger(PLANBINDER_CONFIG_BCVAR_COUNT, count + 1);
		c.setString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + count, name);

		op1.withParameters(c);
	}

	/**
	 * This method creates an OperationInfo object based on the operation-identifier passed.
	 *
	 * @param operationIdentifier
	 * @return
	 * @throws IOException
	 */
	protected abstract INFO createOperationInfo(String operationIdentifier) throws IOException;

	private void createCoGroupOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, applyCoGroupOperation(op1, op2, info.keys1, info.keys2, info));
	}

	protected abstract DataSet applyCoGroupOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, INFO info);

	private void createCrossOperation(int mode, INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && info.projectionKeys1 == null && info.projectionKeys2 == null) {
			//UDF-Cross
			sets.put(info.setID, applyCrossOperation(op1, op2, mode, info));
		} else {
			DefaultCross defaultResult;
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
			if (info.projections.length == 0) {
				sets.put(info.setID, defaultResult.name("DefaultCross"));
			} else {
				ProjectCross project = null;
				for (ProjectionEntry pe : info.projections) {
					switch (pe.side) {
						case FIRST:
							project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
							break;
						case SECOND:
							project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
							break;
					}
				}
				sets.put(info.setID, project.name("ProjectCross"));
			}
		}
	}

	protected abstract DataSet applyCrossOperation(DataSet op1, DataSet op2, int mode, INFO info);

	private void createDistinctOperation(INFO info) {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.distinct());
	}

	private void createFilterOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFilterOperation(op1, info));
	}

	protected abstract DataSet applyFilterOperation(DataSet op1, INFO info);

	private void createFlatMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyFlatMapOperation(op1, info));
	}

	protected abstract DataSet applyFlatMapOperation(DataSet op1, INFO info);

	private void createFirstOperation(INFO info) {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.first(info.count));
	}

	private void createGroupOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.groupBy(info.keys1));
	}

	private void createGroupReduceOperation(INFO info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyGroupReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((UnsortedGrouping) op1, info));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, applyGroupReduceOperation((SortedGrouping) op1, info));
		}
	}

	protected abstract DataSet applyGroupReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(UnsortedGrouping op1, INFO info);

	protected abstract DataSet applyGroupReduceOperation(SortedGrouping op1, INFO info);

	private void createJoinOperation(int mode, INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);

		if (info.types != null && info.projectionKeys1 == null & info.projectionKeys2 == null) {
			//UDF-Join
			sets.put(info.setID, applyJoinOperation(op1, op2, info.keys1, info.keys2, mode, info));
		} else {
			//Key-Join
			DefaultJoin defaultResult;
			switch (mode) {
				case 0:
					defaultResult = op1.join(op2).where(info.keys1).equalTo(info.keys2);
					break;
				case 1:
					defaultResult = op1.joinWithHuge(op2).where(info.keys1).equalTo(info.keys2);
					break;
				case 2:
					defaultResult = op1.joinWithTiny(op2).where(info.keys1).equalTo(info.keys2);
					break;
				default:
					throw new IllegalArgumentException("Invalid join mode specified.");
			}
			if (info.projections.length == 0) {
				sets.put(info.setID, defaultResult.name("DefaultJoin"));
			} else {
				//Project-Join
				ProjectJoin project = null;
				for (ProjectionEntry pe : info.projections) {
					switch (pe.side) {
						case FIRST:
							project = project == null ? defaultResult.projectFirst(pe.keys) : project.projectFirst(pe.keys);
							break;
						case SECOND:
							project = project == null ? defaultResult.projectSecond(pe.keys) : project.projectSecond(pe.keys);
							break;
					}
				}
				sets.put(info.setID, project.name("ProjectJoin"));
			}
		}
	}

	protected abstract DataSet applyJoinOperation(DataSet op1, DataSet op2, int[] firstKeys, int[] secondKeys, int mode, INFO info);

	private void createMapOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapOperation(op1, info));
	}

	protected abstract DataSet applyMapOperation(DataSet op1, INFO info);

	private void createMapPartitionOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, applyMapPartitionOperation(op1, info));
	}

	protected abstract DataSet applyMapPartitionOperation(DataSet op1, INFO info);

	protected void createProjectOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op1.project(info.keys1));
	}

	private void createRebalanceOperation(INFO info) {
		DataSet op = (DataSet) sets.get(info.parentID);
		sets.put(info.setID, op.rebalance());
	}

	private void createReduceOperation(INFO info) {
		Object op1 = sets.get(info.parentID);
		if (op1 instanceof DataSet) {
			sets.put(info.setID, applyReduceOperation((DataSet) op1, info));
			return;
		}
		if (op1 instanceof UnsortedGrouping) {
			sets.put(info.setID, applyReduceOperation((UnsortedGrouping) op1, info));
		}
	}

	protected abstract DataSet applyReduceOperation(DataSet op1, INFO info);

	protected abstract DataSet applyReduceOperation(UnsortedGrouping op1, INFO info);

	protected void createSortOperation(INFO info) {
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
			sets.put(info.setID, ((UnsortedGrouping) op1).sortGroup(info.field, o));
			return;
		}
		if (op1 instanceof SortedGrouping) {
			sets.put(info.setID, ((SortedGrouping) op1).sortGroup(info.field, o));
		}
	}

	protected void createUnionOperation(INFO info) {
		DataSet op1 = (DataSet) sets.get(info.parentID);
		DataSet op2 = (DataSet) sets.get(info.otherID);
		sets.put(info.setID, op1.union(op2).name("Union"));
	}
}
