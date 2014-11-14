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
package org.apache.flink.languagebinding.api.java.common.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.MAPPED_FILE_SIZE;

/**
 * General-purpose class to write data to memory-mapped files.
 */
public class Sender implements Serializable {
	public static final byte TYPE_TUPLE = (byte) 11;
	public static final byte TYPE_BOOLEAN = (byte) 10;
	public static final byte TYPE_BYTE = (byte) 9;
	public static final byte TYPE_SHORT = (byte) 8;
	public static final byte TYPE_INTEGER = (byte) 7;
	public static final byte TYPE_LONG = (byte) 6;
	public static final byte TYPE_DOUBLE = (byte) 4;
	public static final byte TYPE_FLOAT = (byte) 5;
	public static final byte TYPE_CHAR = (byte) 3;
	public static final byte TYPE_STRING = (byte) 2;
	public static final byte TYPE_BYTES = (byte) 1;
	public static final byte TYPE_NULL = (byte) 0;

	private final AbstractRichFunction function;

	private File outputFile;
	private RandomAccessFile outputRAF;
	private FileChannel outputChannel;
	protected MappedByteBuffer fileBuffer;

	private final int offsetLimit = MAPPED_FILE_SIZE - 1024 * 1024 * 3;

	private final Serializer[] serializer = new Serializer[2];

	public Sender(AbstractRichFunction function) {
		this.function = function;
	}

	//=====Setup========================================================================================================
	public void open(String path) throws IOException {
		setupMappedFile(path);
	}

	private void setupMappedFile(String path) throws FileNotFoundException, IOException {
		String outputFilePath = function == null
				? FLINK_TMP_DATA_DIR + "/" + "input"
				: path;

		File x = new File(FLINK_TMP_DATA_DIR);
		x.mkdirs();

		outputFile = new File(outputFilePath);
		if (outputFile.exists()) {
			outputFile.delete();
		}
		outputFile.createNewFile();
		outputRAF = new RandomAccessFile(outputFilePath, "rw");
		outputRAF.setLength(MAPPED_FILE_SIZE);
		outputRAF.seek(MAPPED_FILE_SIZE - 1);
		outputRAF.writeByte(0);
		outputRAF.seek(0);
		outputChannel = outputRAF.getChannel();
		fileBuffer = outputChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAPPED_FILE_SIZE);
	}

	public void close() throws IOException {
		closeMappedFile();
	}

	private void closeMappedFile() throws IOException {
		outputChannel.close();
		outputRAF.close();
	}

	/**
	 * Resets this object to the post-configuration state.
	 */
	public void reset() {
		serializer[0] = null;
		serializer[1] = null;
		fileBuffer.clear();
	}

	//=====Serialization================================================================================================
	/**
	 * Writes a single record to the memory-mapped file. This method does NOT take care of synchronization. The user
	 * must guarantee that the file may be written to before calling this method. This method essentially reserves the
	 * whole buffer for one record. As such it imposes some performance restrictions and should only be used when
	 * absolutely necessary.
	 *
	 * @param value record to send
	 * @return size of the written buffer
	 * @throws IOException
	 */
	public int sendRecord(Object value) throws IOException {
		fileBuffer.clear();
		int group = 0;

		fileBuffer.position(0);
		serializer[group] = getSerializer(value);
		serializer[group].serialize(value);
		int size = fileBuffer.position();

		reset();
		return size;
	}

	/**
	 * Extracts records from an iterator and writes them to the memory-mapped file. This method assumes that all values
	 * in the iterator are of the same type. This method does NOT take care of synchronization. The caller must
	 * guarantee that the file may be written to before calling this method.
	 *
	 * @param i iterator containing records
	 * @param group group to which the iterator belongs, most notably used by CoGroup-functions.
	 * @return size of the written buffer
	 * @throws IOException
	 */
	public int sendBuffer(Iterator i, int group) throws IOException {
		fileBuffer.clear();

		Object value;
		if (serializer[group] == null) {
			value = i.next();
			serializer[group] = getSerializer(value);
			serializer[group].serialize(value);
		}
		while (fileBuffer.position() < offsetLimit && i.hasNext()) {
			value = i.next();
			serializer[group].serialize(value);
		}

		int size = fileBuffer.position();
		return size;
	}

	private enum SupportedTypes {
		TUPLE, BOOLEAN, BYTE, CHARACTER, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, OTHER, NULL
	}

	//=====Serializer===================================================================================================
	private Serializer getSerializer(Object value) throws IOException {
		String className = value.getClass().getSimpleName().toUpperCase();
		if (className.startsWith("TUPLE")) {
			className = "TUPLE";
		}
		SupportedTypes type = SupportedTypes.valueOf(className);
		switch (type) {
			case TUPLE:
				bufferType(TYPE_TUPLE, ((Tuple) value).getArity());
				return new TupleSerializer((Tuple) value);
			case BOOLEAN:
				bufferType(TYPE_BOOLEAN, 0);
				return new BooleanSerializer();
			case BYTE:
				bufferType(TYPE_BYTE, 0);
				return new ByteSerializer();
			case CHARACTER:
				bufferType(TYPE_CHAR, 0);
				return new CharSerializer();
			case SHORT:
				bufferType(TYPE_SHORT, 0);
				return new ShortSerializer();
			case INTEGER:
				bufferType(TYPE_INTEGER, 0);
				return new IntSerializer();
			case LONG:
				bufferType(TYPE_LONG, 0);
				return new LongSerializer();
			case STRING:
				bufferType(TYPE_STRING, 0);
				return new StringSerializer();
			case FLOAT:
				bufferType(TYPE_FLOAT, 0);
				return new FloatSerializer();
			case DOUBLE:
				bufferType(TYPE_DOUBLE, 0);
				return new DoubleSerializer();
			case NULL:
				bufferType(TYPE_NULL, 0);
				return new NullSerializer();
			default:
				throw new IllegalArgumentException("Unknown TypeID encountered: " + type);
		}
	}

	private void bufferType(byte type, int tupleSize) throws IOException {
		switch (type) {
			case TYPE_TUPLE:
				fileBuffer.put(TYPE_TUPLE);
				fileBuffer.putInt(tupleSize);
				break;
			case TYPE_BOOLEAN:
				fileBuffer.put(TYPE_BOOLEAN);
				break;
			case TYPE_BYTE:
				fileBuffer.put(TYPE_BYTE);
				break;
			case TYPE_CHAR:
				fileBuffer.put(TYPE_STRING);
				break;
			case TYPE_SHORT:
				fileBuffer.put(TYPE_SHORT);
				break;
			case TYPE_INTEGER:
				fileBuffer.put(TYPE_INTEGER);
				break;
			case TYPE_LONG:
				fileBuffer.put(TYPE_LONG);
				break;
			case TYPE_STRING:
				fileBuffer.put(TYPE_STRING);
				break;
			case TYPE_FLOAT:
				fileBuffer.put(TYPE_FLOAT);
				break;
			case TYPE_DOUBLE:
				fileBuffer.put(TYPE_DOUBLE);
				break;
			case TYPE_NULL:
				fileBuffer.put(TYPE_NULL);
				break;
			default:
				throw new IllegalArgumentException("Unknown TypeID encountered: " + type);
		}
	}

	private abstract class Serializer<T> {
		public abstract void serialize(T value);
	}

	private class ByteSerializer extends Serializer<Byte> {
		@Override
		public void serialize(Byte value) {
			fileBuffer.put(value);
		}
	}

	private class BooleanSerializer extends Serializer<Boolean> {
		@Override
		public void serialize(Boolean value) {
			fileBuffer.put(value ? (byte) 1 : (byte) 0);
		}
	}

	private class CharSerializer extends Serializer<Character> {
		@Override
		public void serialize(Character value) {
			fileBuffer.put((value + "").getBytes());
		}
	}

	private class ShortSerializer extends Serializer<Short> {
		@Override
		public void serialize(Short value) {
			fileBuffer.putShort(value);
		}
	}

	private class IntSerializer extends Serializer<Integer> {
		@Override
		public void serialize(Integer value) {
			fileBuffer.putInt(value);
		}
	}

	private class LongSerializer extends Serializer<Long> {
		@Override
		public void serialize(Long value) {
			fileBuffer.putLong(value);
		}
	}

	private class StringSerializer extends Serializer<String> {
		private byte[] buffer;

		@Override
		public void serialize(String value) {
			buffer = value.getBytes();
			fileBuffer.putInt(buffer.length);
			fileBuffer.put(buffer);
		}
	}

	private class FloatSerializer extends Serializer<Float> {
		@Override
		public void serialize(Float value) {
			fileBuffer.putFloat(value);
		}
	}

	private class DoubleSerializer extends Serializer<Double> {
		@Override
		public void serialize(Double value) {
			fileBuffer.putDouble(value);
		}
	}

	private class NullSerializer extends Serializer<Object> {
		@Override
		public void serialize(Object value) {
		}
	}

	private class TupleSerializer extends Serializer<Tuple> {
		private final Serializer[] serializer;

		public TupleSerializer(Tuple value) throws IOException {
			serializer = new Serializer[value.getArity()];
			for (int x = 0; x < serializer.length; x++) {
				serializer[x] = getSerializer(value.getField(x));
			}
		}

		@Override
		public void serialize(Tuple value) {
			for (int x = 0; x < serializer.length; x++) {
				serializer[x].serialize(value.getField(x));
			}
		}
	}
}
