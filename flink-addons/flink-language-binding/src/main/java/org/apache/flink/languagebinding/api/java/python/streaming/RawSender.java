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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.languagebinding.api.java.streaming.Sender;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/*
 MetaByte scheme:
 |		Group(00/11)	 |
 |ContFlag:1|EndFlag:1|LastFlag:1|Size:5|

 */
public class RawSender extends Sender {
	public static final int SIGNAL_CONT = 128;
	public static final int SIGNAL_END = 64;

	public static final int MASK_LAST = 32;
	public static final int MASK_GROUP0 = 0;
	public static final int MASK_GROUP1 = 192;

	public static final int TYPE_BOOLEAN = 128;
	public static final int TYPE_BYTE = 64;
	public static final int TYPE_SHORT = 32;
	public static final int TYPE_INTEGER = 16;
	public static final int TYPE_LONG = 8;
	public static final int TYPE_DOUBLE = 4;
	public static final int TYPE_FLOAT = 5;
	public static final int TYPE_STRING = 2;
	public static final int TYPE_OTHER = 1;
	public static final int TYPE_NULL = 0;

	private final SupportedTypes[][] classes = new SupportedTypes[2][];
	private final AbstractFunction function;

	public enum SupportedTypes {
		BOOLEAN,
		BYTE,
		CHARACTER,
		SHORT,
		INTEGER,
		LONG,
		FLOAT,
		DOUBLE,
		STRING,
		OTHER,
		NULL
	}

	public RawSender(AbstractFunction function, OutputStream stream) {
		super(stream);
		this.function = function;
	}

	@Override
	public void sendRecord(Object value) throws IOException {
		sendRecord(value, 0, false);
	}

	@Override
	public void sendRecord(Object value, int group, boolean isLast) throws IOException {
		int meta = 0;

		if (group == 1) {
			meta |= MASK_GROUP1;
		}

		if (isLast) {
			meta |= MASK_LAST;
		}

		if (classes[group] == null) {
			extractClasses(value, group);
		}

		if (value instanceof Tuple) {
			meta |= ((Tuple) value).getArity();
			outStream.write(meta);
			outStream.flush();
			for (int x = 0; x < ((Tuple) value).getArity(); x++) {
				sendField(((Tuple) value).getField(x), x, group);
			}
		} else {
			meta |= 31;
			outStream.write(meta);
			outStream.flush();
			sendField(value, 0, group);
		}
	}

	private void extractClasses(Object value, int group) {
		if (value instanceof Tuple) {
			Tuple tuple = (Tuple) value;
			classes[group] = new SupportedTypes[tuple.getArity()];
			for (int x = 0; x < tuple.getArity(); x++) {
				getClassForValue(tuple.getField(x), x, group);
			}
		} else {
			classes[group] = new SupportedTypes[1];
			getClassForValue(value, 0, group);
		}
	}

	private void getClassForValue(Object value, int index, int group) {
		try {
			classes[group][index] = SupportedTypes.valueOf(value.getClass().getSimpleName().toUpperCase());
		} catch (IllegalArgumentException iae) {
			classes[group][index] = SupportedTypes.OTHER;
		} catch (NullPointerException npe) {
			classes[group][index] = SupportedTypes.NULL;
		}
	}

	private void sendField(Object value, int index, int group) throws IOException {
		byte[] buffer;
		switch (classes[group][index]) {
			case BOOLEAN:
				outStream.write(TYPE_BOOLEAN);
				if ((Boolean) value) {
					outStream.write(1);
				} else {
					outStream.write(0);
				}
				break;
			case BYTE:
				outStream.write(TYPE_BYTE);
				outStream.write((Byte) value);
				break;
			case CHARACTER:
				outStream.write(TYPE_STRING);
				outStream.write(((Character) value + "").getBytes());
				break;
			case SHORT:
				outStream.write(TYPE_SHORT);
				buffer = new byte[2];
				ByteBuffer.wrap(buffer).putShort((Short) value);
				outStream.write(buffer);
				break;
			case INTEGER:
				outStream.write(TYPE_INTEGER);
				buffer = new byte[4];
				ByteBuffer.wrap(buffer).putInt((Integer) value);
				outStream.write(buffer);
				break;
			case LONG:
				outStream.write(TYPE_LONG);
				buffer = new byte[8];
				ByteBuffer.wrap(buffer).putLong((Long) value);
				outStream.write(buffer);
				break;
			case STRING:
				outStream.write(TYPE_STRING);
				buffer = new byte[4];
				ByteBuffer.wrap(buffer).putInt(((String) value).getBytes("UTF-8").length);
				outStream.write(buffer);
				outStream.write(((String) value).getBytes());
				break;
			case FLOAT:
				outStream.write(TYPE_FLOAT);
				buffer = new byte[4];
				ByteBuffer.wrap(buffer).putFloat((Float) value);
				outStream.write(buffer);
				break;
			case DOUBLE:
				outStream.write(TYPE_DOUBLE);
				buffer = new byte[8];
				ByteBuffer.wrap(buffer).putDouble((Double) value);
				outStream.write(buffer);
				break;
			case OTHER:
				outStream.write(TYPE_STRING);
				buffer = new byte[4];
				ByteBuffer.wrap(buffer).putInt(value.toString().getBytes().length);
				outStream.write(value.toString().getBytes());
				break;
			case NULL:
				outStream.write(TYPE_NULL);
				break;
		}
		outStream.flush();
	}

	@Override
	public void sendContinueSignal() throws IOException {
		outStream.write(SIGNAL_CONT);
		outStream.flush();
	}

	@Override
	public void sendCompletionSignal() throws IOException {
		outStream.write(SIGNAL_END);
		outStream.flush();
	}
}
