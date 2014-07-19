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
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.SIGNAL_END;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_BOOLEAN;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_BYTE;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_DOUBLE;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_INTEGER;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_LONG;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_NULL;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_SHORT;
import static org.apache.flink.languagebinding.api.java.python.streaming.RawSender.TYPE_STRING;
import org.apache.flink.languagebinding.api.java.streaming.Receiver;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class RawReceiver extends Receiver {
	private boolean receivedLast = false;
	private final AbstractFunction function;

	public RawReceiver(AbstractFunction function, InputStream in) {
		super(in);
		this.function = function;
	}

	public class Sentinel {
	}

	@Override
	public Object receiveRecord() throws IOException {
		int meta = inStream.read();
		if (meta == SIGNAL_END) {
			return new Sentinel();
		}
		try {
			receivedLast = (meta & 32) == 32;
			int size = meta & 31;
			if (size < 31) {
				Tuple tuple = createTuple(size);
				for (int x = 0; x < tuple.getArity(); x++) {
					tuple.setField(receiveField(x), x);
				}
				return tuple;
			}
			return receiveField(0);
		} catch (IllegalArgumentException iae) {
			throw new IllegalArgumentException(iae.getMessage() + " meta = " + meta, iae);
		}
	}

	@Override
	public void receiveRecords(Collector collector) throws IOException {
		Object value = receiveRecord();
		if (!(value instanceof Sentinel)) {
			collector.collect(value);
			while (!receivedLast) {
				collector.collect(receiveRecord());
			}
			receivedLast = false;
		} else {
			receivedLast = true;
		}

	}

	private Object receiveField(int index) throws IOException {
		int type = inStream.read();
		switch (type) {
			case TYPE_BOOLEAN:
				return readBool();
			case TYPE_BYTE:
				return readByte();
			case TYPE_SHORT:
				return readShort();
			case TYPE_INTEGER:
				return readInt();
			case TYPE_LONG:
				return readLong();
			case TYPE_STRING:
				return readString();
			case TYPE_DOUBLE:
				return readDouble();
			case TYPE_NULL:
				return null;
			default:
				System.out.flush();
				throw new IllegalArgumentException("Unknown TypeID encountered: " + type);
		}
	}

	private boolean readBool() throws IOException {
		int value = inStream.read();
		return value == 1;
	}

	private byte readByte() throws IOException {
		byte[] bytes = new byte[1];
		inStream.read(bytes);
		return bytes[0];
	}

	private short readShort() throws IOException {
		int i1 = inStream.read();
		int i2 = inStream.read();
		return (short) (i1 + i2);
	}

	private int readInt() throws IOException {
		byte[] buffer = new byte[4];
		inStream.read(buffer, 0, 4);
		return ByteBuffer.wrap(buffer).getInt();
	}

	private long readLong() throws IOException {
		byte[] buffer = new byte[8];
		inStream.read(buffer, 0, 8);
		return ByteBuffer.wrap(buffer).getLong();
	}

	private String readString() throws IOException {
		int size = readInt();
		int read = 0;
		int toRead = size;
		byte[] buffer = new byte[size];
		while (toRead > 0) {
			read += inStream.read(buffer, read, toRead);
			toRead -= read;
		}
		return new String(buffer, "UTF-8");
	}

	private double readDouble() throws IOException {
		byte[] buffer = new byte[8];
		inStream.read(buffer, 0, 8);
		return ByteBuffer.wrap(buffer).getDouble();
	}
}
