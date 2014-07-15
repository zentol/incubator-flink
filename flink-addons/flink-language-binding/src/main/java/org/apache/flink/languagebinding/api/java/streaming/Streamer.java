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
package org.apache.flink.languagebinding.api.java.streaming;

import org.apache.flink.util.Collector;
import java.io.IOException;
import java.util.Iterator;

/**
 * General purpose class to stream data between two processes with 1 input/output types.
 */
public abstract class Streamer {
	protected Sender sender;
	protected Receiver receiver;

	public abstract void open() throws IOException;

	/**
	 Closes this streamer.
	 @throws IOException 
	 */
	public void close() throws IOException {
		sender.sendCompletionSignal();
		sender.close();
		receiver.close();
	}

	/**
	 * Sends one record and returns one record.
	 * Used by Map/Filter-functions.
	 * @param record
	 * @return result
	 * @throws IOException 
	 */
	public Object stream(Object record) throws IOException {
		sender.sendRecord(record);
		Object result = receiver.receiveRecord();
		return result;
	}

	/**
	 * Sends two records and returns one record. Used by Cross/Join-functions.
	 * Used by Reduce-functions.
	 * @param record1 first record
	 * @param record2 second record
	 * @return result
	 * @throws IOException
	 */
	public Object stream(Object record1, Object record2) throws IOException {
		sender.sendRecord(record1);
		sender.sendRecord(record2);
		return receiver.receiveRecord();
	}

	/**
	 * Sends one record of both groups and returns one record. Used by Cross/Join-functions.
	 * Used by Join/Cross-functions.
	 * @param record1 first record
	 * @param record2 second record
	 * @return result
	 * @throws IOException
	 */
	public Object streamWithGroups(Object record1, Object record2) throws IOException {
		sender.sendRecord(record1, 0, false);
		sender.sendRecord(record2, 1, false);
		return receiver.receiveRecord();
	}

	/**
	 * Sends one record and collects multiple records.
	 * Used by FlatMap-functions.
	 * @param record
	 * @param collector
	 * @throws IOException 
	 */
	public void stream(Object record, Collector collector) throws IOException {
		sender.sendRecord(record);
		receiver.receiveRecords(collector);
	}

	/**
	 * Sends multiple record and collects multiple records.
	 * Used by GroupReduce-functions.
	 * @param iterator
	 * @param collector
	 * @throws IOException 
	 */
	public void stream(Iterator iterator, Collector collector) throws IOException {
		if (iterator.hasNext()) {
			sender.sendContinueSignal();
			sender.sendRecords(iterator);
			receiver.receiveRecords(collector);
		}
	}

	/**
	 * Sends multiple records of two groups and collects multiple records.
	 * Used by CoGroup-functions.
	 * @param iterator1
	 * @param iterator2
	 * @param collector
	 * @throws IOException
	 */
	public void stream(Iterator iterator1, Iterator iterator2, Collector collector) throws IOException {
		sender.sendContinueSignal();
		if (iterator1.hasNext()) {
			sender.sendContinueSignal();
		} else {
			sender.sendCompletionSignal();
		}
		if (iterator2.hasNext()) {
			sender.sendContinueSignal();
		} else {
			sender.sendCompletionSignal();
		}
		while (iterator1.hasNext() || iterator2.hasNext()) {
			if (iterator1.hasNext()) {
				sender.sendRecord(iterator1.next(), 0, !iterator1.hasNext());
			}
			if (iterator2.hasNext()) {
				sender.sendRecord(iterator2.next(), 1, !iterator2.hasNext());
			}
		}
		receiver.receiveRecords(collector);
	}
}
