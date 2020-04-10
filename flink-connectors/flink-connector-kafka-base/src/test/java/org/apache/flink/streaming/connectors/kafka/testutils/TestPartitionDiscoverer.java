/*
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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;

import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utility {@link AbstractPartitionDiscoverer} for tests that allows
 * mocking the sequence of consecutive metadata fetch calls to Kafka.
 */
public class TestPartitionDiscoverer extends AbstractPartitionDiscoverer {

	private final KafkaTopicsDescriptor topicsDescriptor;

	private final Function<Integer, List<String>> mockGetAllTopicsReturnSequence;
	private final Function<Integer, List<KafkaTopicPartition>> mockGetAllPartitionsForTopicsReturnSequence;

	private int getAllTopicsInvokeCount = 0;
	private int getAllPartitionsForTopicsInvokeCount = 0;

	public TestPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks,
			Function<Integer, List<String>> mockGetAllTopicsReturnSequence,
			Function<Integer, List<KafkaTopicPartition>> mockGetAllPartitionsForTopicsReturnSequence) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);

		this.topicsDescriptor = topicsDescriptor;
		this.mockGetAllTopicsReturnSequence = mockGetAllTopicsReturnSequence;
		this.mockGetAllPartitionsForTopicsReturnSequence = mockGetAllPartitionsForTopicsReturnSequence;
	}

	@Override
	protected List<String> getAllTopics() {
		assertTrue(topicsDescriptor.isTopicPattern());
		return mockGetAllTopicsReturnSequence.apply(getAllTopicsInvokeCount++);
	}

	@Override
	protected List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) {
		if (topicsDescriptor.isFixedTopics()) {
			assertEquals(topicsDescriptor.getFixedTopics(), topics);
		} else {
			assertEquals(mockGetAllTopicsReturnSequence.apply(getAllPartitionsForTopicsInvokeCount - 1), topics);
		}
		return mockGetAllPartitionsForTopicsReturnSequence.apply(getAllPartitionsForTopicsInvokeCount++);
	}

	@Override
	protected void initializeConnections() {
		// nothing to do
	}

	@Override
	protected void wakeupConnections() {
		// nothing to do
	}

	@Override
	protected void closeConnections() {
		// nothing to do
	}
}
