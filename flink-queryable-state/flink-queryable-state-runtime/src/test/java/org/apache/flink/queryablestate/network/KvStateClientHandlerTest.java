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

package org.apache.flink.queryablestate.network;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ClientHandler}.
 */
public class KvStateClientHandlerTest {

	/**
	 * Tests that on reads the expected callback methods are called and read
	 * buffers are recycled.
	 */
	@Test
	public void testReadCallbacksAndBufferRecycling() throws Exception {
		final CapturingClientHandlerCallback callback = new CapturingClientHandlerCallback();

		final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
				new MessageSerializer<>(new KvStateInternalRequest.KvStateInternalRequestDeserializer(), new KvStateResponse.KvStateResponseDeserializer());
		final EmbeddedChannel channel = new EmbeddedChannel(new ClientHandler<>("Test Client", serializer, callback));

		final byte[] content = new byte[0];
		final KvStateResponse response = new KvStateResponse(content);

		//
		// Request success
		//
		ByteBuf buf = MessageSerializer.serializeResponse(channel.alloc(), 1222112277L, response);
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		assertThat(callback.onRequestResultCalls.get(0).f0, is(1222112277L));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Request failure
		//
		buf = MessageSerializer.serializeRequestFailure(
				channel.alloc(),
				1222112278,
				new RuntimeException("Expected test Exception"));
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		Tuple2<Long, Throwable> onRequestFailureCall1 = callback.onRequestFailureCalls.get(0);
		assertThat(onRequestFailureCall1.f0, is(1222112278L));
		assertThat(onRequestFailureCall1.f1, instanceOf(RuntimeException.class));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Server failure
		//
		buf = MessageSerializer.serializeServerFailure(
				channel.alloc(),
				new RuntimeException("Expected test Exception"));
		buf.skipBytes(4); // skip frame length

		// Verify callback
		channel.writeInbound(buf);
		assertThat(callback.onFailureCalls.get(0), instanceOf(RuntimeException.class));

		//
		// Unexpected messages
		//
		buf = channel.alloc().buffer(4).writeInt(1223823);

		// Verify callback
		channel.writeInbound(buf);
		assertThat(callback.onFailureCalls.get(1), instanceOf(IllegalStateException.class));
		assertEquals("Buffer not recycled", 0, buf.refCnt());

		//
		// Exception caught
		//
		channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));
		assertThat(callback.onFailureCalls.get(2), instanceOf(RuntimeException.class));

		//
		// Channel inactive
		//
		channel.pipeline().fireChannelInactive();
		assertThat(callback.onFailureCalls.get(3), instanceOf(ClosedChannelException.class));
	}

	private static class CapturingClientHandlerCallback implements ClientHandlerCallback<KvStateResponse> {

		List<Tuple2<Long, KvStateResponse>> onRequestResultCalls = new ArrayList<>();
		List<Tuple2<Long, Throwable>> onRequestFailureCalls = new ArrayList<>();
		List<Throwable> onFailureCalls = new ArrayList<>();

		@Override
		public void onRequestResult(long requestId, KvStateResponse response) {
			onRequestResultCalls.add(Tuple2.of(requestId, response));
		}

		@Override
		public void onRequestFailure(long requestId, Throwable cause) {
			onRequestFailureCalls.add(Tuple2.of(requestId, cause));
		}

		@Override
		public void onFailure(Throwable cause) {
			onFailureCalls.add(cause);
		}
	}

}
