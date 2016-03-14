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

package org.apache.flink.runtime.webmonitor.handlers;

/*****************************************************************************
 * This code is based on the "HttpStaticFileServerHandler" from the
 * Netty project's HTTP server example.
 *
 * See http://netty.io and
 * https://github.com/netty/netty/blob/4.0/example/src/main/java/io/netty/example/http/file/HttpStaticFileServerHandler.java
 *****************************************************************************/

import akka.dispatch.OnComplete;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.router.KeepAliveWrite;
import io.netty.handler.codec.http.router.Routed;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.webmonitor.JobManagerRetriever;
import org.apache.flink.runtime.webmonitor.files.MimeTypes;
import org.apache.flink.runtime.webmonitor.files.StaticFileServerHandler;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.Await;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Request handler that returns the TaskManager log/out files.
 *
 * <p>This code is based on the "HttpStaticFileServerHandler" from the Netty project's HTTP server
 * example.</p>
 */
@ChannelHandler.Sharable
public class TaskManagerLogHandler extends SimpleChannelInboundHandler<Routed> {

	/** Default logger, if none is specified */
	private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(StaticFileServerHandler.class);

	// ------------------------------------------------------------------------

	/** JobManager retriever */
	private final JobManagerRetriever retriever;

	private final Future<String> localJobManagerAddressFuture;

	private final FiniteDuration timeout;

	/** Keep track of last transmitted log, to clean up old ones */
	private final HashMap<String, BlobKey> lastSubmittedLog = new HashMap<>();
	private final HashMap<String, BlobKey> lastSubmittedStdout = new HashMap<>();

	/** Keep track of request status, prevents multiple log requests for a single TM running concurrently */
	private final HashMap<String, Boolean> lastRequestCompleted = new HashMap<>();

	/** The log for all error reporting */
	private final Logger logger;

	/** indicates which log file should be displayed; true indicates .log, false indicates .out */
	private final boolean serveLogFile;
	private final ExecutionContextExecutor executor;

	private String localJobManagerAddress;

	public TaskManagerLogHandler(
		JobManagerRetriever retriever,
		ExecutionContextExecutor executor,
		Future<String> localJobManagerAddressPromise,
		FiniteDuration timeout,
		boolean serveLogFile) throws IOException {

		this(retriever, executor, localJobManagerAddressPromise, timeout, DEFAULT_LOGGER, serveLogFile);
	}

	public TaskManagerLogHandler(
		JobManagerRetriever retriever,
		ExecutionContextExecutor executor,
		Future<String> localJobManagerAddressFuture,
		FiniteDuration timeout,
		Logger logger, boolean serveLogFile) throws IOException {

		this.retriever = checkNotNull(retriever);
		this.executor = checkNotNull(executor);
		this.localJobManagerAddressFuture = checkNotNull(localJobManagerAddressFuture);
		this.timeout = checkNotNull(timeout);
		this.logger = checkNotNull(logger);
		this.serveLogFile = serveLogFile;
	}

	// ------------------------------------------------------------------------
	//  Responses to requests
	// ------------------------------------------------------------------------

	@Override
	public void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		if (localJobManagerAddressFuture.isCompleted()) {
			if (localJobManagerAddress == null) {
				localJobManagerAddress = Await.result(localJobManagerAddressFuture, timeout);
			}

			final HttpRequest request = routed.request();

			Option<Tuple2<ActorGateway, Integer>> jobManager = retriever.getJobManagerGatewayAndWebPort();

			if (jobManager.isDefined()) {
				// Redirect to leader if necessary
				String redirectAddress = HandlerRedirectUtils.getRedirectAddress(
					localJobManagerAddress, jobManager.get());

				if (redirectAddress != null) {
					HttpResponse redirect = HandlerRedirectUtils.getRedirectResponse(redirectAddress, "");
					KeepAliveWrite.flush(ctx, routed.request(), redirect);
				} else {
					respondAsLeader(ctx, request, routed.pathParams(), jobManager.get()._1());
				}
			} else {
				KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
			}
		} else {
			KeepAliveWrite.flush(ctx, routed.request(), HandlerRedirectUtils.getUnavailableResponse());
		}
	}

	/**
	 * Response when running with leading JobManager.
	 */
	private void respondAsLeader(final ChannelHandlerContext ctx, final HttpRequest request, final Map<String, String> pathParams, final ActorGateway jobManager) throws Exception {
		final String taskManagerID = pathParams.get(TaskManagersHandler.TASK_MANAGER_ID_KEY);

		boolean fetch;
		synchronized (lastRequestCompleted) {
			if (!lastRequestCompleted.containsKey(taskManagerID)) {
				lastRequestCompleted.put(taskManagerID, true);
			}
			fetch = lastRequestCompleted.get(taskManagerID);
			lastRequestCompleted.put(taskManagerID, false);
		}

		//fetch taskmanager logs if no other process is currently doing it
		if (fetch) {
			//get taskmanager gateway
			InstanceID instanceID = new InstanceID(StringUtils.hexStringToByte(taskManagerID));
			Future<Object> future = jobManager.ask(new JobManagerMessages.RequestTaskManagerInstance(instanceID), timeout);
			JobManagerMessages.TaskManagerInstance instance = (JobManagerMessages.TaskManagerInstance) Await.result(future, timeout);
			Instance taskManager = instance.instance().get();

			Future<Object> isBlobServiceDefined = taskManager.getActorGateway().ask(TaskManagerMessages.getIsBlobServiceDefined(), timeout);

			if (!(Boolean) Await.result(isBlobServiceDefined, timeout)) {
				display(ctx, request, "BlobService unavailable, cannot upload TaskManager logs.");
				return;
			}

			//send log request to taskmanager
			Future<Object> blobKeyFuture = taskManager.getActorGateway().ask(serveLogFile ? TaskManagerMessages.getRequestTaskManagerLog() : TaskManagerMessages.getRequestTaskManagerStdout(), timeout);
			blobKeyFuture.onComplete(new OnComplete<Object>() {
				@Override
				public void onComplete(Throwable failure, Object success) throws Throwable {
					try {
						BlobKey blobKey = (BlobKey) success;

						//delete previous log file, if it is different than the current one
						if ((serveLogFile ? lastSubmittedLog : lastSubmittedStdout).containsKey(taskManagerID)) {
							if (!blobKey.equals((serveLogFile ? lastSubmittedLog : lastSubmittedStdout).get(taskManagerID))) {
								jobManager.tell(JobManagerMessages.getDeleteTaskManagerLog((serveLogFile ? lastSubmittedLog : lastSubmittedStdout).remove(taskManagerID)));
								(serveLogFile ? lastSubmittedLog : lastSubmittedStdout).put(taskManagerID, blobKey);
							}
						} else {
							(serveLogFile ? lastSubmittedLog : lastSubmittedStdout).put(taskManagerID, blobKey);
						}

						//send blobkey to jobmanager
						Future<Object> logPathFuture = jobManager.ask(JobManagerMessages.getRequestTaskManagerLog(blobKey), timeout);
						String filePath = (String) Await.result(logPathFuture, timeout);

						File file = new File(filePath);
						final RandomAccessFile raf;
						try {
							raf = new RandomAccessFile(file, "r");
						} catch (FileNotFoundException e) {
							sendError(ctx, NOT_FOUND);
							return;
						}
						long fileLength = raf.length();

						HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
						setContentTypeHeader(response, file);

						if (HttpHeaders.isKeepAlive(request)) {
							response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
						}
						HttpHeaders.setContentLength(response, fileLength);

						// write the initial line and the header.
						ctx.write(response);

						// write the content.
						ctx.write(new DefaultFileRegion(raf.getChannel(), 0, fileLength), ctx.newProgressivePromise())
							.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<? super Void>>() {
								@Override
								public void operationComplete(io.netty.util.concurrent.Future<? super Void> future) throws Exception {
									synchronized (lastRequestCompleted) {
										lastRequestCompleted.put(taskManagerID, true);
									}
								}
							});
						ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

						// close the connection, if no keep-alive is needed
						if (!HttpHeaders.isKeepAlive(request)) {
							lastContentFuture.addListener(ChannelFutureListener.CLOSE);
						}
					} catch (Exception e) {
						logger.error("Serving of taskmanager log failed: " + e.getMessage());
					}
				}
			}, executor);
		} else {
			display(ctx, request, "loading...");
			return;
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (ctx.channel().isActive()) {
			logger.error("Caught exception", cause);
			sendError(ctx, INTERNAL_SERVER_ERROR);
		}
	}

	private void display(ChannelHandlerContext ctx, HttpRequest request, String message) {
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		response.headers().set(CONTENT_TYPE, "text/plain");

		if (HttpHeaders.isKeepAlive(request)) {
			response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
		}

		byte[] buf = message.getBytes();

		ByteBuf b = Unpooled.copiedBuffer(buf);

		HttpHeaders.setContentLength(response, buf.length);

		// write the initial line and the header.
		ctx.write(response);

		ctx.write(b);

		ChannelFuture lastContentFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);

		// close the connection, if no keep-alive is needed
		if (!HttpHeaders.isKeepAlive(request)) {
			lastContentFuture.addListener(ChannelFutureListener.CLOSE);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities to encode headers and responses
	// ------------------------------------------------------------------------

	/**
	 * Writes a simple  error response message.
	 *
	 * @param ctx    The channel context to write the response to.
	 * @param status The response status.
	 */
	private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
		FullHttpResponse response = new DefaultFullHttpResponse(
			HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
		response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");

		// close the connection as soon as the error message is sent.
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * Sets the content type header for the HTTP Response.
	 *
	 * @param response HTTP response
	 * @param file     file to extract content type
	 */
	private static void setContentTypeHeader(HttpResponse response, File file) {
		String mimeType = MimeTypes.getMimeTypeForFileName(file.getName());
		String mimeFinal = mimeType != null ? mimeType : MimeTypes.getDefaultMimeType();
		response.headers().set(CONTENT_TYPE, mimeFinal);
	}
}
