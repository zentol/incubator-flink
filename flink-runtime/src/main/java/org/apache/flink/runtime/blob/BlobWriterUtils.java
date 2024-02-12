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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.BiFunctionWithException;

import java.io.IOException;
import java.util.Optional;

/** BlobWriter is used to upload data to the BLOB store. */
public class BlobWriterUtils {

    /**
     * Serializes the given value and offloads it to the BlobServer as a transient blob if its size exceeds the minimum
     * offloading size of the BlobServer.
     *
     * @param value to serialize
     * @param blobWriter to use to offload the serialized value
     * @param <T> type of the value to serialize
     *
     * @return Either the serialized value or the stored blob key
     *
     * @throws IOException if the data cannot be serialized
     */
    public static <T> Either<SerializedValue<T>, TransientBlobKey> serializeAndTryTransientOffload(
            T value, BlobWriter blobWriter) throws IOException {
        Preconditions.checkNotNull(value);

        final SerializedValue<T> serializedValue = new SerializedValue<>(value);

        return internalTryOffload(
                serializedValue,
                blobWriter,
                (blobWriter1, bytes) -> blobWriter.putTransient(bytes));
    }

    /**
     * Serializes the given value and offloads it to the BlobServer if its size exceeds the minimum
     * offloading size of the BlobServer.
     *
     * @param value to serialize
     * @param jobId to which the value belongs.
     * @param blobWriter to use to offload the serialized value
     * @param <T> type of the value to serialize
     *
     * @return Either the serialized value or the stored blob key
     *
     * @throws IOException if the data cannot be serialized
     */
    public static <T> Either<SerializedValue<T>, PermanentBlobKey> serializeAndTryOffload(
            T value, JobID jobId, BlobWriter blobWriter) throws IOException {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(jobId);

        final SerializedValue<T> serializedValue = new SerializedValue<>(value);

        return internalTryOffload(
                serializedValue,
                blobWriter,
                (writer, bytes) -> blobWriter.putPermanent(jobId, bytes));
    }

    private static <T, K> Either<SerializedValue<T>, K> internalTryOffload(
            SerializedValue<T> serializedValue,
            BlobWriter blobWriter,
            BiFunctionWithException<BlobWriter, byte[], K, IOException> fn) {

        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(blobWriter);

        if (serializedValue.getByteArray().length < blobWriter.getMinOffloadingSize()) {
            return Either.Left(serializedValue);
        } else {
            return internalOffloadWithException(serializedValue, blobWriter, fn)
                    .map(Either::<SerializedValue<T>, K>Right)
                    .orElse(Either.Left(serializedValue));
        }
    }

    /**
     * Offloads the given value to the BlobServer.
     *
     * @param serializedValue value to offload
     * @param jobId to which the value belongs.
     * @param blobWriter to use to offload the serialized value
     * @param <T> actual value type
     *
     * @return stored blob key
     */
    public static <T> Optional<PermanentBlobKey> offloadWithException(
            SerializedValue<T> serializedValue, JobID jobId, BlobWriter blobWriter) {
        Preconditions.checkNotNull(jobId);
        return internalOffloadWithException(
                serializedValue,
                blobWriter,
                (writer, bytes) -> blobWriter.putPermanent(jobId, bytes));
    }

    private static <T, K> Optional<K> internalOffloadWithException(
            SerializedValue<T> serializedValue,
            BlobWriter blobWriter,
            BiFunctionWithException<BlobWriter, byte[], K, IOException> fn) {
        Preconditions.checkNotNull(serializedValue);
        Preconditions.checkNotNull(blobWriter);
        try {
            final K blobKey = fn.apply(blobWriter, serializedValue.getByteArray());
            return Optional.of(blobKey);
        } catch (IOException e) {
            BlobWriter.LOG.warn("Failed to offload value to BLOB store.", e);
            return Optional.empty();
        }
    }

    private BlobWriterUtils() {
    }
}
