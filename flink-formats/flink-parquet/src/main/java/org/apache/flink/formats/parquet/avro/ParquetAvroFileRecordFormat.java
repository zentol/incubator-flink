/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import javax.annotation.Nullable;

import java.io.IOException;

/** TODO: Add javadoc. */
public class ParquetAvroFileRecordFormat implements FileRecordFormat<GenericRecord> {
    private final transient Schema schema;

    public ParquetAvroFileRecordFormat(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Reader<GenericRecord> createReader(
            Configuration config, Path filePath, long splitOffset, long splitLength)
            throws IOException {

        final FileSystem fs = filePath.getFileSystem();
        final FileStatus status = fs.getFileStatus(filePath);
        final FSDataInputStream in = fs.open(filePath);

        return new MyReader(
                AvroParquetReader.<GenericRecord>builder(new InputFileWrapper(in, status.getLen()))
                        .withDataModel(GenericData.get())
                        .build());
    }

    @Override
    public Reader<GenericRecord> restoreReader(
            Configuration config,
            Path filePath,
            long restoredOffset,
            long splitOffset,
            long splitLength) {
        // not called if checkpointing isn't used
        return null;
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return new GenericRecordAvroTypeInfo(schema);
    }

    private static class MyReader implements FileRecordFormat.Reader<GenericRecord> {

        private final ParquetReader<GenericRecord> parquetReader;

        private MyReader(ParquetReader<GenericRecord> parquetReader) {
            this.parquetReader = parquetReader;
        }

        @Nullable
        @Override
        public GenericRecord read() throws IOException {
            return parquetReader.read();
        }

        @Override
        public void close() throws IOException {
            parquetReader.close();
        }
    }

    private static class InputFileWrapper implements InputFile {

        private final FSDataInputStream inputStream;
        private final long length;

        private InputFileWrapper(FSDataInputStream inputStream, long length) {
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new SeekableInputStreamAdapter(inputStream);
        }
    }

    private static class SeekableInputStreamAdapter extends DelegatingSeekableInputStream {

        private final FSDataInputStream inputStream;

        private SeekableInputStreamAdapter(FSDataInputStream inputStream) {
            super(inputStream);
            this.inputStream = inputStream;
        }

        @Override
        public long getPos() throws IOException {
            return inputStream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            inputStream.seek(newPos);
        }
    }
}
