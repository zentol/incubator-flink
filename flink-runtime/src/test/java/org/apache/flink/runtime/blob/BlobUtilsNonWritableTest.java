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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.OperatingSystem;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** Tests for {@link BlobUtils} working on non-writable directories. */
public class BlobUtilsNonWritableTest {

    private static final String CANNOT_CREATE_THIS = "cannot-create-this";

    @TempDir File blobUtilsTestDirectory;

    @BeforeEach
    public void before(@TempDir File tmpDir) throws IOException {
        assumeTrue(!OperatingSystem.isWindows()); // setWritable doesn't work on Windows.

        // Prepare test directory
        assertTrue(blobUtilsTestDirectory.setExecutable(true, false));
        assertTrue(blobUtilsTestDirectory.setReadable(true, false));
        assertTrue(blobUtilsTestDirectory.setWritable(false, false));
    }

    @AfterEach
    public void after() {
        // Cleanup test directory, ensure it was empty
        assertTrue(blobUtilsTestDirectory.delete());
    }

    @Test
    public void testExceptionOnCreateStorageDirectoryFailure() throws IOException {
        Configuration config = new Configuration();
        config.setString(
                BlobServerOptions.STORAGE_DIRECTORY,
                new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS).getAbsolutePath());
        // Should throw an Exception
        assertThatThrownBy(() -> BlobUtils.createBlobStorageDirectory(config, null))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testExceptionOnCreateCacheDirectoryFailureNoJob() throws IOException {
        // Should throw an Exception
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS),
                                        null,
                                        new TransientBlobKey()))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testExceptionOnCreateCacheDirectoryFailureForJobTransient() throws IOException {
        // Should throw an Exception
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS),
                                        new JobID(),
                                        new TransientBlobKey()))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testExceptionOnCreateCacheDirectoryFailureForJobPermanent() throws IOException {
        // Should throw an Exception
        assertThatThrownBy(
                        () ->
                                BlobUtils.getStorageLocation(
                                        new File(blobUtilsTestDirectory, CANNOT_CREATE_THIS),
                                        new JobID(),
                                        new PermanentBlobKey()))
                .isInstanceOf(IOException.class);
    }
}
