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
package org.apache.flink.client.program;

import org.apache.flink.configuration.Configuration;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link OptimizerPlanEnvironment}. */
public class OptimizerPlanEnvironmentTest {

    /**
     * Test the two modes for handling stdout/stderr of user program. (1) Capturing the output and
     * including it only in the exception (2) Leaving the output untouched
     */
    @Test
    public void testStdOutStdErrHandling() throws Exception {
        runOutputTest(true, new String[] {"System.out: hello out!", "System.err: hello err!"});
        runOutputTest(false, new String[] {"System.out: (none)", "System.err: (none)"});
    }

    private void runOutputTest(boolean suppressOutput, String[] expectedCapturedOutput)
            throws ProgramInvocationException {
        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder().setEntryPointClassName(getClass().getName()).build();

        try {
            // Flink will throw an error because no job graph will be generated by the main method.
            PackagedProgramUtils.getPipelineFromProgram(
                    packagedProgram, new Configuration(), 1, suppressOutput);
            Assert.fail("This should have failed to create the Flink Plan.");
        } catch (ProgramInvocationException e) {
            // Test that that Flink captured the expected stdout/stderr
            for (String expected : expectedCapturedOutput) {
                assertThat(e.getMessage(), containsString(expected));
            }
        }
    }

    /**
     * Main method for {@code testEnsureStdoutStdErrIsRestored()}. This will not create a valid
     * Flink program. We will just use this program to check whether stdout/stderr is captured in a
     * byte buffer or directly printed to the console.
     */
    public static void main(String[] args) {
        // Print something to stdout/stderr for output suppression test
        System.out.println("hello out!");
        System.err.println("hello err!");
    }
}
