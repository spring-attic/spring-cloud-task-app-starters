/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.timestamp;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.OutputCapture;

/**
 * Verifies that the Task Application outputs the correct task log entries.
 *
 * @author Glenn Renfro
 *
 */
public class TimestampTaskTests {

    @Rule
    public OutputCapture outputCapture = new OutputCapture();

    @Test
    public void testTimeStampApp() throws Exception {
        final String TEST_DATE_DOTS = ".......";
        final String CREATE_TASK_MESSAGE = "Creating: TaskExecution{executionId=";
        final String UPDATE_TASK_MESSAGE = "Updating: TaskExecution with executionId=1 with the following";
        String[] args = { "--format=yyyy" + TEST_DATE_DOTS };

        assertEquals(0, SpringApplication.exit(SpringApplication
                .run(TestTimestampTaskApplication.class, args)));

        String output = this.outputCapture.toString();

        assertTrue("Unable to find the timestamp: " + output,
                output.contains(TEST_DATE_DOTS));
        assertTrue("Test results do not show create task message: " + output,
                output.contains(CREATE_TASK_MESSAGE));
        assertTrue("Test results do not show success message: " + output,
                output.contains(UPDATE_TASK_MESSAGE));
    }



    @SpringBootApplication
    public static class TestTimestampTaskApplication {
        public static void main(String[] args) {
            SpringApplication.run(TestTimestampTaskApplication.class, args);
        }
    }

}