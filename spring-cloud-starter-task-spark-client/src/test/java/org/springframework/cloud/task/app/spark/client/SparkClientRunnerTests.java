/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.cloud.task.app.spark.client;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Thomas Risberg
 * @author Soby Chacko
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=SparkClientRunnerTests.TestSparkClientTaskApplication.class)
@IntegrationTest({"spark.app-name:pi",
        "spark.app-jar:dummy.jar",
        "spark.app-class:org.apache.spark.examples.JavaSparkPi",
        "spark.app-args:10"})
@Ignore
public class SparkClientRunnerTests {

    @Test
    public void testExampleSparkApp() throws Exception {
    }

    @SpringBootApplication
    public static class TestSparkClientTaskApplication {
        public static void main(String[] args) {
            SpringApplication.run(TestSparkClientTaskApplication.class, args);
        }
    }
}
