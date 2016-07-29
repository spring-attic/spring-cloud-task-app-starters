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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.deploy.SparkSubmit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

/**
 * {@link CommandLineRunner} implementation that will run a Spark App in client mode using
 * configuration properties provided.
 *
 * @author Thomas Risberg
 */
@EnableTask
@Configuration
@EnableConfigurationProperties(SparkClientTaskProperties.class)
public class SparkClientTaskConfiguration {

    @Bean
    public CommandLineRunner commandLineRunner() {
        return new SparkAppClientRunner();
    }

    private class SparkAppClientRunner implements CommandLineRunner {

        private final Log logger = LogFactory.getLog(SparkAppClientRunner.class);

        @Autowired
        private SparkClientTaskProperties config;


        @Override
        public void run(String... args) throws Exception {
            ArrayList<String> argList = new ArrayList<>();
            if (StringUtils.hasText(config.getAppName())) {
                argList.add("--name");
                argList.add(config.getAppName());
            }
            argList.add("--class");
            argList.add(config.getAppClass());
            argList.add("--master");
            argList.add(config.getMaster());
            argList.add("--deploy-mode");
            argList.add("client");

            argList.add(config.getAppJar());

            if (StringUtils.hasText(config.getResourceFiles())) {
                argList.add("--files");
                argList.add(config.getResourceFiles());
            }

            if (StringUtils.hasText(config.getResourceArchives())) {
                argList.add("--jars");
                argList.add(config.getResourceArchives());
            }

            argList.addAll(Arrays.asList(config.getAppArgs()));

            try {
                SparkSubmit.main(argList.toArray(new String[argList.size()]));
            } catch (Throwable t) {
                logger.error("Spark Application failed: " + t.getMessage(), t);
                throw new RuntimeException("Spark Application failed", t);
            }
        }
    }
}