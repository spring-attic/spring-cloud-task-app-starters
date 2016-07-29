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

package org.springframework.cloud.task.app.spark.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.deploy.master.DriverState;
import org.apache.spark.deploy.rest.CreateSubmissionRequest;
import org.apache.spark.deploy.rest.RestSubmissionClient;
import org.apache.spark.deploy.rest.SubmitRestProtocolResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import scala.Predef$;
import scala.collection.JavaConverters$;

/**
 * {@link CommandLineRunner} implementation that will run a Spark App in cluster mode using
 * configuration properties provided.
 *
 * @author Thomas Risberg
 */
@EnableTask
@Configuration
@EnableConfigurationProperties(SparkClusterTaskProperties.class)
public class SparkClusterTaskConfiguration {

    @Bean
    public CommandLineRunner commandLineRunner() {
        return new SparkAppClusterRunner();
    }

    private class SparkAppClusterRunner implements CommandLineRunner {

        private final Log logger = LogFactory.getLog(SparkAppClusterRunner.class);

        @Autowired
        private SparkClusterTaskProperties config;

        @Override
        public void run(String... args) throws Exception {

            RestSubmissionClient rsc = new RestSubmissionClient(config.getRestUrl());

            Map<String, String> sparkProps = new HashMap<>();
            sparkProps.put("spark.app.name", config.getAppName());
            sparkProps.put("spark.master", config.getMaster());
            if (StringUtils.hasText(config.getExecutorMemory())) {
                sparkProps.put("spark.executor.memory", config.getExecutorMemory());
            }
            if (StringUtils.hasText(config.getResourceArchives())) {
                sparkProps.put("spark.jars", config.getAppJar().trim() + "," + config.getResourceArchives().trim());
            } else {
                sparkProps.put("spark.jars", config.getAppJar());
            }
            if (StringUtils.hasText(config.getResourceFiles())) {
                sparkProps.put("spark.files", config.getResourceFiles());
            }
            scala.collection.immutable.Map<String, String> envMap =
                    JavaConverters$.MODULE$.mapAsScalaMapConverter(new HashMap<String, String>()).asScala()
                            .toMap(Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());
            scala.collection.immutable.Map<String, String> propsMap =
                    JavaConverters$.MODULE$.mapAsScalaMapConverter(sparkProps).asScala()
                            .toMap(Predef$.MODULE$.<scala.Tuple2<String, String>>conforms());

            CreateSubmissionRequest csr = rsc.constructSubmitRequest(
                    config.getAppJar(),
                    config.getAppClass(),
                    config.getAppArgs(),
                    propsMap,
                    envMap);

            SubmitRestProtocolResponse resp = rsc.createSubmission(csr);

            String submissionId = getJsonProperty(resp.toJson(), "submissionId");
            logger.info("Submitted Spark App with submissionId: " + submissionId);

            String appState;

            while (true) {
                Thread.sleep(config.getAppStatusPollInterval());
                SubmitRestProtocolResponse stat = rsc.requestSubmissionStatus(submissionId, false);
                appState = getJsonProperty(stat.toJson(), "driverState");
                if (!(appState.equals(DriverState.SUBMITTED().toString()) ||
                        appState.equals(DriverState.RUNNING().toString()) ||
                        appState.equals(DriverState.RELAUNCHING().toString()) ||
                        appState.equals(DriverState.UNKNOWN().toString()))) {
                    System.out.println("Spark App completed with status: " + appState);
                    appState = appState;
                    break;
                }
            }
            if (!appState.equals(DriverState.FINISHED().toString())) {
                throw new RuntimeException("Spark App submission " + submissionId + " failed with status " + appState);
            }
        }

        private String getJsonProperty(String json, String prop) {
            try {
                HashMap<String, Object> props =
                        new ObjectMapper().readValue(json, HashMap.class);
                return props.get(prop).toString();
            } catch (IOException ioe) {
                return null;
            }
        }
    }
}