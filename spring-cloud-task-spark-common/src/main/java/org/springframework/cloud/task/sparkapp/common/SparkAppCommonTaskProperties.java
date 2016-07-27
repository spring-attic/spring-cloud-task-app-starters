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

package org.springframework.cloud.task.sparkapp.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotNull;

/**
 * Common configuration properties to be used for all Spark App submissions.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class SparkAppCommonTaskProperties {

    /**
     * The name of the Spark application.
     */
    @Value("${spring.application.name:sparkapp-task}")
    private String appName;

    /**
     * The main class for the Spark application.
     */
    private String appClass;

    /**
     * The path to a bundled jar that includes your application and its dependencies, excluding any Spark dependencies.
     */
    private String appJar;

    /**
     * The path to a bundled jar that includes your application and its dependencies, excluding any Spark dependencies.
     */
    private String[] appArgs = new String[]{};

    /**
     * A comma separated list of files to be included with the app submission.
     */
    private String resourceFiles;

    /**
     * The A comma separated list of archive files to be included with the app submission.
     */
    private String resourceArchives;

    /**
     * The memory setting to be used for each executor.
     */
    private String executorMemory = "1024M";

    /**
     * The arguments for the Spark app.
     */
    private String[] sparkAppArgs = new String[]{};

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    @NotNull
    public String getAppClass() {
        return appClass;
    }

    public void setAppClass(String appClass) {
        this.appClass = appClass;
    }

    @NotNull
    public String getAppJar() {
        return appJar;
    }

    public void setAppJar(String appJar) {
        this.appJar = appJar;
    }

    public String[] getAppArgs() {
        return appArgs;
    }

    public void setAppArgs(String[] appArgs) {
        this.appArgs = appArgs;
    }

    public String getResourceFiles() {
        return resourceFiles;
    }

    public void setResourceFiles(String resourceFiles) {
        this.resourceFiles = resourceFiles;
    }

    public String getResourceArchives() {
        return resourceArchives;
    }

    public void setResourceArchives(String resourceArchives) {
        this.resourceArchives = resourceArchives;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String[] getSparkAppArgs() {
        return sparkAppArgs;
    }

    public void setSparkAppArgs(String[] sparkAppArgs) {
        this.sparkAppArgs = sparkAppArgs;
    }
}