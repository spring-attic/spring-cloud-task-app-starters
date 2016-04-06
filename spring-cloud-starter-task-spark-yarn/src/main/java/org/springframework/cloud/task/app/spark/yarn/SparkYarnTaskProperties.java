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

package org.springframework.cloud.task.app.spark.yarn;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.task.sparkapp.common.SparkAppCommonTaskProperties;

/**
 * Configuration properties to be used for YARN submission. These are in addition to the ones
 * defined in the common one.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class SparkYarnTaskProperties extends SparkAppCommonTaskProperties {

    /**
     * The path for the Spark Assembly jar to use.
     */
    private String sparkAssemblyJar;

    /**
     * The number of executors to use.
     */
    private int numExecutors = 1;

    @NotNull
    public String getSparkAssemblyJar() {
        return sparkAssemblyJar;
    }

    public void setSparkAssemblyJar(String sparkAssemblyJar) {
        this.sparkAssemblyJar = sparkAssemblyJar;
    }

    public int getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
    }
}