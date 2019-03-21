/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.spark.cluster;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.task.sparkapp.common.SparkAppCommonTaskProperties;

/**
 * Configuration properties to be used for cluster submission. These are in addition to the ones
 * defined in the common one.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class SparkClusterTaskProperties extends SparkAppCommonTaskProperties {

    /**
     * The URL for the Spark REST API to be used (spark://host:port).
     */
    private String restUrl = "spark://localhost:6066";

    /**
     * The interval (ms) to use for polling for the App status.
     */
    private long appStatusPollInterval = 10000L;

    public String getRestUrl() {
        return restUrl;
    }

    public void setRestUrl(String restUrl) {
        this.restUrl = restUrl;
    }

    public long getAppStatusPollInterval() {
        return appStatusPollInterval;
    }

    public void setAppStatusPollInterval(long appStatusPollInterval) {
        this.appStatusPollInterval = appStatusPollInterval;
    }
}