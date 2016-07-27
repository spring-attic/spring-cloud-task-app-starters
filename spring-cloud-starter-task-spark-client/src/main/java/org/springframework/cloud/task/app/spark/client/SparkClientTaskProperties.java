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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties to be used for cluster submission. These are in addition to the ones
 * defined in the common one.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties("sparkclient")
public class SparkClientTaskProperties {

    /**
     * The master setting to be used (local, local[N] or local[*]).
     */
    private String master = "local";

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }
}