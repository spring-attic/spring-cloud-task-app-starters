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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * @author Thomas Risberg
 */
public class SparkClusterTaskPropertiesTests {

    @Test
    public void testRestUrlCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "rest-url: spark://foo:6066");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getRestUrl(), equalTo("spark://foo:6066"));
    }

    @Test
    public void testAppStatusPollIntervalCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "rest-url: spark://dummy:6066");
        EnvironmentTestUtils.addEnvironment(context, "app-status-poll-interval: 20000");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppStatusPollInterval(), equalTo(20000L));
    }

    @Configuration
    @EnableConfigurationProperties(SparkClusterTaskProperties.class)
    static class Conf {
    }
}