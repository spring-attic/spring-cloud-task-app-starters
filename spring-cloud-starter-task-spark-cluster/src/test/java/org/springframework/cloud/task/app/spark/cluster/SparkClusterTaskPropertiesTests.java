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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * @author Thomas Risberg
 */
public class SparkClusterTaskPropertiesTests {

    @Test
    public void testMasterCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.master: spark://borneo:7077");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getMaster(), equalTo("spark://borneo:7077"));
    }

    @Test
    public void testRestUrlCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.rest-url: spark://foo:6066");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getRestUrl(), equalTo("spark://foo:6066"));
    }

    @Test
    public void testAppStatusPollIntervalCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.rest-url: spark://dummy:6066");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-status-poll-interval: 20000");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppStatusPollInterval(), equalTo(20000L));
    }

    @Test
    public void testAppNameCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-name: test");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppName(), equalTo("test"));
    }

    @Test
    public void testAppClassCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: MyTestClass");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppClass(), equalTo("MyTestClass"));
    }

    @Test(expected = BeanCreationException.class)
    public void testAppClassIsRequired() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        context.register(Conf.class);
        context.refresh();
    }

    @Test
    public void testAppJarCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: my-app-jar-0.0.1.jar");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppJar(), equalTo("my-app-jar-0.0.1.jar"));
    }

    @Test(expected = BeanCreationException.class)
    public void testAppJarIsRequired() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        context.register(Conf.class);
        context.refresh();
    }

    @Test
    public void testAppArgsCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-args: arg1,arg2");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getAppArgs(), equalTo(new String[]{"arg1", "arg2"}));
    }

    @Test
    public void testResourceFilesCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.resource-files: test.txt");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getResourceFiles(), equalTo("test.txt"));
    }

    @Test
    public void testResourceArchivesCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.resource-archives: foo.jar,bar.jar");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getResourceArchives(), equalTo("foo.jar,bar.jar"));
    }

    @Test
    public void testExecutorMemoryCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.executor-memory: 2048M");
        context.register(Conf.class);
        context.refresh();
        SparkClusterTaskProperties properties = context.getBean(SparkClusterTaskProperties.class);
        assertThat(properties.getExecutorMemory(), equalTo("2048M"));
    }

    @Configuration
    @EnableConfigurationProperties(SparkClusterTaskProperties.class)
    static class Conf {
    }
}