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

package org.springframework.cloud.task.app.spark.yarn;

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
public class SparkYarnTaskPropertiesTests {

    @Test
    public void testSparkAssemblyJarCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context,
                "spark.assembly-jar: hdfs:///app/spark/spark-assembly-1.6.2-hadoop2.6.0.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getAssemblyJar(), equalTo("hdfs:///app/spark/spark-assembly-1.6.2-hadoop2.6.0.jar"));
    }

    @Test(expected = BeanCreationException.class)
    public void testSparkAssemblyJarIsRequired() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        context.register(Conf.class);
        context.refresh();
    }

    @Test
    public void testNumExecutorsCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.num-executors: 4");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getNumExecutors(), equalTo(4));
    }

    @Test
    public void testAppNameCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-name: test");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getAppName(), equalTo("test"));
    }

    @Test
    public void testAppClassCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: MyTestClass");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
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
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getAppJar(), equalTo("my-app-jar-0.0.1.jar"));
    }

    @Test(expected = BeanCreationException.class)
    public void testAppJarIsRequired() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        context.register(Conf.class);
        context.refresh();
    }

    @Test
    public void testAppArgsCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-args: arg1,arg2");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getAppArgs(), equalTo(new String[]{"arg1", "arg2"}));
    }

    @Test
    public void testResourceFilesCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.resource-files: test.txt");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getResourceFiles(), equalTo("test.txt"));
    }

    @Test
    public void testResourceArchivesCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.resource-archives: foo.jar,bar.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getResourceArchives(), equalTo("foo.jar,bar.jar"));
    }

    @Test
    public void testExecutorMemoryCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "spark.app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "spark.app-jar: dummy.jar");
	    EnvironmentTestUtils.addEnvironment(context, "spark.assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark.executor-memory: 2048M");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getExecutorMemory(), equalTo("2048M"));
    }

    @Configuration
    @EnableConfigurationProperties(SparkYarnTaskProperties.class)
    static class Conf {
    }
}