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
        EnvironmentTestUtils.addEnvironment(context, "app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context,
                "spark-assembly-jar: hdfs:///app/spark/spark-assembly-1.5.2-hadoop2.6.0.jar");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getSparkAssemblyJar(), equalTo("hdfs:///app/spark/spark-assembly-1.5.2-hadoop2.6.0.jar"));
    }

    @Test(expected = BeanCreationException.class)
    public void testSparkAssemblyJarIsRequired() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        context.register(Conf.class);
        context.refresh();
    }

    @Test
    public void testNumExecutorsCanBeCustomized() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        EnvironmentTestUtils.addEnvironment(context, "app-class: Dummy");
        EnvironmentTestUtils.addEnvironment(context, "app-jar: dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "spark-assembly-jar: hdfs:///app/spark/dummy.jar");
        EnvironmentTestUtils.addEnvironment(context, "num-executors: 4");
        context.register(Conf.class);
        context.refresh();
        SparkYarnTaskProperties properties = context.getBean(SparkYarnTaskProperties.class);
        assertThat(properties.getNumExecutors(), equalTo(4));
    }

    @Configuration
    @EnableConfigurationProperties(SparkYarnTaskProperties.class)
    static class Conf {
    }
}