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

package org.springframework.cloud.task.app.sqoop.job;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Thomas Risberg
 */
public class SqoopToolJobPropertiesTests {

	@Test
	public void testActionCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "action: create");
		EnvironmentTestUtils.addEnvironment(context, "job-name: myjob");
		EnvironmentTestUtils.addEnvironment(context, "metastore-url: jdbc:hsqldb:hsql://localhost:16000/sqoop");
		context.register(Conf.class);
		context.refresh();
		SqoopJobTaskProperties properties = context.getBean(SqoopJobTaskProperties.class);
		assertThat(properties.getAction(), equalTo("create"));
	}

	@Test
	public void testJobNameCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "action: exec");
		EnvironmentTestUtils.addEnvironment(context, "job-name: anotherjob");
		EnvironmentTestUtils.addEnvironment(context, "metastore-url: jdbc:hsqldb:hsql://localhost:16000/sqoop");
		context.register(Conf.class);
		context.refresh();
		SqoopJobTaskProperties properties = context.getBean(SqoopJobTaskProperties.class);
		assertThat(properties.getJobName(), equalTo("anotherjob"));
	}

	@Test
	public void testMetastoreUrlCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "action: exec");
		EnvironmentTestUtils.addEnvironment(context, "job-name: myjob");
		EnvironmentTestUtils.addEnvironment(context, "metastore-url: jdbc:hsqldb:hsql://localhost:12345/sqoop");
		context.register(Conf.class);
		context.refresh();
		SqoopJobTaskProperties properties = context.getBean(SqoopJobTaskProperties.class);
		assertThat(properties.getMetastoreUrl(), equalTo("jdbc:hsqldb:hsql://localhost:12345/sqoop"));
	}

	@Test
	public void testMetastoreUsernameCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "action: exec");
		EnvironmentTestUtils.addEnvironment(context, "job-name: myjob");
		EnvironmentTestUtils.addEnvironment(context, "metastore-url: jdbc:hsqldb:hsql://localhost:16000/sqoop");
		EnvironmentTestUtils.addEnvironment(context, "metastore-username: spring");
		context.register(Conf.class);
		context.refresh();
		SqoopJobTaskProperties properties = context.getBean(SqoopJobTaskProperties.class);
		assertThat(properties.getMetastoreUsername(), equalTo("spring"));
	}

	@Test
	public void testMetastorePasswordCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "action: exec");
		EnvironmentTestUtils.addEnvironment(context, "job-name: myjob");
		EnvironmentTestUtils.addEnvironment(context, "metastore-url: jdbc:hsqldb:hsql://localhost:16000/sqoop");
		EnvironmentTestUtils.addEnvironment(context, "metastore-password: secret");
		context.register(Conf.class);
		context.refresh();
		SqoopJobTaskProperties properties = context.getBean(SqoopJobTaskProperties.class);
		assertThat(properties.getMetastorePassword(), equalTo("secret"));
	}

	@Configuration
	@EnableConfigurationProperties(SqoopJobTaskProperties.class)
	static class Conf {
	}
}
