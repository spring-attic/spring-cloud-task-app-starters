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

package org.springframework.cloud.task.sqoop.common;

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
public class SqoopCommonTaskPropertiesTests {

	@Test
	public void testCommandCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "command: export");
		context.register(Conf.class);
		context.refresh();
		SqoopCommonTaskProperties properties = context.getBean(SqoopCommonTaskProperties.class);
		assertThat(properties.getCommand(), equalTo("export"));
	}

	@Test
	public void testConnectCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "connect: jdbc:foo://bar:1234");
		context.register(Conf.class);
		context.refresh();
		SqoopCommonTaskProperties properties = context.getBean(SqoopCommonTaskProperties.class);
		assertThat(properties.getConnect(), equalTo("jdbc:foo://bar:1234"));
	}

	@Test
	public void testToolArgsCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "tool-args: --table foo");
		context.register(Conf.class);
		context.refresh();
		SqoopCommonTaskProperties properties = context.getBean(SqoopCommonTaskProperties.class);
		assertThat(properties.getToolArgs(), equalTo("--table foo"));
	}

	@Configuration
	@EnableConfigurationProperties(SqoopCommonTaskProperties.class)
	static class Conf {
	}
}
