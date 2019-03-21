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

package org.springframework.cloud.task.app.sqoop.tool;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Thomas Risberg
 */
public class SqoopToolTaskPropertiesTests {

	@Test
	public void testCommandCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		EnvironmentTestUtils.addEnvironment(context, "command: export");
		context.register(Conf.class);
		context.refresh();
		SqoopToolTaskProperties properties = context.getBean(SqoopToolTaskProperties.class);
		assertThat(properties.getCommand(), equalTo("export"));
	}

	@Test(expected=BeanCreationException.class)
	public void testCommandIsRequired() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
	}

	@Configuration
	@EnableConfigurationProperties(SqoopToolTaskProperties.class)
	static class Conf {
	}
}
