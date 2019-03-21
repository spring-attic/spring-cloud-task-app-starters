/*
 * Copyright 2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.springframework.cloud.task.jdbchdfs.common;

import org.junit.Test;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.jdbchdfs.common.support.JdbcHdfsDataSourceProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Verifies that configurations for the data source properties are
 * correct.
 *
 * @author Glenn Renfro
 */
public class JdbcHdfsDataSourcePropertiesTests {
	private static final String DRIVER_CLASS_ONE = "driverClassOne";
	private static final String USER_NAME_ONE= "userNameOne";
	private static final String PASSWORD_ONE= "passwordOne";
	private static final String URL_ONE= "urlOne";


	/**
	 * Verify that the defaults for the properties are properly set.
	 */
	@Test
	public void defaultPropertyTests() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
		JdbcHdfsDataSourceProperties properties = context.getBean(JdbcHdfsDataSourceProperties.class);
		assertNull(properties.getDriverClassName());
		assertNull(properties.getPassword());
		assertNull(properties.getUrl());
		assertNull(properties.getUsername());
	}

	/**
	 * Verify setters and getters are working properly.
	 */
	@Test
	public void testSetters() {
		JdbcHdfsDataSourceProperties properties = new JdbcHdfsDataSourceProperties();
		properties.setDriverClassName(DRIVER_CLASS_ONE);
		properties.setUsername(USER_NAME_ONE);
		properties.setPassword(PASSWORD_ONE);
		properties.setUrl(URL_ONE);
		assertEquals(DRIVER_CLASS_ONE, properties.getDriverClassName());
		assertEquals(USER_NAME_ONE, properties.getUsername());
		assertEquals(PASSWORD_ONE, properties.getPassword());
		assertEquals(URL_ONE, properties.getUrl());

	}

	@Configuration
	@EnableConfigurationProperties(JdbcHdfsDataSourceProperties.class)
	static class Conf {
	}
}
