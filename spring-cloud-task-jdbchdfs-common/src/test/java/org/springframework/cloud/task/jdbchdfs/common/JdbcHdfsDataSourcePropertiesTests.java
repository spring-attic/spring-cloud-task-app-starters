/*
 * Copyright 2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
	private static final String DRIVER_CLASS_TWO = "driverClassTwo";
	private static final String USER_NAME_ONE= "userNameOne";
	private static final String USER_NAME_TWO = "userNameTwo";
	private static final String PASSWORD_ONE= "passwordOne";
	private static final String PASSWORD_TWO = "passwordTwo";
	private static final String URL_ONE= "urlOne";
	private static final String URL_TWO = "urlTwo";


	/**
	 * Verify that the defaults for the properties are properly set.
	 */
	@Test
	public void defaultPropertyTests() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
		JdbcHdfsDataSourceProperties properties = context.getBean(JdbcHdfsDataSourceProperties.class);
		assertNull(properties.getJdbchdfs_datasource_driverClassName());
		assertNull(properties.getJdbchdfs_datasource_password());
		assertNull(properties.getJdbchdfs_datasource_url());
		assertNull(properties.getJdbchdfs_datasource_username());
		assertNull(properties.getTask_datasource_driverClassName());
		assertNull(properties.getTask_datasource_password());
		assertNull(properties.getTask_datasource_url());
		assertNull(properties.getTask_datasource_username());
	}

	/**
	 * Verify setters and getters are working properly.
	 */
	@Test
	public void testSetters() {
		JdbcHdfsDataSourceProperties properties = new JdbcHdfsDataSourceProperties();
		properties.setJdbchdfs_datasource_driverClassName(DRIVER_CLASS_ONE);
		properties.setTask_datasource_driverClassName(DRIVER_CLASS_TWO);
		properties.setJdbchdfs_datasource_username(USER_NAME_ONE);
		properties.setTask_datasource_username(USER_NAME_TWO);
		properties.setJdbchdfs_datasource_password(PASSWORD_ONE);
		properties.setTask_datasource_password(PASSWORD_TWO);
		properties.setJdbchdfs_datasource_url(URL_ONE);
		properties.setTask_datasource_url(URL_TWO);
		assertEquals(DRIVER_CLASS_ONE, properties.getJdbchdfs_datasource_driverClassName());
		assertEquals(DRIVER_CLASS_TWO, properties.getTask_datasource_driverClassName());
		assertEquals(USER_NAME_ONE, properties.getJdbchdfs_datasource_username());
		assertEquals(USER_NAME_TWO, properties.getTask_datasource_username());
		assertEquals(PASSWORD_ONE, properties.getJdbchdfs_datasource_password());
		assertEquals(PASSWORD_TWO, properties.getTask_datasource_password());
		assertEquals(URL_ONE, properties.getJdbchdfs_datasource_url());
		assertEquals(URL_TWO, properties.getTask_datasource_url());

	}

	@Configuration
	@EnableConfigurationProperties(JdbcHdfsDataSourceProperties.class)
	static class Conf {
	}
}
