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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.task.jdbchdfs.common.support.JdbcHdfsDataSourceConfiguration;
import org.springframework.cloud.task.jdbchdfs.common.support.JdbcHdfsDataSourceProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Verify DataSourceConfiguration intializes properly.
 *
 * @author Glenn Renfro
 */
public class JdbcHdfsDataSourceConfigurationTests {

	@Autowired
	private AnnotationConfigApplicationContext context;

	private static final String EXCEPTION_MESSAGE = "This was expected";

	private JdbcHdfsDataSourceProperties props;

	private Connection connection;

	@Before
	public void setup() {
		context = new AnnotationConfigApplicationContext();
		context.register(JdbcHdfsDataSourceProperties.class);
		context.refresh();
		this.props = context.getBean(JdbcHdfsDataSourceProperties.class);
		connection = null;
	}

	@After
	public void tearDown() throws SQLException{
		if (connection != null) {
			connection.close();
		}
		if (context != null && context.isActive()) {
			context.close();
		}
	}

	/**
	 * Verify that the Task DataSource is created.
	 */
	@Test
	public void testTaskDataSource() throws SQLException{
		Properties properties = new Properties();
		properties.put("spring.datasource.url","jdbc:hsqldb:mem:task");
		properties.put("spring.datasource.driverClassName","org.hsqldb.jdbc.JDBCDriver");
		properties.put("spring.datasource.username","sa");
		properties.put("spring.datasource.password","");

		context.getEnvironment().getPropertySources().addFirst(new PropertiesPropertySource("options", properties));
		context.register(JdbcHdfsDataSourceConfiguration.class);
		JdbcHdfsDataSourceConfiguration config = context.getBean(JdbcHdfsDataSourceConfiguration.class);
		DataSource dataSource = config.taskDataSource();
		assertNotNull(dataSource);
		connection = dataSource.getConnection();
		assertTrue(connection.getMetaData().getURL().endsWith("task"));
	}

	/**
	 * Verify that the Task DataSource Fails to create if properties are not set.
	 */
	@Test(expected = SQLException.class)
	public void testFailTaskDataSource() throws SQLException{
		context.register(JdbcHdfsDataSourceConfiguration.class);
		JdbcHdfsDataSourceConfiguration config = context.getBean(JdbcHdfsDataSourceConfiguration.class);
		DataSource dataSource = config.taskDataSource();
		assertNotNull(dataSource);
		connection = dataSource.getConnection();
	}

	/**
	 * Verify that the JdbcHdfs DataSource is created.
	 */
	@Test
	public void testJdbcHdfsDataSource() throws SQLException{
		this.props.setUrl("jdbc:hsqldb:mem:jdbchdfs");
		this.props.setPassword("");
		this.props.setUsername("sa");
		this.props.setDriverClassName("org.hsqldb.jdbc.JDBCDriver");

		context.register(JdbcHdfsDataSourceConfiguration.class);
		JdbcHdfsDataSourceConfiguration config = context.getBean(JdbcHdfsDataSourceConfiguration.class);
		DataSource dataSource = config.jdbcHdfsDataSource();
		assertNotNull(dataSource);
		connection = dataSource.getConnection();
		assertTrue(connection.getMetaData().getURL().endsWith("jdbchdfs"));
	}

	/**
	 * Verify that the JdbcHdfs DataSource Fails to create if properties are not set.
	 */
	@Test(expected = SQLException.class)
	public void testFailJdbcHdfsDataSource() throws SQLException{
		context.register(JdbcHdfsDataSourceConfiguration.class);
		JdbcHdfsDataSourceConfiguration config = context.getBean(JdbcHdfsDataSourceConfiguration.class);
		DataSource dataSource = config.jdbcHdfsDataSource();
		assertNotNull(dataSource);
		connection = dataSource.getConnection();
	}


}
