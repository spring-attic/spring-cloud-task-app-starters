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
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

/**
 * Test the JdbcHdfsTaskProperties to verify the properties.
 *
 * @author Glenn Renfro
 */
public class JdbcHdfsTaskPropertyTests {

	public static final String FS_URI = "TEST_FS_URI";
	public static final int COMMIT_INTERVAL = 9990000;
	public static final String DELIMITER = "DELIMITER";
	public static final int PARTITION_COUNT = 9999999;
	public static final String FILE_NAME = "FILE_NAME";
	public static final long ROLLOVER = 9999999;
	public static final String FILE_EXTENSION = "EXTENSION";
	public static final String DIRECTORY = "DIRECTORY";
	public static final String COLUMN_NAMES = "a,b,c";
	public static final String CHECK_COLUMN = "ACOLUMN";
	public static final String TABLE_NAME = "TABLE_NAME";
	public static final String SQL = "SELECT * FROM WOW";
	public static final String PARTITION_COLUMN = "PARTITION_COL";
	public static final String NAMED_NODE_PRINCIPLE = "NAMED NODE PRINCIPLE";
	public static final String USER_PRINCIPLE = "USER PRINCIPLE";
	public static final String REGISTER_URL_HANDLER = "REGISTER URL HANDLER";
	public static final String PROPERTIES_LOCATION = "PROPERTIES LOCATION";
	public static final String RM_MANAGER_PRINCIPLE = "RM MANAGER PRINCIPLE";
	public static final String SECURITY_METHOD = "SECURITY METHOD";
	public static final String USER_KEY_TAB = "USER KEY TAB";
	public static final int MAX_WORKERS= 9999999;

	/**
	 * Verify that the defaults are set properly.
	 */
	@Test
	public void defaultPropertyTests() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
		JdbcHdfsTaskProperties properties = context.getBean(JdbcHdfsTaskProperties.class);
		assertEquals(null, properties.getFsUri());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_COMMIT_INTERVAL, properties.getCommitInterval());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_DELIMITER, properties.getDelimiter());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_PARTITION_COUNT, properties.getPartitions());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_FILE_NAME, properties.getFileName());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_ROLLOVER, properties.getRollover());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_DIRECTORY, properties.getDirectory());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_FILE_EXTENSION, properties.getFileExtension());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_MAX_WORKERS, properties.getMaxWorkers());

		assertFalse(properties.isRestartable());
		assertNull(properties.getCheckColumn());
		assertNull(properties.getColumnNames());
		assertNull(properties.getTableName());
		assertNull(properties.getSql());
		assertNull(properties.getPartitionColumn());

		assertNull(properties.getNameNodePrincipal());
		assertNull(properties.getPropertiesLocation());
		assertNull(properties.getNameNodePrincipal());
		assertNull(properties.getRegisterUrlHandler());
		assertNull(properties.getRmManagerPrincipal());
		assertNull(properties.getSecurityMethod());
		assertNull(properties.getUserKeyTab());
		assertNull(properties.getUserPrincipal());
	}

	/**
	 * Verify that the sets are functioning properly.
	 */
	@Test
	public void propertySetTests() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
		JdbcHdfsTaskProperties properties = context.getBean(JdbcHdfsTaskProperties.class);
		properties.setFsUri(FS_URI);
		properties.setDelimiter(DELIMITER);
		properties.setCommitInterval(COMMIT_INTERVAL);
		properties.setPartitions(PARTITION_COUNT);
		properties.setFileName(FILE_NAME);
		properties.setRollover(ROLLOVER);
		properties.setFileExtension(FILE_EXTENSION);
		properties.setDirectory(DIRECTORY);
		properties.setRestartable(true);
		properties.setCheckColumn(CHECK_COLUMN);
		properties.setColumnNames(COLUMN_NAMES);
		properties.setTableName(TABLE_NAME);
		properties.setSql(SQL);
		properties.setPartitionColumn(PARTITION_COLUMN);
		properties.setNameNodePrincipal(NAMED_NODE_PRINCIPLE);
		properties.setUserPrincipal(USER_PRINCIPLE);
		properties.setPropertiesLocation(PROPERTIES_LOCATION);
		properties.setRegisterUrlHandler(REGISTER_URL_HANDLER);
		properties.setRmManagerPrincipal(RM_MANAGER_PRINCIPLE);
		properties.setUserKeyTab(USER_KEY_TAB);
		properties.setSecurityMethod(SECURITY_METHOD);
		properties.setMaxWorkers(MAX_WORKERS);

		assertEquals(FS_URI, properties.getFsUri());
		assertEquals(DELIMITER, properties.getDelimiter());
		assertEquals(COMMIT_INTERVAL, properties.getCommitInterval());
		assertEquals(PARTITION_COUNT, properties.getPartitions());
		assertEquals(FILE_NAME, properties.getFileName());
		assertEquals(ROLLOVER, properties.getRollover());
		assertEquals(DIRECTORY, properties.getDirectory());
		assertEquals(FILE_EXTENSION, properties.getFileExtension());

		assertEquals(true, properties.isRestartable());
		assertEquals(CHECK_COLUMN, properties.getCheckColumn());
		assertEquals(COLUMN_NAMES, properties.getColumnNames());
		assertEquals(TABLE_NAME, properties.getTableName());
		assertEquals(SQL, properties.getSql());
		assertEquals(PARTITION_COLUMN, properties.getPartitionColumn());

		assertEquals(NAMED_NODE_PRINCIPLE, properties.getNameNodePrincipal());
		assertEquals(PROPERTIES_LOCATION, properties.getPropertiesLocation());
		assertEquals(REGISTER_URL_HANDLER, properties.getRegisterUrlHandler());
		assertEquals(RM_MANAGER_PRINCIPLE, properties.getRmManagerPrincipal());
		assertEquals(SECURITY_METHOD, properties.getSecurityMethod());
		assertEquals(USER_KEY_TAB, properties.getUserKeyTab());
		assertEquals(USER_PRINCIPLE, properties.getUserPrincipal());
		assertEquals(MAX_WORKERS, properties.getMaxWorkers());
	}

	@Configuration
	@EnableConfigurationProperties(JdbcHdfsTaskProperties.class)
	static class Conf {
	}
}
