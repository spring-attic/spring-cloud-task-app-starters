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

public class JdbcHdfsTaskPropertyTests {

	@Test
	public void defaultPropertyTests() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(Conf.class);
		context.refresh();
		JdbcHdfsTaskProperties properties = context.getBean(JdbcHdfsTaskProperties.class);
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_FS_URI, properties.getFsUri());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_COMMIT_INTERVAL, properties.getCommitInterval());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_DELIMITER, properties.getDelimiter());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_PARTITION_COUNT, properties.getPartitions());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_FILE_NAME, properties.getFileName());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_ROLLOVER, properties.getRollover());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_DIRECTORY, properties.getDirectory());
		assertEquals(JdbcHdfsTaskProperties.DEFAULT_FILE_EXTENSION, properties.getFileExtension());

		assertFalse(properties.isRestartable());
		assertNull(properties.getCheckColumn());
		assertNull(properties.getColumnNames());
		assertNull(properties.getTableName());
		assertNull(properties.getSql());
		assertNull(properties.getPartitionColumn());

		assertNull(properties.getNameNodePrinciple());
		assertNull(properties.getPropertiesLocation());
		assertNull(properties.getNameNodePrinciple());
		assertNull(properties.getRegisterUrlHandler());
		assertNull(properties.getRmManagerPrinciple());
		assertNull(properties.getSecurityMethod());
		assertNull(properties.getUserKeyTab());
		assertNull(properties.getUserPrinciple());
	}

	@Configuration
	@EnableConfigurationProperties(JdbcHdfsTaskProperties.class)
	static class Conf {
	}
}
