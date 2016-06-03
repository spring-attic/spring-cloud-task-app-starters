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

import java.util.HashMap;
import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Glenn Renfro
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {NamedColumnJdbcItemReaderTests.DBConfiguration.class})
@DirtiesContext
public class NamedColumnJdbcItemReaderTests {

	@Autowired
	DataSource dataSource;

	NamedColumnJdbcItemReaderFactory factory;

	@Before
	public void setup() {
		factory = new NamedColumnJdbcItemReaderFactory();
		factory.setDataSource(dataSource);
		factory.setDelimiter(",");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNoSqlNoTable() throws Exception {
		factory.afterPropertiesSet();
	}

	@Test
	public void testTableColumns() throws Exception {
		factory.setTableName("test");
		factory.setColumnNames("id, name");
		factory.afterPropertiesSet();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullColumns() throws Exception {
		factory.setTableName("foo");
		factory.afterPropertiesSet();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullTable() throws Exception {
		factory.setColumnNames("foo, bar");
		factory.afterPropertiesSet();
	}

	@Test
	public void testIsSingleton() {
		assertTrue(factory.isSingleton());
	}

	@Test
	public void testGetObjectType() {
		assertEquals(NamedColumnJdbcItemReader.class, factory.getObjectType());
	}

	@Test
	public void testPartitionedSql() throws Exception {
		factory.setColumnNames("foo, bar");
		factory.setTableName("baz");
		factory.setPartitionClause("WHERE foo BETWEEN 17 AND 42");
		DataSource dataSource = new SingleConnectionDataSource("jdbc:hsqldb:mem:test", "sa", "", false);
		factory.setDataSource(dataSource);
		factory.afterPropertiesSet();
		assertEquals("Partitioned SQL", "SELECT foo, bar FROM baz WHERE foo BETWEEN 17 AND 42",
				ReflectionTestUtils.getField(factory, "sql"));
	}

	@Test
	public void testGetTableRows() throws Exception {
		factory.setTableName("test");
		factory.setColumnNames("id, name");
		factory.afterPropertiesSet();
		NamedColumnJdbcItemReader reader = factory.getObject();
		reader.afterPropertiesSet();
		reader.open(new ExecutionContext(new HashMap<String, Object>()));

		verifyRead(reader, "1,Bob");
		verifyRead(reader, "2,Jane");
		verifyRead(reader, "3,John");
		verifyRead(reader, null);
	}

	@Test
	public void testGetSqlRows() throws Exception {
		NamedColumnJdbcItemReaderFactory factory = new NamedColumnJdbcItemReaderFactory();
		factory.setDataSource(dataSource);
		factory.setSql("SELECT name, id FROM test");
		factory.afterPropertiesSet();

		NamedColumnJdbcItemReader reader = factory.getObject();
		reader.afterPropertiesSet();
		reader.open(new ExecutionContext(new HashMap<String, Object>()));
		reader.setDelimiter(",");
		verifyRead(reader, "Bob,1");
		verifyRead(reader, "Jane,2");
		verifyRead(reader, "John,3");
		verifyRead(reader, null);
	}

	private void verifyRead(NamedColumnJdbcItemReader reader, String expectedResult) throws Exception {
		String result = reader.read();
		if (expectedResult == null) {
			assertNull(result);
		}
		else {
			assertEquals(expectedResult, result);
		}
	}

	@EnableAutoConfiguration
	public static class DBConfiguration {
	}
}
