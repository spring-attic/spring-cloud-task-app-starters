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

package org.springframework.cloud.task.app.sqoop.job;

import static org.junit.Assert.assertTrue;

import javax.sql.DataSource;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.hsqldb.Server;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.task.sqoop.common.SqoopToolDatabaseConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Verifies that the Sqoop Tool Task Application runs without throwing exception.
 *
 * NOTE: This test is using the local file system and running mapreduce in local mode.
 *
 * NOTE: We also start an HSQLDB database server with a data path of 'target/db'
 *
 * @author Thomas Risberg
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes={SqoopToolDatabaseConfiguration.class,
		SqoopToolJobApplicationTests.TestConfig.class, SqoopToolJobApplicationTests.TestSqoopJobTaskApplication.class})
@IntegrationTest
public abstract class SqoopToolJobApplicationTests {

	private static String testDir;

	static {
		try {
			testDir = Files.createTempDirectory("sqoop-test-").toFile().getAbsolutePath();
			FileUtils.deleteDirectory(new File(testDir));
		} catch (IOException e) {
			testDir = System.getProperty("${java.io.tmpdir}") + "/sqoop-test-" + UUID.randomUUID();
		}
		System.setProperty("sqoop.test.dir", testDir);
	}

	@IntegrationTest({"spring.cloud.task.closecontext.enable:false",
			"spring.hadoop.fsUri=file:///",
			"spring.hadoop.config.mapreduce.framework.name=local",
			"metastore-url=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"metastore-username=sa",
			"action=create",
			"job-name=newtest",
			"command=import",
			"connect=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"username=sa",
			"tool-args=--table FOO --target-dir ${sqoop.test.dir} -m 1 --outdir target/java"})
	public static class SqoopToolCreateTests extends SqoopToolJobApplicationTests {

		@Autowired
		private DataSource dataSource;

		@Test
		public void testCreateJob() throws Exception {
			JdbcTemplate db = new JdbcTemplate(dataSource);
			int count =
					db.queryForObject("SELECT COUNT(*) FROM SQOOP_SESSIONS WHERE JOB_NAME = 'newtest'", Integer.class);
			assertTrue("Metadata rows should have been created", count > 50);
		}
	}

	@IntegrationTest({"spring.cloud.task.closecontext.enable:false",
			"spring.hadoop.fsUri=file:///",
			"spring.hadoop.config.mapreduce.framework.name=local",
			"metastore-url=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"metastore-username=sa",
			"action=exec",
			"connect=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"username=sa",
			"job-name=mytest",
			"tool-args=--target-dir ${sqoop.test.dir}"})
	public static class SqoopToolExecTests extends SqoopToolJobApplicationTests {

		@Test
		public void testExecJob() throws Exception {
			File testOutput = new File(testDir);
			assertTrue(testOutput.exists());
			File[] files = testOutput.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					if (name.startsWith("part")) {
						return true;
					}
					return false;
				}
			});
			assertTrue("At least one data file should have been created", files.length > 0);
		}
	}

	@IntegrationTest({"spring.cloud.task.closecontext.enable:false",
			"spring.hadoop.fsUri=file:///",
			"spring.hadoop.config.mapreduce.framework.name=local",
			"metastore-url=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"metastore-username=sa",
			"action=delete",
			"job-name=oldtest"})
	public static class SqoopToolDeleteTests extends SqoopToolJobApplicationTests {

		@Autowired
		private DataSource dataSource;

		@Test
		public void testDeleteJob() throws Exception {
			JdbcTemplate db = new JdbcTemplate(dataSource);
			int count =
					db.queryForObject("SELECT COUNT(*) FROM SQOOP_SESSIONS WHERE JOB_NAME = 'oldtest'", Integer.class);
			assertTrue("Metadata rows should have been deleted", count == 0);
		}
	}

	@AfterClass
	public static void cleanup() throws IOException {
		FileUtils.deleteDirectory(new File(testDir));
	}

	@Configuration
	public static class TestConfig {

		@Bean
		DataSource dataSource(Server dbServer) {
			int hsqldbPort = dbServer.getPort();
			String host = "localhost";
			String dbName = dbServer.getDatabaseName(0, true);
			String dbUrl = "jdbc:hsqldb:hsql://" + host +":" + hsqldbPort + "/" + dbName;
			SingleConnectionDataSource dataSource =
					new SingleConnectionDataSource(dbUrl, "sa", "", true);
			return dataSource;
		}

		@Bean
		DatabasePopulator databasePopulator(DataSource dataSource) throws SQLException, InterruptedException {
			DatabasePopulator dbp = new ResourceDatabasePopulator(false, false, "UTF-8",
					new ClassPathResource("sqoop-schema-ddl.sql"),
					new ClassPathResource("sqoop-root-data.sql"),
					new ClassPathResource("sqoop-sessions-data.sql"),
					new ClassPathResource("sqoop-test-schema-ddl.sql"),
					new ClassPathResource("sqoop-test-data.sql"));
			dbp.populate(DataSourceUtils.getConnection(dataSource));
			return dbp;
		}
	}
	@SpringBootApplication
	public static class TestSqoopJobTaskApplication {
		public static void main(String[] args) {
			SpringApplication.run(TestSqoopJobTaskApplication.class, args);
		}
	}
}
