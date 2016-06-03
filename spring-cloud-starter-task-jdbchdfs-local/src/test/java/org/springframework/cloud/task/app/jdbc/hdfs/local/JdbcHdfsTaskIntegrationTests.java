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

package org.springframework.cloud.task.app.jdbc.hdfs.local;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.hsqldb.Server;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.datasource.init.DatabasePopulator;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;

/**
 * Verifies that the Task Application outputs the correct task log entries.
 *
 * @author Glenn Renfro
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = {JdbcHdfsDatabaseConfiguration.class,
		JdbcHdfsTaskIntegrationTests.TestConfig.class, JdbcHdfsTaskIntegrationTests.TestJdbcHdfsTaskApplication.class})
@IntegrationTest
public abstract class JdbcHdfsTaskIntegrationTests {

	@IntegrationTest({"spring.cloud.task.closecontext.enable:false",
			"hadoop.home.dir=/data",
			"directory=${java.io.tmpdir}/jdbchdfs-task/",
			"spring.profiles.active=master",
			"fsUri=file:///",
			"spring.datasource.url=jdbc:hsqldb:hsql://localhost:${db.server.port}/test",
			"spring.datasource.username=sa",
			"tableName=FOO",
			"partitionColumn=id",
			"columnNames=PROPKEY,PROPVALUE"})
	public static class JdbcHDFSBasicTest extends JdbcHdfsTaskIntegrationTests {

		@Autowired
		private DataSource dataSource;

		@Autowired
		ConfigurableApplicationContext applicationContext;

		@Value("${directory}")
		private String testDir;

		@Autowired
		FsShell fsShell;

		@After
		public void cleanup() {
			if (fsShell.test(testDir)) {
				fsShell.rmr(testDir);
			}

			if (applicationContext != null && applicationContext.isActive()) {
				applicationContext.close();
			}
		}

		@Test
		public void testCreateJob() throws Exception {
			File testOutput = new File(testDir);
			Assert.assertTrue(testOutput.exists());
			checkPartitionInstance(testDir, "p0-0.csv", "ONE,ONEVALUE\nTWO,TWOVALUE\n");
			checkPartitionInstance(testDir, "p1-0.csv", "THREE,THREEVALUE\nFOUR,FOURVALUE\n");
			checkPartitionInstance(testDir, "p2-0.csv", "FIVE,FIVEVALUE\nSIX,SIXVALUE\n");
		}

		private void checkPartitionInstance(String testDir, final String fileSuffix, String expectedData) throws Exception {
			File testOutput = new File(testDir);
			Assert.assertTrue(testOutput.exists());
			File[] partitionFile = testOutput.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(fileSuffix);
				}

			});
			Assert.assertTrue(partitionFile.length > 0);
			File dataFile = partitionFile[0];
			assertNotNull(dataFile);
			Assert.assertThat(readFile(dataFile.getPath(), Charset.forName("UTF-8")), equalTo(expectedData));

		}

		private String readFile(String path, Charset encoding) throws IOException {
			byte[] encoded = Files.readAllBytes(Paths.get(path));
			return new String(encoded, encoding);
		}
	}

	@SpringBootApplication
	public static class TestJdbcHdfsTaskApplication {
		public static void main(String[] args) {
			SpringApplication.run(TestJdbcHdfsTaskApplication.class, args);
		}
	}


	@Configuration
	public static class TestConfig {
		@Bean
		DataSource dataSource(Server dbServer) {
			int hsqldbPort = dbServer.getPort();
			String host = "localhost";
			String dbName = dbServer.getDatabaseName(0, true);
			String dbUrl = "jdbc:hsqldb:hsql://" + host + ":" + hsqldbPort + "/" + dbName;
			SingleConnectionDataSource dataSource =
					new SingleConnectionDataSource(dbUrl, "sa", "", true);
			return dataSource;
		}

		@Bean
		DatabasePopulator databasePopulator(DataSource dataSource) throws SQLException, InterruptedException {
			DatabasePopulator dbp = new ResourceDatabasePopulator(false, false, "UTF-8",
					new ClassPathResource("jdbchdfs-schema.sql"),
					new ClassPathResource("jdbchdfs-data.sql"));
			dbp.populate(DataSourceUtils.getConnection(dataSource));
			return dbp;
		}
	}
}