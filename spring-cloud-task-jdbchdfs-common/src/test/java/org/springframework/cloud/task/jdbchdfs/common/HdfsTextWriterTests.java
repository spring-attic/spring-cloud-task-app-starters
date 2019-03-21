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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.springframework.util.FileSystemUtils;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotNull;

/**
 * @author Glenn Renfro
 */
public class HdfsTextWriterTests {

	public static final String ROW_1 = "Hello world you rock";
	public static final String ROW_2 = "And Rock on";
	public static final String ROW_TERMINATOR = "\n";

	private String tmpDir;

	private HdfsTextItemWriter writer;

	JdbcHdfsTaskProperties props;


	@Before
	public void setup() throws Exception {
		tmpDir = System.getProperty("java.io.tmpdir") + "/jdbchdfs-task";
		File file = new File(tmpDir);
		if (file.exists()) {
			FileSystemUtils.deleteRecursively(file);
		}
		props = new JdbcHdfsTaskProperties();
		props.setFsUri("file:///");
		props.setDirectory(tmpDir);
		props.setFileName("dataWriterBasicTest");
	}

	@After
	public void tearDown() {
		if (writer != null) {
			writer.close();
		}
		File file = new File(tmpDir);
		if (file.exists()) {
			FileSystemUtils.deleteRecursively(file);
		}
	}

	@Test
	public void testDataWriterBasic() throws Exception {
		props.setRollover(100);
		HdfsTextItemWriterFactory factory = new HdfsTextItemWriterFactory(new org.apache.hadoop.conf.Configuration(), props, "part1");
		writer = factory.getObject();
		List<String> list = new ArrayList<String>();
		list.add(ROW_1);
		writer.write(list);
		list = new ArrayList<String>();
		list.add(ROW_2);
		writer.write(list);
		writer.close();
		checkPartitionInstance(tmpDir, "-0.csv", ROW_1 + ROW_TERMINATOR + ROW_2 + ROW_TERMINATOR);
	}

	@Test
	public void testDataWriterRollover() throws Exception {
		props.setRollover(1);
		HdfsTextItemWriterFactory factory = new HdfsTextItemWriterFactory(new org.apache.hadoop.conf.Configuration(), props, "part1");
		writer = factory.getObject();
		List<String> list = new ArrayList<String>();
		list.add(ROW_1);
		writer.write(list);
		list = new ArrayList<String>();
		list.add(ROW_2);
		writer.write(list);
		writer.close();
		checkPartitionInstance(tmpDir, "-0.csv", ROW_1 + ROW_TERMINATOR);
		checkPartitionInstance(tmpDir, "-1.csv", ROW_2 + ROW_TERMINATOR);
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
