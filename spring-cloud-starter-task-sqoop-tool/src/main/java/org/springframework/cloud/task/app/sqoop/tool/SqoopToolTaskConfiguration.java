/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.task.app.sqoop.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.util.Jars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.sqoop.common.SqoopCommonRunnerUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link CommandLineRunner} implementation that will run a Sqoop Tool.
 * This implementation currently supports import and export commands.
 *
 * @author Thomas Risberg
 */
@EnableTask
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties({ SqoopToolTaskProperties.class })
public class SqoopToolTaskConfiguration {

	@Bean
	public CommandLineRunner commandLineRunner() {
		return new SqoopToolRunner();
	}

	private class SqoopToolRunner implements CommandLineRunner {

		private final Logger logger = LoggerFactory.getLogger(SqoopToolRunner.class);

		@Autowired
		private SqoopToolTaskProperties props;

		@Autowired
		private Configuration hadoopConfiguration;

		@Value("${spring.hadoop.config.mapreduce.framework.name:yarn}")
		private String mapreduceFramework;


		@Override
		public void run(String... args) throws Exception {

			List<String> finalArguments = createFinalArguments();

			logger.info("Running Sqoop tool with arguments: " + finalArguments);

			Configuration configuration = new Configuration(hadoopConfiguration);
			logger.info("Setting mapreduce.framework.name to " + mapreduceFramework);
			configuration.set("mapreduce.framework.name", mapreduceFramework);

			final int ret = Sqoop.runTool(finalArguments.toArray(new String[finalArguments.size()]), configuration);

			logger.info("Sqoop tool completed with return code: " + ret);

			if (ret != 0) {
				throw new RuntimeException("Sqoop job failed - return code " + ret);
			}
		}

		protected List<String> createFinalArguments() {
			List<String> finalArguments = new ArrayList<String>();
			String command = props.getCommand();
			finalArguments.add(command);
			SqoopCommonRunnerUtils.setConnectProperties(props, finalArguments);
			if (command.toLowerCase().startsWith("import") || command.toLowerCase().startsWith("export")) {
				finalArguments.add("--hadoop-mapred-home=" + Jars.getJarPathForClass(Jars.class)
						.substring(0, Jars.getJarPathForClass(Jars.class).lastIndexOf("/")));
			}
			List<String> toolArguments = new ArrayList<String>();
			if (StringUtils.hasText(props.getToolArgs())) {
				String[] args = props.getToolArgs().split("\\s+");
				for (String arg : args) {
					if (arg.startsWith("--hive") || arg.startsWith("--hcatalog") ||
							arg.startsWith("--hbase") || arg.startsWith("--accumulo")) {
						throw new IllegalArgumentException(
								arg + " is incompatible with tool execution from a distributed task.");
					}
					toolArguments.add(arg);
				}
			}
			finalArguments.addAll(toolArguments);
			return finalArguments;
		}
	}

}
