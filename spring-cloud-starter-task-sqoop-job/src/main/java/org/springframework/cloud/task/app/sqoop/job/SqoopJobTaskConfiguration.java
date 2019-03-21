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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.util.Jars;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.cloud.task.sqoop.common.SqoopCommonRunnerUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StringUtils;

/**
 * {@link CommandLineRunner} implementation that will create or run a Sqoop Job.
 *
 * @author Thomas Risberg
 */
@EnableTask
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties({ SqoopJobTaskProperties.class })
public class SqoopJobTaskConfiguration {

	@Bean
	public CommandLineRunner commandLineRunner() {
		return new SqoopJobRunner();
	}

	private class SqoopJobRunner implements CommandLineRunner {

		private final Log logger = LogFactory.getLog(SqoopJobRunner.class);

		private SqoopJobTaskProperties props;

		@Autowired
		private Configuration hadoopConfiguration;

		@Value("${spring.hadoop.config.mapreduce.framework.name:yarn}")
		private String mapreduceFramework;

		@Override
		public void run(String... args) throws Exception {

			List<String> finalJobArguments = createFinalJobArguments();

			logger.info("Running Sqoop job with arguments: " + finalJobArguments);

			Configuration configuration = new Configuration(hadoopConfiguration);
			logger.info("Setting mapreduce.framework.name to " + mapreduceFramework);
			configuration.set("mapreduce.framework.name", mapreduceFramework);
			configuration.set("sqoop.metastore.client.autoconnect.url", props.getMetastoreUrl());
			if (StringUtils.hasText(props.getMetastoreUsername())) {
				configuration.set("sqoop.metastore.client.autoconnect.username", props.getMetastoreUsername());
			}
			if (StringUtils.hasText(props.getMetastorePassword())) {
				configuration.set("sqoop.metastore.client.autoconnect.password", props.getMetastorePassword());
			}

			final int ret = Sqoop.runTool(finalJobArguments.toArray(new String[finalJobArguments.size()]), configuration);

			logger.info("Sqoop job completed with return code: " + ret);

			if (ret != 0) {
				throw new RuntimeException("Sqoop job failed - return code " + ret);
			}
		}

		public SqoopJobTaskProperties getProps() {
			return props;
		}

		@Autowired
		public void setProps(SqoopJobTaskProperties props) {
			this.props = props;
		}

		protected List<String> createFinalJobArguments() {
			List<String> finalArguments = new ArrayList<String>();
			finalArguments.add("job");
			String action = props.getAction();
			finalArguments.add("--" + action);
			finalArguments.add(props.getJobName());
			finalArguments.add("--");
			String command = props.getCommand();
			if (action.toLowerCase().startsWith("create")) {
				finalArguments.add(command);
			}
			if (action.toLowerCase().startsWith("exec")) {
				finalArguments.add("--hadoop-mapred-home");
				finalArguments.add(Jars.getJarPathForClass(Jars.class)
						.substring(0, Jars.getJarPathForClass(Jars.class).lastIndexOf("/")));
			}
			SqoopCommonRunnerUtils.setConnectProperties(props, finalArguments);
			List<String> toolArguments = new ArrayList<String>();
			if (StringUtils.hasText(props.getToolArgs())) {
				String[] args = props.getToolArgs().split("\\s+");
				for (String arg : args) {
					if (arg.startsWith("--hive") || arg.startsWith("--hcatalog") ||
							arg.startsWith("--hbase") || arg.startsWith("--accumulo")) {
						throw new IllegalArgumentException(
								arg + " is incompatible with job execution from a distributed task.");
					}
					toolArguments.add(arg);
				}
			}
			finalArguments.addAll(toolArguments);
			return finalArguments;
		}
	}

}
