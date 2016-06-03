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

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.deployer.spi.local.LocalDeployerProperties;
import org.springframework.cloud.deployer.spi.local.LocalTaskLauncher;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.jdbchdfs.common.JdbcHdfsConfiguration;
import org.springframework.cloud.task.jdbchdfs.common.JdbcHdfsTaskProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * A batch job task that migrates data from a Jdbc repository to a hdfs file.
 */
@Configuration
@EnableConfigurationProperties({JdbcHdfsTaskProperties.class})
@Import(JdbcHdfsConfiguration.class)
public class JdbcHdfsLocalTaskConfiguration {

	@Bean
	public TaskLauncher taskLauncher() {
		LocalDeployerProperties localDeployerProperties = new LocalDeployerProperties();

		localDeployerProperties.setDeleteFilesOnExit(false);

		return new LocalTaskLauncher(localDeployerProperties);
	}

}
