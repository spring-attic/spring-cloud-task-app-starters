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

package org.springframework.cloud.task.jdbchdfs.common.support;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Supports the datasource configurations required for the JdbcHdfs applicaiton.
 *
 * @author Glenn Renfro
 */
@EnableConfigurationProperties({JdbcHdfsDataSourceProperties.class})
public class JdbcHdfsDataSourceConfiguration {

	@Autowired
	private JdbcHdfsDataSourceProperties props;

	@Bean(name="taskDataSource")
	@Primary
	public DataSource taskDataSource() {
		return DataSourceBuilder.create().driverClassName(props.getTask_datasource_driverClassName())
				.url(props.getTask_datasource_url())
				.username(props.getTask_datasource_username())
				.password(props.getTask_datasource_password()).build();
	}


	@Bean(name="jdbchdfsDataSource")
	public DataSource jdbcHdfsDataSource() {
		return DataSourceBuilder.create().driverClassName(props.getJdbchdfs_datasource_driverClassName())
				.url(props.getJdbchdfs_datasource_url())
				.username(props.getJdbchdfs_datasource_username())
				.password(props.getJdbchdfs_datasource_password()).build();
	}

}
