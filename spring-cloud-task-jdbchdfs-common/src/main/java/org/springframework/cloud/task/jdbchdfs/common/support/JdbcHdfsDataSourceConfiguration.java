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

package org.springframework.cloud.task.jdbchdfs.common.support;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

/**
 * Supports the datasource configurations required for the JdbcHdfs application.
 *
 * @author Glenn Renfro
 */
@EnableConfigurationProperties({JdbcHdfsDataSourceProperties.class})
public class JdbcHdfsDataSourceConfiguration {

	@Autowired
	private JdbcHdfsDataSourceProperties props;

	@Autowired
	private Environment environment;

	@Bean(name="taskDataSource")
	@Primary
	public DataSource taskDataSource() {
		return getDefaultDataSource();
	}


	@Bean(name="jdbchdfsDataSource")
	public DataSource jdbcHdfsDataSource() {
		DataSource dataSource;
		if(props.getUrl() != null && props.getUsername() != null) {
			dataSource = DataSourceBuilder.create().driverClassName(props.getDriverClassName())
				.url(props.getUrl())
				.username(props.getUsername())
				.password(props.getPassword()).build();
		} else {
			dataSource = getDefaultDataSource();
		}
		return dataSource;
	}

	private DataSource getDefaultDataSource() {
		return DataSourceBuilder.create().driverClassName(environment.getProperty("spring.datasource.driverClassName"))
				.url(environment.getProperty("spring.datasource.url"))
				.username(environment.getProperty("spring.datasource.username"))
				.password(environment.getProperty("spring.datasource.password")).build();
	}
}
