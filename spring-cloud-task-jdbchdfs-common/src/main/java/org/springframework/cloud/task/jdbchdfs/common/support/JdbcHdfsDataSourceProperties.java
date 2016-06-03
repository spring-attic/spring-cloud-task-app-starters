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

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Establishes the Configuration Properties for the JdbcHdfs task datasources.
 * @author Glenn Renfro
 */
@ConfigurationProperties("jdbchdfs.datasource")
public class JdbcHdfsDataSourceProperties {

	/**
	 * The url of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String url;

	/**
	 * The driver of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String driverClassName;

	/**
	 * The username of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String username;

	/**
	 * The password of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String password;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
