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
@ConfigurationProperties("jdbchdfs.db")
public class JdbcHdfsDataSourceProperties {

	/**
	 * The url of the datasource that will be used by spring cloud task to record task and job information.
	 */
	private String task_datasource_url;

	/**
	 * The driver of the datasource that will be used by spring cloud task to record task and job information.
	 */
	private String task_datasource_driverClassName;

	/**
	 * The username of the datasource that will be used by spring cloud task to record task and job information.
	 */
	private String task_datasource_username;

	/**
	 * The password of the datasource that will be used by spring cloud task to record task and job information.
	 */
	private String task_datasource_password;

	/**
	 * The url of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String jdbchdfs_datasource_url;

	/**
	 * The driver of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String jdbchdfs_datasource_driverClassName;

	/**
	 * The username of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String jdbchdfs_datasource_username;

	/**
	 * The password of the datasource that will be used by jdbhdfs app to retrieve table input.
	 */
	private String jdbchdfs_datasource_password;

	public String getTask_datasource_url() {
		return task_datasource_url;
	}

	public void setTask_datasource_url(String task_datasource_url) {
		this.task_datasource_url = task_datasource_url;
	}

	public String getTask_datasource_driverClassName() {
		return task_datasource_driverClassName;
	}

	public void setTask_datasource_driverClassName(String task_datasource_driverClassName) {
		this.task_datasource_driverClassName = task_datasource_driverClassName;
	}

	public String getTask_datasource_username() {
		return task_datasource_username;
	}

	public void setTask_datasource_username(String task_datasource_username) {
		this.task_datasource_username = task_datasource_username;
	}

	public String getTask_datasource_password() {
		return task_datasource_password;
	}

	public void setTask_datasource_password(String task_datasource_password) {
		this.task_datasource_password = task_datasource_password;
	}

	public String getJdbchdfs_datasource_url() {
		return jdbchdfs_datasource_url;
	}

	public void setJdbchdfs_datasource_url(String jdbchdfs_datasource_url) {
		this.jdbchdfs_datasource_url = jdbchdfs_datasource_url;
	}

	public String getJdbchdfs_datasource_driverClassName() {
		return jdbchdfs_datasource_driverClassName;
	}

	public void setJdbchdfs_datasource_driverClassName(String jdbchdfs_datasource_driverClassName) {
		this.jdbchdfs_datasource_driverClassName = jdbchdfs_datasource_driverClassName;
	}

	public String getJdbchdfs_datasource_username() {
		return jdbchdfs_datasource_username;
	}

	public void setJdbchdfs_datasource_username(String jdbchdfs_datasource_username) {
		this.jdbchdfs_datasource_username = jdbchdfs_datasource_username;
	}

	public String getJdbchdfs_datasource_password() {
		return jdbchdfs_datasource_password;
	}

	public void setJdbchdfs_datasource_password(String jdbchdfs_datasource_password) {
		this.jdbchdfs_datasource_password = jdbchdfs_datasource_password;
	}
}
