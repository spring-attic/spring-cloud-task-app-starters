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

package org.springframework.cloud.task.sqoop.common;

import javax.validation.constraints.AssertTrue;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Configuration properties to be used by the SqoopJobRunner.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class SqoopCommonTaskProperties {

	/**
	 * The Sqoop command to execute.
	 */
	private String command;

	/**
	 * The Sqoop connect string (JDBC URL) to use.
	 */
	private String connect;

	/**
	 * The database username to use.
	 */
	String username;

	/**
	 * The database password to use.
	 */
	String password;

	/**
	 * The database password-file to use.
	 */
	String passwordFile;

	/**
	 * The Sqoop specific job arguments as a complete String, passed as is to the command job.
	 */
	private String toolArgs;

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public String getConnect() {
		return connect;
	}

	public void setConnect(String connect) {
		this.connect = connect;
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

	public String getPasswordFile() {
		return passwordFile;
	}

	public void setPasswordFile(String passwordFile) {
		this.passwordFile = passwordFile;
	}

	public String getToolArgs() {
		return toolArgs;
	}

	public void setToolArgs(String toolArgs) {
		this.toolArgs = toolArgs;
	}

	@AssertTrue(message = "Unsupported command, the Sqoop Task currently only supports 'import' and 'export'.")
	private boolean isSupportedCommand() {
		return (StringUtils.isEmpty(command) || command.equals("import") || command.equals("export"));
	}
}
