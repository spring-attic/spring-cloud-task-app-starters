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

package org.springframework.cloud.task.app.sqoop.job;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.task.sqoop.common.SqoopCommonTaskProperties;

/**
 * Configuration properties to be used by the SqoopJobRunner.
 *
 * @author Thomas Risberg
 */
@ConfigurationProperties
public class SqoopJobTaskProperties extends SqoopCommonTaskProperties {

	/**
	 * The Sqoop job action to execute.
	 */
	private String action;

	/**
	 * The Sqoop job name to be used for this job action.
	 */
	private String jobName;

	/**
	 * The Sqoop Metastore URL to be used for this job action.
	 */
	private String metastoreUrl;

	/**
	 * The Sqoop Metastore username to be used for this job action.
	 */
	private String metastoreUsername;

	/**
	 * The Sqoop Metastore password to be used for this job action.
	 */
	private String metastorePassword;


	@NotNull
	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	@NotNull
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@NotNull
	public String getMetastoreUrl() {
		return metastoreUrl;
	}

	public void setMetastoreUrl(String metastoreUrl) {
		this.metastoreUrl = metastoreUrl;
	}

	public String getMetastoreUsername() {
		return metastoreUsername;
	}

	public void setMetastoreUsername(String metastoreUsername) {
		this.metastoreUsername = metastoreUsername;
	}

	public String getMetastorePassword() {
		return metastorePassword;
	}

	public void setMetastorePassword(String metastorePassword) {
		this.metastorePassword = metastorePassword;
	}
}
