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

package org.springframework.cloud.task.jdbchdfs.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Establishes the Configuration Properties for the JdbcHdfs task.
 * @author Glenn Renfro
 */
@ConfigurationProperties("jdbchdfs")
public class JdbcHdfsTaskProperties {

	public static final String DEFAULT_FS_URI = "hdfs://localhost:8020";

	public static final int DEFAULT_PARTITION_COUNT = 4;

	public static final String DEFAULT_FILE_NAME = "jdbchdfs";

	public static final long DEFAULT_ROLLOVER = 1000000000;

	public static final String DEFAULT_DIRECTORY = "/data";

	public static final String DEFAULT_FILE_EXTENSION = "csv";

	public static final String DEFAULT_DELIMITER = ",";

	public static final int DEFAULT_COMMIT_INTERVAL = 1000;

	public static final int DEFAULT_MAX_WORKERS = 2;

	/**
	 * The URI to the hadoop file system.
	 */
	private String fsUri = DEFAULT_FS_URI;

	/**
	 * The properties location to be used.
	 */
	private String propertiesLocation;

	/**
	 * The security method to be used.
	 */
	private String securityMethod;

	/**
	 * The user key tab to be used.
	 */
	private String userKeyTab;

	/**
	 * The user principal to be used.
	 */
	private String userPrincipal;

	/**
	 * The name node principal to be used.
	 */
	private String nameNodePrincipal;

	/**
	 * The rm manager principal to be used.
	 */
	private String rmManagerPrincipal;

	/**
	 * The register url handler to be used.
	 */
	private String registerUrlHandler;

	/**
	 * The number of partitions to be created.
	 */
	private int partitions = DEFAULT_PARTITION_COUNT;

	/**
	 * The name of the file to be written to the file system.
	 */
	private String fileName = DEFAULT_FILE_NAME;

	/**
	 * The number of bytes to be written before file rolls over.
	 */
	private long rollover = DEFAULT_ROLLOVER;

	/**
	 * The directory the files are to be written.
	 */
	private String directory = DEFAULT_DIRECTORY;

	/**
	 * The extension that will be applied to the files.
	 */
	private String fileExtension = DEFAULT_FILE_EXTENSION;

	/**
	 * The name of the table to be queried.
	 */
	private String tableName;

	/**
	 * The name of the columns to be queried.
	 */
	private String columnNames;

	/**
	 * Sql to be used to retrieve the data.
	 */
	private String sql;

	/**
	 * The commit interval for the application.
	 */
	private int commitInterval = DEFAULT_COMMIT_INTERVAL;

	/**
	 * The delimiter used to split the columns of data in the output file.
	 */
	private String delimiter = DEFAULT_DELIMITER;

	/**
	 * The name of the column used to partition the data.
	 */
	private String partitionColumn;

	/**
	 * The name of the column used to determine if the data should be read.
	 */
	private String checkColumn;

	/**
	 * Is the batch job restartable.
	 */
	private boolean restartable;

	/**
	 * Maximum number of concurrent workers.
	 */
	private int maxWorkers = DEFAULT_MAX_WORKERS;

	public String getFsUri() {
		return fsUri;
	}

	public void setFsUri(String fsUri) {
		this.fsUri = fsUri;
	}

	public String getPropertiesLocation() {
		return propertiesLocation;
	}

	public void setPropertiesLocation(String propertiesLocation) {
		this.propertiesLocation = propertiesLocation;
	}

	public String getSecurityMethod() {
		return securityMethod;
	}

	public void setSecurityMethod(String securityMethod) {
		this.securityMethod = securityMethod;
	}

	public String getUserKeyTab() {
		return userKeyTab;
	}

	public void setUserKeyTab(String userKeyTab) {
		this.userKeyTab = userKeyTab;
	}

	public String getUserPrincipal() {
		return userPrincipal;
	}

	public void setUserPrincipal(String userPrincipal) {
		this.userPrincipal = userPrincipal;
	}

	public String getNameNodePrincipal() {
		return nameNodePrincipal;
	}

	public void setNameNodePrincipal(String nameNodePrincipal) {
		this.nameNodePrincipal = nameNodePrincipal;
	}

	public String getRmManagerPrincipal() {
		return rmManagerPrincipal;
	}

	public void setRmManagerPrincipal(String rmManagerPrincipal) {
		this.rmManagerPrincipal = rmManagerPrincipal;
	}

	public String getRegisterUrlHandler() {
		return registerUrlHandler;
	}

	public void setRegisterUrlHandler(String registerUrlHandler) {
		this.registerUrlHandler = registerUrlHandler;
	}

	public int getPartitions() {
		return partitions;
	}

	public void setPartitions(int partitions) {
		this.partitions = partitions;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getDirectory() {
		return directory;
	}

	public void setDirectory(String directory) {
		this.directory = directory;
	}

	public String getFileExtension() {
		return fileExtension;
	}

	public void setFileExtension(String fileExtension) {
		this.fileExtension = fileExtension;
	}

	public long getRollover() {
		return rollover;
	}

	public void setRollover(long rollover) {
		this.rollover = rollover;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(String columnNames) {
		this.columnNames = columnNames;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public int getCommitInterval() {
		return commitInterval;
	}

	public void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public String getPartitionColumn() {
		return partitionColumn;
	}

	public void setPartitionColumn(String partitionColumn) {
		this.partitionColumn = partitionColumn;
	}

	public String getCheckColumn() {
		return checkColumn;
	}

	public void setCheckColumn(String checkColumn) {
		this.checkColumn = checkColumn;
	}

	public boolean isRestartable() {
		return restartable;
	}

	public void setRestartable(boolean restartable) {
		this.restartable = restartable;
	}

	public int getMaxWorkers() {
		return maxWorkers;
	}

	public void setMaxWorkers(int maxWorkers) {
		this.maxWorkers = maxWorkers;
	}
}
