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
 * @author Glenn Renfro
 */
@ConfigurationProperties
public class JdbcHdfsTaskProperties {

	public static final String DEFAULT_FS_URI = "hdfs://localhost:8020";

	public static final int DEFAULT_PARTITION_COUNT = 4;

	public static final String DEFAULT_FILE_NAME = "jdbchdfs";

	public static final long DEFAULT_ROLLOVER = 1000000000;

	public static final String DEFAULT_DIRECTORY = "/data";

	public static final String DEFAULT_FILE_EXTENSION = "csv";

	public static final String DEFAULT_DELIMITER = ",";

	public static final int DEFAULT_COMMIT_INTERVAL = 1000;

	private String fsUri = DEFAULT_FS_URI;

	private String propertiesLocation;

	private String securityMethod;

	private String userKeyTab;

	private String userPrinciple;

	private String nameNodePrinciple;

	private String rmManagerPrinciple;

	private String registerUrlHandler;

	private int partitions = DEFAULT_PARTITION_COUNT;

	private String fileName = DEFAULT_FILE_NAME;

	private long rollover = DEFAULT_ROLLOVER;

	private String directory = DEFAULT_DIRECTORY;

	private String fileExtension = DEFAULT_FILE_EXTENSION;

	private String tableName;

	private String columnNames;

	private String sql;

	private int commitInterval = DEFAULT_COMMIT_INTERVAL;

	private String delimiter = DEFAULT_DELIMITER;

	private String partitionColumn;

	private String checkColumn;

	private boolean restartable;

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

	public String getUserPrinciple() {
		return userPrinciple;
	}

	public void setUserPrinciple(String userPrinciple) {
		this.userPrinciple = userPrinciple;
	}

	public String getNameNodePrinciple() {
		return nameNodePrinciple;
	}

	public void setNameNodePrinciple(String nameNodePrinciple) {
		this.nameNodePrinciple = nameNodePrinciple;
	}

	public String getRmManagerPrinciple() {
		return rmManagerPrinciple;
	}

	public void setRmManagerPrinciple(String rmManagerPrinciple) {
		this.rmManagerPrinciple = rmManagerPrinciple;
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
}
