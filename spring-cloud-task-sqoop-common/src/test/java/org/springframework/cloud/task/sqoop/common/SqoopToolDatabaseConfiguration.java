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

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.SocketUtils;

/**
 * @author Thomas Risberg
 */
@Configuration
public class SqoopToolDatabaseConfiguration {

	private static final Log logger = LogFactory.getLog(SqoopToolDatabaseConfiguration.class);

	@Bean(destroyMethod = "stop")
	public Server databaseServer() throws SQLException, IOException {
		DriverManager.registerDriver(new org.hsqldb.jdbcDriver());
		int hsqldbPort = SocketUtils.findAvailableTcpPort(10000);
		System.setProperty("db.server.port", Integer.toString(hsqldbPort));
		logger.info("Database is using port: " + Integer.toString(hsqldbPort));
		HsqlProperties configProps = new HsqlProperties();
		configProps.setProperty("server.port", hsqldbPort);
		configProps.setProperty("server.database.0", "file:target/db/test");
		configProps.setProperty("server.dbname.0", "test");
		Server server = new org.hsqldb.Server();
		server.setLogWriter(null);
		server.setErrWriter(null);
		server.setRestartOnShutdown(false);
		server.setNoSystemExit(true);
		server.setProperties(configProps);
		server.start();
		return server;
	}
}
