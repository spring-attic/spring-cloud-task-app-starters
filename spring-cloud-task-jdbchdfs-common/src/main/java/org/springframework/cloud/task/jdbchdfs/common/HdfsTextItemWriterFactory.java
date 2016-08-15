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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.store.output.OutputStreamWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;
import org.springframework.util.StringUtils;

/**
 * Factory for configuring a {@link org.springframework.cloud.task.jdbchdfs.common.HdfsTextItemWriter}.  This factory
 * will establish the strategies required for the writer.  Specifically, the name and rollover strategy as well as
 * establish the temporary extension.
 *
 * @author Glenn Renfro
 */
public class HdfsTextItemWriterFactory implements FactoryBean<HdfsTextItemWriter>, InitializingBean {

	public static final String RM_MANAGER_PRINCIPAL = "rm-manager-principal";
	public static final String NAMENODE_PRINCIPAL = "namenode-principal";
	public static final String USER_PRINCIPAL = "user-principal";
	public static final String USER_KEYTAB = "user-keytab";
	public static final String SECURITY_METHOD = "security-method";
	public static final String PROPERTIES_LOCATION = "properties-location";
	public static final String REGISTER_URL_HANDLER = "register-url-handler";

	private HdfsTextItemWriter hdfsTextItemWriter;

	public HdfsTextItemWriterFactory(Configuration configuration, JdbcHdfsTaskProperties props,
			String partitionSuffix ) throws Exception{
		List<FileNamingStrategy> strategies = new ArrayList<>();
		strategies.add(new StaticFileNamingStrategy(props.getFileName() + partitionSuffix));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy(props.getFileExtension(), "."));
		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.setStrategies(strategies);
		RolloverStrategy rolloverStrategy = new SizeRolloverStrategy(props.getRollover());
		Path baseDirPath = new Path(props.getDirectory());
		setupConfiguration(configuration, props);
		OutputStreamWriter writer = new OutputStreamWriter(configuration, baseDirPath, null);
		writer.setInWritingSuffix(".tmp");
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setRolloverStrategy(rolloverStrategy);
		hdfsTextItemWriter = new HdfsTextItemWriter();
		hdfsTextItemWriter.setLineAggregator(new org.springframework.batch.item.file.transform.PassThroughLineAggregator());
		hdfsTextItemWriter.setStoreWriter(writer);
		if (writer instanceof InitializingBean) {
			((InitializingBean) writer).afterPropertiesSet();
		}

	}

	private void setupConfiguration(Configuration configuration, JdbcHdfsTaskProperties props) {
		if (StringUtils.hasText(props.getFsUri())) {
			configuration.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, props.getFsUri());
		}
		if (StringUtils.hasText(props.getRmManagerPrincipal())) {
			configuration.set(RM_MANAGER_PRINCIPAL, props.getRmManagerPrincipal());
		}
		if (StringUtils.hasText(props.getNameNodePrincipal())) {
			configuration.set(NAMENODE_PRINCIPAL, props.getNameNodePrincipal());
		}
		if (StringUtils.hasText(props.getUserPrincipal())) {
			configuration.set(USER_PRINCIPAL, props.getUserPrincipal());
		}
		if (StringUtils.hasText(props.getUserKeyTab())) {
			configuration.set(USER_KEYTAB, props.getUserKeyTab());
		}
		if (StringUtils.hasText(props.getSecurityMethod())) {
			configuration.set(SECURITY_METHOD, props.getSecurityMethod());
		}
		if (StringUtils.hasText(props.getPropertiesLocation())) {
			configuration.set(PROPERTIES_LOCATION, props.getPropertiesLocation());
		}
		if (StringUtils.hasText(props.getRegisterUrlHandler())) {
			configuration.set(REGISTER_URL_HANDLER, props.getRegisterUrlHandler());
		}
	}

	@Override
	public HdfsTextItemWriter getObject() throws Exception {
		return hdfsTextItemWriter;
	}

	@Override
	public Class<?> getObjectType() {
		return HdfsTextItemWriter.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
	}
}
