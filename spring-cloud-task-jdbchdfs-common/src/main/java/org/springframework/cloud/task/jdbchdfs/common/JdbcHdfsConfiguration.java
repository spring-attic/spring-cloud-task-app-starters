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


import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.deployer.resource.maven.MavenResource;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.util.StringUtils;

/**
 * Establishes the beans required to migrate the data from jdbc to hdfs.
 *
 * @author Glenn Renfro
 */

@EnableTask
@EnableBatchProcessing
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties({JdbcHdfsTaskProperties.class})
public class JdbcHdfsConfiguration {

	public static final String[] PROMOTION_LISTENER_KEYS = {"batch.incremental.maxId"};

	public static final String FS_DEFAULT_FS = "fs.defaultFS";
	public static final String RM_MANAGER_PRINCIPLE = "rm-manager-principal";
	public static final String NAMENODE_PRINCIPLE = "namenode-principal";
	public static final String USER_PRINCIPLE = "user-principal";
	public static final String USER_KEYTAB = "user-keytab";
	public static final String SECURITY_METHOD = "security-method";
	public static final String PROPERTIES_LOCATION = "properties-location";
	public static final String REGISTER_URL_HANDLER = "register-url-handler";

	@Autowired
	private Configuration hadoopConfiguration;

	@Autowired
	private JdbcHdfsTaskProperties props;

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	public JobRepository jobRepository;

	@Autowired(required = false)
	private ConfigurableApplicationContext context;

	@Value("${jdbc.hdfs.task.resource:org.springframework.cloud.task.app:"
			+ "jdbc-hdfs-local-task:1.0.0.BUILD-SNAPSHOT}")
	private String taskResource;

	@Bean
	@StepScope
	public NamedColumnJdbcItemReader namedColumnJdbcItemReader(
			@Value("#{stepExecutionContext['partClause']}") String partClause) throws Exception {
		NamedColumnJdbcItemReaderFactory namedColumnJdbcItemReaderFactory = new NamedColumnJdbcItemReaderFactory();
		namedColumnJdbcItemReaderFactory.setDataSource(this.dataSource);
		namedColumnJdbcItemReaderFactory.setPartitionClause(partClause);
		namedColumnJdbcItemReaderFactory.setTableName(props.getTableName());
		namedColumnJdbcItemReaderFactory.setColumnNames(props.getColumnNames());
		namedColumnJdbcItemReaderFactory.setSql(props.getSql());
		namedColumnJdbcItemReaderFactory.setFetchSize(props.getCommitInterval());
		namedColumnJdbcItemReaderFactory.setDelimiter(props.getDelimiter());
		namedColumnJdbcItemReaderFactory.afterPropertiesSet();
		return namedColumnJdbcItemReaderFactory.getObject();
	}

	@Bean
	public JobExplorerFactoryBean jobExplorer() {
		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
		jobExplorerFactoryBean.setDataSource(this.dataSource);
		return jobExplorerFactoryBean;
	}

	@Bean
	@Profile("master")
	public DeployerPartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer) throws Exception {
		MavenResource resource = MavenResource.parse(taskResource);

		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler(taskLauncher, jobExplorer, resource, "workerStep");

		Map<String, String> environmentProperties = new HashMap<>();
		environmentProperties.put("spring.profiles.active", "worker");

		partitionHandler.setEnvironmentProperties(environmentProperties);
		partitionHandler.setMaxWorkers(props.getMaxWorkers());

		return partitionHandler;
	}

	@Bean
	@StepScope
	@Profile("master")
	public IncrementalColumnRangePartitioner partitioner(JobExplorer jobExplorer, @Value("#{stepExecutionContext['overrideCheckColumnValue']}") Long overrideValue) {
		IncrementalColumnRangePartitioner partitioner = new IncrementalColumnRangePartitioner();
		partitioner.setTable(props.getTableName());
		partitioner.setColumn(props.getPartitionColumn());
		partitioner.setJobExplorer(jobExplorer);
		partitioner.setCheckColumn(props.getCheckColumn());
		partitioner.setOverrideValue(overrideValue);
		partitioner.setDataSource(this.dataSource);
		partitioner.setPartitions(props.getPartitions());
		return partitioner;
	}

	@Bean
	@Profile("worker")
	public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
		DeployerStepExecutionHandler handler = new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);

		return handler;
	}

	@Bean
	@StepScope
	public HdfsTextItemWriter writer(@Value("#{stepExecutionContext['partSuffix']}") String suffix) throws Exception {

		if (StringUtils.hasText(props.getFsUri())) {
			hadoopConfiguration.set(FS_DEFAULT_FS, props.getFsUri());
		}
		if (StringUtils.hasText(props.getRmManagerPrinciple())) {
			hadoopConfiguration.set(RM_MANAGER_PRINCIPLE, props.getRmManagerPrinciple());
		}
		if (StringUtils.hasText(props.getNameNodePrinciple())) {
			hadoopConfiguration.set(NAMENODE_PRINCIPLE, props.getNameNodePrinciple());
		}
		if (StringUtils.hasText(props.getUserPrinciple())) {
			hadoopConfiguration.set(USER_PRINCIPLE, props.getUserPrinciple());
		}
		if (StringUtils.hasText(props.getUserKeyTab())) {
			hadoopConfiguration.set(USER_KEYTAB, props.getUserKeyTab());
		}
		if (StringUtils.hasText(props.getSecurityMethod())) {
			hadoopConfiguration.set(SECURITY_METHOD, props.getSecurityMethod());
		}
		if (StringUtils.hasText(props.getPropertiesLocation())) {
			hadoopConfiguration.set(PROPERTIES_LOCATION, props.getPropertiesLocation());
		}
		if (StringUtils.hasText(props.getRegisterUrlHandler())) {
			hadoopConfiguration.set(REGISTER_URL_HANDLER, props.getRegisterUrlHandler());
		}

		HdfsTextItemWriter writer = new HdfsTextItemWriter();
		writer.setLineAggregator(new org.springframework.batch.item.file.transform.PassThroughLineAggregator());
		writer.setFileName(props.getFileName() + suffix);
		writer.setBasePath(props.getDirectory());
		writer.setFileExtension(props.getFileExtension());
		writer.setRolloverThresholdInBytes(props.getRollover());
		writer.setFsUri(props.getFsUri());
		writer.setConfiguration(hadoopConfiguration);

		return writer;
	}


	@Bean
	public ExecutionContextPromotionListener promotionListener() {
		ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
		executionContextPromotionListener.setKeys(PROMOTION_LISTENER_KEYS);
		return executionContextPromotionListener;
	}

	@Bean
	@Profile("master")
	public Step step1(PartitionHandler partitionHandler, ExecutionContextPromotionListener promotionListener,
			IncrementalColumnRangePartitioner partitioner) throws Exception {
		Step worker = workerStep();
		Step step1 = stepBuilderFactory.get("step1")
				.partitioner(worker.getName(), partitioner)
				.step(worker)
				.partitionHandler(partitionHandler)
				.listener(promotionListener)
				.listener(partitioner)
				.build();
		return step1;
	}

	@Bean
	public Step workerStep() throws Exception {
		Step step = stepBuilderFactory.get("workerStep")
				.chunk(props.getCommitInterval())
				.reader(namedColumnJdbcItemReader(null))
				.writer(writer(null))
				.build();
		return step;
	}

	@Bean
	@Profile("master")
	public Job partitionedJob(PartitionHandler partitionHandler, ExecutionContextPromotionListener promotionListener,
			 IncrementalColumnRangePartitioner partitioner) throws Exception {
		JobBuilder jobBuilder =  jobBuilderFactory.get("partitionedJob");
		if(!props.isRestartable()) {
			jobBuilder.preventRestart();
		}
		return jobBuilder.start(step1(partitionHandler, promotionListener, partitioner))
				.build();
	}

}
