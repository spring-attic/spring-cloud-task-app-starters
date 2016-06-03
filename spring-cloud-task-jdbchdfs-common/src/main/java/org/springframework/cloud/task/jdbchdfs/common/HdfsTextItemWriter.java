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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.output.OutputStreamWriter;
import org.springframework.data.hadoop.store.strategy.naming.ChainedFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.FileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.RollingFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.naming.StaticFileNamingStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.RolloverStrategy;
import org.springframework.data.hadoop.store.strategy.rollover.SizeRolloverStrategy;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Writes items as a byte array to a destination as specified by the fsURI.
 *
 * @author Glenn Renfro
 */
public class HdfsTextItemWriter<T> extends AbstractItemStreamItemWriter<T> implements InitializingBean {

	private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

	public static final String DEFAULT_FILENAME = "data";

	public static final String DEFAULT_BASE_PATH = "/data/";

	public static final String DEFAULT_FILE_EXTENSION = "log";

	public static long DEFAULT_ROLLOVER_THRESHOLD_IN_BYTES = 10 * 1024 * 1024; // 10MB

	private volatile String charset = "UTF-8";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private String fileName = DEFAULT_FILENAME;

	private String basePath = DEFAULT_BASE_PATH;

	private String fileExtension = DEFAULT_FILE_EXTENSION;

	private long rolloverThresholdInBytes = DEFAULT_ROLLOVER_THRESHOLD_IN_BYTES;

	private String fsUri;

	private volatile Configuration configuration;

	private volatile DataStoreWriter<byte[]> storeWriter;

	private LineAggregator<T> lineAggregator;

	private String lineSeparator = DEFAULT_LINE_SEPARATOR;

	public void afterPropertiesSet() throws Exception {
		Assert.notNull(lineAggregator, "A LineAggregator must be provided.");
		Configuration configurationToUse = null;
		if (StringUtils.hasText(fsUri)) {
			configurationToUse = new Configuration(configuration);
			configurationToUse.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, fsUri);
		}
		else {
			configurationToUse = configuration;
		}

		List<FileNamingStrategy> strategies = new ArrayList<>();
		strategies.add(new StaticFileNamingStrategy(fileName));
		strategies.add(new RollingFileNamingStrategy());
		strategies.add(new StaticFileNamingStrategy(fileExtension, "."));
		ChainedFileNamingStrategy fileNamingStrategy = new ChainedFileNamingStrategy();
		fileNamingStrategy.setStrategies(strategies);
		RolloverStrategy rolloverStrategy = new SizeRolloverStrategy(rolloverThresholdInBytes);
		Path baseDirPath = new Path(basePath);
		OutputStreamWriter writer = new OutputStreamWriter(configurationToUse, baseDirPath, null);
		writer.setInWritingSuffix(".tmp");
		writer.setFileNamingStrategy(fileNamingStrategy);
		writer.setRolloverStrategy(rolloverStrategy);
		storeWriter = writer;
		if (storeWriter instanceof InitializingBean) {
			((InitializingBean) storeWriter).afterPropertiesSet();
		}
	}


	@Override
	/**
	 * Aggregates the items to a single string and writes the data out using {@link OutputStreamWriter} as a byte array.
	 *
	 * @param list of objects that will be written to the specified fsUri.
	 */
	public void write(List list) throws Exception {
		storeWriter.write(getItemsAsByte(list));
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		logger.debug("Flushing store writer");
		if (storeWriter != null) {
			try {
				storeWriter.flush();
			}
			catch (IOException e) {
				throw new IllegalStateException("Error while flushing store writer", e);
			}
		}
	}

	@Override
	public void close() {
		logger.debug("Closing item writer");
		if (storeWriter != null) {
			try {
				storeWriter.close();
			}
			catch (IOException e) {
				throw new IllegalStateException("Error while closing writer", e);
			}
			finally {
				storeWriter = null;
			}
		}
	}

	/**
	 * Converts the list of items to a byte array.
	 *
	 * @param items
	 * @return string of items
	 */
	private byte[] getItemsAsByte(List<? extends T> items) {

		StringBuilder lines = new StringBuilder();
		for (T item : items) {
			lines.append(lineAggregator.aggregate(item) + lineSeparator);
		}
		try {
			return lines.toString().getBytes(this.charset);
		}
		catch (UnsupportedEncodingException e) {
			throw new WriteFailedException("Could not write data.", e);
		}
	}


	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	public String getFileExtension() {
		return fileExtension;
	}

	public void setFileExtension(String fileExtension) {
		this.fileExtension = fileExtension;
	}

	public long getRolloverThresholdInBytes() {
		return rolloverThresholdInBytes;
	}

	public void setRolloverThresholdInBytes(long rolloverThresholdInBytes) {
		this.rolloverThresholdInBytes = rolloverThresholdInBytes;
	}

	public String getFsUri() {
		return fsUri;
	}

	public void setFsUri(String fsUri) {
		this.fsUri = fsUri;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	/**
	 * Public setter for the {@link LineAggregator}. This will be used to translate the item into a line for output.
	 *
	 * @param lineAggregator the {@link LineAggregator} to set
	 */
	public void setLineAggregator(LineAggregator<T> lineAggregator) {
		this.lineAggregator = lineAggregator;
	}
}
