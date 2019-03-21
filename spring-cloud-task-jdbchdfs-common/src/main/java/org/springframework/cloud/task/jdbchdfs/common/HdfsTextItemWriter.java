/*
 * Copyright 2016 the original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.WriteFailedException;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.data.hadoop.store.DataStoreWriter;
import org.springframework.data.hadoop.store.output.OutputStreamWriter;

/**
 * Writes items as a byte array to a destination as specified by the fsURI.
 *
 * @author Glenn Renfro
 */
public class HdfsTextItemWriter<T> extends AbstractItemStreamItemWriter<T> {

	private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

	private volatile String charset = "UTF-8";

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private volatile DataStoreWriter<byte[]> storeWriter;

	private LineAggregator<T> lineAggregator;

	private String lineSeparator = DEFAULT_LINE_SEPARATOR;

	/**
	 * Aggregates the items to a single string and writes the data out using {@link OutputStreamWriter} as a byte array.
	 *
	 * @param list of objects that will be written to the specified fsUri.
	 */
	@Override
	public void write(List list) throws Exception {
		storeWriter.write(getItemsAsByte(list));
	}

	@Override
	public void update(ExecutionContext executionContext) {
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
				storeWriter.flush();
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

	/**
	 * Public setter for the {@link LineAggregator}. This will be used to translate the item into a line for output.
	 *
	 * @param lineAggregator the {@link LineAggregator} to set
	 */
	public void setLineAggregator(LineAggregator<T> lineAggregator) {
		this.lineAggregator = lineAggregator;
	}

	public DataStoreWriter<byte[]> getStoreWriter() {
		return storeWriter;
	}

	public void setStoreWriter(DataStoreWriter<byte[]> storeWriter) {
		this.storeWriter = storeWriter;
	}
}
