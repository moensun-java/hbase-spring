/*
 * Copyright 2011-2015 the original author or authors.
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
package com.moensun.habse.spring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Central class for accessing the HBase API. Simplifies the use of HBase and helps to avoid common errors.
 * It executes core HBase workflow, leaving application code to invoke actions and extract results.
 *
 * @author Costin Leau
 * @author Shaun Elliott
 */
public class HbaseTemplate extends HbaseAccessor implements HbaseOperations {
	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseTemplate.class);

	private volatile Connection connection;

	public HbaseTemplate(Configuration configuration) {
		this.setConfiguration(configuration);
		Assert.notNull(configuration, " a valid configuration is required");
	}


	@Override
	public <T> List<T> find(String tableName, String family, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.setCaching(defaultCaching);
		scan.addFamily(Bytes.toBytes(family));
		return this.find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) {
		Scan scan = new Scan();
		scan.setCaching(defaultCaching);
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return this.find(tableName, scan, action);
	}

	@Override
	public <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) {
		return this.execute(tableName, table -> {
			int caching = scan.getCaching();
			if (caching == 1) {
				scan.setCaching(defaultCaching);
			}
			try (ResultScanner scanner = table.getScanner(scan)) {
				List<T> rs = new ArrayList<T>();
				int rowNum = 0;
				for (Result result : scanner) {
					rs.add(action.mapRow(result, rowNum++));
				}
				return rs;
			}
		});
	}

	@Override
	public <T> T find(String tableName, String family, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.setCaching(defaultCaching);
		scan.addFamily(family.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action) {
		Scan scan = new Scan();
		scan.setCaching(defaultCaching);
		scan.addColumn(family.getBytes(getCharset()), qualifier.getBytes(getCharset()));
		return find(tableName, scan, action);
	}

	@Override
	public <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) {
		return execute(tableName, table -> {
			scan.setFilter(new KeyOnlyFilter());
			try (ResultScanner rs = table.getScanner(scan)) {
				return action.extractData(rs);
			}
		});
	}

	private void warmUpConnectionCache(Connection connection, TableName tn) throws IOException {
		try (RegionLocator locator = connection.getRegionLocator(tn)) {
			LOGGER.info("为表 {} 预热 区域位置缓存{}", tn, locator.getAllRegionLocations().size());
		}
	}


	@Override
	public <T> T get(String tableName, String rowName, final RowMapper<T> mapper) {
		return this.get(tableName, rowName, null, null, mapper);
	}

	@Override
	public <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) {
		return this.get(tableName, rowName, familyName, null, mapper);
	}

	@Override
	public <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) {
		return this.execute(tableName, table -> {
			Get get = new Get(Bytes.toBytes(rowName));
			if (StringUtils.isNotBlank(familyName)) {
				byte[] family = Bytes.toBytes(familyName);
				if (StringUtils.isNotBlank(qualifier)) {
					get.addColumn(family, Bytes.toBytes(qualifier));
				} else {
					get.addFamily(family);
				}
			}
			Result result = table.get(get);
			return mapper.mapRow(result, 0);
		});
	}

	@Override
	public void execute(String tableName, MutatorCallback action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");
		final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
			for (int i = 0; i < e.getNumExceptions(); i++) {
				LOGGER.info("Failed to sent put " + e.getRow(i) + ".");
			}
		};
		Connection connection = this.getConnection();
		TableName fn = TableName.valueOf(tableName);
		BufferedMutatorParams mutatorParams = new BufferedMutatorParams(fn).listener(listener);
		try (final BufferedMutator mutator = connection.getBufferedMutator(mutatorParams)) {
			action.doInMutator(mutator);
		} catch (Throwable throwable) {
			throw new HbaseSystemException(throwable);
		}
	}

	@Override
	public <T> T execute(String tableName, TableCallback<T> action) {
		Assert.notNull(action, "Callback object must not be null");
		Assert.notNull(tableName, "No table specified");

		try (Table table = getConnection().getTable(TableName.valueOf(tableName))) {
			return action.doInTable(table);
		} catch (Throwable throwable) {
			throw new HbaseSystemException(throwable);
		}
	}

	@Override
	public void saveOrUpdate(String tableName, final Mutation mutation) {
		this.execute(tableName, mutator -> {
			mutator.mutate(mutation);
		});
	}

	@Override
	public void saveOrUpdates(String tableName, final List<Mutation> mutations) {
		this.execute(tableName, mutator -> {
			mutator.mutate(mutations);
		});
	}

	@Override
	public void put(String tableName, final String rowKey, final String familyName, final String qualifier, final byte[] value) {
		Assert.hasLength(rowKey, "rowKey cant not be null ");
		Assert.hasLength(familyName, "familyName can not be null ");
		Assert.hasLength(qualifier, "qualifier cat not be null ");
		Assert.notNull(value, "value can not be null ");
		execute(tableName, table -> {
			Put put = new Put(rowKey.getBytes(getCharset()));
			put.addColumn(familyName.getBytes(getCharset()), qualifier.getBytes(getCharset()), value);
			table.put(put);
			return null;
		});
	}

	@Override
	public void put(String tableName, String rowKey, String familyName, Map<String, byte[]> cellDate) {
		Assert.hasLength(rowKey, "rowKey cant not be null ");
		Assert.hasLength(familyName, "familyName can not be null ");
		Assert.notNull(cellDate, "cellDate can not be null ");
		execute(tableName, mutator -> {
			List<Put> puts = new ArrayList<>();
			cellDate.forEach((s, bytes) -> {
				Put p = new Put(Bytes.toBytes(rowKey));
				p.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(s), bytes);
				puts.add(p);
			});
			mutator.mutate(puts);
		});
	}

	@Override
	public void delete(String tableName, final String rowName, final String familyName) {
		delete(tableName, rowName, familyName, null);
	}

	@Override
	public void delete(String tableName, final String rowKey, final String familyName, final String qualifier) {
		Assert.hasLength(rowKey, "rowKey cant not be null ");
		Assert.hasLength(familyName, "familyName can not be null ");
		execute(tableName, mutator -> {
			Delete delete = new Delete(rowKey.getBytes(getCharset()));
			byte[] family = familyName.getBytes(getCharset());
			if (qualifier != null) {
				delete.addColumn(family, qualifier.getBytes(getCharset()));
			} else {
				delete.addFamily(family);
			}
			mutator.mutate(delete);
		});
	}

	public Connection getConnection() {
		if (null == this.connection) {
			synchronized (this) {
				if (null == this.connection) {
					try {
						ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(
								Runtime.getRuntime().availableProcessors() * 8,
								Integer.MAX_VALUE,
								60L,
								TimeUnit.SECONDS,
								new SynchronousQueue<>());
						// init pool
						poolExecutor.prestartCoreThread();
						this.connection = ConnectionFactory.createConnection(getConfiguration(), poolExecutor);
					} catch (IOException e) {
						LOGGER.error(" connection资源池创建失败", e);
					}
				}
			}
		}
		return this.connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
}