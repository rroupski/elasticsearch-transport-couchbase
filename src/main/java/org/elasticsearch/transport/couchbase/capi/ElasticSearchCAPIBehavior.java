/**
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.elasticsearch.transport.couchbase.capi;

import com.couchbase.capi.CAPIBehavior;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.metrics.MeanMetric;

import javax.servlet.UnavailableException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ElasticSearchCAPIBehavior implements CAPIBehavior
{
	private final static String TooManyRequest = "Too many concurrent requests";

	private final ESLogger logger;

	private final Map<String, String> buckets;

	private final AtomicInteger activeRevsDiffRequests;
	private final MeanMetric meanRevsDiffRequests;

	private final AtomicInteger activeBulkDocsRequests;
	private final MeanMetric meanBulkDocsRequests;

	private final AtomicInteger totalTooManyConcurrentRequestsErrors;

	private final Map<String, ElasticIndex> indexes;
	private final int maxConcurrentRequests;

	public ElasticSearchCAPIBehavior(
		final Client client,
		final ESLogger logger,
		final Map<String, String> buckets,
		final int maxConcurrentRequests,
		final Map<String, BucketContext> contexts)
	{
		this.logger = logger;
		this.buckets = buckets;
		this.maxConcurrentRequests = maxConcurrentRequests;

		this.activeRevsDiffRequests = new AtomicInteger();
		this.meanRevsDiffRequests = new MeanMetric();

		this.activeBulkDocsRequests = new AtomicInteger();
		this.meanBulkDocsRequests = new MeanMetric();

		this.totalTooManyConcurrentRequestsErrors = new AtomicInteger();
		this.indexes = createIndexes(client, logger, contexts);

		logger.info("couchbase plugin loaded");
	}

	@Override
	public Map<String, Object> welcome()
	{
		logger.info("welcome");

		final Map<String, Object> responseMap = new HashMap<>();
		responseMap.put("welcome", "elasticsearch-transport-couchbase");
		return responseMap;
	}

	@Override
	public String databaseExists(final String database)
	{
		if (logger.isDebugEnabled())
			logger.debug("databaseExists({})", database);

		final ElasticIndex index = elasticIndex(database);

		return index != null && index.exists() ? null : "missing";
	}

	@Override
	public Map<String, Object> getDatabaseDetails(final String database)
	{
		if (logger.isDebugEnabled())
			logger.debug("getDatabaseDetails({})", database);

		final String doesNotExistReason = databaseExists(database);
		if (doesNotExistReason == null)
		{
			final Map<String, Object> responseMap = new HashMap<>();
			responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
			return responseMap;
		}
		return null;
	}

	@Override
	public boolean createDatabase(final String database)
	{
		logger.warn("createDatabase({})", database);
		throw new UnsupportedOperationException("Creating indexes is not supported");
	}

	@Override
	public boolean deleteDatabase(final String database)
	{
		logger.warn("deleteDatabase({})", database);
		throw new UnsupportedOperationException("Deleting indexes is not supported");
	}

	@Override
	public boolean ensureFullCommit(final String database)
	{
		if (logger.isInfoEnabled())
			logger.info("deleteDatabase({})", database);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> revsDiff(
		final String database, final Map<String, Object> revsMap) throws UnavailableException
	{
		if (logger.isDebugEnabled())
			logger.debug("revsDiff({}, {})", database, revsMap);

		final long start = start();
		try
		{
			activeRevsDiffRequests.getAndIncrement();
			return elasticIndex(database).diff(revsMap);
		}
		finally
		{
			finish(start);
			activeRevsDiffRequests.getAndDecrement();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object> bulkDocs(final String database, final List<Map<String, Object>> docs)
		throws UnavailableException
	{
		if (logger.isDebugEnabled())
			logger.debug("bulkDocs({}, size={})", database, docs.size());

		final long start = start();
		try
		{
			activeBulkDocsRequests.getAndIncrement();
			return elasticIndex(database).index(docs);
		}
		finally
		{
			finish(start);
			activeBulkDocsRequests.getAndDecrement();
		}
	}

	@Override
	public Map<String, Object> getDocument(final String database, final String docId)
	{
		if (logger.isInfoEnabled())
			logger.info("getDocument({}, {})", database, docId);

		return elasticIndex(database).getDocument(docId);
	}

	@Override
	public String storeDocument(
		final String database, final String docId, final Map<String, Object> document)
	{
		if (logger.isInfoEnabled())
		{
			if (logger.isDebugEnabled())
				logger.debug("storeDocument({}, {}, {})", database, docId, document);
			else
				logger.info("storeDocument({}, {})", database, docId);
		}

		return elasticIndex(database).storeDocument(docId, document);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getLocalDocument(final String database, final String docId)
	{
		if (logger.isInfoEnabled())
			logger.info("getLocalDocument({}, {})", database, docId);

		return elasticIndex(database).getDocument(docId);
	}

	@Override
	public String storeLocalDocument(
		final String database, final String docId, final Map<String, Object> document)
	{
		if (logger.isInfoEnabled())
		{
			if (logger.isDebugEnabled())
				logger.debug("storeLocalDocument({}, {}, {})", database, docId, document);
			else
				logger.info("storeLocalDocument({}, {})", database, docId);
		}

		return elasticIndex(database).storeDocument(docId, document);
	}

	@Override
	public InputStream getAttachment(final String database, final String docId, final String attachmentName)
	{
		logger.warn("getAttachment({}, {}, {})", database, docId, attachmentName);
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public String storeAttachment(
		final String database, final String docId,
		final String attachmentName, final String contentType, final InputStream input)
	{
		logger.warn("storeAttachment({}, {}, {})", database, docId, attachmentName);
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public InputStream getLocalAttachment(
		final String database, final String docId, final String attachmentName)
	{
		logger.warn("getLocalAttachment({}, {}, {})", database, docId, attachmentName);
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public String storeLocalAttachment(
		final String database, final String docId,
		final String attachmentName, final String contentType, final InputStream input)
	{
		logger.warn("storeLocalAttachment({}, {}, {})", database, docId, attachmentName);
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	private static Map<String, ElasticIndex> createIndexes(
		final Client client,
		final ESLogger logger,
		final Map<String, BucketContext> contexts)
	{
		final Map<String, ElasticIndex> indexes = new HashMap<>();

		for (final Map.Entry<String, BucketContext> i : contexts.entrySet())
		{
			final String name = i.getKey();
			indexes.put(name, ElasticIndex.create(name, i.getValue(), client, logger));
		}

		return indexes;
	}

	private final ElasticIndex elasticIndex(final String database)
	{
		return indexes.get(baseIndexName(database));
	}

	private final long start() throws UnavailableException
	{
		// check to see if too many requests are already active
		if (activeBulkDocsRequests.get() + activeRevsDiffRequests.get() >= maxConcurrentRequests)
		{
			totalTooManyConcurrentRequestsErrors.getAndIncrement();
			logger.warn(TooManyRequest);
			throw new UnavailableException(TooManyRequest);
		}

		return System.currentTimeMillis();
	}

	private final void finish(final long start)
	{
		meanRevsDiffRequests.inc(System.currentTimeMillis() - start);
	}

	private static String getDatabaseNameWithoutUUID(final String database)
	{
		final int pos = database.indexOf(';');
		return pos >= 0 ? database.substring(0, pos) : database;
	}

	private static final String baseIndexName(final String database)
	{
		final int pos = database.indexOf('/');
		return pos >= 0 ? database.substring(0, pos) : database;
	}

	@Override
	public Map<String, Object> getStats()
	{
		logger.info("getStats");

		final Map<String, Object> stats = new HashMap<>();

		final Map<String, Object> bulkDocsStats = new HashMap<>();
		bulkDocsStats.put("activeCount", activeBulkDocsRequests.get());
		bulkDocsStats.put("totalCount", meanBulkDocsRequests.count());
		bulkDocsStats.put("totalTime", meanBulkDocsRequests.sum());
		bulkDocsStats.put("avgTime", meanBulkDocsRequests.mean());

		final Map<String, Object> revsDiffStats = new HashMap<>();
		revsDiffStats.put("activeCount", activeRevsDiffRequests.get());
		revsDiffStats.put("totalCount", meanRevsDiffRequests.count());
		revsDiffStats.put("totalTime", meanRevsDiffRequests.sum());
		revsDiffStats.put("avgTime", meanRevsDiffRequests.mean());

		stats.put("_bulk_docs", bulkDocsStats);
		stats.put("_revs_diff", revsDiffStats);
		stats.put("tooManyConcurrentRequestsErrors", totalTooManyConcurrentRequestsErrors.get());

		return stats;
	}

	@Override
	public String getVBucketUUID(final String pool, final String bucket, final int vbucket)
	{
		// this function gets called too often
//		if (logger.isDebugEnabled())
//			logger.debug("getVBucketUUID({}, {})", bucket, vbucket);
//
		return make_vBucketUUID(getBucketUUID(pool, bucket), vbucket);
	}

	@Override
	public String getBucketUUID(final String pool, final String bucket)
	{
		// this function gets called too often
//		if (logger.isDebugEnabled())
//			logger.debug("getBucketUUID({})", bucket);

		final String uuid = buckets.get(bucket);
		if (uuid != null)
			return uuid;

		logger.error("getBucketUUID unable to find bucket: {}", bucket);
		throw new RuntimeException("CouchbaseBehavior: failed to find bucket uuid");
	}

	private static String make_vBucketUUID(final String uuid, final int vbucket)
	{
		return uuid.substring(0, 28) + String.format("%04x", vbucket);
	}

}
