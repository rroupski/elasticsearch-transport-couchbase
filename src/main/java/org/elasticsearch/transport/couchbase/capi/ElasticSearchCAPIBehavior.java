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
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;

import javax.servlet.UnavailableException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class ElasticSearchCAPIBehavior implements CAPIBehavior
{
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
	private static final String DATE_FORMAT = "-yyyy-MM-dd";
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

	static final String CheckpointDoc = "doc";

	private final ObjectMapper mapper = new ObjectMapper();
	private final Client client;
	private final ESLogger logger;

	private final String checkpointDocumentType;
	private final boolean resolveConflicts;

	private final CounterMetric activeRevsDiffRequests;
	private final MeanMetric meanRevsDiffRequests;
	private final CounterMetric activeBulkDocsRequests;
	private final MeanMetric meanBulkDocsRequests;
	private final CounterMetric totalTooManyConcurrentRequestsErrors;

	private final long maxConcurrentRequests;
	private final long bulkIndexRetries;
	private final long bulkIndexRetryWaitMs;

	private final TypeSelector typeSelector;
	private final boolean timeBasedIndex;

	private final Cache<String, String> bucketUUIDCache;

	private final Map<String, String> documentTypeParentFields;
	private final Map<String, String> documentTypeRoutingFields;

	@SuppressWarnings("UnusedParameters")
	public ElasticSearchCAPIBehavior(
		final Client client,
		final ESLogger logger,
		final TypeSelector typeSelector,
		final String checkpointDocumentType,
		final String dynamicTypePath,
		final boolean resolveConflicts,
		final long maxConcurrentRequests,
		final long bulkIndexRetries,
		final long bulkIndexRetryWaitMs,
		final Cache<String, String> bucketUUIDCache,
		final Map<String, String> documentTypeParentFields,
		final Map<String, String> documentTypeRoutingFields)
	{
		this.client = client;
		this.logger = logger;
		this.typeSelector = typeSelector;
		this.timeBasedIndex = typeSelector.timeBasedIndex();
		this.checkpointDocumentType = checkpointDocumentType;
		this.resolveConflicts = resolveConflicts;

		this.activeRevsDiffRequests = new CounterMetric();
		this.meanRevsDiffRequests = new MeanMetric();
		this.activeBulkDocsRequests = new CounterMetric();
		this.meanBulkDocsRequests = new MeanMetric();
		this.totalTooManyConcurrentRequestsErrors = new CounterMetric();

		this.maxConcurrentRequests = maxConcurrentRequests;
		this.bulkIndexRetries = bulkIndexRetries;
		this.bulkIndexRetryWaitMs = bulkIndexRetryWaitMs;
		this.bucketUUIDCache = bucketUUIDCache;

		this.documentTypeParentFields = documentTypeParentFields;
		this.documentTypeRoutingFields = documentTypeRoutingFields;
	}

	@Override
	public Map<String, Object> welcome()
	{
		final Map<String, Object> responseMap = new HashMap<String, Object>();
		responseMap.put("welcome", "elasticsearch-transport-couchbase");
		return responseMap;
	}

	@Override
	public String databaseExists(final String database)
	{
		final String index = indexName(database);
		if (indexExists(index))
		{
			final String uuid = bucketUUID(database);
			if (uuid != null)
			{
				logger.debug("included uuid, validating");
				final String actualUUID = getBucketUUID("default", index);
				if (!uuid.equals(actualUUID))
					return "don't_match";
			}
			else
			{
				logger.debug("no uuid in database name");
			}

			return null;
		}

		return "missing";
	}

	@Override
	public Map<String, Object> getDatabaseDetails(final String database)
	{
		final String doesNotExistReason = databaseExists(database);
		if (doesNotExistReason == null)
		{
			final Map<String, Object> responseMap = new HashMap<String, Object>();
			responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
			return responseMap;
		}
		return null;
	}

	@Override
	public boolean createDatabase(final String database)
	{
		throw new UnsupportedOperationException("Creating indexes is not supported");
	}

	@Override
	public boolean deleteDatabase(final String database)
	{
		throw new UnsupportedOperationException("Deleting indexes is not supported");
	}

	@Override
	public boolean ensureFullCommit(final String database)
	{
		return true;
	}

	/**
	 * NOTE: This method is not actually supported -- it always reports that the document version is
	 * 'missing'.
	 *
	 * @throws UnavailableException
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> revsDiff(
		final String database, final Map<String, Object> revsMap) throws UnavailableException
	{
		// check to see if too many requests are already active
		if (activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= maxConcurrentRequests)
		{
			totalTooManyConcurrentRequestsErrors.inc();
			throw new UnavailableException("Too many concurrent requests");
		}

		final long start = System.currentTimeMillis();
		activeRevsDiffRequests.inc();

		if (logger.isTraceEnabled())
			logger.trace("_revs_diff request for {} : {}", database, revsMap);

		// start with all entries in the response map
		final Map<String, Object> responseMap = new HashMap<String, Object>();
		for (final Entry<String, Object> entry : revsMap.entrySet())
		{
			final String id = entry.getKey();
			final String revs = (String) entry.getValue();
			final Map<String, String> rev = new HashMap<String, String>();
			rev.put("missing", revs);
			responseMap.put(id, rev);
		}

		// if resolve conflicts mode is enabled
		// perform a multi-get query to find information
		// about revisions we already have
		if (resolveConflicts)
		{
			final MultiGetRequestBuilder builder = client.prepareMultiGet();

			int added = 0;
			if (documentTypeRoutingFields != null)
			{
				for (final String id : responseMap.keySet())
				{
					final String index = indexName(database, id);
					final String type = typeSelector.getType(index, id);
					if (documentTypeRoutingFields.containsKey(type))
					{
						// if this type requires special routing, we can't find it without the doc body
						// so we skip this id in the lookup to avoid errors
						continue;
					}

					builder.add(index, type, id);
					added++;
				}
			}

			if (added > 0)
			{
				final MultiGetResponse response = builder.execute().actionGet();
				for (final MultiGetItemResponse item : response)
				{
					if (item.isFailed())
					{
						logger.warn("_revs_diff get failure on index: {} id: {} message: {}",
							item.getIndex(), item.getId(), item.getFailure().getMessage());
					}
					else
					{
						if (item.getResponse().isExists())
						{
							final String itemId = item.getId();
							final Map<String, Object> source = item.getResponse().getSourceAsMap();
							if (source != null)
							{
								final Map<String, Object> meta = (Map<String, Object>) source.get("meta");
								// Note: 'meta' has been removed so it will be always null
								if (meta != null)
								{
									final String rev = (String) meta.get("rev");

									//retrieve the revision passed in from Couchbase
									final Map<String, String> sourceRevMap = (Map<String, String>) responseMap
										.get(itemId);

									final String sourceRev = sourceRevMap.get("missing");
									if (rev.equals(sourceRev))
									{
										// if our revision is the same as the source rev
										// remove it from the response map
										responseMap.remove(itemId);

										if (logger.isTraceEnabled())
											logger
												.trace("_revs_diff already have id: {} rev: {}", itemId, rev);
									}
								}
							}
						}
					}
				}
			}
			else
			{
				logger.debug("skipping multi-get, no documents to look for");
			}

			if (logger.isTraceEnabled())
				logger.trace("_revs_diff response AFTER conflict resolution {}", responseMap);
		}

		meanRevsDiffRequests.inc(System.currentTimeMillis() - start);
		activeRevsDiffRequests.dec();
		return responseMap;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Object> bulkDocs(final String database, final List<Map<String, Object>> docs)
		throws UnavailableException
	{
		// check to see if too many requests are already active
		if (activeBulkDocsRequests.count() + activeRevsDiffRequests.count() >= maxConcurrentRequests)
		{
			totalTooManyConcurrentRequestsErrors.inc();
			throw new UnavailableException("Too many concurrent requests");
		}

		activeBulkDocsRequests.inc();

		final long start = System.currentTimeMillis();

		// keep a map of the id - rev for building the response
		final Map<String, String> revisions = new HashMap<String, String>();

		// put requests into this map, not directly into the bulk request
		final Map<String, IndexRequest> bulkIndexRequests = new HashMap<String, IndexRequest>();
		final Map<String, DeleteRequest> bulkDeleteRequests = new HashMap<String, DeleteRequest>();

		for (final Map<String, Object> doc : docs)
		{
			// these are the top-level elements that could be in the document sent by Couchbase
			final Map<String, Object> meta = (Map<String, Object>) doc.get("meta");
			if (meta == null)
			{
				// if there is no meta-data section, there is nothing we can do
				logger.warn("Document without meta in bulk_docs, ignoring....");
				continue;
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("Document with meta: {}", meta);
			}

			final Map<String, Object> json = getDoc(doc, meta);

			final String id = (String) meta.get("id");
			final String rev = (String) meta.get("rev");
			revisions.put(id, rev);

			final String index = indexName(database, id);
			final String type = typeSelector.getType(index, id);

			if (deleted(meta))
			{
				bulkDeleteRequests.put(id, client.prepareDelete(index, type, id).request());
			}
			else
			{
				final IndexRequestBuilder builder =
					client.prepareIndex(index, type, id).setSource(json);

				final long ttl = getTTL(meta);
				if (ttl > 0)
					builder.setTTL(ttl);

				final String parentField = contains(documentTypeParentFields, type);
				if (parentField != null)
				{
					final Object parent = jsonMapPath(json, parentField);
					if (parent instanceof String)
					{
						builder.setParent((String) parent);
					}
					else
					{
						logger
							.warn("Unable to determine parent. Parent field: {}, doc: {}", parentField, id);
					}
				}

				final String routingField = contains(documentTypeRoutingFields, type);
				if (routingField != null)
				{
					final Object routing = jsonMapPath(json, routingField);
					if (routing instanceof String)
					{
						builder.setRouting((String) routing);
					}
					else
					{
						logger
							.warn("Unable to determine route. Routing field: {}, doc: {}", routingField, id);
					}
				}
				bulkIndexRequests.put(id, builder.request());
			}
		}

		final List<Object> result = new ArrayList<Object>();
		for (int i = 0; i < bulkIndexRetries; ++i)
		{
			// build the bulk request for this iteration
			final BulkRequestBuilder bulkBuilder = client.prepareBulk();
			for (final Entry<String, IndexRequest> entry : bulkIndexRequests.entrySet())
				bulkBuilder.add(entry.getValue());

			for (final Entry<String, DeleteRequest> entry : bulkDeleteRequests.entrySet())
				bulkBuilder.add(entry.getValue());

			final BulkResponse response = bulkBuilder.execute().actionGet();
			if (response.hasFailures())
			{
				for (final BulkItemResponse bulkItemResponse : response.getItems())
				{
					if (bulkItemResponse.isFailed())
					{
						final String failure = bulkItemResponse.getFailure().getMessage();

						// if the error is fatal don't retry
						if (!failure.contains("EsRejectedExecutionException"))
							throw new RuntimeException("indexing error " + failure);
					}
					else
					{
						final String itemId = bulkItemResponse.getId();

						result.add(makeResponse(itemId, revisions));

						// remove the item from the bulk requests list
						// so we don't try to index it again
						bulkIndexRequests.remove(itemId);
						bulkDeleteRequests.remove(itemId);
					}
				}

				try
				{
					Thread.sleep(this.bulkIndexRetryWaitMs);
				}
				catch (final InterruptedException e)
				{
					throw new RuntimeException(e);
				}
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("bulk index succeeded after {} tries", i + 1);

				for (final BulkItemResponse bulkItemResponse : response.getItems())
					result.add(makeResponse(bulkItemResponse.getId(), revisions));

				final long end = System.currentTimeMillis();
				meanBulkDocsRequests.inc(end - start);
				activeBulkDocsRequests.dec();

				return result;
			}
		}

		throw new RuntimeException("indexing error, bulk failed after all retries");
	}

	private static final long getTTL(final Map<String, Object> meta)
	{
		final Integer expiration = (Integer) meta.get("expiration");
		return expiration != null ? (expiration.longValue() * 1000) - System.currentTimeMillis() : 0;
	}

	private static final String contains(final Map<String, String> fields, final String type)
	{
		return fields != null ? fields.get(type) : null;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getDoc(final Map<String, Object> doc, final Map<String, Object> meta)
	{
		final Map<String, Object> json = (Map<String, Object>) doc.get("json");

		if (json != null)
			return json;

		if (meta.containsKey("deleted"))
		{
			if (logger.isDebugEnabled()) logger.debug("delete doc: {}", meta.get("id"));
		}
		else if (nonJSONMode(meta.get("att_reason")))
		{
			// optimization, this tells us the body isn't json
			if (logger.isDebugEnabled()) logger.debug("non-json doc: {}", meta.get("id"));
		}
		else
		{
			// no plain json, let's try parsing the base64 data
			final String base64 = (String) doc.get("base64");
			if (base64 != null)
			{
				try
				{
					final byte[] decodedData = Base64.decode(base64);
					try
					{
						// now try to parse the decoded data as json
						return (Map<String, Object>) mapper.readValue(decodedData, Map.class);
					}
					catch (final IOException e)
					{
						logger.error("Unable to parse data as JSON, doc: {}", meta.get("id"));
						logger.error("Data: {}. Parse error: {}", new String(decodedData), e);
					}
				}
				catch (final IOException e)
				{
					logger.error("Unable to base64-decode doc: {}", meta.get("id"));
					logger.error("Data: {}. Parse error: {}", base64, e);
				}
			}
			else
			{
				logger.warn("not base64 encoded doc: {}", meta.get("id"));
			}
		}

		return new HashMap<String, Object>();
	}

	private static boolean nonJSONMode(final Object reason)
	{
		return "non-JSON mode".equals(reason) || "invalid_json".equals(reason);
	}

	private static boolean deleted(final Map<String, Object> meta)
	{
		final Object deleted = meta.get("deleted");
		return deleted instanceof Boolean ? (Boolean) deleted : false;
	}

	@Override
	public Map<String, Object> getDocument(final String database, final String docId)
	{
		final String index = indexName(database, docId);
		return getDocument(index, docId, typeSelector.getType(index, docId));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getLocalDocument(final String database, final String docId)
	{
		final GetResponse response = client
			.prepareGet(indexName(database, docId), checkpointDocumentType, docId).execute()
			.actionGet();

		return
			response.isExists() ?
				(Map<String, Object>) response.getSourceAsMap().get(CheckpointDoc) : null;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> getDocument(final String index, final String docId, final String docType)
	{
		final GetResponse response = client.prepareGet(index, docType, docId).execute().actionGet();

		return
			response.isExists() ?
				(Map<String, Object>) response.getSourceAsMap().get("doc") : null;
	}

	@Override
	public String storeDocument(
		final String database, final String docId, final Map<String, Object> document)
	{
		final String index = indexName(database, docId);
		return storeDocument(index, docId, document, typeSelector.getType(index, docId));
	}

	@Override
	public String storeLocalDocument(
		final String database, final String docId, final Map<String, Object> document)
	{
		return storeDocument(indexName(database, docId), docId, document, checkpointDocumentType);
	}

	private String storeDocument(
		final String index, final String docId, final Map<String, Object> document, final String docType)
	{
		// normally we just use the revision number present in the document
		String documentRevision = (String) document.get("_rev");
		if (documentRevision == null)
		{
			// if there isn't one we need to generate a revision number
			documentRevision = generateRevisionNumber();
			document.put("_rev", documentRevision);
		}

		return
			client
				.prepareIndex(index, docType, docId)
				.setSource(document)
				.execute()
				.actionGet()
				.isCreated() ? documentRevision : null;
	}

	private static String generateRevisionNumber()
	{
		return "1-" + UUID.randomUUID().toString();
	}

	@Override
	public InputStream getAttachment(final String database, final String docId, final String attachmentName)
	{
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public String storeAttachment(
		final String database, final String docId,
		final String attachmentName, final String contentType, final InputStream input)
	{
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public InputStream getLocalAttachment(
		final String database, final String docId, final String attachmentName)
	{
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	@Override
	public String storeLocalAttachment(
		final String database, final String docId,
		final String attachmentName, final String contentType, final InputStream input)
	{
		throw new UnsupportedOperationException("Attachments are not supported");
	}

	private static String indexName(final String database)
	{
		final int pos = database.indexOf('/');
		return pos > 0 ? database.substring(0, pos) : database;
	}

	private final String indexName(final String database, final String docId)
	{
		final String name = indexName(database);

		if (timeBasedIndex) try
		{
			return name + indexTime(docId);
		}
		catch (final IllegalArgumentException ex)
		{
			logger.warn("Invalid document ID: " + docId);
		}

		return name;
	}

	private static String indexTime(final String docId)
	{
		final UUID uuid = UUID.fromString(docId);
		final long time = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;

		return dateFormat.format(new Date(time));
	}

	private static String bucketUUID(final String database)
	{
		final int pos = database.indexOf(';');
		return pos >= 0 ? database.substring(pos + 1) : null;
	}

	private static String getDatabaseNameWithoutUUID(final String database)
	{
		final int pos = database.indexOf(';');
		return pos >= 0 ? database.substring(0, pos) : database;
	}

	public Map<String, Object> getStats()
	{
		final Map<String, Object> stats = new HashMap<String, Object>();

		final Map<String, Object> bulkDocsStats = new HashMap<String, Object>();
		bulkDocsStats.put("activeCount", activeBulkDocsRequests.count());
		bulkDocsStats.put("totalCount", meanBulkDocsRequests.count());
		bulkDocsStats.put("totalTime", meanBulkDocsRequests.sum());
		bulkDocsStats.put("avgTime", meanBulkDocsRequests.mean());

		final Map<String, Object> revsDiffStats = new HashMap<String, Object>();
		revsDiffStats.put("activeCount", activeRevsDiffRequests.count());
		revsDiffStats.put("totalCount", meanRevsDiffRequests.count());
		revsDiffStats.put("totalTime", meanRevsDiffRequests.sum());
		revsDiffStats.put("avgTime", meanRevsDiffRequests.mean());

		stats.put("_bulk_docs", bulkDocsStats);
		stats.put("_revs_diff", revsDiffStats);
		stats.put("tooManyConcurrentRequestsErrors", totalTooManyConcurrentRequestsErrors.count());

		return stats;
	}

	protected String lookupUUID(final String bucket, final String id)
	{
		final GetRequestBuilder builder = client.prepareGet();
		builder.setIndex(bucket);
		builder.setId(id);
		builder.setType(checkpointDocumentType);
		builder.setFetchSource(true);

		final GetResponse response = builder.execute().actionGet();
		if (response.isExists())
			return getCheckpointDocID(response.getSourceAsMap());

		return null;
	}

	@SuppressWarnings("unchecked")
	static final String getCheckpointDocID(final Map<String, Object> source)
	{
		final Map<String, Object> docMap = (Map<String, Object>) source.get(CheckpointDoc);
		return (String) docMap.get("uuid");
	}

	protected void storeUUID(final String bucket, final String id, final String uuid)
	{
		final Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("uuid", uuid);

		final Map<String, Object> json = new HashMap<String, Object>();
		json.put(CheckpointDoc, doc);

		final IndexRequestBuilder builder = client.prepareIndex();
		builder.setIndex(bucket);
		builder.setId(id);
		builder.setType(checkpointDocumentType);
		builder.setSource(json);
		builder.setOpType(OpType.CREATE);

		if (!builder.execute().actionGet().isCreated())
			logger.error("unable to create new uuid");
	}

	public String getVBucketUUID(final String pool, final String bucket, final int vbucket)
	{
		if (indexExists(bucket))
		{
			final String key = String.format("vbucket%dUUID", vbucket);

			String bucketUUID = lookupUUID(bucket, key);
			int tries = 0;
			while (bucketUUID == null && tries < 100)
			{
				if (logger.isDebugEnabled())
					logger.debug("vbucket {} UUID doesn't exist yet, creating new UUID", vbucket);

				final String newUUID = UUID.randomUUID().toString().replace("-", "");
				storeUUID(bucket, key, newUUID);
				bucketUUID = lookupUUID(bucket, key);
				tries++;
			}

			if (bucketUUID == null)
				throw new RuntimeException("failed to find/create bucket uuid after 100 tries");

			return bucketUUID;
		}

		return null;
	}

	@Override
	public String getBucketUUID(final String pool, final String bucket)
	{
		// first look for bucket UUID in cache
		String bucketUUID = bucketUUIDCache.getIfPresent(bucket);
		if (bucketUUID != null)
		{
			logger.debug("found bucket UUID in cache");
			return bucketUUID;
		}

		logger.debug("bucket UUID not in cache, looking up");
		if (indexExists(bucket))
		{
			int tries = 0;
			bucketUUID = lookupUUID(bucket, "bucketUUID");
			while (bucketUUID == null && tries < 100)
			{
				if (logger.isDebugEnabled())
					logger.debug("bucket UUID doesn't exist yet, creating, attempt: {}", tries + 1);

				final String newUUID = UUID.randomUUID().toString().replace("-", "");
				storeUUID(bucket, "bucketUUID", newUUID);
				bucketUUID = lookupUUID(bucket, "bucketUUID");
				tries++;
			}

			if (bucketUUID != null)
			{
				// store it in the cache
				bucketUUIDCache.put(bucket, bucketUUID);
				return bucketUUID;
			}
		}

		throw new RuntimeException("failed to find/create bucket uuid");
	}

	@SuppressWarnings("unchecked")
	static Object jsonMapPath(final Map<String, Object> json, final String path)
	{
		final int dotIndex = path.indexOf('.');
		if (dotIndex >= 0)
		{
			final String pathThisLevel = path.substring(0, dotIndex);
			final Object current = json.get(pathThisLevel);
			final String pathRest = path.substring(dotIndex + 1);

			if (pathRest.isEmpty())
				return current;

			if (current instanceof Map)
				return jsonMapPath((Map<String, Object>) current, pathRest);
		}
		else
		{
			return json.get(path);
		}

		return null;
	}

	private final boolean indexExists(final String index)
	{
		// timeBasedIndex == true: using dynamic indexes
		return timeBasedIndex || exists(index);
	}

	private final boolean exists(final String index)
	{
		return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
	}

	private static final Map<String, Object> makeResponse(final String itemId, final Map<String, String> revisions)
	{
		final Map<String, Object> response = new HashMap<String, Object>();

		response.put("id", itemId);
		response.put("rev", revisions.get(itemId));

		return response;
	}
}
