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

import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

class ElasticIndex
{
	protected static final String DEFAULT_TYPE = "undefined";
	protected static final String ID = "id";
	protected static final String REV = "rev";
	protected static final String META = "meta";

	protected final BucketContext context;

	protected String bucket;
	protected final String index;

	protected final Client client;
	protected final ESLogger logger;

	private final ObjectMapper mapper = new ObjectMapper();

	ElasticIndex(final String bucket, final BucketContext context, final Client client, final ESLogger logger)
	{
		this.context = context;
		this.client = client;
		this.logger = logger;

		this.bucket = bucket;
		this.index = indexBaseName(bucket);
	}


	/** Database Operations * */

	boolean exists()
	{
		return exists(index);
	}

	String indexName(final String docId)
	{
		return indexBaseName(bucket);
	}

	@SuppressWarnings("MethodMayBeStatic")
	String getType(final String docId)
	{
		final int pos = docId.indexOf(':');

		return pos > 0 ? docId.substring(0, pos) : DEFAULT_TYPE;
	}

	@SuppressWarnings("MethodMayBeStatic")
	Map<String, Object> diff(final Map<String, Object> revsMap)
	{
		// start with all entries in the response map
		final Map<String, Object> responseMap = new HashMap<>(revsMap.size());
		for (final Map.Entry<String, Object> entry : revsMap.entrySet())
		{
			final String id = entry.getKey();
			final String val = (String) entry.getValue();

			final Map<String, String> rev = new HashMap<>();
			rev.put("missing", val);

			responseMap.put(id, rev);
		}

		return responseMap;
	}

	List<Object> index(final List<Map<String, Object>> docs)
	{
		// keep a map of the id - rev for building the response
		final Map<String, String> revisions = new HashMap<>();

		// put requests into this map, not directly into the bulk request
		final Map<String, IndexRequest> bulkIndexRequests = new HashMap<>();
		final Map<String, DeleteRequest> bulkDeleteRequests = new HashMap<>();

		prepareIndexStatements(docs, revisions, bulkIndexRequests, bulkDeleteRequests);

		final List<Object> result = new ArrayList<>();
		for (int i = 0; i < context.bulkIndexRetries; ++i)
		{
			// build the bulk request for this iteration
			final BulkRequestBuilder bulkBuilder = client.prepareBulk();
			for (final Map.Entry<String, IndexRequest> entry : bulkIndexRequests.entrySet())
				bulkBuilder.add(entry.getValue());

			for (final Map.Entry<String, DeleteRequest> entry : bulkDeleteRequests.entrySet())
				bulkBuilder.add(entry.getValue());

			if (bulkBuilder.numberOfActions() == 0) return result;

			final BulkResponse response = bulkBuilder.execute().actionGet();
			if (response.hasFailures())
			{
				for (final BulkItemResponse bulkItemResponse : response.getItems())
				{
					if (bulkItemResponse.isFailed())
					{
						if (bulkItemResponse.getFailure().getStatus() == RestStatus.TOO_MANY_REQUESTS)
						{
							logger.warn("{} - elasticsearch is too busy - status: {}, message: {}",
								index, bulkItemResponse.getFailure().getStatus(), bulkItemResponse.getFailureMessage());
							continue; // retry again later
						}

						// if the error is fatal don't retry but tell couchbase that the operation
						// was successful, so it will not retry to re-index the same document again and again
						logger.error("{} - elasticsearch indexing error - status: {}, message: {}",
							index, bulkItemResponse.getFailure().getStatus(), bulkItemResponse.getFailureMessage());
					}

					final String itemId = bulkItemResponse.getId();

					// remove the item from the bulk requests list, so we don't try to index it again
					bulkIndexRequests.remove(itemId);
					bulkDeleteRequests.remove(itemId);
					result.add(makeResponse(itemId, revisions));
				}

				if (bulkIndexRequests.isEmpty() && bulkDeleteRequests.isEmpty())
				{
					if (logger.isDebugEnabled())
						logger
							.debug("{} - bulk index succeeded after {} tries and a few failures", index, i);

					return result;
				}

				try
				{
					Thread.sleep(context.bulkIndexRetryWaitMs);
				}
				catch (final InterruptedException e)
				{
					throw new RuntimeException(e);
				}
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("{} - bulk index succeeded after {} tries", index, i);

				for (final BulkItemResponse bulkItemResponse : response.getItems())
					result.add(makeResponse(bulkItemResponse.getId(), revisions));

				return result;
			}
		}

		logger.error("{} - bulk index failed after {} retries", index, context.bulkIndexRetries);
		throw new RuntimeException("indexing error, bulk failed after all retries");
	}

	@SuppressWarnings("unchecked")
	void prepareIndexStatements(
		final List<Map<String, Object>> docs,
		final Map<String, String> revisions,
		final Map<String, IndexRequest> bulkIndexRequests,
		final Map<String, DeleteRequest> bulkDeleteRequests)
	{
		for (final Map<String, Object> doc : docs)
		{
			// these are the top-level elements that could be in the document sent by Couchbase
			final Map<String, Object> meta = (Map<String, Object>) doc.get(META);
			if (meta == null)
			{
				// if there is no meta-data section, there is nothing we can do
				logger.warn("{} - Document without meta in bulk_docs, ignoring....", index);
			}
			else
			{
				if (logger.isDebugEnabled())
					logger.debug("{} - document metadata: {}", meta, index);

				final String id = (String) meta.get(ID);
				revisions.put(id, (String) meta.get(REV));

				if (shouldIndex(id))
				{
					if (deleted(meta))
					{
						updateDeleteRequests(bulkDeleteRequests, id);
					}
					else
					{
						final String index = indexName(id);
						final String type = getType(id);

						final Map<String, Object> json = getDoc(doc, meta);
						final IndexRequestBuilder builder = client.prepareIndex(index, type, id);

						builder.setSource(json);

						updateTTL(builder, meta);
						updateParent(builder, type, id, json);
						updateRouting(builder, type, id, json);

						bulkIndexRequests.put(id, builder.request());
					}
				}
			}
		}
	}

	protected boolean shouldIndex(final String id)
	{
		return true;
	}

	protected void updateTTL(
		final IndexRequestBuilder builder, final Map<String, Object> meta)
	{
		final long ttl = getTTL(meta);
		if (ttl > 0)
			builder.setTTL(ttl);
	}

	protected void updateDeleteRequests(
		final Map<String, DeleteRequest> requests, final String id)
	{
		requests.put(id, client.prepareDelete(indexName(id), getType(id), id).request());
	}

	protected void updateRouting(
		final IndexRequestBuilder builder,
		final String type,
		final String id,
		final Map<String, Object> json)
	{
		final String routingField = contains(context.documentTypeRoutingFields, type);
		if (routingField != null)
		{
			final Object routing = jsonMapPath(json, routingField);
			if (routing instanceof String)
			{
				builder.setRouting((String) routing);
			}
			else
			{
				logger.warn(
					"{} - Unable to determine route. Routing field: {}, doc: {}", index, routingField, id);
			}
		}
	}

	protected void updateParent(
		final IndexRequestBuilder builder,
		final String type,
		final String id,
		final Map<String, Object> json)
	{
		final String parentField = contains(context.documentTypeParentFields, type);
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
					.warn("{} - Unable to determine parent. Parent field: {}, doc: {}",
						index, parentField, id);
			}
		}
	}

	private static boolean nonJSONMode(final Object reason)
	{
		return "non-JSON mode".equals(reason) || "invalid_json".equals(reason);
	}

	static boolean deleted(final Map<String, Object> meta)
	{
		final Object deleted = meta.get("deleted");
		return deleted instanceof Boolean ? (Boolean) deleted : false;
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

	private static final Map<String, Object> makeResponse(final String itemId, final Map<String, String> revisions)
	{
		final Map<String, Object> response = new HashMap<>();

		response.put("id", itemId);
		response.put("rev", revisions.get(itemId));

		return response;
	}

	String storeDocument(final String docId, final Map<String, Object> document)
	{
		return storeDocument(indexName(docId), getType(docId), docId, document);
	}

	Map<String, Object> getDocument(final String docId)
	{
		return getDocument(indexName(docId), getType(docId), docId);
	}

	private static String indexBaseName(final String database)
	{
		final int pos = database.indexOf('/');
		return pos > 0 ? database.substring(0, pos) : database;
	}

	private final boolean exists(final String index)
	{
		return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getDocument(
		final String index, final String docType, final String docId)
	{
		final GetResponse response = client.prepareGet(index, docType, docId).execute().actionGet();

		return
			response.isExists() ?
				(Map<String, Object>) response.getSourceAsMap().get("doc") : null;
	}

	final String storeDocument(
		final String index, final String docType, final String docId, final Map<String, Object> document)
	{
		// normally we just use the revision number present in the document
		String documentRevision = (String) document.get("_rev");
		if (documentRevision == null)
		{
			// if there isn't one we need to generate a revision number
			documentRevision = generateRevisionNumber();
			document.put("_rev", documentRevision);
		}

		return client
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
	Map<String, Object> getDoc(final Map<String, Object> doc, final Map<String, Object> meta)
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

		return new HashMap<>();
	}


	@Override
	public String toString()
	{
		return bucket;
	}

	static ElasticIndex create(
		final String name, final BucketContext context, final Client client, final ESLogger logger)
	{
		switch (context.type)
		{
			case BucketContext.TimeBasedIndex:
				return new EventsIndex(name, context, client, logger);
			case BucketContext.DefaultIndex:
				return new MDRIndex(name, context, client, logger);
			default:
				throw new IllegalArgumentException("Illegal type: " + context.type);
		}
	}
}
