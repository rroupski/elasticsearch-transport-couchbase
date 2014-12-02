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

import com.couchbase.capi.CouchbaseBehavior;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.logging.ESLogger;

import java.util.*;

public class ElasticSearchCouchbaseBehavior implements CouchbaseBehavior
{
	static final String DefaultPoolName = "default";
	static final String CheckpointDoc = "doc";
	static final String BucketUUID = "bucketUUID";

	protected Client client;
	protected ESLogger logger;
	protected String checkpointDocumentType;
	protected Cache<String, String> bucketUUIDCache;

	public ElasticSearchCouchbaseBehavior(
		final Client client,
		final ESLogger logger,
		final String checkpointDocumentType,
		final Cache<String, String> bucketUUIDCache)
	{
		this.client = client;
		this.logger = logger;
		this.checkpointDocumentType = checkpointDocumentType;
		this.bucketUUIDCache = bucketUUIDCache;
	}

	@Override
	public List<String> getPools()
	{
		final List<String> result = new ArrayList<String>();
		result.add(DefaultPoolName);
		return result;
	}

	@Override
	public String getPoolUUID(final String pool)
	{
		final ClusterStateRequestBuilder builder = cluster().prepareState();
		final ClusterStateResponse response = builder.execute().actionGet();
		final ClusterName name = response.getClusterName();
		return UUID.nameUUIDFromBytes(name.toString().getBytes()).toString().replace("-", "");
	}

	@Override
	public Map<String, Object> getPoolDetails(final String pool)
	{
		if (DefaultPoolName.equals(pool))
		{
			final Map<String, Object> bucket = new HashMap<String, Object>();
			bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

			final Map<String, Object> responseMap = new HashMap<String, Object>();
			responseMap.put("buckets", bucket);

			final List<Object> nodes = getNodesServingPool(pool);
			responseMap.put("nodes", nodes);

			return responseMap;
		}
		return null;
	}

	@Override
	public List<String> getBucketsInPool(final String pool)
	{
		if (logger.isDebugEnabled())
			logger.debug("CouchbaseBehavior: getBucketsInPool({})", pool);

		if (isDefaultPool(pool))
		{
			final List<String> bucketNameList = new ArrayList<String>();

			final ClusterStateRequestBuilder stateBuilder = cluster().prepareState();
			final ClusterStateResponse response = stateBuilder.execute().actionGet();
			final ImmutableOpenMap<String, IndexMetaData> indices =
				response.getState().getMetaData().getIndices();

			for (final ObjectCursor<String> index : indices.keys())
			{
				if (logger.isDebugEnabled())
					logger.debug("CouchbaseBehavior: add index: {}", index.value);

				bucketNameList.add(index.value);

				final IndexMetaData indexMetaData = indices.get(index.value);
				final ImmutableOpenMap<String, AliasMetaData> aliases = indexMetaData.aliases();

				for (final ObjectCursor<String> alias : aliases.keys())
				{
					if (logger.isDebugEnabled())
						logger.debug("CouchbaseBehavior: add alias: {}", alias.value);

					bucketNameList.add(alias.value);
				}
			}

			return bucketNameList;
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
			if (logger.isDebugEnabled())
				logger.debug("CouchbaseBehavior: found bucket '{}' in cache", bucket);

			return bucketUUID;
		}

		if (logger.isDebugEnabled())
			logger.debug("CouchbaseBehavior: bucket '{}' not in cache, looking up", bucket);

		if (indexExists(bucket))
		{
			for (int i = 0; i < 100; ++i)
			{
				bucketUUID = lookupBucketUUID(bucket, BucketUUID);
				if (bucketUUID == null)
				{
					if (logger.isDebugEnabled())
						logger
							.debug("CouchbaseBehavior: {} bucketUUID '{}' not found, creating new UUID",
								i + 1, bucket);

					storeBucketUUID(bucket, BucketUUID, generateUUID());
					continue;
				}

				// store it in the cache
				bucketUUIDCache.put(bucket, bucketUUID);
				return bucketUUID;
			}
		}

		throw new RuntimeException("CouchbaseBehavior: failed to find/create bucket uuid");
	}

	@Override
	public List<Object> getNodesServingPool(final String pool)
	{
		if (logger.isDebugEnabled())
			logger.debug("CouchbaseBehavior: getNodesServingPool({})", pool);

		if (isDefaultPool(pool))
		{
			final NodesInfoRequestBuilder infoBuilder = cluster().prepareNodesInfo((String[]) null);
			final NodesInfoResponse infoResponse = infoBuilder.execute().actionGet();

			// extract what we need from this response
			final List<Object> nodes = new ArrayList<Object>();
			for (final NodeInfo nodeInfo : infoResponse.getNodes())
			{

				// FIXME there has to be a better way than
				// parsing this string
				// but so far I have not found it
				if (nodeInfo.getServiceAttributes() != null)
				{
					for (final Map.Entry<String, String> e : nodeInfo.getServiceAttributes().entrySet())
					{
						if ("couchbase_address".equals(e.getKey()))
						{
							final String value = e.getValue();

							final int start = value.lastIndexOf('/');
							final int end = value.lastIndexOf(']');

							final String hostPort = value.substring(start + 1, end);
							final String[] parts = hostPort.split(":");

							final Map<String, Object> nodePorts = new HashMap<String, Object>();
							nodePorts.put("direct", Integer.parseInt(parts[1]));

							final Map<String, Object> node = new HashMap<String, Object>();
							node.put("couchApiBase", String.format("http://%s/", hostPort));
							node.put("hostname", hostPort);
							node.put("ports", nodePorts);

							nodes.add(node);
						}
					}
				}
			}
			return nodes;
		}
		return null;
	}

	@Override
	public final Map<String, Object> getStats()
	{
		return new HashMap<String, Object>();
	}

	static final String generateUUID()
	{
		return UUID.randomUUID().toString().replace("-", "");
	}

	@SuppressWarnings("unchecked")
	static final String getCheckpointDocID(final Map<String, Object> source)
	{
		final Map<String, Object> docMap = (Map<String, Object>) source.get(CheckpointDoc);
		return (String) docMap.get("uuid");
	}

	private String lookupBucketUUID(final String index, final String id)
	{
		if (logger.isDebugEnabled())
			logger.debug("CouchbaseBehavior: '{}' lookup {}", index, id);

		final GetRequestBuilder builder = client.prepareGet();
		builder.setIndex(index);
		builder.setId(id);
		builder.setType(checkpointDocumentType);
		builder.setFetchSource(true);

		final GetResponse response = builder.execute().actionGet();

		if (response.isExists())
			return getCheckpointDocID(response.getSourceAsMap());

		// uuid does not exists
		return null;
	}

	private void storeBucketUUID(final String index, final String id, final String uuid)
	{
		if (logger.isDebugEnabled())
			logger.debug("CouchbaseBehavior: store {} -> {} in index: {}", id, uuid, index);

		final Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("uuid", uuid);

		final Map<String, Object> json = new HashMap<String, Object>();
		json.put(ElasticSearchCouchbaseBehavior.CheckpointDoc, doc);

		final IndexRequestBuilder builder = client
			.prepareIndex()
			.setIndex(index)
			.setType(checkpointDocumentType)
			.setId(id)
			.setSource(json)
			.setOpType(OpType.CREATE);

		if (!builder.execute().actionGet().isCreated())
			logger.error("CouchbaseBehavior: unable to store uuid: {}", uuid);
	}

	private final boolean indexExists(final String index)
	{
		return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
	}

	private final ClusterAdminClient cluster()
	{
		return client.admin().cluster();
	}

	private static final boolean isDefaultPool(final String pool)
	{
		return DefaultPoolName.equals(pool);
	}
}
