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
		result.add("default");
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
		if ("default".equals(pool))
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
		if ("default".equals(pool))
		{
			final List<String> bucketNameList = new ArrayList<String>();

			final ClusterStateRequestBuilder stateBuilder = cluster().prepareState();
			final ClusterStateResponse response = stateBuilder.execute().actionGet();
			final ImmutableOpenMap<String, IndexMetaData> indices = response.getState().getMetaData()
				.getIndices();
			for (final ObjectCursor<String> index : indices.keys())
			{
				bucketNameList.add(index.value);
				final IndexMetaData indexMetaData = indices.get(index.value);
				final ImmutableOpenMap<String, AliasMetaData> aliases = indexMetaData.aliases();
				for (final ObjectCursor<String> alias : aliases.keys())
				{
					bucketNameList.add(alias.value);
				}
			}

			return bucketNameList;
		}
		return null;
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
			return ElasticSearchCAPIBehavior.getCheckpointDocID(response.getSourceAsMap());

		// uuid does not exists
		return null;
	}

	protected void storeUUID(final String bucket, final String id, final String uuid)
	{
		final Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("uuid", uuid);

		final Map<String, Object> toBeIndexed = new HashMap<String, Object>();
		toBeIndexed.put(ElasticSearchCAPIBehavior.CheckpointDoc, doc);

		final IndexRequestBuilder builder = client.prepareIndex();
		builder.setIndex(bucket);
		builder.setId(id);
		builder.setType(checkpointDocumentType);
		builder.setSource(toBeIndexed);
		builder.setOpType(OpType.CREATE);

		if (!builder.execute().actionGet().isCreated())
			logger.error("did not succeed creating uuid");
	}

	@Override
	public String getBucketUUID(final String pool, final String bucket)
	{
		// first look for bucket UUID in cache
		String bucketUUID = this.bucketUUIDCache.getIfPresent(bucket);
		if (bucketUUID != null)
		{
			logger.debug("found bucket UUID in cache");
			return bucketUUID;
		}

		logger.debug("bucket UUID not in cache, looking up");
		if (indexExists(bucket))
		{
			int tries = 0;
			bucketUUID = this.lookupUUID(bucket, "bucketUUID");
			while (bucketUUID == null && tries < 100)
			{
				if (logger.isDebugEnabled())
					logger.debug("bucket UUID doesn't exist yet, creaating, attempt: {}", tries + 1);

				final String newUUID = UUID.randomUUID().toString().replace("-", "");
				storeUUID(bucket, "bucketUUID", newUUID);
				bucketUUID = this.lookupUUID(bucket, "bucketUUID");
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

	@Override
	public List<Object> getNodesServingPool(final String pool)
	{
		if ("default".equals(pool))
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
					for (final Map.Entry<String, String> nodeAttribute : nodeInfo
						.getServiceAttributes().entrySet())
					{
						if (nodeAttribute.getKey().equals(
							"couchbase_address"))
						{
							final int start = nodeAttribute.getValue().lastIndexOf("/");
							final int end = nodeAttribute.getValue().lastIndexOf("]");
							final String hostPort = nodeAttribute.getValue().substring(start + 1, end);
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

	private final boolean indexExists(final String index)
	{
		return client.admin().indices().prepareExists(index).execute().actionGet().isExists();
	}

	private final ClusterAdminClient cluster()
	{
		return client.admin().cluster();
	}
}
