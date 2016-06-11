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
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.logging.ESLogger;

import java.util.*;

public class ElasticSearchCouchbaseBehavior implements CouchbaseBehavior
{
	static final String DefaultPoolName = "default";

	private final Client client;
	private final ESLogger logger;

	private final Map<String, String> buckets;

	public ElasticSearchCouchbaseBehavior(
		final Client client,
		final ESLogger logger,
		final Map<String, String> buckets)
	{
		this.client = client;
		this.logger = logger;
		this.buckets = buckets;
	}

	@Override
	public List<String> getPools()
	{
		final List<String> result = new ArrayList<>();
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
			final Map<String, Object> bucket = new HashMap<>();
			bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

			final Map<String, Object> responseMap = new HashMap<>();
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
			return new ArrayList<>(buckets.keySet());

		return null;
	}

	@Override
	public String getBucketUUID(final String pool, final String bucket)
	{
		if (logger.isDebugEnabled())
			logger.debug("getBucketUUID({})", bucket);

		if (buckets.containsKey(bucket))
			return buckets.get(bucket);

		logger.error("getBucketUUID unable to find bucket: {}", bucket);
		throw new RuntimeException("CouchbaseBehavior: failed to find bucket uuid");
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
			final List<Object> nodes = new ArrayList<>();
			for (final NodeInfo nodeInfo : infoResponse.getNodes())
			{
				final ImmutableMap<String, String> attributes = nodeInfo.getServiceAttributes();

				// FIXME there has to be a better way than
				// parsing this string
				// but so far I have not found it
				if (attributes != null)
				{
					attributes.entrySet().stream().filter(e -> "couchbase_address".equals(e.getKey()))
						.forEach(e ->
						{
							final String value = e.getValue();

							final int start = value.lastIndexOf('/');
							final int end = value.lastIndexOf(']');

							final String hostPort = value.substring(start + 1, end);
							final String[] parts = hostPort.split(":");

							final Map<String, Object> nodePorts = new HashMap<>();
							nodePorts.put("direct", Integer.parseInt(parts[1]));

							final Map<String, Object> node = new HashMap<>();
							node.put("couchApiBase", String.format("http://%s/", hostPort));
							node.put("hostname", hostPort);
							node.put("ports", nodePorts);

							nodes.add(node);
						});
				}
			}
			return nodes;
		}
		return null;
	}

	@Override
	public final Map<String, Object> getStats()
	{
		return new HashMap<>();
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
