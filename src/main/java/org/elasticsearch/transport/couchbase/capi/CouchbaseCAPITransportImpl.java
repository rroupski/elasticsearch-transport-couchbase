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
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.transport.couchbase.CouchbaseCAPITransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CouchbaseCAPITransportImpl extends AbstractLifecycleComponent<CouchbaseCAPITransport>
	implements CouchbaseCAPITransport
{
	private CAPIServer server;
	private final Client client;
	private final NetworkService networkService;

	private final String port;
	private final String bindHost;
	private final String publishHost;

	private final String username;
	private final String password;

	private final int numVbuckets;

	private final int maxConcurrentRequests;

	private BoundTransportAddress boundAddress;

	private CAPIBehavior capiBehavior;
	private CouchbaseBehavior couchbaseBehavior;

	final Map<String, BucketContext> contexts;

	@SuppressWarnings("UnusedParameters")
	@Inject
	public CouchbaseCAPITransportImpl(
		final Settings settings,
		final RestController restController,
		final NetworkService networkService,
		final IndicesService indicesService,
		final MetaDataMappingService metaDataMappingService,
		final Client client)
	{
		super(settings);

		this.networkService = networkService;
		this.client = client;
		this.port = settings.get("couchbase.port", "9091-10091");
		this.bindHost = componentSettings.get("bind_host");
		this.publishHost = componentSettings.get("publish_host");
		this.username = settings.get("couchbase.username", "Administrator");
		this.password = settings.get("couchbase.password", "");

		this.contexts = createContexts(
			new BucketContext(settings.getAsSettings("couchbase")),
			settings.getAsSettings("couchbase.buckets"));

		this.numVbuckets = settings.getAsInt("couchbase.num_vbuckets", defaultNumVbuckets());
		this.maxConcurrentRequests = settings.getAsInt("couchbase.maxConcurrentRequests", 1024);
	}

	private static Map<String, BucketContext> createContexts(
		final BucketContext context, final Settings buckets)
	{
		final Map<String, BucketContext> indexes = new HashMap<>();

		if (buckets != null)
		{
			for (final String name : buckets.names())
				indexes.put(name, new BucketContext(context, buckets.getAsSettings(name)));
		}

		return indexes;
	}

	@Override
	protected void doStart() throws ElasticsearchException
	{
		// Bind and start to accept incoming connections.
		final InetAddress hostAddressX;
		try
		{
			hostAddressX = networkService.resolveBindHostAddress(bindHost);
		}
		catch (final IOException e)
		{
			throw new BindHttpException("Failed to resolve host [" + bindHost + "]", e);
		}

		final InetAddress publishAddressHostX;
		try
		{
			publishAddressHostX = networkService.resolvePublishHostAddress(publishHost);
		}
		catch (final IOException e)
		{
			throw new BindHttpException("Failed to resolve publish address host [" + publishHost + "]", e);
		}

		final Map<String, String> buckets = buildBucketMap(contexts);

		capiBehavior = new ElasticSearchCAPIBehavior(
			client, logger, buckets, maxConcurrentRequests, contexts);

		couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client, logger, buckets);

		final AtomicReference<Exception> lastException = new AtomicReference<>();
		if (new PortsRange(port).iterate(portNumber ->
		{
			try
			{
				server = new CAPIServer(
					capiBehavior,
					couchbaseBehavior,
					new InetSocketAddress(hostAddressX, portNumber),
					CouchbaseCAPITransportImpl.this.username,
					CouchbaseCAPITransportImpl.this.password,
					numVbuckets);

				if (publishAddressHostX != null)
				{
					server.setPublishAddress(publishAddressHostX);
				}

				server.start();
			}
			catch (final Exception e)
			{
				lastException.set(e);
				return false;
			}
			return true;
		}))
		{
			final InetSocketAddress boundAddress = server.getBindAddress();
			final InetSocketAddress publishAddress = new InetSocketAddress(
				publishAddressHostX, boundAddress.getPort());

			this.boundAddress = new BoundTransportAddress(
				new InetSocketTransportAddress(boundAddress),
				new InetSocketTransportAddress(publishAddress));
		}
		else
			throw new BindHttpException("Failed to bind to [" + port + "]", lastException.get());
	}

	private final static Map<String, String> buildBucketMap(final Map<String, BucketContext> contexts)
	{
		final Map<String, String> buckets = new HashMap<>(contexts.size());

		for (final Map.Entry<String, BucketContext> bucket : contexts.entrySet())
			buckets.put(bucket.getKey(), bucket.getValue().uuid);

		return Collections.unmodifiableMap(buckets);
	}

	@Override
	protected void doStop() throws ElasticsearchException
	{
		if (server != null)
		{
			try
			{
				server.stop();
			}
			catch (final Exception e)
			{
				throw new ElasticsearchException("Error stopping jetty", e);
			}
		}
	}

	@Override
	protected void doClose() throws ElasticsearchException
	{
		// N/A
	}

	@Override
	public BoundTransportAddress boundAddress()
	{
		return boundAddress;
	}

	private static int defaultNumVbuckets()
	{
		return System.getProperty("os.name").toLowerCase().contains("mac") ? 64 : 1024;
	}
}
