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
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CouchbaseCAPITransportImpl extends AbstractLifecycleComponent<CouchbaseCAPITransport> implements CouchbaseCAPITransport
{

	public static final String DEFAULT_DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";

	private CAPIBehavior capiBehavior;
	private CouchbaseBehavior couchbaseBehavior;
	private CAPIServer server;
	private final Client client;
	private final NetworkService networkService;

	private final String port;
	private final String bindHost;
	private final String publishHost;

	private final String username;
	private final String password;

	private final Boolean resolveConflicts;

	private BoundTransportAddress boundAddress;

	private final String checkpointDocumentType;
	private final String dynamicTypePath;

	private final int numVbuckets;

	private final long maxConcurrentRequests;

	private final long bulkIndexRetries;
	private final long bulkIndexRetryWaitMs;
	private final Cache<String, String> bucketUUIDCache;

	private final TypeSelector typeSelector;

	private final Map<String, String> documentTypeParentFields;
	private final Map<String, String> documentTypeRoutingFields;

	@SuppressWarnings("UnusedParameters")
	@Inject
	public CouchbaseCAPITransportImpl
		(final Settings settings,
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
		this.checkpointDocumentType = settings.get("couchbase.checkpointDocumentType", DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
		this.dynamicTypePath = settings.get("couchbase.dynamicTypePath");
		this.resolveConflicts = settings.getAsBoolean("couchbase.resolveConflicts", true);
		this.maxConcurrentRequests = settings.getAsLong("couchbase.maxConcurrentRequests", 1024L);
		this.bulkIndexRetries = settings.getAsLong("couchbase.bulkIndexRetries", 10L);
		this.bulkIndexRetryWaitMs = settings.getAsLong("couchbase.bulkIndexRetryWaitMs", 1000L);

		final long bucketUUIDCacheEvictMs = settings
			.getAsLong("couchbase.bucketUUIDCacheEvictMs", 300000L);
		final Class<? extends TypeSelector> typeSelectorClass = settings
			.<TypeSelector>getAsClass("couchbase.typeSelector", DefaultTypeSelector.class);
		try
		{
			this.typeSelector = typeSelectorClass.newInstance();
			this.typeSelector.configure(settings);
		}
		catch (final Exception e)
		{
			throw new ElasticsearchException("couchbase.typeSelector", e);
		}

		int defaultNumVbuckets = 1024;
		if (System.getProperty("os.name").toLowerCase().contains("mac"))
		{
			logger.info("Detected platform is Mac, changing default num_vbuckets to 64");
			defaultNumVbuckets = 64;
		}

		this.numVbuckets = settings.getAsInt("couchbase.num_vbuckets", defaultNumVbuckets);

		this.bucketUUIDCache = CacheBuilder.newBuilder()
			.expireAfterWrite(bucketUUIDCacheEvictMs, TimeUnit.MILLISECONDS).build();

		this.documentTypeParentFields = settings.getByPrefix("couchbase.documentTypeParentFields.")
			.getAsMap();

		if (logger.isInfoEnabled())
		{
			for (final String key : documentTypeParentFields.keySet())
			{
				final String parentField = documentTypeParentFields.get(key);
				logger.info("Using field {} as parent for type {}", parentField, key);
			}
		}

		this.documentTypeRoutingFields = settings.getByPrefix("couchbase.documentTypeRoutingFields.")
			.getAsMap();
		if (logger.isInfoEnabled())
		{
			for (final String key : documentTypeRoutingFields.keySet())
			{
				final String routingField = documentTypeRoutingFields.get(key);
				logger.info("Using field {} as routing for type {}", routingField, key);
			}
		}
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

		capiBehavior = new ElasticSearchCAPIBehavior(client, logger, typeSelector, checkpointDocumentType, dynamicTypePath, resolveConflicts, maxConcurrentRequests, bulkIndexRetries, bulkIndexRetryWaitMs, bucketUUIDCache, documentTypeParentFields, documentTypeRoutingFields);
		couchbaseBehavior = new ElasticSearchCouchbaseBehavior(client, logger, checkpointDocumentType, bucketUUIDCache);

		final AtomicReference<Exception> lastException = new AtomicReference<Exception>();

		if (new PortsRange(port).iterate(new PortsRange.PortCallback()
		{
			@Override
			public boolean onPortNumber(final int portNumber)
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
			}
		}))
		{
			final InetSocketAddress boundAddress = server.getBindAddress();
			final InetSocketAddress publishAddress = new InetSocketAddress(publishAddressHostX, boundAddress.getPort());

			this.boundAddress = new BoundTransportAddress(new InetSocketTransportAddress(boundAddress), new InetSocketTransportAddress(publishAddress));
		}
		else
			throw new BindHttpException("Failed to bind to [" + port + "]", lastException.get());
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

	}

	@Override
	public BoundTransportAddress boundAddress()
	{
		return boundAddress;
	}
}
