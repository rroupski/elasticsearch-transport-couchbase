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
package org.elasticsearch.transport.couchbase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.rest.RestController;

public class CouchbaseCAPI extends AbstractLifecycleComponent<CouchbaseCAPI>
{
	private final CouchbaseCAPITransport transport;

	private final NodeService nodeService;

	private static final String CouchbaseAddress = "couchbase_address";

	@SuppressWarnings("UnusedParameters")
	@Inject
	public CouchbaseCAPI(
		final Settings settings,
		final CouchbaseCAPITransport transport,
		final RestController restController,
		final NodeService nodeService)
	{
		super(settings);
		this.transport = transport;
		this.nodeService = nodeService;
	}

	@Override
	protected void doStart() throws ElasticsearchException
	{
		transport.start();

		if (logger.isInfoEnabled())
			logger.info("started couchbase plugin on: {}", transport.boundAddress());

		nodeService
			.putAttribute(CouchbaseAddress, transport.boundAddress().publishAddress().toString());
	}

	@Override
	protected void doStop() throws ElasticsearchException
	{
		nodeService.removeAttribute(CouchbaseAddress);
		transport.stop();
	}

	@Override
	protected void doClose() throws ElasticsearchException
	{
		transport.close();
	}
}
