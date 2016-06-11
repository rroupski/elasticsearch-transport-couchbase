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

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

class EventsIndex extends ElasticIndex
{
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
	private static final String DATE_FORMAT = "-yyyy-MM-dd";
	private static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern(DATE_FORMAT);

	EventsIndex(
		final String bucket,
		final BucketContext context,
		final Client client,
		final ESLogger logger)
	{
		super(bucket, context, client, logger);
	}

	@Override
	final String indexName(final String docId)
	{
		final String name = super.indexName(docId);

		try
		{
			return name + indexTime(docId);
		}
		catch (final IllegalArgumentException | UnsupportedOperationException ex)
		{
			logger.warn("Invalid document ID: {}, error: {}, using base index: {}",
				docId, ex.getMessage(), name);
		}

		return name;
	}

	//	@Override
//	final Map<String, Object> diff(final Map<String, Object> revsMap)
//	{
//		// it is all good
//		return new HashMap<>();
//	}
//
	@Override
	protected final void updateTTL(
		final IndexRequestBuilder builder, final Map<String, Object> meta)
	{
		// ignore
	}

	@Override
	protected final void updateDeleteRequests(
		final Map<String, DeleteRequest> requests, final String id)
	{
		// ignore
	}

	@Override
	protected final void updateParent(
		final IndexRequestBuilder builder,
		final String type,
		final String id,
		final Map<String, Object> json)
	{
		// N/A
	}

	@Override
	protected final void updateRouting(
		final IndexRequestBuilder builder,
		final String type,
		final String id,
		final Map<String, Object> json)
	{
		// N/A
	}

	@Override
	final boolean exists()
	{
		// true: using dynamic indexes
		return true;
	}

	private static String indexTime(final String docId)
	{
		final int pos = docId.indexOf(':');
		if (pos > 0)
		{
			final UUID uuid = UUID.fromString(docId.substring(pos + 1));
			final long time = (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;

			return dateFormat.format(
				LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
		}

		return "";
	}
}
