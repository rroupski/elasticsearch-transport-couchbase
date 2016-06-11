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

import org.elasticsearch.common.settings.Settings;

import java.util.Map;


class BucketContext
{
	final static String DefaultIndex = "default";
	final static String TimeBasedIndex = "time_based";

	final String uuid;
	final String type;

	final String defaultDocumentType;

	final boolean timeBasedIndex;

	// TODO: is it needed?
	final Boolean resolveConflicts;

	final int bulkIndexRetries;
	final int bulkIndexRetryWaitMs;

	final Map<String, String> documentTypeParentFields;
	final Map<String, String> documentTypeRoutingFields;

	BucketContext(final Settings couchbase)
	{
		uuid = null;
		type = DefaultIndex;
		defaultDocumentType = couchbase.get("defaultDocumentType", "undefined");
		resolveConflicts = couchbase.getAsBoolean("resolveConflicts", true);
		bulkIndexRetries = couchbase.getAsInt("bulkIndexRetries", 10);
		bulkIndexRetryWaitMs = couchbase.getAsInt("bulkIndexRetryWaitMs", 1000);
		timeBasedIndex = couchbase.getAsBoolean("timeBasedIndex", false);

		documentTypeParentFields = couchbase.getByPrefix("documentTypeParentFields.").getAsMap();
		documentTypeRoutingFields = couchbase.getByPrefix("documentTypeRoutingFields.").getAsMap();
	}

	BucketContext(final BucketContext context, final Settings bucket)
	{
		uuid = bucket.get("uuid");
		type = bucket.get("type", context.type);
		defaultDocumentType = bucket.get("defaultDocumentType", context.defaultDocumentType);
		resolveConflicts = bucket.getAsBoolean("resolveConflicts", context.resolveConflicts);
		bulkIndexRetries = bucket.getAsInt("bulkIndexRetries", context.bulkIndexRetries);
		bulkIndexRetryWaitMs = bucket.getAsInt("bulkIndexRetryWaitMs", context.bulkIndexRetryWaitMs);
		timeBasedIndex = bucket.getAsBoolean("timeBasedIndex", context.timeBasedIndex);

		documentTypeParentFields = bucket.getByPrefix("documentTypeParentFields.").getAsMap();
		documentTypeRoutingFields = bucket.getByPrefix("documentTypeRoutingFields.").getAsMap();
	}
}
