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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class MDRIndex extends ElasticIndex
{
    private static final String EMPTY_STRING = "";
    private static final String OBJ_CLASSES = "obj_classes";

	MDRIndex(
		final String bucket,
		final BucketContext context,
		final Client client,
		final ESLogger logger)
	{
		super(bucket, context, client, logger);
	}

	@Override
	protected final boolean shouldIndex(final String id)
	{

        return id.indexOf('#') < 0;
	}

    /*
    Looks at the json object to deduce the body.
    For subclassed objects, type is a concatenation
    */
    @SuppressWarnings("unchecked")
    private final String getTypeForMDRDocument(final Map<String, Object> json)
    {
        final Object o = json.get(OBJ_CLASSES);
        String type = EMPTY_STRING;
        if( o == null)
        {
            logger.warn("Unable to determine type for json {}", json);
            return type;
        }

        // TODO We should come up with a better naming scheme when database for MDR is resolved.
        for(final String s : (ArrayList<String>) o)
        {
            type += s.trim().replace(' ', '-');
        }

        if(logger.isDebugEnabled()) {
            logger.debug("Type data: {}", o.getClass());
            logger.debug("Type value: {}", type);
        }

        return type;
    }

    @Override
    void prepareIndexStatements(
            final List<Map<String, Object>> docs,
            final Map<String, String> revisions,
            final Map<String, IndexRequest> bulkIndexRequests,
            final Map<String, DeleteRequest> bulkDeleteRequests)
    {
        for (final Map<String, Object> doc : docs)
        {
            // these are the top-level elements that could be in the document sent by Couchbase
            @SuppressWarnings("unchecked") final Map<String, Object> meta = (Map<String, Object>) doc.get(META);
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
                        final String indexName = indexName(id);
                        final Map<String, Object> json = getDoc(doc, meta);

                        final String type = getTypeForMDRDocument(json);
                        if(type.equals(EMPTY_STRING))
                        {
                            logger.warn("Unable to determine type for doc {} {}", id, json);
                            continue;
                        }

                        final IndexRequestBuilder builder = client.prepareIndex(indexName, type, id);

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
}
