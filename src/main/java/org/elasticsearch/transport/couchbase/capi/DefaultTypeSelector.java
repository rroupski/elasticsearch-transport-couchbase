package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.settings.Settings;

public class DefaultTypeSelector implements TypeSelector
{

	public static final String DEFAULT_DOCUMENT_TYPE_DOCUMENT = "couchbaseDocument";
	public static final String DEFAULT_DOCUMENT_TYPE_CHECKPOINT = "couchbaseCheckpoint";

	private String defaultDocumentType;
	private String checkpointDocumentType;
	private boolean timeBasedIndex;

	@Override
	public void configure(final Settings settings)
	{
		defaultDocumentType = settings.get("couchbase.defaultDocumentType", DEFAULT_DOCUMENT_TYPE_DOCUMENT);
		checkpointDocumentType = settings.get("couchbase.checkpointDocumentType", DEFAULT_DOCUMENT_TYPE_CHECKPOINT);
		timeBasedIndex = settings.getAsBoolean("couchbase.timeBasedIndex", false);
	}

	@Override
	public String getType(final String index, final String docId)
	{
		return docId.startsWith("_local/") ? checkpointDocumentType : defaultDocumentType;
	}

	@Override
	public boolean timeBasedIndex()
	{
		return timeBasedIndex;
	}
}
