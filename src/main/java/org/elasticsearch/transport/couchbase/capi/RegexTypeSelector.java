package org.elasticsearch.transport.couchbase.capi;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RegexTypeSelector implements TypeSelector
{
	protected final ESLogger logger = Loggers.getLogger(getClass());

	private String defaultDocumentType;
	private boolean timeBasedIndex;
	private Map<String, Pattern> documentTypePatterns;

	@Override
	public void configure(final Settings settings)
	{
		timeBasedIndex = settings.getAsBoolean("couchbase.timeBasedIndex", false);
		defaultDocumentType = settings
			.get("couchbase.defaultDocumentType", DefaultTypeSelector.DEFAULT_DOCUMENT_TYPE_DOCUMENT);

		documentTypePatterns = new HashMap<String, Pattern>();

		final Map<String, String> documentTypePatternStrings = settings
			.getByPrefix("couchbase.documentTypes.").getAsMap();

		for (final Map.Entry<String, String> e : documentTypePatternStrings.entrySet())
		{
			if (logger.isInfoEnabled())
				logger.info("See document type: {} with pattern: {} compiling...",
					e.getKey(), e.getValue());

			documentTypePatterns.put(e.getKey(), Pattern.compile(e.getValue()));
		}
	}

	@Override
	public String getType(final String index, final String docId)
	{
		for (final Map.Entry<String, Pattern> typePattern : documentTypePatterns.entrySet())
		{
			if (typePattern.getValue().matcher(docId).matches())
			{
				return typePattern.getKey();
			}
		}

		return defaultDocumentType;
	}

	@Override
	public boolean timeBasedIndex()
	{
		return timeBasedIndex;
	}
}
