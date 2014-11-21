package org.elasticsearch.transport.couchbase.capi;

/**
 * A simple extension of the DefaultTypeSelector, which uses 'type':'id' pattern to encode the
 * elasticsearch type.
 */
public class SimpleTypeSelector extends DefaultTypeSelector
{
	@Override
	public String getType(final String index, final String docId)
	{
		final int pos = docId.indexOf(':');

		return pos > 0 ? docId.substring(0, pos) : super.getType(index, docId);
	}
}
