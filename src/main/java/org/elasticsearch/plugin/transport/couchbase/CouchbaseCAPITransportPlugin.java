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
package org.elasticsearch.plugin.transport.couchbase;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.transport.couchbase.CouchbaseCAPI;
import org.elasticsearch.transport.couchbase.CouchbaseCAPIModule;

import java.util.Collection;

import static org.elasticsearch.common.collect.Lists.newArrayList;

public class CouchbaseCAPITransportPlugin extends AbstractPlugin
{

	private final Settings settings;

	public CouchbaseCAPITransportPlugin(final Settings settings)
	{
		this.settings = settings;
	}

	@Override
	public String name()
	{
		return "transport-couchbase";
	}

	@Override
	public String description()
	{
		return "Couchbase Transport";
	}

	@Override
	public Collection<Class<? extends Module>> modules()
	{
		final Collection<Class<? extends Module>> modules = newArrayList();
		if (settings.getAsBoolean("couchbase.enabled", true))
		{
			modules.add(CouchbaseCAPIModule.class);
		}

		return modules;
	}

	@Override
	public Collection<Class<? extends LifecycleComponent>> services()
	{
		final Collection<Class<? extends LifecycleComponent>> services = newArrayList();
		if (settings.getAsBoolean("couchbase.enabled", true))
		{
			services.add(CouchbaseCAPI.class);
		}

		return services;
	}
}
