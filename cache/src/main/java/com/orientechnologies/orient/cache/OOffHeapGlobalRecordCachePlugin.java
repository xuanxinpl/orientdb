/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.cache;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OGlobalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Global cache contained in the resource space of OStorage and shared by all database instances that work on top of that Storage.
 * 
 * @author Luca Garulli
 */
public class OOffHeapGlobalRecordCachePlugin extends OServerPluginAbstract implements ODatabaseLifecycleListener {
  final ConcurrentHashMap<String, OOffHeapGlobalRecordCache> caches = new ConcurrentHashMap<String, OOffHeapGlobalRecordCache>();

  @Override
  public void startup() {
    OLogManager.instance().info(this, "Starting off-heap cache...");
    Orient.instance().addDbLifecycleListener(this);
    caches.clear();
  }

  @Override
  public void shutdown() {
    OLogManager.instance().info(this, "Shutting down off-heap cache...");
    Orient.instance().removeDbLifecycleListener(this);
    for (OOffHeapGlobalRecordCache c : caches.values())
      c.clear();
    caches.clear();
  }

  @Override
  public String getName() {
    return "offheap-cache";
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LATE;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    installGlobalCache(iDatabase);
  }

  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    installGlobalCache(iDatabase);
  }

  protected synchronized void installGlobalCache(ODatabaseInternal iDatabase) {
    final OGlobalRecordCache gc = iDatabase.getMetadata().getGlobalCache();
    if (!(gc instanceof OOffHeapGlobalRecordCache))
      ((OMetadataInternal) iDatabase.getMetadata()).replaceGlobalCache(getDatabaseCache(iDatabase.getName()));
  }

  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, OClass iClass) {
  }

  @Override
  public void onDropClass(final ODatabaseInternal iDatabase, OClass iClass) {
  }

  private OGlobalRecordCache getDatabaseCache(final String name) {
    OOffHeapGlobalRecordCache cache = caches.get(name);
    if (cache == null) {
      cache = new OOffHeapGlobalRecordCache();
      caches.putIfAbsent(name, cache);
    }
    return cache;
  }
}
