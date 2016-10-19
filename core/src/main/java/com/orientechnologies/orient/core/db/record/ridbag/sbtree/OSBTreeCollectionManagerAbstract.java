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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public abstract class OSBTreeCollectionManagerAbstract implements OCloseable, OSBTreeCollectionManager {
  public static final String FILE_NAME_PREFIX  = "collections_";
  public static final String DEFAULT_EXTENSION = ".sbc";

  /**
   * Generates a lock name for the given cluster ID.
   *
   * @param clusterId
   *          the cluster ID to generate the lock name for.
   *
   * @return the generated lock name.
   */
  public static String generateLockName(int clusterId) {
    return FILE_NAME_PREFIX + clusterId + DEFAULT_EXTENSION;
  }

  protected final int                                                                    evictionThreshold;
  protected final int                                                                    cacheMaxSize;
  private final ConcurrentLinkedHashMap<OBonsaiCollectionPointer, SBTreeBonsaiContainer> treeCache = new ConcurrentLinkedHashMap.Builder<OBonsaiCollectionPointer, SBTreeBonsaiContainer>()
      .maximumWeightedCapacity(Long.MAX_VALUE).build();

  public OSBTreeCollectionManagerAbstract() {
    this(OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger(),
        OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger());
  }

  public OSBTreeCollectionManagerAbstract(int evictionThreshold, int cacheMaxSize) {
    this.evictionThreshold = evictionThreshold;
    this.cacheMaxSize = cacheMaxSize;
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(int clusterId) {
    return loadSBTree(createSBTree(clusterId, null));
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID) {
    OSBTreeBonsai<OIdentifiable, Integer> tree = createTree(clusterId);
    return tree.getCollectionPointer();
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(OBonsaiCollectionPointer collectionPointer) {
    final OSBTreeBonsai<OIdentifiable, Integer> tree;

    SBTreeBonsaiContainer container = treeCache.get(collectionPointer);
    if (container != null) {
      container.usagesCounter.incrementAndGet();
      tree = container.tree;
    } else {
      tree = loadTree(collectionPointer);
      if (tree != null) {
        assert tree.getRootBucketPointer().equals(collectionPointer.getRootPointer());

        container = new SBTreeBonsaiContainer(tree);
        container.usagesCounter.incrementAndGet();

        treeCache.putIfAbsent(collectionPointer, container);
      }
    }

    if (tree != null)
      evict();

    return tree;
  }

  @Override
  public void releaseSBTree(OBonsaiCollectionPointer collectionPointer) {
    final SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
    assert container != null;
    container.usagesCounter.decrementAndGet();
    assert container.usagesCounter.get() >= 0;

    evict();
  }

  @Override
  public void delete(OBonsaiCollectionPointer collectionPointer) {
    SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
    assert container != null;

    if (container.usagesCounter.get() != 0)
      throw new IllegalStateException("Cannot delete SBTreeBonsai instance because it is used in other thread.");

    treeCache.remove(collectionPointer);
  }

  private void evict() {
    if (treeCache.size() <= cacheMaxSize)
      return;

    synchronized (this) {
      if (treeCache.size() <= cacheMaxSize)
        return;

      for (OBonsaiCollectionPointer collectionPointer : treeCache.ascendingKeySetWithLimit(evictionThreshold)) {
        SBTreeBonsaiContainer container = treeCache.getQuietly(collectionPointer);
        if (container != null && container.usagesCounter.get() == 0)
          treeCache.remove(collectionPointer);
      }
    }
  }

  @Override
  public void close() {
    treeCache.clear();
  }

  public void clear() {
    treeCache.clear();
  }

  protected abstract OSBTreeBonsai<OIdentifiable, Integer> createTree(int clusterId);

  protected abstract OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer);

  int size() {
    return treeCache.size();
  }

  private static final class SBTreeBonsaiContainer {
    private final OSBTreeBonsai<OIdentifiable, Integer> tree;
    private AtomicInteger                               usagesCounter = new AtomicInteger(0);

    private SBTreeBonsaiContainer(OSBTreeBonsai<OIdentifiable, Integer> tree) {
      this.tree = tree;
    }
  }
}
