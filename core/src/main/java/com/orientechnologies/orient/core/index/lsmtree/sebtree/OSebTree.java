/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.index.lsmtree.sebtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.index.OAlwaysGreaterKey;
import com.orientechnologies.orient.core.index.OAlwaysLessKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.lsmtree.*;
import com.orientechnologies.orient.core.index.lsmtree.encoders.OEncoder;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of SB-Tree (Sequentially Efficient B-tree) by Patrick E. O'Neil.
 *
 * @author Sergey Sitnikov
 */
public class OSebTree<K, V> extends ODurableComponent implements OTree<K, V> {

  /* internal */ static final int ENCODERS_VERSION             = 0;
  /* internal */ static final int BLOCK_SIZE                   = 16 /* pages, must be even */;
  /* internal */ static final int INLINE_KEYS_SIZE_THRESHOLD   = 16 /* bytes */;
  /* internal */ static final int INLINE_VALUES_SIZE_THRESHOLD = 10 /* bytes */;

  private static final int BLOCK_HALF                = BLOCK_SIZE / 2;
  private static final int IN_MEMORY_PAGES_THRESHOLD = 64 * 1024 * 1024 / OSebTreeNode.MAX_PAGE_SIZE_BYTES;

  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  private boolean inMemory;
  private boolean full = false;

  private OEncoder.Provider<K> keyProvider;
  private OEncoder.Provider<V> valueProvider;
  private OBinarySerializer<K> keySerializer;

  private OType[] keyTypes;
  private int     keySize;
  private boolean nullKeyAllowed;

  private long fileId;

  private boolean opened           = false;
  private String  currentOperation = null;

  public OSebTree(OAbstractPaginatedStorage storage, String name, String extension, boolean inMemory) {
    super(storage, name, extension, name + extension);
    this.inMemory = inMemory;
  }

  public void create(OBinarySerializer<K> keySerializer, OType[] keyTypes, int keySize, boolean nullKeyAllowed,
      OBinarySerializer<V> valueSerializer) {
    assert !opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation("creation", false);
      try {
        acquireExclusiveLock();
        try {
          this.keySerializer = keySerializer;
          this.keyProvider = selectKeyProvider(keySerializer, keyTypes);
          this.keyTypes = keyTypes == null ? null : Arrays.copyOf(keyTypes, keyTypes.length);
          this.keySize = keySize;
          this.nullKeyAllowed = nullKeyAllowed;
          this.valueProvider = selectValueProvider(valueSerializer);

          fileId = addFile(atomicOperation, getFullName());

          if (nullKeyAllowed) {
            final OSebTreeNode<K, V> nullNode = createNode(atomicOperation).beginCreate();
            try {
              nullNode.create(true);
            } finally {
              releaseNode(atomicOperation, nullNode.endWrite());
            }
          }

          final OSebTreeNode<K, V> rootNode = createNode(atomicOperation).beginCreate();
          try {
            rootNode.create(true);
          } finally {
            releaseNode(atomicOperation, rootNode.endWrite());
          }

          opened = true;
        } finally {
          releaseExclusiveLock();
        }

        endSuccessfulAtomicOperation();
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  public void open(OBinarySerializer<K> keySerializer, OType[] keyTypes, int keySize, boolean nullKeyAllowed,
      OBinarySerializer<V> valueSerializer) {
    assert !opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      acquireExclusiveLock();
      try {
        this.keySerializer = keySerializer;
        this.keyProvider = selectKeyProvider(keySerializer, keyTypes);
        this.keyTypes = keyTypes;
        this.keySize = keySize;
        this.nullKeyAllowed = nullKeyAllowed;
        this.valueProvider = selectValueProvider(valueSerializer);

        fileId = openFile(atomicOperation(), getFullName());
      } finally {
        releaseExclusiveLock();
      }
    } catch (IOException e) {
      throw error("Exception while opening of SebTree " + getName(), e);
    } finally {
      end(statistic);
    }
  }

  public void close() {
    assert opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      acquireExclusiveLock();
      try {
        readCache.closeFile(fileId, true, writeCache);
        opened = false;
      } catch (IOException e) {
        throw error("Exception while closing of SebTree " + getName(), e);
      } finally {
        releaseExclusiveLock();
      }
    } finally {
      end(statistic);
    }
  }

  public void reset() {
    assert opened;

    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation("reset", true);
      try {
        acquireExclusiveLock();
        try {
          truncateFile(atomicOperation, fileId);

          if (nullKeyAllowed) {
            final OSebTreeNode<K, V> nullNode = createNode(atomicOperation).beginCreate();
            try {
              nullNode.create(true);
            } finally {
              releaseNode(atomicOperation, nullNode.endWrite());
            }
          }

          final OSebTreeNode<K, V> rootNode = createNode(atomicOperation).beginCreate();
          try {
            rootNode.create(true);
          } finally {
            releaseNode(atomicOperation, rootNode.endWrite());
          }
        } finally {
          releaseExclusiveLock();
        }

        endSuccessfulAtomicOperation();
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  public void delete() {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      final OAtomicOperation atomicOperation = startAtomicOperation("delete", false);
      try {
        acquireExclusiveLock();
        try {
          if (opened)
            deleteFile(atomicOperation, fileId);
          else if (isFileExists(atomicOperation, getFullName())) {
            final long fileId = openFile(atomicOperation, getFullName());
            deleteFile(atomicOperation, fileId);
          }
        } finally {
          releaseExclusiveLock();
        }

        endSuccessfulAtomicOperation();
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public long size() {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return size(atomicOperation());
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean contains(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return contains(atomicOperation(), internalKey(key));
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public V get(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return get(atomicOperation(), internalKey(key));
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public V get(K key, OModifiableBoolean found) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return get(atomicOperation(), internalKey(key), found);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean put(K key, V value) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      startAtomicOperation("put", true);
      try {
        final boolean result = putValue(atomicOperation(), internalKey(key), value);
        endSuccessfulAtomicOperation();
        return result;
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public boolean remove(K key) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      startAtomicOperation("remove", true);
      try {
        final boolean result = remove(atomicOperation(), internalKey(key));
        endSuccessfulAtomicOperation();
        return result;
      } catch (Exception e) {
        throw endFailedAtomicOperation(e);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public K firstKey() {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return firstKey(atomicOperation());
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public K lastKey() {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return lastKey(atomicOperation());
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public OKeyValueCursor<K, V> range(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, true,
            internalRangeKey(beginningKey, true, beginning == OCursor.Beginning.Inclusive),
            internalRangeKey(endKey, false, end == OCursor.End.Inclusive), beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public OKeyCursor<K> keyRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, false,
            internalRangeKey(beginningKey, true, beginning == OCursor.Beginning.Inclusive),
            internalRangeKey(endKey, false, end == OCursor.End.Inclusive), beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  @Override
  public OValueCursor<V> valueRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end,
      OCursor.Direction direction) {
    final OSessionStoragePerformanceStatistic statistic = start();
    try {
      atomicOperationsManager.acquireReadLock(this);
      try {
        return new Cursor<>(atomicOperation(), this, true,
            internalRangeKey(beginningKey, true, beginning == OCursor.Beginning.Inclusive),
            internalRangeKey(endKey, false, end == OCursor.End.Inclusive), beginning, end, direction);
      } finally {
        atomicOperationsManager.releaseReadLock(this);
      }
    } finally {
      end(statistic);
    }
  }

  /* internal */ void dump() throws IOException {
    final OSebTreeNode<K, V> root = getRootNode(null).beginRead();
    try {
      System.out.println("Size: " + root.getTreeSize());
      dump(root, 0);
    } finally {
      releaseNode(null, root.endRead());
    }
  }

  private OEncoder.Provider<K> selectKeyProvider(OBinarySerializer<K> keySerializer, OType[] keyTypes) {
    return OEncoder.runtime().getProviderForKeySerializer(keySerializer, keyTypes, OEncoder.Size.PreferFixed);
  }

  private OEncoder.Provider<V> selectValueProvider(OBinarySerializer<V> valueSerializer) {
    return OEncoder.runtime().getProviderForValueSerializer(valueSerializer, OEncoder.Size.PreferVariable);
  }

  private OSessionStoragePerformanceStatistic start() {
    final OSessionStoragePerformanceStatistic statistic = performanceStatisticManager.getSessionPerformanceStatistic();
    if (statistic != null)
      statistic.startComponentOperation(getFullName(), OSessionStoragePerformanceStatistic.ComponentType.INDEX);
    return statistic;
  }

  private void end(OSessionStoragePerformanceStatistic statistic) {
    if (statistic != null)
      statistic.completeComponentOperation();
  }

  private OSebTreeNode<K, V> createNode(OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry page = addPage(atomicOperation, fileId);
    if (inMemory && !full)
      pinPage(atomicOperation, page);
    //    System.out.println("* " + page.getPageIndex());
    return new OSebTreeNode<>(page, keyProvider, valueProvider);
  }

  private Creation<K, V> createNode(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, int rank,
      OSebTreeNode<K, V> node, K key) throws IOException {

    final OSebTreeNode<K, V> parent = path.get(path.size() - rank - 2).beginWrite();
    boolean blockSplit = false;
    try {
      final int searchIndex = parent.indexOf(key);
      final OSebTreeNode.Marker marker = parent.nearestMarker(searchIndex);

      if (marker.blockPagesUsed < BLOCK_SIZE) {
        updateMarkersOnCreate(atomicOperation, parent, marker);
        return new Creation<>(node, getNode(atomicOperation, marker.blockIndex + marker.blockPagesUsed));
      }

      final Creation<K, V> creation = splitBlock(atomicOperation, path, rank, node, parent, marker, searchIndex, key);
      blockSplit = true;
      return creation;
    } finally {
      if (!blockSplit) // block split may change the parent, so it managed while splitting
        parent.endWrite();
    }
  }

  private void updateMarkersOnCreate(OAtomicOperation atomicOperation, OSebTreeNode<K, V> parent, OSebTreeNode.Marker marker)
      throws IOException {

    parent.updateMarker(marker.index, marker.blockPagesUsed + 1);
    //parent.verifyNonLeaf();

    if ((parent.isContinuedFrom() && marker.index == 0)) {
      long siblingPointer = parent.getLeftSibling();
      while (true) {
        final OSebTreeNode<K, V> siblingParent = getNode(atomicOperation, siblingPointer).beginWrite();
        try {
          siblingParent.updateMarker(siblingParent.getMarkerCount() - 1, marker.blockPagesUsed + 1);
          if (!(siblingParent.isContinuedFrom() && siblingParent.getMarkerCount() == 1))
            break;

          //siblingParent.verifyNonLeaf();

          siblingPointer = siblingParent.getLeftSibling();
        } finally {
          releaseNode(atomicOperation, siblingParent.endWrite());
        }
      }
    }

    if ((parent.isContinuedTo() && marker.index == parent.getMarkerCount() - 1)) {
      long siblingPointer = parent.getRightSibling();
      while (true) {
        final OSebTreeNode<K, V> siblingParent = getNode(atomicOperation, siblingPointer).beginWrite();
        try {
          siblingParent.updateMarker(0, marker.blockPagesUsed + 1);
          if (!(siblingParent.isContinuedTo() && siblingParent.getMarkerCount() == 1))
            break;

          //siblingParent.verifyNonLeaf();

          siblingPointer = siblingParent.getRightSibling();
        } finally {
          releaseNode(atomicOperation, siblingParent.endWrite());
        }
      }
    }
  }

  private Creation<K, V> splitBlock(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, int rank,
      OSebTreeNode<K, V> node, OSebTreeNode<K, V> parent, OSebTreeNode.Marker marker, int keyIndex, K key) throws IOException {

    final Block block = collectBlockInformation(atomicOperation, parent, marker, keyIndex);
    final long[] oldBlockPointers = block.pointers;
    final long[] newBlockPointers = new long[BLOCK_SIZE];
    final long newBlock = allocateBlock(atomicOperation);

    // Move the right part to the new block.

    OSebTreeNode<K, V> nodeReplacement = node;

    for (int i = 0; i < BLOCK_HALF; ++i) {
      final long oldPageIndex = oldBlockPointers[BLOCK_HALF + i];
      final long newPageIndex = newBlockPointers[BLOCK_HALF + i] = newBlock + i;

      final OSebTreeNode<K, V> oldNode =
          oldPageIndex == node.getPointer() ? node : getNode(atomicOperation, oldPageIndex).beginCreate();
      try {
        final OSebTreeNode<K, V> newNode = getNode(atomicOperation, newPageIndex).beginCreate();
        try {
          newNode.cloneFrom(oldNode);
        } finally {
          if (node == oldNode) {
            nodeReplacement = newNode;
            path.set(path.size() - rank - 1, nodeReplacement);
            nodeReplacement.endWrite().beginWrite(); // reinitialize the node with a new data
          } else
            releaseNode(atomicOperation, newNode.endWrite());
        }
      } finally {
        if (oldNode != node || nodeReplacement != node)
          releaseNode(atomicOperation, oldNode.endWrite());
      }
    }

    // "Collapse-left".

    final boolean[] usedPagesMap = new boolean[BLOCK_HALF];
    for (int i = 0; i < BLOCK_HALF; ++i) {
      final int index = (int) (oldBlockPointers[i] - marker.blockIndex);
      if (index < BLOCK_HALF) {
        usedPagesMap[index] = true;
        newBlockPointers[i] = oldBlockPointers[i];
      }
    }

    int lastFree = 0;
    for (int i = 0; i < BLOCK_HALF; ++i) {
      if (newBlockPointers[i] == 0) {
        final long oldPageIndex = oldBlockPointers[i];

        for (int j = lastFree; ; ++j)
          if (!usedPagesMap[j]) {
            final long newPageIndex = newBlockPointers[i] = marker.blockIndex + j;

            final OSebTreeNode<K, V> oldNode =
                oldPageIndex == node.getPointer() ? node : getNode(atomicOperation, oldPageIndex).beginCreate();
            try {
              final OSebTreeNode<K, V> newNode = getNode(atomicOperation, newPageIndex).beginCreate();
              try {
                newNode.cloneFrom(oldNode);
              } finally {
                if (node == oldNode) {
                  nodeReplacement = newNode;
                  path.set(path.size() - rank - 1, nodeReplacement);
                  nodeReplacement.endWrite().beginWrite(); // reinitialize the node with a new data
                } else
                  releaseNode(atomicOperation, newNode.endWrite());
              }
            } finally {
              if (oldNode != node || nodeReplacement != node)
                releaseNode(atomicOperation, oldNode.endWrite());
            }

            lastFree = j + 1;
            break;
          }
      }
    }

    // Update pointers.

    relinkNodes(atomicOperation, oldBlockPointers, newBlockPointers, nodeReplacement);

    // Update markers.

    updateMarkersOnSplit(atomicOperation, newBlockPointers, oldBlockPointers, block.targetNodeIndex, block.parents, parent,
        block.markers, newBlock, path, rank, key);

    final boolean targetInNewBlock = block.targetNodeIndex >= BLOCK_HALF;
    final OSebTreeNode<K, V> newNode = getNode(atomicOperation,
        targetInNewBlock ? newBlock + BLOCK_HALF : marker.blockIndex + BLOCK_HALF);
    return new Creation<>(nodeReplacement, newNode);
  }

  private Block collectBlockInformation(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, OSebTreeNode.Marker marker,
      int keyIndex) throws IOException {

    final List<Long> pointers = new ArrayList<>(BLOCK_SIZE /* exact size */);
    final List<OSebTreeNode.Marker> markers = new ArrayList<>(BLOCK_SIZE /* just a guess */);
    final List<Long> parents = new ArrayList<>(BLOCK_SIZE /* just a guess */);

    final long targetPointer = node.pointerAt(keyIndex);

    pointers.add(node.pointerAt(marker.pointerIndex));
    markers.add(marker);
    parents.add(node.getPointer());
    final int lastPointerIndexOfMarker = node.getLastPointerIndexOfMarkerAt(marker.index);
    for (int i = marker.pointerIndex + 1; i <= lastPointerIndexOfMarker; ++i)
      pointers.add(node.pointerAt(i));

    if (node.isContinuedFrom() && marker.index == 0) {
      long siblingPointer = node.getLeftSibling();
      while (true) {
        final OSebTreeNode<K, V> sibling = getNode(atomicOperation, siblingPointer).beginRead();
        try {
          final OSebTreeNode.Marker siblingMarker = sibling.markerAt(sibling.getMarkerCount() - 1);

          pointers.add(0, sibling.pointerAt(siblingMarker.pointerIndex));
          markers.add(0, siblingMarker);
          parents.add(0, sibling.getPointer());
          final int lastPointerIndexOfSiblingMarker = sibling.getLastPointerIndexOfMarkerAt(siblingMarker.index);
          int added = 1;
          for (int j = siblingMarker.pointerIndex + 1; j <= lastPointerIndexOfSiblingMarker; ++j)
            pointers.add(added++, sibling.pointerAt(j));

          if (!(sibling.isContinuedFrom() && sibling.getMarkerCount() == 1))
            break;

          siblingPointer = sibling.getLeftSibling();
        } finally {
          releaseNode(atomicOperation, sibling.endRead());
        }
      }
    }

    if (node.isContinuedTo() && marker.index == node.getMarkerCount() - 1) {
      long siblingPointer = node.getRightSibling();
      while (true) {
        final OSebTreeNode<K, V> sibling = getNode(atomicOperation, siblingPointer).beginRead();
        try {
          final OSebTreeNode.Marker siblingMarker = sibling.markerAt(0);

          pointers.add(sibling.pointerAt(siblingMarker.pointerIndex));
          markers.add(siblingMarker);
          parents.add(sibling.getPointer());
          final int lastPointerIndexOfSiblingMarker = sibling.getLastPointerIndexOfMarkerAt(siblingMarker.index);
          for (int j = siblingMarker.pointerIndex + 1; j <= lastPointerIndexOfSiblingMarker; ++j)
            pointers.add(sibling.pointerAt(j));

          if (!(sibling.isContinuedTo() && sibling.getMarkerCount() == 1))
            break;

          siblingPointer = sibling.getRightSibling();
        } finally {
          releaseNode(atomicOperation, sibling.endRead());
        }
      }
    }

    assert pointers.size() == BLOCK_SIZE;
    assert parents.size() == markers.size();
    return new Block(pointers, parents, markers, targetPointer);
  }

  @SuppressWarnings("ConstantConditions")
  private void updateMarkersOnSplit(OAtomicOperation atomicOperation, long[] newPointers, long[] oldPointers, int targetNodeIndex,
      long[] parents, OSebTreeNode<K, V> parentOnPath, OSebTreeNode.Marker[] markers, long newBlock, List<OSebTreeNode<K, V>> path,
      int rank, K key) throws IOException {

    final boolean targetInNewBlock = targetNodeIndex >= BLOCK_HALF;

    OSebTreeNode<K, V> middleMarkerNode = null;
    int middleMarkerPointerIndex = 0;
    K middleMarkerPointerKey = null;

    int pointer = 0;
    for (int i = 0; i < parents.length; ++i) {
      final long parentPointer = parents[i];
      final OSebTreeNode.Marker marker = markers[i];
      final boolean onPath = parentPointer == parentOnPath.getPointer();

      final OSebTreeNode<K, V> node = onPath ? parentOnPath : getNode(atomicOperation, parentPointer).beginWrite();
      try {
        if (pointer >= BLOCK_HALF)
          node.updateMarker(marker.index, newBlock, targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF);
        else
          node.updateMarker(marker.index, !targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF);

        final int lastPointerIndexOfMarker = node.getLastPointerIndexOfMarkerAt(marker.index);
        for (int j = marker.pointerIndex; j <= lastPointerIndexOfMarker; ++j) {
          if (pointer == BLOCK_HALF) { // insert new middle marker if needed
            if (j != marker.pointerIndex) { // it's needed
              assert j != -1; // if it's first, the index should be equal to marker pointer index
              middleMarkerNode = node;
              middleMarkerPointerIndex = j;
              middleMarkerPointerKey = node.keyAt(j);
            } else if (j == -1) // first marker in the node, the marker itself is already updated
              node.setContinuedFrom(false);
          }

          if (oldPointers[pointer] != newPointers[pointer])
            node.updatePointer(j, newPointers[pointer]);

          ++pointer;
        }

        if (pointer == BLOCK_HALF) // next node will have new middle marker at the beginning
          node.setContinuedTo(false);

      } finally {
        //        if (middleMarkerNode != node)
        //          node.verifyNonLeaf();

        if (!onPath && middleMarkerNode != node)
          releaseNode(atomicOperation, node.endWrite());
      }
    }

    try {
      if (middleMarkerNode != null)
        insertMarker(atomicOperation, middleMarkerNode, middleMarkerPointerIndex, middleMarkerPointerKey, newBlock,
            targetInNewBlock ? BLOCK_HALF + 1 : BLOCK_HALF, path, rank, key);
    } finally {
      path.get(path.size() - rank - 2).endWrite();
    }
  }

  private void insertMarker(OAtomicOperation atomicOperation, OSebTreeNode<K, V> markerNode, int pointerIndex, K pointerKey,
      long blockIndex, int blockPagesUsed, List<OSebTreeNode<K, V>> path, int rank, K key) throws IOException {

    // Fast path.

    if (markerNode.markerFits()) {
      markerNode.insertMarkerForPointerAt(pointerIndex, blockIndex, blockPagesUsed);
      //markerNode.verifyNonLeaf();

      if (markerNode != path.get(path.size() - rank - 2))
        releaseNode(atomicOperation, markerNode.endWrite());
      return;
    }

    // General case. First, find marker path and merge it with the original key path.

    System.out.println("marker split");

    final List<OSebTreeNode<K, V>> markerPath = new ArrayList<>(16);
    findLeafWithPath(atomicOperation, pointerKey, markerPath);
    assert markerPath.size() == path.size();
    assert markerPath.get(markerPath.size() - rank - 2).getPointer() == markerNode.getPointer();

    if (markerNode != path.get(path.size() - rank - 2)) {
      releaseNode(atomicOperation, markerPath.get(markerPath.size() - rank - 2));
      markerPath.set(markerPath.size() - rank - 2, markerNode);
    }

    for (int i = 0; i < path.size(); ++i)
      if (path.get(i).getPointer() == markerPath.get(i).getPointer()) {
        releaseNode(atomicOperation, markerPath.get(i));
        markerPath.set(i, path.get(i));
      } else
        break;

    // Split and insert.

    try {
      markerSplitInsert(atomicOperation, markerNode, pointerIndex, markerPath, path, rank + 1, key, pointerKey, blockIndex,
          blockPagesUsed);
    } finally {
      markerNode = markerPath.get(markerPath.size() - rank - 2);
      if (markerNode != path.get(path.size() - rank - 2))
        markerNode.endWrite();
    }

    // Release nodes that are on marker path only, we don't need them anymore.

    assert markerPath.size() == path.size();
    for (int i = 0; i < path.size(); ++i) {
      final OSebTreeNode<K, V> nodeOnMarkerPath = markerPath.get(i);
      if (nodeOnMarkerPath != path.get(i))
        releaseNode(atomicOperation, nodeOnMarkerPath);
    }
  }

  private void markerSplitInsert(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, int pointerIndex,
      List<OSebTreeNode<K, V>> markerPath, List<OSebTreeNode<K, V>> valuePath, int rank, K valueKey, K markerKey, long blockIndex,
      int blockPagesUsed) throws IOException {
    assert !node.isLeaf();
    assert pointerIndex >= 0; // new middle marker can't be inserted for the left pointer, since left pointer always has it

    if (isRoot(node))
      markerSplitInsertRoot(atomicOperation, node, pointerIndex, markerPath, valuePath, valueKey, blockIndex, blockPagesUsed);
    else
      markerSplitInsertNonRoot(atomicOperation, node, pointerIndex, markerPath, valuePath, rank, valueKey, markerKey, blockIndex,
          blockPagesUsed);
  }

  private void markerSplitInsertRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, int pointerIndex,
      List<OSebTreeNode<K, V>> markerPath, List<OSebTreeNode<K, V>> valuePath, K valueKey, long blockIndex, int blockPagesUsed)
      throws IOException {
    final long newBlockIndex = allocateBlock(atomicOperation);

    OSebTreeNode<K, V> newKeyNode = null;
    OSebTreeNode<K, V> newMarkerNode = null;
    final int newMarkerPointerIndex;

    final OSebTreeNode<K, V> leftNode = getNode(atomicOperation, newBlockIndex).beginCreate();
    try {
      leftNode.create(false);
      final OSebTreeNode<K, V> rightNode = getNode(atomicOperation, newBlockIndex + 1).beginCreate();
      try {
        rightNode.create(false);

        // Determine split point.

        final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
        final int halfIndex = node.getSize() - entriesToMove;

        final K separator = node.keyAt(halfIndex);
        final int rightNodeSize = entriesToMove - 1;

        if (pointerIndex < halfIndex) {
          newMarkerNode = leftNode;
          newMarkerPointerIndex = pointerIndex;
        } else {
          newMarkerNode = rightNode;
          newMarkerPointerIndex = pointerIndex - halfIndex - 1;
        }

        // Deduce new key node.

        newKeyNode = OSebTreeNode.compareKeys(valueKey, separator) < 0 ? leftNode : rightNode;

        // Build right node.

        final int separatorSize = node.getKeyEncoder().exactSize(separator);
        node.moveTailTo(rightNode, rightNodeSize);
        rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

        if (newMarkerPointerIndex == -1) {
          rightNode.insertMarker(0, -1, blockIndex, blockPagesUsed);
          leftNode.setContinuedTo(false);
          rightNode.setContinuedFrom(false);
        } else {
          ((newMarkerNode == rightNode) ? rightNode : node)
              .insertMarkerForPointerAt(newMarkerPointerIndex, blockIndex, blockPagesUsed); // like it already were here
          final OSebTreeNode.Marker rightMarker = node.markerAt(node.getMarkerCount() - 1);
          rightNode.insertMarker(0, -1, rightMarker.blockIndex, rightMarker.blockPagesUsed);
          if (rightMarker.pointerIndex == node.getSize() - 1) {
            leftNode.setContinuedTo(false);
            rightNode.setContinuedFrom(false);
          } else {
            leftNode.setContinuedTo(true);
            rightNode.setContinuedFrom(true);
          }
        }

        node.delete(node.getSize() - 1, separatorSize, node.getPointerEncoder().maximumSize());

        // Build left node.

        node.moveTailTo(leftNode, node.getSize());

        leftNode.setRightSibling(rightNode.getPointer());
        rightNode.setLeftSibling(leftNode.getPointer());

        leftNode.setLeftPointer(node.getLeftPointer());
        node.setLeftPointer(leftNode.getPointer());

        final OSebTreeNode.Marker leftMarker = node.markerAt(0);
        leftNode.insertMarker(0, -1, leftMarker.blockIndex, leftMarker.blockPagesUsed);

        // Setup fresh root.

        final int fullSeparatorSize = node.fullEntrySize(separatorSize, node.getPointerEncoder().maximumSize());
        node.checkEntrySize(fullSeparatorSize, this);
        node.insertPointer(0, separator, separatorSize, rightNode.getPointer());

        node.updateMarker(0, newBlockIndex, 2);
      } finally {
        if (newMarkerNode != rightNode && newKeyNode != rightNode)
          releaseNode(atomicOperation, rightNode.endWrite());
      }
    } finally {
      if (newMarkerNode != leftNode && newKeyNode != leftNode)
        releaseNode(atomicOperation, leftNode.endWrite());
    }

    node.endWrite();

    markerPath.add(1, newMarkerNode);
    valuePath.add(1, newKeyNode);
  }

  private void markerSplitInsertNonRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, int pointerIndex,
      List<OSebTreeNode<K, V>> markerPath, List<OSebTreeNode<K, V>> valuePath, int rank, K valueKey, K markerKey, long blockIndex,
      int blockPagesUsed) throws IOException {

    OSebTreeNode<K, V> newKeyNode = null;
    OSebTreeNode<K, V> newMarkerNode = null;
    final int newMarkerPointerIndex;

    final Creation<K, V> creation = createNode(atomicOperation, markerPath, rank, node, markerKey);
    node = creation.oldNode;
    final OSebTreeNode<K, V> rightNode = creation.newNode.beginCreate();
    try {
      rightNode.create(false);

      final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
      final int halfIndex = node.getSize() - entriesToMove;

      final K separator = node.keyAt(halfIndex);
      final int rightNodeSize = entriesToMove - 1;

      // Determine split point.

      if (pointerIndex < halfIndex) {
        newMarkerNode = node;
        newMarkerPointerIndex = pointerIndex;
      } else {
        newMarkerNode = rightNode;
        newMarkerPointerIndex = pointerIndex - halfIndex - 1;
      }

      // Deduce new key node.

      if (valuePath.get(valuePath.size() - rank - 1) == node)
        newKeyNode = OSebTreeNode.compareKeys(valueKey, separator) < 0 ? node : rightNode;

      // Build right node.

      node.moveTailTo(rightNode, rightNodeSize);

      rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

      if (newMarkerPointerIndex == -1) {
        node.setContinuedTo(false);
        rightNode.setContinuedFrom(false);
        rightNode.insertMarker(0, -1, blockIndex, blockPagesUsed);
      } else {
        ((newMarkerNode == rightNode) ? rightNode : node)
            .insertMarkerForPointerAt(newMarkerPointerIndex, blockIndex, blockPagesUsed); // like it already were here
        final OSebTreeNode.Marker rightMarker = node.markerAt(node.getMarkerCount() - 1);
        rightNode.insertMarker(0, -1, rightMarker.blockIndex, rightMarker.blockPagesUsed);
        rightNode.setContinuedTo(node.isContinuedTo());
        if (rightMarker.pointerIndex == node.getSize() - 1) {
          node.setContinuedTo(false);
          rightNode.setContinuedFrom(false);
        } else {
          node.setContinuedTo(true);
          rightNode.setContinuedFrom(true);
        }
      }

      node.delete(node.getSize() - 1, node.getKeyEncoder().exactSize(separator), node.getPointerEncoder().maximumSize());

      // Adjust pointers.

      final long rightSiblingPointer = node.getRightSibling();
      if (rightSiblingPointer != 0) {
        final OSebTreeNode<K, V> oldRightSibling = getNode(atomicOperation, rightSiblingPointer).beginWrite();
        try {
          oldRightSibling.setLeftSibling(rightNode.getPointer());
        } finally {
          releaseNode(atomicOperation, oldRightSibling.endWrite());
        }
        rightNode.setRightSibling(rightSiblingPointer);
      }
      node.setRightSibling(rightNode.getPointer());
      rightNode.setLeftSibling(node.getPointer());

      insertPointer(atomicOperation, markerPath.get(markerPath.size() - rank - 2), separator, rightNode.getPointer(), markerPath,
          rank + 1, valuePath, valueKey);
    } finally {
      if (newMarkerNode != rightNode && newKeyNode != rightNode)
        releaseNode(atomicOperation, rightNode.endWrite());
    }

    if (newMarkerNode != node && newKeyNode != node)
      releaseNode(atomicOperation, node.endWrite());

    markerPath.set(markerPath.size() - rank - 1, newMarkerNode);
    if (newKeyNode != null)
      valuePath.set(valuePath.size() - rank - 1, newKeyNode);
  }

  private void relinkNodes(OAtomicOperation atomicOperation, long[] oldPointers, long[] newPointers, OSebTreeNode<K, V> targetNode)
      throws IOException {

    for (int i = 0; i < BLOCK_SIZE; ++i) {
      final long oldPointer = oldPointers[i];
      final long newPointer = newPointers[i];

      if ((i == 0 && oldPointer != newPointer) || (i > 0 && oldPointers[i - 1] != newPointers[i - 1]) || (i < BLOCK_SIZE - 1
          && oldPointers[i + 1] != newPointers[i + 1]) || (i == BLOCK_SIZE - 1 && oldPointer != newPointer)) {
        final OSebTreeNode<K, V> node =
            newPointer == targetNode.getPointer() ? targetNode : getNode(atomicOperation, newPointer).beginWrite();
        try {

          if (i == 0 && oldPointer != newPointer && node.getLeftSibling() != 0) {
            final OSebTreeNode<K, V> sibling = getNode(atomicOperation, node.getLeftSibling()).beginWrite();
            try {
              sibling.setRightSibling(newPointer);
            } finally {
              releaseNode(atomicOperation, sibling.endWrite());
            }
          }

          if (i > 0 && oldPointers[i - 1] != newPointers[i - 1])
            node.setLeftSibling(newPointers[i - 1]);

          if (i < BLOCK_SIZE - 1 && oldPointers[i + 1] != newPointers[i + 1])
            node.setRightSibling(newPointers[i + 1]);

          if (i == BLOCK_SIZE - 1 && oldPointer != newPointer && node.getRightSibling() != 0) {
            final OSebTreeNode<K, V> sibling = getNode(atomicOperation, node.getRightSibling()).beginWrite();
            try {
              sibling.setLeftSibling(newPointer);
            } finally {
              releaseNode(atomicOperation, sibling.endWrite());
            }
          }

        } finally {
          if (node != targetNode)
            releaseNode(atomicOperation, node.endWrite());
        }
      }
    }
  }

  private OSebTreeNode<K, V> getNode(OAtomicOperation atomicOperation, long pageIndex) throws IOException {
    final OCacheEntry page;

    //    System.out.println("+ " + pageIndex);

    //    if (!nullKeyAllowed && pageIndex > 0 || nullKeyAllowed && pageIndex > 1) {
    //      // preload to the block end
    //      final int preload = (int) (BLOCK_SIZE - (pageIndex - 1 - (nullKeyAllowed ? 1 : 0)) % BLOCK_SIZE);
    //      page = loadPage(atomicOperation, fileId, pageIndex, false, preload);
    //    } else
    page = loadPage(atomicOperation, fileId, pageIndex, inMemory);

    return new OSebTreeNode<>(page, keyProvider, valueProvider);
  }

  private void releaseNode(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node) {
    //    System.out.println("- " + node.getPointer());
    releasePage(atomicOperation, node.getPage());
  }

  private long getRootPageIndex() {
    return nullKeyAllowed ? 1 : 0;
  }

  private long getNullPageIndex() {
    assert nullKeyAllowed;
    return 0;
  }

  private OSebTreeNode<K, V> getRootNode(OAtomicOperation atomicOperation) throws IOException {
    return getNode(atomicOperation, getRootPageIndex());
  }

  private OSebTreeNode<K, V> getNullNode(OAtomicOperation atomicOperation) throws IOException {
    return getNode(atomicOperation, getNullPageIndex());
  }

  private OSebTreeNode<K, V> findLeaf(OAtomicOperation atomicOperation, K key) throws IOException {
    if (key == null)
      return getNullNode(atomicOperation);

    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        releaseNode(atomicOperation, node.endRead());
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(node.indexOf(key));
      } finally {
        releaseNode(atomicOperation, node.endRead());
      }
    }
  }

  private OSebTreeNode<K, V> findLeafWithPath(OAtomicOperation atomicOperation, K key, List<OSebTreeNode<K, V>> path)
      throws IOException {
    if (key == null)
      return getNullNode(atomicOperation);

    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();
      path.add(node);

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        node.endRead();
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(node.indexOf(key));
      } finally {
        node.endRead();
      }
    }
  }

  private OSebTreeNode<K, V> leftMostLeaf(OAtomicOperation atomicOperation) throws IOException {
    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        releaseNode(atomicOperation, node.endRead());
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(-1);
      } finally {
        releaseNode(atomicOperation, node.endRead());
      }
    }
  }

  private OSebTreeNode<K, V> rightMostLeaf(OAtomicOperation atomicOperation) throws IOException {
    long nodePage = getRootPageIndex();
    while (true) {
      final OSebTreeNode<K, V> node = getNode(atomicOperation, nodePage).beginRead();

      final boolean leaf;
      try {
        leaf = node.isLeaf();
      } catch (Exception e) {
        releaseNode(atomicOperation, node.endRead());
        throw e;
      }

      if (leaf)
        return node.endRead();

      try {
        nodePage = node.pointerAt(node.getSize() - 1);
      } finally {
        releaseNode(atomicOperation, node.endRead());
      }
    }
  }

  private int indexOfKeyInLeaf(OSebTreeNode<K, V> leaf, K key) {
    return key == null ? leaf.getSize() == 1 ? 0 : -1 : leaf.indexOf(key);
  }

  private long size(OAtomicOperation atomicOperation) {
    try {
      final OSebTreeNode<K, V> rootNode = getRootNode(atomicOperation).beginRead();
      try {
        return rootNode.getTreeSize();
      } finally {
        releaseNode(atomicOperation, rootNode.endRead());
      }
    } catch (IOException e) {
      throw error("Error while retrieving the size of SebTree " + getName(), e);
    }
  }

  private K firstKey(OAtomicOperation atomicOperation) {
    try {
      OSebTreeNode<K, V> leaf = leftMostLeaf(atomicOperation).beginRead();
      try {

        long siblingPointer;
        while (leaf.getSize() == 0 && (siblingPointer = leaf.getRightSibling()) != 0) {
          releaseNode(atomicOperation, leaf.endRead());
          leaf = getNode(atomicOperation, siblingPointer).beginRead();
        }

        return leaf.getSize() == 0 ? null : leaf.keyAt(0);
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error while retrieving the first key of SebTree " + getName(), e);
    }
  }

  private K lastKey(OAtomicOperation atomicOperation) {
    try {
      OSebTreeNode<K, V> leaf = rightMostLeaf(atomicOperation).beginRead();
      try {

        long siblingPointer;
        while (leaf.getSize() == 0 && (siblingPointer = leaf.getLeftSibling()) != 0) {
          releaseNode(atomicOperation, leaf.endRead());
          leaf = getNode(atomicOperation, siblingPointer).beginRead();
        }

        return leaf.getSize() == 0 ? null : leaf.keyAt(leaf.getSize() - 1);
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error while retrieving the last key of SebTree " + getName(), e);
    }
  }

  private boolean contains(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        return nullKey ? leaf.getSize() == 1 : leaf.indexOf(key) >= 0;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private V get(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;
        return keyFound ? leaf.valueAt(keyIndex) : null;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private V get(OAtomicOperation atomicOperation, K key, OModifiableBoolean found) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginRead();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;
        found.setValue(keyFound);
        return keyFound ? leaf.valueAt(keyIndex) : null;
      } finally {
        releaseNode(atomicOperation, leaf.endRead());
      }
    } catch (IOException e) {
      throw error("Error during key lookup in SebTree " + getName(), e);
    }
  }

  private boolean putValue(OAtomicOperation atomicOperation, K key, V value) {
    if (key == null)
      checkNullKeyAllowed();

    try {
      final List<OSebTreeNode<K, V>> path = new ArrayList<>(16);

      OSebTreeNode<K, V> leaf = findLeafWithPath(atomicOperation, key, path).beginWrite();
      try {
        int keyIndex = indexOfKeyInLeaf(leaf, key);

        if (!tryPutValue(atomicOperation, leaf, keyIndex, key, value)) {
          final Splitting<K, V> splitting = split(atomicOperation, leaf, key, keyIndex, path, 0, null, null);
          leaf = splitting.node;
          keyIndex = splitting.keyIndex;

          if (!tryPutValue(atomicOperation, leaf, keyIndex, key, value))
            throw new OSebTreeException("Split failed.", this);
        }

        return OSebTreeNode.isInsertionPoint(keyIndex);
      } finally {
        leaf.endWrite();

        for (OSebTreeNode<K, V> node : path)
          releaseNode(atomicOperation, node);
      }

    } catch (IOException e) {
      throw error("Error during put in SebTree " + getName(), e);
    }
  }

  private boolean tryPutValue(OAtomicOperation atomicOperation, OSebTreeNode<K, V> leaf, int keyIndex, K key, V value)
      throws IOException {
    final boolean keyExists = !OSebTreeNode.isInsertionPoint(keyIndex);

    final int keySize = leaf.getKeyEncoder().exactSize(key);
    final int valueSize = leaf.getValueEncoder().exactSize(value);
    final int fullEntrySize = leaf.fullEntrySize(keySize, valueSize);
    leaf.checkEntrySize(fullEntrySize, this);

    final int currentKeySize = keyExists ? leaf.keySizeAt(keyIndex) : 0;
    final int currentValueSize = keyExists ? leaf.valueSizeAt(keyIndex) : 0;
    final int currentFullEntrySize = keyExists ? leaf.fullEntrySize(currentKeySize, currentValueSize) : 0;
    final int sizeDelta = fullEntrySize - currentFullEntrySize;
    final boolean entryFits = leaf.deltaFits(sizeDelta);

    if (entryFits) {
      if (keyExists)
        leaf.updateValue(keyIndex, value, valueSize, currentValueSize);
      else {
        leaf.insertValue(keyIndex, key, keySize, value, valueSize);
        updateTreeSize(atomicOperation, +1);
      }
    }

    return entryFits;
  }

  private Splitting<K, V> split(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, int rank, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey) throws IOException {
    if (isRoot(node))
      return splitRoot(atomicOperation, node, key, keyIndex, path, secondaryPath, secondaryKey);
    else
      return splitNonRoot(atomicOperation, path, node, key, keyIndex, rank, secondaryPath, secondaryKey);
  }

  private Splitting<K, V> splitRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey) throws IOException {
    if (node.isLeaf())
      return splitLeafRoot(atomicOperation, node, key, keyIndex, path);
    else
      return splitNonLeafRoot(atomicOperation, node, key, keyIndex, path, secondaryPath, secondaryKey);
  }

  private Splitting<K, V> splitNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, OSebTreeNode<K, V> node,
      K key, int keyIndex, int rank, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey) throws IOException {
    if (node.isLeaf())
      return splitLeafNonRoot(atomicOperation, path, node, key, keyIndex);
    else
      return splitNonLeafNonRoot(atomicOperation, path, node, key, keyIndex, rank, secondaryPath, secondaryKey);
  }

  private Splitting<K, V> splitLeafRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path) throws IOException {

    final long blockIndex = allocateBlock(atomicOperation);

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final OSebTreeNode<K, V> leftNode = getNode(atomicOperation, blockIndex).beginCreate();
    try {
      leftNode.create(true);
      final OSebTreeNode<K, V> rightNode = getNode(atomicOperation, blockIndex + 1).beginCreate();
      try {
        rightNode.create(true);

        final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
        final int halfIndex = node.getSize() - entriesToMove;

        final K separator;
        final int rightNodeSize;
        if (OSebTreeNode.isInsertionPoint(keyIndex)) {
          final int index = OSebTreeNode.toIndex(keyIndex);
          if (index <= halfIndex) {
            newKeyNode = leftNode;
            newKeyIndex = keyIndex;
            separator = node.keyAt(halfIndex);
            rightNodeSize = entriesToMove;
          } else {
            newKeyNode = rightNode;
            newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
            separator = newKeyIndex == -1 ? key : node.keyAt(halfIndex + 1);
            rightNodeSize = entriesToMove - 1;
          }
        } else {
          if (keyIndex <= halfIndex) {
            newKeyNode = leftNode;
            newKeyIndex = keyIndex;
            separator = keyIndex == halfIndex ? node.keyAt(halfIndex + 1) : node.keyAt(halfIndex);
            rightNodeSize = keyIndex == halfIndex ? entriesToMove - 1 : entriesToMove;
          } else {
            newKeyNode = rightNode;
            newKeyIndex = keyIndex - halfIndex - 1;
            separator = node.keyAt(halfIndex + 1);
            rightNodeSize = entriesToMove - 1;
          }
        }

        node.moveTailTo(rightNode, rightNodeSize);
        node.moveTailTo(leftNode, node.getSize());

        leftNode.setRightSibling(rightNode.getPointer());
        rightNode.setLeftSibling(leftNode.getPointer());

        node.convertToNonLeaf();
        node.setLeftPointer(leftNode.getPointer());

        final int separatorSize = node.getKeyEncoder().exactSize(separator);
        final int fullSeparatorSize = node.fullEntrySize(separatorSize, node.getPointerEncoder().maximumSize());
        node.checkEntrySize(fullSeparatorSize, this);
        node.insertPointer(0, separator, separatorSize, rightNode.getPointer());

        node.insertMarker(0, -1, blockIndex, 2);
      } finally {
        if (newKeyNode != rightNode)
          releaseNode(atomicOperation, rightNode.endWrite());
      }
    } finally {
      if (newKeyNode != leftNode)
        releaseNode(atomicOperation, leftNode.endWrite());
    }

    node.endWrite();

    path.add(newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitNonLeafRoot(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, int keyIndex,
      List<OSebTreeNode<K, V>> path, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey) throws IOException {

    final long newBlockIndex = allocateBlock(atomicOperation);

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;
    OSebTreeNode<K, V> newSecondaryKeyNode = null;

    final OSebTreeNode<K, V> leftNode = getNode(atomicOperation, newBlockIndex).beginCreate();
    try {
      leftNode.create(false);
      final OSebTreeNode<K, V> rightNode = getNode(atomicOperation, newBlockIndex + 1).beginCreate();
      try {
        rightNode.create(false);

        // Determine split point.

        final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
        final int halfIndex = node.getSize() - entriesToMove;

        final K separator;
        final int rightNodeSize;
        assert OSebTreeNode.isInsertionPoint(keyIndex);
        final int index = OSebTreeNode.toIndex(keyIndex);

        if (index < halfIndex) {
          newKeyNode = leftNode;
          newKeyIndex = keyIndex;
          separator = node.keyAt(halfIndex - 1);
          rightNodeSize = entriesToMove;
        } else if (index == halfIndex) {
          newKeyNode = rightNode;
          newKeyIndex = Integer.MIN_VALUE;
          separator = key;
          rightNodeSize = entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
          separator = node.keyAt(halfIndex);
          rightNodeSize = entriesToMove - 1;
        }

        // Decide on secondary path.

        if (secondaryPath != null)
          newSecondaryKeyNode = OSebTreeNode.compareKeys(secondaryKey, separator) < 0 ? leftNode : rightNode;

        // Build right node.

        final int separatorSize = node.getKeyEncoder().exactSize(separator);

        node.moveTailTo(rightNode, rightNodeSize);

        if (separator != key)
          rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

        final OSebTreeNode.Marker rightMarker = node.markerAt(node.getMarkerCount() - 1);
        rightNode.insertMarker(0, -1, rightMarker.blockIndex, rightMarker.blockPagesUsed);
        if (separator != key && rightMarker.pointerIndex == node.getSize() - 1) {
          leftNode.setContinuedTo(false);
          rightNode.setContinuedFrom(false);
        } else {
          leftNode.setContinuedTo(true);
          rightNode.setContinuedFrom(true);
        }

        if (separator != key)
          node.delete(node.getSize() - 1, separatorSize, node.getPointerEncoder().maximumSize());

        // Build left node.

        node.moveTailTo(leftNode, node.getSize());

        leftNode.setRightSibling(rightNode.getPointer());
        rightNode.setLeftSibling(leftNode.getPointer());

        leftNode.setLeftPointer(node.getLeftPointer());
        node.setLeftPointer(leftNode.getPointer());

        final OSebTreeNode.Marker leftMarker = node.markerAt(0);
        leftNode.insertMarker(0, -1, leftMarker.blockIndex, leftMarker.blockPagesUsed);

        // Setup fresh root.

        final int fullSeparatorSize = node.fullEntrySize(separatorSize, node.getPointerEncoder().maximumSize());
        node.checkEntrySize(fullSeparatorSize, this);
        node.insertPointer(0, separator, separatorSize, rightNode.getPointer());

        node.updateMarker(0, newBlockIndex, 2);
      } finally {
        if (newKeyNode != rightNode && newSecondaryKeyNode != rightNode)
          releaseNode(atomicOperation, rightNode.endWrite());
      }
    } finally {
      if (newKeyNode != leftNode && newSecondaryKeyNode != leftNode)
        releaseNode(atomicOperation, leftNode.endWrite());
    }

    node.endWrite();

    path.add(1, newKeyNode);
    if (secondaryPath != null)
      secondaryPath.add(1, newSecondaryKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitLeafNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path, OSebTreeNode<K, V> node,
      K key, int keyIndex) throws IOException {

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;

    final Creation<K, V> creation = createNode(atomicOperation, path, 0, node, key);
    node = creation.oldNode;
    final OSebTreeNode<K, V> rightNode = creation.newNode.beginCreate();
    try {
      rightNode.create(true);

      // Determine split point.

      final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
      final int halfIndex = node.getSize() - entriesToMove;

      final K separator;
      final int rightNodeSize;
      if (OSebTreeNode.isInsertionPoint(keyIndex)) {
        final int index = OSebTreeNode.toIndex(keyIndex);
        if (index <= halfIndex) {
          newKeyNode = node;
          newKeyIndex = keyIndex;
          separator = node.keyAt(halfIndex);
          rightNodeSize = entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
          separator = newKeyIndex == -1 ? key : node.keyAt(halfIndex + 1);
          rightNodeSize = entriesToMove - 1;
        }
      } else {
        if (keyIndex <= halfIndex) {
          newKeyNode = node;
          newKeyIndex = keyIndex;
          separator = keyIndex == halfIndex ? node.keyAt(halfIndex + 1) : node.keyAt(halfIndex);
          rightNodeSize = keyIndex == halfIndex ? entriesToMove - 1 : entriesToMove;
        } else {
          newKeyNode = rightNode;
          newKeyIndex = keyIndex - halfIndex - 1;
          separator = node.keyAt(halfIndex + 1);
          rightNodeSize = entriesToMove - 1;
        }
      }

      node.moveTailTo(rightNode, rightNodeSize);

      final long rightSiblingPointer = node.getRightSibling();
      if (rightSiblingPointer != 0) {
        final OSebTreeNode<K, V> oldRightSibling = getNode(atomicOperation, rightSiblingPointer).beginWrite();
        try {
          oldRightSibling.setLeftSibling(rightNode.getPointer());
        } finally {
          releaseNode(atomicOperation, oldRightSibling.endWrite());
        }
        rightNode.setRightSibling(rightSiblingPointer);
      }
      node.setRightSibling(rightNode.getPointer());
      rightNode.setLeftSibling(node.getPointer());

      insertPointer(atomicOperation, path.get(path.size() - 2), separator, rightNode.getPointer(), path, 1, null, null);
    } finally {
      if (newKeyNode != rightNode)
        releaseNode(atomicOperation, rightNode.endWrite());
    }

    if (newKeyNode != node)
      releaseNode(atomicOperation, node.endWrite());

    path.set(path.size() - 1, newKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private Splitting<K, V> splitNonLeafNonRoot(OAtomicOperation atomicOperation, List<OSebTreeNode<K, V>> path,
      OSebTreeNode<K, V> node, K key, int keyIndex, int rank, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey)
      throws IOException {

    OSebTreeNode<K, V> newKeyNode = null;
    final int newKeyIndex;
    OSebTreeNode<K, V> newSecondaryKeyNode = null;

    final Creation<K, V> creation = createNode(atomicOperation, path, rank, node, key);
    node = creation.oldNode;
    final OSebTreeNode<K, V> rightNode = creation.newNode.beginCreate();
    try {
      rightNode.create(false);

      // Determine split point.

      final int entriesToMove = node.countEntriesToMoveUntilHalfFree();
      final int halfIndex = node.getSize() - entriesToMove;

      final K separator;
      final int rightNodeSize;
      assert OSebTreeNode.isInsertionPoint(keyIndex);
      final int index = OSebTreeNode.toIndex(keyIndex);

      if (index < halfIndex) {
        newKeyNode = node;
        newKeyIndex = keyIndex;
        separator = node.keyAt(halfIndex - 1);
        rightNodeSize = entriesToMove;
      } else if (index == halfIndex) {
        newKeyNode = rightNode;
        newKeyIndex = Integer.MIN_VALUE;
        separator = key;
        rightNodeSize = entriesToMove;
      } else {
        newKeyNode = rightNode;
        newKeyIndex = OSebTreeNode.toInsertionPoint(index - halfIndex - 1);
        separator = node.keyAt(halfIndex);
        rightNodeSize = entriesToMove - 1;
      }

      // Decide on secondary path.

      if (secondaryPath != null && secondaryPath.get(secondaryPath.size() - rank - 1) == node)
        newSecondaryKeyNode = OSebTreeNode.compareKeys(secondaryKey, separator) < 0 ? node : rightNode;

      // Build the right node.

      node.moveTailTo(rightNode, rightNodeSize);

      if (separator != key)
        rightNode.setLeftPointer(node.pointerAt(node.getSize() - 1));

      final OSebTreeNode.Marker rightMarker = node.markerAt(node.getMarkerCount() - 1);
      rightNode.insertMarker(0, -1, rightMarker.blockIndex, rightMarker.blockPagesUsed);
      rightNode.setContinuedTo(node.isContinuedTo());
      if (separator != key && rightMarker.pointerIndex == node.getSize() - 1) {
        node.setContinuedTo(false);
        rightNode.setContinuedFrom(false);
      } else {
        node.setContinuedTo(true);
        rightNode.setContinuedFrom(true);
      }

      if (separator != key)
        node.delete(node.getSize() - 1, node.getKeyEncoder().exactSize(separator), node.getPointerEncoder().maximumSize());

      // Adjust pointers.

      final long rightSiblingPointer = node.getRightSibling();
      if (rightSiblingPointer != 0) {
        final OSebTreeNode<K, V> oldRightSibling = getNode(atomicOperation, rightSiblingPointer).beginWrite();
        try {
          oldRightSibling.setLeftSibling(rightNode.getPointer());
        } finally {
          releaseNode(atomicOperation, oldRightSibling.endWrite());
        }
        rightNode.setRightSibling(rightSiblingPointer);
      }
      node.setRightSibling(rightNode.getPointer());
      rightNode.setLeftSibling(node.getPointer());

      // Register the separator in parent.

      insertPointer(atomicOperation, path.get(path.size() - rank - 2), separator, rightNode.getPointer(), path, rank + 1,
          secondaryPath, secondaryKey);
    } finally {
      if (newKeyNode != rightNode && newSecondaryKeyNode != rightNode)
        releaseNode(atomicOperation, rightNode.endWrite());
    }

    if (newKeyNode != node && newSecondaryKeyNode != node)
      releaseNode(atomicOperation, node.endWrite());

    path.set(path.size() - rank - 1, newKeyNode);
    if (newSecondaryKeyNode != null)
      secondaryPath.set(secondaryPath.size() - rank - 1, newSecondaryKeyNode);
    return new Splitting<>(newKeyNode, newKeyIndex);
  }

  private void insertPointer(OAtomicOperation atomicOperation, OSebTreeNode<K, V> node, K key, long pointer,
      List<OSebTreeNode<K, V>> path, int rank, List<OSebTreeNode<K, V>> secondaryPath, K secondaryKey) throws IOException {

    node.beginWrite();
    try {
      int keyIndex = node.indexOf(key);

      if (!tryInsertPointer(node, keyIndex, key, pointer)) {
        final Splitting<K, V> splitting = split(atomicOperation, node, key, keyIndex, path, rank, secondaryPath, secondaryKey);
        node = splitting.node;
        keyIndex = splitting.keyIndex;

        if (keyIndex == Integer.MIN_VALUE)
          node.setLeftPointer(pointer);
        else if (!tryInsertPointer(node, keyIndex, key, pointer))
          throw new OSebTreeException("Split failed.", this);
      }
    } finally {
      node.endWrite();
    }
  }

  private boolean tryInsertPointer(OSebTreeNode<K, V> node, int keyIndex, K key, long pointer) throws IOException {
    final int keySize = node.getKeyEncoder().exactSize(key);
    final int pointerSize = node.getPointerEncoder().maximumSize();
    final int fullEntrySize = node.fullEntrySize(keySize, pointerSize);
    node.checkEntrySize(fullEntrySize, this);
    final boolean entryFits = node.deltaFits(fullEntrySize);

    if (entryFits) {
      final int index = OSebTreeNode.isInsertionPoint(keyIndex) ? OSebTreeNode.toIndex(keyIndex) : keyIndex;
      node.insertPointer(index, key, keySize, pointer);
    }

    return entryFits;
  }

  private boolean remove(OAtomicOperation atomicOperation, K key) {
    final boolean nullKey = key == null;
    if (nullKey)
      checkNullKeyAllowed();

    try {
      final OSebTreeNode<K, V> leaf = findLeaf(atomicOperation, key);

      leaf.beginWrite();
      try {
        final int keyIndex = nullKey ? 0 : leaf.indexOf(key);
        final boolean keyFound = nullKey ? leaf.getSize() == 1 : keyIndex >= 0;

        if (!keyFound)
          return false;

        final int keySize = leaf.keySizeAt(keyIndex);
        final int valueSize = leaf.valueSizeAt(keyIndex);

        leaf.delete(keyIndex, keySize, valueSize);
        updateTreeSize(atomicOperation, -1);

        return true;
      } finally {
        releaseNode(atomicOperation, leaf.endWrite());
      }
    } catch (IOException e) {
      throw error("Error during key removal in SebTree " + getName(), e);
    }
  }

  private boolean isRoot(OSebTreeNode<K, V> node) {
    return node.getPointer() == getRootPageIndex();
  }

  private OAtomicOperation atomicOperation() {
    return atomicOperationsManager.getCurrentOperation();
  }

  private OAtomicOperation startAtomicOperation(String operation, boolean nonTx) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(nonTx);
    } catch (IOException e) {
      throw error("Error during SebTree " + operation, e);
    }

    currentOperation = operation;
    return atomicOperation;
  }

  private void endSuccessfulAtomicOperation() {
    try {
      endAtomicOperation(false, null);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during commit of the SebTree atomic operation.", e);
    }
    currentOperation = null;
  }

  private RuntimeException endFailedAtomicOperation(Exception exception) {
    try {
      endAtomicOperation(true, exception);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during rollback of the SebTree atomic operation.", e);
    }

    final String operation = currentOperation;
    currentOperation = null;

    if (exception instanceof RuntimeException)
      return (RuntimeException) exception;
    else
      return error("Error during SebTree " + operation, exception);
  }

  private RuntimeException error(String message, Exception exception) {
    return OException.wrapException(new OSebTreeException(message, this), exception);
  }

  private void checkNullKeyAllowed() {
    if (!nullKeyAllowed)
      throw new OSebTreeException("Null keys are not allowed by SebTree " + getName(), this);
  }

  private K internalKey(K key) {
    return key == null ? null : keySerializer.preprocess(key, (Object[]) keyTypes);
  }

  @SuppressWarnings("unchecked")
  private K internalRangeKey(K key, boolean beginning, boolean inclusive) {
    key = internalKey(key);

    if (keySize == 1)
      return key;

    if (!(key instanceof OCompositeKey))
      return key;
    final OCompositeKey compositeKey = (OCompositeKey) key;

    if (compositeKey.getKeys().size() == keySize)
      return key;

    final OCompositeKey rangeKey = new OCompositeKey(compositeKey);
    final int subkeysToAdd = keySize - rangeKey.getKeys().size();

    final Comparable<?> subkey;
    if (beginning)
      subkey = inclusive ? ALWAYS_LESS_KEY : ALWAYS_GREATER_KEY;
    else
      subkey = inclusive ? ALWAYS_GREATER_KEY : ALWAYS_LESS_KEY;

    for (int i = 0; i < subkeysToAdd; ++i)
      rangeKey.addKey(subkey);

    return (K) rangeKey;
  }

  private void updateTreeSize(OAtomicOperation atomicOperation, long delta) throws IOException {
    final OSebTreeNode<K, V> root = getRootNode(atomicOperation).beginWrite();
    try {
      root.setTreeSize(root.getTreeSize() + delta);
    } finally {
      releaseNode(atomicOperation, root.endWrite());
    }
  }

  public boolean isFull() {
    return full;
  }

  private long allocateBlock(OAtomicOperation atomicOperation) throws IOException {
    final OSebTreeNode<K, V> firstNode = createNode(atomicOperation).beginCreate().createDummy().endWrite();
    final long firstPage = firstNode.getPointer();
    releaseNode(atomicOperation, firstNode);

    if (!full && firstPage >= IN_MEMORY_PAGES_THRESHOLD) {
      full = true;
      unpinAllPages(atomicOperation, fileId);
    }

    for (int i = 0; i < BLOCK_SIZE - 1; ++i)
      releaseNode(atomicOperation, createNode(atomicOperation).beginCreate().createDummy().endWrite());

    return firstPage;
  }

  private void dump(OSebTreeNode<K, V> node, int level) throws IOException {
    node.dump(level);

    if (!node.isLeaf())
      for (int i = -1; i < node.getSize(); ++i) {
        final OSebTreeNode<K, V> child = getNode(null, node.pointerAt(i)).beginRead();
        try {
          dump(child, level + 1);
        } finally {
          releaseNode(null, child.endRead());
        }
      }
  }

  private static class Splitting<K, V> {
    public final OSebTreeNode<K, V> node;
    public final int                keyIndex;

    public Splitting(OSebTreeNode<K, V> node, int keyIndex) {
      this.node = node;
      this.keyIndex = keyIndex;
    }
  }

  private static class Creation<K, V> {
    public final OSebTreeNode<K, V> oldNode;
    public final OSebTreeNode<K, V> newNode;

    private Creation(OSebTreeNode<K, V> oldNode, OSebTreeNode<K, V> newNode) {
      this.oldNode = oldNode;
      this.newNode = newNode;
    }
  }

  private static class Block {
    public final int                   targetNodeIndex;
    public final long[]                pointers;
    public final long[]                parents;
    public final OSebTreeNode.Marker[] markers;

    private Block(List<Long> pointers, List<Long> parents, List<OSebTreeNode.Marker> markers, long targetPointer) {
      this.pointers = new long[pointers.size()];

      int targetNodeIndex = -1;
      for (int i = 0; i < pointers.size(); ++i) {
        final long pointer = pointers.get(i);
        this.pointers[i] = pointer;
        if (pointer == targetPointer)
          targetNodeIndex = i;
      }

      assert targetNodeIndex != -1;
      this.targetNodeIndex = targetNodeIndex;

      this.parents = new long[parents.size()];
      for (int i = 0; i < parents.size(); ++i)
        this.parents[i] = parents.get(i);

      this.markers = new OSebTreeNode.Marker[markers.size()];
      for (int i = 0; i < markers.size(); ++i)
        this.markers[i] = markers.get(i);
    }
  }

  private static class Cursor<K, V> implements OKeyValueCursor<K, V> {

    private final OSebTree<K, V> tree;
    private final boolean        fetchValues;
    private final K              beginningKey;
    private final K              endKey;
    private final Beginning      beginning;
    private final End            end;
    private final Direction      direction;

    private long leafPointer;
    private int  recordIndex;

    private boolean hasRecord = false;
    private K key;
    private V value;

    public Cursor(OAtomicOperation atomicOperation, OSebTree<K, V> tree, boolean fetchValues, K beginningKey, K endKey,
        OCursor.Beginning beginning, OCursor.End end, OCursor.Direction direction) {

      this.tree = tree;
      this.fetchValues = fetchValues;
      this.beginningKey = direction == Direction.Forward ? beginningKey : endKey;
      this.endKey = direction == Direction.Forward ? endKey : beginningKey;
      this.beginning = direction == Direction.Forward ? beginning : Beginning.values()[end.ordinal()];
      this.end = direction == Direction.Forward ? end : End.values()[beginning.ordinal()];
      this.direction = direction;

      initialize(atomicOperation);
    }

    @Override
    public boolean next() {
      final OSessionStoragePerformanceStatistic statistic = tree.start();
      try {
        tree.atomicOperationsManager.acquireReadLock(tree);
        try {
          return next(tree.atomicOperation());
        } finally {
          tree.atomicOperationsManager.releaseReadLock(tree);
        }
      } finally {
        tree.end(statistic);
      }
    }

    @Override
    public K key() {
      assert hasRecord;
      return key;
    }

    @Override
    public V value() {
      assert hasRecord;
      return value;
    }

    private void initialize(OAtomicOperation atomicOperation) {
      try {
        if (beginning == Beginning.Open) {
          final OSebTreeNode<K, V> leaf = (direction == Direction.Forward ?
              tree.leftMostLeaf(atomicOperation) :
              tree.rightMostLeaf(atomicOperation)).beginRead();
          try {
            leafPointer = leaf.getPointer();
            recordIndex = direction == Direction.Forward ? 0 : leaf.getSize() - 1;
          } finally {
            tree.releaseNode(atomicOperation, leaf.endRead());
          }
        } else {
          final OSebTreeNode<K, V> leaf = tree.findLeaf(atomicOperation, beginningKey).beginRead();
          try {
            leafPointer = leaf.getPointer();
            recordIndex = leaf.indexOf(beginningKey);

            if (OSebTreeNode.isInsertionPoint(recordIndex)) {
              recordIndex = OSebTreeNode.toIndex(recordIndex);
              if (direction == Direction.Reverse)
                --recordIndex;
            } else if (beginning == Beginning.Exclusive)
              recordIndex += direction == Direction.Forward ? +1 : -1;
          } finally {
            tree.releaseNode(atomicOperation, leaf.endRead());
          }
        }
      } catch (IOException e) {
        throw tree.error("Error while constructing cursor for SebTree " + tree.getName(), e);
      }
    }

    private boolean next(OAtomicOperation atomicOperation) {
      try {
        hasRecord = false;

        while (leafPointer >= 0) {
          final OSebTreeNode<K, V> leaf = tree.getNode(atomicOperation, leafPointer).beginRead();
          try {
            if (recordIndex == Integer.MAX_VALUE)
              recordIndex = leaf.getSize() - 1;

            if (recordIndex >= 0 && recordIndex < leaf.getSize()) {
              key = leaf.keyAt(recordIndex);

              if (end != End.Open) {
                final int order = OSebTreeNode.compareKeys(key, endKey);

                if (direction == Direction.Forward)
                  hasRecord = end == End.Inclusive ? order <= 0 : order < 0;
                else
                  hasRecord = end == End.Inclusive ? order >= 0 : order > 0;

                if (!hasRecord) {
                  leafPointer = -1;
                  break;
                }
              }

              if (fetchValues)
                value = leaf.valueAt(recordIndex);

              recordIndex += direction == Direction.Forward ? +1 : -1;

              hasRecord = true;
              break;
            } else {
              leafPointer = direction == Direction.Forward ? leaf.getRightSibling() : leaf.getLeftSibling();
              recordIndex = direction == Direction.Forward ? 0 : Integer.MAX_VALUE;

              if (leafPointer == 0)
                leafPointer = -1;
            }
          } finally {
            tree.releaseNode(atomicOperation, leaf.endRead());
          }
        }

        return hasRecord;
      } catch (IOException e) {
        throw tree.error("Error while iterating SebTree " + tree.getName(), e);
      }
    }
  }

}
