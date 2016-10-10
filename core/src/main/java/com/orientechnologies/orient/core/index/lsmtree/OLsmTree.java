/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index.lsmtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Sergey Sitnikov
 */
public class OLsmTree<K, V> extends ODurableComponent {

  private OBinarySerializer<K> keySerializer;
  private OType[]              keyTypes;
  private int                  keySize;
  private boolean              nullKeyAllowed;
  private OBinarySerializer<V> valueSerializer;

  private long fileId;

  private boolean opened           = false;
  private String  currentOperation = null;

  public OLsmTree(OAbstractPaginatedStorage storage, String name, String extension) {
    super(storage, name, extension, name + extension);
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
          this.keyTypes = keyTypes == null ? null : Arrays.copyOf(keyTypes, keyTypes.length);
          this.keySize = keySize;
          this.nullKeyAllowed = nullKeyAllowed;
          this.valueSerializer = valueSerializer;

          fileId = addFile(atomicOperation, getFullName());

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
        this.keyTypes = keyTypes;
        this.keySize = keySize;
        this.nullKeyAllowed = nullKeyAllowed;
        this.valueSerializer = valueSerializer;

        fileId = openFile(atomicOperation(), getFullName());
      } finally {
        releaseExclusiveLock();
      }
    } catch (IOException e) {
      throw error("Exception while opening of LsmTree " + getName(), e);
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
        throw error("Exception while closing of LsmTree " + getName(), e);
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

  private OAtomicOperation atomicOperation() {
    return atomicOperationsManager.getCurrentOperation();
  }

  private OAtomicOperation startAtomicOperation(String operation, boolean nonTx) {
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = startAtomicOperation(nonTx);
    } catch (IOException e) {
      throw error("Error during LsmTree " + operation, e);
    }

    currentOperation = operation;
    return atomicOperation;
  }

  private void endSuccessfulAtomicOperation() {
    try {
      endAtomicOperation(false, null);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during commit of the LsmTree atomic operation.", e);
    }
    currentOperation = null;
  }

  private RuntimeException endFailedAtomicOperation(Exception exception) {
    try {
      endAtomicOperation(true, exception);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error during rollback of the LsmTree atomic operation.", e);
    }

    final String operation = currentOperation;
    currentOperation = null;

    if (exception instanceof RuntimeException)
      return (RuntimeException) exception;
    else
      return error("Error during LsmTree " + operation, exception);
  }

  private RuntimeException error(String message, Exception exception) {
    return OException.wrapException(new OLsmTreeException(message, this), exception);
  }

}
