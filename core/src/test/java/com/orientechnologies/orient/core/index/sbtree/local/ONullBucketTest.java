package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.directmemory.OByteBufferContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class ONullBucketTest {
  @Test
  public void testEmptyBucket() {
    final int pageSize = 1024;
    OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointer = new OCachePointer(container, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);
    Assert.assertNull(bucket.getValue());

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddGetValue() throws IOException {
    final int pageSize = 1024;
    OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointer = new OCachePointer(container, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "test");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddRemoveValue() throws IOException {
    final int pageSize = 1024;
    OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointer = new OCachePointer(container, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }

  @Test
  public void testAddRemoveAddValue() throws IOException {
    final int pageSize = 1024;

    OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointer = new OCachePointer(container, bufferPool, 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntryImpl(0, 0, cachePointer, false);
    cacheEntry.acquireExclusiveLock();

    ONullBucket<String> bucket = new ONullBucket<>(cacheEntry, OStringSerializer.INSTANCE, true);

    bucket.setValue(new OSBTreeValue<>(false, -1, "test"));
    bucket.removeValue();

    OSBTreeValue<String> treeValue = bucket.getValue();
    Assert.assertNull(treeValue);

    bucket.setValue(new OSBTreeValue<>(false, -1, "testOne"));

    treeValue = bucket.getValue();
    Assert.assertEquals(treeValue.getValue(), "testOne");

    cacheEntry.releaseExclusiveLock();
    cachePointer.decrementReferrer();
  }
}
