package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.ArrayList;
import java.util.Iterator;

import com.orientechnologies.common.directmemory.OByteBufferContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.local.twoq.LRUList;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.02.13
 */
public abstract class AbstractLRUListTestTemplate {
  protected LRUList lruList;

  @Test
  public void testSingleAdd() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    final OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointer = new OCachePointer(container, bufferPool, 0, 0);
    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointer, false));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointer, false));
  }

  @Test
  public void testAddTwo() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OByteBufferContainer containerOne = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerTwo = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(containerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(containerTwo, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne, false));
  }

  @Test
  public void testAddThree() {
    final int pageSize = 1;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OByteBufferContainer containerOne = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerTwo = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerThree = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(containerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(containerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(containerThree, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne, false));
  }

  @Test
  public void testAddThreePutMiddleToTop() {
    final int pageSize = 1;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OByteBufferContainer containerOne = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerTwo = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerThree = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(containerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(containerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(containerThree, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne, false));
  }

  @Test
  public void testAddThreePutMiddleToTopChangePointer() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OByteBufferContainer containerOne = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerTwo = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerThree = bufferPool.acquire(pageSize, true);
    OByteBufferContainer containerFour = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(containerOne, bufferPool, 0, 0);
    OCachePointer cachePointerTwo = new OCachePointer(containerTwo, bufferPool, 0, 0);
    OCachePointer cachePointerThree = new OCachePointer(containerThree, bufferPool, 0, 0);
    OCachePointer cachePointerFour = new OCachePointer(containerFour, bufferPool, 0, 0);

    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne, false));
    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerTwo, false));
    lruList.putToMRU(new OCacheEntryImpl(3, 30, cachePointerThree, false));

    lruList.putToMRU(new OCacheEntryImpl(1, 20, cachePointerFour, false));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 20, cachePointerFour, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(3, 30, cachePointerThree, false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 10, cachePointerOne, false));
  }

  @Test
  public void testAddElevenPutMiddleToTopChangePointer() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);

      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OCacheEntryImpl(1, 50, cachePointers[5], false));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 50, cachePointers[5], false));

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAddOneRemoveLRU() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(container, bufferPool, 0, 0);
    lruList.putToMRU(new OCacheEntryImpl(1, 10, cachePointerOne, false));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  @Test
  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    final int pageSize = 1;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OByteBufferContainer container = bufferPool.acquire(pageSize, true);

    OCachePointer cachePointerOne = new OCachePointer(container, bufferPool, 0, 0);
    OCacheEntry cacheEntry = new OCacheEntryImpl(1, 10, cachePointerOne, false);
    lruList.putToMRU(cacheEntry);
    cacheEntry.incrementUsages();

    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);
  }

  @Test
  public void testAddElevenRemoveLRU() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);

      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAddElevenRemoveMiddle() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);

      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.remove(1, 50), new OCacheEntryImpl(1, 50, cachePointers[5], false));
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAddElevenGetMiddle() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OCachePointer[] cachePointers = new OCachePointer[11];

    for (int i = 0; i < 11; i++) {
      OByteBufferContainer buffer = bufferPool.acquire(pageSize, true);

      cachePointers[i] = new OCachePointer(buffer, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    Assert.assertEquals(lruList.get(1, 50), new OCacheEntryImpl(1, 50, cachePointers[5], false));

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAdd9128() {
    final int pageSize = 1;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);
      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAdd9128Get() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);
      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      Assert.assertEquals(lruList.get(1, i * 10), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAdd9128Remove4564() {
    final int pageSize = 1;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);
      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    for (int i = 4564; i < 9128; i++)
      Assert.assertEquals(lruList.remove(1, i * 10), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testAdd9128PutLastAndMiddleToTop() {
    final int pageSize = 1;
    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    OCachePointer[] cachePointers = new OCachePointer[9128];

    for (int i = 0; i < 9128; i++) {
      OByteBufferContainer container = bufferPool.acquire(pageSize, true);
      cachePointers[i] = new OCachePointer(container, bufferPool, 0, 0);
      lruList.putToMRU(new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }

    lruList.putToMRU(new OCacheEntryImpl(1, 0, cachePointers[0], false));
    lruList.putToMRU(new OCacheEntryImpl(1, 4500 * 10, cachePointers[4500], false));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 4500 * 10, cachePointers[4500], false));
    Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, 0, cachePointers[0], false));

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      Assert.assertEquals(entryIterator.next(), new OCacheEntryImpl(1, i * 10, cachePointers[i], false));
    }
  }

  @Test
  public void testInverseIterator() {
    final ArrayList<OCacheEntry> entries = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      final OCacheEntry cacheEntry = new OCacheEntryImpl(1, i, null, false);

      entries.add(cacheEntry);
      lruList.putToMRU(cacheEntry);
    }

    final Iterator<OCacheEntry> reverseIterator = lruList.reverseIterator();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(reverseIterator.hasNext());
      final OCacheEntry cacheEntry = reverseIterator.next();
      Assert.assertEquals(entries.get(i), cacheEntry);
      Assert.assertTrue(i < 9 == reverseIterator.hasNext());
    }
  }
}
