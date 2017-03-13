package com.orientechnologies.common.directmemory;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class OTwoSizeBufferPool implements OBufferPool {
  private final long maxHeapAllocationSize;

  private final long maxAllocationSize;
  private final int  pageSize;

  private int    heapSize;
  private Heap[] heaps;
  private long   allocationPosition;

  private final OTwoSizeQueueList pageQueue       = new OTwoSizeQueueList();
  private final OTwoSizeQueueList quatroPageQueue = new OTwoSizeQueueList();

  private final Object lock = new Object();

  public OTwoSizeBufferPool(long maxAllocationSize, long maxHeapAllocationSize, int pageSize) {
    if (maxHeapAllocationSize <= 0)
      maxHeapAllocationSize = 1 << 30;

    this.maxHeapAllocationSize = maxHeapAllocationSize;

    if (1 << (32 - Integer.numberOfLeadingZeros(pageSize - 1)) != pageSize) {
      throw new IllegalArgumentException("Page size parameter has to be power of two but passed value equals to " + pageSize);
    }

    this.pageSize = pageSize;
    this.maxAllocationSize = (maxAllocationSize / pageSize) * pageSize;
    this.heaps = new Heap[0];
  }

  @Override
  public OByteBufferHolder acquire(int chunkSize, boolean clean) {
    final int pages = chunkSize / pageSize;

    if ((pages != 1 && pages != 4) || pages * pageSize != chunkSize) {
      throw new IllegalArgumentException(
          "Chunk size should be equal to page size (" + pageSize + ") or should be 4 times bigger than page size, passed in value "
              + chunkSize);
    }

    final OByteBufferHolder result;
    synchronized (lock) {
      if (pages == 1) {
        result = allocateSinglePage();
      } else {
        result = allocateQuatroPage();
      }
    }

    if (clean) {
      final byte[] cleaner = new byte[1024];
      final int cleanCount = chunkSize / 1024;
      final int reminder = chunkSize - cleanCount * cleaner.length;

      final ByteBuffer buffer = result.getBuffer();
      buffer.position(0);

      for (int i = 0; i < cleanCount; i++) {
        buffer.put(cleaner);
      }

      if (reminder > 0) {
        final byte[] rem = new byte[reminder];
        buffer.put(rem);
      }
    }

    assert result.isAcquired();

    result.getBuffer().position(0);

    return result;
  }

  @Override
  public void release(OByteBufferContainer container) {
    final OByteBufferHolder holder = (OByteBufferHolder) container;

    assert holder.isAcquired();

    synchronized (lock) {
      final int pages = holder.getBuffer().capacity() / pageSize;

      assert (pages == 4 || pages == 1) && (pages * pageSize) == holder.getBuffer().capacity();

      if (pages == 4) {
        quatroPageQueue.push(holder);
      } else {
        final Heap heap = heaps[holder.heap];

        final int startPage = (holder.position / (4 * pageSize)) * 4;

        final OByteBufferHolder[] map = heap.pageMap;

        boolean isFree = true;
        final OByteBufferHolder[] buddies = new OByteBufferHolder[3];
        int buddyIndex = 0;

        for (int i = 0; i < 4; i++) {
          if (startPage + i >= map.length) {
            isFree = false;
            break;
          }

          final OByteBufferHolder buddy = map[startPage + i];

          if (buddy == null) {
            isFree = false;
            break;
          }

          if (buddy == holder) {
            continue;
          }

          if (buddy.isAcquired()) {
            isFree = false;
            break;
          }

          assert !buddy.isAcquired();

          buddies[buddyIndex] = buddy;
          buddyIndex++;
        }

        if (isFree) {
          final ByteBuffer heapBuffer = heap.buffer;

          heapBuffer.limit(startPage * pageSize + 4 * pageSize);
          heapBuffer.position(startPage * pageSize);

          final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
          final OByteBufferHolder merged = new OByteBufferHolder(buffer, holder.heap, startPage * pageSize);

          for (OByteBufferHolder buddy : buddies) {
            pageQueue.remove(buddy);
          }

          quatroPageQueue.push(merged);

          for (int i = 0; i < 4; i++) {
            map[startPage + i] = merged;
          }
        } else {
          pageQueue.push(holder);
        }
      }
    }
  }

  long allocationPosition() {
    return allocationPosition;
  }

  int[] heaps() {
    int[] result = new int[heapSize];
    for (int i = 0; i < heapSize; i++) {
      Heap heap = heaps[i];
      result[i] = heap.buffer.capacity();
    }

    return result;
  }

  int[][] snapshot() {
    int[][] result = new int[heapSize][];

    for (int i = 0; i < heapSize; i++) {
      final Heap heap = heaps[i];

      result[i] = new int[heap.pageMap.length];
      for (int j = 0; j < result[i].length; j++) {
        final OByteBufferHolder holder = heap.pageMap[j];
        if (holder != null) {
          assert holder.heap == i;
          result[i][j] = holder.position;
        } else {
          result[i][j] = -1;
        }
      }
    }

    return result;
  }

  boolean[][] acquiredSnapshot() {
    boolean[][] result = new boolean[heapSize][];

    for (int i = 0; i < heapSize; i++) {
      final Heap heap = heaps[i];

      result[i] = new boolean[heap.pageMap.length];

      for (int j = 0; j < result[i].length; j++) {
        final OByteBufferHolder holder = heap.pageMap[j];
        if (holder != null) {
          result[i][j] = holder.isAcquired();
        } else {
          result[i][j] = false;
        }
      }
    }

    return result;
  }

  private OByteBufferHolder allocateQuatroPage() {
    final int pageCount = 4;

    OByteBufferHolder holder = quatroPageQueue.pull();

    if (holder == null) {
      final OByteBufferHolder mergedHolder = roundAllocationTillFourIfNeeded();

      if (mergedHolder != null)
        return mergedHolder;

      long newAllocationPosition = allocationPosition + pageCount * pageSize;

      final int heapIndex = (int) (allocationPosition / maxHeapAllocationSize);
      final int newHeapIndex = (int) ((newAllocationPosition - 1) / maxHeapAllocationSize);

      assert heapIndex == newHeapIndex || heapIndex + 1 == newHeapIndex;

      if (heapIndex != newHeapIndex) {
        final int restOfPages = (int) ((newHeapIndex * maxHeapAllocationSize - allocationPosition) / pageSize);

        assert restOfPages < 4 && restOfPages >= 1;

        int position = (int) (allocationPosition - heapIndex * maxHeapAllocationSize);

        final Heap heap = heaps[heapIndex];

        ByteBuffer heapBuffer = heap.buffer;
        OByteBufferHolder[] map = heap.pageMap;

        for (int i = 0; i < restOfPages; i++) {
          heapBuffer.limit(position + pageSize);
          heapBuffer.position(position);

          final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
          final OByteBufferHolder h = new OByteBufferHolder(buffer, heapIndex, position);

          pageQueue.push(h);

          map[position / pageSize] = h;

          position += pageSize;
          allocationPosition += pageSize;
        }

        assert newHeapIndex * maxHeapAllocationSize == allocationPosition;
      }

      newAllocationPosition = allocationPosition + pageCount * pageSize;

      if (newAllocationPosition > maxAllocationSize) {
        throw new OutOfMemoryError(
            "Requested memory size " + newAllocationPosition + " is bigger than allowed memory size " + maxAllocationSize);
      }

      allocateHeapIfNeeded(newHeapIndex);

      final Heap heap = heaps[newHeapIndex];

      final ByteBuffer heapBuffer = heap.buffer;
      final int position = (int) (allocationPosition - newHeapIndex * maxHeapAllocationSize);

      heapBuffer.limit(position + pageCount * pageSize);
      heapBuffer.position(position);

      final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
      final OByteBufferHolder h = new OByteBufferHolder(buffer, newHeapIndex, position);

      final OByteBufferHolder[] map = heap.pageMap;

      final int start = position / pageSize;
      for (int i = 0; i < 4; i++) {
        assert map[start + i] == null;

        map[start + i] = h;
      }

      allocationPosition = newAllocationPosition;

      return h;
    }

    return holder;
  }

  private OByteBufferHolder roundAllocationTillFourIfNeeded() {
    final int pageCount = 4;

    final int heapIndex = (int) (allocationPosition / maxHeapAllocationSize);

    int pageIndex = (int) (allocationPosition - heapIndex * maxHeapAllocationSize) / pageSize;
    int reminder = pageCount - pageIndex & 3;

    if (reminder == 0)
      return null;

    long newAllocationPosition = allocationPosition + (pageCount + reminder) * pageSize;

    if (newAllocationPosition > maxAllocationSize) {
      throw new OutOfMemoryError(
          "Requested memory size " + newAllocationPosition + " is bigger than allowed memory size " + maxAllocationSize);
    }

    Heap heap = heaps[heapIndex];

    OByteBufferHolder[] map = heap.pageMap;
    final ByteBuffer heapBuffer = heap.buffer;

    final int startPage = pageIndex - (pageCount - reminder);
    assert (startPage & 3) == 0;

    boolean isFree = false;

    if (startPage + pageCount < map.length) {
      for (int i = 0; i < pageCount; i++) {
        final OByteBufferHolder h = map[startPage + i];

        if (h != null) {
          isFree = !h.isAcquired();
        } else {
          isFree = true;
        }

        if (!isFree)
          break;
      }
    }

    if (isFree) {
      final int position = startPage * pageSize;

      heapBuffer.limit(position + pageCount * pageSize);
      heapBuffer.position(position);

      final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
      final OByteBufferHolder merged = new OByteBufferHolder(buffer, heapIndex, position);

      for (int i = 0; i < pageCount; i++) {
        final OByteBufferHolder old = map[startPage + i];

        if (old != null) {
          pageQueue.remove(old);
        }

        map[startPage + i] = merged;
      }

      allocationPosition += reminder * pageSize;

      return merged;
    }

    int position = pageIndex * pageSize;

    while (reminder > 0 && reminder < 4) {
      if (position >= maxHeapAllocationSize)
        break;

      heapBuffer.limit(position + pageSize);
      heapBuffer.position(position);

      final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
      final OByteBufferHolder cap = new OByteBufferHolder(buffer, heapIndex, position);

      pageQueue.push(cap);

      assert map[pageIndex] == null;

      map[pageIndex] = cap;

      pageIndex++;
      reminder--;
      position += pageSize;

      allocationPosition += pageSize;
    }

    return null;
  }

  private OByteBufferHolder allocateSinglePage() {
    OByteBufferHolder holder = pageQueue.pull();

    if (holder == null) {
      holder = quatroPageQueue.pull();

      if (holder != null) {
        return splitQuatroPage(holder);
      }

      final long newAllocationPosition = allocationPosition + pageSize;

      if (newAllocationPosition > maxAllocationSize) {
        throw new OutOfMemoryError(
            "Requested memory size " + newAllocationPosition + " is bigger than allowed memory size " + maxAllocationSize);
      }

      final int heapIndex = (int) (allocationPosition / maxHeapAllocationSize);

      allocateHeapIfNeeded(heapIndex);

      final Heap heap = heaps[heapIndex];

      final ByteBuffer heapBuffer = heap.buffer;
      final OByteBufferHolder[] map = heap.pageMap;

      final int position = (int) (allocationPosition - heapIndex * maxHeapAllocationSize);

      heapBuffer.limit(position + pageSize);
      heapBuffer.position(position);

      final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());

      final OByteBufferHolder result = new OByteBufferHolder(buffer, heapIndex, position);

      assert map[position / pageSize] == null;

      map[position / pageSize] = result;

      allocationPosition += pageSize;

      return result;
    } else {
      return holder;
    }
  }

  private void allocateHeapIfNeeded(int heapIndex) {
    while (heapIndex >= heapSize) {
      heapSize++;

      if (heapSize > heaps.length) {
        heaps = Arrays.copyOf(heaps, Math.max(heaps.length << 1, 1));
      }

      final long allocated = (heapSize - 1) * maxHeapAllocationSize;
      final int allocationSize = (int) Math.min(maxHeapAllocationSize, maxAllocationSize - allocated);

      final Heap heap = new Heap(allocationSize, pageSize);
      heaps[heapSize - 1] = heap;
    }
  }

  private OByteBufferHolder splitQuatroPage(OByteBufferHolder holder) {
    final Heap heap = heaps[holder.heap];

    final ByteBuffer heapBuffer = heap.buffer;
    final OByteBufferHolder[] map = heap.pageMap;

    for (int i = 0; i < 3; i++) {
      final int position = holder.position + i * pageSize;

      heapBuffer.limit(position + pageSize);
      heapBuffer.position(position);

      final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());

      final OByteBufferHolder splitted = new OByteBufferHolder(buffer, holder.heap, position);

      assert map[position / pageSize] == holder;

      map[position / pageSize] = splitted;

      pageQueue.push(splitted);
    }

    final int position = holder.position + 3 * pageSize;

    heapBuffer.limit(position + pageSize);
    heapBuffer.position(position);

    final ByteBuffer buffer = heapBuffer.slice().order(ByteOrder.nativeOrder());
    final OByteBufferHolder splitted = new OByteBufferHolder(buffer, holder.heap, position);

    assert map[position / pageSize] == holder;
    map[position / pageSize] = splitted;

    return splitted;
  }

  private static final class Heap {
    private final ByteBuffer          buffer;
    private final OByteBufferHolder[] pageMap;

    Heap(int allocationSize, int pageSize) {
      buffer = ByteBuffer.allocateDirect(allocationSize).order(ByteOrder.nativeOrder());
      pageMap = new OByteBufferHolder[allocationSize / pageSize];
    }
  }

  public static OTwoSizeBufferPool instance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private static final OTwoSizeBufferPool INSTANCE;

    static {
      // page size in bytes
      final int pageSize = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

      // Maximum amount of chunk size which should be allocated at once by system
      final int memoryChunkSize = OGlobalConfiguration.MEMORY_CHUNK_SIZE.getValueAsInteger();

      final long diskCacheSize = OGlobalConfiguration.DISK_CACHE_SIZE.getValueAsInteger() * 1024L * 1024L;

      // instance of byte buffer which should be used by all storage components
      INSTANCE = new OTwoSizeBufferPool((long) (diskCacheSize * 1.2), memoryChunkSize, pageSize);
    }
  }

}
