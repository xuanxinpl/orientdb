package com.orientechnologies.common.directmemory;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class OByteBufferPoolTest {
  @Test
  public void testAcquireReleaseSinglePage() {
    final int pageSize = 12;

    OByteBufferPool pool = new OByteBufferPool(pageSize);
    Assert.assertEquals(pool.getSize(), 0);

    OByteBufferContainer container = pool.acquire(pageSize, true);
    ByteBuffer buffer = container.getBuffer();
    Assert.assertEquals(pool.getSize(), 0);
    Assert.assertEquals(buffer.position(), 0);

    buffer.position(10);
    buffer.put((byte) 42);

    pool.release(container);

    Assert.assertEquals(pool.getSize(), 1);

    container = pool.acquire(pageSize, false);
    buffer = container.getBuffer();

    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(10), 42);

    Assert.assertEquals(pool.getSize(), 0);

    pool.release(container);

    Assert.assertEquals(pool.getSize(), 1);

    container = pool.acquire(pageSize, true);
    buffer = container.getBuffer();

    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(10), 0);

    Assert.assertEquals(pool.getSize(), 0);

    pool.release(container);
    Assert.assertEquals(pool.getSize(), 1);
  }

  @Test
  public void testAcquireReleasePageWithPreallocation() {
    final int pageSize = 10;

    OByteBufferPool pool = new OByteBufferPool(pageSize, 300, 200);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);

    Assert.assertEquals(pool.getSize(), 0);

    List<OByteBufferContainer> containers = new ArrayList<>();
    for (int i = 0; i < 99; i++) {
      containers.add(pool.acquire(pageSize, false));
      assertBufferOperations(pool, 0, pageSize);
    }

    for (int i = 0; i < 99; i++) {
      final OByteBufferContainer container = containers.get(i);
      final ByteBuffer buffer = container.getBuffer();

      buffer.position(8);
      buffer.put((byte) 42);

      pool.release(containers.get(i));
      assertBufferOperations(pool, i + 1, pageSize);
    }
  }

  @Test
  @Ignore
  public void testAcquireReleasePageWithPreallocationInMT() throws Exception {
    final int pageSize = 10;
    final OByteBufferPool pool = new OByteBufferPool(pageSize, 300, 200);

    Assert.assertEquals(pool.getMaxPagesPerChunk(), 16);

    final List<Future<Void>> futures = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(1);
    final ExecutorService executor = Executors.newFixedThreadPool(8);

    for (int i = 0; i < 5; i++) {
      futures.add(executor.submit(() -> {
        latch.await();

        try {
          for (int n = 0; n < 1000000; n++) {
            List<OByteBufferContainer> containers = new ArrayList<>();

            for (int j = 0; j < 2000; j++) {
              containers.add(pool.acquire(pageSize, false));
            }

            for (int j = 0; j < 2000; j++) {
              final OByteBufferContainer container = containers.get(j);
              pool.release(container);
            }

          }
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }

        return null;
      }));
    }

    latch.countDown();

    for (Future<Void> future : futures) {
      future.get();
    }

  }

  private void assertBufferOperations(OByteBufferPool pool, int initialSize, int pageSize) {
    OByteBufferContainer container = pool.acquire(pageSize, true);
    ByteBuffer buffer = container.getBuffer();

    Assert.assertEquals(pool.getSize(), initialSize);
    Assert.assertEquals(buffer.position(), 0);

    buffer.position(8);
    buffer.put((byte) 42);

    pool.release(container);
    Assert.assertEquals(pool.getSize(), initialSize + 1);

    container = pool.acquire(pageSize, false);
    buffer = container.getBuffer();
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(8), 42);

    Assert.assertEquals(pool.getSize(), initialSize);

    pool.release(container);
    Assert.assertEquals(pool.getSize(), initialSize + 1);

    container = pool.acquire(pageSize, true);
    buffer = container.getBuffer();
    Assert.assertEquals(buffer.position(), 0);
    Assert.assertEquals(buffer.get(8), 0);

    Assert.assertEquals(pool.getSize(), initialSize);

    buffer.position(8);
    buffer.put((byte) 42);

    pool.release(container);
    Assert.assertEquals(pool.getSize(), initialSize + 1);
  }

}
