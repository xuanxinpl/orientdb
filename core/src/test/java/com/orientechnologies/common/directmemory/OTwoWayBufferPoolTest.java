package com.orientechnologies.common.directmemory;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class OTwoWayBufferPoolTest {
  private static final int PAGE_SIZE           = 2;
  private static final int MAX_HEAP_SIZE       = 16;
  private static final int MAX_ALLOCATION_SIZE = 43;

  @Test
  public void testAllocateDeallocateSinglePage() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    final OByteBufferHolder holder = bufferPool.acquire(PAGE_SIZE, false);
    Assert.assertTrue(holder.isAcquired());

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

  }

  @Test
  public void testAllocateOneAndFourPage() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    final OByteBufferHolder firstOnePageHolder = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(PAGE_SIZE, bufferPool.allocationPosition());

    final OByteBufferHolder secondOnePageHolder = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(2 * PAGE_SIZE, bufferPool.allocationPosition());

    final OByteBufferHolder thirdOnePageHolder = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(3 * PAGE_SIZE, bufferPool.allocationPosition());

    final OByteBufferHolder fourthOnePageHolder = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(firstOnePageHolder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(thirdOnePageHolder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(fourthOnePageHolder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(secondOnePageHolder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    final OByteBufferHolder fourPageHolder = bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(fourPageHolder);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());
  }

  @Test
  public void testAllocateEightOnePages() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] {}, bufferPool.acquiredSnapshot());

    final OByteBufferHolder[] holders = new OByteBufferHolder[8];

    holders[0] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(PAGE_SIZE, bufferPool.allocationPosition());

    holders[1] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(2 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[2] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(3 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[3] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[4] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, -1, -1, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, false, false, false } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(5 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[5] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, -1, -1 } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(6 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[6] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, -1 } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, false } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(7 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[7] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[0]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { false, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[2]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { false, true, false, true, true, true, true, true } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[4]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, true, false, true, true, true } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[6]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, true, false, true, false, true } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[1]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, true, false, true, false, true } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[3]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, true, false, true } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[5]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, true } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[7]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());
  }

  @Test
  public void testAllocateEightAndFourOnePages() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] {}, bufferPool.acquiredSnapshot());

    final OByteBufferHolder[] holders = new OByteBufferHolder[8];

    holders[0] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(PAGE_SIZE, bufferPool.allocationPosition());

    holders[1] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(2 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[2] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(3 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[3] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[4] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, -1, -1, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(5 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[5] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, -1, -1 } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(6 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[6] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, -1 } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(7 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[7] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[2]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, false, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[6]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, false, true, true, true, false, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[5]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, true, true, false, false, true } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[3]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, true, false, false, true } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[4]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, true } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.release(holders[7]);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[2] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[3] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[7] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, true } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[4] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, false, false, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[5] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, false, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    holders[6] = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    for (OByteBufferHolder holder : holders) {
      bufferPool.release(holder);
    }

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());
  }

  @Test
  public void testAllocateAtTheBorder() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] {}, bufferPool.acquiredSnapshot());

    for (int i = 0; i < 8; i++) {
      Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));
    }

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(8 * PAGE_SIZE, bufferPool.allocationPosition());

    Assert.assertNotNull(bufferPool.acquire(4 * PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true },
        { true, true, true, true, false, false, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(12 * PAGE_SIZE, bufferPool.allocationPosition());
  }

  @Test
  public void testExhaustBuffer() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] {}, bufferPool.acquiredSnapshot());

    for (int i = 0; i < 7; i++) {
      bufferPool.acquire(PAGE_SIZE, false);
    }

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, -1 } },
        bufferPool.snapshot());
    Assert
        .assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(7 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, false },
        { true, true, true, true, false, false, false, false } }, bufferPool.acquiredSnapshot());

    Assert.assertEquals(12 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true },
        { true, true, true, true, false, false, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(12 * PAGE_SIZE, bufferPool.allocationPosition());

    for (int i = 0; i < 2; i++) {
      bufferPool.acquire(PAGE_SIZE, false);
    }

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true },
        { true, true, true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(14 * PAGE_SIZE, bufferPool.allocationPosition());

    bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE, 10 }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE }, { 0, 0, 0, 0, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(
        new boolean[][] { { true, true, true, true, true, true, true, true }, { true, true, true, true, true, true, false, false },
            { true, true, true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(20 * PAGE_SIZE, bufferPool.allocationPosition());

    Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE, 10 }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE }, { 0, 0, 0, 0, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(
        new boolean[][] { { true, true, true, true, true, true, true, true }, { true, true, true, true, true, true, true, false },
            { true, true, true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(20 * PAGE_SIZE, bufferPool.allocationPosition());

    Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE, 10 }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE }, { 0, 0, 0, 0, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(
        new boolean[][] { { true, true, true, true, true, true, true, true }, { true, true, true, true, true, true, true, true },
            { true, true, true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(20 * PAGE_SIZE, bufferPool.allocationPosition());

    Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE, 10 }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE }, { 0, 0, 0, 0, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(
        new boolean[][] { { true, true, true, true, true, true, true, true }, { true, true, true, true, true, true, true, true },
            { true, true, true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(21 * PAGE_SIZE, bufferPool.allocationPosition());

    try {
      bufferPool.acquire(PAGE_SIZE, false);
      Assert.fail();
    } catch (OutOfMemoryError e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testExhaustBufferTwo() {
    OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(34, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] {}, bufferPool.acquiredSnapshot());

    for (int i = 0; i < 7; i++) {
      bufferPool.acquire(PAGE_SIZE, false);
    }

    bufferPool.acquire(4 * PAGE_SIZE, false);
    bufferPool.acquire(PAGE_SIZE, false);
    for (int i = 0; i < 2; i++) {
      bufferPool.acquire(PAGE_SIZE, false);
    }

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE, MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE, 6 * PAGE_SIZE, 7 * PAGE_SIZE },
            { 0, 0, 0, 0, 4 * PAGE_SIZE, 5 * PAGE_SIZE, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true },
        { true, true, true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());

    try {
      bufferPool.acquire(4 * PAGE_SIZE, false);
      Assert.fail();
    } catch (OutOfMemoryError e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testInvalidPageSize() {
    try {
      OTwoWayBufferPool pool = new OTwoWayBufferPool(32, 16, 6);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testAcquireInvalidPageNumber() {
    final OTwoWayBufferPool pool = new OTwoWayBufferPool(32, 16, 2);
    try {
      pool.acquire(5, false);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testPageCleanOne() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    OByteBufferHolder firstPage = bufferPool.acquire(PAGE_SIZE, false);
    OByteBufferHolder secondPage = bufferPool.acquire(PAGE_SIZE, false);

    ByteBuffer firstPageBuffer = firstPage.getBuffer();

    firstPageBuffer.position(0);
    firstPageBuffer.put((byte) 42);

    ByteBuffer secondPageBuffer = secondPage.getBuffer();

    secondPageBuffer.position(0);
    secondPageBuffer.put((byte) 42);

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);

    firstPage = bufferPool.acquire(PAGE_SIZE, false);
    secondPage = bufferPool.acquire(PAGE_SIZE, false);

    firstPageBuffer = firstPage.getBuffer();
    secondPageBuffer = secondPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(42, firstPageBuffer.get());

    secondPageBuffer.position(0);
    Assert.assertEquals(42, secondPageBuffer.get());

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);

    firstPage = bufferPool.acquire(PAGE_SIZE, true);
    secondPage = bufferPool.acquire(PAGE_SIZE, true);

    firstPageBuffer = firstPage.getBuffer();
    secondPageBuffer = secondPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(0, firstPageBuffer.get());

    secondPageBuffer.position(0);
    Assert.assertEquals(0, secondPageBuffer.get());

    firstPageBuffer.position(0);
    firstPageBuffer.put((byte) 42);

    secondPageBuffer.position(0);
    secondPageBuffer.put((byte) 42);

    OByteBufferHolder thirdPage = bufferPool.acquire(PAGE_SIZE, false);
    ByteBuffer thirdPageBuffer = thirdPage.getBuffer();

    thirdPageBuffer.position(0);
    thirdPageBuffer.put((byte) 42);

    OByteBufferHolder fourthPage = bufferPool.acquire(PAGE_SIZE, false);
    ByteBuffer fourthPageBuffer = fourthPage.getBuffer();

    fourthPageBuffer.position(0);
    fourthPageBuffer.put((byte) 42);

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);
    bufferPool.release(thirdPage);
    bufferPool.release(fourthPage);

    firstPage = bufferPool.acquire(PAGE_SIZE, true);
    firstPageBuffer = firstPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(0, firstPageBuffer.get());

    secondPage = bufferPool.acquire(PAGE_SIZE, true);
    secondPageBuffer = secondPage.getBuffer();

    secondPageBuffer.position(0);
    Assert.assertEquals(0, secondPageBuffer.get());

    thirdPage = bufferPool.acquire(PAGE_SIZE, true);
    thirdPageBuffer = thirdPage.getBuffer();

    thirdPageBuffer.position(0);
    Assert.assertEquals(0, thirdPageBuffer.get());

    fourthPage = bufferPool.acquire(PAGE_SIZE, true);
    fourthPageBuffer = fourthPage.getBuffer();

    fourthPageBuffer.position(0);
    Assert.assertEquals(0, fourthPageBuffer.get());

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);
    bufferPool.release(thirdPage);
    bufferPool.release(fourthPage);
  }

  @Test
  public void testPageCleanTwo() {
    final int pageSize = 2048;

    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(1024 * 1024, 256 * 1024, pageSize);

    OByteBufferHolder firstPage = bufferPool.acquire(pageSize, false);
    OByteBufferHolder secondPage = bufferPool.acquire(pageSize, false);

    ByteBuffer firstPageBuffer = firstPage.getBuffer();

    firstPageBuffer.position(0);
    firstPageBuffer.putInt(42);

    ByteBuffer secondPageBuffer = secondPage.getBuffer();

    secondPageBuffer.position(0);
    secondPageBuffer.putInt(42);

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);

    firstPage = bufferPool.acquire(pageSize, false);
    secondPage = bufferPool.acquire(pageSize, false);

    firstPageBuffer = firstPage.getBuffer();
    secondPageBuffer = secondPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(42, firstPageBuffer.getInt());

    secondPageBuffer.position(0);
    Assert.assertEquals(42, secondPageBuffer.getInt());

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);

    firstPage = bufferPool.acquire(pageSize, true);
    secondPage = bufferPool.acquire(pageSize, true);

    firstPageBuffer = firstPage.getBuffer();
    secondPageBuffer = secondPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(0, firstPageBuffer.getInt());

    secondPageBuffer.position(0);
    Assert.assertEquals(0, secondPageBuffer.getInt());

    firstPageBuffer.position(0);
    firstPageBuffer.putInt(42);

    secondPageBuffer.position(0);
    secondPageBuffer.putInt(42);

    OByteBufferHolder thirdPage = bufferPool.acquire(pageSize, false);
    ByteBuffer thirdPageBuffer = thirdPage.getBuffer();

    thirdPageBuffer.position(0);
    thirdPageBuffer.putInt(42);

    OByteBufferHolder fourthPage = bufferPool.acquire(pageSize, false);
    ByteBuffer fourthPageBuffer = fourthPage.getBuffer();

    fourthPageBuffer.position(0);
    fourthPageBuffer.putInt(42);

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);
    bufferPool.release(thirdPage);
    bufferPool.release(fourthPage);

    firstPage = bufferPool.acquire(pageSize, true);
    firstPageBuffer = firstPage.getBuffer();

    firstPageBuffer.position(0);
    Assert.assertEquals(0, firstPageBuffer.getInt());

    secondPage = bufferPool.acquire(pageSize, true);
    secondPageBuffer = secondPage.getBuffer();

    secondPageBuffer.position(0);
    Assert.assertEquals(0, secondPageBuffer.getInt());

    thirdPage = bufferPool.acquire(pageSize, true);
    thirdPageBuffer = thirdPage.getBuffer();

    thirdPageBuffer.position(0);
    Assert.assertEquals(0, thirdPageBuffer.getInt());

    fourthPage = bufferPool.acquire(pageSize, true);
    fourthPageBuffer = fourthPage.getBuffer();

    fourthPageBuffer.position(0);
    Assert.assertEquals(0, fourthPageBuffer.getInt());

    bufferPool.release(firstPage);
    bufferPool.release(secondPage);
    bufferPool.release(thirdPage);
    bufferPool.release(fourthPage);
  }

  @Test
  public void testPartiallyFilledHeap() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(6, 6, 2);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][][] {}, bufferPool.acquiredSnapshot());

    OByteBufferHolder firstPage = bufferPool.acquire(2, false);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(2, bufferPool.allocationPosition());

    OByteBufferHolder secondPage = bufferPool.acquire(2, false);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 2, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(4, bufferPool.allocationPosition());

    OByteBufferHolder thirdPage = bufferPool.acquire(2, false);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 2, 4 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(6, bufferPool.allocationPosition());

    bufferPool.release(firstPage);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 2, 4 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, true } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(6, bufferPool.allocationPosition());

    bufferPool.release(thirdPage);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 2, 4 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(6, bufferPool.allocationPosition());

    bufferPool.release(secondPage);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 2, 4 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false } }, bufferPool.acquiredSnapshot());
    Assert.assertEquals(6, bufferPool.allocationPosition());
  }

  @Test
  public void testExhaustHeapNotFitInQuatroPage() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(18 * PAGE_SIZE, 6 * PAGE_SIZE, PAGE_SIZE);

    for (int i = 0; i < 4; i++) {
      Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));
    }

    Assert.assertEquals(4 * PAGE_SIZE, bufferPool.allocationPosition());
    Assert.assertArrayEquals(new int[] { 6 * PAGE_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());

    Assert.assertNotNull(bufferPool.acquire(4 * PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { 6 * PAGE_SIZE, 6 * PAGE_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE }, { 0, 0, 0, 0, -1, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false }, { true, true, true, true, false, false } },
        bufferPool.acquiredSnapshot());
    Assert.assertEquals(10 * PAGE_SIZE, bufferPool.allocationPosition());
  }

  @Test
  public void testExhaustHeapNotFitInQuatroPageOverflow() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(8 * PAGE_SIZE, 6 * PAGE_SIZE, PAGE_SIZE);

    for (int i = 0; i < 4; i++) {
      Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));
    }

    Assert.assertArrayEquals(new int[] { 6 * PAGE_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false } }, bufferPool.acquiredSnapshot());

    try {
      bufferPool.acquire(4 * PAGE_SIZE, false);
      Assert.fail();
    } catch (OutOfMemoryError e) {
      Assert.assertTrue(true);
    }

  }

  @Test
  public void testExhaustPartialHeapByQuatroPage() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(10, 6, 2);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][][] {}, bufferPool.acquiredSnapshot());

    OByteBufferHolder firstPage = bufferPool.acquire(2, false);

    Assert.assertArrayEquals(new int[] { 6 }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false } }, bufferPool.acquiredSnapshot());

    try {
      bufferPool.acquire(8, false);
      Assert.fail();
    } catch (OutOfMemoryError e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testAllocationQuatroPageAfterSinglePage() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][][] {}, bufferPool.acquiredSnapshot());

    OByteBufferHolder firstPage = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    OByteBufferHolder quatroPage = bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, true, true, true, true } },
        bufferPool.acquiredSnapshot());

    bufferPool.release(firstPage);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, true, true, true, true } },
        bufferPool.acquiredSnapshot());

    bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
  }

  @Test
  public void testAllocationQuatroPageAfterSinglePageInTheMiddle() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][][] {}, bufferPool.acquiredSnapshot());

    OByteBufferHolder firstPage = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    OByteBufferHolder secondPage = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    bufferPool.release(firstPage);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    OByteBufferHolder quatroPage = bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, true, false, false, true, true, true, true } },
        bufferPool.acquiredSnapshot());

    bufferPool.release(secondPage);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, true, true, true, true } },
        bufferPool.acquiredSnapshot());

    bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE, 4 * PAGE_SIZE } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, true, true, true } }, bufferPool.acquiredSnapshot());
  }

  @Test
  public void testAllocationQuatroPageAfterSinglePageRelease() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(MAX_ALLOCATION_SIZE, MAX_HEAP_SIZE, PAGE_SIZE);

    Assert.assertArrayEquals(new int[] {}, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] {}, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][][] {}, bufferPool.acquiredSnapshot());

    OByteBufferHolder firstPage = bufferPool.acquire(PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    bufferPool.release(firstPage);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, -1, -1, -1, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { false, false, false, false, false, false, false, false } },
        bufferPool.acquiredSnapshot());

    bufferPool.acquire(4 * PAGE_SIZE, false);

    Assert.assertArrayEquals(new int[] { MAX_HEAP_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, 0, 0, 0, -1, -1, -1, -1 } }, bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, false, false, false, false } },
        bufferPool.acquiredSnapshot());
  }

  @Test
  public void testAllocationQuatroPageAtTheEndOfTheHeap() {
    final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(18 * PAGE_SIZE, 6 * PAGE_SIZE, PAGE_SIZE);

    for (int i = 0; i < 5; i++) {
      Assert.assertNotNull(bufferPool.acquire(PAGE_SIZE, false));
    }

    Assert.assertArrayEquals(new int[] { 6 * PAGE_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, -1, } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, false } }, bufferPool.acquiredSnapshot());

    Assert.assertNotNull(bufferPool.acquire(4 * PAGE_SIZE, false));

    Assert.assertArrayEquals(new int[] { 6 * PAGE_SIZE, 6 * PAGE_SIZE }, bufferPool.heaps());
    Assert.assertArrayEquals(
        new int[][] { { 0, PAGE_SIZE, 2 * PAGE_SIZE, 3 * PAGE_SIZE, 4 * PAGE_SIZE, 5 * PAGE_SIZE }, { 0, 0, 0, 0, -1, -1 } },
        bufferPool.snapshot());
    Assert.assertArrayEquals(new boolean[][] { { true, true, true, true, true, false }, { true, true, true, true, false, false } },
        bufferPool.acquiredSnapshot());
  }

  @Test
  public void testThreadSafety() throws Exception {
    for (int i = 0; i < 100; i++) {
      System.out.println("Iteration " + i);

      final int pageSize = 2;

      final ExecutorService executor = Executors.newCachedThreadPool();
      final OTwoWayBufferPool bufferPool = new OTwoWayBufferPool(1024, 130, pageSize);

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicBoolean stop = new AtomicBoolean();

      final List<Future> futures = new ArrayList<>();

      futures.add(executor.submit(new RequestReleasePages(bufferPool, latch, stop, pageSize)));
      futures.add(executor.submit(new RequestTenPagesReleaseTenPages(bufferPool, latch, stop, pageSize)));
      futures.add(executor.submit(new RequestTenReleaseTenReverseOrder(bufferPool, latch, stop, pageSize)));
      futures.add(executor.submit(new RequestTenPagesReleaseOneAtRandom(bufferPool, latch, stop, pageSize)));

      futures.add(executor.submit(new RequestReleasePages(bufferPool, latch, stop, 4 * pageSize)));
      futures.add(executor.submit(new RequestTenPagesReleaseTenPages(bufferPool, latch, stop, 4 * pageSize)));
      futures.add(executor.submit(new RequestTenReleaseTenReverseOrder(bufferPool, latch, stop, 4 * pageSize)));
      futures.add(executor.submit(new RequestTenPagesReleaseOneAtRandom(bufferPool, latch, stop, 4 * pageSize)));

      latch.countDown();

      Thread.sleep(10000);

      stop.set(true);

      for (Future future : futures) {
        future.get();
      }

      System.out.println(bufferPool.allocationPosition());
      Assert
          .assertArrayEquals(new boolean[][] { new boolean[65], new boolean[65], new boolean[65] }, bufferPool.acquiredSnapshot());

      int[][] acquiredSnapshot = bufferPool.snapshot();

      int[] acquired1 = acquiredSnapshot[0];
      Assert.assertEquals(65, acquired1.length);

      for (int j = 0; j < 64; j++) {
        final int pageIndex = j / 4;
        Assert.assertEquals(acquired1[j], pageSize * pageIndex * 4);
      }

      Assert.assertEquals(acquired1[64], 64 * pageSize);

      int[] acquired2 = acquiredSnapshot[1];
      Assert.assertEquals(65, acquired2.length);

      for (int j = 0; j < 64; j++) {
        final int pageIndex = j / 4;
        Assert.assertEquals(acquired2[j], pageSize * pageIndex * 4);
      }

      Assert.assertEquals(acquired2[64], 64 * pageSize);

      int[] acquired3 = acquiredSnapshot[2];
      Assert.assertEquals(65, acquired3.length);

      int lastFourItemsIndex = -1;
      for (int j = 0; j < 64; j++) {
        final int pageIndex = j / 4;
        if (acquired3[j] != pageSize * pageIndex * 4) {
          lastFourItemsIndex = j;
          break;
        }
      }

      System.out.println("Not four index " + lastFourItemsIndex);

      if (lastFourItemsIndex != -1) {
        int counter = 1;

        boolean notAllocated = false;
        for (int j = lastFourItemsIndex; j < 65; j++) {
          final int position = acquired3[j];

          if (notAllocated) {
            Assert.assertEquals(-1, position);
          } else {
            if (position == -1) {
              notAllocated = true;
              System.out.println("Not allocated " + j);
            } else {
              Assert.assertEquals(j * pageSize, position);
              counter++;
              Assert.assertTrue(counter < 4);
            }
          }
        }
      }
    }
  }

  private final static class RequestReleasePages implements Callable<Void> {
    private final OTwoWayBufferPool bufferPool;
    private final CountDownLatch    latch;
    private final AtomicBoolean     stop;
    private final int               pageSize;

    public RequestReleasePages(OTwoWayBufferPool bufferPool, CountDownLatch latch, AtomicBoolean stop, int pageSize) {
      this.bufferPool = bufferPool;
      this.latch = latch;
      this.stop = stop;
      this.pageSize = pageSize;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      while (!stop.get()) {
        final OByteBufferHolder holder = bufferPool.acquire(pageSize, false);
        bufferPool.release(holder);
      }

      return null;
    }
  }

  private final static class RequestTenPagesReleaseTenPages implements Callable<Void> {
    private final OTwoWayBufferPool bufferPool;
    private final CountDownLatch    latch;
    private final AtomicBoolean     stop;
    private final int               pageSize;

    public RequestTenPagesReleaseTenPages(OTwoWayBufferPool bufferPool, CountDownLatch latch, AtomicBoolean stop, int pageSize) {
      this.bufferPool = bufferPool;
      this.latch = latch;
      this.stop = stop;
      this.pageSize = pageSize;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      while (!stop.get()) {
        List<OByteBufferHolder> holders = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
          holders.add(bufferPool.acquire(pageSize, false));
        }

        for (OByteBufferHolder holder : holders) {
          bufferPool.release(holder);
        }
      }
      return null;
    }
  }

  private static final class RequestTenReleaseTenReverseOrder implements Callable<Void> {
    private final OTwoWayBufferPool bufferPool;
    private final CountDownLatch    latch;
    private final AtomicBoolean     stop;
    private final int               pageSize;

    public RequestTenReleaseTenReverseOrder(OTwoWayBufferPool bufferPool, CountDownLatch latch, AtomicBoolean stop, int pageSize) {
      this.bufferPool = bufferPool;
      this.latch = latch;
      this.stop = stop;
      this.pageSize = pageSize;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      while (!stop.get()) {
        List<OByteBufferHolder> holders = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
          holders.add(bufferPool.acquire(pageSize, false));
        }

        for (int i = 9; i >= 0; i--) {
          bufferPool.release(holders.get(i));
        }
      }

      return null;
    }
  }

  private static final class RequestTenPagesReleaseOneAtRandom implements Callable<Void> {
    private final OTwoWayBufferPool bufferPool;
    private final CountDownLatch    latch;
    private final AtomicBoolean     stop;
    private final int               pageSize;

    public RequestTenPagesReleaseOneAtRandom(OTwoWayBufferPool bufferPool, CountDownLatch latch, AtomicBoolean stop, int pageSize) {
      this.bufferPool = bufferPool;
      this.latch = latch;
      this.stop = stop;
      this.pageSize = pageSize;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      List<OByteBufferHolder> holders = new ArrayList<>();

      for (int i = 0; i < 10; i++) {
        holders.add(bufferPool.acquire(pageSize, false));
      }

      Random random = new Random();

      while (!stop.get()) {
        final int index = random.nextInt(holders.size());
        bufferPool.release(holders.get(index));

        holders.set(index, bufferPool.acquire(pageSize, false));
      }

      for (OByteBufferHolder holder : holders) {
        bufferPool.release(holder);
      }

      return null;
    }
  }

}
