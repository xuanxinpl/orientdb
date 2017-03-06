package com.orientechnologies.common.directmemory;

import java.nio.ByteBuffer;

public class OByteBufferHolder {
  OByteBufferHolder prev;
  OByteBufferHolder next;

  private final ByteBuffer buffer;

  final int heap;
  final int position;

  public OByteBufferHolder(ByteBuffer buffer, int heap, int position) {
    this.buffer = buffer;
    this.heap = heap;
    this.position = position;
  }

  boolean isAcquired() {
    return prev == null && next == null;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }
}
