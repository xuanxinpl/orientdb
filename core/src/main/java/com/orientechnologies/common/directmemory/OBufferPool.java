package com.orientechnologies.common.directmemory;

public interface OBufferPool {
  OByteBufferContainer acquire(int chunkSize, boolean clean);

  void release(OByteBufferContainer holder);
}
