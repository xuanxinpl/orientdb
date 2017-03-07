package com.orientechnologies.common.directmemory;

import java.nio.ByteBuffer;

public class OSimpleBufferContainer implements OByteBufferContainer {
  private final ByteBuffer buffer;

  public OSimpleBufferContainer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public ByteBuffer getBuffer() {
    return buffer;
  }
}
