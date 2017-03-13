package com.orientechnologies.common.directmemory;

import java.nio.ByteBuffer;

public interface OByteBufferContainer extends Comparable<OByteBufferContainer> {
  ByteBuffer getBuffer();
}
