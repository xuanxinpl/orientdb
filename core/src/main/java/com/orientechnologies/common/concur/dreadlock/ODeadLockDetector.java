package com.orientechnologies.common.concur.dreadlock;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ODeadLockDetector {
  private final Set<OThreadWaitForVertex> waitForVertices = Collections
      .newSetFromMap(new ConcurrentHashMap<OThreadWaitForVertex, Boolean>());

  public void registerThreadWaitForVertex(final OThreadWaitForVertex waitForVertex) {
    waitForVertices.add(waitForVertex);
  }
}
