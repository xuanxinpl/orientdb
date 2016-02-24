package com.orientechnologies.common.concur.dreadlock;

import java.util.concurrent.atomic.AtomicReference;

public class OThreadLocalWaitForVertex extends ThreadLocal<OThreadWaitForVertex> {
  private final ODeadLockDetector deadLockDetector;


  public OThreadLocalWaitForVertex(ODeadLockDetector deadLockDetector) {
    this.deadLockDetector = deadLockDetector;
  }

  @Override
  protected OThreadWaitForVertex initialValue() {
    final OThreadWaitForVertex waitForVertex = new OThreadWaitForVertex(Thread.currentThread());
    deadLockDetector.registerThreadWaitForVertex(waitForVertex);

    return waitForVertex;
  }
}
