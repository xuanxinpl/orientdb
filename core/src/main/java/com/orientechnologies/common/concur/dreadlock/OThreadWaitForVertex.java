package com.orientechnologies.common.concur.dreadlock;

import java.lang.ref.WeakReference;

public class OThreadWaitForVertex {
  private final    WeakReference<Thread> threadWeakReference;
  private volatile OLockWaitForVertex    waitingFor;

  public OThreadWaitForVertex(Thread thread) {
    this.threadWeakReference = new WeakReference<Thread>(thread);
  }

  public OLockWaitForVertex getWaitingFor() {
    return waitingFor;
  }

  public void setWaitingFor(OLockWaitForVertex waitingFor) {
    this.waitingFor = waitingFor;
  }
}
