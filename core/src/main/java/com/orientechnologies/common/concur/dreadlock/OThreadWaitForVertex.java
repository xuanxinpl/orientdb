package com.orientechnologies.common.concur.dreadlock;

import java.lang.ref.WeakReference;

public class OThreadWaitForVertex {
  private final    WeakReference<Thread> threadWeakReference;
  private volatile OLockWaitForVertex    waitingFor;

  int index = -1;

  /**
   * Holds index of stack frame in depth first search of Tarjan algorithm.
   *
   * @see OTarjanWaitForGraph#findSCC()
   */
  int tarjanIndex = -1;

  /**
   * Holds index of lowest vertex accessible from stack in Tarjan algorithm.
   *
   * @see OTarjanWaitForGraph#findSCC()
   */
  int tarjanLowLink = -1;

  /**
   * Indicates whether this vertex is still inside of stack in Tarjan algorithm.
   *
   * @see OTarjanWaitForGraph#findSCC()
   */
  boolean tarjanOnStack = false;

  public OThreadWaitForVertex(int index) {
    this.index = index;
    this.threadWeakReference = null;
  }

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
