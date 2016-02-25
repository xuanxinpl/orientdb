package com.orientechnologies.common.concur.dreadlock;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class OThreadWaitForVertex extends OWaitForVertex {
  private final    WeakReference<Thread> threadWeakReference;
  private volatile OLockWaitForVertex    waitingFor;

  public OThreadWaitForVertex(int index) {
    super(index);
    this.threadWeakReference = null;
  }

  public OThreadWaitForVertex(Thread thread) {
    super(-1);
    this.threadWeakReference = new WeakReference<Thread>(thread);
  }

  public OLockWaitForVertex getWaitingFor() {
    return waitingFor;
  }

  public void setWaitingFor(OLockWaitForVertex waitingFor) {
    this.waitingFor = waitingFor;
  }

  @Override
  public Set<OLockWaitForVertex> getAdjacentVertexes() {
    if (waitingFor != null)
      return Collections.singleton(this.waitingFor);

    return Collections.emptySet();
  }
}
