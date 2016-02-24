package com.orientechnologies.common.concur.dreadlock;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class OLockWaitForVertex {
  private final WeakReference<OReentrantReadWriteLock> readWriteLockWeakReference;

  private final Set<OThreadWaitForVertex> acquiredBy = Collections
      .newSetFromMap(new ConcurrentHashMap<OThreadWaitForVertex, Boolean>());

  public OLockWaitForVertex(OReentrantReadWriteLock readWriteLock) {
    this.readWriteLockWeakReference = new WeakReference<OReentrantReadWriteLock>(readWriteLock);
  }

  public Set<OThreadWaitForVertex> getAcquiredBy() {
    return Collections.unmodifiableSet(acquiredBy);
  }

  public void addAcquiredBy(OThreadWaitForVertex waitForVertex) {
    acquiredBy.add(waitForVertex);
  }

  public void removeAcquiredBy(OThreadWaitForVertex waitForVertex) {
    acquiredBy.remove(waitForVertex);
  }
}
