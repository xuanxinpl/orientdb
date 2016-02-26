package com.orientechnologies.common.concur.dreadlock;

import java.util.HashSet;
import java.util.Set;

public class OJohnsonWaitForVertex extends OAbstractWaitForVertex {
  private final Set<OJohnsonWaitForVertex> adjacentList = new HashSet<OJohnsonWaitForVertex>();

  private final OAbstractWaitForVertex wrapped;

  public OJohnsonWaitForVertex(OAbstractWaitForVertex wrapped) {
    super(wrapped.index);

    this.wrapped = wrapped;
  }

  public void addAdjacentVertex(OJohnsonWaitForVertex v) {
    adjacentList.add(v);
  }

  @Override
  public Set<OJohnsonWaitForVertex> getAdjacentVertexes() {
    return adjacentList;
  }

  public OAbstractWaitForVertex getWrappedVertex() {
    return wrapped;
  }
}
