package com.orientechnologies.common.concur.dreadlock;

import java.util.List;
import java.util.Set;

/**
 * Base class for both lock and thread vertices of wait-for graph is used to detect cycles in provided
 * graph instance.
 */
public abstract class OWaitForVertex {
  /**
   * Index of vertex is used only for testing of graph algorithms.
   */
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

  public OWaitForVertex(int index) {
    this.index = index;
  }

  public abstract Set<? extends OWaitForVertex> getAdjacentVertexes();
}
