package com.orientechnologies.common.concur.dreadlock;

import java.util.*;

/**
 * Johnson algorithm to find elementary cycles inside of strongly connected components.
 * Input of this algorithm is list of thread and lock vertices which form strongly connected component of wait-for graph.
 * <p>
 * Output of this algorithm is list of thread and lock vertexes which form cycles in strongly connected component.
 * WARNING Input will be changes as result of work of this algorithm;
 */
public class OJohnsonWaitForGraph {
  private final Map<OWaitForVertex, Boolean> blocked = new HashMap<OWaitForVertex, Boolean>();
  private final List<OWaitForVertex> scc;
  private final Deque<OWaitForVertex> nodeStack = new ArrayDeque<OWaitForVertex>();
  //private final Map<OWaitForVertex, List<>>

  public OJohnsonWaitForGraph(List<OWaitForVertex> scc) {
    this.scc = scc;
  }

  public List<List<OWaitForVertex>> findCycles() {
    final List<List<OWaitForVertex>> result = new ArrayList<List<OWaitForVertex>>();

    return result;
  }

  private void circuit(List<List<OWaitForVertex>> result, List<OWaitForVertex> scc) {

  }
}
