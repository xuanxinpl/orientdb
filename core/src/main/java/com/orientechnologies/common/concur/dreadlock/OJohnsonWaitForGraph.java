package com.orientechnologies.common.concur.dreadlock;

import java.util.*;

/**
 * Johnson algorithm to find elementary cycles inside of strongly connected components.
 * Input of this algorithm is list of thread and lock vertices which form strongly connected component of wait-for graph.
 * <p>
 * Output of this algorithm is list of thread and lock vertexes which form cycles in strongly connected component.
 * WARNING Input will be changes as result of work of this algorithm;
 */
public class OJohnsonWaitForGraph<T extends OAbstractWaitForVertex> {
  private final Set<T> blocked = new HashSet<T>();

  private final List<T> scc;

  private final Deque<T>                            nodeStack    = new ArrayDeque<T>();
  private final Map<T, Set<T>> blockedPaths = new HashMap<T, Set<T>>();

  private OAbstractWaitForVertex startVertex = null;

  public OJohnsonWaitForGraph(List<T> scc) {
    this.scc = scc;
  }

  public List<List<T>> findCycles() {
    final List<List<T>> result = new ArrayList<List<T>>();

    final Deque<? extends OAbstractWaitForVertex> sccsQueue = new ArrayDeque<OAbstractWaitForVertex>();
    List<? extends OAbstractWaitForVertex> currentSCC = scc;
    while (!currentSCC.isEmpty()) {
      startVertex = currentSCC.get(0);

      circuit(result, startVertex);

      startVertex.getAdjacentVertexes().clear();
      blocked.clear();

      for (T u : scc) {
        u.getAdjacentVertexes().remove(startVertex);
      }

      blockedPaths.clear();
      final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(currentSCC);
    }

    return result;
  }

  private boolean circuit(List<List<T>> result, T v) {
    boolean cycleFound = false;

    nodeStack.push(v);
    blocked.add(v);

    for (T w : (Set<T>)v.getAdjacentVertexes()) {
      if (w == startVertex) {
        cycleFound = true;

        fullFillCycleResult(result);
      } else if (!blocked.contains(w)) {
        if (circuit(result, w)) {
          cycleFound = true;
        }
      }
    }

    if (cycleFound) {
      unblock(v);
    } else {
      for (T w : (Set<T>)v.getAdjacentVertexes()) {
        Set<T> blocked = blockedPaths.get(w);

        if (blocked == null) {
          blocked = new HashSet<T>();
          blockedPaths.put(w, blocked);
        }

        blocked.add(v);
      }
    }

    nodeStack.pop();
    return cycleFound;
  }

  private void fullFillCycleResult(List<List<T>> result) {
    List<T> cycleResult = new ArrayList<T>();
    for (T w : nodeStack) {
      cycleResult.add(w);
    }

    result.add(cycleResult);
  }

  private void unblock(T u) {
    if (blocked.remove(u)) {
      final Set<T> path = blockedPaths.get(u);
      if (path != null) {
        final Iterator<T> blockedIterator = path.iterator();

        while (blockedIterator.hasNext()) {
          final T w = blockedIterator.next();
          blockedIterator.remove();
          unblock(w);
        }
      }
    }
  }
}
