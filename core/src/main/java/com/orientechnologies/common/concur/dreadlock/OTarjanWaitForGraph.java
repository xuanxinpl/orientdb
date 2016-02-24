package com.orientechnologies.common.concur.dreadlock;

import java.util.*;

/**
 * Tarjan algorithm which is used to find strong connected components in wait-for graph.
 */
public class OTarjanWaitForGraph {
  private final Deque<OThreadWaitForVertex> stack = new ArrayDeque<OThreadWaitForVertex>();

  private final Set<OThreadWaitForVertex> waitForVertices;
  /**
   * Index of stack frame in depth first search
   */
  private       int                       index;

  public OTarjanWaitForGraph(Set<OThreadWaitForVertex> waitForVertices) {
    this.waitForVertices = waitForVertices;
  }

  /**
   * Finds all strongly connected components
   * (component each vertex of which has path to other vertexes which implies presence of cycle) in wait-for graph.
   *
   * @return List of all found strongly connected components.
   */
  public List<List<OThreadWaitForVertex>> findSCC() {
    final List<List<OThreadWaitForVertex>> result = new ArrayList<List<OThreadWaitForVertex>>();

    for (OThreadWaitForVertex waitForVertex : waitForVertices) {
      if (waitForVertex.tarjanIndex < 0) {
        strongConnect(result, waitForVertex);
      }

    }

    for (OThreadWaitForVertex waitForVertex : waitForVertices) {
      waitForVertex.tarjanIndex = -1;
      waitForVertex.tarjanLowLink = -1;
      waitForVertex.tarjanOnStack = false;
    }
    return result;
  }

  /**
   * Recursion is used in Tarjan algorithm to find all SCC(strongly connected components).
   * <p>
   * We process only vertex which is related to threads because:
   * <ol>
   * <li>Thread has only single wait-for edge to lock vertex</li>
   * <li>Vertex can not be at the same time locked by thread and is being waited by thread so it can not create cycles which consist on single thread and single lock</li>
   * </ol>
   *
   * @param sccs          List of all found SCCs
   * @param waitForVertex Currently processed wait-for vertex.
   */
  private void strongConnect(List<List<OThreadWaitForVertex>> sccs, OThreadWaitForVertex waitForVertex) {
    waitForVertex.tarjanIndex = index;
    waitForVertex.tarjanLowLink = index;

    index++;
    stack.push(waitForVertex);

    waitForVertex.tarjanOnStack = true;

    final OLockWaitForVertex lockWaitForVertex = waitForVertex.getWaitingFor();

    if (lockWaitForVertex != null) {
      for (OThreadWaitForVertex w : lockWaitForVertex.getAcquiredBy()) {
        //Successor w has not yet been visited; recurse on it
        if (w.tarjanIndex < 0) {
          strongConnect(sccs, w);
          waitForVertex.tarjanLowLink = Math.min(w.tarjanLowLink, waitForVertex.tarjanLowLink);
        } else if (w.tarjanOnStack) {
          // Successor w is in stack and hence in the current SCC
          waitForVertex.tarjanLowLink = Math.min(w.tarjanIndex, waitForVertex.tarjanLowLink);
        }
      }
    }

    if (waitForVertex.tarjanIndex == waitForVertex.tarjanLowLink) {
      final List<OThreadWaitForVertex> scc = new ArrayList<OThreadWaitForVertex>();
      OThreadWaitForVertex w;
      do {
        w = stack.pop();
        w.tarjanOnStack = false;
        scc.add(w);
      } while (w != waitForVertex);

      if (scc.size() > 1) {
        sccs.add(scc);
      }

    }
  }
}
