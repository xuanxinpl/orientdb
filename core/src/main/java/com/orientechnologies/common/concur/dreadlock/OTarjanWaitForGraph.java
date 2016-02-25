package com.orientechnologies.common.concur.dreadlock;

import java.util.*;

/**
 * Tarjan algorithm which is used to find strong connected components in wait-for graph.
 * Input of this component is set of thread wait-for vertices, and output is list of list of thread and lock wait-for vertices which
 * forms strongly connected components.
 */
public class OTarjanWaitForGraph {
  private final Deque<OWaitForVertex> stack = new ArrayDeque<OWaitForVertex>();

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
  public List<Map<OWaitForVertex, List<OWaitForVertex>>> findSCC() {
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> result = new ArrayList<Map<OWaitForVertex, List<OWaitForVertex>>>();

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
  private void strongConnect(List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs, OWaitForVertex waitForVertex) {
    waitForVertex.tarjanIndex = index;
    waitForVertex.tarjanLowLink = index;

    index++;
    stack.push(waitForVertex);

    waitForVertex.tarjanOnStack = true;

    for (OWaitForVertex w : waitForVertex.getAdjacentVertexes()) {
      //Successor w has not yet been visited; recurse on it
      if (w.tarjanIndex < 0) {
        strongConnect(sccs, w);
        waitForVertex.tarjanLowLink = Math.min(w.tarjanLowLink, waitForVertex.tarjanLowLink);
      } else if (w.tarjanOnStack) {
        // Successor w is in stack and hence in the current SCC
        waitForVertex.tarjanLowLink = Math.min(w.tarjanIndex, waitForVertex.tarjanLowLink);
      }
    }

    if (waitForVertex.tarjanIndex == waitForVertex.tarjanLowLink) {
      Set<OWaitForVertex> scc = null;
      OWaitForVertex w;

      do {
        w = stack.pop();
        w.tarjanOnStack = false;

        if (w != waitForVertex) {
          if (scc == null) {
            scc = new HashSet<OWaitForVertex>();
          }

          scc.add(w);
        } else if (scc != null) {
          scc.add(w);
        }
      } while (w != waitForVertex);

      if (scc != null) {
        //create subgraph of strong connected component
        final Map<OWaitForVertex, List<OWaitForVertex>> subGraph = new HashMap<OWaitForVertex, List<OWaitForVertex>>();
        for (OWaitForVertex v : scc) {
          final List<OWaitForVertex> adjacentList = new ArrayList<OWaitForVertex>();
          subGraph.put(v, adjacentList);

          for (OWaitForVertex u : v.getAdjacentVertexes()) {
            if (scc.contains(u)) {
              adjacentList.add(u);
            }
          }
        }

        sccs.add(subGraph);
      }
    }
  }
}
