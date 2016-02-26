package com.orientechnologies.common.concur.dreadlock;

import java.util.*;

/**
 * Tarjan algorithm which is used to find strong connected components in wait-for graph.
 * Input of this component is set of thread wait-for vertices, and output is list of list of thread and lock wait-for vertices which
 * forms strongly connected components.
 */
public class OTarjanWaitForGraph {
  private final Deque<OAbstractWaitForVertex> stack = new ArrayDeque<OAbstractWaitForVertex>();

  private final Collection<? extends OAbstractWaitForVertex> waitForVertices;
  /**
   * Index of stack frame in depth first search
   */
  private       int                       index;

  public OTarjanWaitForGraph(Collection<? extends OAbstractWaitForVertex> waitForVertices) {
    this.waitForVertices = waitForVertices;
  }

  /**
   * Finds all strongly connected components
   * (component each vertex of which has path to other vertexes which implies presence of cycle) in wait-for graph.
   *
   * @return List of all found strongly connected components.
   */
  public List<List<OJohnsonWaitForVertex>> findSCC() {
    final List<List<OJohnsonWaitForVertex>> result = new ArrayList<List<OJohnsonWaitForVertex>>();

    for (OAbstractWaitForVertex waitForVertex : waitForVertices) {
      if (waitForVertex.tarjanIndex < 0) {
        strongConnect(result, waitForVertex);
      }
    }

    for (OAbstractWaitForVertex waitForVertex : waitForVertices) {
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
  private void strongConnect(List<List<OJohnsonWaitForVertex>> sccs, OAbstractWaitForVertex waitForVertex) {
    waitForVertex.tarjanIndex = index;
    waitForVertex.tarjanLowLink = index;

    index++;
    stack.push(waitForVertex);

    waitForVertex.tarjanOnStack = true;

    for (OAbstractWaitForVertex w : waitForVertex.getAdjacentVertexes()) {
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
      Set<OAbstractWaitForVertex> scc = null;
      OAbstractWaitForVertex w;

      do {
        w = stack.pop();
        w.tarjanOnStack = false;

        if (w != waitForVertex) {
          if (scc == null) {
            scc = new HashSet<OAbstractWaitForVertex>();
          }

          scc.add(w);
        } else if (scc != null) {
          scc.add(w);
        }
      } while (w != waitForVertex);

      if (scc != null) {
        //create subgraph of strong connected component
        final Map<OAbstractWaitForVertex, OJohnsonWaitForVertex> mapper = new HashMap<OAbstractWaitForVertex, OJohnsonWaitForVertex>();
        final List<OJohnsonWaitForVertex> subGraph = new ArrayList<OJohnsonWaitForVertex>(scc.size());

        for (OAbstractWaitForVertex v : scc) {
          OJohnsonWaitForVertex wrapper = wrapVertex(mapper, v);
          subGraph.add(wrapper);

          for (OAbstractWaitForVertex u : v.getAdjacentVertexes()) {
            if (scc.contains(u)) {
              OJohnsonWaitForVertex uw = wrapVertex(mapper, u);
              wrapper.addAdjacentVertex(uw);
            }
          }
        }

        sccs.add(subGraph);
      }
    }
  }

  private OJohnsonWaitForVertex wrapVertex(Map<OAbstractWaitForVertex, OJohnsonWaitForVertex> mapper, OAbstractWaitForVertex v) {
    OJohnsonWaitForVertex wrapper = mapper.get(v);

    if (wrapper == null) {
      if(!(v instanceof OJohnsonWaitForVertex)) {
        wrapper = new OJohnsonWaitForVertex(v);
      } else {
        OJohnsonWaitForVertex jv = (OJohnsonWaitForVertex)v;
        wrapper = new OJohnsonWaitForVertex(jv.getWrappedVertex());
      }

      mapper.put(v, wrapper);
    }

    return wrapper;
  }
}
