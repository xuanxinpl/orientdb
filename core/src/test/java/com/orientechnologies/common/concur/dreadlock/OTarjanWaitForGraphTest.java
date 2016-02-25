package com.orientechnologies.common.concur.dreadlock;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

@Test
public class OTarjanWaitForGraphTest {
  public void testNoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithoutCycles();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.isEmpty());
  }

  public void testSingleCycle() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithOneCycle();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Map<OWaitForVertex, List<OWaitForVertex>> scc = new HashMap<OWaitForVertex, List<OWaitForVertex>>();

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    scc.put(L1, Collections.<OWaitForVertex>singletonList(T2));

    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    scc.put(T2, Collections.<OWaitForVertex>singletonList(L2));

    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    scc.put(T9, Collections.<OWaitForVertex>singletonList(L1));

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testTwoCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithTwoCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Map<OWaitForVertex, List<OWaitForVertex>> scc = new HashMap<OWaitForVertex, List<OWaitForVertex>>();

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    scc.put(L1, Arrays.<OWaitForVertex>asList(T2, T4));


    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    scc.put(L2, Collections.<OWaitForVertex>singletonList(T9));
    scc.put(T2, Collections.<OWaitForVertex>singletonList(L2));

    scc.put(T9, Collections.<OWaitForVertex>singletonList(L1));

    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    scc.put(T4, Collections.<OWaitForVertex>singletonList(L4));

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    scc.put(L4, Collections.<OWaitForVertex>singletonList(T5));

    scc.put(T5, Collections.<OWaitForVertex>singletonList(L1));

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testThreeCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Map<OWaitForVertex, List<OWaitForVertex>> scc = new HashMap<OWaitForVertex, List<OWaitForVertex>>();

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    scc.put(L1, Arrays.<OWaitForVertex>asList(T2, T4));

    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    scc.put(L2, Arrays.<OWaitForVertex>asList(T9, T8));
    scc.put(T2, Collections.<OWaitForVertex>singletonList(L2));

    scc.put(T9, Collections.<OWaitForVertex>singletonList(L1));

    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    scc.put(T4, Collections.<OWaitForVertex>singletonList(L4));

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    scc.put(L4, Collections.<OWaitForVertex>singletonList(T5));

    scc.put(T5, Collections.<OWaitForVertex>singletonList(L1));

    scc.put(T8, Collections.<OWaitForVertex>singletonList(L1));

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testThreeCyclesTwoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesTwoComponents();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<Map<OWaitForVertex, List<OWaitForVertex>>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 2);

    final Map<OWaitForVertex, List<OWaitForVertex>> sccOne = new HashMap<OWaitForVertex, List<OWaitForVertex>>();

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    sccOne.put(L1, Arrays.<OWaitForVertex>asList(T2, T4));

    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    sccOne.put(L2, Collections.<OWaitForVertex>singletonList(T9));
    sccOne.put(T2, Collections.<OWaitForVertex>singletonList(L2));

    sccOne.put(T9, Collections.<OWaitForVertex>singletonList(L1));

    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    sccOne.put(T4, Collections.<OWaitForVertex>singletonList(L4));

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    sccOne.put(L4, Collections.<OWaitForVertex>singletonList(T5));

    sccOne.put(T5, Collections.<OWaitForVertex>singletonList(L1));

    final Map<OWaitForVertex, List<OWaitForVertex>> sccTwo = new HashMap<OWaitForVertex, List<OWaitForVertex>>();
    final OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    sccTwo.put(L3, Collections.<OWaitForVertex>singletonList(T7));

    final OLockWaitForVertex L12 = new OLockWaitForVertex(112);
    sccTwo.put(T7, Collections.<OWaitForVertex>singletonList(L12));

    final OThreadWaitForVertex T13 = new OThreadWaitForVertex(13);
    sccTwo.put(T13, Collections.<OWaitForVertex>singletonList(L3));

    Assert.assertTrue(graphsEqual(sccOne, sccs.get(0)) || graphsEqual(sccTwo, sccs.get(0)));
    Assert.assertTrue(graphsEqual(sccOne, sccs.get(1)) || graphsEqual(sccTwo, sccs.get(1)));
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithoutCycles() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    T1.setWaitingFor(L1);

    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    vertices.add(T2);
    final OThreadWaitForVertex T3 = new OThreadWaitForVertex(3);
    vertices.add(T3);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    vertices.add(T4);

    L1.addAcquiredBy(T2);
    L1.addAcquiredBy(T3);
    L1.addAcquiredBy(T4);

    OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    OLockWaitForVertex L4 = new OLockWaitForVertex(104);

    T2.setWaitingFor(L2);
    T3.setWaitingFor(L3);
    T4.setWaitingFor(L4);

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    vertices.add(T5);
    final OThreadWaitForVertex T6 = new OThreadWaitForVertex(6);
    vertices.add(T6);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    vertices.add(T7);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    vertices.add(T8);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    vertices.add(T9);

    L4.addAcquiredBy(T5);
    L4.addAcquiredBy(T6);

    L3.addAcquiredBy(T7);

    L2.addAcquiredBy(T8);
    L2.addAcquiredBy(T9);

    final OThreadWaitForVertex T10 = new OThreadWaitForVertex(10);
    vertices.add(T10);

    final OLockWaitForVertex L10 = new OLockWaitForVertex(110);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);
    return vertices;
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithOneCycle() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    T1.setWaitingFor(L1);

    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    vertices.add(T2);
    final OThreadWaitForVertex T3 = new OThreadWaitForVertex(3);
    vertices.add(T3);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    vertices.add(T4);

    L1.addAcquiredBy(T2);
    L1.addAcquiredBy(T3);
    L1.addAcquiredBy(T4);

    OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    OLockWaitForVertex L4 = new OLockWaitForVertex(104);

    T2.setWaitingFor(L2);
    T3.setWaitingFor(L3);
    T4.setWaitingFor(L4);

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    vertices.add(T5);
    final OThreadWaitForVertex T6 = new OThreadWaitForVertex(6);
    vertices.add(T6);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    vertices.add(T7);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    vertices.add(T8);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    vertices.add(T9);

    L4.addAcquiredBy(T5);
    L4.addAcquiredBy(T6);

    L3.addAcquiredBy(T7);

    L2.addAcquiredBy(T8);
    L2.addAcquiredBy(T9);

    final OThreadWaitForVertex T10 = new OThreadWaitForVertex(10);
    vertices.add(T10);

    final OLockWaitForVertex L10 = new OLockWaitForVertex(110);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);

    T9.setWaitingFor(L1);
    return vertices;
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithTwoCyclesOneComponent() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    T1.setWaitingFor(L1);

    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    vertices.add(T2);
    final OThreadWaitForVertex T3 = new OThreadWaitForVertex(3);
    vertices.add(T3);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    vertices.add(T4);

    L1.addAcquiredBy(T2);
    L1.addAcquiredBy(T3);
    L1.addAcquiredBy(T4);

    OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    OLockWaitForVertex L4 = new OLockWaitForVertex(104);

    T2.setWaitingFor(L2);
    T3.setWaitingFor(L3);
    T4.setWaitingFor(L4);

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    vertices.add(T5);
    final OThreadWaitForVertex T6 = new OThreadWaitForVertex(6);
    vertices.add(T6);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    vertices.add(T7);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    vertices.add(T8);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    vertices.add(T9);

    L4.addAcquiredBy(T5);
    L4.addAcquiredBy(T6);

    L3.addAcquiredBy(T7);

    L2.addAcquiredBy(T8);
    L2.addAcquiredBy(T9);

    final OThreadWaitForVertex T10 = new OThreadWaitForVertex(10);
    vertices.add(T10);

    final OLockWaitForVertex L10 = new OLockWaitForVertex(110);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);

    T9.setWaitingFor(L1);
    T5.setWaitingFor(L1);
    return vertices;
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithThreeCyclesOneComponent() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    T1.setWaitingFor(L1);

    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    vertices.add(T2);
    final OThreadWaitForVertex T3 = new OThreadWaitForVertex(3);
    vertices.add(T3);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    vertices.add(T4);

    L1.addAcquiredBy(T2);
    L1.addAcquiredBy(T3);
    L1.addAcquiredBy(T4);

    OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    OLockWaitForVertex L4 = new OLockWaitForVertex(104);

    T2.setWaitingFor(L2);
    T3.setWaitingFor(L3);
    T4.setWaitingFor(L4);

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    vertices.add(T5);
    final OThreadWaitForVertex T6 = new OThreadWaitForVertex(6);
    vertices.add(T6);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    vertices.add(T7);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    vertices.add(T8);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    vertices.add(T9);

    L4.addAcquiredBy(T5);
    L4.addAcquiredBy(T6);

    L3.addAcquiredBy(T7);

    L2.addAcquiredBy(T8);
    L2.addAcquiredBy(T9);

    final OThreadWaitForVertex T10 = new OThreadWaitForVertex(10);
    vertices.add(T10);

    final OLockWaitForVertex L10 = new OLockWaitForVertex(110);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);

    T9.setWaitingFor(L1);
    T5.setWaitingFor(L1);
    T8.setWaitingFor(L1);
    return vertices;
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithThreeCyclesTwoComponents() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    T1.setWaitingFor(L1);

    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    vertices.add(T2);
    final OThreadWaitForVertex T3 = new OThreadWaitForVertex(3);
    vertices.add(T3);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    vertices.add(T4);

    L1.addAcquiredBy(T2);
    L1.addAcquiredBy(T3);
    L1.addAcquiredBy(T4);

    OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    OLockWaitForVertex L4 = new OLockWaitForVertex(104);

    T2.setWaitingFor(L2);
    T3.setWaitingFor(L3);
    T4.setWaitingFor(L4);

    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);
    vertices.add(T5);
    final OThreadWaitForVertex T6 = new OThreadWaitForVertex(6);
    vertices.add(T6);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    vertices.add(T7);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    vertices.add(T8);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    vertices.add(T9);

    L4.addAcquiredBy(T5);
    L4.addAcquiredBy(T6);

    L3.addAcquiredBy(T7);

    L2.addAcquiredBy(T8);
    L2.addAcquiredBy(T9);

    final OThreadWaitForVertex T10 = new OThreadWaitForVertex(10);
    vertices.add(T10);

    final OLockWaitForVertex L10 = new OLockWaitForVertex(110);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);

    T9.setWaitingFor(L1);
    T5.setWaitingFor(L1);

    final OLockWaitForVertex L12 = new OLockWaitForVertex(112);
    T7.setWaitingFor(L12);

    final OThreadWaitForVertex T13 = new OThreadWaitForVertex(13);
    L12.addAcquiredBy(T13);

    T13.setWaitingFor(L3);

    return vertices;
  }

  private boolean graphsEqual(Map<OWaitForVertex, List<OWaitForVertex>> expected,
      Map<OWaitForVertex, List<OWaitForVertex>> actual) {

    for (Map.Entry<OWaitForVertex, List<OWaitForVertex>> e : expected.entrySet()) {
      final OWaitForVertex v = e.getKey();

      final List<OWaitForVertex> adjacentList = findRelatedAdjacentList(v, actual);
      if (adjacentList == null) {
        return false;
      }

      if (!adjacentListAreEquals(e.getValue(), adjacentList))
        return false;
    }

    return true;
  }

  private List<OWaitForVertex> findRelatedAdjacentList(OWaitForVertex v, Map<OWaitForVertex, List<OWaitForVertex>> graph) {
    for (Map.Entry<OWaitForVertex, List<OWaitForVertex>> entry : graph.entrySet()) {
      if (entry.getKey().index == v.index) {
        return entry.getValue();
      }
    }

    return null;
  }

  private boolean adjacentListAreEquals(List<OWaitForVertex> expected, List<OWaitForVertex> actual) {
    final List<OWaitForVertex> actualCopy = new ArrayList<OWaitForVertex>(actual);

    for (OWaitForVertex ve : expected) {
      for (OWaitForVertex va : actual) {
        if (ve.index == va.index) {
          if(!actualCopy.remove(va))
            return false;
        }
      }
    }

    return actualCopy.isEmpty();
  }

}
