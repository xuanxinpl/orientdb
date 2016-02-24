package com.orientechnologies.common.concur.dreadlock;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Test
public class OTarjanWaitForGraphTest {
  public void testNoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithoutCycles();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OThreadWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.isEmpty());
  }

  public void testSingleCycle() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithOneCycle();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OThreadWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Set<Integer> ccIndexes = new HashSet<Integer>();
    ccIndexes.add(2);
    ccIndexes.add(9);

    final List<OThreadWaitForVertex> cycle = sccs.get(0);
    for (OThreadWaitForVertex w : cycle) {
      Assert.assertTrue(ccIndexes.remove(w.index));
    }

    Assert.assertTrue(ccIndexes.isEmpty());
  }

  public void testTwoCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithTwoCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OThreadWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Set<Integer> ccIndexes = new HashSet<Integer>();
    ccIndexes.add(2);
    ccIndexes.add(9);
    ccIndexes.add(4);
    ccIndexes.add(5);

    final List<OThreadWaitForVertex> cycle = sccs.get(0);
    for (OThreadWaitForVertex w : cycle) {
      Assert.assertTrue(ccIndexes.remove(w.index));
    }

    Assert.assertTrue(ccIndexes.isEmpty());
  }

  public void testThreeCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OThreadWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final Set<Integer> ccIndexes = new HashSet<Integer>();
    ccIndexes.add(2);
    ccIndexes.add(9);
    ccIndexes.add(4);
    ccIndexes.add(5);
    ccIndexes.add(8);

    final List<OThreadWaitForVertex> cycle = sccs.get(0);
    for (OThreadWaitForVertex w : cycle) {
      Assert.assertTrue(ccIndexes.remove(w.index));
    }

    Assert.assertTrue(ccIndexes.isEmpty());
  }

  public void testThreeCyclesTwoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesTwoComponents();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OThreadWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 2);

    final Set<Integer> ccIndexesOne = new HashSet<Integer>();
    ccIndexesOne.add(7);
    ccIndexesOne.add(13);

    final Set<Integer> ccIndexesTwo = new HashSet<Integer>();
    ccIndexesTwo.add(2);
    ccIndexesTwo.add(9);
    ccIndexesTwo.add(4);
    ccIndexesTwo.add(5);

    Set<Set<Integer>> ccIndexes = new HashSet<Set<Integer>>();
    ccIndexes.add(ccIndexesOne);
    ccIndexes.add(ccIndexesTwo);

    for (List<OThreadWaitForVertex> cycle : sccs) {
      final Set<Integer> cycleIndexes = new HashSet<Integer>();
      for (OThreadWaitForVertex v : cycle) {
        cycleIndexes.add(v.index);
      }

      Set<Integer> ccIndexToRemove = null;
      for (Set<Integer> ccIndex : ccIndexes) {
        if (ccIndex.equals(cycleIndexes)) {
          ccIndexToRemove = ccIndex;
          break;
        }
      }

      Assert.assertNotNull(ccIndexToRemove);
      Assert.assertTrue(ccIndexes.remove(ccIndexToRemove));
    }
  }

  private Set<OThreadWaitForVertex> generateWaitForGraphWithoutCycles() {
    final Set<OThreadWaitForVertex> vertices = new HashSet<OThreadWaitForVertex>();

    final OThreadWaitForVertex T1 = new OThreadWaitForVertex(1);
    vertices.add(T1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(null);
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

    OLockWaitForVertex L2 = new OLockWaitForVertex(null);
    OLockWaitForVertex L3 = new OLockWaitForVertex(null);
    OLockWaitForVertex L4 = new OLockWaitForVertex(null);

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

    final OLockWaitForVertex L10 = new OLockWaitForVertex(null);
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

    final OLockWaitForVertex L1 = new OLockWaitForVertex(null);
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

    OLockWaitForVertex L2 = new OLockWaitForVertex(null);
    OLockWaitForVertex L3 = new OLockWaitForVertex(null);
    OLockWaitForVertex L4 = new OLockWaitForVertex(null);

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

    final OLockWaitForVertex L10 = new OLockWaitForVertex(null);
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

    final OLockWaitForVertex L1 = new OLockWaitForVertex(null);
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

    OLockWaitForVertex L2 = new OLockWaitForVertex(null);
    OLockWaitForVertex L3 = new OLockWaitForVertex(null);
    OLockWaitForVertex L4 = new OLockWaitForVertex(null);

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

    final OLockWaitForVertex L10 = new OLockWaitForVertex(null);
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

    final OLockWaitForVertex L1 = new OLockWaitForVertex(null);
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

    OLockWaitForVertex L2 = new OLockWaitForVertex(null);
    OLockWaitForVertex L3 = new OLockWaitForVertex(null);
    OLockWaitForVertex L4 = new OLockWaitForVertex(null);

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

    final OLockWaitForVertex L10 = new OLockWaitForVertex(null);
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

    final OLockWaitForVertex L1 = new OLockWaitForVertex(null);
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

    OLockWaitForVertex L2 = new OLockWaitForVertex(null);
    OLockWaitForVertex L3 = new OLockWaitForVertex(null);
    OLockWaitForVertex L4 = new OLockWaitForVertex(null);

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

    final OLockWaitForVertex L10 = new OLockWaitForVertex(null);
    T10.setWaitingFor(L10);

    final OThreadWaitForVertex T11 = new OThreadWaitForVertex(11);
    vertices.add(T11);
    final OThreadWaitForVertex T12 = new OThreadWaitForVertex(12);
    vertices.add(T12);

    L10.addAcquiredBy(T11);
    L10.addAcquiredBy(T12);

    T9.setWaitingFor(L1);
    T5.setWaitingFor(L1);

    final OLockWaitForVertex L12 = new OLockWaitForVertex(null);
    T7.setWaitingFor(L12);

    final OThreadWaitForVertex T13 = new OThreadWaitForVertex(13);
    L12.addAcquiredBy(T13);

    T13.setWaitingFor(L3);

    return vertices;
  }

}
