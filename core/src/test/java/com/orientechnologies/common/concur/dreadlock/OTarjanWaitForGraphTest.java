package com.orientechnologies.common.concur.dreadlock;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

@Test
public class OTarjanWaitForGraphTest extends OAbstractWaitForGraphTest {
  public void testNoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithoutCycles();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.isEmpty());
  }

  public void testSingleCycle() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithOneCycle();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);

    final OJohnsonWaitForVertex L1W = new OJohnsonWaitForVertex(L1);
    final OJohnsonWaitForVertex T2W = new OJohnsonWaitForVertex(T2);
    final OJohnsonWaitForVertex L2W = new OJohnsonWaitForVertex(L2);
    final OJohnsonWaitForVertex T9W = new OJohnsonWaitForVertex(T9);

    L1W.addAdjacentVertex(T2W);
    T2W.addAdjacentVertex(L2W);
    L2W.addAdjacentVertex(T9W);
    T9W.addAdjacentVertex(L1W);

    final List<OJohnsonWaitForVertex> scc = Arrays.asList(L1W, T2W, L2W, T9W);

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testTwoCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithTwoCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);

    final OJohnsonWaitForVertex L1W = new OJohnsonWaitForVertex(L1);
    final OJohnsonWaitForVertex T2W = new OJohnsonWaitForVertex(T2);
    final OJohnsonWaitForVertex T4W = new OJohnsonWaitForVertex(T4);
    final OJohnsonWaitForVertex T9W = new OJohnsonWaitForVertex(T9);
    final OJohnsonWaitForVertex L2W = new OJohnsonWaitForVertex(L2);
    final OJohnsonWaitForVertex L4W = new OJohnsonWaitForVertex(L4);
    final OJohnsonWaitForVertex T5W = new OJohnsonWaitForVertex(T5);

    L4W.addAdjacentVertex(T5W);

    T5W.addAdjacentVertex(L1W);

    L1W.addAdjacentVertex(T2W);
    L1W.addAdjacentVertex(T4W);

    L2W.addAdjacentVertex(T9W);

    T2W.addAdjacentVertex(L2W);

    T4W.addAdjacentVertex(L4W);

    T9W.addAdjacentVertex(L1W);

    final List<OJohnsonWaitForVertex> scc = new ArrayList<OJohnsonWaitForVertex>();
    scc.add(L1W);
    scc.add(T2W);
    scc.add(T4W);
    scc.add(T9W);
    scc.add(L2W);
    scc.add(L4W);
    scc.add(T5W);

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testThreeCyclesOneSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OThreadWaitForVertex T8 = new OThreadWaitForVertex(8);
    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);

    final OJohnsonWaitForVertex L1W = new OJohnsonWaitForVertex(L1);
    final OJohnsonWaitForVertex T2W = new OJohnsonWaitForVertex(T2);
    final OJohnsonWaitForVertex T4W = new OJohnsonWaitForVertex(T4);
    final OJohnsonWaitForVertex T9W = new OJohnsonWaitForVertex(T9);
    final OJohnsonWaitForVertex L2W = new OJohnsonWaitForVertex(L2);
    final OJohnsonWaitForVertex T8W = new OJohnsonWaitForVertex(T8);
    final OJohnsonWaitForVertex L4W = new OJohnsonWaitForVertex(L4);
    final OJohnsonWaitForVertex T5W = new OJohnsonWaitForVertex(T5);

    T4W.addAdjacentVertex(L4W);

    L2W.addAdjacentVertex(T9W);
    L2W.addAdjacentVertex(T8W);

    T2W.addAdjacentVertex(L2W);

    T9W.addAdjacentVertex(L1W);

    L1W.addAdjacentVertex(T2W);
    L1W.addAdjacentVertex(T4W);

    L4W.addAdjacentVertex(T5W);

    T5W.addAdjacentVertex(L1W);

    T8W.addAdjacentVertex(L1W);

    final List<OJohnsonWaitForVertex> scc = Arrays.asList(L1W, T2W, T4W, T9W, L2W, T8W, L4W, T5W);

    Assert.assertTrue(graphsEqual(scc, sccs.get(0)));
  }

  public void testThreeCyclesTwoSCC() {
    final Set<OThreadWaitForVertex> vertices = generateWaitForGraphWithThreeCyclesTwoComponents();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(vertices);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 2);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OThreadWaitForVertex T4 = new OThreadWaitForVertex(4);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OLockWaitForVertex L4 = new OLockWaitForVertex(104);
    final OThreadWaitForVertex T5 = new OThreadWaitForVertex(5);

    final OJohnsonWaitForVertex L1WOne = new OJohnsonWaitForVertex(L1);
    final OJohnsonWaitForVertex T2WOne = new OJohnsonWaitForVertex(T2);
    final OJohnsonWaitForVertex T4WOne = new OJohnsonWaitForVertex(T4);
    final OJohnsonWaitForVertex T9WOne = new OJohnsonWaitForVertex(T9);
    final OJohnsonWaitForVertex L2WOne = new OJohnsonWaitForVertex(L2);
    final OJohnsonWaitForVertex L4WOne = new OJohnsonWaitForVertex(L4);
    final OJohnsonWaitForVertex T5WOne = new OJohnsonWaitForVertex(T5);

    L2WOne.addAdjacentVertex(T9WOne);

    T2WOne.addAdjacentVertex(L2WOne);

    T9WOne.addAdjacentVertex(L1WOne);

    T4WOne.addAdjacentVertex(L4WOne);

    L4WOne.addAdjacentVertex(T5WOne);

    T5WOne.addAdjacentVertex(L1WOne);

    L1WOne.addAdjacentVertex(T2WOne);
    L1WOne.addAdjacentVertex(T4WOne);

    final List<OJohnsonWaitForVertex> sccOne = Arrays.asList(L1WOne, T2WOne, T4WOne, T9WOne, L2WOne, L4WOne, T5WOne);

    final OLockWaitForVertex L3 = new OLockWaitForVertex(103);
    final OThreadWaitForVertex T7 = new OThreadWaitForVertex(7);
    final OLockWaitForVertex L12 = new OLockWaitForVertex(112);
    final OThreadWaitForVertex T13 = new OThreadWaitForVertex(13);

    final OJohnsonWaitForVertex L3WTwo = new OJohnsonWaitForVertex(L3);
    final OJohnsonWaitForVertex T7WTwo = new OJohnsonWaitForVertex(T7);
    final OJohnsonWaitForVertex L12WTwo = new OJohnsonWaitForVertex(L12);
    final OJohnsonWaitForVertex T13WTwo = new OJohnsonWaitForVertex(T13);

    T13WTwo.addAdjacentVertex(L3WTwo);
    L3WTwo.addAdjacentVertex(T7WTwo);
    T7WTwo.addAdjacentVertex(L12WTwo);
    L12WTwo.addAdjacentVertex(T13WTwo);

    final List<OJohnsonWaitForVertex> sccTwo = Arrays.asList(L3WTwo, T7WTwo, L12WTwo, T13WTwo);

    Assert.assertTrue(graphsEqual(sccOne, sccs.get(0)) || graphsEqual(sccTwo, sccs.get(0)));
    Assert.assertTrue(graphsEqual(sccOne, sccs.get(1)) || graphsEqual(sccTwo, sccs.get(1)));
  }
}
