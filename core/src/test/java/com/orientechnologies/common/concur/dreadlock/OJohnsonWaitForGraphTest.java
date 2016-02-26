package com.orientechnologies.common.concur.dreadlock;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Test
public class OJohnsonWaitForGraphTest extends OAbstractWaitForGraphTest {
  public void testFindSingleCycle() {
    final Set<OThreadWaitForVertex> graph = generateWaitForGraphWithOneCycle();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(graph);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final OJohnsonWaitForGraph<OJohnsonWaitForVertex> johnsonWaitForGraph = new OJohnsonWaitForGraph<OJohnsonWaitForVertex>(
        sccs.get(0));

    final List<List<OJohnsonWaitForVertex>> cycles = johnsonWaitForGraph.findCycles();

    Assert.assertEquals(cycles.size(), 1);

    final OLockWaitForVertex L1 = new OLockWaitForVertex(101);
    final OThreadWaitForVertex T2 = new OThreadWaitForVertex(2);
    final OLockWaitForVertex L2 = new OLockWaitForVertex(102);
    final OThreadWaitForVertex T9 = new OThreadWaitForVertex(9);

    final OJohnsonWaitForVertex L1W = new OJohnsonWaitForVertex(L1);
    final OJohnsonWaitForVertex T2W = new OJohnsonWaitForVertex(T2);
    final OJohnsonWaitForVertex L2W = new OJohnsonWaitForVertex(L2);
    final OJohnsonWaitForVertex T9W = new OJohnsonWaitForVertex(T9);

    final List<OJohnsonWaitForVertex> expectedCycle = Arrays.asList(L1W, T2W, L2W, T9W);

    Assert.assertTrue(adjacentListsAreEqual(expectedCycle, cycles.get(0)));
  }

  public void testFindTwoCycles() {
    final Set<OThreadWaitForVertex> graph = generateWaitForGraphWithTwoCyclesOneComponent();

    final OTarjanWaitForGraph tarjanWaitForGraph = new OTarjanWaitForGraph(graph);
    final List<List<OJohnsonWaitForVertex>> sccs = tarjanWaitForGraph.findSCC();

    Assert.assertTrue(sccs.size() == 1);

    final OJohnsonWaitForGraph<OJohnsonWaitForVertex> johnsonWaitForGraph = new OJohnsonWaitForGraph<OJohnsonWaitForVertex>(
        sccs.get(0));

    final List<List<OJohnsonWaitForVertex>> cycles = johnsonWaitForGraph.findCycles();
    Assert.assertEquals(cycles.size(), 2);

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

    final List<OJohnsonWaitForVertex> cycleOne = Arrays.asList(L1W, T2W, L2W, T9W);
    final List<OJohnsonWaitForVertex> cycleTwo = Arrays.asList(L1W, T4W, L4W, T5W);

    Assert.assertTrue(adjacentListsAreEqual(cycleOne, cycles.get(0)) || adjacentListsAreEqual(cycleTwo, cycles.get(0)));
    Assert.assertTrue(adjacentListsAreEqual(cycleOne, cycles.get(1)) || adjacentListsAreEqual(cycleTwo, cycles.get(2)));
  }
}
