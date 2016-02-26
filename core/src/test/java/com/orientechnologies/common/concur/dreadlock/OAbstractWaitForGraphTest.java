package com.orientechnologies.common.concur.dreadlock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OAbstractWaitForGraphTest {
  protected Set<OThreadWaitForVertex> generateWaitForGraphWithoutCycles() {
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

  protected Set<OThreadWaitForVertex> generateWaitForGraphWithOneCycle() {
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

  protected Set<OThreadWaitForVertex> generateWaitForGraphWithTwoCyclesOneComponent() {
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

  protected Set<OThreadWaitForVertex> generateWaitForGraphWithThreeCyclesOneComponent() {
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

  protected Set<OThreadWaitForVertex> generateWaitForGraphWithThreeCyclesTwoComponents() {
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

  protected boolean graphsEqual(List<OJohnsonWaitForVertex> expected, List<OJohnsonWaitForVertex> actual) {
    if (expected.size() != actual.size())
      return false;

    for (OJohnsonWaitForVertex ew : expected) {
      OJohnsonWaitForVertex foundAW = null;
      final int ewIndex = ((OAbstractWaitForVertex) ew.getWrappedVertex()).index;

      for (OJohnsonWaitForVertex aw : actual) {
        final int awIndex = ((OAbstractWaitForVertex) aw.getWrappedVertex()).index;

        if (awIndex == ewIndex) {
          foundAW = aw;
          break;
        }
      }

      if (foundAW == null) {
        return false;
      }

      if (!adjacentListsAreEqual(ew.getAdjacentVertexes(), foundAW.getAdjacentVertexes())) {
        return false;
      }
    }

    return true;
  }

  protected boolean adjacentListsAreEqual(List<OJohnsonWaitForVertex> expected, List<OJohnsonWaitForVertex> actual) {
    return adjacentListsAreEqual(new HashSet<OJohnsonWaitForVertex>(expected), new HashSet<OJohnsonWaitForVertex>(actual));
  }

  private boolean adjacentListsAreEqual(Set<OJohnsonWaitForVertex> expected, Set<OJohnsonWaitForVertex> actual) {
    final List<OJohnsonWaitForVertex> actualCopy = new ArrayList<OJohnsonWaitForVertex>(actual);

    for (OJohnsonWaitForVertex ve : expected) {
      for (OJohnsonWaitForVertex va : actual) {
        if (((OAbstractWaitForVertex) ve.getWrappedVertex()).index == ((OAbstractWaitForVertex) va.getWrappedVertex()).index) {
          if (!actualCopy.remove(va))
            return false;
        }
      }
    }

    return actualCopy.isEmpty();
  }

}
