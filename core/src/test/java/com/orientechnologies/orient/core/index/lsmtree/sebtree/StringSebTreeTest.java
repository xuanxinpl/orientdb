/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.index.lsmtree.sebtree;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.lsmtree.OCursor.Beginning;
import com.orientechnologies.orient.core.index.lsmtree.OCursor.Direction;
import com.orientechnologies.orient.core.index.lsmtree.OCursor.End;
import com.orientechnologies.orient.core.index.lsmtree.OKeyValueCursor;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 * @author Sergey Sitnikov
 */
public class StringSebTreeTest {

  private static final boolean DEBUG = false;
  private static final boolean DUMP  = false;

  private static final int RANDOMIZED_TESTS_ITERATIONS = 1000;

  private static final int LARGE = OSebTreeNode.MAX_ENTRY_SIZE / 2 - 50;
  private static final int SMALL = LARGE / 2;

  private static ODatabaseDocumentTx      db;
  private static OSebTree<String, String> tree;

  @BeforeClass
  public static void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    db = new ODatabaseDocumentTx("memory:" + buildDirectory + "/StringSebTreeTest");
    System.out.println(db.getURL());
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    tree = new OSebTree<>((OAbstractPaginatedStorage) db.getStorage(), "tree", ".seb", true);
    tree.create(OStringSerializer.INSTANCE, null, 1, false, OStringSerializer.INSTANCE);
  }

  @AfterClass
  public static void afterClass() {
    tree.delete();
    db.drop();
  }

  @After
  public void afterMethod() {
    tree.reset();
  }

  @Test
  public void testBasicOperations() {
    final OModifiableBoolean found = new OModifiableBoolean();

    assertEquals(0, tree.size());
    assertEquals(tree.firstKey(), null);
    assertEquals(tree.lastKey(), null);

    found.setValue(true);
    assertEquals(null, tree.get("nonexistent key", found));
    assertFalse(found.getValue());

    assertTrue(tree.put("key1", "value1"));
    assertEquals(1, tree.size());
    assertTrue(tree.contains("key1"));
    assertEquals("value1", tree.get("key1"));
    assertEquals(tree.firstKey(), "key1");
    assertEquals(tree.lastKey(), "key1");

    found.setValue(false);
    assertEquals("value1", tree.get("key1", found));
    assertTrue(found.getValue());

    assertFalse(tree.put("key1", "new value1"));
    assertTrue(tree.contains("key1"));
    assertEquals(1, tree.size());
    assertEquals("new value1", tree.get("key1"));

    assertTrue(tree.put("key2", "value2"));
    assertTrue(tree.contains("key2"));
    assertEquals(2, tree.size());
    assertEquals("value2", tree.get("key2"));
    assertEquals("new value1", tree.get("key1"));
    assertEquals(tree.firstKey(), "key1");
    assertEquals(tree.lastKey(), "key2");

    assertFalse(tree.remove("nonexistent key"));

    assertTrue(tree.remove("key1"));
    assertEquals(1, tree.size());
    assertEquals("value2", tree.get("key2"));
    assertEquals(tree.firstKey(), "key2");
    assertEquals(tree.lastKey(), "key2");
  }

  @Test
  public void testDensePutRemove() throws IOException {
    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testDensePutRemove seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final TreeMap<String, String> expected = new TreeMap<>();

    for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
      switch (random.nextInt(2)) {
      case 0: {
        final String key = "key" + random.nextInt(count);
        final String value = "value" + random.nextInt(count);
        assertEquals(expected.put(key, value) == null, tree.put(key, value));
      }
      break;
      case 1: {
        final String key = "key" + random.nextInt(count);
        assertEquals(expected.remove(key) != null, tree.remove(key));
      }
      break;
      }
    }

    verifyTree(expected, tree, random);
  }

  @Test
  public void testLargeDensePutRemove() throws IOException {
    final int SIZE = OSebTreeNode.MAX_ENTRY_SIZE / 4 - 50;
    final String keyPostfix = dup('k', SIZE);
    final String valuePostfix = dup('v', SIZE);

    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testLargeDensePutRemove seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final TreeMap<String, String> expected = new TreeMap<>();

    try {
      for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
        switch (random.nextInt(3)) {

        case 0: {
          final int keyId = random.nextInt(count);
          final int valueId = random.nextInt(count);
          final String key = keyId + keyPostfix;
          final String value = valueId + valuePostfix;
          if (DEBUG)
            System.out.println("put: " + keyId + " -> " + valueId);
          assertEquals(expected.put(key, value) == null, tree.put(key, value));

          if (DEBUG)
            tree.dump();
        }
        break;

        case 1: {
          final int keyId = random.nextInt(count);
          final String key = keyId + keyPostfix;
          if (DEBUG)
            System.out.println("delete: " + keyId);
          assertEquals(expected.remove(key) != null, tree.remove(key));

          if (DEBUG)
            tree.dump();
        }
        break;

        case 2: {
          if (!expected.isEmpty()) {
            final String key = expected.keySet().toArray(new String[0])[random.nextInt(expected.size())];
            final String value = expected.get(key);
            final String newValue = value.length() * 2 < (OSebTreeNode.MAX_ENTRY_SIZE / 2 - SIZE) - 100 ?
                value + value :
                value.substring(0, value.length() / 2);
            assertEquals(expected.put(key, newValue) == null, tree.put(key, newValue));

            if (DEBUG)
              tree.dump();
          }
        }
        break;

        }
      }

      verifyTree(expected, tree, random);
    } finally {
      if (DUMP) {
        tree.dump();

        System.out.println("Size: " + expected.size());
        for (Map.Entry<String, String> e : expected.entrySet()) {
          System.out.println(e.getKey().substring(0, 3) + " -> " + e.getValue().substring(0, 3));
        }
      }
    }
  }

  @Test
  public void testLargeKeyDensePutRemove() throws IOException {
    final int SIZE = OSebTreeNode.MAX_ENTRY_SIZE - 50;
    final String keyPostfix = dup('k', SIZE);
    final String valuePostfix = "v";

    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testLargeKeyDensePutRemove seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final TreeMap<String, String> expected = new TreeMap<>();

    try {
      for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
        switch (random.nextInt(3)) {

        case 0: {
          final int keyId = random.nextInt(count);
          final int valueId = random.nextInt(count);
          final String key = keyId + keyPostfix;
          final String value = valueId + valuePostfix;
          if (DEBUG)
            System.out.println("put: " + keyId + " -> " + valueId);
          assertEquals(expected.put(key, value) == null, tree.put(key, value));

          if (DEBUG)
            tree.dump();
        }
        break;

        case 1: {
          final int keyId = random.nextInt(count);
          final String key = keyId + keyPostfix;
          if (DEBUG)
            System.out.println("delete: " + keyId);
          assertEquals(expected.remove(key) != null, tree.remove(key));

          if (DEBUG)
            tree.dump();
        }
        break;

        case 2: {
          if (!expected.isEmpty()) {
            final String key = expected.keySet().toArray(new String[0])[random.nextInt(expected.size())];
            final int valueId = random.nextInt(count);
            final String value = valueId + valuePostfix;
            if (DEBUG)
              System.out.println("update: " + (key.length() > 3 ? key.substring(0, 3) : key) + " -> " + value);
            assertEquals(expected.put(key, value) == null, tree.put(key, value));

            if (DEBUG)
              tree.dump();
          }
        }
        break;

        }
      }

      verifyTree(expected, tree, random);
    } finally {
      if (DUMP) {
        tree.dump();

        System.out.println("Size: " + expected.size());
        for (Map.Entry<String, String> e : expected.entrySet()) {
          System.out.println(
              (e.getKey().length() > 3 ? e.getKey().substring(0, 3) : e.getKey()) + " -> " + (e.getValue().length() > 3 ?
                  e.getValue().substring(0, 3) :
                  e.getValue()));
        }
      }
    }
  }

  @Test
  public void testLargeVariablePut() throws IOException {
    final int MAX_SIZE = OSebTreeNode.MAX_ENTRY_SIZE / 2 - 50;

    // marker split seeds: 1474360278867L, 1474370347066L, 1474546657660L
    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testLargeVariablePut seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final TreeMap<String, String> expected = new TreeMap<>();

    try {
      for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
        final int keyId = random.nextInt(count);
        final int valueId = random.nextInt(count);
        final String key = keyId + randomString(random, MAX_SIZE);
        final String value = valueId + randomString(random, MAX_SIZE);
        if (DEBUG)
          System.out.println("put: " + keyId + " -> " + valueId);
        assertEquals(expected.put(key, value) == null, tree.put(key, value));

        if (DEBUG)
          tree.dump();
      }

      verifyTree(expected, tree, random);
    } finally {
      if (DUMP) {
        tree.dump();

        System.out.println("Size: " + expected.size());
        for (Map.Entry<String, String> e : expected.entrySet()) {
          System.out.println(
              (e.getKey().length() > 3 ? e.getKey().substring(0, 3) : e.getKey()) + " -> " + (e.getValue().length() > 3 ?
                  e.getValue().substring(0, 3) :
                  e.getValue()));
        }
      }
    }

  }

  @Test
  public void testSmallVariablePut() throws IOException {
    final int MAX_SIZE = 256;

    final long seed = System.currentTimeMillis();
    final Random random = new Random(seed);
    System.out.println("StringSebTreeTest.testSmallVariablePut seed: " + seed);

    final int count = (int) Math.sqrt(RANDOMIZED_TESTS_ITERATIONS);
    final TreeMap<String, String> expected = new TreeMap<>();

    try {
      for (int i = 0; i < RANDOMIZED_TESTS_ITERATIONS; ++i) {
        final int keyId = random.nextInt(count);
        final int valueId = random.nextInt(count);
        final String key = keyId + randomString(random, MAX_SIZE);
        final String value = valueId + randomString(random, MAX_SIZE);
        if (DEBUG)
          System.out.println("put: " + keyId + " -> " + valueId);
        assertEquals(expected.put(key, value) == null, tree.put(key, value));

        if (DEBUG)
          tree.dump();
      }

      verifyTree(expected, tree, random);
    } finally {
      if (DUMP) {
        tree.dump();

        System.out.println("Size: " + expected.size());
        for (Map.Entry<String, String> e : expected.entrySet()) {
          System.out.println(
              (e.getKey().length() > 3 ? e.getKey().substring(0, 3) : e.getKey()) + " -> " + (e.getValue().length() > 3 ?
                  e.getValue().substring(0, 3) :
                  e.getValue()));
        }
      }
    }

  }

  @Test
  public void testLeafRootSplit() {
    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String C = dup('C', LARGE);
    final String a = dup('a', SMALL);
    final String b = dup('b', SMALL);
    final String c = dup('c', SMALL);

    tree.put("a", A);
    tree.put("b", B);
    tree.put("c", C);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("b", B);
    tree.put("a", A);
    tree.put("c", C);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("b", B);
    tree.put("c", C);
    tree.put("a", A);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("c", C);
    tree.put("a", A);
    tree.put("b", B);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("c", C);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("b", B);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();

    tree.put("a", a);
    tree.put("b", b);
    tree.put("c", c);
    tree.put("a", A);
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    tree.reset();
  }

  @Test
  public void testNonLeafSplit() {
    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String X = dup('X', LARGE);
    final String Y = dup('Y', LARGE);
    final String Z = dup('Z', LARGE);
    final String x = dup('x', SMALL);
    final String y = dup('y', SMALL);
    final String z = dup('z', SMALL);

    makeNonLeafRootTree();
    tree.put(A, "A");
    tree.put(X, "X");
    tree.put(Y, "Y");
    tree.put(Z, "Z");
    tree.put(B, "B");
    assertTrue(tree.contains("a"));
    assertTrue(tree.contains("b"));
    assertTrue(tree.contains("c"));
    assertTrue(tree.contains(A));
    assertTrue(tree.contains(X));
    assertTrue(tree.contains(Y));
    assertTrue(tree.contains(Z));
  }

  private static String dup(char char_, int length) {
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; ++i)
      builder.append(char_);
    return builder.toString();
  }

  private String randomString(Random random, int maxLength) {
    final int length = random.nextInt(maxLength) + 3;
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; ++i)
      builder.append((char) ('a' + random.nextInt('z' - 'a')));
    return builder.toString();
  }

  private static void makeNonLeafRootTree() {
    tree.reset();

    final String A = dup('A', LARGE);
    final String B = dup('B', LARGE);
    final String C = dup('C', LARGE);

    tree.put("a", A);
    tree.put("b", B);
    tree.put("c", C);
  }

  private static void verifyTree(TreeMap<String, String> expected, OSebTree<String, String> actual, Random random) {
    for (String key : expected.keySet())
      assertEquals(expected.get(key), actual.get(key));

    assertEquals(expected.size(), actual.size());
    assertEquals(expected.firstKey(), actual.firstKey());
    assertEquals(expected.lastKey(), actual.lastKey());

    final String[] expectedKeys = expected.keySet().toArray(new String[0]);
    final String beginningKey = expectedKeys[random.nextInt(expectedKeys.length)];
    final String endKey = expectedKeys[random.nextInt(expectedKeys.length)];

    verifyWholeCursor(expected, actual);
    verifyOpenEndCursor(expected, actual, beginningKey);
    verifyOpenBeginningCursor(expected, actual, endKey);
    verifyCursor(expected, actual, beginningKey, endKey);
  }

  private static void verifyWholeCursor(TreeMap<String, String> expected, OSebTree<String, String> actual) {
    OKeyValueCursor<String, String> cursor;

    // whole forward
    cursor = actual.range(null, null, Beginning.Open, End.Open, Direction.Forward);
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // whole reverse
    cursor = actual.range(null, null, Beginning.Open, End.Open, Direction.Reverse);
    for (Map.Entry<String, String> entry : expected.descendingMap().entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());
  }

  private static void verifyOpenEndCursor(TreeMap<String, String> expected, OSebTree<String, String> actual, String beginningKey) {
    OKeyValueCursor<String, String> cursor;

    // forward exclusive beginning open end
    cursor = actual.range(beginningKey, null, Beginning.Exclusive, End.Open, Direction.Forward);
    for (Map.Entry<String, String> entry : expected.tailMap(beginningKey, false).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // reverse exclusive beginning open end
    cursor = actual.range(beginningKey, null, Beginning.Exclusive, End.Open, Direction.Reverse);
    for (Map.Entry<String, String> entry : expected.descendingMap().headMap(beginningKey, false).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // forward inclusive beginning open end
    cursor = actual.range(beginningKey, null, Beginning.Inclusive, End.Open, Direction.Forward);
    for (Map.Entry<String, String> entry : expected.tailMap(beginningKey, true).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // reverse inclusive beginning open end
    cursor = actual.range(beginningKey, null, Beginning.Inclusive, End.Open, Direction.Reverse);
    for (Map.Entry<String, String> entry : expected.descendingMap().headMap(beginningKey, true).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());
  }

  private static void verifyOpenBeginningCursor(TreeMap<String, String> expected, OSebTree<String, String> actual, String endKey) {
    OKeyValueCursor<String, String> cursor;

    // forward open beginning exclusive end
    cursor = actual.range(null, endKey, Beginning.Open, End.Exclusive, Direction.Forward);
    for (Map.Entry<String, String> entry : expected.headMap(endKey, false).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // reverse open beginning exclusive end
    cursor = actual.range(null, endKey, Beginning.Open, End.Exclusive, Direction.Reverse);
    for (Map.Entry<String, String> entry : expected.descendingMap().tailMap(endKey, false).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // forward open beginning inclusive end
    cursor = actual.range(null, endKey, Beginning.Open, End.Inclusive, Direction.Forward);
    for (Map.Entry<String, String> entry : expected.headMap(endKey, true).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());

    // reverse open beginning inclusive end
    cursor = actual.range(null, endKey, Beginning.Open, End.Inclusive, Direction.Reverse);
    for (Map.Entry<String, String> entry : expected.descendingMap().tailMap(endKey, true).entrySet()) {
      assertTrue(cursor.next());
      assertEquals(entry.getKey(), cursor.key());
      assertEquals(entry.getValue(), cursor.value());
    }
    assertFalse(cursor.next());
  }

  private static void verifyCursor(TreeMap<String, String> expected, OSebTree<String, String> actual, String beginningKey,
      String endKey) {
    OKeyValueCursor<String, String> cursor;

    // forward exclusive beginning exclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Exclusive, End.Exclusive, Direction.Forward);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.subMap(beginningKey, false, endKey, false).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // reverse exclusive beginning exclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Exclusive, End.Exclusive, Direction.Reverse);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.descendingMap().subMap(endKey, false, beginningKey, false).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // forward inclusive beginning exclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Inclusive, End.Exclusive, Direction.Forward);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.subMap(beginningKey, true, endKey, false).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // reverse inclusive beginning exclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Inclusive, End.Exclusive, Direction.Reverse);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.descendingMap().subMap(endKey, false, beginningKey, true).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // forward exclusive beginning inclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Exclusive, End.Inclusive, Direction.Forward);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.subMap(beginningKey, false, endKey, true).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // reverse exclusive beginning inclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Exclusive, End.Inclusive, Direction.Reverse);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.descendingMap().subMap(endKey, true, beginningKey, false).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // forward inclusive beginning inclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Inclusive, End.Inclusive, Direction.Forward);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.subMap(beginningKey, true, endKey, true).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());

    // reverse inclusive beginning inclusive end
    cursor = actual.range(beginningKey, endKey, Beginning.Inclusive, End.Inclusive, Direction.Reverse);
    if (beginningKey.compareTo(endKey) <= 0)
      for (Map.Entry<String, String> entry : expected.descendingMap().subMap(endKey, true, beginningKey, true).entrySet()) {
        assertTrue(cursor.next());
        assertEquals(entry.getKey(), cursor.key());
        assertEquals(entry.getValue(), cursor.value());
      }
    assertFalse(cursor.next());
  }
}
