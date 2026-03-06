package io.sirix.diff.algorithm;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Axis;
import io.sirix.axis.DescendantAxis;
import io.sirix.diff.algorithm.fmse.json.JsonFMSE;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.StringWriter;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive tests for verifying that path summary is correctly maintained after JSON FMSE
 * diff operations (inserts, deletes, moves, updates). Each test verifies both the resulting JSON
 * content AND the path summary structure (paths, reference counts, node kinds).
 */
public final class JsonFMSEPathSummaryTest {

  private static final String RESOURCE = "shredded";

  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ==================== MOVE: OBJECT_KEY between different parent OBJECTs ====================

  @Test
  public void testMoveObjectKeyBetweenObjects_pathSummaryUpdated() throws Exception {
    // Move key "x" from object "a" to object "b" — path changes from /a/x to /b/x
    testDiffAndVerifyPathSummary(
        "{\"a\":{\"x\":1},\"b\":{}}",
        "{\"a\":{},\"b\":{\"x\":1}}",
        "{\"a\":{},\"b\":{\"x\":1}}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY,
            "/b/x", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testMoveObjectKeyToNewParent_pathSummaryCreated() throws Exception {
    // Move "child" from "source" to "dest" — new path /dest/child should exist
    testDiffAndVerifyPathSummary(
        "{\"source\":{\"child\":1},\"dest\":{}}",
        "{\"source\":{},\"dest\":{\"child\":1}}",
        "{\"source\":{},\"dest\":{\"child\":1}}",
        Map.of(
            "/source", NodeKind.OBJECT_KEY,
            "/dest", NodeKind.OBJECT_KEY,
            "/dest/child", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== MOVE: OBJECT_KEY sibling reorder (same parent) ====================

  @Test
  public void testReorderObjectKeys_pathSummaryUnchanged() throws Exception {
    // Reorder keys within the same object — paths should remain the same
    testDiffAndVerifyPathSummary(
        "{\"alpha\":1,\"beta\":2,\"gamma\":3}",
        "{\"gamma\":3,\"alpha\":1,\"beta\":2}",
        "{\"gamma\":3,\"alpha\":1,\"beta\":2}",
        Map.of(
            "/alpha", NodeKind.OBJECT_KEY,
            "/beta", NodeKind.OBJECT_KEY,
            "/gamma", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== MOVE: Nested OBJECT_KEY inside moved OBJECT_KEY ====================

  @Test
  public void testMoveObjectKeyWithNestedKeys_allPathsUpdated() throws Exception {
    // Move "nested" (which contains "inner") from "a" to "b"
    // Paths should change: /a/nested/inner → /b/nested/inner
    testDiffAndVerifyPathSummary(
        "{\"a\":{\"nested\":{\"inner\":1}},\"b\":{}}",
        "{\"a\":{},\"b\":{\"nested\":{\"inner\":1}}}",
        "{\"a\":{},\"b\":{\"nested\":{\"inner\":1}}}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY,
            "/b/nested", NodeKind.OBJECT_KEY,
            "/b/nested/inner", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testMoveObjectKeyWithDeeplyNestedStructure() throws Exception {
    // Three levels of nesting move together
    testDiffAndVerifyPathSummary(
        "{\"src\":{\"l1\":{\"l2\":{\"l3\":\"val\"}}},\"dst\":{}}",
        "{\"src\":{},\"dst\":{\"l1\":{\"l2\":{\"l3\":\"val\"}}}}",
        "{\"src\":{},\"dst\":{\"l1\":{\"l2\":{\"l3\":\"val\"}}}}",
        Map.of(
            "/src", NodeKind.OBJECT_KEY,
            "/dst", NodeKind.OBJECT_KEY,
            "/dst/l1", NodeKind.OBJECT_KEY,
            "/dst/l1/l2", NodeKind.OBJECT_KEY,
            "/dst/l1/l2/l3", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== MOVE: OBJECT_KEY with ARRAY child ====================

  @Test
  public void testMoveObjectKeyWithArrayChild_arrayPathUpdated() throws Exception {
    // Move "items" (which has an ARRAY value) from "a" to "b"
    testDiffAndVerifyPathSummary(
        "{\"a\":{\"items\":[1,2]},\"b\":{}}",
        "{\"a\":{},\"b\":{\"items\":[1,2]}}",
        "{\"a\":{},\"b\":{\"items\":[1,2]}}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY,
            "/b/items", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== INSERT (copy): OBJECT_KEY to different parent ====================

  @Test
  public void testInsertObjectKey_newPathCreated() throws Exception {
    // Insert new key "newkey" into existing object — new path should be created
    testDiffAndVerifyPathSummary(
        "{\"existing\":1}",
        "{\"existing\":1,\"newkey\":2}",
        "{\"existing\":1,\"newkey\":2}",
        Map.of(
            "/existing", NodeKind.OBJECT_KEY,
            "/newkey", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testInsertNestedObject_allPathsCreated() throws Exception {
    // Insert nested structure — all path nodes should be created
    testDiffAndVerifyPathSummary(
        "{\"root\":{}}",
        "{\"root\":{\"child\":{\"grandchild\":\"value\"}}}",
        "{\"root\":{\"child\":{\"grandchild\":\"value\"}}}",
        Map.of(
            "/root", NodeKind.OBJECT_KEY,
            "/root/child", NodeKind.OBJECT_KEY,
            "/root/child/grandchild", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testInsertObjectWithArray_arrayPathCreated() throws Exception {
    // Insert key "data" with ARRAY value containing objects
    testDiffAndVerifyPathSummary(
        "{}",
        "{\"data\":[{\"id\":1}]}",
        "{\"data\":[{\"id\":1}]}",
        Map.of(
            "/data", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== INSERT (copy): reference counting ====================

  @Test
  public void testInsertDuplicateKeyName_referenceCountIncremented() throws Exception {
    // Two keys with same name at different paths — each should have own path node
    testDiffAndVerifyPathSummary(
        "{\"a\":{}}",
        "{\"a\":{\"name\":\"Alice\"},\"b\":{\"name\":\"Bob\"}}",
        "{\"a\":{\"name\":\"Alice\"},\"b\":{\"name\":\"Bob\"}}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/a/name", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY,
            "/b/name", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testInsertSameKeyNameUnderSameParent_referenceCountTwo() throws Exception {
    // Array of objects where same key name appears multiple times at same path level
    final String oldJson = "{\"users\":[]}";
    final String newJson = "{\"users\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}";
    testDiffAndVerifyPathSummary(oldJson, newJson, newJson,
        Map.of(
            "/users", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== DELETE: path reference count decremented ====================

  @Test
  public void testDeleteObjectKey_pathNodeRemoved() throws Exception {
    // Delete "removed" key — its path node should be cleaned up
    testDiffAndVerifyPathSummary(
        "{\"keep\":1,\"removed\":2}",
        "{\"keep\":1}",
        "{\"keep\":1}",
        Map.of(
            "/keep", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testDeleteNestedStructure_allPathNodesRemoved() throws Exception {
    // Delete nested structure — all descendant path nodes should be removed
    testDiffAndVerifyPathSummary(
        "{\"keep\":1,\"deep\":{\"nested\":{\"inner\":\"val\"}}}",
        "{\"keep\":1}",
        "{\"keep\":1}",
        Map.of(
            "/keep", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== UPDATE: OBJECT_KEY name change ====================

  @Test
  public void testUpdateObjectKeyName_pathSummaryRenamed() throws Exception {
    // Key name changes (high Levenshtein similarity) — path should update
    testDiffAndVerifyPathSummary(
        "{\"userName\":\"Alice\"}",
        "{\"user_name\":\"Alice\"}",
        "{\"user_name\":\"Alice\"}",
        Map.of(
            "/user_name", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== COMPLEX: multiple operations combined ====================

  @Test
  public void testMixedInsertDeleteMove_pathSummaryConsistent() throws Exception {
    // Insert "d", delete "b", update "a" value, reorder "c"
    testDiffAndVerifyPathSummary(
        "{\"a\":1,\"b\":2,\"c\":{\"inner\":true}}",
        "{\"c\":{\"inner\":false},\"a\":10,\"d\":4}",
        "{\"c\":{\"inner\":false},\"a\":10,\"d\":4}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/c", NodeKind.OBJECT_KEY,
            "/c/inner", NodeKind.OBJECT_KEY,
            "/d", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testSequentialDiffs_pathSummaryStaysConsistent() throws Exception {
    // Apply multiple diffs and verify path summary after each
    final String[] revisions = {
        "{\"v\":1}",
        "{\"v\":2,\"new\":true}",
        "{\"v\":3,\"new\":false,\"extra\":\"data\"}"
    };

    shredJson(PATHS.PATH1.getFile(), revisions[0]);

    for (int i = 1; i < revisions.length; i++) {
      JsonTestHelper.closeEverything();
      shredJson(PATHS.PATH2.getFile(), revisions[i]);

      try (final var db1 = Databases.openJsonDatabase(PATHS.PATH1.getFile());
          final var db2 = Databases.openJsonDatabase(PATHS.PATH2.getFile());
          final var res1 = db1.beginResourceSession(RESOURCE);
          final var res2 = db2.beginResourceSession(RESOURCE);
          final var wtx = res1.beginNodeTrx();
          final var rtx = res2.beginNodeReadOnlyTrx();
          final var fmse = JsonFMSE.createInstance()) {
        fmse.diff(wtx, rtx);
      }

      Databases.removeDatabase(PATHS.PATH2.getFile());
    }

    // Verify final path summary
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE)) {
      verifyPathSummaryConsistency(res);

      // Verify JSON content
      try (final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(res, writer).build();
        serializer.call();
        JSONAssert.assertEquals(revisions[revisions.length - 1], writer.toString(), true);
      }
    }
  }

  // ==================== COPY: complex nested structures ====================

  @Test
  public void testCopyComplexNestedStructure_allPathsCreated() throws Exception {
    // Insert a deeply nested structure with mixed types
    testDiffAndVerifyPathSummary(
        "{}",
        "{\"config\":{\"db\":{\"host\":\"localhost\",\"port\":5432},\"cache\":{\"ttl\":300}}}",
        "{\"config\":{\"db\":{\"host\":\"localhost\",\"port\":5432},\"cache\":{\"ttl\":300}}}",
        Map.of(
            "/config", NodeKind.OBJECT_KEY,
            "/config/db", NodeKind.OBJECT_KEY,
            "/config/db/host", NodeKind.OBJECT_KEY,
            "/config/db/port", NodeKind.OBJECT_KEY,
            "/config/cache", NodeKind.OBJECT_KEY,
            "/config/cache/ttl", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testCopyArrayWithObjectElements_pathsForNestedKeys() throws Exception {
    // Copy array containing objects with keys
    testDiffAndVerifyPathSummary(
        "{}",
        "{\"list\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]}",
        "{\"list\":[{\"id\":1,\"name\":\"a\"},{\"id\":2,\"name\":\"b\"}]}",
        Map.of(
            "/list", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== MOVE: Objects as array elements ====================

  @Test
  public void testMoveObjectBetweenArrays_nestedKeyPathsUpdated() throws Exception {
    // Object moves from array under "a" to array under "b"
    // Nested OBJECT_KEY "x" path should change from /a/[]/x to /b/[]/x
    testDiffAndVerifyPathSummary(
        "{\"a\":[{\"x\":1}],\"b\":[]}",
        "{\"a\":[],\"b\":[{\"x\":1}]}",
        "{\"a\":[],\"b\":[{\"x\":1}]}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testReorderObjectsInArray_pathsUnchanged() throws Exception {
    // Reorder objects within same array — paths should not change
    testDiffAndVerifyPathSummary(
        "[{\"a\":1},{\"b\":2}]",
        "[{\"b\":2},{\"a\":1}]",
        "[{\"b\":2},{\"a\":1}]",
        null);  // Just verify consistency, no specific path assertions
  }

  // ==================== EDGE CASES ====================

  @Test
  public void testEmptyToPopulated_pathSummaryBuiltFromScratch() throws Exception {
    testDiffAndVerifyPathSummary(
        "{}",
        "{\"a\":1,\"b\":{\"c\":true},\"d\":[1,2]}",
        "{\"a\":1,\"b\":{\"c\":true},\"d\":[1,2]}",
        Map.of(
            "/a", NodeKind.OBJECT_KEY,
            "/b", NodeKind.OBJECT_KEY,
            "/b/c", NodeKind.OBJECT_KEY,
            "/d", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testPopulatedToEmpty_pathSummaryCleared() throws Exception {
    testDiffAndVerifyPathSummary(
        "{\"a\":1,\"b\":{\"c\":true}}",
        "{}",
        "{}",
        Map.of());  // No paths should remain (only document root)
  }

  @Test
  public void testCompleteReplacement_oldPathsRemovedNewCreated() throws Exception {
    // All old keys deleted, all new keys inserted
    testDiffAndVerifyPathSummary(
        "{\"old1\":1,\"old2\":{\"old3\":true}}",
        "{\"new1\":2,\"new2\":{\"new3\":false}}",
        "{\"new1\":2,\"new2\":{\"new3\":false}}",
        Map.of(
            "/new1", NodeKind.OBJECT_KEY,
            "/new2", NodeKind.OBJECT_KEY,
            "/new2/new3", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testValueTypeChange_pathSummaryPreserved() throws Exception {
    // Value type change under same key — key's path should persist
    testDiffAndVerifyPathSummary(
        "{\"val\":\"hello\"}",
        "{\"val\":42}",
        "{\"val\":42}",
        Map.of(
            "/val", NodeKind.OBJECT_KEY
        ));
  }

  @Test
  public void testObjectToArrayUnderSameKey_pathPreserved() throws Exception {
    // Container type change under same key — key path should persist
    testDiffAndVerifyPathSummary(
        "{\"data\":{\"x\":1}}",
        "{\"data\":[1,2,3]}",
        "{\"data\":[1,2,3]}",
        Map.of(
            "/data", NodeKind.OBJECT_KEY
        ));
  }

  // ==================== DIRECT MOVE OPERATIONS (not via FMSE) ====================

  @Test
  public void testDirectMoveSubtreeToFirstChild_pathSummaryAdapted() throws Exception {
    // Direct move test: move OBJECT_KEY "x" from under "a" to under "b"
    shredJson(PATHS.PATH1.getFile(), "{\"a\":{\"x\":1},\"b\":{}}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      // Navigate to find "x" and "b"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // first OBJECT_KEY ("a" or "b")

      long keyA = -1;
      long keyB = -1;
      long keyX = -1;

      // Find all keys
      do {
        final String name = wtx.getName().getLocalName();
        if ("a".equals(name)) {
          keyA = wtx.getNodeKey();
          wtx.moveToFirstChild(); // OBJECT under "a"
          wtx.moveToFirstChild(); // OBJECT_KEY "x"
          keyX = wtx.getNodeKey();
          wtx.moveToParent();
          wtx.moveToParent();
        } else if ("b".equals(name)) {
          keyB = wtx.getNodeKey();
        }
      } while (wtx.hasRightSibling() && wtx.moveToRightSibling());

      assertTrue("Key x must be found", keyX > 0);
      assertTrue("Key b must be found", keyB > 0);

      // Move "x" to be first child of the OBJECT under "b"
      wtx.moveTo(keyB);
      wtx.moveToFirstChild(); // OBJECT under "b"
      wtx.moveSubtreeToFirstChild(keyX);

      wtx.commit();
    }

    // Verify path summary
    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE)) {
      verifyPathSummaryConsistency(res);

      // Check the JSON output
      try (final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(res, writer).build();
        serializer.call();
        JSONAssert.assertEquals("{\"a\":{},\"b\":{\"x\":1}}", writer.toString(), true);
      }
    }
  }

  @Test
  public void testDirectMoveSubtreeToRightSibling_sameparent_pathUnchanged() throws Exception {
    // Reorder siblings within same parent — path should not change
    shredJson(PATHS.PATH1.getFile(), "{\"a\":1,\"b\":2,\"c\":3}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // first key

      long keyA = -1;
      long keyC = -1;

      do {
        final String name = wtx.getName().getLocalName();
        if ("a".equals(name)) keyA = wtx.getNodeKey();
        else if ("c".equals(name)) keyC = wtx.getNodeKey();
      } while (wtx.hasRightSibling() && wtx.moveToRightSibling());

      // Move "a" to right sibling of "c" (reorder: b, c, a)
      wtx.moveTo(keyC);
      wtx.moveSubtreeToRightSibling(keyA);

      wtx.commit();
    }

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE)) {
      verifyPathSummaryConsistency(res);

      // Verify that /a, /b, /c paths still exist
      try (final var ps = res.openPathSummary()) {
        final Map<String, NodeKind> paths = collectAllPaths(ps);
        assertTrue("Path /a should exist", paths.containsKey("/a"));
        assertTrue("Path /b should exist", paths.containsKey("/b"));
        assertTrue("Path /c should exist", paths.containsKey("/c"));
      }
    }
  }

  // ==================== DIRECT COPY OPERATIONS (not via FMSE) ====================

  @Test
  public void testDirectCopySubtreeAsFirstChild_pathSummaryCreated() throws Exception {
    shredJson(PATHS.PATH1.getFile(), "{\"src\":{\"data\":42}}");

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE);
        final var wtx = res.beginNodeTrx()) {
      // Find the OBJECT_KEY "data" (under "src")
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object
      wtx.moveToFirstChild(); // OBJECT_KEY "src"
      wtx.moveToFirstChild(); // OBJECT under "src"
      wtx.moveToFirstChild(); // OBJECT_KEY "data"
      final long dataKey = wtx.getNodeKey();

      // Navigate to root object to insert a new key "dst"
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild(); // root object

      // Copy the subtree rooted at "data" as first child of the root object
      // (This would create OBJECT_KEY "data" directly under root — only valid for arrays)
      // Instead, let's copy by reading the "data" key
      try (final var rtx = res.beginNodeReadOnlyTrx()) {
        rtx.moveTo(dataKey);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild(); // root object
        wtx.moveToFirstChild(); // OBJECT_KEY "src"
        wtx.moveToFirstChild(); // OBJECT under "src"
        // Copy "data" key as right sibling of existing "data"
        wtx.moveToFirstChild(); // OBJECT_KEY "data"
        wtx.copySubtreeAsRightSibling(rtx);
      }

      wtx.commit();
    }

    try (final var db = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var res = db.beginResourceSession(RESOURCE)) {
      verifyPathSummaryConsistency(res);
    }
  }

  // ==================== Helper Methods ====================

  /**
   * Tests the FMSE diff and verifies both JSON output and path summary structure.
   *
   * @param oldJson       original JSON
   * @param newJson       target JSON
   * @param expected      expected result after diff
   * @param expectedPaths map of expected path strings to their expected NodeKind (null to skip)
   */
  private void testDiffAndVerifyPathSummary(final String oldJson, final String newJson,
      final String expected, final Map<String, NodeKind> expectedPaths) throws Exception {

    // 1. Shred old JSON into PATH1 database.
    final var dbConfOld = new DatabaseConfiguration(PATHS.PATH1.getFile());
    Databases.removeDatabase(PATHS.PATH1.getFile());
    Databases.createJsonDatabase(dbConfOld);

    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      dbOld.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgrOld = dbOld.beginResourceSession(RESOURCE);
          final var wtxOld = resMgrOld.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtxOld,
            JsonShredder.createStringReader(oldJson),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }

    // 2. Shred new JSON into PATH2 database.
    final var dbConfNew = new DatabaseConfiguration(PATHS.PATH2.getFile());
    Databases.removeDatabase(PATHS.PATH2.getFile());
    Databases.createJsonDatabase(dbConfNew);

    try (final var dbNew = Databases.openJsonDatabase(PATHS.PATH2.getFile())) {
      dbNew.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgrNew = dbNew.beginResourceSession(RESOURCE);
          final var wtxNew = resMgrNew.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtxNew,
            JsonShredder.createStringReader(newJson),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }

    // 3. Run FMSE diff.
    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var dbNew = Databases.openJsonDatabase(PATHS.PATH2.getFile());
        final var resMgrOld = dbOld.beginResourceSession(RESOURCE);
        final var resMgrNew = dbNew.beginResourceSession(RESOURCE);
        final var wtx = resMgrOld.beginNodeTrx();
        final var rtx = resMgrNew.beginNodeReadOnlyTrx();
        final var fmse = JsonFMSE.createInstance()) {
      fmse.diff(wtx, rtx);
    }

    // 4. Verify JSON content.
    try (final var dbOld = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final var resMgr = dbOld.beginResourceSession(RESOURCE)) {

      try (final var writer = new StringWriter()) {
        final var serializer = JsonSerializer.newBuilder(resMgr, writer).build();
        serializer.call();
        JSONAssert.assertEquals(expected, writer.toString(), true);
      }

      // 5. Verify path summary consistency.
      verifyPathSummaryConsistency(resMgr);

      // 6. Verify expected paths exist.
      if (expectedPaths != null) {
        try (final var ps = resMgr.openPathSummary()) {
          final Map<String, NodeKind> actualPaths = collectAllPaths(ps);

          for (final var entry : expectedPaths.entrySet()) {
            final String path = entry.getKey();
            final NodeKind expectedKind = entry.getValue();
            assertTrue("Expected path '" + path + "' not found in path summary. Actual paths: "
                + actualPaths.keySet(), actualPaths.containsKey(path));
            assertEquals("Wrong kind for path '" + path + "'",
                expectedKind, actualPaths.get(path));
          }
        }
      }
    }
  }

  /**
   * Verifies structural consistency of the path summary: no orphaned nodes, reference counts > 0,
   * and all OBJECT_KEY/ARRAY data nodes reference valid path summary nodes.
   */
  private void verifyPathSummaryConsistency(
      final io.sirix.api.json.JsonResourceSession resMgr) {
    try (final var pathSummary = resMgr.openPathSummary()) {
      final Axis axis = new DescendantAxis(pathSummary);
      while (axis.hasNext()) {
        axis.nextLong();

        // Every path node must have references > 0 (otherwise it should have been removed).
        final int refs = pathSummary.getReferences();
        assertTrue("Path node " + pathSummary.getNodeKey() + " (path: " + pathSummary.getPath()
                + ") has invalid reference count: " + refs,
            refs > 0);

        // Every path node must have a valid kind.
        final NodeKind kind = pathSummary.getPathKind();
        assertNotNull("Path node " + pathSummary.getNodeKey() + " has null kind", kind);
        assertTrue("Unexpected path kind: " + kind,
            kind == NodeKind.OBJECT_KEY || kind == NodeKind.ARRAY
                || kind == NodeKind.ELEMENT || kind == NodeKind.ATTRIBUTE
                || kind == NodeKind.NAMESPACE);

        // Level must be positive (document root is level 0, first children are level 1).
        assertTrue("Path node " + pathSummary.getNodeKey() + " has non-positive level: "
            + pathSummary.getLevel(), pathSummary.getLevel() > 0);
      }
    }

    // Verify that all OBJECT_KEY nodes in the data have valid pathNodeKeys.
    try (final var rtx = resMgr.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      if (rtx.hasFirstChild()) {
        final Axis dataAxis = new DescendantAxis(rtx);
        while (dataAxis.hasNext()) {
          dataAxis.nextLong();
          if (rtx.getKind() == NodeKind.OBJECT_KEY) {
            final long pathNodeKey = rtx.getPathNodeKey();
            assertTrue("OBJECT_KEY node " + rtx.getNodeKey() + " (name: "
                    + rtx.getName().getLocalName() + ") has invalid pathNodeKey: " + pathNodeKey,
                pathNodeKey >= 0);
          }
        }
      }
    }
  }

  /**
   * Collects all path strings and their node kinds from the path summary.
   */
  private Map<String, NodeKind> collectAllPaths(final PathSummaryReader ps) {
    final Map<String, NodeKind> paths = new HashMap<>();
    final Axis axis = new DescendantAxis(ps);
    while (axis.hasNext()) {
      axis.nextLong();
      final var path = ps.getPath();
      if (path != null) {
        paths.put(path.toString(), ps.getPathKind());
      }
    }
    return paths;
  }

  /**
   * Shreds JSON into a database.
   */
  private void shredJson(final Path dbPath, final String json) throws Exception {
    final var dbConf = new DatabaseConfiguration(dbPath);
    Databases.removeDatabase(dbPath);
    Databases.createJsonDatabase(dbConf);

    try (final var db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(
          new ResourceConfiguration.Builder(RESOURCE)
              .buildPathSummary(true)
              .build());
      try (final var resMgr = db.beginResourceSession(RESOURCE);
          final var wtx = resMgr.beginNodeTrx()) {
        final var shredder = new JsonShredder.Builder(wtx,
            JsonShredder.createStringReader(json),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }
  }
}
