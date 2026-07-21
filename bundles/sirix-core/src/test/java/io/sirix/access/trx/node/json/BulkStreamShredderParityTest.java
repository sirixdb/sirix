/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential parity gate for the bulk-stream shredding fast lane
 * (docs/BULK_INGESTION.md §3): every fixture is imported twice — once through the
 * cursor-free fast lane, once through the classic cursor-based loop — and the two
 * resources must be indistinguishable node by node: identical nodeKeys, kinds, names,
 * values, structural links, childCount, descendantCount, hashes, and path node keys,
 * plus byte-identical serialization. A post-import edit applied to both must keep them
 * equal (incremental hash math builds on the imported values).
 */
final class BulkStreamShredderParityTest {

  private static final String[] FIXTURES = {
      // Flat records — the analytical bulk-import shape.
      "[{\"name\":\"a\",\"age\":1,\"active\":true},{\"name\":\"b\",\"age\":2,\"active\":false}]",
      // Nested objects + arrays, mixed primitive kinds, null values.
      "{\"a\":{\"b\":{\"c\":[1,2.5,\"x\",null,true]}},\"d\":[],\"e\":{},\"f\":null}",
      // Arrays of arrays, empty containers in all positions.
      "[[],[[]],[[1],[2,[3,[4]]]],{},{\"x\":[]},{\"y\":{}}]",
      // Duplicate field names across sibling records (dictionary + path summary sharing).
      "[{\"k\":1},{\"k\":2},{\"k\":\"three\"},{\"k\":null},{\"k\":[1]},{\"k\":{\"k\":true}}]",
      // Single-element documents. (Scalar roots are rejected by insertSubtree on BOTH
      // lanes — "JSON to insert must begin with an array or object" — so they are not
      // part of the parity surface.)
      "[42]",
      "{\"only\":\"one\"}",
      // Scalar values in every position inside containers instead.
      "[\"s\",17,2.25,true,null]",
      // Field names with unicode + escapes.
      "{\"\\u00e4\\u00f6\\u00fc\":\"umlauts\",\"with \\\"quotes\\\"\":1}",
  };

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonNodeTrxImpl.setBulkFastLaneEnabledForTests(true);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("Fast lane and classic lane produce node-identical resources (ROLLING hashes + path summary)")
  void parityWithHashesAndPathSummary() {
    assertLaneParity(HashType.ROLLING, true, 0);
  }

  @Test
  @DisplayName("Fast lane parity with hashes disabled and no path summary")
  void parityWithoutHashesAndPathSummary() {
    assertLaneParity(HashType.NONE, false, 0);
  }

  @Test
  @DisplayName("Fast lane parity across auto-commit rotations (tiny threshold, large fixture)")
  void parityAcrossAutoCommitRotations() {
    // A document large enough to actually cross a 32-modification threshold many times
    // (~2.7k inserts → ~80 rotations): the fast lane must interoperate with the
    // intermediate-commit machinery (epoch transitions, autoCommit hash-mode flip,
    // container-cache invalidation) exactly like the classic lane. The small FIXTURES
    // documents stay below the threshold and would make this test vacuously green.
    final StringBuilder json = new StringBuilder(1 << 15);
    json.append('[');
    for (int i = 0; i < 300; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"user-").append(i)
          .append("\",\"age\":").append(18 + (i * 37) % 63)
          .append(",\"active\":").append((i % 3) == 0)
          .append(",\"tags\":[\"t").append(i % 7).append("\",\"t").append(i % 11)
          .append("\"]}");
    }
    json.append(']');
    assertLaneParityForDocument(json.toString(), HashType.ROLLING, true, 32, "rotations");
  }

  @Test
  @DisplayName("Deep nesting combined with rotations")
  void parityWithDeepNestingAcrossRotations() {
    final StringBuilder deep = new StringBuilder();
    final int depth = 150;
    deep.append("{\"v\":1");
    for (int i = 0; i < depth; i++) {
      deep.append(",\"n\":{\"v\":").append(i);
    }
    deep.append("}".repeat(depth)).append('}');
    assertLaneParityForDocument(deep.toString(), HashType.ROLLING, true, 48, "deep-rotations");
  }

  @Test
  @DisplayName("Deep nesting beyond the initial frame capacity (stack growth)")
  void parityWithDeepNesting() {
    final StringBuilder deep = new StringBuilder();
    final int depth = 200;
    deep.append("{\"v\":1");
    for (int i = 0; i < depth; i++) {
      deep.append(",\"n\":{\"v\":").append(i);
    }
    deep.append("}".repeat(depth)).append('}');
    assertLaneParityForDocument(deep.toString(), HashType.ROLLING, true, 0, "deep");
  }

  @Test
  @DisplayName("Post-import edits keep both lanes identical (incremental hash math)")
  void parityAfterPostImportEdit() {
    final String document = FIXTURES[0];
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      final String fast = importResource(db, "edit-fast", document, HashType.ROLLING, true, 0, true);
      final String classic = importResource(db, "edit-classic", document, HashType.ROLLING, true, 0, false);
      assertEquals(classic, fast, "imports must serialize identically before the edit");

      applyEdit(db, "edit-fast");
      applyEdit(db, "edit-classic");

      try (final JsonResourceSession fastSession = db.beginResourceSession("edit-fast");
           final JsonResourceSession classicSession = db.beginResourceSession("edit-classic")) {
        assertEquals(serialize(classicSession), serialize(fastSession),
            "post-import edit must produce identical documents on both lanes");
        assertNodeParity(classicSession, fastSession, "post-edit");
      }
    }
  }

  /** Move to the first record's "age" field and replace its number — exercises rolling-hash updates. */
  private static void applyEdit(final Database<JsonResourceSession> db, final String resource) {
    try (final JsonResourceSession session = db.beginResourceSession(resource);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();   // array
      wtx.moveToFirstChild();   // first record object
      wtx.moveToFirstChild();   // first field
      wtx.moveToRightSibling(); // "age" field
      wtx.setNumberValue(99);
      wtx.commit();
    }
  }

  private void assertLaneParity(final HashType hashType, final boolean pathSummary,
      final int autoCommitThreshold) {
    int i = 0;
    for (final String fixture : FIXTURES) {
      assertLaneParityForDocument(fixture, hashType, pathSummary, autoCommitThreshold, "f" + i++);
      JsonTestHelper.deleteEverything();
    }
  }

  private void assertLaneParityForDocument(final String document, final HashType hashType,
      final boolean pathSummary, final int autoCommitThreshold, final String tag) {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      final String fast =
          importResource(db, tag + "-fast", document, hashType, pathSummary, autoCommitThreshold, true);
      final String classic =
          importResource(db, tag + "-classic", document, hashType, pathSummary, autoCommitThreshold, false);
      assertEquals(classic, fast, "serialization mismatch for fixture: " + document);

      try (final JsonResourceSession fastSession = db.beginResourceSession(tag + "-fast");
           final JsonResourceSession classicSession = db.beginResourceSession(tag + "-classic")) {
        assertNodeParity(classicSession, fastSession, document);
      }
    }
  }

  private static String importResource(final Database<JsonResourceSession> db, final String resource,
      final String document, final HashType hashType, final boolean pathSummary,
      final int autoCommitThreshold, final boolean fastLane) {
    db.createResource(ResourceConfiguration.newBuilder(resource)
        .storeDiffs(false)
        .hashKind(hashType)
        .buildPathSummary(pathSummary)
        .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
        .build());
    JsonNodeTrxImpl.setBulkFastLaneEnabledForTests(fastLane);
    try (final JsonResourceSession session = db.beginResourceSession(resource)) {
      try (final JsonNodeTrx wtx = autoCommitThreshold > 0
          ? session.beginNodeTrx(autoCommitThreshold)
          : session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(document), JsonNodeTrx.Commit.NO);
        // Guard against vacuous comparisons: the "fast" import must actually have engaged
        // the bulk lane, and the "classic" import must not have.
        assertEquals(fastLane, ((JsonNodeTrxImpl) wtx).bulkStreamInsertUsed(),
            "unexpected shredding lane for resource " + resource);
        wtx.commit();
      }
      return serialize(session);
    } finally {
      JsonNodeTrxImpl.setBulkFastLaneEnabledForTests(true);
    }
  }

  private static String serialize(final JsonResourceSession session) {
    final StringWriter out = new StringWriter();
    new JsonSerializer.Builder(session, out).build().call();
    return out.toString();
  }

  /**
   * Full document-order walk asserting per-node equality of every identity-bearing and
   * structural field between the two resources.
   */
  private static void assertNodeParity(final JsonResourceSession expectedSession,
      final JsonResourceSession actualSession, final String context) {
    try (final JsonNodeReadOnlyTrx expected = expectedSession.beginNodeReadOnlyTrx();
         final JsonNodeReadOnlyTrx actual = actualSession.beginNodeReadOnlyTrx()) {
      expected.moveToDocumentRoot();
      actual.moveToDocumentRoot();
      long compared = 0;
      boolean expectedHasMore;
      boolean actualHasMore;
      do {
        assertNodeEquals(expected, actual, context);
        compared++;
        expectedHasMore = advanceDocumentOrder(expected);
        actualHasMore = advanceDocumentOrder(actual);
        assertEquals(expectedHasMore, actualHasMore,
            "walk length mismatch after " + compared + " nodes: " + context);
      } while (expectedHasMore);
      assertTrue(compared > 1, "walk visited only the document root: " + context);
    }
  }

  /** Document-order (pre-order) traversal step; returns {@code false} at the end. */
  private static boolean advanceDocumentOrder(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      return true;
    }
    while (!rtx.hasRightSibling()) {
      if (!rtx.hasParent() || rtx.getKind() == NodeKind.JSON_DOCUMENT) {
        return false;
      }
      rtx.moveToParent();
    }
    rtx.moveToRightSibling();
    return true;
  }

  private static void assertNodeEquals(final JsonNodeReadOnlyTrx expected,
      final JsonNodeReadOnlyTrx actual, final String context) {
    final String at = context + " @ nodeKey " + expected.getNodeKey();
    assertEquals(expected.getNodeKey(), actual.getNodeKey(), "nodeKey " + at);
    assertEquals(expected.getKind(), actual.getKind(), "kind " + at);
    assertEquals(expected.getParentKey(), actual.getParentKey(), "parentKey " + at);
    assertEquals(expected.getFirstChildKey(), actual.getFirstChildKey(), "firstChildKey " + at);
    assertEquals(expected.getLastChildKey(), actual.getLastChildKey(), "lastChildKey " + at);
    assertEquals(expected.getLeftSiblingKey(), actual.getLeftSiblingKey(), "leftSiblingKey " + at);
    assertEquals(expected.getRightSiblingKey(), actual.getRightSiblingKey(), "rightSiblingKey " + at);
    assertEquals(expected.getChildCount(), actual.getChildCount(), "childCount " + at);
    assertEquals(expected.getDescendantCount(), actual.getDescendantCount(), "descendantCount " + at);
    assertEquals(expected.getHash(), actual.getHash(), "hash " + at);
    assertEquals(expected.getPathNodeKey(), actual.getPathNodeKey(), "pathNodeKey " + at);
    if (expected.getKind() == NodeKind.OBJECT_NAMED_STRING || expected.getKind() == NodeKind.STRING_VALUE
        || expected.getKind() == NodeKind.OBJECT_NAMED_NUMBER || expected.getKind() == NodeKind.NUMBER_VALUE
        || expected.getKind() == NodeKind.OBJECT_NAMED_BOOLEAN || expected.getKind() == NodeKind.BOOLEAN_VALUE) {
      assertEquals(expected.getValue(), actual.getValue(), "value " + at);
    }
    if (expected.getKind().playsObjectKeyRole()) {
      assertEquals(expected.getName(), actual.getName(), "name " + at);
    }
  }
}
