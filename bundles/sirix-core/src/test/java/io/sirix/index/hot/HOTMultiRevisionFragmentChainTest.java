/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.cas.CASIndexListenerFactory;
import io.sirix.index.name.NameIndexListenerFactory;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-revision fragment-chain regression test for the HOT leaf-page CoW path.
 *
 * <p>Verifies that under the default SLIDING_SNAPSHOT versioning, an index entry written in
 * revision N remains visible at every later revision provided no explicit deletion has been
 * applied. This is the surface area that the original {@code HOTLeafPageCowTest} suite missed:
 * round-tripping a single fragment through the wire format does not exercise the writer-side
 * fragment chain that the reader walks at next-revision read time.</p>
 *
 * <p>Five revisions on a NAME index, each adding one new key. After commit N, the latest read
 * must see all keys ever inserted up to N — the HOT writer must record the prior on-disk fragment
 * key on the leaf reference's {@code pageFragments} so the reader's
 * {@code combineHOTLeafPages} chain reconstruction sees the full set.</p>
 */
@DisplayName("HOT Multi-Revision Fragment Chain Regression")
final class HOTMultiRevisionFragmentChainTest {

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOT() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("name index across 5 revisions: every key persists through chain reconstruction")
  void nameIndexMultiRevisionPersistsAllKeys() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this regression");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"alpha\",\"keyA\":1}]}"));
      trx.commit();
    }

    appendItemAndCommit(database, "{\"name\":\"beta\",\"keyB\":2}");
    appendItemAndCommit(database, "{\"name\":\"gamma\",\"keyC\":3}");
    appendItemAndCommit(database, "{\"name\":\"delta\",\"keyD\":4}");
    appendItemAndCommit(database, "{\"name\":\"epsilon\",\"keyE\":5}");

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNameKeyCount(rtx, ic, nameIndexDef, "name", 5);
      assertNameKeyCount(rtx, ic, nameIndexDef, "keyA", 1);
      assertNameKeyCount(rtx, ic, nameIndexDef, "keyB", 1);
      assertNameKeyCount(rtx, ic, nameIndexDef, "keyC", 1);
      assertNameKeyCount(rtx, ic, nameIndexDef, "keyD", 1);
      assertNameKeyCount(rtx, ic, nameIndexDef, "keyE", 1);
    }
  }

  @Test
  @DisplayName("CAS index across 5 revisions: cumulative value distribution is correct at HEAD")
  void casIndexMultiRevisionCumulative() {
    assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this regression");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef casIndexDef;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var path = parse("/orders/[]/status", PathParser.Type.JSON);
      casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(path), 0,
          IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"orders\":[{\"id\":1,\"status\":\"new\"}]}"));
      trx.commit();
    }

    appendOrderAndCommit(database, 2, "pending");
    appendOrderAndCommit(database, 3, "pending");
    appendOrderAndCommit(database, 4, "shipped");
    appendOrderAndCommit(database, 5, "delivered");

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
      assertStatusCardinality(rtx, ic, casIndexDef, "new", 1);
      assertStatusCardinality(rtx, ic, casIndexDef, "pending", 2);
      assertStatusCardinality(rtx, ic, casIndexDef, "shipped", 1);
      assertStatusCardinality(rtx, ic, casIndexDef, "delivered", 1);
    }
  }

  @Test
  @DisplayName("read at every intermediate revision sees the cumulative-up-to-that-revision view")
  void readAtIntermediateRevisionsIsCumulative() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this regression");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final int[] revisions = new int[4];

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"alpha\"}]}"));
      trx.commit();
      revisions[0] = trx.getResourceSession().getMostRecentRevisionNumber();
    }

    appendItemAndCommit(database, "{\"name\":\"beta\"}");
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      revisions[1] = session.getMostRecentRevisionNumber();
    }
    appendItemAndCommit(database, "{\"name\":\"gamma\"}");
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      revisions[2] = session.getMostRecentRevisionNumber();
    }
    appendItemAndCommit(database, "{\"name\":\"delta\"}");
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      revisions[3] = session.getMostRecentRevisionNumber();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commit = 1; commit <= 4; commit++) {
        final int revision = revisions[commit - 1];
        try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
          assertNameKeyCount(rtx, ic, nameIndexDef, "name", commit,
              "rev " + revision + " 'name' cardinality (commit " + commit + ")");
        }
      }
    }
  }

  private static void appendItemAndCommit(
      final Database<JsonResourceSession> database, final String json) {
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      trx.moveToFirstChild();
      trx.moveToFirstChild();
      trx.moveToLastChild();
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(json));
      trx.commit();
    }
  }

  private static void appendOrderAndCommit(
      final Database<JsonResourceSession> database, final int id, final String status) {
    final String json = String.format("{\"id\":%d,\"status\":\"%s\"}", id, status);
    appendItemAndCommit(database, json);
  }

  private static void assertNameKeyCount(
      final NodeReadOnlyTrx rtx, final IndexController<?, ?> ic, final IndexDef nameIndexDef,
      final String name, final long expected) {
    assertNameKeyCount(rtx, ic, nameIndexDef, name, expected,
        "expected " + expected + " '" + name + "' keys");
  }

  private static void assertNameKeyCount(
      final NodeReadOnlyTrx rtx, final IndexController<?, ?> ic, final IndexDef nameIndexDef,
      final String name, final long expected, final String message) {
    final var iter = ic.openNameIndex(rtx.getStorageEngineReader(), nameIndexDef,
        ic.createNameFilter(Set.of(name)));
    if (expected == 0) {
      if (!iter.hasNext()) {
        return;
      }
      assertEquals(0L, iter.next().getNodeKeys().getLongCardinality(), message);
      return;
    }
    assertTrue(iter.hasNext(), message + " - index entry missing");
    assertEquals(expected, iter.next().getNodeKeys().getLongCardinality(), message);
  }

  private static void assertStatusCardinality(
      final JsonNodeReadOnlyTrx rtx, final IndexController<?, ?> ic, final IndexDef casIndexDef,
      final String status, final long expected) {
    final var iter = ic.openCASIndex(rtx.getStorageEngineReader(), casIndexDef,
        ic.createCASFilter(Set.of("/orders/[]/status"), new Str(status), SearchMode.EQUAL,
            new JsonPCRCollector(rtx)));
    if (expected == 0) {
      if (!iter.hasNext()) {
        return;
      }
      assertEquals(0L, iter.next().getNodeKeys().getLongCardinality(),
          "status '" + status + "' should not be in index");
      return;
    }
    assertTrue(iter.hasNext(), "expected status '" + status + "' to be in CAS index");
    assertEquals(expected, iter.next().getNodeKeys().getLongCardinality(),
        "expected " + expected + " '" + status + "' status entries");
  }
}
