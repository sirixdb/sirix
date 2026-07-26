/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.name.NameIndexListenerFactory;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression test for the HOT leaf-page CoW path under {@link VersioningType#DIFFERENTIAL}.
 *
 * <p>DIFFERENTIAL keeps a length-1 fragment chain: a read reconstructs the HEAD leaf from exactly
 * two on-disk fragments — the newest delta plus its anchor. The correctness requirement is that the
 * anchor is the last <b>full dump</b> and every delta is <b>cumulative</b> since that dump. If the
 * anchor were instead the immediately-prior <em>sparse</em> delta (the pre-fix behaviour), then a
 * key written in revision N but not touched again would fall out of the two-fragment read window
 * two revisions later and silently vanish, even though no deletion ever occurred.</p>
 *
 * <p>Each revision here touches the shared name-index leaf by adding exactly one new key while
 * leaving all earlier keys untouched (a sparse per-revision delta). With
 * {@code maxNumberOfRevisionsToRestore(3)} the read window spans two fragments, yet seven distinct
 * keys are inserted across seven revisions. Every key must remain visible at HEAD — the five oldest
 * keys are precisely the ones a non-cumulative, prior-delta-anchored chain would lose.</p>
 */
@DisplayName("HOT DIFFERENTIAL Fragment Chain Regression")
final class HOTDifferentialVersioningFragmentChainTest {

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
  @DisplayName("sparse per-revision deltas: keys older than the read window survive at HEAD")
  void differentialSparseDeltasPreserveOldKeys() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this regression");

    // DIFFERENTIAL with a window of 3 revisions -> a HEAD read combines two on-disk fragments.
    final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(),
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                             .versioningApproach(VersioningType.DIFFERENTIAL)
                             .maxNumberOfRevisionsToRestore(3)
                             .build());
    final IndexDef nameIndexDef;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      // Revision 1 seeds the leaf with the first key.
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"k1\":1}]}"));
      trx.commit();
    }

    // Revisions 2..7: each adds exactly one new key and touches nothing else (sparse delta).
    appendItemAndCommit(database, "{\"k2\":2}");
    appendItemAndCommit(database, "{\"k3\":3}");
    appendItemAndCommit(database, "{\"k4\":4}");
    appendItemAndCommit(database, "{\"k5\":5}");
    appendItemAndCommit(database, "{\"k6\":6}");
    appendItemAndCommit(database, "{\"k7\":7}");

    // At HEAD every key ever inserted must still resolve. The oldest keys (k1..k5) are the ones a
    // non-cumulative, prior-delta-anchored DIFFERENTIAL chain would drop from the read window.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
      // The seeded item carries both "items" and "k1"; every appended item carries one "kN".
      assertNameKeyCount(rtx, ic, nameIndexDef, "items", 1);
      for (int k = 1; k <= 7; k++) {
        assertNameKeyCount(rtx, ic, nameIndexDef, "k" + k, 1);
      }
    }
  }

  @Test
  @DisplayName("read at every intermediate revision is cumulative-up-to-that-revision under DIFFERENTIAL")
  void differentialIntermediateRevisionsAreCumulative() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled for this regression");

    final var database = JsonTestHelper.getDatabaseWithResourceConfig(JsonTestHelper.PATHS.PATH1.getFile(),
        ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                             .versioningApproach(VersioningType.DIFFERENTIAL)
                             .maxNumberOfRevisionsToRestore(3)
                             .build());
    final IndexDef nameIndexDef;
    final int[] revisions = new int[6];

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"n1\"}]}"));
      trx.commit();
      revisions[0] = trx.getResourceSession().getMostRecentRevisionNumber();
    }

    for (int i = 2; i <= 6; i++) {
      appendItemAndCommit(database, "{\"name\":\"n" + i + "\"}");
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        revisions[i - 1] = session.getMostRecentRevisionNumber();
      }
    }

    // At each historical revision the cumulative "name" cardinality must equal the commit index —
    // a sparse-delta window would under-count early revisions once they leave the two-fragment span.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commit = 1; commit <= 6; commit++) {
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
}
