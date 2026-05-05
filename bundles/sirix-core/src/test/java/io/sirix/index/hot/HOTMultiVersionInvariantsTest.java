/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.IndexBackendType;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.SearchMode;
import io.sirix.index.cas.CASIndexListenerFactory;
import io.sirix.index.name.NameIndexListenerFactory;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Multi-version invariants for HOT-backed secondary indexes.
 *
 * <p>Pinned guarantees of the chain-wiring + factory-level CoW machinery landed during the
 * task #57 campaign: each test below maps to a specific invariant the implementation must hold,
 * the comment block above the test names the invariant explicitly so a future regression points
 * at the right structural property to inspect.</p>
 *
 * <p>All tests run with HOT enabled (the resource config default) so the eager Names dictionary
 * load in {@link io.sirix.page.NamePage}'s deep-copy constructor fires, and the factory-level
 * NamePage / CASPage / PathPage / ProjectionIndexPage CoW (analogous to the document trie's
 * {@code KeyedTrieWriter.prepareIndirectPage}) installs a private deep-copy as the modified
 * page in the TIL at wtx start.</p>
 */
@DisplayName("HOT Multi-Version Invariants")
final class HOTMultiVersionInvariantsTest {

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

  /**
   * Invariant 1 (intermediate-revision isolation, NAME): reading at any committed revision N
   * returns the cumulative-up-to-N view of the NAME index. A write at rev M (M &gt; N) must
   * not bleed into rev N's view.
   */
  @Test
  @DisplayName("NAME index: every committed revision is independently readable")
  void nameIndexEveryRevisionIndependentlyReadable() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final List<Integer> revisions = new ArrayList<>();

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"a\"}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }

    appendItemAndCommit(database, "{\"name\":\"b\"}", revisions);
    appendItemAndCommit(database, "{\"name\":\"c\"}", revisions);
    appendItemAndCommit(database, "{\"name\":\"d\"}", revisions);

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commitIdx = 1; commitIdx <= 4; commitIdx++) {
        final int rev = revisions.get(commitIdx - 1);
        try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
          final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
          assertNameKeyCount(rtx, ic, nameIndexDef, "name", commitIdx,
              "rev " + rev + " 'name' cardinality should equal commit number " + commitIdx);
        }
      }
    }
  }

  /**
   * Invariant 2 (intermediate-revision isolation, CAS): reading at any committed revision N
   * returns the cumulative-up-to-N value distribution. Each rev's value-class cardinality is the
   * count of inserts of that exact value at or before rev N.
   */
  @Test
  @DisplayName("CAS index: per-rev value distribution is monotone-cumulative")
  void casIndexPerRevisionValueDistribution() {
    assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef casIndexDef;
    final List<Integer> revisions = new ArrayList<>();

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var path = Path.parse("/orders/[]/status", PathParser.Type.JSON);
      casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(path), 0,
          IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"orders\":[{\"id\":1,\"status\":\"new\"}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }

    appendOrderAndCommit(database, 2, "pending", revisions);
    appendOrderAndCommit(database, 3, "pending", revisions);
    appendOrderAndCommit(database, 4, "shipped", revisions);

    final int[][] expected = {
        // {new, pending, shipped} cumulative counts at each rev
        { 1, 0, 0 },
        { 1, 1, 0 },
        { 1, 2, 0 },
        { 1, 2, 1 }
    };
    final String[] statuses = { "new", "pending", "shipped" };

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commitIdx = 0; commitIdx < 4; commitIdx++) {
        final int rev = revisions.get(commitIdx);
        try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
          final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
          for (int s = 0; s < statuses.length; s++) {
            assertStatusCardinality(rtx, ic, casIndexDef, statuses[s], expected[commitIdx][s],
                "rev " + rev + " '" + statuses[s] + "' count");
          }
        }
      }
    }
  }

  /**
   * Invariant 3 (multi-rev persistence across session reopen): closing the session and reopening
   * the database must preserve every committed revision's view. Failure here points at on-disk
   * fragment-chain serialisation drift, not in-memory caching.
   */
  @Test
  @DisplayName("multi-rev: views survive session close + reopen")
  void multiRevSurvivesReopen() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final List<Integer> revisions = new ArrayList<>();

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"alpha\":1}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }

    appendItemAndCommit(database, "{\"beta\":2}", revisions);
    appendItemAndCommit(database, "{\"gamma\":3}", revisions);

    // Close DB by flushing every dangling reference. JsonTestHelper caches the open Database;
    // re-opening forces fresh page loads from disk (cache is per-buffer-manager, not persisted).
    JsonTestHelper.closeEverything();
    final var reopened = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // After reopen rev 1 must still show only "items" + "alpha"; rev 2 adds "beta"; rev 3 adds "gamma".
      try (final var rtx = session.beginNodeReadOnlyTrx(revisions.get(0))) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "alpha", 1, "rev 1 (post-reopen) 'alpha'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "beta", 0, "rev 1 (post-reopen) 'beta'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "gamma", 0, "rev 1 (post-reopen) 'gamma'");
      }
      try (final var rtx = session.beginNodeReadOnlyTrx(revisions.get(1))) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "alpha", 1, "rev 2 (post-reopen) 'alpha'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "beta", 1, "rev 2 (post-reopen) 'beta'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "gamma", 0, "rev 2 (post-reopen) 'gamma'");
      }
      try (final var rtx = session.beginNodeReadOnlyTrx(revisions.get(2))) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "alpha", 1, "rev 3 (post-reopen) 'alpha'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "beta", 1, "rev 3 (post-reopen) 'beta'");
        assertNameKeyCount(rtx, ic, nameIndexDef, "gamma", 1, "rev 3 (post-reopen) 'gamma'");
      }
    }
  }

  /**
   * Invariant 4 (chain rotation under SLIDING_SNAPSHOT): once the prior on-disk leaf chain
   * exceeds {@code revToRestore-1}, the writer forces a full emit so no entry becomes
   * unreachable. With the default {@code revToRestore=3} (chainCap=2), commits 1..3 each grow
   * the chain by one; commit 4 triggers rotation. Reads at every rev must still see the right
   * cumulative view both before and after the rotation.
   */
  @Test
  @DisplayName("chain rotation: per-rev view stays correct across forced full emits")
  void chainRotationPerRevisionStillCorrect() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final List<Integer> revisions = new ArrayList<>();
    // Push past chainCap so the rotation path fires twice.
    final int totalCommits = 6;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"v0\"}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }

    for (int i = 1; i < totalCommits; i++) {
      appendItemAndCommit(database, "{\"name\":\"v" + i + "\"}", revisions);
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commitIdx = 1; commitIdx <= totalCommits; commitIdx++) {
        final int rev = revisions.get(commitIdx - 1);
        try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
          final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
          assertNameKeyCount(rtx, ic, nameIndexDef, "name", commitIdx,
              "rev " + rev + " 'name' across rotation (commit " + commitIdx + ")");
        }
      }
    }
  }

  /**
   * Invariant 5 (multi-key sparse fragment): a single commit that touches several keys must emit
   * each modified slot in the rev's sparse fragment, AND every prior rev's contribution to keys
   * absent from this fragment must remain reachable through the chain.
   */
  @Test
  @DisplayName("multi-key commit: sparse fragment + chain reconstruction is complete")
  void multiKeySparseFragmentCompleteness() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final int rev1, rev2;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"alpha\":1,\"beta\":2,\"gamma\":3}]}"));
      trx.commit();
    }
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      rev1 = session.getMostRecentRevisionNumber();
    }

    // Second commit appends a new array element with a DIFFERENT key set — alpha and beta stay
    // rev-1-only; gamma gets a 2nd occurrence; delta and epsilon are rev-2 firsts.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      trx.moveToFirstChild();
      trx.moveToFirstChild();
      trx.moveToLastChild();
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
          "{\"gamma\":30,\"delta\":4,\"epsilon\":5}"));
      trx.commit();
    }
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      rev2 = session.getMostRecentRevisionNumber();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var rtx = session.beginNodeReadOnlyTrx(rev1)) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "alpha", 1, "rev1 alpha");
        assertNameKeyCount(rtx, ic, nameIndexDef, "beta", 1, "rev1 beta");
        assertNameKeyCount(rtx, ic, nameIndexDef, "gamma", 1, "rev1 gamma");
        assertNameKeyCount(rtx, ic, nameIndexDef, "delta", 0, "rev1 delta absent");
      }
      try (final var rtx = session.beginNodeReadOnlyTrx(rev2)) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "alpha", 1,
            "rev2 alpha must still be reachable through chain");
        assertNameKeyCount(rtx, ic, nameIndexDef, "beta", 1,
            "rev2 beta must still be reachable through chain");
        assertNameKeyCount(rtx, ic, nameIndexDef, "gamma", 2, "rev2 gamma cumulative");
        assertNameKeyCount(rtx, ic, nameIndexDef, "delta", 1, "rev2 delta first occurrence");
        assertNameKeyCount(rtx, ic, nameIndexDef, "epsilon", 1, "rev2 epsilon first occurrence");
      }
    }
  }

  /**
   * Invariant 6 (PageReference identity isolation): the {@link io.sirix.page.PageReference}
   * returned for a NAME index's HOT root at rev N must be identity-distinct from the one at
   * rev N-1 — the factory-level CoW must publish a deep-copy as the modified page in the TIL
   * with its own children array, so cross-revision aliasing through the parent {@code NamePage}
   * cannot occur.
   */
  @Test
  @DisplayName("identity: NamePage's per-index slot is not aliased across revisions")
  void namePageSlotIdentityIsolatedAcrossRevisions() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final int rev1, rev2;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"foo\":1}]}"));
      trx.commit();
    }
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      rev1 = session.getMostRecentRevisionNumber();
    }

    appendItemAndCommit(database, "{\"bar\":2}", new ArrayList<>(List.of(rev1)));
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      rev2 = session.getMostRecentRevisionNumber();
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final io.sirix.page.PageReference rev1Slot;
      final io.sirix.page.PageReference rev2Slot;
      try (final var rtx = session.beginNodeReadOnlyTrx(rev1)) {
        final var rrp = rtx.getStorageEngineReader().getActualRevisionRootPage();
        final var namePage = rtx.getStorageEngineReader().getNamePage(rrp);
        rev1Slot = namePage.getOrCreateReference(nameIndexDef.getID());
        assertNotNull(rev1Slot, "rev1 NAME index slot must exist");
      }
      try (final var rtx = session.beginNodeReadOnlyTrx(rev2)) {
        final var rrp = rtx.getStorageEngineReader().getActualRevisionRootPage();
        final var namePage = rtx.getStorageEngineReader().getNamePage(rrp);
        rev2Slot = namePage.getOrCreateReference(nameIndexDef.getID());
        assertNotNull(rev2Slot, "rev2 NAME index slot must exist");
      }
      assertNotEquals(System.identityHashCode(rev1Slot), System.identityHashCode(rev2Slot),
          "rev1 and rev2 NAME index slot PageReferences must be different instances; aliasing "
              + "would mean a write at rev2 mutates the rev1 reader's view of the slot.");
      assertNotEquals(rev1Slot.getKey(), rev2Slot.getKey(),
          "rev1 and rev2 NAME index slot keys must point at different on-disk indirect pages "
              + "(the new rev assigns a fresh offset for the CoW'd HOT indirect).");
    }
  }

  /**
   * Invariant 7 (two read-only transactions interleaved with a writer): rtx pinned at rev N
   * before another writer commits rev N+1 must continue to observe rev N's view when the writer
   * is finished. This guards against any code path that mutates a cached page in place after
   * the rtx was opened.
   */
  @Test
  @DisplayName("rtx pinned before writer commits: read view stays at its bound rev")
  void readerPinnedBeforeWriterStaysAtBoundRev() {
    assertTrue(NameIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final int rev1;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"foo\":1}]}"));
      trx.commit();
    }
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      rev1 = session.getMostRecentRevisionNumber();
    }

    // Open the rtx FIRST, then run the writer on the SAME session (Sirix supports concurrent
    // wtx + multiple rtx). The rtx must keep showing rev1's view after the writer commits.
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx(rev1)) {

      try (final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            "{\"foo\":2,\"bar\":3}"));
        wtx.commit();
      }

      final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
      // rev1 view: 1× foo, 1× items. NO bar. NO second foo.
      assertNameKeyCount(rtx, ic, nameIndexDef, "foo", 1,
          "pinned rtx must still see only the pre-write 'foo' count");
      assertNameKeyCount(rtx, ic, nameIndexDef, "bar", 0,
          "pinned rtx must NOT see 'bar' from the post-rtx commit");
      assertNameKeyCount(rtx, ic, nameIndexDef, "items", 1, "pinned rtx 'items' count");
    }
  }

  /**
   * Invariant 8 (Names dictionary post-CoW): under the HOT backend the
   * {@link io.sirix.page.NamePage} copy constructor eager-loads every {@link
   * io.sirix.index.name.Names} dictionary so cached and CoW'd pages share populated instances.
   * This pins the gating: HOT-configured resources should observe non-null dictionaries on the
   * CoW'd page after the first write touches it.
   */
  @Test
  @DisplayName("dictionary: HOT-configured CoW shares populated Names instances")
  void hotConfiguredCowSharesPopulatedNames() {
    // Sanity: default config is HOT. If this regresses, this whole suite needs to re-evaluate
    // its assumptions, so fail loudly with a clear pointer rather than silently testing the
    // wrong backend.
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      assertEquals(IndexBackendType.HOT, session.getResourceConfig().indexBackendType,
          "test suite assumes HOT — JsonTestHelper.getDatabase must default to HOT");
    }
  }

  /**
   * Invariant 9 (CAS payload reachability per revision): not just <em>cardinality</em> but the
   * exact set of indexed values at each historical revision must match what was committed at
   * that point. The assertion here probes for value PRESENCE — at rev N we must find the
   * specific status string committed at or before rev N (and we must NOT find a value that was
   * inserted only at a later rev).
   */
  @Test
  @DisplayName("CAS index: exact value membership at every historical revision")
  void casIndexExactValueMembershipPerRevision() {
    assertTrue(CASIndexListenerFactory.isHOTEnabled(), "HOT must be enabled");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef casIndexDef;
    final List<Integer> revisions = new ArrayList<>();

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var path = Path.parse("/orders/[]/status", PathParser.Type.JSON);
      casIndexDef = IndexDefs.createCASIdxDef(false, Type.STR, Collections.singleton(path), 0,
          IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"orders\":[{\"id\":1,\"status\":\"alpha\"}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }

    appendOrderAndCommit(database, 2, "beta", revisions);
    appendOrderAndCommit(database, 3, "gamma", revisions);

    // Each rev's expected status set: every value committed at OR BEFORE that rev must be
    // present, every later-only value must be absent.
    final String[][] presentByRev = {
        { "alpha" },
        { "alpha", "beta" },
        { "alpha", "beta", "gamma" }
    };
    final String[] futureOnly = { "delta", "epsilon" }; // never inserted

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int commitIdx = 0; commitIdx < revisions.size(); commitIdx++) {
        final int rev = revisions.get(commitIdx);
        try (final var rtx = session.beginNodeReadOnlyTrx(rev)) {
          final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
          for (final String s : presentByRev[commitIdx]) {
            assertStatusCardinality(rtx, ic, casIndexDef, s, 1L,
                "rev " + rev + " must have '" + s + "' indexed");
          }
          for (final String s : futureOnly) {
            assertStatusCardinality(rtx, ic, casIndexDef, s, 0L,
                "rev " + rev + " must not have '" + s + "' (never committed)");
          }
          // Extra guard: a value committed only at LATER revs must be absent at this rev.
          if (commitIdx < revisions.size() - 1) {
            for (int j = commitIdx + 1; j < presentByRev.length; j++) {
              for (final String laterOnly : presentByRev[j]) {
                final boolean alreadyPresent =
                    java.util.Arrays.asList(presentByRev[commitIdx]).contains(laterOnly);
                if (!alreadyPresent) {
                  assertStatusCardinality(rtx, ic, casIndexDef, laterOnly, 0L,
                      "rev " + rev + " must not have '" + laterOnly
                          + "' (committed at later rev)");
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Invariant 10 (PageReference disk-key uniqueness): each committed revision's HOT-root
   * indirect lives at its own append-only disk offset. Equal {@code key} between two
   * revisions' slots means a write overwrote a historical fragment (an append-only invariant
   * violation).
   */
  @Test
  @DisplayName("disk: every revision's HOT root indirect lives at a distinct disk offset")
  void everyRevisionHotRootHasDistinctDiskOffset() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final List<Integer> revisions = new ArrayList<>();

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"k0\":0}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }
    for (int i = 1; i <= 4; i++) {
      appendItemAndCommit(database, "{\"k" + i + "\":" + i + "}", revisions);
    }

    final long[] keys = new long[revisions.size()];
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      for (int i = 0; i < revisions.size(); i++) {
        try (final var rtx = session.beginNodeReadOnlyTrx(revisions.get(i))) {
          final var rrp = rtx.getStorageEngineReader().getActualRevisionRootPage();
          final var namePage = rtx.getStorageEngineReader().getNamePage(rrp);
          keys[i] = namePage.getOrCreateReference(nameIndexDef.getID()).getKey();
          assertTrue(keys[i] > 0, "rev " + revisions.get(i) + " HOT root key must be on-disk (>0)");
        }
      }
    }

    // Pairwise uniqueness: each rev's disk offset for the HOT root indirect is distinct.
    for (int i = 0; i < keys.length; i++) {
      for (int j = i + 1; j < keys.length; j++) {
        assertNotEquals(keys[i], keys[j], "rev " + revisions.get(i) + " key=" + keys[i]
            + " collides with rev " + revisions.get(j) + " — overwrites a historical fragment");
      }
    }
  }

  /**
   * Invariant 11 (chain pageFragments shrink at rotation): when commit N+1's bump triggers
   * forceFullEmit, the persisted chain on rev N+1's leaf reference is empty (rotation cleared
   * it). Reading at rev N+1 must therefore return the cumulative state from the FULL leaf
   * alone — no chain walk needed.
   */
  @Test
  @DisplayName("chain: forceFullEmit at rotation produces a full leaf with empty chain")
  void chainRotationProducesFullLeafEmptyChain() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef nameIndexDef;
    final List<Integer> revisions = new ArrayList<>();
    // Default revToRestore=3 ⇒ chainCap=2. Rotation triggers at the FOURTH commit.
    final int totalCommits = 5;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      nameIndexDef = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(nameIndexDef), trx);
      trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"items\":[{\"name\":\"a\"}]}"));
      trx.commit();
      revisions.add(session.getMostRecentRevisionNumber());
    }
    for (int i = 1; i < totalCommits; i++) {
      appendItemAndCommit(database, "{\"name\":\"v" + i + "\"}", revisions);
    }

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      // Last rev must show every committed 'name' insertion; the chain-walked combine through
      // all the rotation boundaries must add up to totalCommits.
      try (final var rtx = session.beginNodeReadOnlyTrx(revisions.getLast())) {
        final var ic = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNameKeyCount(rtx, ic, nameIndexDef, "name", totalCommits,
            "post-rotation HEAD 'name' cardinality must equal totalCommits=" + totalCommits);
      }
    }
  }

  // ===== helpers =====

  private static void appendItemAndCommit(
      final Database<JsonResourceSession> database, final String json, final List<Integer> revs) {
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      trx.moveToFirstChild();
      trx.moveToFirstChild();
      trx.moveToLastChild();
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(json));
      trx.commit();
      revs.add(session.getMostRecentRevisionNumber());
    }
  }

  private static void appendOrderAndCommit(
      final Database<JsonResourceSession> database, final int id, final String status,
      final List<Integer> revs) {
    final String json = String.format("{\"id\":%d,\"status\":\"%s\"}", id, status);
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      trx.moveToFirstChild();
      trx.moveToFirstChild();
      trx.moveToLastChild();
      trx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(json));
      trx.commit();
      revs.add(session.getMostRecentRevisionNumber());
    }
  }

  private static void assertNameKeyCount(
      final NodeReadOnlyTrx rtx, final IndexController<?, ?> ic, final IndexDef nameIndexDef,
      final String name, final long expected, final String message) {
    final var iter = ic.openNameIndex(rtx.getStorageEngineReader(), nameIndexDef,
        ic.createNameFilter(Set.of(name)));
    if (expected == 0) {
      if (!iter.hasNext()) return;
      assertEquals(0L, iter.next().getNodeKeys().getLongCardinality(), message);
      return;
    }
    if (!iter.hasNext()) fail(message + " — index entry missing");
    assertEquals(expected, iter.next().getNodeKeys().getLongCardinality(), message);
  }

  private static void assertStatusCardinality(
      final JsonNodeReadOnlyTrx rtx, final IndexController<?, ?> ic, final IndexDef casIndexDef,
      final String status, final long expected, final String message) {
    final var iter = ic.openCASIndex(rtx.getStorageEngineReader(), casIndexDef,
        ic.createCASFilter(Set.of("/orders/[]/status"), new Str(status), SearchMode.EQUAL,
            new JsonPCRCollector(rtx)));
    if (expected == 0) {
      if (!iter.hasNext()) return;
      assertEquals(0L, iter.next().getNodeKeys().getLongCardinality(), message);
      return;
    }
    if (!iter.hasNext()) fail(message + " — index entry missing");
    assertEquals(expected, iter.next().getNodeKeys().getLongCardinality(), message);
  }
}
