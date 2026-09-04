/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DELETIONS across the sliding window: a tombstoned key must stay deleted through fragment
 * reconstruction, through window eviction, and through a leaf split — including after the tombstone
 * itself has aged out of the window.
 *
 * <h2>Why this exists separately from {@link HOTCompleteDumpMergeTest}</h2>
 *
 * Every probe in the task #57 investigation WROTE or UPDATED slots; not one ever DELETED one. So
 * two surfaces went entirely unexercised: tombstones, and the eviction path
 * ({@code carryForwardAgingHOTEntries}) which is a different method from the merge walk. The
 * dangerous case is their interaction — if the carry-forward re-emitted the live value of a key
 * that a newer fragment had tombstoned, a deleted row would come back and be written into the HEAD
 * fragment, where it can never age out again. That is the same resurrection shape as #57 but
 * through eviction rather than merge, and it would be a wrong answer on the shipping default
 * (SLIDING_SNAPSHOT, {@code revisionsToRestore = 3}).
 *
 * <h2>The witness is the specific mechanism, not "something ran"</h2>
 *
 * Asserting "the deleted key is absent" is trivially satisfied by a run where no eviction ever
 * happened — the empty-enumeration mistake. Each case therefore asserts the counter for the
 * mechanism it is actually about, BEFORE its absence assertion: merge cases assert
 * {@code multiFragmentMerges > 0}, eviction cases assert {@code carryForwardRotations > 0}, and the
 * split case asserts an indirect projection root. "A merge ran" is NOT a witness that an eviction
 * ran, and the two are different code paths.
 *
 * <h2>Window arithmetic, so the commit counts are not folklore</h2>
 *
 * {@code hotSlidingSnapshotEvicts} uses {@code chainCap = revisionsToRestore - 1 = 2}, and evicts
 * once {@code fragments.size() + 1 > chainCap}. So eviction begins as soon as two on-disk fragments
 * exist — from roughly the third commit on a given leaf — and every later commit evicts again. The
 * sweep below therefore runs well past that point and asserts after EVERY commit, which also covers
 * the phase at which the tombstone itself is the entry being dropped.
 */
final class HOTTombstoneEvictionTest {

  private static final String RESOURCE = "hot-tombstone";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Small, per the reproducer's brief — these cases are about deletion, not about splitting. */
  private static final int KEYS = 8;

  /** The key that gets deleted and must never come back. */
  private static final long DELETED_KEY = 3L;

  /** A key that is never touched after seeding — it must survive every eviction. */
  private static final long UNTOUCHED_KEY = 5L;

  /** The key rewritten on every commit, to keep producing fragments. */
  private static final long CHURN_KEY = 1L;

  private static final int PAYLOAD_BYTES = 3 * 1024;

  /** Matches {@link HOTCompleteDumpMergeTest}: the row count at which a structural split appears. */
  private static final int SPLIT_KEYS = 2000;

  @BeforeEach
  void setUp() {
    // The rotation and split witnesses are gated behind -Dsirix.hot.mergeDiag (these counters sit
    // on the default commit path and cannot be always-on). With the gate off they read zero, so the
    // witnesses would fail — correctly, but confusingly. Say plainly what is wrong instead.
    assertTrue(VersioningType.hotMergeDiagEnabled(),
        "HOT merge diagnostics are OFF, so the rotation and split witnesses cannot fire. Run with "
            + "-Dsirix.hot.mergeDiag=true (the gradle test configuration sets it).");
    JsonTestHelper.deleteEverything();
    VersioningType.resetFragmentMergeCounters();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * CASE 1 — tombstone through a MERGE. The live value sits in an older in-window fragment and the
   * tombstone in a newer one, so reconstruction must let the tombstone shadow it.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void aTombstoneMustShadowALiveValueInAnOlderFragment(final VersioningType versioning) throws IOException {
    createResource(versioning);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      seed(session);
      churn(session, 2);
      delete(session, DELETED_KEY);
      churn(session, 4);

      assertTrue(VersioningType.multiFragmentMerges() > 0,
          versioning + ": no reconstruction happened, so this case proves nothing. " + counters());
      assertDeletedStaysDeleted(session, versioning + " after merge");
    }
  }

  /**
   * CASE 2 and CASE 4 — tombstone through EVICTION, asserted at every phase.
   *
   * <p>
   * Running the assertion after every commit is what covers case 4 (the tombstone itself aging out)
   * without guessing which commit that happens on: whichever phase drops it, the very next check
   * catches a resurrection, and the failure message names the phase. Cheaper and less fragile than
   * computing the eviction phase and asserting only there.
   * </p>
   */
  @Test
  void aDeletedKeyMustNotReturnWhenItsFragmentsAgeOutOfTheWindow() throws IOException {
    createResource(VersioningType.SLIDING_SNAPSHOT);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      seed(session);
      delete(session, DELETED_KEY);

      long rotationsFirstSeenAtPhase = -1;
      for (int phase = 0; phase < 8; phase++) {
        churn(session, 10 + phase);
        if (rotationsFirstSeenAtPhase < 0 && VersioningType.carryForwardRotations() > 0) {
          rotationsFirstSeenAtPhase = phase;
        }
        assertDeletedStaysDeleted(session, "phase " + phase);
      }

      // THE WITNESS, and it is the whole point of this case: without a rotation the absence above
      // is an absence of eviction, not a property of eviction.
      assertTrue(VersioningType.carryForwardRotations() > 0,
          "no fragment ever aged out, so this case says nothing about eviction. " + counters());
      assertTrue(rotationsFirstSeenAtPhase >= 0, "rotation phase not observed. " + counters());
    }
  }

  /**
   * CASE 3 — the full composition: a tombstone, a structural split that relocates entries around it,
   * and then enough commits that the window ROTATES on the split leaf.
   *
   * <p>
   * The first version of this case asserted only the split and measured {@code rotations=0} — it
   * composed a tombstone with a split but never evicted anything, so the interaction the reproducer
   * was asked for was still untested. It now asserts BOTH mechanisms fired, which is the difference
   * between "these two features were present" and "these two features interacted".
   * </p>
   */
  @Test
  void deletedKeysMustNotReturnWhenASplitRelocatesThemAndTheWindowThenRotates() throws IOException {
    createResource(VersioningType.SLIDING_SNAPSHOT);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      // Seed a small prefix, delete inside it, THEN grow the leaf until it splits, so the split
      // relocates entries across keys that are already tombstoned.
      writeRange(session, 0, 64, 1);
      delete(session, DELETED_KEY);
      writeRange(session, 64, SPLIT_KEYS, 2);

      // Now churn past the window cap so the post-split fragments age out underneath the tombstone.
      for (int phase = 0; phase < 6; phase++) {
        writeRange(session, 0, 1, 20 + phase);
        assertDeletedStaysDeleted(session, "post-split phase " + phase);
      }

      try (JsonNodeReadOnlyTrx probe = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        final PageReference rootReference = ProjectionIndexHOTStorage.rootReference(reader, 0);
        assertNotNull(rootReference, "projection HOT root must exist");
        assertTrue(reader.loadHOTPage(rootReference) instanceof HOTIndirectPage,
            "the projection remained a root leaf, so the split half of this case is not covered. " + counters());
      }
      assertTrue(VersioningType.carryForwardRotations() > 0,
          "the window never rotated, so this case is a split test, not an interaction test. " + counters());
      assertDeletedStaysDeleted(session, "after split and rotation");
    }
  }

  // ---------------------------------------------------------------- assertions

  /**
   * The deleted key must be gone AND its neighbours must still be there. Asserting only the absence
   * would pass on a store that lost everything, which is the twin every decline arm needs.
   */
  private static void assertDeletedStaysDeleted(final JsonResourceSession session, final String where) {
    try (JsonNodeTrx probe = session.beginNodeTrx()) {
      final StorageEngineReader reader = probe.getStorageEngineReader();
      assertNull(ProjectionIndexHOTStorage.readBlob(reader, 0, DELETED_KEY),
          where + ": the deleted key came back. " + counters());
      assertNotNull(ProjectionIndexHOTStorage.readBlob(reader, 0, UNTOUCHED_KEY),
          where + ": an untouched live key was LOST, so the absence above is not evidence of " + "correct deletion. "
              + counters());
      assertNotNull(ProjectionIndexHOTStorage.readBlob(reader, 0, CHURN_KEY),
          where + ": the churned key was lost. " + counters());
    }
  }

  private static String counters() {
    return "single=" + VersioningType.singleFragmentReads() + " merges=" + VersioningType.multiFragmentMerges()
        + " walked=" + VersioningType.fragmentsWalked() + " shortCircuit=" + VersioningType.completeDumpShortCircuits()
        + " walkedPastDump=" + VersioningType.completeDumpsWalkedPast() + " carryFwdRotations="
        + VersioningType.carryForwardRotations() + " carryFwdReemitted="
        + VersioningType.carryForwardEntriesReemitted();
  }

  // ---------------------------------------------------------------- fixture

  private static void createResource(final VersioningType versioning) throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
  }

  private static void seed(final JsonResourceSession session) {
    writeRange(session, 0, KEYS, 1);
  }

  /** Rewrite only the churn key, with fresh content so the page actually dirties. */
  private static void churn(final JsonResourceSession session, final int marker) {
    writeRange(session, (int) CHURN_KEY - 1, (int) CHURN_KEY, marker);
  }

  private static void writeRange(final JsonResourceSession session, final int from, final int toExclusive,
      final int commitMarker) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      for (int i = from; i < toExclusive; i++) {
        // A FRESH array per slot — a reused, mutated buffer changes bytes under an already-computed
        // content hash and every slot then fails verification (measured in the sibling fixture).
        final byte[] payload = new byte[PAYLOAD_BYTES];
        payload[0] = (byte) (i & 0xFF);
        payload[1] = (byte) ((i >>> 8) & 0xFF);
        // Distinct per commit: rewriting a slot with IDENTICAL content need not dirty the page, and
        // then no fragment forms and nothing evicts or reconstructs.
        payload[2] = (byte) commitMarker;
        storage.putBlob(i + 1, payload);
      }
      wtx.commit();
    }
  }

  private static void delete(final JsonResourceSession session, final long slotKey) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).tombstoneBlob(slotKey);
      wtx.commit();
    }
  }
}
