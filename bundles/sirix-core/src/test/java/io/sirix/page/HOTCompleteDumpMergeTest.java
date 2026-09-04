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
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task #57: pins the invariant that the HOT fragment merge never walks past a fragment declaring
 * itself a complete dump.
 *
 * <h2>What this suite is, and what it is NOT</h2>
 *
 * It is NOT a regression test for a fixed defect — nothing was fixed, because nothing could be made
 * to break. {@link HOTLeafPage#completeDump} exists to stop a merge pulling entries out of older
 * fragments; its own javadoc names the failure it prevents — "entries that were moved to the
 * right-half page during the split get resurrected from the base revision". On a plain reading,
 * {@code mergeHOTFragmentsByKey} consults the flag for the NEWEST fragment only, and since {@code
 * copy()} does not carry the flag forward, the commit after a split emits a sparse head, the head
 * check stops firing, and the walk should run straight past the dump into pre-split fragments.
 *
 * <p>
 * IT NEVER DOES. Across three versioning strategies, six commit phases, three window widths, point
 * reads, content-byte comparison, occurrence counting and a full range enumeration over descriptor
 * and column-segment slots — with a structural split confirmed to have run in every one — no merge
 * has ever reached past a complete dump, and no key has ever been resurrected. Some guard upstream
 * of the merge prevents it and has NOT been identified. The merge was therefore left exactly as it
 * was: an unexplained absence is not a licence to change the storage layer.
 * </p>
 *
 * <p>
 * SO THIS SUITE PINS THE ABSENCE INSTEAD OF FIXING THE PRESENCE. Today every case passes against
 * unmodified production code. If a future change removes or weakens whichever guard is doing the
 * work, these assertions fire and point at this investigation, rather than at a corrupt database
 * discovered months later. That is the whole value: a protected unknown beats an unprotected one.
 * </p>
 *
 * <h2>Why these tests are shaped this way</h2>
 *
 * WITNESS BEFORE ASSERTION. Every case proves the merge path was ENTERED, via a counter, before it
 * asserts anything about correctness. Running on a fragmenting strategy is not the same as
 * reconstructing: measured on this codebase, a leaf is still served from a single fragment after
 * one update commit and only merges from the second. A test that skipped the witness could pass
 * while never reconstructing anything — which is exactly how an earlier versioned-merge test in
 * this same area passed with its merge counters reading zero.
 *
 * NEVER FULL. {@link VersioningType#FULL} is immune by construction ({@code
 * bumpHOTPageFragmentChain} returns false, so no chain exists), so a FULL arm would pass vacuously
 * and prove nothing. The parameterised cases cover the three fragmenting strategies; FULL appears
 * only in {@link #fullIsImmuneByConstruction()}, which asserts the immunity itself rather than
 * borrowing it as coverage.
 *
 * THE CANARY IS THE SHARP ASSERTION. {@code completeDumpsWalkedPast()} counts merges that walked
 * through a fragment declaring itself complete — the defect's PRECONDITION, independent of whether
 * any particular key was visibly resurrected. Asserting on the mechanism rather than the symptom is
 * what makes this a guard: a change that re-enables the walk trips it immediately, without needing
 * a workload unlucky enough to expose a wrong answer.
 *
 * <p>
 * AND THE SPLIT ITSELF IS ASSERTED. Starting from a root leaf, an indirect root is a durable
 * production-state witness that a structural split actually ran. A refactor that stops splitting
 * would otherwise leave every assertion below trivially satisfied.
 * </p>
 */
final class HOTCompleteDumpMergeTest {

  private static final String RESOURCE = "hot-complete-dump";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /**
   * Enough distinct row groups with fat payloads to force at least one structural leaf split.
   *
   * <p>
   * MEASURED, and the number matters. At 400 nothing splits at all: the fixture ran clean and
   * "passed" while covering nothing, which is the decoration these tests exist to avoid. A probe over
   * 400 / 2000 / 8000 showed the structural split first appears at 2000. Do not lower this without
   * re-running that probe; the split, not the row count, is the trigger — and
   * {@link #assertVulnerablePathRan} now enforces that mechanically rather than by comment.
   * </p>
   *
   * <p>
   * RETRACTED: an earlier revision of this comment also claimed the probe showed {@code
   * completeDumpsWalkedPast=1} at 2000. It did not. That reading came from an over-broad counter that
   * fired on ENCOUNTERING a complete dump anywhere in the chain, including as the last fragment,
   * which is harmless. Under the counter's current (and correct) definition — fragments visited
   * BEHIND one that declared the chain complete — the probe reads zero, as everything else has.
   * </p>
   */
  private static final int ROW_GROUPS = 2000;

  private static final int PAYLOAD_BYTES = 3 * 1024;

  @BeforeEach
  void setUp() {
    // THE INSTRUMENT MUST BE LIVE, ASSERTED BEFORE ANYTHING ELSE. Every counter this class relies
    // on is gated behind -Dsirix.hot.mergeDiag, because they sit on the default read and commit
    // paths and cannot be always-on. With the gate off they all read zero — and "zero walked past a
    // complete dump" from a DISABLED instrument is indistinguishable from the same reading from a
    // healthy one, so the whole class would pass while measuring nothing. The build supplies the
    // flag; this catches the run where it did not.
    assertTrue(VersioningType.hotMergeDiagEnabled(),
        "HOT merge diagnostics are OFF, so every counter reads zero and every assertion in this "
            + "class would pass vacuously. Run with -Dsirix.hot.mergeDiag=true (the gradle test "
            + "configuration sets it).");
    JsonTestHelper.deleteEverything();
    VersioningType.resetFragmentMergeCounters();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * The four-commit minimum: create, fill to force a split, commit again so the head fragment is
   * sparse, then read. This is the shape in which the code reading says the walk should run through
   * the complete dump into pre-split fragments. It does not, on any of the three strategies.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void aReadAfterASplitMustNotWalkPastTheCompleteDump(final VersioningType versioning) throws IOException {
    createResource(versioning);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      commit1_seed(session);
      commit2_fillToForceSplit(session);
      commit3_touchOneKey(session);
      commit4_touchOneKey(session);

      // NO counter reset here. Reconstruction happens on the read-modify-write COMMIT path, not on
      // the blob reads below - measured: resetting before the read gives merges=0 and the witness
      // fails even though the defect fired during the commits. The counters therefore span the
      // whole scenario, reset once in setUp.
      // NOTE: no clearAllCaches here - see the probe finding below.
      final Set<Long> distinct = readAllRowGroups(session);

      // WITNESS FIRST: without these the assertions below could pass on the single-fragment path,
      // or on a run where no split ever happened.
      assertStructuralSplitRan(versioning, session);
      assertTrue(VersioningType.multiFragmentMerges() > 0,
          versioning + ": the merge path was never entered, so this case proves nothing. " + counters());

      // THE MECHANISM. Nonzero means the walk went through a fragment that declared itself the
      // complete state of its page — the task #57 precondition, whether or not a key visibly changed.
      assertEquals(0, VersioningType.completeDumpsWalkedPast(),
          versioning + ": the merge walked past a complete dump, so pre-split fragments contributed "
              + "entries the split had relocated");

      // THE CONSEQUENCE, asserted separately so a failure says which layer broke.
      assertEquals(ROW_GROUPS, distinct.size(),
          versioning + ": every row group must be readable exactly once after the split");
    }
  }

  /**
   * The five-commit escalation, SLIDING_SNAPSHOT only. A fix that stops NEW resurrection but leaves
   * entries the carry-forward already wrote into the head fragment would pass the four-commit case
   * and still be wrong on a real database, because those entries can never age out again.
   */
  @Test
  void slidingSnapshotMustNotMakeResurrectedEntriesPermanent() throws IOException {
    createResource(VersioningType.SLIDING_SNAPSHOT);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      commit1_seed(session);
      commit2_fillToForceSplit(session);
      commit3_touchOneKey(session);
      commit4_touchOneKey(session);
      commit5_touchOneKey(session);

      // NO counter reset here. Reconstruction happens on the read-modify-write COMMIT path, not on
      // the blob reads below - measured: resetting before the read gives merges=0 and the witness
      // fails even though the defect fired during the commits. The counters therefore span the
      // whole scenario, reset once in setUp.
      // NOTE: no clearAllCaches here - see the probe finding below.
      final Set<Long> distinct = readAllRowGroups(session);

      assertStructuralSplitRan(VersioningType.SLIDING_SNAPSHOT, session);
      assertTrue(VersioningType.multiFragmentMerges() > 0, "the merge path must have been entered. " + counters());
      assertEquals(0, VersioningType.completeDumpsWalkedPast(),
          "after five commits the window has rotated; nothing may have walked behind a complete dump");
      assertEquals(ROW_GROUPS, distinct.size(), "no row group may be lost or duplicated after rotation");
    }
  }

  /**
   * Historical reads are believed correct — at the split revision the newest fragment IS the dump, so
   * the head check fires and the read is sound. Asserted rather than assumed, because it is precisely
   * the property a careless fix could break.
   */
  @Test
  void readingTheRevisionAtWhichTheSplitHappenedStaysCorrect() throws IOException {
    createResource(VersioningType.SLIDING_SNAPSHOT);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      commit1_seed(session);
      commit2_fillToForceSplit(session);
      final int splitRevision = session.getMostRecentRevisionNumber();
      commit3_touchOneKey(session);
      commit4_touchOneKey(session);

      // NO counter reset here. Reconstruction happens on the read-modify-write COMMIT path, not on
      // the blob reads below - measured: resetting before the read gives merges=0 and the witness
      // fails even though the merge ran during the commits. The counters therefore span the whole
      // scenario, reset once in setUp.
      assertStructuralSplitRan(VersioningType.SLIDING_SNAPSHOT, session);

      // A HISTORICAL reader, opened AT the split revision. An earlier version of this case captured
      // splitRevision and then read through a HEAD transaction, passing the revision to a helper
      // that ignored the parameter entirely — so it re-read exactly what every other case reads and
      // asserted nothing whatsoever about the split revision. The revision has to be bound to the
      // READER, because readBlob takes no revision of its own.
      try (JsonNodeReadOnlyTrx atSplitTrx = session.beginNodeReadOnlyTrx(splitRevision)) {
        assertEquals(splitRevision, atSplitTrx.getRevisionNumber(),
            "the probe must actually be positioned at the split revision, or this case is testing " + "the head again");
        final Set<Long> atSplit = readAllRowGroups(atSplitTrx.getStorageEngineReader());
        assertEquals(ROW_GROUPS, atSplit.size(),
            "the revision at which the split happened must read correctly — the head check fires there");
      }
    }
  }

  /** FULL has no fragment chain at all, so it cannot exhibit this defect — asserted, not assumed. */
  @Test
  void fullIsImmuneByConstruction() throws IOException {
    createResource(VersioningType.FULL);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      commit1_seed(session);
      commit2_fillToForceSplit(session);
      commit3_touchOneKey(session);
      commit4_touchOneKey(session);

      // NO counter reset here. Reconstruction happens on the read-modify-write COMMIT path, not on
      // the blob reads below - measured: resetting before the read gives merges=0 and the witness
      // fails even though the defect fired during the commits. The counters therefore span the
      // whole scenario, reset once in setUp.
      // NOTE: no clearAllCaches here - see the probe finding below.
      final Set<Long> distinct = readAllRowGroups(session);

      // The immunity is only meaningful if FULL ran the SAME scenario — same split, same commits —
      // and still kept no chain. Without this the arm could be immune merely by doing less.
      assertStructuralSplitRan(VersioningType.FULL, session);
      assertEquals(ROW_GROUPS, distinct.size(), "FULL must read every row group exactly once");
      assertEquals(0, VersioningType.multiFragmentMerges(),
          "FULL keeps no fragment chain, so nothing should ever reconstruct — this is WHY a FULL arm "
              + "cannot serve as coverage for the other strategies");
      assertEquals(0, VersioningType.completeDumpsWalkedPast());
    }
  }

  /**
   * Production-state coverage guard: starting from a root leaf, only a structural split can create an
   * indirect root. This remains meaningful when implementation-specific split counters disappear.
   */
  private static void assertStructuralSplitRan(final VersioningType versioning, final JsonResourceSession session) {
    try (JsonNodeReadOnlyTrx probe = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = probe.getStorageEngineReader();
      final PageReference rootReference = ProjectionIndexHOTStorage.rootReference(reader, 0);
      assertNotNull(rootReference, versioning + ": projection HOT root must exist");
      assertTrue(reader.loadHOTPage(rootReference) instanceof HOTIndirectPage,
          versioning + ": the projection remained a root leaf, so this case did not execute a structural split; "
              + "re-tune ROW_GROUPS before trusting its other assertions. " + counters());
    }
  }

  /** Every counter, because a single counter's zero is not a fact — the whole set triangulates. */
  private static String counters() {
    return "single=" + VersioningType.singleFragmentReads() + " merges=" + VersioningType.multiFragmentMerges()
        + " walked=" + VersioningType.fragmentsWalked() + " shortCircuit=" + VersioningType.completeDumpShortCircuits()
        + " walkedPastDump=" + VersioningType.completeDumpsWalkedPast() + " carryFwd="
        + VersioningType.carryForwardRotations();
  }

  /**
   * INCREMENTAL is claimed to self-heal because its rotation resets the chain. Unasserted claims
   * decay, so this pins it: after enough commits to rotate, the merge must stop reaching back.
   */
  @Test
  void incrementalRotationResetsTheChain() throws IOException {
    createResource(VersioningType.INCREMENTAL);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      commit1_seed(session);
      commit2_fillToForceSplit(session);
      for (int extra = 0; extra < 8; extra++) {
        write(session, 0, 1, 10 + extra);
      }

      final Set<Long> distinct = readAllRowGroups(session);
      assertStructuralSplitRan(VersioningType.INCREMENTAL, session);
      assertTrue(VersioningType.multiFragmentMerges() > 0,
          "the merge path must have been entered across the rotation. " + counters());
      assertEquals(ROW_GROUPS, distinct.size(), "every row group readable after rotation");
      assertEquals(0, VersioningType.completeDumpsWalkedPast(),
          "INCREMENTAL must not walk past a complete dump either - its rotation shortens exposure, "
              + "it does not make the walk correct");
    }
  }

  // ---------------------------------------------------------------- fixture

  private static void createResource(final VersioningType versioning) throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).build());
    }
  }

  private static void commit1_seed(final JsonResourceSession session) {
    write(session, 0, 8, 1);
  }

  /** The split: enough fat entries in one go that the leaf must divide. */
  private static void commit2_fillToForceSplit(final JsonResourceSession session) {
    write(session, 0, ROW_GROUPS, 2);
  }

  private static void commit3_touchOneKey(final JsonResourceSession session) {
    write(session, 0, 1, 3);
  }

  // All three touch THE SAME slot, and that is the whole point: a fragment chain forms per LEAF, so
  // successive commits must revisit the same leaf or every read stays single-fragment. Measured -
  // with commits 3/4/5 touching slots 1/2/3 the fixture reported merges=0 while splits were
  // happening, and the witness correctly refused to let it claim coverage.
  private static void commit4_touchOneKey(final JsonResourceSession session) {
    write(session, 0, 1, 4);
  }

  private static void commit5_touchOneKey(final JsonResourceSession session) {
    write(session, 0, 1, 5);
  }

  private static void write(final JsonResourceSession session, final int from, final int toExclusive,
      final int commitMarker) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      for (int i = from; i < toExclusive; i++) {
        // A FRESH array per slot. Reusing one and mutating it stores a reference whose bytes then
        // change under the already-computed hash, and every slot fails verification with a length
        // that matches — which is what the FULL arm caught on the first run, doing exactly the job
        // an immune control is for.
        final byte[] payload = new byte[PAYLOAD_BYTES];
        payload[0] = (byte) (i & 0xFF);
        payload[1] = (byte) ((i >>> 8) & 0xFF);
        // Distinct bytes per commit. Rewriting a slot with IDENTICAL content need not dirty the
        // page, and then no new fragment forms and nothing ever reconstructs - measured: without
        // this the fixture reported merges=0 while splits were happening.
        payload[2] = (byte) commitMarker;
        storage.putBlob(i + 1, payload);
      }
      wtx.commit();
    }
  }

  private static Set<Long> readAllRowGroups(final JsonResourceSession session) {
    try (JsonNodeTrx probe = session.beginNodeTrx()) {
      return readAllRowGroups(probe.getStorageEngineReader());
    }
  }

  /** Every slot that answers, collected as a SET so a duplicate shows up as a missing member. */
  /**
   * Read every row group and key the result on the CONTENT, not on the loop counter.
   *
   * <p>
   * The earlier version added {@code i + 1} — the key it had just asked for — so the set counted "how
   * many slots answered non-null" and nothing more. A slot returning some OTHER row group's payload
   * was structurally invisible, which is precisely the failure this class exists to detect. The
   * payload encodes its own row-group index in bytes 0..1 (see {@link #write}), so the identity is
   * checked against the key that was requested and the set is built from what came back.
   * </p>
   *
   * <p>
   * SCOPE, stated because the earlier comment overclaimed: point reads cannot see DUPLICATION — two
   * stored copies of one key still answer once. Duplicate detection needs the range enumeration over
   * descriptor and column-segment slots, which lives outside this class.
   * </p>
   */
  private static Set<Long> readAllRowGroups(final StorageEngineReader reader) {
    final Set<Long> seen = new HashSet<>();
    for (int i = 0; i < ROW_GROUPS; i++) {
      final long requestedKey = (long) i + 1;
      final byte[] blob = ProjectionIndexHOTStorage.readBlob(reader, 0, requestedKey);
      if (blob != null && blob.length > 0) {
        final long identityInPayload = ((blob[0] & 0xFFL) | ((blob[1] & 0xFFL) << 8)) + 1L;
        assertEquals(requestedKey, identityInPayload,
            "slot " + requestedKey + " answered with row group " + identityInPayload
                + "'s payload — a wrong-content read, which keying on the loop counter would have " + "hidden. "
                + counters());
        seen.add(identityInPayload);
      }
    }
    return seen;
  }
}
