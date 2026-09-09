/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.HOTIndirectPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task #57, THE QUIET ARM — asserted on BYTES, because the symptom it looks for would not throw.
 *
 * <h2>What is established here, and what is only predicted</h2>
 *
 * PREDICTED BY A CODE READING, NEVER OBSERVED: if a merge walked past a fragment that declared
 * itself a complete dump, it could serve a row group's segment from a pre-split copy. The loud form
 * of that would raise — {@code collectSlotsRange} finding two descriptor slots for one row group —
 * but the column-segment side carries no duplicate check, so were the split point to fall in the
 * composite key's low bits, a row group could keep a single descriptor while some SEGMENTS
 * duplicated, and the store would simply answer with one of them.
 *
 * <p>
 * NO EXECUTION HAS EVER PRODUCED EITHER FORM. This class passes against unmodified production code,
 * as did every other probe run for task #57 — three strategies, six phases, three window widths,
 * point reads, content bytes and a full range enumeration, with a ref-carrying incremental split
 * confirmed each time. What this class therefore IS is a pinned invariant rather than a regression
 * test: if a future change makes the walk reachable, the byte comparison below fails instead of a
 * database quietly serving stale segments.
 * </p>
 *
 * <h2>Why bytes rather than an exception</h2>
 *
 * Nothing here is asserted via a throw. Every segment is read back and its CONTENT compared against
 * what the most recent commit wrote, because the quiet form would produce no other signal — a test
 * waiting for an exception would pass straight through it.
 *
 * <p>
 * It lives in this package rather than beside the HOT-layer fixtures because the segment-slot API
 * is projection-internal. Strategy-matrix, eviction and historical-read coverage sit in
 * {@code io.sirix.page.HOTCompleteDumpMergeTest} and
 * {@code io.sirix.page.HOTTombstoneEvictionTest}, at the layer where the merge itself lives.
 * </p>
 */
final class ProjectionSegmentResurrectionTest {

  private static final String RESOURCE = "projection-segment-resurrection";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /** Measured: a ref-carrying incremental leaf split appears within this many row groups. */
  private static final int ROW_GROUPS = 2000;

  @BeforeEach
  void setUp() throws IOException {
    // The witnesses below are gated behind -Dsirix.hot.mergeDiag; with the gate off they read zero
    // and this class would fail confusingly rather than saying what is actually wrong.
    assertTrue(VersioningType.hotMergeDiagEnabled(),
        "HOT merge diagnostics are OFF, so the merge and split witnesses cannot fire. Run with "
            + "-Dsirix.hot.mergeDiag=true (the gradle test configuration sets it).");
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(VersioningType.SLIDING_SNAPSHOT).build());
    }
    VersioningType.resetFragmentMergeCounters();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * Named for what it PROVES, not for the defect it was written to hunt. The previous name —
   * {@code aResurrectedSegmentIsServedWithPreSplitBytesAndThrowsNothing} — asserted the opposite of
   * the body, which requires {@code staleBytes == 0}; a reader scanning method names would have come
   * away believing the resurrection was an established behaviour of this codebase.
   */
  @Test
  void everySegmentServesItsLatestBytesAcrossASplit() throws IOException {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {

      // Four commits: fill and split, then two that leave a SPARSE head fragment over the split
      // leaf. Distinct markers per commit, because rewriting identical bytes does not dirty the
      // page and then no fragment chain forms at all.
      writeSegments(session, ROW_GROUPS, (byte) 1);
      writeSegments(session, ROW_GROUPS, (byte) 2);
      writeSegments(session, 1, (byte) 3);
      writeSegments(session, 1, (byte) 4);

      // TWO WITNESSES, both required before any absence assertion below means anything. Starting
      // from a root leaf, the shared driver can create an indirect root only by a structural split;
      // this is a production-state witness and does not depend on a retired legacy counter.
      assertTrue(VersioningType.multiFragmentMerges() > 0,
          "the merge path was never entered, so this case proves nothing (merges="
              + VersioningType.multiFragmentMerges() + ")");

      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        assertTrue(reader.loadHOTPage(ProjectionIndexHOTStorage.rootReference(reader, 0)) instanceof HOTIndirectPage,
            "the projection remained a root leaf, so this case did not execute a structural split");
        assertTrue(ProjectionIndexHOTStorage.segmentPageOffset(reader, 0,
            ProjectionIndexHOTStorage.columnSegmentSlotKey(1, 0), 0) >= 0,
            "the split fixture must use a referenced segment, not an inline slot payload");
        int missing = 0;
        int staleBytes = 0;
        for (long rg = 1; rg <= ROW_GROUPS; rg++) {
          final byte[] got = ProjectionIndexHOTStorage.readColumnSegmentSlot(reader, 0,
              ProjectionIndexHOTStorage.columnSegmentSlotKey(rg, 0));
          // Row group 1 was rewritten by the last two commits; every other one last saw marker 2.
          final byte expected = rg == 1
              ? (byte) 4
              : (byte) 2;
          if (got == null || got.length == 0) {
            missing++;
          } else if (got[0] != expected) {
            staleBytes++;
          }
        }
        assertEquals(0, missing, "no segment may be lost across the split");
        assertEquals(0, staleBytes,
            "a segment was served carrying bytes from before the split. This is the quiet arm of "
                + "task #57: nothing throws, the store just answers with the wrong copy.");
      }

      assertEquals(0, VersioningType.completeDumpsWalkedPast(),
          "the merge must not walk past a complete dump - the mechanism behind the stale bytes above");
    }
  }

  /** One commit writing {@code count} single-column segments, each stamped with {@code marker}. */
  private static void writeSegments(final JsonResourceSession session, final int count, final byte marker) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      for (long rg = 1; rg <= count; rg++) {
        final byte[] segment = new byte[600];
        segment[0] = marker;
        segment[1] = (byte) rg;
        storage.putColumnSegmentSlot(ProjectionIndexHOTStorage.columnSegmentSlotKey(rg, 0), segment);
      }
      wtx.commit();
    }
  }
}
