/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Binna-conformance gate for the HOT trie under the projection index's key shape — the shape that
 * historically broke it.
 *
 * <p>
 * <b>The defect this pins down.</b> Slot keys are {@code (rowGroupId << 16) | slotKind}, appended
 * in ascending order. At a boundary key whose dense partial is NOVEL at the routing node (measured
 * concretely: partial {@code 110} arriving among children {@code 000..101} at row group 192),
 * subset-fallback routing landed the key in the {@code 100} child and the bespoke merge path
 * absorbed it there. One leaf then held two disjoint key ranges ({@code 128..159} and
 * {@code 192..196}) with the {@code 160..191} leaf BETWEEN them — every range scan returned row
 * groups out of key order, and any bounded scan whose upper bound fell inside the displaced span
 * silently truncated. In Binna's HOT this cannot happen: a mismatch bit at or above an ancestor's
 * discriminative bit always branches ({@code insertNewValueIntoNode}); it never merges.
 *
 * <p>
 * The properties asserted here are the thesis's, stated operationally:
 * <ol>
 * <li><b>Scan order</b> — an unbounded range scan yields every key exactly once, strictly ascending
 * (I8 ≡ I7 under complete masks).</li>
 * <li><b>Bounded-scan completeness</b> — with the DEFAULT early-exit enabled, a bounded scan
 * returns exactly the keys in its window, for cut points chosen to fall inside the span the
 * historical defect displaced.</li>
 * <li><b>I12 (subtree-ranges-disjoint)</b> — each child subtree owns a contiguous, non-overlapping,
 * ascending key span; the validator invariant added for this defect.</li>
 * <li><b>Segment references survive</b> — the side map rides the leaf holding its owning slot
 * across whatever structural operations the build performed.</li>
 * </ol>
 */
@DisplayName("HOT Binna conformance — projection key shape")
final class HOTBinnaConformanceTest {

  private static final String RESOURCE_NAME = "hot-binna-conformance";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  /**
   * Row groups written. 300 comfortably crosses the historical failure boundary (~160, first
   * novel-partial event at 192) and leaves room for the further boundaries past it.
   */
  private static final int ROW_GROUPS = 300;
  private static final int SLOT_KINDS = 9;

  private static long slotKey(final long rowGroupId, final int slotKind) {
    return (rowGroupId << 16) | slotKind;
  }

  private static byte[] keyBytes(final long slotKey) {
    final byte[] out = new byte[8];
    PathKeySerializer.INSTANCE.serialize(slotKey, out, 0);
    return out;
  }

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("Ascending appends with per-row-group slots build a lex-ordered, complete, I12-clean trie")
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void ascendingAppendsStayLexOrderedAndComplete() throws IOException {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        final byte[] value = new byte[16];
        for (long rg = 1; rg <= ROW_GROUPS; rg++) {
          for (int kind = 0; kind < SLOT_KINDS; kind++) {
            value[0] = (byte) rg;
            value[1] = (byte) kind;
            storage.writeSlotValue(slotKey(rg, kind), value.clone());
          }
        }
        wtx.commit();
      }

      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
        assertNotNull(root, "projection HOT root must exist after commit");

        // (1) Unbounded scan: strictly ascending, and exactly the written key set.
        final List<Long> scanned = collectSlotKeys(reader, root, null, null);
        assertEquals(ROW_GROUPS * SLOT_KINDS, scanned.size(),
            "unbounded scan must yield every written key exactly once");
        long expected = slotKey(1, 0);
        int kindCursor = 0;
        long rgCursor = 1;
        for (final long got : scanned) {
          assertEquals(expected, got,
              "scan order diverges from ascending key order at rowGroup " + rgCursor + " slotKind " + kindCursor);
          kindCursor++;
          if (kindCursor == SLOT_KINDS) {
            kindCursor = 0;
            rgCursor++;
          }
          expected = slotKey(rgCursor, kindCursor);
        }

        // (2) Bounded scans, DEFAULT early-exit: cut points straddling the historical
        // displacement (159/192) and later power-of-two boundaries.
        for (final int cut : new int[] {40, 100, 170, 200, 260}) {
          final List<Long> bounded =
              collectSlotKeys(reader, root, keyBytes(slotKey(1, 0)), keyBytes(slotKey(cut, SLOT_KINDS - 1)));
          assertEquals(cut * SLOT_KINDS, bounded.size(), "bounded scan [1.." + cut
              + "] is short — an out-of-order page made the " + "early exit fire before all in-range keys were seen");
        }

        // (3) The validator, including I12 (subtree-ranges-disjoint).
        final HOTInvariantValidator.Result result = HOTInvariantValidator.validate(root, reader);
        result.assertOk();
      }
    }
  }

  @Test
  @DisplayName("Segment references follow their owning slot through boundary-key structural operations")
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void segmentReferencesSurviveStructuralOperations() throws IOException {
    final int subId = 1;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        final byte[] value = new byte[16];
        final byte[] segment = new byte[64];
        // Interleave exactly like the real build: descriptor slot first, then its segment ref —
        // so refs are present on the leaves BEFORE the boundary keys trigger branch/rebuild.
        for (long rg = 1; rg <= ROW_GROUPS; rg++) {
          for (int kind = 0; kind < SLOT_KINDS; kind++) {
            value[0] = (byte) rg;
            storage.writeSlotValue(slotKey(rg, kind), value.clone());
          }
          segment[0] = (byte) rg;
          storage.putSegmentPage(slotKey(rg, 0), subId, segment.clone());
        }
        wtx.commit();
      }

      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
        assertNotNull(root, "projection HOT root must exist after commit");
        int missing = 0;
        final StringBuilder detail = new StringBuilder();
        try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
          for (long rg = 1; rg <= ROW_GROUPS; rg++) {
            final byte[] ownerKey = keyBytes(slotKey(rg, 0));
            final HOTLeafPage leaf = trieReader.navigateToLeaf(root, ownerKey);
            final long refKey = HOTLeafPage.overflowPageRefKey(slotKey(rg, 0), subId);
            if (leaf == null || leaf.findEntry(ownerKey) < 0 || leaf.getPageReference(refKey) == null) {
              missing++;
              if (missing <= 5) {
                detail.append("\nrowGroup ")
                      .append(rg)
                      .append(leaf == null
                          ? " (no leaf)"
                          : " (slot or ref absent)");
              }
            }
          }
        }
        assertEquals(0, missing, "segment references orphaned from their owning slot — a "
            + "structural operation rebuilt leaves without carrying the side map:" + detail);
      }
    }
  }

  /**
   * Range-scan {@code [fromKey, toKey]} (null = unbounded) over {@code root}, returning the decoded
   * slot keys in VISIT order — the assertion input for both order and completeness.
   */
  private static List<Long> collectSlotKeys(final StorageEngineReader reader, final PageReference root,
      final byte @org.jspecify.annotations.Nullable [] fromKey,
      final byte @org.jspecify.annotations.Nullable [] toKey) {
    final List<Long> out = new ArrayList<>();
    try (HOTTrieReader trieReader = new HOTTrieReader(reader);
        HOTRangeCursor cursor = trieReader.range(root, fromKey, toKey)) {
      while (cursor.hasNext()) {
        final long slotKey = cursor.currentLeafPage().decodeKey8BE(cursor.currentEntryIndex()) ^ 0x8000_0000_0000_0000L;
        out.add(slotKey);
        cursor.advance();
      }
    }
    return out;
  }
}
