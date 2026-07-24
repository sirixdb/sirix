/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3 suite: descriptor-layout storage (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §9 P3) —
 * putRowGroup/getRowGroup/readRowGroup/readAllRowGroups/tombstoneRowGroup/putBlob over real commits. The
 * decisive test is {@code singleColumnChangeSharesEverySegmentButOne}: the storage-level
 * proof of the SLIDING_SNAPSHOT containment claim (§2.5), asserted on the segment pages'
 * durable offset keys across revisions.
 */
final class ProjectionIndexDescriptorStorageTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  private static final byte[] KINDS = {
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
  };

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops"};

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** Deterministic leaf; {@code ageBump} shifts only column 0's values (single-column edit). */
  private static byte[] rawLeaf(final int rows, final long keyBase, final long ageBump) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random rng = new Random(keyBase);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = new boolean[3];
    final boolean[] unrep = new boolean[3];
    final boolean[] nonIntegral = new boolean[3];
    long key = keyBase;
    for (int i = 0; i < rows; i++) {
      key += 4 + rng.nextInt(5);
      longs[0] = 18 + rng.nextInt(48) + ageBump;
      bools[1] = rng.nextBoolean();
      strings[2] = DEPTS[rng.nextInt(DEPTS.length)];
      present[0] = true;
      present[1] = true;
      present[2] = true;
      Arrays.fill(unrep, false);
      Arrays.fill(nonIntegral, false);
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page.serialize();
  }

  private static long segmentDiskKey(final JsonNodeReadOnlyTrx rtx, final long rowGroupId, final int columnSegmentId) {
    // Observable identity of a committed segment page: its durable offset key. Equal keys
    // across revisions prove the page was SHARED by reference (the CoW carry-forward no-op),
    // not merely rewritten with identical bytes.
    final long offset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
        INDEX_NUMBER, rowGroupId, columnSegmentId);
    assertTrue(offset >= 0, "segment " + columnSegmentId + " must exist and be resolved, offset=" + offset);
    return offset;
  }

  @Test
  void putGetReadRoundTripAcrossCommit() {
    final byte[] raw = rawLeaf(700, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroup(1, raw);
        assertArrayEquals(raw, storage.getRowGroup(1), "same-trx readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "cold-reopen readback");
      }
    }
  }

  @Test
  void segmentSlotLayoutRoundTripAndPruning() {
    // EXPLORATORY segment ⇔ slot layout: every segment is its own HOT slot (zone-map descriptor at
    // slotKind 0). Proves byte-identical assembly same-trx and cold-reopen, across two row groups
    // whose composite keys must not collide, descriptor-only row count (no segment reads), and a
    // per-group tombstone that leaves the other group byte-identical.
    final byte[] rawA = rawLeaf(700, 10_000L, 0);
    final byte[] rawB = rawLeaf(512, 90_000L, 3);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rawA));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(rawB));
        assertArrayEquals(rawA, storage.getRowGroupFromColumnSegmentSlots(1), "same-trx group 1");
        assertArrayEquals(rawB, storage.getRowGroupFromColumnSegmentSlots(2), "same-trx group 2");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertArrayEquals(rawA, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "cold-reopen group 1 byte-identical");
        assertArrayEquals(rawB, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 2),
            "cold-reopen group 2 byte-identical");
        assertEquals(700, ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "descriptor-only rowCount group 1 (no segment reads)");
        assertEquals(512, ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(r, INDEX_NUMBER, 2),
            "descriptor-only rowCount group 2");
        // F1: the stored descriptor must be zone-map-only — every segment lives in its own slot,
        // none inline in the descriptor (else it would be double-stored and read from the descriptor).
        final byte[] desc = ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
        assertNotNull(desc);
        assertTrue(RowGroupDescriptor.isDescriptor(desc));
        for (int i = 0; i < RowGroupDescriptor.columnSegmentCount(desc); i++) {
          assertFalse(RowGroupDescriptor.entryIsInline(desc, i),
              "segment-slot descriptor must be zone-map-only (no inline entries)");
        }
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.tombstoneRowGroupAsColumnSegmentSlots(1);
        assertNull(storage.getRowGroupFromColumnSegmentSlots(1), "same-trx tombstoned group 1 gone");
        assertArrayEquals(rawB, storage.getRowGroupFromColumnSegmentSlots(2), "group 2 intact after group 1 tombstone");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertNull(ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "committed tombstone group 1 gone");
        assertArrayEquals(rawB, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 2),
            "group 2 still byte-identical after group 1 tombstone");
      }
    }
  }

  @Test
  void segmentSlotsAreBareWithNoRedundantOnDiskHash() {
    // Write-side format proof: a segment slot carries NO blob marker and NO on-disk hash — its byteLen
    // and XXH3 content hash live in the descriptor entry, re-checked by verifyColumnSegment at assembly. So
    // the SAME raw bytes round-trip through the bare segment reader, but the blob reader REJECTS a
    // segment slot (no PIXB magic → not a blob), proving the redundant 17-byte hashed marker is gone.
    // The descriptor slot, by contrast, stays a hashed blob — nothing else backs its integrity.
    final int keysSeg = 0; // KEYS(0)
    final int body0Seg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0); // large row count → referenced
    final byte[] raw = rawLeaf(400, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(raw));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "bare segment assembly round-trips byte-identical");
        for (final int columnSegmentId : new int[] {keysSeg, body0Seg}) {
          final long slotKey = ProjectionIndexHOTStorage.columnSegmentSlotKey(1, columnSegmentId);
          assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(r, INDEX_NUMBER, slotKey),
              "segment " + columnSegmentId + " reads back through the bare reader");
          assertThrows(IllegalStateException.class,
              () -> ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER, slotKey),
              "segment " + columnSegmentId + " slot carries no blob marker/hash — the blob reader must reject it");
        }
        assertNotNull(ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1)),
            "the descriptor slot stays a hashed blob (nothing else backs its integrity)");
      }
    }
  }

  @Test
  void segmentSlotRePutSharesUnchangedSegmentPagesAndRewritesChanged() {
    // Per-segment carry-forward at slot granularity: an identical re-put is a no-op (the referenced
    // BODY(0) page keeps its offset), and a real column-0 change rewrites exactly that segment's page.
    final int body0Seg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    final byte[] v1 = rawLeaf(900, 50_000L, 0);
    final byte[] v2 = rawLeaf(900, 50_000L, 1); // same keys, only column 0's values bump
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(v1));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) { // rev2: identical re-put → no-op share
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(v1));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) { // rev3: column 0 changed → BODY(0) rewritten
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(v2));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
           JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2);
           JsonNodeReadOnlyTrx r3 = session.beginNodeReadOnlyTrx(3)) {
        assertArrayEquals(v1, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r1.getStorageEngineReader(), INDEX_NUMBER, 1), "rev1 byte-identical");
        assertArrayEquals(v1, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r2.getStorageEngineReader(), INDEX_NUMBER, 1), "rev2 (identical re-put) byte-identical");
        assertArrayEquals(v2, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r3.getStorageEngineReader(), INDEX_NUMBER, 1), "rev3 byte-identical");
        final long body0Key = ProjectionIndexHOTStorage.columnSegmentSlotKey(1, body0Seg);
        final long off1 = ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        final long off2 = ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        final long off3 = ProjectionIndexHOTStorage.segmentPageOffset(r3.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        assertTrue(off1 >= 0, "BODY(0) is a referenced (large) segment with a page");
        assertEquals(off1, off2, "identical re-put shares the unchanged BODY(0) page (carry-forward)");
        assertNotEquals(off2, off3, "column-0 change rewrites the BODY(0) segment page");
      }
    }
  }

  @Test
  void segmentSlotReadAllLeavesEnumeratesByteIdenticalAndLoudOnGap() {
    final byte[] l1 = rawLeaf(400, 10_000L, 0);
    final byte[] l2 = rawLeaf(600, 40_000L, 1);
    final byte[] l3 = rawLeaf(0, 80_000L, 0); // empty tail leaf
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        storage.putRowGroupAsColumnSegmentSlots(3, ProjectionIndexColumnSegmentCodec.encode(l3));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(r, INDEX_NUMBER, 3);
        assertEquals(3, all.size());
        assertArrayEquals(l1, all.get(0), "leaf 1 byte-identical");
        assertArrayEquals(l2, all.get(1), "leaf 2 byte-identical");
        assertArrayEquals(l3, all.get(2), "empty tail leaf 3 byte-identical");
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(r, INDEX_NUMBER, 4),
            "loud on a missing leaf (contiguity)");
      }
    }
  }

  @Test
  void segmentSlotEnumeratesTinyInlineLeavesByteIdentical() {
    // Serving fixtures use 5-row leaves whose segments are all <512 bytes → INLINE blobs; the
    // referenced-segment fixtures (400+ rows) never exercise that path. Round-trip tiny leaves
    // through the range-scan enumerator to prove inline assembly is byte-identical.
    final byte[] l1 = rawLeaf(3, 10_000L, 0);
    final byte[] l2 = rawLeaf(5, 40_000L, 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 2);
        assertEquals(2, all.size(), "both tiny inline leaves enumerated");
        assertArrayEquals(l1, all.get(0), "tiny inline leaf 1 byte-identical");
        assertArrayEquals(l2, all.get(1), "tiny inline leaf 2 byte-identical");
      }
    }
  }

  @Test
  void segmentSlotEnumeratesUncommittedReferencedLeavesInWalk() {
    // Uncommitted (this-transaction) referenced segments are swizzled, unflushed pages with no
    // durable offset — the coalesced batch path (which resolves by offset) cannot see them. The
    // walk must resolve such refs in-walk through their live reference, exactly as the descriptor
    // path's readAllRowGroups does, so a same-transaction build-then-read still serves. Large leaves
    // force referenced (not inline) segments; the read happens BEFORE commit.
    final byte[] l1 = rawLeaf(400, 10_000L, 0);
    final byte[] l2 = rawLeaf(600, 40_000L, 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        // Read through the writer's OWN reader, still uncommitted — the refs are unresolved here.
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            wtx.getStorageEngineWriter(), INDEX_NUMBER, 2);
        assertEquals(2, all.size(), "uncommitted referenced leaves enumerate in-walk");
        assertArrayEquals(l1, all.get(0), "uncommitted referenced leaf 1 byte-identical");
        assertArrayEquals(l2, all.get(1), "uncommitted referenced leaf 2 byte-identical");
        wtx.commit();
      }
      // And after commit the same bytes come back through the coalesced batch path.
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 2);
        assertArrayEquals(l1, all.get(0), "committed referenced leaf 1 byte-identical");
        assertArrayEquals(l2, all.get(1), "committed referenced leaf 2 byte-identical");
      }
    }
  }

  @Test
  void segmentSlotEnumerationSkipsFenceAndMetadataBlobs() {
    // In the segment-slot layout EVERY slot is a blob — including the slot-0 metadata (PIXM) and the
    // fence chunks at CHUNK_SLOT_BASE (2^40). The range-scan enumerator distinguishes leaf slots by
    // key; a real store carries both companions, so writing them here proves the walk does not
    // mis-read a fence/metadata blob as a leaf descriptor (which would throw on validate()).
    final byte[] l1 = rawLeaf(3, 10_000L, 0);
    final byte[] l2 = rawLeaf(400, 40_000L, 1); // large → referenced segments alongside the fences
    final ProjectionIndexMetadata meta = new ProjectionIndexMetadata("/[]",
        new String[] {"/[]/age", "/[]/active", "/[]/dept"}, new String[] {"age", "active", "dept"},
        KINDS, 2, 1).withColumnSegmentSlotLayout();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        storage.putBlob(0, meta.serialize());
        // Fence chunks over the two leaves' record-key zones — exactly what finishPersist writes.
        ProjectionIndexFences.write(storage, 2, new long[] {10_001L, 40_001L},
            new long[] {39_999L, 60_000L}, 0);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 2);
        assertEquals(2, all.size(), "fence + metadata blobs must not be counted as leaves");
        assertArrayEquals(l1, all.get(0), "leaf 1 byte-identical despite fence/metadata companions");
        assertArrayEquals(l2, all.get(1), "leaf 2 byte-identical despite fence/metadata companions");
        // The auto-layout entry point (which parses the metadata blob) dispatches identically.
        final List<byte[]> auto = ProjectionIndexHOTStorage.readAllRowGroupsAutoLayout(
            rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(2, auto.size(), "auto-layout enumerates exactly the leaves");
        assertArrayEquals(l2, auto.get(1), "auto-layout leaf 2 byte-identical");
      }
    }
  }

  @Test
  void segmentSlotEnumerationIsLoudOnALeakedOrphanBeyondLeafCount() {
    // Invariant parity with the descriptor path (sumLiveDescriptorRows' full-range scan catches a
    // live slot past rowGroupCount): a rebuild bug that leaves a live descriptor at rowGroupCount+1 must be
    // LOUD in the segment-slot readers too, not silently tolerated. Here three leaves are live but
    // rowGroupCount is 2 — leaf 3 is the leaked orphan the upper-probe must catch.
    final byte[] l1 = rawLeaf(400, 10_000L, 0);
    final byte[] l2 = rawLeaf(600, 40_000L, 1);
    final byte[] orphan = rawLeaf(100, 80_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        storage.putRowGroupAsColumnSegmentSlots(3, ProjectionIndexColumnSegmentCodec.encode(orphan));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(r, INDEX_NUMBER, 2),
            "enumeration must be loud on a live descriptor beyond rowGroupCount (leaked orphan)");
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.sumRowsFromColumnSegmentSlots(r, INDEX_NUMBER, 2),
            "row-count sum must be loud on a leaked orphan too");
        // Control: with the honest rowGroupCount (3) both readers succeed — the probe only fires on a
        // genuine orphan, never on the last legitimate leaf.
        assertEquals(3, ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(r, INDEX_NUMBER, 3).size(),
            "the honest rowGroupCount enumerates cleanly");
        assertEquals(1100, ProjectionIndexHOTStorage.sumRowsFromColumnSegmentSlots(r, INDEX_NUMBER, 3),
            "the honest rowGroupCount sums cleanly");
      }
    }
  }

  @Test
  void readAllLeavesAutoLayoutDispatchesToSegmentSlotReaderByMetadataFlag() {
    // F2 discriminator: the slot-0 metadata's FLAG_COLUMN_SEGMENT_SLOT_LAYOUT tells the catalog to read a
    // segment-slot sub-tree with the segment-slot reader (not the descriptor-layout reader, which
    // would skip the blob descriptor slots and see zero leaves).
    final byte[] a = rawLeaf(400, 10_000L, 0);
    final byte[] b = rawLeaf(300, 40_000L, 1);
    final ProjectionIndexMetadata segMeta = new ProjectionIndexMetadata("/[]",
        new String[] {"/[]/age", "/[]/active", "/[]/dept"}, new String[] {"age", "active", "dept"},
        KINDS, 2, 1).withColumnSegmentSlotLayout();
    assertTrue(segMeta.isColumnSegmentSlotLayout());
    // Stale is checked before layout: a stale segment-slot store reads empty regardless of layout.
    final ProjectionIndexMetadata staleSeg = ProjectionIndexMetadata.staleTombstone().withColumnSegmentSlotLayout();
    assertTrue(staleSeg.isStale() && staleSeg.isColumnSegmentSlotLayout(), "stale + segment-slot representable");
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(a));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(b));
        storage.putBlob(0, segMeta.serialize());
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // Metadata round-trips the layout flag.
        assertTrue(ProjectionIndexMetadata.parse(
            ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L))
            .isColumnSegmentSlotLayout(), "layout flag persisted");
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsAutoLayout(
            rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(2, all.size(), "auto-layout dispatched to the segment-slot reader");
        assertArrayEquals(a, all.get(0), "leaf 1 byte-identical via dispatch");
        assertArrayEquals(b, all.get(1), "leaf 2 byte-identical via dispatch");
        // Negative control: the descriptor-layout reader CANNOT read a segment-slot store — its bare
        // segment slots (leading discriminator byte, no descriptor/blob magic) trip the mixed-layout
        // guard — so the layout dispatch is genuinely necessary, not cosmetic.
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(), INDEX_NUMBER),
            "descriptor-layout readAllRowGroups must reject a segment-slot store — dispatch required");
      }
    }
  }

  @Test
  void segmentSlotShrinkTombstonesVanishedSegments() {
    // A row group that loses a segment (here: shrink to an empty leaf, which drops the DICT) must
    // tombstone exactly the vanished segment slot, and the empty leaf must round-trip.
    final int dictSeg = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2);
    final byte[] full = rawLeaf(300, 70_000L, 0); // 3 columns incl. a string DICT
    final byte[] empty = rawLeaf(0, 70_000L, 0);  // rowCount 0 → no DICT segment
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(full));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(rtx.getStorageEngineReader(), INDEX_NUMBER,
            ProjectionIndexHOTStorage.columnSegmentSlotKey(1, dictSeg)), "DICT slot present before shrink");
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(empty));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertArrayEquals(empty, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "empty leaf round-trips byte-identical");
        assertEquals(0, ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(r, INDEX_NUMBER, 1));
        assertNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(r, INDEX_NUMBER,
            ProjectionIndexHOTStorage.columnSegmentSlotKey(1, dictSeg)), "DICT slot tombstoned on shrink");
      }
    }
  }

  @Test
  void singleColumnChangeSharesEverySegmentButOne() {
    // This test is about page-level carry-forward sharing: identical durable OFFSET keys across
    // revisions prove a segment PAGE was shared by reference. That is only observable for
    // REFERENCED segments — inline segments carry no page — so pin the referenced model here.
    // (The hybrid's inline sharing rides the descriptor and is covered by the codec round trips.)
    ProjectionIndexColumnSegmentCodec.setInlinePolicyForTesting(0, 0);
    try {
    final byte[] v1 = rawLeaf(900, 50_000L, 0);
    final byte[] v2 = rawLeaf(900, 50_000L, 1); // only column 0's values differ
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, v1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, v2);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
           JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        // Revision isolation: each revision assembles its own bytes.
        assertArrayEquals(v1, ProjectionIndexHOTStorage.readRowGroup(r1.getStorageEngineReader(), INDEX_NUMBER, 1));
        assertArrayEquals(v2, ProjectionIndexHOTStorage.readRowGroup(r2.getStorageEngineReader(), INDEX_NUMBER, 1));
        // Containment: KEYS, BODY(1), BODY(2), DICT(2) identical across revisions
        // (shared by the hash no-op); BODY(0) differs.
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()),
            segmentDiskKey(r2, 1, ProjectionIndexColumnSegmentCodec.keysColumnSegmentId()));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1)),
            segmentDiskKey(r2, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1)));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2)),
            segmentDiskKey(r2, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2)));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)),
            segmentDiskKey(r2, 1, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)));
        assertNotEquals(segmentDiskKey(r1, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)),
            segmentDiskKey(r2, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)),
            "the edited column's BODY must be a new page");
      }
    }
    } finally {
      ProjectionIndexColumnSegmentCodec.clearInlinePolicyForTesting();
    }
  }

  @Test
  void smallSegmentsInlineIntoDescriptorNoPageButRoundTrip() {
    // Under the default hybrid thresholds the tiny DICT(2) (8 short departments) is inlined into
    // the descriptor slot — no side-map page is written for it — yet the leaf still reads back
    // byte-identically across a cold reopen, and a large referenced segment still carries a page.
    final byte[] raw = rawLeaf(900, 60_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, raw);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final long dictOffset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2));
        assertTrue(dictOffset < 0, "small DICT(2) must be inline (no page), got offset=" + dictOffset);
        final long bodyOffset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
        assertTrue(bodyOffset >= 0, "large BODY(0) must still be a referenced page, got offset=" + bodyOffset);
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "cold-reopen readback with a mix of inline and referenced segments");
      }
    }
  }

  @Test
  void shrinkGrowShrinkNeverResurrectsStaleSegments() {
    // 3-column leaf → 1-column leaf (BODY(1)/BODY(2)/DICT(2) refs must vanish) → back to 3.
    final byte[] wide1 = rawLeaf(300, 7_000L, 0);
    final byte[] narrow;
    {
      final ProjectionIndexRowGroupPage page =
          new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
      final long[] longs = new long[1];
      final boolean[] bools = new boolean[1];
      final String[] strings = new String[1];
      final boolean[] present = {true};
      final boolean[] unrep = new boolean[1];
      final boolean[] nonIntegral = new boolean[1];
      for (int i = 0; i < 100; i++) {
        longs[0] = i;
        assertTrue(page.appendRow(7_000L + i, longs, bools, strings, present, unrep, nonIntegral));
      }
      narrow = page.serialize();
    }
    final byte[] wide2 = rawLeaf(300, 8_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, wide1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, narrow);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(narrow, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "narrow leaf must assemble without stale wide segments");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2)),
            "vanished column's BODY ref must be removed");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)),
            "vanished column's DICT ref must be removed");
      }
      // Time travel still serves the wide revision.
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertArrayEquals(wide1, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1));
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, wide2);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(wide2, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "grow-back must serve fresh segments, not resurrected ones");
      }
    }
  }

  @Test
  void tombstoneVersusLiveEmptyLeaf() {
    final byte[] empty = new ProjectionIndexRowGroupPage(KINDS).serialize();
    final byte[] full = rawLeaf(50, 3_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroup(1, empty);   // live empty leaf
        storage.putRowGroup(2, full);
        storage.tombstoneRowGroup(2);    // tombstoned leaf
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(empty, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "live empty leaf reads as its raw empty form, NOT as absent");
        assertNull(ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(), INDEX_NUMBER, 2),
            "tombstoned leaf reads as absent");
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(1, all.size(), "readAllRowGroups includes the live empty leaf, skips the tombstone");
        assertArrayEquals(empty, all.get(0));
      }
    }
  }

  @Test
  void readAllLeavesParityAcrossSplits() {
    final int numLeaves = 220;
    final byte[][] raws = new byte[numLeaves][];
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          raws[i] = rawLeaf(400, 100_000L * (i + 1), 0);
          storage.putRowGroup(i + 1, raws[i]);
        }
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size());
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(raws[i], all.get(i), "leaf " + (i + 1) + " parity");
          assertArrayEquals(raws[i], ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
              INDEX_NUMBER, i + 1), "point read parity for leaf " + (i + 1));
        }
      }
    }
  }

  @Test
  void metadataSizedBlobRoundTripsAtSlotZero() {
    final byte[] metadata = new byte[1_500_000]; // ~97k leaves × 16-byte fences
    new Random(11).nextBytes(metadata);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, metadata);
        storage.putRowGroup(1, rawLeaf(100, 500L, 0));
        assertArrayEquals(metadata, storage.getBlob(0), "same-trx blob readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(metadata, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0), "cold blob readback with hash verification");
        // The blob slot must not surface in leaf enumeration.
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(1, all.size());
      }
    }
  }

  @Test
  void blobTombstoneRemovesTheSegmentRefAndCarryForwardSharesThePage() {
    final byte[] metadata = new byte[200_000];
    new Random(13).nextBytes(metadata);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putBlob(0, metadata);
        wtx.commit();
      }
      // Unchanged re-put must share the blob's segment page by reference (carry-forward).
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, metadata);
        storage.putRowGroup(1, rawLeaf(20, 900L, 0)); // dirty something so the commit is non-empty
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
           JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        final long off1 = ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0);
        final long off2 = ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0);
        assertTrue(off1 >= 0);
        assertEquals(off1, off2, "unchanged blob must be shared by reference, not rewritten");
      }
      // Tombstoning the blob slot must remove its segment ref — not leak the MB-scale page
      // into every future fragment.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).tombstoneRowGroup(0);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0), "blob segment ref must be removed by the tombstone");
      }
    }
  }

  @Test
  void smallBlobStoresInlineWithoutAnOverflowPage() {
    final byte[] meta = new byte[200]; // ≤ BLOB_INLINE_MAX → inline in the slot value
    new Random(21).nextBytes(meta);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, meta);
        storage.putRowGroup(1, rawLeaf(50, 300L, 0));
        assertArrayEquals(meta, storage.getBlob(0), "same-trx inline blob readback");
        assertNull(storage.getSegmentPageBytes(0, 0), "an inline blob writes no segment page");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(meta, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0), "cold inline blob readback with hash verification");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0), "no page exists for an inline blob");
        // The inline blob slot must not surface in leaf enumeration.
        assertEquals(1, ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(),
            INDEX_NUMBER).size());
      }
    }
  }

  @Test
  void blobMigratesBetweenReferencedAndInlineDroppingStalePages() {
    final byte[] big = new byte[2000]; // > BLOB_INLINE_MAX → referenced (OverflowPage)
    new Random(22).nextBytes(big);
    final byte[] small = new byte[100]; // ≤ BLOB_INLINE_MAX → inline
    new Random(23).nextBytes(small);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putBlob(0, big);
        wtx.commit();
      }
      // Referenced → inline: the migration must drop the now-orphaned page.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, small);
        storage.putRowGroup(1, rawLeaf(10, 700L, 0));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(small, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0));
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0), "referenced→inline must drop the stale page");
      }
      // Inline → referenced: the migration must (re)create the page.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, big);
        storage.putRowGroup(2, rawLeaf(10, 900L, 0));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(big, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0));
        assertNotNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0, 0), "inline→referenced must create a page");
      }
    }
  }

  /** Deterministic pseudo-payload for the legacy chunked layout. */
  private static byte[] legacyChunkBytes(final int slot, final int size) {
    final byte[] bytes = new byte[size];
    new Random(0xC0FFEEL ^ slot).nextBytes(bytes);
    return bytes;
  }

  /** Fixed chunk size of the removed pre-redesign (v1, chunked) storage layout. */
  private static final int LEGACY_CHUNK_SIZE = 4096;

  /**
   * Fabricates the PRE-redesign chunked layout byte-for-byte: the payload is split into
   * 4096-byte chunks and each chunk is written at the raw composite key
   * {@code (rowGroupId << 8) | chunkIdx} via the package-private {@code writeSlotValue} seam,
   * which serializes the raw long key with {@code PathKeySerializer} — identical bytes to the
   * removed legacy put path. The legacy metadata payload at slot 0 is composite key 0.
   */
  private static void writeLegacyChunkedLeaf(final ProjectionIndexHOTStorage storage,
      final long rowGroupId, final byte[] payload) {
    final int chunks = Math.max(1, (payload.length + LEGACY_CHUNK_SIZE - 1) / LEGACY_CHUNK_SIZE);
    for (int chunkIdx = 0; chunkIdx < chunks; chunkIdx++) {
      final int off = chunkIdx * LEGACY_CHUNK_SIZE;
      final int len = Math.min(LEGACY_CHUNK_SIZE, payload.length - off);
      storage.writeSlotValue((rowGroupId << 8) | chunkIdx, Arrays.copyOfRange(payload, off, off + len));
    }
  }

  /**
   * Reader-side reassembly of a legacy chunked leaf (what the removed {@code readOne} did):
   * concatenate chunk slots until a missing/partial chunk ends the payload.
   */
  private static byte[] readLegacyChunkedLeaf(final StorageEngineReader reader, final long rowGroupId) {
    final PageReference rootRef = ProjectionIndexHOTStorage.rootReference(reader, INDEX_NUMBER);
    assertNotNull(rootRef, "projection sub-tree must exist");
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = new byte[8];
      byte[] out = new byte[0];
      for (int chunkIdx = 0; chunkIdx < 256; chunkIdx++) {
        PathKeySerializer.INSTANCE.serialize((rowGroupId << 8) | chunkIdx, keyBuf, 0);
        final MemorySegment slice = trieReader.get(rootRef, keyBuf);
        if (slice == null || slice.byteSize() == 0) {
          break;
        }
        final int n = (int) slice.byteSize();
        final int prior = out.length;
        out = Arrays.copyOf(out, prior + n);
        MemorySegment.copy(slice, ValueLayout.JAVA_BYTE, 0, out, prior, n);
        if (n < LEGACY_CHUNK_SIZE) {
          break; // partial chunk = final chunk
        }
      }
      return out;
    }
  }

  /**
   * The §6 migration path: a rebuild over a v1 (chunked) store must reset the sub-tree —
   * selectively clearing is impossible (composite chunk keys would poison descriptor
   * enumeration with mixed-layout errors forever).
   */
  @Test
  void rebuildOverLegacyChunkedStoreResetsTheSubtree() {
    final byte[] fresh = rawLeaf(120, 44_000L, 0);
    final byte[] metadata = new byte[4_096];
    new Random(17).nextBytes(metadata);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      // Revision 1: a legacy chunked store — metadata chunks at slot 0, leaves at 1..N
      // (composite keys). The pre-redesign layout is fabricated byte-for-byte through the
      // writeSlotValue seam since the legacy put API no longer exists.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writeLegacyChunkedLeaf(storage, 0, metadata);
        writeLegacyChunkedLeaf(storage, 1, legacyChunkBytes(1, 6_000));
        writeLegacyChunkedLeaf(storage, 2, legacyChunkBytes(2, 6_000));
        wtx.commit();
      }
      // Revision 2: the migration flow — slot 0 is unreadable as a blob (legacy), the
      // sub-tree is reset, and a fresh descriptor-layout build lands.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertThrows(IllegalStateException.class, () -> storage.getBlob(0),
            "legacy slot-0 payload must be unreadable as a blob");
        storage.resetTree();
        storage.putRowGroup(1, fresh);
        storage.putBlob(0, metadata);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // No mixed-layout error: the legacy chunks are gone with the old tree.
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(1, all.size(), "only the fresh descriptor leaf must be enumerated");
        assertArrayEquals(fresh, all.get(0));
        assertArrayEquals(metadata, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0));
      }
      // Time travel: revision 1 still serves the legacy chunked view.
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertArrayEquals(legacyChunkBytes(1, 6_000),
            readLegacyChunkedLeaf(rtx.getStorageEngineReader(), 1));
      }
    }
  }

  @Test
  void putLeafRejectsNullAndMixedLayoutFailsLoudly() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertThrows(IllegalArgumentException.class, () -> storage.putRowGroup(1, null));
        // The two layouts use disjoint key spaces except where the legacy composite key
        // (rowGroupId << 8 | chunkIdx) collides with a plain descriptor slot key — legacy leaf 0
        // chunk 0 encodes exactly like descriptor slot 0. A descriptor read of such a slot must
        // fail loudly (mixed layouts are a migration bug), never silently misparse.
        writeLegacyChunkedLeaf(storage, 0, rawLeaf(50, 1L, 0));
        assertThrows(IllegalStateException.class, () -> storage.getRowGroup(0));
        wtx.rollback();
      }
    }
  }

  @Test
  void emptyStoreReadsAsAbsent() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(ProjectionIndexHOTStorage.readAllRowGroups(rtx.getStorageEngineReader(),
            INDEX_NUMBER).isEmpty(), "no projection sub-tree installed → empty enumeration");
        assertNull(ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "point read on an empty store must be absent");
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0),
            "blob read on an empty store must be absent");
      }
    }
  }

  // ── Wide row groups: > 84 columns (the pre-widening 8-bit sub-id cap) ────────────────────────
  // With 150 columns the highest BODY segment id is 3·149+1 = 448 > 255, so every composite key that
  // carries a segment id — the descriptor's per-entry id field, the HOT side-map sub-id
  // (overflowPageRefKey), and the segment-slot slotKind — must hold 16 bits. Before the widening
  // both writes threw MAX_COLUMNS=84 / "out of range for the side-map".

  private static final int WIDE_COLS = 150;

  /** A wide all-long leaf: {@code cols} NUMERIC_LONG columns × {@code rows} rows of random values. */
  private static byte[] wideRawLeaf(final int rows, final int cols) {
    final byte[] kinds = new byte[cols];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG);
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[cols];
    final boolean[] bools = new boolean[cols];
    final String[] strings = new String[cols];
    final boolean[] present = new boolean[cols];
    final boolean[] unrep = new boolean[cols];
    final boolean[] nonIntegral = new boolean[cols];
    Arrays.fill(present, true);
    final Random rng = new Random(0xC0FFEEL);
    long key = 1_000L;
    for (int i = 0; i < rows; i++) {
      key += 1 + rng.nextInt(4);
      for (int c = 0; c < cols; c++) {
        // High-entropy distinct values so each BODY segment spills to a referenced OverflowPage,
        // exercising overflowPageRefKey with a columnSegmentId past the old 255 ceiling.
        longs[c] = rng.nextLong();
      }
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page.serialize();
  }

  @Test
  void descriptorLayoutSupportsMoreThan84Columns() {
    assertTrue(RowGroupDescriptor.MAX_COLUMNS > 84,
        "widening must lift the column cap; MAX_COLUMNS=" + RowGroupDescriptor.MAX_COLUMNS);
    final byte[] raw = wideRawLeaf(512, WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroup(1, raw);
        assertArrayEquals(raw, new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .getRowGroup(1), "same-trx readback of a " + WIDE_COLS + "-column leaf");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroup(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "descriptor layout: " + WIDE_COLS + "-column leaf must round-trip cold");
      }
    }
  }

  @Test
  void columnSegmentSlotLayoutIsRecoverableFromTheSlotKeysAlone() {
    // The layout is sticky and slot 0's metadata is normally its only record — but the corruption
    // valve fires precisely when slot 0 is unreadable, and a tombstone that guessed "descriptor"
    // there would send the next rebuild to the wrong layout and mix raw-keyed with composite-keyed
    // row groups beyond recovery. The slot keys themselves must therefore be enough to tell.
    final byte[] raw = rawLeaf(300, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertFalse(storage.probeColumnSegmentSlotLayout(), "an empty store is not segment-slot");
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(raw));
        assertTrue(storage.probeColumnSegmentSlotLayout(),
            "a store with a descriptor at rowGroupId<<16 must probe as segment-slot");
        wtx.commit();
      }
    }
  }

  @Test
  void descriptorLayoutIsRecoverableFromTheSlotKeysAlone() {
    final byte[] raw = rawLeaf(300, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroup(1, raw);
        assertFalse(storage.probeColumnSegmentSlotLayout(),
            "a live raw slot 1 exists only in the descriptor layout");
        wtx.commit();
      }
    }
  }

  @Test
  void segmentSlotWritesTreatAnUnreadableDescriptorAsAbsentRatherThanThrowing() {
    // The descriptor-layout write paths read their prior value with a raw, never-throwing read, so a
    // damaged prior is simply overwritten. The segment-slot twins keep the descriptor in a VERIFIED
    // blob, so a throw there escapes into the corruption valve -> tombstone -> rebuildFully returns
    // early on stale -> a fresh create re-enters the same read and throws again: permanently dead
    // where the descriptor layout self-heals. Corrupt the descriptor slot and prove the write path
    // still makes progress.
    final byte[] a = rawLeaf(300, 10_000L, 0);
    final byte[] b = rawLeaf(310, 10_000L, 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(a));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // Overwrite the descriptor slot with a BARE (non-blob) value: it is non-empty but carries no
        // blob marker, so the verifying read rejects it exactly as a damaged blob would.
        final long descriptorSlot = ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1);
        storage.putColumnSegmentSlot(descriptorSlot, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        assertThrows(IllegalStateException.class, () -> storage.getBlob(descriptorSlot),
            "the verifying read must still reject the corrupted descriptor");
        // The write path must nevertheless make progress rather than propagating that throw.
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(b));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(b, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "the overwritten row group must read back intact");
      }
    }
  }

  @Test
  void segmentSlotLayoutSupportsMoreThan84Columns() {
    final byte[] raw = wideRawLeaf(512, WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(raw));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "segment-slot layout: " + WIDE_COLS + "-column leaf must round-trip cold");
      }
    }
  }

  // ── Very wide row groups: the descriptor itself outgrows the u16 slot value ──────────────────
  // At ~2100 numeric columns the descriptor's entry table alone (31 B/entry) exceeds
  // MAX_SLOT_VALUE_BYTES (0xFFFF). The descriptor-directory layout stores it as an inline slot value
  // and must reject it; the segment-slot layout stores it via putBlob, which spills it into an
  // OverflowPage, so it round-trips fine — the point of keying each column segment in its own slot.

  private static final int VERY_WIDE_COLS = 2100; // ⇒ descriptor ≈ 67 KB > 0xFFFF

  @Test
  void segmentSlotLayoutSpillsWideDescriptorPastTheU16SlotValue() {
    final byte[] raw = wideRawLeaf(16, VERY_WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER)
            .putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(raw));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        // The stored descriptor blob really is past the u16 wall (i.e. it spilled to a page).
        final byte[] desc = ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
        assertNotNull(desc, "descriptor must be present");
        assertTrue(desc.length > RowGroupDescriptor.MAX_SLOT_VALUE_BYTES,
            "descriptor (" + desc.length + " B) must exceed the u16 slot-value limit to prove the spill");
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            reader, INDEX_NUMBER, 1),
            "segment-slot layout: " + VERY_WIDE_COLS + "-column leaf must round-trip through the spilled descriptor");
      }
    }
  }

  @Test
  void descriptorLayoutRejectsRowGroupWiderThanTheU16SlotValue() {
    final byte[] raw = wideRawLeaf(16, VERY_WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final IllegalStateException e = assertThrows(IllegalStateException.class,
            () -> storage.putRowGroup(1, raw));
        assertTrue(e.getMessage().contains("slot-value limit"), e.getMessage());
        wtx.rollback();
      }
    }
  }
}
