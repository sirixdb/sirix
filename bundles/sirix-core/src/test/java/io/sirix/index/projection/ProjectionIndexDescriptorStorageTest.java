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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3 suite: descriptor-layout storage (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §9 P3) —
 * putLeaf/getLeaf/readLeaf/readAllLeaves/tombstoneLeaf/putBlob over real commits. The
 * decisive test is {@code singleColumnChangeSharesEverySegmentButOne}: the storage-level
 * proof of the SLIDING_SNAPSHOT containment claim (§2.5), asserted on the segment pages'
 * durable offset keys across revisions.
 */
final class ProjectionIndexDescriptorStorageTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
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
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
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

  private static long segmentDiskKey(final JsonNodeReadOnlyTrx rtx, final long leafIndex, final int segmentId) {
    // Observable identity of a committed segment page: its durable offset key. Equal keys
    // across revisions prove the page was SHARED by reference (the CoW carry-forward no-op),
    // not merely rewritten with identical bytes.
    final long offset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
        INDEX_NUMBER, leafIndex, segmentId);
    assertTrue(offset >= 0, "segment " + segmentId + " must exist and be resolved, offset=" + offset);
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
        storage.putLeaf(1, raw);
        assertArrayEquals(raw, storage.getLeaf(1), "same-trx readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "cold-reopen readback");
      }
    }
  }

  @Test
  void singleColumnChangeSharesEverySegmentButOne() {
    // This test is about page-level carry-forward sharing: identical durable OFFSET keys across
    // revisions prove a segment PAGE was shared by reference. That is only observable for
    // REFERENCED segments — inline segments carry no page — so pin the referenced model here.
    // (The hybrid's inline sharing rides the descriptor and is covered by the codec round trips.)
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 0;
    try {
    final byte[] v1 = rawLeaf(900, 50_000L, 0);
    final byte[] v2 = rawLeaf(900, 50_000L, 1); // only column 0's values differ
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, v1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, v2);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
           JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        // Revision isolation: each revision assembles its own bytes.
        assertArrayEquals(v1, ProjectionIndexHOTStorage.readLeaf(r1.getStorageEngineReader(), INDEX_NUMBER, 1));
        assertArrayEquals(v2, ProjectionIndexHOTStorage.readLeaf(r2.getStorageEngineReader(), INDEX_NUMBER, 1));
        // Containment: KEYS, BODY(1), BODY(2), DICT(2) identical across revisions
        // (shared by the hash no-op); BODY(0) differs.
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexSegmentCodec.keysSegmentId()),
            segmentDiskKey(r2, 1, ProjectionIndexSegmentCodec.keysSegmentId()));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexSegmentCodec.bodySegmentId(1)),
            segmentDiskKey(r2, 1, ProjectionIndexSegmentCodec.bodySegmentId(1)));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexSegmentCodec.bodySegmentId(2)),
            segmentDiskKey(r2, 1, ProjectionIndexSegmentCodec.bodySegmentId(2)));
        assertEquals(segmentDiskKey(r1, 1, ProjectionIndexSegmentCodec.dictSegmentId(2)),
            segmentDiskKey(r2, 1, ProjectionIndexSegmentCodec.dictSegmentId(2)));
        assertNotEquals(segmentDiskKey(r1, 1, ProjectionIndexSegmentCodec.bodySegmentId(0)),
            segmentDiskKey(r2, 1, ProjectionIndexSegmentCodec.bodySegmentId(0)),
            "the edited column's BODY must be a new page");
      }
    }
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
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
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, raw);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final long dictOffset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexSegmentCodec.dictSegmentId(2));
        assertTrue(dictOffset < 0, "small DICT(2) must be inline (no page), got offset=" + dictOffset);
        final long bodyOffset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexSegmentCodec.bodySegmentId(0));
        assertTrue(bodyOffset >= 0, "large BODY(0) must still be a referenced page, got offset=" + bodyOffset);
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
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
      final ProjectionIndexLeafPage page =
          new ProjectionIndexLeafPage(new byte[] {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG});
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
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, wide1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, narrow);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(narrow, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "narrow leaf must assemble without stale wide segments");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexSegmentCodec.bodySegmentId(2)),
            "vanished column's BODY ref must be removed");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1, ProjectionIndexSegmentCodec.dictSegmentId(2)),
            "vanished column's DICT ref must be removed");
      }
      // Time travel still serves the wide revision.
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertArrayEquals(wide1, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1));
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putLeaf(1, wide2);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(wide2, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "grow-back must serve fresh segments, not resurrected ones");
      }
    }
  }

  @Test
  void tombstoneVersusLiveEmptyLeaf() {
    final byte[] empty = new ProjectionIndexLeafPage(KINDS).serialize();
    final byte[] full = rawLeaf(50, 3_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putLeaf(1, empty);   // live empty leaf
        storage.putLeaf(2, full);
        storage.tombstoneLeaf(2);    // tombstoned leaf
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(empty, ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 1), "live empty leaf reads as its raw empty form, NOT as absent");
        assertNull(ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(), INDEX_NUMBER, 2),
            "tombstoned leaf reads as absent");
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllLeaves(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(1, all.size(), "readAllLeaves includes the live empty leaf, skips the tombstone");
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
          storage.putLeaf(i + 1, raws[i]);
        }
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllLeaves(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size());
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(raws[i], all.get(i), "leaf " + (i + 1) + " parity");
          assertArrayEquals(raws[i], ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(),
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
        storage.putLeaf(1, rawLeaf(100, 500L, 0));
        assertArrayEquals(metadata, storage.getBlob(0), "same-trx blob readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(metadata, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(),
            INDEX_NUMBER, 0), "cold blob readback with hash verification");
        // The blob slot must not surface in leaf enumeration.
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllLeaves(rtx.getStorageEngineReader(), INDEX_NUMBER);
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
        storage.putLeaf(1, rawLeaf(20, 900L, 0)); // dirty something so the commit is non-empty
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
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).tombstoneLeaf(0);
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
   * {@code (leafIndex << 8) | chunkIdx} via the package-private {@code writeSlotValue} seam,
   * which serializes the raw long key with {@code PathKeySerializer} — identical bytes to the
   * removed legacy put path. The legacy metadata payload at slot 0 is composite key 0.
   */
  private static void writeLegacyChunkedLeaf(final ProjectionIndexHOTStorage storage,
      final long leafIndex, final byte[] payload) {
    final int chunks = Math.max(1, (payload.length + LEGACY_CHUNK_SIZE - 1) / LEGACY_CHUNK_SIZE);
    for (int chunkIdx = 0; chunkIdx < chunks; chunkIdx++) {
      final int off = chunkIdx * LEGACY_CHUNK_SIZE;
      final int len = Math.min(LEGACY_CHUNK_SIZE, payload.length - off);
      storage.writeSlotValue((leafIndex << 8) | chunkIdx, Arrays.copyOfRange(payload, off, off + len));
    }
  }

  /**
   * Reader-side reassembly of a legacy chunked leaf (what the removed {@code readOne} did):
   * concatenate chunk slots until a missing/partial chunk ends the payload.
   */
  private static byte[] readLegacyChunkedLeaf(final StorageEngineReader reader, final long leafIndex) {
    final PageReference rootRef = ProjectionIndexHOTStorage.rootReference(reader, INDEX_NUMBER);
    assertNotNull(rootRef, "projection sub-tree must exist");
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final byte[] keyBuf = new byte[8];
      byte[] out = new byte[0];
      for (int chunkIdx = 0; chunkIdx < 256; chunkIdx++) {
        PathKeySerializer.INSTANCE.serialize((leafIndex << 8) | chunkIdx, keyBuf, 0);
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
        storage.putLeaf(1, fresh);
        storage.putBlob(0, metadata);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // No mixed-layout error: the legacy chunks are gone with the old tree.
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAllLeaves(rtx.getStorageEngineReader(), INDEX_NUMBER);
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
        assertThrows(IllegalArgumentException.class, () -> storage.putLeaf(1, null));
        // The two layouts use disjoint key spaces except where the legacy composite key
        // (leafIndex << 8 | chunkIdx) collides with a plain descriptor slot key — legacy leaf 0
        // chunk 0 encodes exactly like descriptor slot 0. A descriptor read of such a slot must
        // fail loudly (mixed layouts are a migration bug), never silently misparse.
        writeLegacyChunkedLeaf(storage, 0, rawLeaf(50, 1L, 0));
        assertThrows(IllegalStateException.class, () -> storage.getLeaf(0));
        wtx.rollback();
      }
    }
  }

  @Test
  void emptyStoreReadsAsAbsent() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(ProjectionIndexHOTStorage.readAllLeaves(rtx.getStorageEngineReader(),
            INDEX_NUMBER).isEmpty(), "no projection sub-tree installed → empty enumeration");
        assertNull(ProjectionIndexHOTStorage.readLeaf(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "point read on an empty store must be absent");
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0),
            "blob read on an empty store must be absent");
      }
    }
  }
}
