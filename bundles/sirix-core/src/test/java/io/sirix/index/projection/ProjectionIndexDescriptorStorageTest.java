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
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P3 suite: segment-slot storage (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §9 P3) — the
 * put/get/read/enumerate/tombstone/putBlob surface over real commits. The decisive test is
 * {@link #singleColumnChangeSharesEverySegmentButOne()}: the storage-level proof of the
 * SLIDING_SNAPSHOT containment claim (§2.5), asserted on the segment pages' durable offset keys
 * across revisions.
 */
final class ProjectionIndexDescriptorStorageTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

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
    final long offset = ProjectionIndexHOTStorage.segmentPageOffset(rtx.getStorageEngineReader(), INDEX_NUMBER,
        ProjectionIndexHOTStorage.columnSegmentSlotKey(rowGroupId, columnSegmentId), 0);
    assertTrue(offset >= 0, "segment " + columnSegmentId + " must exist and be resolved, offset=" + offset);
    return offset;
  }

  /** 128 distinct 24-char departments, so column 2's DICT clears the 512-byte inline threshold. */
  private static final String[] WIDE_DEPTS = wideDepts();

  private static String[] wideDepts() {
    final String[] depts = new String[128];
    for (int i = 0; i < depts.length; i++) {
      depts[i] = "Department-" + (char) ('A' + (i % 26)) + "-" + String.format("%010d", i);
    }
    return depts;
  }

  /**
   * Leaf sized so that KEYS, BODY(0), BODY(2) and DICT(2) each exceed the 512-byte slot-inline
   * threshold and therefore live on their own page — the only segments whose sharing is OBSERVABLE,
   * since an inlined segment has no page identity to compare. Wide random key deltas (~20 bits) and a
   * 128-entry dictionary are what push them over; {@code ageBump} shifts only column 0's values,
   * exactly as {@link #rawLeaf} does.
   */
  private static byte[] pagedSegmentLeaf(final long keyBase, final long ageBump) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random rng = new Random(keyBase);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = new boolean[3];
    final boolean[] unrep = new boolean[3];
    final boolean[] nonIntegral = new boolean[3];
    Arrays.fill(present, true);
    long key = keyBase;
    for (int i = 0; i < ProjectionIndexRowGroupPage.MAX_ROWS; i++) {
      key += 1 + rng.nextInt(1 << 20);
      longs[0] = rng.nextInt(1 << 20) + ageBump;
      bools[1] = rng.nextBoolean();
      strings[2] = WIDE_DEPTS[rng.nextInt(WIDE_DEPTS.length)];
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page.serialize();
  }

  @Test
  void aGrownSlotCascadesThroughADegenerateSplitInsteadOfFailingTheCommit() {
    // A leaf splits on its most significant discriminative bit, and that bit can belong to ONE
    // outlier key: fence chunks live at 2^42 (ProjectionIndexFences.CHUNK_SLOT_BASE), far above
    // every row-group slot, so a full leaf holding both partitions into "every row group" and
    // "the fence" — and frees nothing. Growing a slot on the full half then needs a SECOND split,
    // which partitions on the row-group keys' own MSDB.
    //
    // Regression: the write used to abort the whole commit ("Projection HOT slot update failed
    // after split") because the split was rolled back wholesale whenever the pending value did
    // not fit in its half, making a page that is perfectly splittable look unsplittable.
    final byte[] small = blobPayload(128, 1);
    final byte[] grown = blobPayload(480, 2);
    final int slots = 600; // more than fills a 64 KiB leaf at ~145 bytes per entry
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // The outlier goes in FIRST, so it is present in every leaf this fill splits.
        storage.putBlob(1L << 42, blobPayload(64, 3));
        for (int rowGroupId = 1; rowGroupId <= slots; rowGroupId++) {
          storage.putBlob(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(rowGroupId), small);
        }
        // Grow every slot in place: whichever leaf is full when its turn comes takes the cascade.
        for (int rowGroupId = 1; rowGroupId <= slots; rowGroupId++) {
          storage.putBlob(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(rowGroupId), grown);
        }
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        // Every slot survives the cascade — no key is dropped on either side of a split.
        for (int rowGroupId = 1; rowGroupId <= slots; rowGroupId++) {
          assertArrayEquals(grown,
              ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER,
                  ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(rowGroupId)),
              "row group " + rowGroupId + " must read back its grown payload");
        }
        assertArrayEquals(blobPayload(64, 3), ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER, 1L << 42),
            "the outlier slot the splits partitioned on must survive too");
      }
    }
  }

  /** Deterministic blob bytes — {@code seed} varies the content so a stale read is visible. */
  private static byte[] blobPayload(final int length, final int seed) {
    final byte[] payload = new byte[length];
    for (int i = 0; i < length; i++) {
      payload[i] = (byte) (i * 31 + seed);
    }
    return payload;
  }

  /** Canonical inline PIXB framing whose payload is deliberately changed after hashing. */
  private static byte[] corruptInlineBlob(final byte[] expectedPayload) {
    final int markerBytes = Integer.BYTES + 1 + Integer.BYTES + Long.BYTES;
    final byte[] value = new byte[markerBytes + expectedPayload.length];
    RowGroupDescriptor.putIntLE(value, 0, 0x42584950);
    value[Integer.BYTES] = 0;
    RowGroupDescriptor.putIntLE(value, Integer.BYTES + 1, expectedPayload.length | Integer.MIN_VALUE);
    RowGroupDescriptor.putLongLE(value, Integer.BYTES + 1 + Integer.BYTES,
        ProjectionIndexColumnSegmentCodec.contentHash(expectedPayload));
    System.arraycopy(expectedPayload, 0, value, markerBytes, expectedPayload.length);
    value[value.length - 1] ^= 1;
    return value;
  }

  @Test
  void corruptInlineBlobCannotPassTheUnchangedPutGate() {
    final long slotKey = 31L;
    final byte[] payload = blobPayload(128, 41);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.writeSlotValue(slotKey, corruptInlineBlob(payload));

      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> storage.putBlob(slotKey, payload),
              "matching marker length/hash must not preserve an unverified resident payload");
      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause());
      wtx.rollback();
    }
  }

  @Test
  void malformedReferencedBlobCannotBeTombstonedWithoutItsSidePage() {
    final long slotKey = 32L;
    final byte[] payload = blobPayload(2_000, 42);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putBlob(slotKey, payload);
      storage.writeSlotValue(slotKey, new byte[] {1, 2, 3, 4});

      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> storage.tombstoneBlob(slotKey),
              "an unreadable owner marker must not be deleted while its side page remains");
      assertArrayEquals(payload, storage.getSegmentPageBytes(slotKey, 0),
          "the failed tombstone must not detach the side page");
      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause());
      wtx.rollback();
    }
  }

  @Test
  void singleColumnChangeSharesEverySegmentButOne() {
    // The storage-level proof of the SLIDING_SNAPSHOT containment claim
    // (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.5): a one-column edit costs ONE segment page,
    // not a whole leaf. segmentSlotRePutSharesUnchangedSegmentPagesAndRewritesChanged shows the
    // edited column's page IS rewritten; this shows every OTHER page-backed segment is NOT — the
    // half that makes the claim a containment bound rather than a statement about one column.
    //
    // Scope of the assertion: a segment of ≤512 bytes lives INLINE in its slot value and has no
    // page at all (ProjectionIndexHOTStorage.BLOB_INLINE_MAX), so it cannot witness page identity
    // either way. BODY(1) — a 1024-row boolean bitmap, 128 bytes — is structurally always in that
    // class, and it is asserted to have NO page rather than skipped, so this test cannot be
    // misread as covering all five segments.
    final byte[] v1 = pagedSegmentLeaf(50_000L, 0);
    final byte[] v2 = pagedSegmentLeaf(50_000L, 1); // identical keys/bools/depts — only column 0 differs
    final int body0Seg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    final int body1Seg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1);
    final int[] unchangedPagedSegs = {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2),
        ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)};
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(v1));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(v2));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        // Revision isolation first: sharing is only meaningful if each revision still assembles
        // its OWN bytes — one page serving both would satisfy every offset assertion below.
        assertArrayEquals(v1,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r1.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rev1 byte-identical");
        assertArrayEquals(v2,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r2.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rev2 byte-identical");
        for (final int columnSegmentId : unchangedPagedSegs) {
          assertEquals(segmentDiskKey(r1, 1, columnSegmentId), segmentDiskKey(r2, 1, columnSegmentId),
              "segment " + columnSegmentId + " is untouched by a column-0 edit and must be shared "
                  + "by reference across the revisions");
        }
        assertNotEquals(segmentDiskKey(r1, 1, body0Seg), segmentDiskKey(r2, 1, body0Seg),
            "the edited column's BODY must be a new page");
        assertTrue(
            ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER,
                ProjectionIndexHOTStorage.columnSegmentSlotKey(1, body1Seg), 0) < 0,
            "BODY(1) is a 128-byte boolean bitmap — it must stay inline in its slot value, which is "
                + "why it is outside the page-sharing assertion above");
      }
    }
  }

  @Test
  void putGetReadRoundTripAcrossCommit() {
    final byte[] raw = rawLeaf(700, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(raw));
        assertArrayEquals(raw, storage.getRowGroupFromColumnSegmentSlots(1), "same-trx readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "cold-reopen readback");
      }
    }
  }

  @Test
  void segmentSlotLayoutRoundTripAndPruning() {
    // Segment ⇔ slot layout: every segment is its own HOT slot (zone-map descriptor at
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
        // F1: the stored descriptor must end exactly at its entry table. Every segment lives in its
        // own slot, so there can be no trailing descriptor payload.
        final byte[] desc =
            ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
        assertNotNull(desc);
        RowGroupDescriptor.validate(desc);
        assertEquals(
            27 + RowGroupDescriptor.columnCount(desc) + 2
                + RowGroupDescriptor.columnSegmentCount(desc) * RowGroupDescriptor.ENTRY_BYTES,
            desc.length, "segment-slot descriptor must contain no trailing payload");
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
    // and XXH3 content hash live in the descriptor entry, re-checked by verifyColumnSegment at
    // assembly. So
    // the SAME raw bytes round-trip through the bare segment reader, but the blob reader REJECTS a
    // segment slot (no PIXB magic → not a blob), proving the redundant 17-byte hashed marker is gone.
    // The descriptor slot, by contrast, stays a hashed blob — nothing else backs its integrity.
    final int keysSeg = 0; // KEYS(0)
    final int body0Seg = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0); // large row count → referenced
    final byte[] raw = rawLeaf(400, 10_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(raw));
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
          assertThrows(IllegalStateException.class, () -> ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER, slotKey),
              "segment " + columnSegmentId + " slot carries no blob marker/hash — the blob reader must reject it");
        }
        assertNotNull(
            ProjectionIndexHOTStorage.readBlob(r, INDEX_NUMBER, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1)),
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
        assertTrue(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
                ProjectionIndexColumnSegmentCodec.encode(v1)));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) { // rev2: identical re-put → no-op share
        assertFalse(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
                ProjectionIndexColumnSegmentCodec.encode(v1)));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) { // rev3: column 0 changed → BODY(0) rewritten
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(v2);
        assertThrows(IllegalStateException.class,
            () -> storage.putRowGroupAsColumnSegmentSlots(1, encoded, new long[] {1L << 1}, false));
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1, encoded, new long[] {1L}, false));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2);
          JsonNodeReadOnlyTrx r3 = session.beginNodeReadOnlyTrx(3)) {
        assertArrayEquals(v1,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r1.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rev1 byte-identical");
        assertArrayEquals(v1,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r2.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rev2 (identical re-put) byte-identical");
        assertArrayEquals(v2,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(r3.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rev3 byte-identical");
        final long body0Key = ProjectionIndexHOTStorage.columnSegmentSlotKey(1, body0Seg);
        final long off1 =
            ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        final long off2 =
            ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        final long off3 =
            ProjectionIndexHOTStorage.segmentPageOffset(r3.getStorageEngineReader(), INDEX_NUMBER, body0Key, 0);
        assertTrue(off1 >= 0, "BODY(0) is a referenced (large) segment with a page");
        assertEquals(off1, off2, "identical re-put shares the unchanged BODY(0) page (carry-forward)");
        assertNotEquals(off2, off3, "column-0 change rewrites the BODY(0) segment page");
      }
    }
  }

  @Test
  void columnPatchCarriesUntouchedDescriptorEntriesWithoutTheirBytes() {
    final byte[] before = rawLeaf(900, 60_000L, 0);
    final byte[] after = rawLeaf(900, 60_000L, 1);
    final ProjectionIndexRowGroupPage source = ProjectionIndexRowGroupPage.deserialize(after);
    final ProjectionIndexRowGroupPage ageOnly =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    for (int row = 0; row < source.getRowCount(); row++) {
      assertTrue(ageOnly.appendRow(source.recordKeys()[row], new long[] {source.numericColumn(0)[row]}, new boolean[1],
          new String[1]));
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(before));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final byte[] priorDescriptor = storage.getVerifiedRowGroupDescriptor(1);
        assertNotNull(priorDescriptor);
        final ProjectionIndexColumnSegmentCodec.EncodedColumn age = ProjectionIndexColumnSegmentCodec.encodeColumn(
            ageOnly, 0, new ProjectionIndexColumnSegmentCodec.EncodeWorkspace());
        final ProjectionIndexColumnSegmentCodec.EncodedColumn[] replacements = {age};
        final long[] changedColumns = new long[(RowGroupDescriptor.columnCount(priorDescriptor) + Long.SIZE - 1) >>> 6];
        final byte[] patched =
            ProjectionIndexColumnSegmentCodec.spliceColumns(priorDescriptor, replacements, 1, changedColumns);
        final ProjectionIndexHOTStorage.ColumnPatchResult result =
            storage.putColumnPatches(1, priorDescriptor, patched, replacements, 1, changedColumns);
        assertTrue(result.changed());
        assertEquals(1, result.segmentsWritten(), "only BODY(0) is supplied and rewritten");
        assertEquals(0, result.segmentsTombstoned());
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(after, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1));
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
        new String[] {"/[]/age", "/[]/active", "/[]/dept"}, new String[] {"age", "active", "dept"}, KINDS, 2, 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(l1));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(l2));
        // Fence chunks over the two leaves' record-key zones — exactly what finishPersist writes.
        ProjectionIndexFences.write(storage, 2, new long[] {10_001L, 40_001L}, new long[] {39_999L, 60_000L});
        // Live slot 0 is the publication record and is always written after the units it describes.
        storage.putBlob(0, meta.serialize());
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 2);
        assertEquals(2, all.size(), "fence + metadata blobs must not be counted as leaves");
        assertArrayEquals(l1, all.get(0), "leaf 1 byte-identical despite fence/metadata companions");
        assertArrayEquals(l2, all.get(1), "leaf 2 byte-identical despite fence/metadata companions");
      }
    }
  }

  @Test
  void segmentSlotEnumerationHonorsLinkedPhysicalOrder() {
    final byte[] first = rawLeaf(3, 10_000L, 0);
    final byte[] second = rawLeaf(4, 20_000L, 0);
    final byte[] inserted = rawLeaf(5, 15_000L, 0);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(first));
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(second));
        storage.putRowGroupAsColumnSegmentSlots(3, ProjectionIndexColumnSegmentCodec.encode(inserted));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> ordered = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 3, new int[] {1, 3, 2});
        assertArrayEquals(first, ordered.get(0));
        assertArrayEquals(inserted, ordered.get(1));
        assertArrayEquals(second, ordered.get(2));
      }
    }
  }

  @Test
  void segmentSlotEnumerationIsLoudOnALeakedOrphanBeyondLeafCount() {
    // Invariant parity with the row-count scan (which catches a
    // live slot past rowGroupCount): a rebuild bug that leaves a live descriptor at rowGroupCount+1
    // must be
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
  void segmentSlotShrinkTombstonesVanishedSegments() {
    // A row group that loses a segment (here: shrink to an empty leaf, which drops the DICT) must
    // tombstone exactly the vanished segment slot, and the empty leaf must round-trip.
    final int dictSeg = ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2);
    final byte[] full = rawLeaf(300, 70_000L, 0); // 3 columns incl. a string DICT
    final byte[] empty = rawLeaf(0, 70_000L, 0); // rowCount 0 → no DICT segment
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(full));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(rtx.getStorageEngineReader(), INDEX_NUMBER,
            ProjectionIndexHOTStorage.columnSegmentSlotKey(1, dictSeg)), "DICT slot present before shrink");
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(empty));
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
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(wide1));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(narrow));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(narrow,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "narrow leaf must assemble without stale wide segments");
        assertNull(
            ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 1,
                ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2)),
            "vanished column's BODY ref must be removed");
        assertNull(
            ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 1,
                ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)),
            "vanished column's DICT ref must be removed");
      }
      // Time travel still serves the wide revision.
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertArrayEquals(wide1, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1));
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(wide2));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(wide2,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "grow-back must serve fresh segments, not resurrected ones");
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
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(empty)); // live
                                                                                                     // empty
                                                                                                     // leaf
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(full));
        storage.tombstoneRowGroupAsColumnSegmentSlots(2); // tombstoned leaf
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(empty,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "live empty leaf reads as its raw empty form, NOT as absent");
        assertNull(
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 2),
            "tombstoned leaf reads as absent");
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1);
        assertEquals(1, all.size(), "enumeration returns the live empty leaf as a leaf, not as absent");
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
          storage.putRowGroupAsColumnSegmentSlots(i + 1, ProjectionIndexColumnSegmentCodec.encode(raws[i]));
        }
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, numLeaves);
        assertEquals(numLeaves, all.size());
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(raws[i], all.get(i), "leaf " + (i + 1) + " parity");
          assertArrayEquals(raws[i], ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
              rtx.getStorageEngineReader(), INDEX_NUMBER, i + 1), "point read parity for leaf " + (i + 1));
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
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rawLeaf(100, 500L, 0)));
        assertArrayEquals(metadata, storage.getBlob(0), "same-trx blob readback");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(metadata, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0),
            "cold blob readback with hash verification");
        // The blob slot must not surface in leaf enumeration.
        final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, 1);
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
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rawLeaf(20, 900L, 0))); // dirty
                                                                                                                    // something
                                                                                                                    // so
                                                                                                                    // the
                                                                                                                    // commit
                                                                                                                    // is
                                                                                                                    // non-empty
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        final long off1 = ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER, 0, 0);
        final long off2 = ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(), INDEX_NUMBER, 0, 0);
        assertTrue(off1 >= 0);
        assertEquals(off1, off2, "unchanged blob must be shared by reference, not rewritten");
      }
      // Tombstoning the blob slot must remove its segment ref — not leak the MB-scale page
      // into every future fragment.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).tombstoneBlob(0);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 0, 0),
            "blob segment ref must be removed by the tombstone");
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
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rawLeaf(50, 300L, 0)));
        assertArrayEquals(meta, storage.getBlob(0), "same-trx inline blob readback");
        assertNull(storage.getSegmentPageBytes(0, 0), "an inline blob writes no segment page");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(meta, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0),
            "cold inline blob readback with hash verification");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 0, 0),
            "no page exists for an inline blob");
        // The inline blob slot must not surface in leaf enumeration.
        assertEquals(1, ProjectionIndexHOTStorage
                                                 .readAllRowGroupsFromColumnSegmentSlots(rtx.getStorageEngineReader(),
                                                     INDEX_NUMBER, 1)
                                                 .size());
      }
    }
  }

  @Test
  void referencedBlobIsBatchedBeforeRootCommitAndReadableAcrossPublication() {
    final byte[] payload = new byte[8 * 1024];
    new Random(0x5E6D3E17L).nextBytes(payload);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, payload);

        assertEquals(Constants.NULL_ID_LONG,
            ProjectionIndexHOTStorage.segmentPageOffset(wtx.getStorageEngineWriter(), INDEX_NUMBER, 0, 0),
            "staging must not publish an offset before the append buffer is flushed");
        assertArrayEquals(payload, storage.getBlob(0),
            "same-transaction read before flush must use the resident pending page");

        wtx.getStorageEngineWriter().asyncFlush();
        wtx.getStorageEngineWriter().awaitPendingAsyncFlush();

        assertTrue(ProjectionIndexHOTStorage.segmentPageOffset(wtx.getStorageEngineWriter(), INDEX_NUMBER, 0, 0) >= 0,
            "foreground cleanup must publish the durable offset after the whole batch flushes");
        assertArrayEquals(payload, storage.getBlob(0),
            "same-transaction read after cleanup must resolve the flushed page by offset");
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(payload, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
      }
    }
  }

  @Test
  void threeExplicitEpochsReuseBothNativeReservoirsWithoutOverwritingPayloads() {
    final byte[][] payloads = {new byte[8 * 1024], new byte[9 * 1024], new byte[10 * 1024]};
    for (int i = 0; i < payloads.length; i++) {
      new Random(0xA11CE000L + i).nextBytes(payloads[i]);
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);

        for (int i = 0; i < payloads.length; i++) {
          storage.putBlob(i, payloads[i]);
          wtx.getStorageEngineWriter().asyncFlush();
          wtx.getStorageEngineWriter().awaitPendingAsyncFlush();

          for (int alreadyPublished = 0; alreadyPublished <= i; alreadyPublished++) {
            assertArrayEquals(payloads[alreadyPublished], storage.getBlob(alreadyPublished),
                "reservoir reuse changed blob " + alreadyPublished + " after epoch " + i);
          }
        }
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        for (int i = 0; i < payloads.length; i++) {
          assertArrayEquals(payloads[i],
              ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, i));
        }
      }
    }
  }

  @Test
  void finalCommitDrainsTheLastActiveSidePageBatch() {
    final byte[] payload = new byte[12 * 1024];
    new Random(0xF1A15EEDL).nextBytes(payload);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, payload);
        assertEquals(Constants.NULL_ID_LONG,
            ProjectionIndexHOTStorage.segmentPageOffset(wtx.getStorageEngineWriter(), INDEX_NUMBER, 0, 0));

        // No explicit asyncFlush: commit-time index maintenance has the same shape. The writer's
        // final await must rotate and drain this active tail before HOT root serialization.
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(payload, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
      }
    }
  }

  @Test
  void pendingBulkSideKeyCannotBeReplacedOrRemoved() {
    final long ownerSlotKey = 77L;
    final int segmentId = 7;
    final byte[] original = new byte[8 * 1024];
    final byte[] replacement = new byte[9 * 1024];
    new Random(0xA99E_0D1EL).nextBytes(original);
    new Random(0xBAD5_1DEL).nextBytes(replacement);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putBlob(0, original);
      final IllegalStateException blobReplacementFailure =
          assertThrows(IllegalStateException.class, () -> storage.putBlob(0, replacement));
      assertTrue(blobReplacementFailure.getMessage().contains("append-only"));
      assertArrayEquals(original, storage.getBlob(0),
          "the public blob path must reject a pending replacement before rewriting its hash marker");

      storage.writeSlotValue(ownerSlotKey, new byte[] {1});
      storage.putSegmentPage(ownerSlotKey, segmentId, original);

      assertArrayEquals(original, storage.getSegmentPageBytes(ownerSlotKey, segmentId));

      final IllegalStateException replacementFailure =
          assertThrows(IllegalStateException.class, () -> storage.putSegmentPage(ownerSlotKey, segmentId, replacement));
      assertTrue(replacementFailure.getMessage().contains("append-only"));
      assertArrayEquals(original, storage.getSegmentPageBytes(ownerSlotKey, segmentId),
          "a rejected replacement must leave the staged reference intact");

      final IllegalStateException removalFailure =
          assertThrows(IllegalStateException.class, () -> storage.removeSegmentPage(ownerSlotKey, segmentId));
      assertTrue(removalFailure.getMessage().contains("append-only"));
      assertArrayEquals(original, storage.getSegmentPageBytes(ownerSlotKey, segmentId),
          "a rejected removal must leave the staged reference intact");

      wtx.rollback();
    }
  }

  @Test
  void failedCompoundSegmentReplacementMakesTheTransactionRollbackOnly() {
    final long segmentSlot = ProjectionIndexHOTStorage.columnSegmentSlotKey(1, 0);
    final byte[] original = new byte[8 * 1024];
    final byte[] replacement = new byte[9 * 1024];
    new Random(0x51DEL).nextBytes(original);
    new Random(0xBADL).nextBytes(replacement);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putColumnSegmentSlot(segmentSlot, original);

      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> storage.putColumnSegmentSlot(segmentSlot, replacement));
      assertTrue(failure.getMessage().contains("append-only"));
      assertArrayEquals(original, storage.getSegmentPageBytes(segmentSlot, 0),
          "the failed replacement must leave the original pending page readable until rollback");

      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause(),
          "a caller that catches the compound replacement failure must still be unable to commit");
      wtx.rollback();
    }
  }

  @Test
  void failedRowGroupPublicationAfterEarlierSlotMutationMakesTheTransactionRollbackOnly() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup wide =
        ProjectionIndexColumnSegmentCodec.encode(wideRawLeaf(1, WIDE_COLS));
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup narrow =
        ProjectionIndexColumnSegmentCodec.encode(narrowLeaf(1, 1_000L));
    assertTrue(wide.descriptor().length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "the first descriptor must be referenced so its staged page can reject replacement");

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putRowGroupAsColumnSegmentSlots(1, wide);

      // The shrink tombstones vanished BODY slots before replacing the referenced descriptor. Its
      // descriptor side page is still owned by the append-only staging pipeline, so that later
      // replacement rejects before touching its own slot. Catching the rejection must not allow the
      // earlier tombstones from the same logical row-group publication to commit.
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> storage.putRowGroupAsColumnSegmentSlots(1, narrow));
      assertTrue(failure.getMessage().contains("append-only"));

      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause(),
          "a caught late failure must retain its identity and reject the partially published row group");
      wtx.rollback();
    }
  }

  @Test
  void failedColumnPatchAfterDescriptorPublicationMakesTheTransactionRollbackOnly() {
    final ProjectionIndexRowGroupPage priorPage =
        ProjectionIndexRowGroupPage.deserialize(narrowLeaf(ProjectionIndexRowGroupPage.MAX_ROWS, 2_000L));
    final ProjectionIndexRowGroupPage replacementPage =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    for (int row = 0; row < priorPage.getRowCount(); row++) {
      assertTrue(replacementPage.appendRow(priorPage.recordKeys()[row], new long[] {7L}, new boolean[1], new String[1],
          new boolean[] {true}, new boolean[1], new boolean[1]));
    }

    final ProjectionIndexColumnSegmentCodec.EncodeWorkspace workspace =
        new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup prior =
        ProjectionIndexColumnSegmentCodec.encode(priorPage, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn replacement =
        ProjectionIndexColumnSegmentCodec.encodeColumn(replacementPage, 0, workspace);
    final ProjectionIndexColumnSegmentCodec.EncodedColumn[] replacements = {replacement};
    final long[] changedColumns = new long[1];
    final byte[] patchedDescriptor =
        ProjectionIndexColumnSegmentCodec.spliceColumns(prior.descriptor(), replacements, 1, changedColumns);
    final int bodyId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    assertTrue(RowGroupDescriptor.entryByteLen(prior.descriptor(), RowGroupDescriptor.entryIndexOf(prior.descriptor(),
        bodyId)) > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES);
    assertTrue(RowGroupDescriptor.entryByteLen(patchedDescriptor, RowGroupDescriptor.entryIndexOf(patchedDescriptor,
        bodyId)) <= ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putRowGroupAsColumnSegmentSlots(1, prior);

      // putColumnPatches publishes the new descriptor before shrinking BODY from a staged reference
      // to inline. The pending side page rejects that shrink before the BODY primitive mutates, so
      // the enclosing batch—not merely the individual slot primitive—must poison the transaction.
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> storage.putColumnPatches(1, prior.descriptor(), patchedDescriptor, replacements, 1, changedColumns));
      assertTrue(failure.getMessage().contains("append-only"));

      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause(),
          "a caught segment-shrink rejection must not commit its already-published descriptor");
      wtx.rollback();
    }
  }

  @Test
  void failedRowGroupDeleteAfterEarlierSegmentTombstonesMakesTheTransactionRollbackOnly() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup wide =
        ProjectionIndexColumnSegmentCodec.encode(wideRawLeaf(1, WIDE_COLS));
    assertTrue(wide.descriptor().length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putRowGroupAsColumnSegmentSlots(1, wide);

      // Segment slots are tombstoned first. Removing the final referenced descriptor then rejects
      // because its side page is still pending in the append-only pipeline. The delete is one
      // logical operation, so those earlier tombstones may not become committable partial state.
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> storage.tombstoneRowGroupAsColumnSegmentSlots(1));
      assertTrue(failure.getMessage().contains("append-only"));

      final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
      assertSame(failure, commitFailure.getCause(),
          "a caught descriptor-delete rejection must not commit earlier segment tombstones");
      wtx.rollback();
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
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rawLeaf(10, 700L, 0)));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(small, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 0, 0),
            "referenced→inline must drop the stale page");
      }
      // Inline → referenced: the migration must (re)create the page.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(0, big);
        storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(rawLeaf(10, 900L, 0)));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(big, ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
        assertNotNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER, 0, 0),
            "inline→referenced must create a page");
      }
    }
  }

  @Test
  void virginInitializerRejectsPartiallyPopulatedCurrentTree() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final long segmentSlot = ProjectionIndexHOTStorage.columnSegmentSlotKey(1, 0);
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putColumnSegmentSlot(segmentSlot, new byte[] {1, 2, 3, 4});
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertFalse(storage.isEmptyTree(), "the segment slot must make the physical tree non-virgin");
        assertThrows(IllegalStateException.class, storage::requireVirginTreeForInitialBuild,
            "the initializer must not erase a populated tree, even when metadata was never published");
      }
    }
  }


  @Test
  void emptyStoreReadsAsAbsent() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(ProjectionIndexHOTStorage
                                            .readAllRowGroupsFromColumnSegmentSlots(rtx.getStorageEngineReader(),
                                                INDEX_NUMBER, 0)
                                            .isEmpty(),
            "no projection sub-tree installed → empty enumeration");
        assertNull(
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
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



  /** Single NUMERIC_LONG column — segments KEYS(0) and BODY(0) only, a strict subset of rawLeaf's. */
  private static byte[] narrowLeaf(final int rows, final long keyBase) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final Random rng = new Random(keyBase);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    long key = keyBase;
    for (int i = 0; i < rows; i++) {
      key += 4 + rng.nextInt(5);
      longs[0] = rng.nextInt(1 << 20);
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page.serialize();
  }

  @Test
  void malformedDescriptorCannotBeHiddenByVirginInitializer() {
    final byte[] wide = rawLeaf(300, 10_000L, 0); // KEYS(0), BODY(0), BODY(1), BODY(2), DICT(2)
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(wide));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // Corrupt the descriptor: non-empty, but no blob marker, so the verifying read rejects it.
        storage.putColumnSegmentSlot(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1),
            new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
        assertThrows(IllegalStateException.class,
            () -> storage.getBlob(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1)),
            "the live verifying read must reject the malformed descriptor before publication");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader r = rtx.getStorageEngineReader();
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(r, INDEX_NUMBER, 1),
            "a malformed descriptor must fail loudly rather than serve incomplete data");
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertThrows(IllegalStateException.class,
            () -> storage.getBlob(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1)),
            "the malformed descriptor must remain visible through the normal verifying read");
        assertFalse(storage.isEmptyTree(), "the malformed slot keeps the tree physically non-virgin");
        assertThrows(IllegalStateException.class, storage::requireVirginTreeForInitialBuild,
            "a full initializer must never erase corrupted-but-populated projection state");
      }
    }
  }

  @Test
  void segmentSlotMutationFailsClosedOnUnreadableDescriptor() {
    // Without the descriptor there is no authoritative list of owned segment slots. The one safe
    // behavior is to fail and roll back, preserving the prior committed row group byte-for-byte.
    final byte[] a = rawLeaf(300, 10_000L, 0);
    final byte[] b = rawLeaf(310, 10_000L, 1);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(a));
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
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(b)),
            "a mutation cannot safely diff owned segments without a readable prior descriptor");
        final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit,
            "descriptor corruption encountered by a mutation must poison the transaction");
        assertSame(failure, commitFailure.getCause(), "the commit must retain the authoritative mutation failure");
        wtx.rollback();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(a,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "rollback must preserve the prior committed row group without orphaning segments");
      }
    }
  }

  @Test
  void segmentSlotLayoutSupportsMoreThan84Columns() {
    final byte[] raw = wideRawLeaf(512, WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(raw));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(raw,
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX_NUMBER, 1),
            "segment-slot layout: " + WIDE_COLS + "-column leaf must round-trip cold");
      }
    }
  }

  // ── Very wide row groups: the descriptor itself outgrows the u16 slot value ──────────────────
  // At ~2100 numeric columns the descriptor's entry table alone (31 B/entry) exceeds
  // 0xFFFF. The sole segment-slot layout stores it through putBlob, which spills to an OverflowPage
  // instead of putting a large value in the HOT slot.

  private static final int VERY_WIDE_COLS = 2100; // ⇒ descriptor ≈ 67 KB > 0xFFFF

  @Test
  void segmentSlotLayoutSpillsWideDescriptorPastTheU16SlotValue() {
    final byte[] raw = wideRawLeaf(16, VERY_WIDE_COLS);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).putRowGroupAsColumnSegmentSlots(1,
            ProjectionIndexColumnSegmentCodec.encode(raw));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = rtx.getStorageEngineReader();
        // The stored descriptor blob really is past the u16 wall (i.e. it spilled to a page).
        final byte[] desc = ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER,
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
        assertNotNull(desc, "descriptor must be present");
        assertTrue(desc.length > 0xFFFF,
            "descriptor (" + desc.length + " B) must exceed the u16 slot-value limit to prove the spill");
        assertArrayEquals(raw, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, INDEX_NUMBER, 1),
            "segment-slot layout: " + VERY_WIDE_COLS + "-column leaf must round-trip through the spilled descriptor");
      }
    }
  }

}
