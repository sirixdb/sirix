/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@link ProjectionIndexFences} — the chunked, carry-forward store for the
 * projection's per-leaf record-key zone map. The decisive test is
 * {@link #changingOneLeafRewritesOnlyItsChunk}: the storage-level proof that a commit re-persists
 * only the fence chunk whose leaf moved, sharing every other chunk's page by reference (the whole
 * point of moving the fences out of the single slot-0 blob).
 */
final class ProjectionIndexFencesTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final int FENCE_ENTRY_BYTES = 244;
  private static final int DOCUMENT_NEXT_OFFSET = 16;
  private static final int DOCUMENT_PREVIOUS_OFFSET = 20;

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

  /** Ascending, non-overlapping ranges of {@code count} leaves — a realistic zone map. */
  private static long[][] ranges(final int count, final long base) {
    final long[] first = new long[count];
    final long[] last = new long[count];
    long key = base;
    for (int i = 0; i < count; i++) {
      first[i] = key;
      key += 1000;
      last[i] = key;
      key += 7;
    }
    return new long[][] {first, last};
  }

  private static void overwriteDocumentLink(final ProjectionIndexHOTStorage storage, final int slot,
      final int fieldOffset, final int targetSlot) {
    final int chunkId = (slot - 1) / ProjectionIndexFences.CHUNK_LEAVES;
    final byte[] persisted = storage.getBlob(ProjectionIndexFences.CHUNK_SLOT_BASE + chunkId);
    assertNotNull(persisted, "corruption fixture requires a persisted fence chunk");
    final byte[] corrupted = persisted.clone();
    final int entryOffset = ((slot - 1) % ProjectionIndexFences.CHUNK_LEAVES) * FENCE_ENTRY_BYTES;
    ProjectionIndexRowGroupCodec.putIntLEAt(corrupted, entryOffset + fieldOffset, targetSlot);
    storage.putBlob(ProjectionIndexFences.CHUNK_SLOT_BASE + chunkId, corrupted);
  }

  /**
   * Test-only linear locator for fixtures that mutate fences directly. Production callers locate the
   * document position while resolving the changed JSON node and pass that position into the mutation;
   * keeping this scan here prevents a hidden O(n) compatibility route from entering the production
   * API.
   */
  private static ProjectionIndexFences.DocumentPosition positionAfterForTest(
      final ProjectionIndexFences.Accessor fences, final int afterSlot) {
    final int levels = fences.documentTailPosition().predecessors().length;
    final int[] predecessors = new int[levels];
    final int[] successors = new int[levels];
    boolean found = false;
    int cursor = fences.documentHead();
    int traversed = 0;
    while (cursor != 0) {
      if (++traversed > fences.physicalRowGroupCount()) {
        throw new IllegalStateException("projection document-order test fixture contains a cycle");
      }
      final int height = documentSkipHeightForTest(cursor, levels);
      for (int level = 0; level < height; level++) {
        if (!found) {
          predecessors[level] = cursor;
        } else if (successors[level] == 0) {
          successors[level] = cursor;
        }
      }
      if (cursor == afterSlot) {
        found = true;
      }
      cursor = fences.next(cursor);
    }
    if (!found) {
      throw new IllegalArgumentException("projection document-order test fixture does not contain slot " + afterSlot);
    }
    return new ProjectionIndexFences.DocumentPosition(predecessors, successors);
  }

  private static int documentSkipHeightForTest(final int physicalSlot, final int levels) {
    long mixed = physicalSlot * 0x9E3779B97F4A7C15L;
    mixed ^= mixed >>> 33;
    mixed *= 0xC2B2AE3D27D4EB4FL;
    mixed ^= mixed >>> 29;
    return Math.min(levels, Long.numberOfTrailingZeros(mixed | (1L << (levels - 1))) + 1);
  }

  @Test
  void writeRejectsMisalignedFenceArrays() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      // Exactly one entry per leaf: too short (out-of-bounds) and too long (stale trailing
      // entries read() would ignore) both fail loudly before any slot is written.
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionIndexFences.write(storage, 4, new long[3], new long[4]));
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionIndexFences.write(storage, 4, new long[4], new long[5]));
    }
  }

  @Test
  void chunkCountRoundsUp() {
    assertEquals(0, ProjectionIndexFences.chunkCount(0));
    assertEquals(1, ProjectionIndexFences.chunkCount(1));
    assertEquals(1, ProjectionIndexFences.chunkCount(ProjectionIndexFences.CHUNK_LEAVES));
    assertEquals(2, ProjectionIndexFences.chunkCount(ProjectionIndexFences.CHUNK_LEAVES + 1));
  }

  @Test
  void writeReadRoundTripSpanningMultipleChunks() {
    // 40 leaves = one full 32-entry chunk plus an 8-entry tail chunk.
    final int n = ProjectionIndexFences.CHUNK_LEAVES + 8;
    final long[][] rng = ranges(n, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, n, rng[0], rng[1]);
        final long[][] readBack = ProjectionIndexFences.read(storage, n);
        assertArrayEquals(rng[0], readBack[0], "same-trx first fences");
        assertArrayEquals(rng[1], readBack[1], "same-trx last fences");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      // Fences are read writer-side (the maintenance path reads them at the top
      // of a commit); a fresh write transaction reads the committed state.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final long[][] readBack = ProjectionIndexFences.read(storage, n);
        assertArrayEquals(rng[0], readBack[0], "cold first fences");
        assertArrayEquals(rng[1], readBack[1], "cold last fences");
      }
    }
  }

  @Test
  void buildWriterStreamsCompletedChunksAcrossTransactionEpochs() {
    final int rowGroups = ProjectionIndexFences.CHUNK_LEAVES + 8;
    final long[][] expected = ranges(rowGroups, 11);
    final ProjectionIndexFences.BuildWriter writer = new ProjectionIndexFences.BuildWriter();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int index = 0; index < ProjectionIndexFences.CHUNK_LEAVES; index++) {
          writer.append(storage, expected[0][index], expected[1][index]);
        }
        assertEquals(1, writer.chunksWritten(), "a completed fence chunk must publish in the epoch that completed it");
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int index = ProjectionIndexFences.CHUNK_LEAVES; index < rowGroups; index++) {
          writer.append(storage, expected[0][index], expected[1][index]);
        }
        assertEquals(1, writer.chunksWritten(), "only the partial second chunk remains before finish");
        writer.finish(storage);
        assertEquals(2, writer.chunksWritten());
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertArrayEquals(expected[0], ProjectionIndexFences.read(storage, rowGroups)[0]);
        assertArrayEquals(expected[1], ProjectionIndexFences.read(storage, rowGroups)[1]);
        assertArrayEquals(java.util.stream.IntStream.rangeClosed(1, rowGroups).toArray(),
            ProjectionIndexFences.readPhysicalOrder(storage, rowGroups));
      }
    }
  }

  @Test
  void exactFullFenceChunkTerminatesAfterFinishAndColdReopen() {
    final int rowGroups = ProjectionIndexFences.CHUNK_LEAVES;
    final long[][] expected = ranges(rowGroups, 17);
    final ProjectionIndexFences.BuildWriter writer = new ProjectionIndexFences.BuildWriter();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int index = 0; index < rowGroups; index++) {
          writer.append(storage, expected[0][index], expected[1][index]);
        }
        assertEquals(1, writer.chunksWritten());
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writer.finish(storage);
        wtx.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertArrayEquals(java.util.stream.IntStream.rangeClosed(1, rowGroups).toArray(),
          ProjectionIndexFences.readPhysicalOrder(rtx.getStorageEngineReader(), INDEX_NUMBER, rowGroups));
    }
  }

  @Test
  void changingOneLeafRewritesOnlyItsChunk() {
    // Physical slots 1..32 are chunk 0; slots 33..40 are chunk 1.
    final int n = ProjectionIndexFences.CHUNK_LEAVES + 8;
    final long[][] rng = ranges(n, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, n, rng[0], rng[1]);
        wtx.commit();
      }
      // Move exactly one leaf that lives in chunk 0; chunk 1 must not be rewritten.
      rng[1][10] += 1;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, n);
        assertEquals(1, fences.findSlot(rng[0][0]));
        assertEquals(n, fences.findSlot(rng[1][n - 1]));
        fences.set(11, rng[0][10], rng[1][10]);
        fences.flush(n);
        assertEquals(1, fences.chunksWritten(), "changing slot 11 must write only 32-entry chunk 0");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      final long chunk0 = ProjectionIndexFences.CHUNK_SLOT_BASE;
      final long chunk1 = ProjectionIndexFences.CHUNK_SLOT_BASE + 1;
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        final long c1r1 =
            ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER, chunk1, 0);
        final long c1r2 =
            ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(), INDEX_NUMBER, chunk1, 0);
        assertTrue(c1r1 >= 0, "chunk 1 present");
        assertEquals(c1r1, c1r2, "unchanged chunk 1 must be shared by reference, not rewritten");
        final long c0r1 =
            ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(), INDEX_NUMBER, chunk0, 0);
        final long c0r2 =
            ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(), INDEX_NUMBER, chunk0, 0);
        assertTrue(c0r1 != c0r2, "the touched chunk 0 must be re-persisted at a new offset");
      }
      // The change is still faithfully reconstructed after the partial rewrite.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertArrayEquals(rng[1], ProjectionIndexFences.read(storage, n)[1], "post-rewrite fences");
      }
    }
  }

  @Test
  void unchangedLogicalFenceDoesNotDirtyItsChunk() {
    final long[][] ranges = ranges(2, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      ProjectionIndexFences.write(storage, 2, ranges[0], ranges[1]);
      wtx.commit();
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexFences.Accessor fences =
          ProjectionIndexFences.open(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER), 2);

      // This is the fence-side shape of a membership rewrite that only adds/removes sparse
      // exception rows: the KEYS payload changes, but its normal min/max do not.
      fences.set(1, ranges[0][0], ranges[1][0]);
      fences.flush(2);

      assertEquals(0, fences.chunksWritten(), "unchanged normal bounds must not re-persist their shared fence chunk");
    }
  }

  @Test
  void localSplitLinksANewPhysicalLeafWithoutRekeyingItsSuffix() {
    final long[][] ranges = ranges(3, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 3, ranges[0], ranges[1]);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);
        final long split = ranges[0][0] + 500;
        fences.set(1, ranges[0][0], split - 1);
        final int splitSlot = fences.allocateSlot();
        assertEquals(4, splitSlot, "three base slots make physical slot 4 the first allocated split");
        fences.set(splitSlot, split, ranges[1][0]);
        fences.linkAfter(1, splitSlot, positionAfterForTest(fences, 1));
        fences.flush(4);
        assertEquals(1, fences.chunksWritten(), "slots 1 and 4 both belong to 32-entry chunk 0");
        assertEquals(4, fences.findSlot(split));
        assertEquals(2, fences.findSlot(ranges[0][1]));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(new int[] {1, 4, 2, 3},
            ProjectionIndexFences.readPhysicalOrder(rtx.getStorageEngineReader(), INDEX_NUMBER, 4));
      }
    }
  }

  @Test
  void linkAfterRejectsANonReciprocalSuccessorBeforeMutation() {
    final long[][] baseRanges = ranges(3, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 3, baseRanges[0], baseRanges[1]);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);
        fences.set(1, baseRanges[0][0], baseRanges[0][0] + 499L);
        fences.flush(3);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        overwriteDocumentLink(storage, 2, DOCUMENT_PREVIOUS_OFFSET, 0);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);
        final int newSlot = fences.allocateSlot();
        assertEquals(4, newSlot);
        fences.set(newSlot, baseRanges[0][0] + 500L, baseRanges[1][0]);

        assertThrows(IllegalStateException.class, () -> fences.linkAfter(1, newSlot, fences.documentTailPosition()));
        assertEquals(2, fences.next(1), "failed splice must preserve the corrupt source edge");
        assertEquals(0, fences.previous(2));
        assertEquals(0, fences.ownerBase(newSlot), "failed splice must not assign an owner");
        assertEquals(0, fences.previous(newSlot), "failed splice must not assign a predecessor");
        assertEquals(0, fences.next(newSlot), "failed splice must not assign a successor");
        assertEquals(1, fences.documentHead());
        assertEquals(3, fences.lastPhysicalSlot());
      }
    }
  }

  @Test
  void linkAfterRejectsARecycledSuccessorBeforeMutation() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 2, new long[] {10L, 50L}, new long[] {39L, 59L});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
        fences.set(1, 10L, 19L);
        final int middleSlot = fences.allocateSlot();
        assertEquals(3, middleSlot);
        fences.set(middleSlot, 20L, 29L);
        fences.linkAfter(1, middleSlot, positionAfterForTest(fences, 1));
        final int tailSlot = fences.allocateSlot();
        assertEquals(4, tailSlot);
        fences.set(tailSlot, 30L, 39L);
        fences.linkAfter(middleSlot, tailSlot, positionAfterForTest(fences, middleSlot));
        fences.recycle(middleSlot, positionAfterForTest(fences, middleSlot));
        fences.recycle(tailSlot, positionAfterForTest(fences, tailSlot));
        fences.flush(2);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        overwriteDocumentLink(storage, 1, DOCUMENT_NEXT_OFFSET, 3);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
        assertFalse(fences.isLivePhysicalSlot(3));
        assertFalse(fences.isLivePhysicalSlot(4));
        final int newSlot = fences.allocateSlot();
        assertEquals(4, newSlot, "slot 4 is the free-list head, leaving corrupt successor 3 recycled");
        fences.set(newSlot, 20L, 29L);

        assertThrows(IllegalStateException.class, () -> fences.linkAfter(1, newSlot, fences.documentTailPosition()));
        assertFalse(fences.isLivePhysicalSlot(3));
        assertEquals(3, fences.next(1), "failed splice must preserve the corrupt recycled edge");
        assertEquals(1, fences.previous(2));
        assertEquals(0, fences.ownerBase(newSlot), "failed splice must not assign an owner");
        assertEquals(0, fences.previous(newSlot));
        assertEquals(0, fences.next(newSlot));
        assertEquals(1, fences.documentHead());
        assertEquals(2, fences.lastPhysicalSlot());
      }
    }
  }

  @Test
  void recycleRejectsANonReciprocalSuccessorBeforeMutation() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 2, new long[] {10L, 50L}, new long[] {39L, 59L});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
        fences.set(1, 10L, 19L);
        final int splitSlot = fences.allocateSlot();
        assertEquals(3, splitSlot);
        fences.set(splitSlot, 20L, 39L);
        fences.linkAfter(1, splitSlot, positionAfterForTest(fences, 1));
        fences.flush(3);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        overwriteDocumentLink(storage, 2, DOCUMENT_PREVIOUS_OFFSET, 1);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);
        assertEquals(3, fences.findSlot(25L));

        assertThrows(IllegalStateException.class, () -> fences.recycle(3, fences.documentTailPosition()));
        assertEquals(3, fences.liveRowGroupCount());
        assertTrue(fences.isLivePhysicalSlot(3));
        assertEquals(3, fences.next(1), "failed recycle must not unlink its predecessor");
        assertEquals(1, fences.previous(3));
        assertEquals(2, fences.next(3));
        assertEquals(1, fences.previous(2), "failed recycle must preserve the corrupt reciprocal edge");
        assertEquals(3, fences.findSlot(25L), "numeric routing must remain linked after validation fails");
        assertEquals(1, fences.documentHead());
        assertEquals(2, fences.lastPhysicalSlot());
      }
    }
  }

  @Test
  void recycleRejectsARecycledSuccessorBeforeMutation() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 2, new long[] {10L, 50L}, new long[] {39L, 59L});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
        fences.set(1, 10L, 19L);
        final int middleSlot = fences.allocateSlot();
        assertEquals(3, middleSlot);
        fences.set(middleSlot, 20L, 29L);
        fences.linkAfter(1, middleSlot, positionAfterForTest(fences, 1));
        final int tailSlot = fences.allocateSlot();
        assertEquals(4, tailSlot);
        fences.set(tailSlot, 30L, 39L);
        fences.linkAfter(middleSlot, tailSlot, positionAfterForTest(fences, middleSlot));
        fences.recycle(tailSlot, positionAfterForTest(fences, tailSlot));
        fences.flush(3);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        overwriteDocumentLink(storage, 3, DOCUMENT_NEXT_OFFSET, 4);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);
        assertFalse(fences.isLivePhysicalSlot(4));
        assertEquals(3, fences.findSlot(25L));

        assertThrows(IllegalStateException.class, () -> fences.recycle(3, fences.documentTailPosition()));
        assertEquals(3, fences.liveRowGroupCount());
        assertTrue(fences.isLivePhysicalSlot(3));
        assertFalse(fences.isLivePhysicalSlot(4));
        assertEquals(3, fences.next(1));
        assertEquals(1, fences.previous(3));
        assertEquals(4, fences.next(3), "failed recycle must preserve the corrupt recycled edge");
        assertEquals(3, fences.previous(2));
        assertEquals(3, fences.findSlot(25L), "numeric routing must remain linked after validation fails");
        assertEquals(1, fences.documentHead());
        assertEquals(2, fences.lastPhysicalSlot());
      }
    }
  }

  @Test
  void repeatedLocalSplitsRemainLogarithmicallySearchable() {
    final int leaves = 4096;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 1, new long[] {1L}, new long[] {leaves});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 1);
        // Shrink the base leaf's live range without shrinking its immutable numeric ownership bound.
        fences.set(1, 1L, 1L);
        for (int key = 2; key <= leaves; key++) {
          final int slot = fences.allocateSlot();
          assertEquals(key, slot, "fresh split slots must be allocated in physical-id order");
          fences.set(slot, key, key);
          fences.linkAfter(slot - 1, slot, fences.documentTailPosition());
        }
        for (int iteration = 0; iteration < 512; iteration++) {
          fences.recycle(leaves, fences.documentTailPosition());
          assertEquals(leaves, fences.allocateSlot());
          fences.set(leaves, leaves, leaves);
          fences.linkAfter(leaves - 1, leaves, fences.documentTailPosition());
        }
        fences.flush(leaves);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER), leaves);
        assertEquals(leaves, fences.findSlot(leaves));
        assertTrue(fences.chunksRead() <= 32, "a cold lookup must touch only logarithmically many fence chunks");
      }
    }
  }

  @Test
  void livePhysicalSlotCanExceedLiveRowGroupCountAfterRecycle() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 1, new long[] {10L}, new long[] {39L});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 1);
        fences.set(1, 10L, 19L);
        final int middleSlot = fences.allocateSlot();
        assertEquals(2, middleSlot);
        fences.set(middleSlot, 20L, 29L);
        fences.linkAfter(1, middleSlot, fences.documentTailPosition());
        final int tailSlot = fences.allocateSlot();
        assertEquals(3, tailSlot);
        fences.set(tailSlot, 30L, 39L);
        fences.linkAfter(middleSlot, tailSlot, fences.documentTailPosition());
        fences.recycle(middleSlot, positionAfterForTest(fences, middleSlot));

        assertEquals(2, fences.liveRowGroupCount());
        assertEquals(3, fences.physicalRowGroupCount());
        assertFalse(fences.isLivePhysicalSlot(2), "the recycled hole is not a row group");
        assertTrue(fences.isLivePhysicalSlot(3),
            "physical id 3 remains live even though the live cardinality is only 2");
        fences.flush(2);
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
        assertFalse(fences.isLivePhysicalSlot(2), "cold-opened free-list membership");
        assertTrue(fences.isLivePhysicalSlot(3), "cold-opened live slot above live count");
        assertEquals(3, fences.findSlot(35L));
        assertArrayEquals(new int[] {1, 3}, ProjectionIndexFences.readPhysicalOrder(storage, 2));
        assertEquals(2, fences.allocateSlot(), "the free physical id is reused without renumbering slot 3");
        assertTrue(fences.isLivePhysicalSlot(2));
      }
    }
  }

  @Test
  void exceptionOnlyLeafStaysInDocumentOrderButNotNormalRouting() {
    final long[] first = {2L, Long.MAX_VALUE, 8L};
    final long[] last = {5L, Long.MIN_VALUE, 10L};
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      ProjectionIndexFences.write(storage, 3, first, last);
      final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 3);

      assertEquals(Long.MAX_VALUE, fences.first(2));
      assertEquals(Long.MIN_VALUE, fences.last(2));
      assertArrayEquals(new int[] {1, 2, 3}, ProjectionIndexFences.readPhysicalOrder(storage, 3),
          "exception-only physical leaf 2 remains part of document order");
      assertEquals(1, fences.findSlot(3L));
      assertEquals(-1, fences.findSlot(6L), "no normal fence covers the gap occupied by exception rows");
      assertEquals(3, fences.findSlot(9L));
      assertEquals(-1, fences.findSlot(100L));
    }
  }

  @Test
  void touchedExceptionOnlyLeafValidationReadsOnlyAdjacentFenceChunks() {
    final int rowGroupCount = ProjectionIndexFences.CHUNK_LEAVES * 64 + 2;
    final long[] first = new long[rowGroupCount];
    final long[] last = new long[rowGroupCount];
    Arrays.fill(first, Long.MAX_VALUE);
    Arrays.fill(last, Long.MIN_VALUE);
    first[0] = 2L;
    last[0] = 2L;
    first[rowGroupCount - 1] = 8L;
    last[rowGroupCount - 1] = 8L;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        ProjectionIndexFences.write(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER),
            rowGroupCount, first, last);
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER), rowGroupCount);
        // Put the touched leaf first in chunk 32: its predecessor is in chunk 31 and its successor
        // in chunk 32. The nearest normal leaves are 1,024 exception-only entries away in each
        // direction; the old validation walked all 65 chunks to reach them.
        final int touchedSlot = ProjectionIndexFences.CHUNK_LEAVES * 32 + 1;
        fences.validateTouchedNormalBounds(touchedSlot);

        assertEquals(2, fences.chunksRead(),
            "exception-only validation must read only the touched/adjacent document-link chunks");
      }
    }
  }

  @Test
  void touchedNormalValidationUsesBaseOwnershipAndNumericNeighbors() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      ProjectionIndexFences.write(storage, 2, new long[] {10L, 50L}, new long[] {39L, 59L});
      final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 2);
      fences.set(1, 10L, 19L);
      final int splitSlot = fences.allocateSlot();
      fences.set(splitSlot, 20L, 30L);
      fences.linkAfter(1, splitSlot, positionAfterForTest(fences, 1));

      fences.validateTouchedNormalBounds(1);
      fences.validateTouchedNormalBounds(splitSlot);

      fences.set(1, 10L, 25L);
      final IllegalStateException overlap =
          assertThrows(IllegalStateException.class, () -> fences.validateTouchedNormalBounds(1));
      assertTrue(overlap.getMessage().contains("base 1"));

      fences.set(1, 10L, 19L);
      fences.set(2, 35L, 45L);
      final IllegalStateException escapedOwner =
          assertThrows(IllegalStateException.class, () -> fences.validateTouchedNormalBounds(2));
      assertTrue(escapedOwner.getMessage().contains("escapes base 2 ownership"));
    }
  }

  @Test
  void shrinkingABaseFencePreservesItsNumericOwnershipHighWater() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, 1, new long[] {0L}, new long[] {100L});
        final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, 1);
        fences.set(1, 0L, 10L);
        final int splitSlot = fences.allocateSlot();
        assertEquals(2, splitSlot);
        fences.set(splitSlot, 20L, 30L);
        fences.linkAfter(1, splitSlot, fences.documentTailPosition());

        assertEquals(100L, fences.maxRecordKey(), "base ownership must not shrink with its live fence");
        assertEquals(2, fences.findSlot(25L));
        assertEquals(-1, fences.findSlot(80L), "ownership high-water is routing scope, not a match");
        fences.flush(2);
        assertEquals(1, fences.chunksWritten(), "base and split both touch only 32-entry chunk 0");
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexFences.Accessor fences =
            ProjectionIndexFences.open(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER), 2);
        assertEquals(100L, fences.maxRecordKey());
        assertEquals(2, fences.findSlot(25L));
        assertEquals(-1, fences.findSlot(80L));
      }
    }
  }

  @Test
  void readReturnsNullWhenAChunkIsMissing() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // Nothing written: reading a non-empty zone map must report the gap.
        assertNull(ProjectionIndexFences.read(storage, 4), "no chunks → null");
        // Initialise 33 leaves, then remove only its one-entry chunk 1. This is an actual
        // missing-chunk fixture rather than a populated-tree replacement or row-count/header mismatch.
        final int twoChunkCount = ProjectionIndexFences.CHUNK_LEAVES + 1;
        final long[][] two = ranges(twoChunkCount, 5);
        ProjectionIndexFences.write(storage, twoChunkCount, two[0], two[1]);
        assertArrayEquals(two[0], ProjectionIndexFences.read(storage, twoChunkCount)[0]);
        storage.tombstoneBlob(ProjectionIndexFences.CHUNK_SLOT_BASE + 1);
        assertNull(ProjectionIndexFences.read(storage, twoChunkCount), "persisted chunk 1 absent → null");
      }
    }
  }
}
