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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage for {@link ProjectionIndexFences} — the chunked, carry-forward
 * store for the projection's per-leaf record-key zone map. The decisive test is
 * {@link #changingOneLeafRewritesOnlyItsChunk}: the storage-level proof that a
 * commit re-persists only the fence chunk whose leaf moved, sharing every other
 * chunk's page by reference (the whole point of moving the fences out of the
 * single slot-0 blob).
 */
final class ProjectionIndexFencesTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

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
          () -> ProjectionIndexFences.write(storage, 4, new long[3], new long[4], 0));
      assertThrows(IllegalArgumentException.class,
          () -> ProjectionIndexFences.write(storage, 4, new long[4], new long[5], 0));
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
    // 600 leaves = one full chunk (512) + a partial tail chunk (88).
    final int n = ProjectionIndexFences.CHUNK_LEAVES + 88;
    final long[][] rng = ranges(n, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, n, rng[0], rng[1], 0);
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
  void changingOneLeafRewritesOnlyItsChunk() {
    final int n = ProjectionIndexFences.CHUNK_LEAVES + 88; // two chunks
    final long[][] rng = ranges(n, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, n, rng[0], rng[1], 0);
        wtx.commit();
      }
      // Move exactly one leaf that lives in chunk 0; chunk 1 must not be rewritten.
      rng[1][10] += 1;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, n, rng[0], rng[1], n);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      final long chunk0 = ProjectionIndexFences.CHUNK_SLOT_BASE;
      final long chunk1 = ProjectionIndexFences.CHUNK_SLOT_BASE + 1;
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
           JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        final long c1r1 = ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(),
            INDEX_NUMBER, chunk1, 0);
        final long c1r2 = ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(),
            INDEX_NUMBER, chunk1, 0);
        assertTrue(c1r1 >= 0, "chunk 1 present");
        assertEquals(c1r1, c1r2, "unchanged chunk 1 must be shared by reference, not rewritten");
        final long c0r1 = ProjectionIndexHOTStorage.segmentPageOffset(r1.getStorageEngineReader(),
            INDEX_NUMBER, chunk0, 0);
        final long c0r2 = ProjectionIndexHOTStorage.segmentPageOffset(r2.getStorageEngineReader(),
            INDEX_NUMBER, chunk0, 0);
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
  void shrinkTombstonesOrphanChunks() {
    final int wide = ProjectionIndexFences.CHUNK_LEAVES + 88; // two chunks
    final int narrow = ProjectionIndexFences.CHUNK_LEAVES - 100; // one chunk
    final long[][] wideR = ranges(wide, 5);
    final long[][] narrowR = ranges(narrow, 5);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, wide, wideR[0], wideR[1], 0);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        ProjectionIndexFences.write(storage, narrow, narrowR[0], narrowR[1], wide);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        // The orphaned second chunk is gone (reader-side static read).
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER,
            ProjectionIndexFences.CHUNK_SLOT_BASE + 1), "orphan chunk tombstoned");
      }
      // The surviving fences read back exactly.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        assertArrayEquals(narrowR[0], ProjectionIndexFences.read(storage, narrow)[0]);
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
        // One chunk written, but ask for more than it covers → the missing tail chunk is a gap.
        final long[][] one = ranges(ProjectionIndexFences.CHUNK_LEAVES, 5);
        ProjectionIndexFences.write(storage, ProjectionIndexFences.CHUNK_LEAVES, one[0], one[1], 0);
        assertNull(ProjectionIndexFences.read(storage, ProjectionIndexFences.CHUNK_LEAVES + 1),
            "second chunk absent → null");
        // Reading exactly what was written succeeds.
        assertArrayEquals(one[0],
            ProjectionIndexFences.read(storage, ProjectionIndexFences.CHUNK_LEAVES)[0]);
      }
    }
  }
}
