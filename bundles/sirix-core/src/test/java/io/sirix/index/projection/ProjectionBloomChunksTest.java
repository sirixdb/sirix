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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage, publication, canonical-format, and fail-open coverage for {@link ProjectionBloomChunks}.
 */
final class ProjectionBloomChunksTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final byte[] COLUMN_KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

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

  @Test
  void keyRangesAndChunkBoundariesAreExact() {
    assertEquals(0, ProjectionBloomChunks.chunkCount(0));
    assertEquals(1, ProjectionBloomChunks.chunkCount(1));
    assertEquals(1, ProjectionBloomChunks.chunkCount(ProjectionBloomChunks.CHUNK_LEAVES));
    assertEquals(2, ProjectionBloomChunks.chunkCount(ProjectionBloomChunks.CHUNK_LEAVES + 1));

    final long largestRowGroupSlot =
        ProjectionIndexHOTStorage.columnSegmentSlotKey(ProjectionIndexHOTStorage.MAX_ROW_GROUPS, 0xFFFE);
    final long firstChunk = ProjectionBloomChunks.chunkSlotKey(0, 0);
    final long lastChunk = ProjectionBloomChunks.chunkSlotKey(RowGroupDescriptor.MAX_COLUMNS - 1, 0xFFFF);
    final long firstSetSummary = ProjectionSetSummaryChunks.slotKey(0);
    final long lastSetSummary = ProjectionSetSummaryChunks.slotKey(RowGroupDescriptor.MAX_COLUMNS - 1);
    assertTrue(largestRowGroupSlot < ProjectionIndexFences.CHUNK_SLOT_BASE,
        "row-group namespace must end before fences");
    assertTrue(ProjectionIndexFences.CHUNK_SLOT_BASE < firstChunk, "Bloom chunks must start after fences");
    assertTrue(
        ProjectionIndexFences.CHUNK_SLOT_BASE < ProjectionIndexFences.ORDER_HEADER_SLOT
            && ProjectionIndexFences.ORDER_HEADER_SLOT < firstChunk,
        "row-group order header must stay between fence and Bloom namespaces");
    assertTrue(lastChunk < 1L << 44, "Bloom chunk namespace must stay below 2^44");
    assertTrue(lastChunk < firstSetSummary, "set-summary chunks must start after Bloom chunks");
    assertTrue(lastSetSummary < 1L << 45, "set-summary chunk namespace must stay below 2^45");
    assertEquals(ProjectionBloomChunks.CHUNK_SLOT_BASE + 0xFFFF, ProjectionBloomChunks.chunkSlotKey(0, 0xFFFF));
    assertEquals(ProjectionBloomChunks.CHUNK_SLOT_BASE + 0x1_0000, ProjectionBloomChunks.chunkSlotKey(1, 0));
    assertTrue(
        ProjectionIndexHOTStorage.bloomBlockSlotKey(
            RowGroupDescriptor.MAX_COLUMNS - 1) < ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1),
        "metadata/manifests must end before the first composite row-group slot");

    final Set<Long> familyBoundaries = new HashSet<>();
    familyBoundaries.add(0L); // metadata
    familyBoundaries.add(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
    familyBoundaries.add(ProjectionIndexHOTStorage.bloomBlockSlotKey(RowGroupDescriptor.MAX_COLUMNS - 1));
    familyBoundaries.add(ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
    familyBoundaries.add(largestRowGroupSlot);
    familyBoundaries.add(ProjectionIndexFences.CHUNK_SLOT_BASE);
    familyBoundaries.add(ProjectionIndexFences.CHUNK_SLOT_BASE
        + ProjectionIndexFences.chunkCount(ProjectionIndexHOTStorage.MAX_ROW_GROUPS) - 1L);
    familyBoundaries.add(ProjectionIndexFences.ORDER_HEADER_SLOT);
    familyBoundaries.add(firstChunk);
    familyBoundaries.add(lastChunk);
    familyBoundaries.add(firstSetSummary);
    familyBoundaries.add(lastSetSummary);
    assertEquals(12, familyBoundaries.size(), "reserved key-family boundaries must be pairwise distinct");

    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.chunkSlotKey(-1, 0));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionBloomChunks.chunkSlotKey(RowGroupDescriptor.MAX_COLUMNS, 0));
    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.chunkSlotKey(0, -1));
    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.chunkSlotKey(0, 0x1_0000));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1L));

    assertThrows(IllegalArgumentException.class, () -> new ProjectionIndexMetadata("", new String[0], new String[0],
        new byte[0], ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1, 0));
    final byte[] oversizedWire = new ProjectionIndexMetadata("", new String[0], new String[0], new byte[0],
        ProjectionIndexHOTStorage.MAX_ROW_GROUPS, 0).serialize();
    ProjectionIndexRowGroupCodec.putIntLEAt(oversizedWire, Integer.BYTES + 2,
        ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexMetadata.parse(oversizedWire));
  }

  @Test
  void fullChunkIsEagerButManifestPublishesOnlyAtFinishAndColdReads() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= ProjectionBloomChunks.CHUNK_LEAVES; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)),
            "the full chunk must be persisted eagerly");
        assertNull(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0)),
            "no manifest may expose a partial build");
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS,
            ProjectionBloomChunks.CHUNK_LEAVES), "committed chunks without a manifest are invisible");
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final int tailId = ProjectionBloomChunks.CHUNK_LEAVES + 1;
        writer.append(encoded, tailId, storage);
        writer.finishChunks(storage, tailId, COLUMN_KINDS);
        assertNull(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0)),
            "finishing data chunks alone must not publish them");
        writer.publishManifests(storage, tailId);
        final byte[] manifest = storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
        assertTrue(ProjectionBloomChunks.isManifest(manifest, tailId));
        assertEquals(-1, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(manifest),
            "a manifest must never be accepted as a chunk payload");
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence = ProjectionBloomChunks.read(rtx.getStorageEngineReader(),
            INDEX_NUMBER, COLUMN_KINDS, ProjectionBloomChunks.CHUNK_LEAVES + 1);
        assertNotNull(evidence, "manifest and both chunks must survive a cold read");
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final long present = ProjectionIndexColumnSegmentCodec.bloomHash("present".getBytes(StandardCharsets.UTF_8));
        final long[] keep = prune(evidence[0], ProjectionBloomChunks.CHUNK_LEAVES + 1, present, fetcher);
        assertKept(keep, 0);
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES - 1);
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES);
      }
    } finally {
      writer.release();
    }
  }

  @Test
  void writerRetainsOnlyTheCurrentChunkWindowAcrossManyLeaves() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES * 8 + 17;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
        writer.append(encoded, rowGroupId, storage);
        final int expectedPending = rowGroupId % ProjectionBloomChunks.CHUNK_LEAVES;
        assertEquals(expectedPending, writer.pendingLeavesForTesting());
        assertEquals(expectedPending, writer.retainedSegmentReferencesForTesting(),
            "one string column may retain only its current 256-leaf window");
      }
      assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 7)),
          "completed windows must already be persistent while only the tail stays retained");
      writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
      assertEquals(0, writer.pendingLeavesForTesting());
      assertEquals(0, writer.retainedSegmentReferencesForTesting());
    } finally {
      writer.release();
    }
  }

  @Test
  void malformedTailChunkKeepsOnlyItsSpan() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES + 1;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
        writer.publishManifests(storage, rowGroupCount);
        // Valid PIXB wrapper/hash, deliberately malformed chunk payload: structural corruption must
        // disable only this chunk, never the whole projection and never manufacture negative proof.
        storage.putBlob(ProjectionBloomChunks.chunkSlotKey(0, 1), new byte[] {1, 2, 3, 4});
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence, "the valid first span remains useful");
        final long rejectedByFirst = hashRejectedBy(bloomSegment(encoded));
        final long[] keep = prune(evidence[0], rowGroupCount, rejectedByFirst,
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertDropped(keep, 0, "valid chunk still prunes");
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES, "malformed tail chunk must fail open");
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.tombstoneBlob(ProjectionBloomChunks.chunkSlotKey(0, 1));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence);
        final long[] keep = prune(evidence[0], rowGroupCount, Long.MIN_VALUE,
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES, "missing tail chunk must fail open");
      }
    } finally {
      writer.release();
    }
  }

  @Test
  void deferredFetchUsesFixedWindowsReleasesPayloadsAndRejectsOuterCorruption() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES * 5;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
        writer.publishManifests(storage, rowGroupCount);
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence);
        assertTrue(ProjectionBloomChunks.retainedBytes(evidence) < 1024,
            "five referenced payloads must retain only primitive locators, not their pages");
        final ProjectionColumnStore.ColumnSegmentFetcher delegate =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final int[] fetchStats = new int[2];
        final ProjectionColumnStore.ColumnSegmentFetcher tracking = offsets -> {
          fetchStats[0]++;
          fetchStats[1] = Math.max(fetchStats[1], offsets.length);
          return delegate.fetchAll(offsets);
        };
        final long rejected = hashRejectedBy(bloomSegment(encoded));
        final long[] keep = prune(evidence[0], rowGroupCount, rejected, tracking);
        assertEquals(2, fetchStats[0], "five chunks must fetch as one four-chunk and one padded window");
        assertEquals(ProjectionBloomChunks.FETCH_WINDOW_CHUNKS, fetchStats[1]);
        assertDropped(keep, 0, "valid first chunk must prune");
        assertDropped(keep, rowGroupCount - 1, "valid final chunk must prune");
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting(),
            "caller-scoped scratch must release all page payloads after pruning");

        final ProjectionColumnStore.ColumnSegmentFetcher corrupting = offsets -> {
          final byte[][] pages = delegate.fetchAll(offsets);
          for (int i = 0; i < pages.length; i++) {
            if (pages[i] != null) {
              pages[i] = pages[i].clone();
              pages[i][pages[i].length - 1] ^= 1;
            }
          }
          return pages;
        };
        final long[] corruptKeep = prune(evidence[0], rowGroupCount, rejected, corrupting);
        assertKept(corruptKeep, 0, "outer hash mismatch must fail open");
        assertKept(corruptKeep, rowGroupCount - 1, "every corrupt deferred span must stay kept");
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting());
      }
    } finally {
      writer.release();
    }
  }

  @Test
  void pruneManyNarrowsEveryMaskLikeOnePruneEachInOneWalk() {
    // Five chunks; leaf i holds "v-" + (i % 37): every literal has a known home set, spread over
    // every chunk, and 37 distinct values keep each leaf's filter small enough for real rejections.
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES * 5;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup[] byValue =
        new ProjectionIndexColumnSegmentCodec.EncodedRowGroup[37];
    for (int v = 0; v < byValue.length; v++) {
      byValue[v] = encodedRowGroup("v-" + v);
    }
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          writer.append(byValue[(rowGroupId - 1) % 37], rowGroupId, storage);
        }
        writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
        writer.publishManifests(storage, rowGroupCount);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence);
        assertEquals(5, evidence[0].chunkCount());
        final ProjectionColumnStore.ColumnSegmentFetcher delegate =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final int[] fetches = new int[1];
        final ProjectionColumnStore.ColumnSegmentFetcher tracking = offsets -> {
          fetches[0]++;
          return delegate.fetchAll(offsets);
        };
        final String[] literals = {"v-0", "v-36", "v-5", "absent-a", "v-18", "absent-b", "v-1", "v-1"};
        final long[] hashes = new long[literals.length];
        for (int j = 0; j < hashes.length; j++) {
          hashes[j] = ProjectionIndexColumnSegmentCodec.bloomHash(literals[j].getBytes(StandardCharsets.UTF_8));
        }
        // One walk for all literals ...
        final long[][] many = new long[hashes.length][];
        for (int j = 0; j < hashes.length; j++) {
          many[j] = filled(rowGroupCount);
        }
        fetches[0] = 0;
        final long manyDropped = evidence[0].pruneMany(hashes, many, rowGroupCount, tracking, 0, 5);
        assertEquals(2, fetches[0], "eight literals over five chunks fetch two windows, once");
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting());
        // ... equals one walk per literal.
        long singleDropped = 0;
        for (int j = 0; j < hashes.length; j++) {
          final long[] one = filled(rowGroupCount);
          singleDropped += evidence[0].prune(hashes[j], one, rowGroupCount, delegate);
          assertArrayEquals(one, many[j], "literal " + literals[j]);
        }
        assertEquals(singleDropped, manyDropped);
        // No false negatives: every home leaf of a present literal survives; absent ones lose leaves.
        for (int leaf = 0; leaf < rowGroupCount; leaf++) {
          final int v = leaf % 37;
          if (v == 0) {
            assertKept(many[0], leaf);
          }
          if (v == 36) {
            assertKept(many[1], leaf);
          }
          if (v == 5) {
            assertKept(many[2], leaf);
          }
          if (v == 18) {
            assertKept(many[4], leaf);
          }
          if (v == 1) {
            assertKept(many[6], leaf);
            assertKept(many[7], leaf);
          }
        }
        assertTrue(cardinality(many[3]) < rowGroupCount / 2, "absent-a is rejected somewhere");
        assertTrue(cardinality(many[5]) < rowGroupCount / 2, "absent-b is rejected somewhere");
        assertTrue(cardinality(many[0]) < rowGroupCount / 2, "v-0 lives on 1/37 of the leaves");
        assertArrayEquals(many[6], many[7], "the same literal twice narrows both masks identically");
        // A split walk over chunk ranges equals the whole walk (this is what the parallel path runs).
        final long[][] split = new long[hashes.length][];
        for (int j = 0; j < hashes.length; j++) {
          split[j] = filled(rowGroupCount);
        }
        final long splitDropped = evidence[0].pruneMany(hashes, split, rowGroupCount, delegate, 0, 2)
            + evidence[0].pruneMany(hashes, split, rowGroupCount, delegate, 2, 5);
        assertEquals(manyDropped, splitDropped);
        for (int j = 0; j < hashes.length; j++) {
          assertArrayEquals(many[j], split[j], "split ranges, literal " + literals[j]);
        }
        // An already narrowed mask only loses bits: a literal restricted to one chunk stays there.
        final long[] narrowed = new long[(rowGroupCount + 63) >>> 6];
        for (int leaf = ProjectionBloomChunks.CHUNK_LEAVES; leaf < 2 * ProjectionBloomChunks.CHUNK_LEAVES; leaf++) {
          narrowed[leaf >>> 6] |= 1L << (leaf & 63);
        }
        evidence[0].pruneMany(new long[] {hashes[0]}, new long[][] {narrowed}, rowGroupCount, delegate, 0, 5);
        for (int leaf = 0; leaf < rowGroupCount; leaf++) {
          final boolean inChunk = leaf >= ProjectionBloomChunks.CHUNK_LEAVES && leaf < 2 * ProjectionBloomChunks.CHUNK_LEAVES;
          final boolean bit = (narrowed[leaf >>> 6] & 1L << (leaf & 63)) != 0;
          assertTrue(inChunk || !bit, "leaf " + leaf + " outside the narrowed chunk must stay dropped");
          assertTrue(!(inChunk && leaf % 37 == 0) || bit, "home leaf " + leaf + " inside the chunk survives");
        }
        assertThrows(IllegalArgumentException.class,
            () -> evidence[0].pruneMany(hashes, new long[hashes.length - 1][], rowGroupCount, delegate, 0, 5));
        assertThrows(IllegalArgumentException.class,
            () -> evidence[0].pruneMany(hashes, many, rowGroupCount, delegate, 3, 2));
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting());
      }
    } finally {
      writer.release();
    }
  }

  private static long[] filled(final int leafCount) {
    final long[] keep = new long[(leafCount + 63) >>> 6];
    Arrays.fill(keep, -1L);
    return keep;
  }

  private static int cardinality(final long[] mask) {
    int bits = 0;
    for (final long w : mask) {
      bits += Long.bitCount(w);
    }
    return bits;
  }

  @Test
  void monolithicBlockInManifestSlotIsRejected() {
    final byte[] bloom = bloomSegment(encodedRowGroup("value"));
    final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(new byte[][] {bloom}, 1);
    assertNotNull(block);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0), block);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 1),
            "the root slot accepts only the canonical PBMF manifest, never a monolithic PBLM block");
      }
    }
  }

  @Test
  void reorderedEvidencePrunesLogicalRatherThanPhysicalLeafPositions() {
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writer.append(encodedRowGroup("first"), 1, storage);
        writer.append(encodedRowGroup("second"), 2, storage);
        writer.append(encodedRowGroup("inserted"), 3, storage);
        writer.finishChunks(storage, 3, COLUMN_KINDS);
        writer.publishManifests(storage, 3);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] physical =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 3);
        assertNotNull(physical);
        final ProjectionBloomChunks.ColumnEvidence[] logical =
            ProjectionBloomChunks.reorder(physical, new int[] {1, 3, 2});
        final long hash = ProjectionIndexColumnSegmentCodec.bloomHash("second".getBytes(StandardCharsets.UTF_8));
        final long[] keep = prune(logical[0], 3, hash,
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));

        assertDropped(keep, 0, "the physical first leaf is logical first");
        assertDropped(keep, 1, "the inserted physical third leaf is logical second");
        assertKept(keep, 2, "physical leaf two must map to logical leaf three");
      }
    } finally {
      writer.release();
    }
  }

  @Test
  void malformedManifestDisablesChunkAcceleration() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writer.append(encoded, 1, storage);
        writer.finishChunks(storage, 1, COLUMN_KINDS);
        writer.publishManifests(storage, 1);
        storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0), new byte[] {9, 8, 7});
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 1),
            "malformed manifest must decline to the per-leaf chain");
      }
    } finally {
      writer.release();
    }
  }

  @Test
  void allEmptyVirginBuildLeavesChunkAbsent() {
    final ProjectionBloomChunks.Writer empty = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        appendChunk(empty, emptyEncodedRowGroup(), storage);
        empty.finishChunks(storage, ProjectionBloomChunks.CHUNK_LEAVES, COLUMN_KINDS);
        empty.publishManifests(storage, ProjectionBloomChunks.CHUNK_LEAVES);
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)),
            "an all-empty virgin span needs no payload chunk");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence = ProjectionBloomChunks.read(rtx.getStorageEngineReader(),
            INDEX_NUMBER, COLUMN_KINDS, ProjectionBloomChunks.CHUNK_LEAVES);
        assertNotNull(evidence, "the manifest remains valid with an intentionally absent empty chunk");
        final long[] keep = prune(evidence[0], ProjectionBloomChunks.CHUNK_LEAVES, Long.MIN_VALUE,
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertKept(keep, 0, "empty/missing span is no negative evidence");
      }
    } finally {
      empty.release();
    }
  }

  @Test
  void maintenanceRewritesOnlyTheTouchedBloomChunk() {
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES + 1;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup before = encodedRowGroup("before");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup after = encodedRowGroup("after");
    final ProjectionBloomChunks.Writer bloomWriter = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          bloomWriter.append(before, rowGroupId, storage);
        }
        bloomWriter.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
        bloomWriter.publishManifests(storage, rowGroupCount);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(1, after);
        final LongOpenHashSet changed = new LongOpenHashSet();
        changed.add(1L);
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, rowGroupCount, changed);
        assertEquals(1, stats.rowGroupsRead());
        assertEquals(1, stats.chunksWritten());
        wtx.commit();
      }

      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx revisionOne = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx revisionTwo = session.beginNodeReadOnlyTrx(2)) {
        final long changedBefore = ProjectionIndexHOTStorage.segmentPageOffset(revisionOne.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.chunkSlotKey(0, 0), 0);
        final long changedAfter = ProjectionIndexHOTStorage.segmentPageOffset(revisionTwo.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.chunkSlotKey(0, 0), 0);
        final long untouchedBefore = ProjectionIndexHOTStorage.segmentPageOffset(revisionOne.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.chunkSlotKey(0, 1), 0);
        final long untouchedAfter = ProjectionIndexHOTStorage.segmentPageOffset(revisionTwo.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.chunkSlotKey(0, 1), 0);
        assertNotEquals(changedBefore, changedAfter);
        assertEquals(untouchedBefore, untouchedAfter);
      }
    } finally {
      bloomWriter.release();
    }
  }

  @Test
  void numericMaintenanceDoesNotReadBloomChunks() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      final LongOpenHashSet changed = new LongOpenHashSet();
      changed.add(1L);

      final ProjectionBloomChunks.RewriteStats stats = ProjectionBloomChunks.rewriteTouchedChunks(storage,
          new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 1, changed);

      assertEquals(new ProjectionBloomChunks.RewriteStats(0, 0, 0L, 0L), stats);

      changed.add(2L);
      assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.rewriteTouchedChunks(storage,
          new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 1, changed));
    }
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedRowGroup(final String value) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone());
    page.appendRow(1L, new long[] {0L}, new boolean[] {false}, new String[] {value}, new boolean[] {true},
        new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
    return ProjectionIndexColumnSegmentCodec.encode(page.serialize());
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup emptyEncodedRowGroup() {
    return ProjectionIndexColumnSegmentCodec.encode(
        new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone()).serialize());
  }

  private static void appendChunk(final ProjectionBloomChunks.Writer writer,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final ProjectionIndexHOTStorage storage) {
    for (int rowGroupId = 1; rowGroupId <= ProjectionBloomChunks.CHUNK_LEAVES; rowGroupId++) {
      writer.append(encoded, rowGroupId, storage);
    }
  }

  private static byte[] bloomSegment(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    final int bloomId = ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(0);
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == bloomId) {
        return encoded.segments()[i];
      }
    }
    throw new AssertionError("encoded string row group carries no Bloom segment");
  }

  private static long hashRejectedBy(final byte[] bloomSegment) {
    for (int i = 0; i < 100_000; i++) {
      final long hash = ProjectionIndexColumnSegmentCodec.bloomHash(("absent-" + i).getBytes(StandardCharsets.UTF_8));
      if (!ProjectionIndexColumnSegmentCodec.bloomMayContainHash(bloomSegment, hash)) {
        return hash;
      }
    }
    throw new AssertionError("implausible Bloom filter: admitted every absent probe");
  }

  private static long[] prune(final ProjectionBloomChunks.ColumnEvidence evidence, final int leafCount, final long hash,
      final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
    final long[] keep = new long[(leafCount + 63) >>> 6];
    Arrays.fill(keep, -1L);
    assertTrue(evidence.prune(hash, keep, leafCount, fetcher) >= 0);
    return keep;
  }

  private static void assertKept(final long[] keep, final int leaf) {
    assertKept(keep, leaf, "leaf " + leaf + " must remain kept");
  }

  private static void assertKept(final long[] keep, final int leaf, final String message) {
    assertTrue((keep[leaf >>> 6] & 1L << (leaf & 63)) != 0, message);
  }

  private static void assertDropped(final long[] keep, final int leaf, final String message) {
    assertFalse((keep[leaf >>> 6] & 1L << (leaf & 63)) != 0, message);
  }
}
