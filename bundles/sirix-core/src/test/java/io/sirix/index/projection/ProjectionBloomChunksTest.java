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
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

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
    assertEquals(0, ProjectionBloomChunks.sealedChunkCount(ProjectionBloomChunks.CHUNK_LEAVES - 1));
    assertEquals(1, ProjectionBloomChunks.sealedChunkCount(ProjectionBloomChunks.CHUNK_LEAVES));
    assertEquals(0, ProjectionBloomChunks.openLeafCount(ProjectionBloomChunks.CHUNK_LEAVES));
    assertEquals(1, ProjectionBloomChunks.openLeafCount(ProjectionBloomChunks.CHUNK_LEAVES + 1));

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
    final long firstTail = ProjectionBloomChunks.tailSlotKey(0, 1);
    final long lastTail =
        ProjectionBloomChunks.tailSlotKey(RowGroupDescriptor.MAX_COLUMNS - 1, ProjectionIndexHOTStorage.MAX_ROW_GROUPS);
    assertTrue(lastSetSummary < firstTail, "tail slots must start after set summaries");
    assertTrue(lastTail < 1L << 45, "tail slots must end before flag-summary chunks");
    assertEquals(ProjectionBloomChunks.tailSlotKey(0, ProjectionIndexHOTStorage.MAX_ROW_GROUPS) + 1,
        ProjectionBloomChunks.tailSlotKey(1, 1), "adjacent columns have disjoint tail ranges");
    familyBoundaries.add(firstTail);
    familyBoundaries.add(lastTail);
    assertEquals(14, familyBoundaries.size(), "reserved key-family boundaries must be pairwise distinct");
    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.tailSlotKey(-1, 1));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionBloomChunks.tailSlotKey(RowGroupDescriptor.MAX_COLUMNS, 1));
    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.tailSlotKey(0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionBloomChunks.tailSlotKey(0, ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1));
    assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.sealedChunkCount(-1));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionBloomChunks.openLeafCount(ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1));

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
        // Valid PIXB wrapper/hash, deliberately malformed tail payload: structural corruption must
        // disable only this row group's evidence, never the whole projection and never manufacture
        // negative proof. The open chunk is one tail blob per row group, not a block.
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)), "an open chunk has no block");
        assertNotNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupCount)),
            "its row group is a tail blob");
        storage.putBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupCount), new byte[] {1, 2, 3, 4});
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
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES, "malformed tail blob must fail open");
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.tombstoneBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupCount));
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence);
        final long[] keep = prune(evidence[0], rowGroupCount, Long.MIN_VALUE,
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertKept(keep, ProjectionBloomChunks.CHUNK_LEAVES, "missing tail blob must fail open");
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
        final int windows = (evidence[0].chunkCount() + ProjectionBloomChunks.FETCH_WINDOW_CHUNKS - 1)
            / ProjectionBloomChunks.FETCH_WINDOW_CHUNKS;
        assertEquals(windows, fetchStats[0], "five chunks must fetch in whole windows, the last one padded");
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
        assertEquals((5 + ProjectionBloomChunks.FETCH_WINDOW_CHUNKS - 1) / ProjectionBloomChunks.FETCH_WINDOW_CHUNKS,
            fetches[0], "eight literals over five chunks fetch whole windows, once");
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
          final boolean inChunk =
              leaf >= ProjectionBloomChunks.CHUNK_LEAVES && leaf < 2 * ProjectionBloomChunks.CHUNK_LEAVES;
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

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void batchedPruningRetainsExactMatchesAcrossClustersAndLogicalReordering(final boolean selective,
      final boolean reordered) {
    final int rowGroupCount = 65 * ProjectionBloomChunks.CHUNK_LEAVES + 13;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup present = encodedRowGroup("chosen-value");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup absent = encodedRowGroup("different-value");
    final long hash = ProjectionIndexColumnSegmentCodec.bloomHash("chosen-value".getBytes(StandardCharsets.UTF_8));
    assertFalse(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(bloomSegment(absent), hash));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int leaf = 0; leaf < rowGroupCount; leaf++) {
          // Mix clustered values, one isolated exclusion, and an absent interval across different
          // physical chunks. Reversing the partial final word makes physical chunk boundaries
          // disagree with logical mask-word boundaries.
          final boolean excluded = leaf == 0 || (selective
              ? leaf >= rowGroupCount / 2
              : leaf / ProjectionBloomChunks.CHUNK_LEAVES == 16);
          writer.append(excluded
              ? absent
              : present, leaf + 1, storage);
        }
        writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
        writer.publishManifests(storage, rowGroupCount);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] physical =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(physical);
        final int[] order = new int[rowGroupCount];
        for (int logical = 0; logical < rowGroupCount; logical++) {
          order[logical] = reordered
              ? rowGroupCount - logical
              : logical + 1;
        }
        final ProjectionBloomChunks.ColumnEvidence[] mapped = ProjectionBloomChunks.reorder(physical, order);
        assertNotNull(mapped);
        final ProjectionBloomChunks.ColumnEvidence evidence = mapped[0];
        assertEquals(!reordered, evidence.parallelPruningIsSafe(),
            "reordered physical chunks may share a logical mask word and must not race");
        final ProjectionColumnStore.ColumnSegmentFetcher delegate =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final int[] fetches = new int[1];
        final ProjectionColumnStore.ColumnSegmentFetcher tracking = offsets -> {
          fetches[0]++;
          return delegate.fetchAll(offsets);
        };
        final long[] initial = filled(rowGroupCount);
        for (int logical = 17; logical < rowGroupCount; logical += 17) {
          initial[logical >>> 6] &= ~(1L << (logical & 63));
        }
        final long[] exhaustive = initial.clone();
        final long[] single = initial.clone();
        final int fullDropped = evidence.pruneMany(new long[] {hash}, new long[][] {exhaustive}, rowGroupCount,
            delegate, 0, evidence.chunkCount());
        final int singleDropped = evidence.prune(hash, single, rowGroupCount, tracking);
        assertArrayEquals(exhaustive, single);
        assertEquals(fullDropped, singleDropped);
        assertEquals((evidence.chunkCount() + ProjectionBloomChunks.FETCH_WINDOW_CHUNKS - 1)
            / ProjectionBloomChunks.FETCH_WINDOW_CHUNKS, fetches[0]);
        for (int word = 0; word < initial.length; word++) {
          assertEquals(exhaustive[word], single[word] & exhaustive[word], "no exhaustive candidate may be lost");
          assertEquals(single[word], initial[word] & single[word], "pruning may not resurrect excluded bits");
        }
        for (int logical = 0; logical < rowGroupCount; logical++) {
          final int leaf = order[logical] - 1;
          final boolean excluded = leaf == 0 || (selective
              ? leaf >= rowGroupCount / 2
              : leaf / ProjectionBloomChunks.CHUNK_LEAVES == 16);
          if (!excluded && (initial[logical >>> 6] & (1L << (logical & 63))) != 0) {
            assertKept(single, logical, "exact matches must survive every evidence walk");
          }
        }
        final ProjectionColumnStore.ColumnSegmentFetcher unreadable = offsets -> {
          throw new IllegalStateException("injected optional evidence failure");
        };
        final long[] uncertain = initial.clone();
        final int inlineDropped = evidence.prune(hash, uncertain, rowGroupCount, unreadable);
        // Referenced evidence (every sealed block here) fails open when it cannot be fetched. The open
        // chunk's tail blobs are inline in the HOT leaves the reader already holds, so they still
        // prune; every bit they clear must be one the complete walk clears as well, and only in the
        // open chunk.
        int openChunkDrops = 0;
        for (int logical = 0; logical < rowGroupCount; logical++) {
          final long bit = 1L << (logical & 63);
          final boolean droppedHere = (initial[logical >>> 6] & bit) != 0 && (uncertain[logical >>> 6] & bit) == 0;
          if (droppedHere) {
            assertTrue((single[logical >>> 6] & bit) == 0,
                "inline tail evidence may only clear what the full walk clears");
            assertTrue(order[logical] > 65 * ProjectionBloomChunks.CHUNK_LEAVES,
                "only the open chunk's inline tails prune without a fetch");
            openChunkDrops++;
          }
        }
        assertEquals(openChunkDrops, inlineDropped);
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting());

        final List<RowGroupDirectory> directories = new ArrayList<>(rowGroupCount);
        final int[] segmentIds = present.columnSegmentIds();
        final long[] noOffsets = new long[segmentIds.length];
        final byte[][] noInline = new byte[segmentIds.length][];
        for (final int physicalId : order) {
          directories.add(new RowGroupDirectory(physicalId, present.descriptor(), segmentIds, noOffsets, noInline));
        }
        final ProjectionColumnStore store = new ProjectionColumnStore(directories);
        store.attachBloomBlocks(mapped);
        final long otherHash =
            ProjectionIndexColumnSegmentCodec.bloomHash("different-value".getBytes(StandardCharsets.UTF_8));
        final long[] hashes = {hash, otherHash};
        final long[][] fullMasks = {initial.clone(), initial.clone()};
        final long[][] batchedMasks = {initial.clone(), initial.clone()};
        final int fullCount = evidence.pruneMany(hashes, fullMasks, rowGroupCount, delegate, 0, evidence.chunkCount());
        final ProjectionColumnStore.ColumnSegmentFetcher concurrent = new ProjectionColumnStore.ColumnSegmentFetcher() {
          @Override
          public byte[][] fetchAll(final long[] offsets) {
            return delegate.fetchAll(offsets);
          }

          @Override
          public boolean rangedFetchIsConcurrent() {
            return true;
          }
        };
        assertEquals(fullCount, store.applyBloomPruneMany(0, hashes, batchedMasks, concurrent),
            "the shared evidence walk preserves total exclusions for both literals");
        assertArrayEquals(fullMasks[0], batchedMasks[0]);
        assertArrayEquals(fullMasks[1], batchedMasks[1]);
        // One literal through the store takes the same many-literal walk (split over the common pool
        // when the fetcher allows it) and must clear exactly the bits the evidence's own single-literal
        // walk clears.
        final long[] direct = initial.clone();
        final long[] viaStore = initial.clone();
        final int directCount = evidence.prune(hash, direct, rowGroupCount, delegate);
        assertEquals(directCount, store.applyBloomPrune(0, hash, viaStore, concurrent),
            "a single literal through the store drops what the evidence's own walk drops");
        assertArrayEquals(direct, viaStore);
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
        final long[] keep =
            prune(logical[0], 3, hash, ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));

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

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void maintenanceRewritesOnlyTheTouchedBloomChunk(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
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
        assertNotEquals(changedBefore, changedAfter);
        final byte[] untouchedBefore = ProjectionIndexHOTStorage.readBlob(revisionOne.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.tailSlotKey(0, rowGroupCount));
        final byte[] untouchedAfter = ProjectionIndexHOTStorage.readBlob(revisionTwo.getStorageEngineReader(),
            INDEX_NUMBER, ProjectionBloomChunks.tailSlotKey(0, rowGroupCount));
        assertNotNull(untouchedBefore, "the open row group is a tail blob");
        assertArrayEquals(untouchedBefore, untouchedAfter, "the untouched open row group's tail is unchanged");
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

      assertEquals(new ProjectionBloomChunks.RewriteStats(0, 0, 0L, 0L, 0), stats);

      changed.add(2L);
      assertThrows(IllegalArgumentException.class, () -> ProjectionBloomChunks.rewriteTouchedChunks(storage,
          new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 1, changed));
    }
  }

  /**
   * Open-tail format across every versioning type: a partial chunk is one tail blob per row group, an
   * open-chunk edit writes only its own tail, the chunk folds into a block once when it completes
   * (its tails tombstoned in the same commit), a rollback around the fold leaves the open shape in
   * place, and every revision prunes identically whether its chunk is open or folded.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void openChunkStaysPerRowGroupUntilTheFold(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup before = encodedRowGroup("before");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup after = encodedRowGroup("after");
    final long presentBefore = ProjectionIndexColumnSegmentCodec.bloomHash("before".getBytes(StandardCharsets.UTF_8));
    final long presentAfter = ProjectionIndexColumnSegmentCodec.bloomHash("after".getBytes(StandardCharsets.UTF_8));
    final long rejected = hashRejectedBy(bloomSegment(before));
    final ProjectionBloomChunks.Writer bloomWriter = new ProjectionBloomChunks.Writer();
    final int[] rowGroupsAtRevision = new int[7];
    final int changedRowGroup = leaves + 42;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      // Revision 1: a virgin build of one sealed chunk and 40 open row groups.
      final int built = leaves + 40;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          bloomWriter.append(before, rowGroupId, storage);
        }
        bloomWriter.finishChunks(storage, built, COLUMN_KINDS);
        bloomWriter.publishManifests(storage, built);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)), "the full chunk is a block");
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)), "the open chunk has no block");
        for (int rowGroupId = leaves + 1; rowGroupId <= built; rowGroupId++) {
          assertNotNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "open row group " + rowGroupId + " is a tail blob");
        }
        wtx.commit();
      }
      rowGroupsAtRevision[1] = built;
      // Revision 2: four appended row groups write four tail blobs and nothing else.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = built + 1; rowGroupId <= built + 4; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          changed.add(rowGroupId);
        }
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, built + 4, changed);
        assertEquals(4, stats.rowGroupsRead());
        assertEquals(4, stats.chunksWritten(), "one tail blob per appended row group");
        assertEquals(4L * bloomSegment(before).length,
            stats.bytesWritten() - storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0)).length,
            "the appended fingerprints and the manifest are the only bytes written");
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)));
        wtx.commit();
      }
      rowGroupsAtRevision[2] = built + 4;
      // Revision 3: one open row group changes value: exactly its own tail blob is rewritten.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(changedRowGroup, after);
        final Long2ObjectOpenHashMap<long[]> changedColumns = new Long2ObjectOpenHashMap<>();
        changedColumns.put(changedRowGroup, new long[] {1L});
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, built + 4, changedColumns, false);
        assertEquals(1, stats.rowGroupsRead());
        assertEquals(1, stats.chunksWritten(), "only the changed row group's tail blob is written");
        assertEquals(bloomSegment(after).length, stats.bytesWritten());
        assertArrayEquals(bloomSegment(after), storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, changedRowGroup)));
        assertArrayEquals(bloomSegment(before),
            storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, changedRowGroup + 1)));
        wtx.commit();
      }
      rowGroupsAtRevision[3] = built + 4;
      // Revision 4: the open chunk completes: one block, its tails gone, the manifest at 2 sealed chunks.
      final int folded = 2 * leaves;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = built + 5; rowGroupId <= folded; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          changed.add(rowGroupId);
        }
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, folded, changed);
        assertEquals(folded - built - 4, stats.rowGroupsRead());
        assertEquals(1, stats.chunksWritten(),
            "the completed chunk is one block write: no tail is written only to be tombstoned");
        final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1));
        assertNotNull(block, "the completed chunk folds into a block");
        assertEquals(leaves, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        assertTrue(ProjectionIndexColumnSegmentCodec.bloomBlockIsWellFormed(block, leaves));
        for (int rowGroupId = leaves + 1; rowGroupId <= folded; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "folded row group " + rowGroupId + " keeps no tail blob");
        }
        wtx.commit();
      }
      rowGroupsAtRevision[4] = folded;
      // Revision 5: the next row group opens a new chunk as a tail again; the folded block is untouched.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(folded + 1, before);
        final LongOpenHashSet changed = new LongOpenHashSet();
        changed.add(folded + 1);
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, folded + 1, changed);
        assertEquals(1, stats.chunksWritten());
        assertNotNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, folded + 1)));
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 2)));
        wtx.commit();
      }
      rowGroupsAtRevision[5] = folded + 1;
      // A rolled-back fold leaves the open shape in place.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = folded + 2; rowGroupId <= 3 * leaves; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, 3 * leaves, changed);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 2)),
            "the fold happened in the transaction");
        wtx.rollback();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEquals(5, rtx.getRevisionNumber());
        assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER,
            ProjectionBloomChunks.chunkSlotKey(0, 2)), "a rolled-back fold publishes no block");
        assertNotNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER,
            ProjectionBloomChunks.tailSlotKey(0, folded + 1)), "and the open tail is still there");
      }
      // The same fold can be retried and committed after rollback.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = folded + 2; rowGroupId <= 3 * leaves; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, before);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, 3 * leaves, changed);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 2)));
        assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, folded + 1)));
        wtx.commit();
      }
      rowGroupsAtRevision[6] = 3 * leaves;
    } finally {
      bloomWriter.release();
    }
    // A cold reopen reads every revision's own shape, including both sides of the retried fold.
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      long[][] keepsBefore = null;
      for (int revision = 1; revision <= 6; revision++) {
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          final int rowGroups = rowGroupsAtRevision[revision];
          final ProjectionBloomChunks.ColumnEvidence[] evidence =
              ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroups);
          assertNotNull(evidence, "revision " + revision + " reads its evidence");
          final int sealed = rowGroups / leaves;
          assertEquals(sealed + (rowGroups % leaves == 0
              ? 0
              : 1), evidence[0].chunkCount(), "chunks at revision " + revision);
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
          final long[] keepPresent = prune(evidence[0], rowGroups, presentBefore, fetcher);
          final long[] keepAfter = prune(evidence[0], rowGroups, presentAfter, fetcher);
          final long[] keepRejected = prune(evidence[0], rowGroups, rejected, fetcher);
          assertKept(keepPresent, 0, "a present value is never pruned (revision " + revision + ")");
          assertKept(keepPresent, rowGroups - 1, "a present value is never pruned in the open tail");
          assertDropped(keepRejected, 0, "a rejected hash prunes the sealed block (revision " + revision + ")");
          assertDropped(keepRejected, rowGroups - 1,
              "a rejected hash prunes the open tail (revision " + revision + ")");
          if (revision >= 3) {
            assertKept(keepAfter, changedRowGroup - 1,
                "the changed row group's new value is kept (revision " + revision + ")");
          }
          for (int leaf = 0; leaf < rowGroups; leaf++) {
            final byte[] segment = bloomSegment(revision >= 3 && leaf == changedRowGroup - 1
                ? after
                : before);
            final long bit = 1L << (leaf & 63);
            assertEquals(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(segment, presentBefore),
                (keepPresent[leaf >>> 6] & bit) != 0, "before mask at revision " + revision + ", leaf " + leaf);
            assertEquals(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(segment, presentAfter),
                (keepAfter[leaf >>> 6] & bit) != 0, "after mask at revision " + revision + ", leaf " + leaf);
            assertEquals(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(segment, rejected),
                (keepRejected[leaf >>> 6] & bit) != 0, "rejected mask at revision " + revision + ", leaf " + leaf);
          }
          if (revision == 3) {
            keepsBefore = new long[][] {keepPresent.clone(), keepAfter.clone(), keepRejected.clone()};
          }
          if (revision == 4) {
            final int words = (rowGroupsAtRevision[3] + 63) >>> 6;
            for (int word = 0; word < words; word++) {
              final int validBits = Math.min(Long.SIZE, rowGroupsAtRevision[3] - word * Long.SIZE);
              final long mask = -1L >>> (Long.SIZE - validBits);
              assertEquals(keepsBefore[0][word] & mask, keepPresent[word] & mask,
                  "present mask word " + word + " equal across the fold");
              assertEquals(keepsBefore[1][word] & mask, keepAfter[word] & mask,
                  "after mask word " + word + " equal across the fold");
              assertEquals(keepsBefore[2][word] & mask, keepRejected[word] & mask,
                  "rejected mask word " + word + " equal across the fold");
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void dropColumnRemovesTheOpenChunkTailBlobsToo(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    final int rowGroupCount = ProjectionBloomChunks.CHUNK_LEAVES + 3;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
        writer.append(encoded, rowGroupId, storage);
      }
      writer.finishChunks(storage, rowGroupCount, COLUMN_KINDS);
      writer.publishManifests(storage, rowGroupCount);
      assertEquals(1 + 1 + 3, ProjectionBloomChunks.dropColumn(storage, 0, rowGroupCount),
          "manifest, the sealed block and three tail blobs");
      assertNull(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0)));
      assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)));
      for (int rowGroupId = ProjectionBloomChunks.CHUNK_LEAVES + 1; rowGroupId <= rowGroupCount; rowGroupId++) {
        assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)));
      }
      wtx.commit();
    } finally {
      writer.release();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void recedingHighWaterMarkReopensASealedChunkAndCanFoldItAgain(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int initialCount = 2 * leaves + 3;
    final int reducedCount = leaves + 7;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= initialCount; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, initialCount, COLUMN_KINDS);
        writer.publishManifests(storage, initialCount);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet touched = new LongOpenHashSet();
        touched.add(1L);
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, reducedCount, touched);
        assertTrue(ProjectionBloomChunks.isManifest(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0)),
            reducedCount));
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)));
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)), "the reopened chunk has only tails");
        for (int rowGroupId = leaves + 1; rowGroupId <= reducedCount; rowGroupId++) {
          assertArrayEquals(bloomSegment(encoded), storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)));
        }
        for (int rowGroupId = 2 * leaves + 1; rowGroupId <= initialCount; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)), "removed tails are dropped");
        }
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = reducedCount + 1; rowGroupId <= 2 * leaves; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, 2 * leaves, changed);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)));
        assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, reducedCount)));
        wtx.commit();
      }
    } finally {
      writer.release();
    }
    Databases.clearGlobalCaches();
    final int[] counts = {initialCount, reducedCount, 2 * leaves};
    final long present = ProjectionIndexColumnSegmentCodec.bloomHash("present".getBytes(StandardCharsets.UTF_8));
    final long absent = hashRejectedBy(bloomSegment(encoded));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      for (int revision = 1; revision <= counts.length; revision++) {
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          final int count = counts[revision - 1];
          final ProjectionBloomChunks.ColumnEvidence[] evidence =
              ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, count);
          assertNotNull(evidence);
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
          final long[] keepPresent = prune(evidence[0], count, present, fetcher);
          final long[] keepAbsent = prune(evidence[0], count, absent, fetcher);
          for (int leaf = 0; leaf < count; leaf++) {
            assertKept(keepPresent, leaf);
            assertDropped(keepAbsent, leaf, "absent hash prunes leaf " + leaf);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0", "2", "127", "255"})
  void unsupportedManifestVersionDisablesPruning(final int version) {
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writer.append(encodedRowGroup("present"), 1, storage);
        writer.finishChunks(storage, 1, COLUMN_KINDS);
        writer.publishManifests(storage, 1);
        final byte[] manifest = storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
        assertEquals(1, manifest[Integer.BYTES], "version 1 is the only emitted format");
        manifest[Integer.BYTES] = (byte) version;
        assertFalse(ProjectionBloomChunks.isManifest(manifest, 1));
        storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0), manifest);
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 1));
      }
    } finally {
      writer.release();
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void referencedTailsVerifyLengthAndHashAndReleaseFetchScratch(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone());
    for (int row = 0; row < 512; row++) {
      page.appendRow(row + 1L, new long[] {0L}, new boolean[] {false}, new String[] {"value-" + row},
          new boolean[] {true}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
    }
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    assertTrue(bloomSegment(encoded).length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES);
    final int count = 2 * ProjectionBloomChunks.FETCH_WINDOW_CHUNKS + 1;
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= count; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, count, COLUMN_KINDS);
        writer.publishManifests(storage, count);
        wtx.commit();
      }
    } finally {
      writer.release();
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final ProjectionBloomChunks.ColumnEvidence[] evidence =
          ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, count);
      assertNotNull(evidence);
      final ProjectionColumnStore.ColumnSegmentFetcher delegate =
          ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
      final long absent = hashRejectedBy(bloomSegment(encoded));
      final int[] fetches = new int[1];
      final ProjectionColumnStore.ColumnSegmentFetcher tracking = offsets -> {
        fetches[0]++;
        assertEquals(count, offsets.length, "the whole open chunk must be asked for in one window");
        return delegate.fetchAll(offsets);
      };
      final long[] keep = prune(evidence[0], count, absent, tracking);
      // One fetch is one read transaction. These 33 tails cost 3 under the block window
      // (ceil(33 / FETCH_WINDOW_CHUNKS)) and would cost 33 at a window of 1; the open chunk is one
      // window of its own, so the count is 1 whatever the block window is set to.
      assertEquals(1, fetches[0], "the open chunk's tails must arrive in ONE ranged fetch");
      for (int leaf = 0; leaf < count; leaf++) {
        assertDropped(keep, leaf, "valid referenced tail prunes leaf " + leaf);
      }
      final ProjectionColumnStore.ColumnSegmentFetcher corrupting = offsets -> {
        final byte[][] payloads = delegate.fetchAll(offsets);
        for (int i = 0; i < payloads.length; i++) {
          if (payloads[i] != null) {
            payloads[i] = payloads[i].clone();
            payloads[i][payloads[i].length - 1] ^= 1;
          }
        }
        return payloads;
      };
      final ProjectionColumnStore.ColumnSegmentFetcher truncating = offsets -> {
        final byte[][] payloads = delegate.fetchAll(offsets);
        for (int i = 0; i < payloads.length; i++) {
          if (payloads[i] != null) {
            payloads[i] = Arrays.copyOf(payloads[i], payloads[i].length - 1);
          }
        }
        return payloads;
      };
      final ProjectionColumnStore.ColumnSegmentFetcher failing = offsets -> {
        throw new IllegalStateException("unreadable tail window");
      };
      for (final ProjectionColumnStore.ColumnSegmentFetcher fetcher : List.of(corrupting, truncating, failing)) {
        final long[] corruptKeep = prune(evidence[0], count, absent, fetcher);
        for (int leaf = 0; leaf < count; leaf++) {
          assertKept(corruptKeep, leaf);
        }
        assertTrue(ProjectionBloomChunks.fetchScratchIsClearForTesting());
      }
      final long present = ProjectionIndexColumnSegmentCodec.bloomHash("value-0".getBytes(StandardCharsets.UTF_8));
      final long[] keepPresent = prune(evidence[0], count, present, delegate);
      for (int leaf = 0; leaf < count; leaf++) {
        assertKept(keepPresent, leaf);
      }
    }
  }

  /**
   * A commit that crosses a chunk boundary writes the completed chunk as ONE block and never writes a
   * tail for any of its row groups: the sealed-rewrite path rebuilds the chunk from the tails earlier
   * revisions left plus this commit's own leaves, and tombstones those tails in the same transaction.
   *
   * <p>
   * The changed map here is exactly the one maintenance builds: every slot it allocated, stamped for
   * every column ({@code ProjectionIndexChangeListener.writeRowGroup} records each allocated slot
   * with {@code allColumnWords}). That is what makes the narrow {@code openFirst} safe — the last
   * leaf of every completed chunk lies above the prior physical count, so it is always one of those
   * slots. Crossing a boundary through the listener itself would need {@code CHUNK_LEAVES * MAX_ROWS}
   * = 262 144 projected records, which is far outside fixture scale, so the boundary is crossed here
   * by handing {@code rewriteTouchedChunks} that same input directly.
   *
   * <p>
   * Routing the completed chunk through the tail path instead costs one {@code putBlob} per new row
   * group plus {@link ProjectionBloomChunks#CHUNK_LEAVES} tombstones for the same final bytes, which
   * is what the {@code chunksWritten} bound below refuses.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aBoundaryCrossingCommitWritesOneBlockAndNoTailForTheCompletedChunk(final VersioningType versioning)
      throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int built = leaves - 6;
    final int grown = leaves + 4;
    final int openRowGroups = grown - leaves;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final long present = ProjectionIndexColumnSegmentCodec.bloomHash("present".getBytes(StandardCharsets.UTF_8));
    final long rejected = hashRejectedBy(bloomSegment(encoded));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, built, COLUMN_KINDS);
        writer.publishManifests(storage, built);
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)), "no chunk is sealed yet");
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = built + 1; rowGroupId <= grown; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          changed.add(rowGroupId);
        }
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, grown, changed);

        assertEquals(1 + openRowGroups, stats.chunksWritten(),
            "one block for the completed chunk plus one tail per open row group, and nothing else");
        assertEquals(grown - built, stats.rowGroupsRead());
        assertEquals(leaves + openRowGroups, stats.tailSlotReads(),
            "the completed chunk is recovered from its tails once; each open row group checks its own");
        final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0));
        assertNotNull(block, "the chunk this commit completed is a block");
        assertEquals(leaves, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        for (int rowGroupId = 1; rowGroupId <= leaves; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "completed row group " + rowGroupId + " must never have been written as a tail");
        }
        for (int rowGroupId = leaves + 1; rowGroupId <= grown; rowGroupId++) {
          assertNotNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "open row group " + rowGroupId + " stays a tail blob");
        }
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, grown);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        final long[] keepPresent = prune(evidence[0], grown, present, fetcher);
        final long[] keepRejected = prune(evidence[0], grown, rejected, fetcher);
        for (int leaf = 0; leaf < grown; leaf++) {
          assertKept(keepPresent, leaf);
          assertDropped(keepRejected, leaf,
              "leaf " + leaf + " prunes identically whether it came from a tail or the new block");
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * A string column with no published mark leaves the new count as the only classification, so a
   * chunk the previous revision held as TAILS goes through the sealed-rewrite path. The rewrite must
   * recover those tails: leaf 1's fingerprint exists only as a revision-1 tail blob, and dropping the
   * recovery would put a block over it that carries nothing for that leaf, so an absent hash would
   * stop pruning it.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aSealedChunkWithoutAPublishedMarkRecoversItsTailsIntoTheBlock(final VersioningType versioning)
      throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int built = leaves - 6;
    final int grown = leaves + 4;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final long present = ProjectionIndexColumnSegmentCodec.bloomHash("present".getBytes(StandardCharsets.UTF_8));
    final long rejected = hashRejectedBy(bloomSegment(encoded));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, built, COLUMN_KINDS);
        writer.publishManifests(storage, built);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = built + 1; rowGroupId <= grown; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, grown, changed);
        final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0));
        assertNotNull(block, "the chunk the new count classifies as sealed becomes a block");
        assertEquals(leaves, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        for (int rowGroupId = 1; rowGroupId <= leaves; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "row group " + rowGroupId + " now lives in the block, not in a tail blob");
        }
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, grown);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        final long[] keepPresent = prune(evidence[0], grown, present, fetcher);
        final long[] keepRejected = prune(evidence[0], grown, rejected, fetcher);
        for (int leaf = 0; leaf < grown; leaf++) {
          assertKept(keepPresent, leaf);
          assertDropped(keepRejected, leaf,
              "leaf " + leaf + " kept its fingerprint across the rewrite that reopened its chunk");
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * A sealed chunk whose 256 leaves all carried nothing has no block at all, and nothing to recover
   * either: it was folded, so its tail range is empty. A later commit that gives ONE of its leaves a
   * fingerprint must build the block from that single leaf and prune by it.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void anAllEmptySealedChunkGainsABlockFromOneLateFingerprint(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final int built = ProjectionBloomChunks.CHUNK_LEAVES + 1;
    final int lateRowGroup = 5;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup empty = emptyEncodedRowGroup();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup late = encodedRowGroup("late");
    final long present = ProjectionIndexColumnSegmentCodec.bloomHash("late".getBytes(StandardCharsets.UTF_8));
    final long rejected = hashRejectedBy(bloomSegment(late));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          writer.append(empty, rowGroupId, storage);
        }
        writer.finishChunks(storage, built, COLUMN_KINDS);
        writer.publishManifests(storage, built);
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)),
            "a sealed chunk in which no leaf carries a fingerprint needs no block");
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(lateRowGroup, late);
        final Long2ObjectOpenHashMap<long[]> changedColumns = new Long2ObjectOpenHashMap<>();
        changedColumns.put(lateRowGroup, new long[] {1L});
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, built, changedColumns, false);
        assertEquals(1, stats.rowGroupsRead(), "only the one late row group is read");
        assertEquals(0, stats.tailSlotReads(),
            "a chunk strictly below this column's published mark owns no tail, so none may be probed");
        final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0));
        assertNotNull(block, "the one late fingerprint gives the blockless sealed chunk a block");
        assertEquals(ProjectionBloomChunks.CHUNK_LEAVES, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, built);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        final long[] keepPresent = prune(evidence[0], built, present, fetcher);
        final long[] keepRejected = prune(evidence[0], built, rejected, fetcher);
        assertKept(keepPresent, lateRowGroup - 1);
        assertDropped(keepRejected, lateRowGroup - 1, "the late leaf now carries negative evidence");
        assertKept(keepRejected, 0, "a rowless leaf of the same block is no evidence");
        assertKept(keepRejected, built - 1, "the open row group still carries no fingerprint");
      }
    } finally {
      writer.release();
    }
  }

  /**
   * The block-to-tails conversion a receding mark needs is decided per column, against that column's
   * OWN published mark. One string column whose manifest has gone missing — a state this class's
   * contract models, since a missing manifest only disables pruning for THAT column — must not stop
   * every other column from reopening its block: column 0's seven reopened leaves would otherwise
   * lose their fingerprints to a cleanup that still tombstones the block holding them.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aRecedingMarkReopensEachColumnAgainstItsOwnMark(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int initialCount = 2 * leaves + 3;
    final int reducedCount = leaves + 7;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = twoColumnRowGroup(kinds, "left", "right");
    final byte[] leftFingerprint = bloomSegment(encoded, 0);
    final long present = ProjectionIndexColumnSegmentCodec.bloomHash("left".getBytes(StandardCharsets.UTF_8));
    final long rejected = hashRejectedBy(leftFingerprint);
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= initialCount; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, initialCount, kinds);
        writer.publishManifests(storage, initialCount);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)));
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(1, 1)));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1));
        final LongOpenHashSet touched = new LongOpenHashSet();
        touched.add(1L);
        ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, reducedCount, touched);

        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)),
            "column 0's reopened chunk gives up its block");
        for (int rowGroupId = leaves + 1; rowGroupId <= reducedCount; rowGroupId++) {
          assertArrayEquals(leftFingerprint, storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "column 0 keeps row group " + rowGroupId + "'s fingerprint as a tail");
        }
        assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(1, leaves + 1)),
            "the column with no published mark is not maintained, so it grows no tails");
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, kinds, reducedCount);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        final long[] keepPresent = prune(evidence[0], reducedCount, present, fetcher);
        final long[] keepRejected = prune(evidence[0], reducedCount, rejected, fetcher);
        for (int leaf = 0; leaf < reducedCount; leaf++) {
          assertKept(keepPresent, leaf);
          assertDropped(keepRejected, leaf, "column 0 still prunes reopened leaf " + leaf);
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * The parallel chunk-range split must stay a partition whatever it weighs: every chunk falls in
   * exactly one range, so pruning range by range clears exactly the bits one whole-range walk clears.
   * With a fat open chunk the split must also stop giving that chunk's range a full share of blocks
   * on top of its own page-per-tail cost.
   */
  @Test
  void theWeightedChunkRangeSplitIsAPartitionAndKeepsTheOpenChunkOutOfAFullBlockRange() {
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int sealed = 31;
    final int openTails = 200;
    final int rowGroupCount = sealed * leaves + openTails;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final long absent = hashRejectedBy(bloomSegment(encoded));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
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
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, rowGroupCount);
        assertNotNull(evidence);
        final ProjectionBloomChunks.ColumnEvidence column = evidence[0];
        assertEquals(sealed + 1, column.chunkCount());
        assertTrue(column.parallelPruningIsSafe());

        for (final int ranges : new int[] {1, 2, 3, 4, 8, sealed + 1, sealed + 5}) {
          final int[] bounds = column.weightedRangeBounds(ranges);
          assertEquals(ranges + 1, bounds.length, "one bound per range plus the end");
          assertEquals(0, bounds[0]);
          assertEquals(column.chunkCount(), bounds[ranges], "the ranges must end at the last chunk");
          for (int r = 0; r < ranges; r++) {
            assertTrue(bounds[r] <= bounds[r + 1],
                "bounds must not go backwards at " + r + " for " + ranges + " ranges");
          }
        }
        // Two ranges over 31 blocks and a 200-tail open chunk: sharing hands the open chunk's range
        // 15 blocks on top of its 200 page reads (peak 215), isolating it peaks at 200.
        final int[] two = column.weightedRangeBounds(2);
        assertArrayEquals(new int[] {0, sealed, sealed + 1}, two,
            "the fat open chunk must get a range of its own instead of a full share of blocks");

        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final int words = (rowGroupCount + 63) >>> 6;
        final long[] whole = new long[words];
        Arrays.fill(whole, -1L);
        final int droppedWhole =
            column.pruneMany(new long[] {absent}, new long[][] {whole}, rowGroupCount, fetcher, 0, column.chunkCount());
        assertEquals(rowGroupCount, droppedWhole, "every leaf rejects the absent hash");

        for (final int ranges : new int[] {2, 4}) {
          final int[] bounds = column.weightedRangeBounds(ranges);
          final long[] split = new long[words];
          Arrays.fill(split, -1L);
          int droppedSplit = 0;
          for (int r = 0; r < ranges; r++) {
            if (bounds[r] < bounds[r + 1]) {
              droppedSplit += column.pruneMany(new long[] {absent}, new long[][] {split}, rowGroupCount, fetcher,
                  bounds[r], bounds[r + 1]);
            }
          }
          assertEquals(droppedWhole, droppedSplit, "a " + ranges + "-way split clears the same bits");
          assertArrayEquals(whole, split, "a " + ranges + "-way split is exact");
        }
      }
    } finally {
      writer.release();
    }
  }

  private static void createVersionedResource(final VersioningType versioning) throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                             .versioningApproach(versioning)
                                             .maxNumberOfRevisionsToRestore(3)
                                             .build());
    }
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup encodedRowGroup(final String value) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone());
    page.appendRow(1L, new long[] {0L}, new boolean[] {false}, new String[] {value}, new boolean[] {true},
        new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
    return ProjectionIndexColumnSegmentCodec.encode(page.serialize());
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup emptyEncodedRowGroup() {
    return ProjectionIndexColumnSegmentCodec.encode(new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone()).serialize());
  }

  private static void appendChunk(final ProjectionBloomChunks.Writer writer,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded, final ProjectionIndexHOTStorage storage) {
    for (int rowGroupId = 1; rowGroupId <= ProjectionBloomChunks.CHUNK_LEAVES; rowGroupId++) {
      writer.append(encoded, rowGroupId, storage);
    }
  }

  private static byte[] bloomSegment(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded) {
    return bloomSegment(encoded, 0);
  }

  private static byte[] bloomSegment(final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded,
      final int column) {
    final int bloomId = ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(column);
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == bloomId) {
        return encoded.segments()[i];
      }
    }
    throw new AssertionError("encoded string row group carries no Bloom segment for column " + column);
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup twoColumnRowGroup(final byte[] kinds,
      final String left, final String right) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
    page.appendRow(1L, new long[] {0L, 0L}, new boolean[] {false, false}, new String[] {left, right},
        new boolean[] {true, true}, new boolean[] {false, false}, new boolean[] {false, false},
        new boolean[] {false, false});
    return ProjectionIndexColumnSegmentCodec.encode(page.serialize());
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
