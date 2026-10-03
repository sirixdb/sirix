/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.stubbing.Answer;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.withSettings;

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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
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
          final byte[][] pages = Objects.requireNonNull(delegate.fetchAll(offsets));
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
        final long splitDropped = (long) evidence[0].pruneMany(hashes, split, rowGroupCount, delegate, 0, 2)
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
        final int sealedFetches =
            (ProjectionBloomChunks.sealedChunkCount(rowGroupCount) + ProjectionBloomChunks.FETCH_WINDOW_CHUNKS - 1)
                / ProjectionBloomChunks.FETCH_WINDOW_CHUNKS;
        final int openTailFetches = ProjectionBloomChunks.openLeafCount(rowGroupCount) > 0
            && (bloomSegment(present).length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES
                || bloomSegment(absent).length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES)
                    ? 1
                    : 0;
        assertEquals(sealedFetches + openTailFetches, fetches[0]);
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
          public byte @Nullable [] @Nullable [] fetchAll(final long[] offsets) {
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
        assertNotNull(logical);
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

  @ParameterizedTest
  @CsvSource({"false", "true"})
  void columnSelectionBoundsSameOpenSpanManifestReads(final boolean shapeChanged) {
    final int columns = 64;
    final byte[] kinds = new byte[columns];
    Arrays.fill(kinds, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT);
    final int built = 17;
    final int count = shapeChanged
        ? built + 1
        : built;
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = wideRowGroup(kinds, "before");
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          writer.append(encoded, rowGroupId, storage);
        }
        writer.finishChunks(storage, built, kinds);
        writer.publishManifests(storage, built);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(count, wideRowGroup(kinds, "after"));
        final Long2ObjectOpenHashMap<long[]> changed = new Long2ObjectOpenHashMap<>();
        changed.put((long) count, new long[] {1L});
        final int[] manifestReads = new int[columns];
        final int[] blobReads = new int[1];
        final long[] bytesRead = new long[1];
        final Answer<Object> delegate = delegatesTo(storage);
        final ProjectionIndexHOTStorage observed =
            mock(ProjectionIndexHOTStorage.class, withSettings().defaultAnswer(invocation -> {
              final Object result = delegate.answer(invocation);
              final String method = invocation.getMethod().getName();
              if (method.equals("getBlob")) {
                blobReads[0]++;
                final long slot = invocation.getArgument(0);
                for (int column = 0; column < columns; column++) {
                  if (slot == ProjectionIndexHOTStorage.bloomBlockSlotKey(column)) {
                    manifestReads[column]++;
                  }
                }
              }
              if ((method.equals("getBlob") || method.equals("getVerifiedColumnSegment")) && result != null) {
                bytesRead[0] += ((byte[]) result).length;
              }
              return result;
            }));
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(observed, kinds, count, changed, shapeChanged);
        for (int column = 0; column < columns; column++) {
          assertEquals(shapeChanged || column == 0
              ? 1
              : 0, manifestReads[column], "column " + column);
        }
        final int maintainedColumns = shapeChanged
            ? columns
            : 1;
        assertEquals(2 * maintainedColumns, blobReads[0], "one manifest and one changed tail per maintained column");
        assertEquals(bytesRead[0], stats.bytesRead(), "account for every fetched payload");
        assertEquals(maintainedColumns, stats.rowGroupsRead());
        assertEquals(maintainedColumns, stats.tailSlotReads());
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, kinds, count);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        assertNotNull(evidence);
        for (int column = 0; column < columns; column++) {
          final long hash = ProjectionIndexColumnSegmentCodec.bloomHash((column == 0
              ? "after"
              : "right").getBytes(StandardCharsets.UTF_8));
          final long[] keep = prune(evidence[column], count, hash, fetcher);
          for (int leaf = 0; leaf < count; leaf++) {
            assertEquals(column != 0 || leaf == count - 1, (keep[leaf >>> 6] & (1L << (leaf & 63))) != 0L);
          }
        }
      }
    } finally {
      writer.release();
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
            stats.bytesWritten()
                - Objects.requireNonNull(storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0))).length,
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
            assertNotNull(keepsBefore, "revision 3 captured the masks before folding");
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
        assertNotNull(manifest);
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
        Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
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
      final int[] offsetsSeen = new int[1];
      final ProjectionColumnStore.ColumnSegmentFetcher tracking = offsets -> {
        fetches[0]++;
        offsetsSeen[0] = offsets.length;
        return delegate.fetchAll(offsets);
      };
      final long[] keep = prune(evidence[0], count, absent, tracking);
      // One fetch is one read transaction. These 33 tails cost 3 under the block window
      // (ceil(33 / FETCH_WINDOW_CHUNKS)) and would cost 33 at a window of 1; the open chunk is one
      // window of its own, so the count is 1 whatever the block window is set to.
      assertEquals(1, fetches[0], "the open chunk's tails must arrive in ONE ranged fetch");
      // The request spans the whole fixed scratch, not just the live prefix: a ranged fetcher that
      // gets the full array uses it as-is instead of allocating a copy of the sub-range per prune.
      assertEquals(ProjectionBloomChunks.CHUNK_LEAVES, offsetsSeen[0],
          "the fetch must be asked for over the full tail scratch so no offset copy is allocated");
      for (int leaf = 0; leaf < count; leaf++) {
        assertDropped(keep, leaf, "valid referenced tail prunes leaf " + leaf);
      }
      final ProjectionColumnStore.ColumnSegmentFetcher corrupting = offsets -> {
        final byte[][] payloads = Objects.requireNonNull(delegate.fetchAll(offsets));
        for (int i = 0; i < payloads.length; i++) {
          if (payloads[i] != null) {
            payloads[i] = payloads[i].clone();
            payloads[i][payloads[i].length - 1] ^= 1;
          }
        }
        return payloads;
      };
      final ProjectionColumnStore.ColumnSegmentFetcher truncating = offsets -> {
        final byte[][] payloads = Objects.requireNonNull(delegate.fetchAll(offsets));
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
        assertEquals(built + openRowGroups, stats.tailSlotReads(),
            "the completed chunk is recovered across the published open span only - " + built + " tails, not all "
                + leaves + " slots - and each open row group checks its own");
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

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void listenerBoundaryCrossingWritesOneBlockPerColumnAndNoCompletedTail(final VersioningType versioning)
      throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/left", PathParser.Type.JSON), parse("/[]/right", PathParser.Type.JSON)),
        List.of(Type.STR, Type.STR), INDEX_NUMBER, IndexDef.DbType.JSON);
    final int built = ProjectionBloomChunks.CHUNK_LEAVES - 1;
    final int appended = 2 * ProjectionIndexRowGroupPage.MAX_ROWS;
    final int grown = built + 2;
    final long arrayKey;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                             .versioningApproach(versioning)
                                             .maxNumberOfRevisionsToRestore(3)
                                             .useDeweyIDs(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertArrayAsFirstChild();
          arrayKey = wtx.getNodeKey();
          final long[] keys = new long[built];
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
          final ProjectionFlagSummaryChunks.BuildWriter flags = new ProjectionFlagSummaryChunks.BuildWriter();
          try {
            for (int row = 0; row < built; row++) {
              assertTrue(wtx.moveTo(arrayKey));
              wtx.insertObjectAsLastChild();
              keys[row] = wtx.getNodeKey();
              wtx.insertObjectRecordAsFirstChild("left", new StringValue("before-left"));
              assertTrue(wtx.moveTo(keys[row]));
              wtx.insertObjectRecordAsLastChild("right", new StringValue("before-right"));
              final ProjectionIndexRowExtractor extractor =
                  new ProjectionIndexRowExtractor(definition, wtx.getPathSummary());
              assertTrue(extractor.extractInto(wtx, keys[row]));
              assertTrue(wtx.moveTo(keys[row]));
              final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
              assertTrue(extractor.appendTo(page, keys[row], false, wtx.getDeweyID().toBytes()));
              final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
                  Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
              storage.putRowGroupAsColumnSegmentSlots(row + 1, encoded);
              writer.append(encoded, row + 1, storage);
              flags.append(storage, encoded.descriptor());
            }
            ProjectionIndexFences.write(storage, built, keys, keys);
            writer.finishChunks(storage, built, kinds);
            writer.publishManifests(storage, built);
            flags.finish(storage, built, kinds.length, wtx.getRevisionNumber());
            storage.putBlob(0,
                new ProjectionIndexMetadata(definition.getProjectionRootPath().toString(),
                    definition.getProjectionFields().stream().map(Object::toString).toArray(String[]::new),
                    new String[] {"left", "right"}, kinds, built, wtx.getRevisionNumber()).serialize());
            session.getWtxIndexController(wtx.getRevisionNumber()).createIndexListeners(Set.of(definition), wtx);
            wtx.commit();
          } finally {
            writer.release();
          }
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          for (int row = 0; row < appended; row++) {
            assertTrue(wtx.moveTo(arrayKey));
            wtx.insertObjectAsLastChild();
            final long key = wtx.getNodeKey();
            wtx.insertObjectRecordAsFirstChild("left", new StringValue("after-left"));
            assertTrue(wtx.moveTo(key));
            wtx.insertObjectRecordAsLastChild("right", new StringValue("after-right"));
          }
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          final Answer<Object> delegate = delegatesTo(storage);
          final int[] blockPuts = new int[kinds.length];
          final int[] completedTailPuts = new int[kinds.length];
          final int[] openTailPuts = new int[kinds.length];
          try (MockedConstruction<ProjectionIndexHOTStorage> observed =
              mockConstruction(ProjectionIndexHOTStorage.class, withSettings().defaultAnswer(invocation -> {
                if (invocation.getMethod().getName().equals("putBlob")) {
                  final long slot = invocation.getArgument(0);
                  for (int column = 0; column < kinds.length; column++) {
                    if (slot == ProjectionBloomChunks.chunkSlotKey(column, 0)) {
                      blockPuts[column]++;
                    } else if (slot >= ProjectionBloomChunks.tailSlotKey(column, 1)
                        && slot <= ProjectionBloomChunks.tailSlotKey(column, ProjectionBloomChunks.CHUNK_LEAVES)) {
                      completedTailPuts[column]++;
                    } else if (slot == ProjectionBloomChunks.tailSlotKey(column, grown)) {
                      openTailPuts[column]++;
                    }
                  }
                }
                return delegate.answer(invocation);
              }))) {
            wtx.commit();
            assertFalse(observed.constructed().isEmpty(), "observe the storage used by real listener maintenance");
          }
          assertArrayEquals(new int[] {1, 1}, blockPuts);
          assertArrayEquals(new int[] {0, 0}, completedTailPuts);
          assertArrayEquals(new int[] {1, 1}, openTailPuts);
        }
      }
    }
    for (int revision = 1; revision <= 2; revision++) {
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
          JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final int count = revision == 1
            ? built
            : grown;
        final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
        assertNotNull(handle);
        assertEquals(count, handle.rowGroupCount());
        final List<byte[]> payloads = handle.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, count));
        assertEquals(revision == 1
            ? built
            : built + appended, ProjectionIndexScan.countRows(payloads));
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, kinds, count);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
        for (int column = 0; column < kinds.length; column++) {
          for (final String prefix : List.of("before-", "after-", "absent-")) {
            final byte[] value = (prefix + (column == 0
                ? "left"
                : "right")).getBytes(StandardCharsets.UTF_8);
            final long hash = ProjectionIndexColumnSegmentCodec.bloomHash(value);
            final long[] keep = prune(evidence[column], count, hash, fetcher);
            final List<byte[]> kept = new ArrayList<>();
            for (int leaf = 0; leaf < count; leaf++) {
              if ((keep[leaf >>> 6] & (1L << (leaf & 63))) != 0L) {
                kept.add(payloads.get(leaf));
              }
              if (prefix.equals("after-") && revision == 2 && leaf >= built - 1) {
                assertKept(keep, leaf);
              } else if (prefix.equals("absent-") || (prefix.equals("after-") && leaf < built - 1)) {
                assertDropped(keep, leaf, "unmatched row groups retain negative evidence");
              }
            }
            final long expected = prefix.equals("before-")
                ? built
                : prefix.equals("after-") && revision == 2
                    ? appended
                    : 0;
            final ProjectionIndexScan.ColumnPredicate[] predicates =
                {ProjectionIndexScan.ColumnPredicate.stringEq(column, value)};
            assertEquals(expected, ProjectionIndexScan.conjunctiveCount(payloads, predicates));
            assertEquals(expected, ProjectionIndexScan.conjunctiveCount(kept, predicates));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aSealedChunkWithoutAPublishedMarkDiscardsUnownedTails(final VersioningType versioning) throws IOException {
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
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, grown, changed);
        assertEquals(openRowGroups, stats.tailSlotReads(), "unowned tails cannot supply recovery evidence");
        final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0));
        assertNotNull(block, "the chunk the new count classifies as sealed becomes a block");
        assertEquals(leaves, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        for (int rowGroupId = built + 1; rowGroupId <= leaves; rowGroupId++) {
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
          if (leaf < built) {
            assertKept(keepRejected, leaf, "unowned tails provide no negative evidence");
          } else {
            assertDropped(keepRejected, leaf, "changed rows publish their current fingerprints");
          }
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
  @CsvSource({"FULL,true", "FULL,false", "DIFFERENTIAL,true", "DIFFERENTIAL,false", "INCREMENTAL,true",
      "INCREMENTAL,false", "SLIDING_SNAPSHOT,true", "SLIDING_SNAPSHOT,false"})
  void aRecedingMarkReopensEachColumnAgainstItsOwnMark(final VersioningType versioning, final boolean missingManifest)
      throws IOException {
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
    final String updatedValue = "updated-right";
    final long updatedHash = ProjectionIndexColumnSegmentCodec.bloomHash(updatedValue.getBytes(StandardCharsets.UTF_8));
    assertFalse(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(bloomSegment(encoded, 1), updatedHash));
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
        if (missingManifest) {
          storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1));
        } else {
          storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1), new byte[] {9, 8, 7});
        }
        final LongOpenHashSet touched = new LongOpenHashSet();
        touched.add(1L);
        ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, reducedCount, touched);

        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)),
            "column 0's reopened chunk gives up its block");
        for (int rowGroupId = leaves + 1; rowGroupId <= reducedCount; rowGroupId++) {
          assertArrayEquals(leftFingerprint, storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "column 0 keeps row group " + rowGroupId + "'s fingerprint as a tail");
        }
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
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putRowGroupAsColumnSegmentSlots(leaves + 1, twoColumnRowGroup(kinds, "left", updatedValue));
        final Long2ObjectOpenHashMap<long[]> changed = new Long2ObjectOpenHashMap<>();
        changed.put(leaves + 1L, new long[] {2L});
        ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, reducedCount, changed, false);
        wtx.commit();
      }
      final int refoldedCount = 2 * leaves;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = reducedCount + 1; rowGroupId <= refoldedCount; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, refoldedCount, changed);
        wtx.commit();
      }
    } finally {
      writer.release();
    }
    for (int revision = 4; revision >= 1; revision--) {
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
          JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final int count = revision == 1
            ? initialCount
            : revision == 4
                ? 2 * leaves
                : reducedCount;
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, kinds, count);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
        final long[] keepUpdated = prune(evidence[1], count, updatedHash, fetcher);
        final long[] keepLeft = prune(evidence[0], count, present, fetcher);
        final long[] keepRejected = prune(evidence[0], count, rejected, fetcher);
        final long rightHash = ProjectionIndexColumnSegmentCodec.bloomHash("right".getBytes(StandardCharsets.UTF_8));
        final long[] keepRight = prune(evidence[1], count, rightHash, fetcher);
        for (int leaf = 0; leaf < count; leaf++) {
          assertKept(keepLeft, leaf);
          assertDropped(keepRejected, leaf, "column 0 keeps exact evidence at revision " + revision);
          if (revision >= 3 && leaf == leaves) {
            assertKept(keepUpdated, leaf, "the edited row must survive refolding the reopened chunk");
          } else {
            assertDropped(keepUpdated, leaf, "untouched rows reject the edited value at revision " + revision);
            assertKept(keepRight, leaf);
          }
        }
        if (revision == 2 || revision == 3) {
          for (int column = 0; column < kinds.length; column++) {
            assertNull(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER,
                ProjectionBloomChunks.chunkSlotKey(column, 1)), "an open span cannot retain a sealed block");
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"FULL,true,false", "FULL,false,false", "FULL,true,true", "FULL,false,true", "DIFFERENTIAL,true,false",
      "DIFFERENTIAL,false,false", "DIFFERENTIAL,true,true", "DIFFERENTIAL,false,true", "INCREMENTAL,true,false",
      "INCREMENTAL,false,false", "INCREMENTAL,true,true", "INCREMENTAL,false,true", "SLIDING_SNAPSHOT,true,false",
      "SLIDING_SNAPSHOT,false,false", "SLIDING_SNAPSHOT,true,true", "SLIDING_SNAPSHOT,false,true"})
  void regrownOpenSpansRetireStaleBlocksBeforeRefolding(final VersioningType versioning, final boolean missingManifest,
      final boolean alignedRetreat) throws IOException {
    createVersionedResource(versioning);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int[] counts = alignedRetreat
        ? new int[] {2 * leaves + 3, leaves, leaves + 1, 2 * leaves}
        : new int[] {3 * leaves + 3, leaves + 7, 2 * leaves + 1, 3 * leaves};
    final int editedRowGroup = counts[2];
    final long editedHash =
        ProjectionIndexColumnSegmentCodec.bloomHash("updated-right".getBytes(StandardCharsets.UTF_8));
    assertFalse(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(
        bloomSegment(twoColumnRowGroup(kinds, "left", "right"), 1), editedHash));
    for (int revision = 1; revision <= counts.length; revision++) {
      final int attempts = revision == counts.length
          ? 2
          : 1;
      for (int attempt = 0; attempt < attempts; attempt++) {
        final boolean rollback = attempts == 2 && attempt == 0;
        try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
            JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          final int count = counts[revision - 1];
          if (revision == 1) {
            final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
            try {
              for (int rowGroupId = 1; rowGroupId <= count; rowGroupId++) {
                final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
                    twoColumnRowGroup(kinds, rowGroupId, "left", "right");
                storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
                writer.append(encoded, rowGroupId, storage);
              }
              writer.finishChunks(storage, count, kinds);
              writer.publishManifests(storage, count);
            } finally {
              writer.release();
            }
          } else {
            final LongOpenHashSet changed = new LongOpenHashSet();
            if (revision == 2) {
              if (missingManifest) {
                storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1));
              } else {
                storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1), new byte[] {9, 8, 7});
              }
              changed.add(1L);
            } else {
              for (int rowGroupId = counts[revision - 2] + 1; rowGroupId <= count; rowGroupId++) {
                storage.putRowGroupAsColumnSegmentSlots(rowGroupId,
                    twoColumnRowGroup(kinds, rowGroupId, "left", rowGroupId == editedRowGroup
                        ? "updated-right"
                        : "right"));
                changed.add(rowGroupId);
              }
            }
            ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, count, changed);
          }
          if (rollback) {
            wtx.rollback();
          } else {
            wtx.commit();
          }
          assertEquals(rollback
              ? revision - 1
              : revision, session.getMostRecentRevisionNumber());
        }
        final int visibleRevision = rollback
            ? revision - 1
            : revision;
        assertRegrowthRevision(visibleRevision, counts[visibleRevision - 1], visibleRevision >= 3
            ? editedRowGroup
            : -1, kinds);
      }
    }
    for (int revision = counts.length; revision >= 1; revision--) {
      assertRegrowthRevision(revision, counts[revision - 1], revision >= 3
          ? editedRowGroup
          : -1, kinds);
    }
  }

  private static void assertRegrowthRevision(final int revision, final int count, final int editedRowGroup,
      final byte[] kinds) {
    assertRegrowthRevision(revision, count, editedRowGroup, kinds, -1, -1);
  }

  private static void assertRegrowthRevision(final int revision, final int count, final int editedRowGroup,
      final byte[] kinds, final int uncertainFirst, final int uncertainLast) {
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionBloomChunks.ColumnEvidence[] evidence =
          ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, kinds, count);
      assertNotNull(evidence);
      final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
          ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      final List<byte[]> payloads = new ArrayList<>(count);
      for (int rowGroupId = 1; rowGroupId <= count; rowGroupId++) {
        final byte[] payload = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, rowGroupId);
        assertNotNull(payload);
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
        assertEquals(1, page.getRowCount());
        assertEquals(rowGroupId, page.firstRecordKey());
        payloads.add(payload);
      }
      final String[] values = {"updated-right", "right", "left", "absent"};
      for (int query = 0; query < values.length; query++) {
        final int column = query == 2
            ? 0
            : 1;
        final byte[] value = values[query].getBytes(StandardCharsets.UTF_8);
        final long[] keep = prune(evidence[column], count, ProjectionIndexColumnSegmentCodec.bloomHash(value), fetcher);
        final ProjectionIndexScan.ColumnPredicate[] predicates =
            {ProjectionIndexScan.ColumnPredicate.stringEq(column, value)};
        final LongArrayList expectedKeys = new LongArrayList(count);
        final LongArrayList actualKeys = new LongArrayList(count);
        final LongArrayList prunedKeys = new LongArrayList(count);
        for (int leaf = 0; leaf < count; leaf++) {
          final int rowGroupId = leaf + 1;
          final boolean expectedMatch = switch (query) {
            case 0 -> rowGroupId == editedRowGroup;
            case 1 -> rowGroupId != editedRowGroup;
            case 2 -> true;
            default -> false;
          };
          if (expectedMatch) {
            expectedKeys.add(rowGroupId);
          }
          final boolean actualMatch =
              ProjectionIndexScan.conjunctiveCount(List.of(payloads.get(leaf)), predicates) == 1L;
          if (actualMatch) {
            actualKeys.add(rowGroupId);
          }
          final boolean kept = (keep[leaf >>> 6] & (1L << (leaf & 63))) != 0L;
          if (kept && actualMatch) {
            prunedKeys.add(rowGroupId);
          }
          if (query == 0 && expectedMatch) {
            assertKept(keep, leaf, "the regrown row must survive refolding at revision " + revision);
          } else {
            assertEquals(expectedMatch || (column == 1 && rowGroupId >= uncertainFirst && rowGroupId <= uncertainLast),
                kept, "keep bit for " + values[query] + " at row group " + rowGroupId + " in revision " + revision);
          }
        }
        assertEquals(expectedKeys, actualKeys, "exact stored matching rows in revision " + revision);
        assertEquals(expectedKeys, prunedKeys, "exact matching rows after pruning in revision " + revision);
      }
    }
  }

  enum TailEvidenceLoss {
    MISSING_BLOCK, MALFORMED_BLOCK, EMPTY_SLICE, SEALED_REWRITE, FOLD
  }

  private static Stream<Arguments> tailOwnershipScenarios() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(versioning -> Stream.of(true, false)
                                              .flatMap(missingManifest -> Arrays.stream(TailEvidenceLoss.values())
                                                                                .map(loss -> Arguments.of(versioning,
                                                                                    missingManifest, loss))));
  }

  @ParameterizedTest
  @MethodSource("tailOwnershipScenarios")
  void newlyActivatedAndRecoveredTailsRequireCurrentOwnership(final VersioningType versioning,
      final boolean missingManifest, final TailEvidenceLoss loss) throws IOException {
    createVersionedResource(versioning);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int editedRowGroup = 2 * leaves + 1;
    final boolean recovery = loss == TailEvidenceLoss.SEALED_REWRITE || loss == TailEvidenceLoss.FOLD;
    final int[] counts = recovery
        ? new int[] {2 * leaves + 3, leaves + 7, 3 * leaves, 3 * leaves, editedRowGroup, 3 * leaves}
        : new int[] {2 * leaves + 3, leaves + 7, 3 * leaves, editedRowGroup, 3 * leaves};
    final long editedHash =
        ProjectionIndexColumnSegmentCodec.bloomHash("updated-right".getBytes(StandardCharsets.UTF_8));
    assertFalse(ProjectionIndexColumnSegmentCodec.bloomMayContainHash(
        bloomSegment(twoColumnRowGroup(kinds, "left", "right"), 1), editedHash));
    for (int revision = 1; revision <= counts.length; revision++) {
      final int attempts = revision >= 4
          ? 2
          : 1;
      for (int attempt = 0; attempt < attempts; attempt++) {
        final boolean rollback = attempts == 2 && attempt == 0;
        try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
            JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          final int count = counts[revision - 1];
          if (revision == 1) {
            final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
            try {
              for (int rowGroupId = 1; rowGroupId <= count; rowGroupId++) {
                final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
                    twoColumnRowGroup(kinds, rowGroupId, "left", "right");
                storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
                writer.append(encoded, rowGroupId, storage);
              }
              writer.finishChunks(storage, count, kinds);
              writer.publishManifests(storage, count);
            } finally {
              writer.release();
            }
          } else {
            final LongOpenHashSet changed = new LongOpenHashSet();
            if (revision == 2 || (revision == 4 && recovery)) {
              if (missingManifest) {
                storage.tombstoneBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1));
              } else {
                storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(1), new byte[] {9, 8, 7});
              }
            }
            if (revision == 4) {
              final long blockSlot = ProjectionBloomChunks.chunkSlotKey(1, 2);
              if (loss == TailEvidenceLoss.EMPTY_SLICE) {
                final byte[][] slices =
                    ProjectionIndexColumnSegmentCodec.copyBloomBlockSlices(storage.getBlob(blockSlot), leaves);
                assertNotNull(slices);
                slices[0] = null;
                storage.putBlob(blockSlot,
                    Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encodeBloomBlock(slices, leaves)));
              } else if (loss == TailEvidenceLoss.MALFORMED_BLOCK) {
                storage.putBlob(blockSlot, new byte[] {9, 8, 7});
              } else {
                storage.tombstoneBlob(blockSlot);
              }
            }
            if (revision == 3 || revision == counts.length) {
              for (int rowGroupId = counts[revision - 2] + 1; rowGroupId <= count; rowGroupId++) {
                storage.putRowGroupAsColumnSegmentSlots(rowGroupId,
                    twoColumnRowGroup(kinds, rowGroupId, "left", rowGroupId == editedRowGroup
                        ? "updated-right"
                        : "right"));
                changed.add(rowGroupId);
              }
            } else {
              changed.add(revision == 4 && loss == TailEvidenceLoss.SEALED_REWRITE
                  ? 3L * leaves
                  : 1L);
            }
            ProjectionBloomChunks.rewriteTouchedChunks(storage, kinds, count, changed);
          }
          if (rollback) {
            wtx.rollback();
          } else {
            wtx.commit();
          }
          assertEquals(rollback
              ? revision - 1
              : revision, session.getMostRecentRevisionNumber());
        }
        assertTailOwnershipRevision(rollback
            ? revision - 1
            : revision, counts, editedRowGroup, kinds, loss);
      }
    }
    for (int revision = counts.length; revision >= 1; revision--) {
      assertTailOwnershipRevision(revision, counts, editedRowGroup, kinds, loss);
    }
  }

  private static void assertTailOwnershipRevision(final int revision, final int[] counts, final int editedRowGroup,
      final byte[] kinds, final TailEvidenceLoss loss) {
    final int uncertainFirst = revision >= 4
        ? editedRowGroup
        : -1;
    final int uncertainLast = revision == 4 && loss == TailEvidenceLoss.SEALED_REWRITE
        ? 3 * ProjectionBloomChunks.CHUNK_LEAVES - 1
        : revision == 4 && loss == TailEvidenceLoss.FOLD
            ? 3 * ProjectionBloomChunks.CHUNK_LEAVES
            : uncertainFirst;
    assertRegrowthRevision(revision, counts[revision - 1], revision >= 3
        ? editedRowGroup
        : -1, kinds, uncertainFirst, uncertainLast);
  }

  /**
   * The parallel chunk-range split must stay a partition whatever it weighs: every chunk falls in
   * exactly one range, so pruning range by range clears exactly the bits one whole-range walk clears.
   * An INLINE open chunk must also take the plain even cut: its tails are carried in their locators
   * and fetch nothing, so pricing them as page reads would isolate them into a range that does no I/O
   * at all while a sibling range fetches every block — twice the peak of the even cut, with a worker
   * left idle. Low cardinality is the common shape, so this is the ordinary case, not the exception.
   */
  @Test
  void theWeightedChunkRangeSplitIsAPartitionAndLeavesAnInlineOpenChunkOnTheEvenCut() {
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int sealed = 31;
    final int openTails = 100;
    final int rowGroupCount = sealed * leaves + openTails;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    assertTrue(bloomSegment(encoded).length <= ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "this fixture's point is an all-inline open chunk; got a " + bloomSegment(encoded).length + "-byte tail");
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
        // Two ranges over 31 blocks and 100 INLINE tails: the open chunk fetches nothing, so the even
        // cut peaks at 16 block fetches, while isolating it would peak at all 31.
        final int[] two = column.weightedRangeBounds(2);
        assertArrayEquals(new int[] {0, 16, sealed + 1}, two,
            "an open chunk that fetches nothing must not buy a range of its own");

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

  /**
   * A chunk this commit completes in which every leaf turns out to carry no fingerprint rebuilds to a
   * null block, which equals its absent prior — so the rewrite takes the unchanged-block shortcut.
   * The tails it has already absorbed must still be retired. Leaving them lets the fold downstream
   * rebuild a block out of a fingerprint whose row group is now rowless, and a probe that fingerprint
   * rejects would then prune a leaf on evidence that no longer describes anything.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void aCompletingChunkThatRebuildsToNoBlockStillRetiresTheTailsItAbsorbed(final VersioningType versioning)
      throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int built = leaves + 1;
    final int grown = 2 * leaves;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup valued = encodedRowGroup("present");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup rowless = emptyEncodedRowGroup();
    final long rejected = hashRejectedBy(bloomSegment(valued));
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= built; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, valued);
          writer.append(valued, rowGroupId, storage);
        }
        writer.finishChunks(storage, built, COLUMN_KINDS);
        writer.publishManifests(storage, built);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, leaves + 1)),
            "the open chunk's one row group starts out as a tail blob");
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // Every leaf of the completing chunk becomes rowless, including the one that held the only
        // fingerprint in it, so the rebuilt block is null and matches the absent prior exactly.
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = leaves + 1; rowGroupId <= grown; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, rowless);
          changed.add(rowGroupId);
        }
        ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, grown, changed);

        for (int rowGroupId = leaves + 1; rowGroupId <= grown; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "row group " + rowGroupId + "'s absorbed tail must not survive the rewrite");
        }
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)),
            "a completed chunk in which no leaf carries a fingerprint must not gain a block");
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)),
            "the chunk that was already sealed keeps its block");
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, grown);
        assertNotNull(evidence);
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, 2);
        final long[] keepRejected = prune(evidence[0], grown, rejected, fetcher);
        for (int leaf = 0; leaf < leaves; leaf++) {
          assertDropped(keepRejected, leaf, "the already sealed chunk still prunes leaf " + leaf);
        }
        for (int leaf = leaves; leaf < grown; leaf++) {
          assertKept(keepRejected, leaf,
              "rowless leaf " + leaf + " carries no fingerprint, so no resurrected one may prune it");
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * The mirror of the inline case: an open chunk whose tails are REFERENCED really does cost one page
   * read each, and it cannot be divided without giving up its single ranged fetch, so it must get a
   * range of its own rather than a full share of blocks on top.
   */
  @Test
  void theWeightedChunkRangeSplitIsolatesAnOpenChunkOfReferencedTails() {
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int sealed = 31;
    final int openTails = 100;
    final int rowGroupCount = sealed * leaves + openTails;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup inline = encodedRowGroup("present");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup referenced = fatRowGroup();
    assertTrue(bloomSegment(referenced).length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "the open chunk's tails must exceed the inline threshold or they cost no page read; got "
            + bloomSegment(referenced).length + " bytes");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        // Only the open chunk's tails have to be fat: the split prices the sealed chunks at one page
        // read each whatever they hold, so the blocks stay cheap to build.
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          writer.append(rowGroupId > sealed * leaves
              ? referenced
              : inline, rowGroupId, storage);
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

        // Sharing hands the open chunk's range 15 blocks on top of its 100 page reads (peak 115);
        // isolating it peaks at 100.
        assertArrayEquals(new int[] {0, sealed, sealed + 1}, column.weightedRangeBounds(2),
            "a referenced open chunk must get a range of its own instead of a full share of blocks");
        for (final int ranges : new int[] {1, 2, 3, 4, 8}) {
          final int[] bounds = column.weightedRangeBounds(ranges);
          assertEquals(0, bounds[0]);
          assertEquals(column.chunkCount(), bounds[ranges], "the ranges must end at the last chunk");
          for (int r = 0; r < ranges; r++) {
            assertTrue(bounds[r] <= bounds[r + 1],
                "bounds must not go backwards at " + r + " for " + ranges + " ranges");
          }
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * A commit that completes TWO chunks at once must probe tails only where a tail can be: the chunk
   * at the column's published mark owns the published open span, and the chunk above it has never had
   * a tail slot written in its life. Sweeping all 256 slots of that second chunk costs 256 reads plus
   * 256 tombstone descents per string column and can never find anything.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void completingTwoChunksAtOnceProbesTailsOnlyWhereTheyCanExist(final VersioningType versioning) throws IOException {
    createVersionedResource(versioning);
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int built = leaves - 6;
    final int grown = 2 * leaves + 8;
    final int openRowGroups = grown - 2 * leaves;
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
        final LongOpenHashSet changed = new LongOpenHashSet();
        for (int rowGroupId = built + 1; rowGroupId <= grown; rowGroupId++) {
          storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
          changed.add(rowGroupId);
        }
        final ProjectionBloomChunks.RewriteStats stats =
            ProjectionBloomChunks.rewriteTouchedChunks(storage, COLUMN_KINDS, grown, changed);

        assertEquals(built + openRowGroups, stats.tailSlotReads(),
            "only chunk 0 owns tails (" + built + " of them) and each open row group checks its own; "
                + "chunk 1 has never held a tail slot, so it must be probed zero times");
        assertEquals(2 + openRowGroups, stats.chunksWritten(),
            "one block per completed chunk plus one tail per open row group");
        assertEquals(grown - built, stats.rowGroupsRead());
        for (int chunkId = 0; chunkId < 2; chunkId++) {
          final byte[] block = storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, chunkId));
          assertNotNull(block, "completed chunk " + chunkId + " is a block");
          assertEquals(leaves, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(block));
        }
        for (int rowGroupId = 1; rowGroupId <= 2 * leaves; rowGroupId++) {
          assertNull(storage.getBlob(ProjectionBloomChunks.tailSlotKey(0, rowGroupId)),
              "completed row group " + rowGroupId + " keeps no tail blob");
        }
        for (int rowGroupId = 2 * leaves + 1; rowGroupId <= grown; rowGroupId++) {
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
          assertDropped(keepRejected, leaf, "leaf " + leaf + " prunes across both completed chunks");
        }
      }
    } finally {
      writer.release();
    }
  }

  /**
   * The even cut's peak must be priced on the range that actually HOLDS the open chunk. When the even
   * split's last range comes out empty the open chunk sits in the second-to-last one with blocks
   * beside it, and modelling it as the last range prices those blocks at zero — so a cut whose real
   * peak is higher is scored as the cheaper one and isolation is wrongly declined.
   */
  @Test
  void theEvenCutIsPricedOnTheRangeHoldingTheOpenChunkEvenWhenItsLastRangeIsEmpty() {
    final int leaves = ProjectionBloomChunks.CHUNK_LEAVES;
    final int sealed = 9;
    final int openTails = 2;
    final int ranges = 6;
    final int rowGroupCount = sealed * leaves + openTails;
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup inline = encodedRowGroup("present");
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup referenced = fatRowGroup();
    assertTrue(bloomSegment(referenced).length > ProjectionIndexHOTStorage.INLINE_SEGMENT_MAX_BYTES,
        "the open chunk's tails must be referenced or they cost no page read");
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= rowGroupCount; rowGroupId++) {
          writer.append(rowGroupId > sealed * leaves
              ? referenced
              : inline, rowGroupId, storage);
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

        // The shape this regression needs: nine blocks and the open chunk over six ranges puts the
        // even cut at 2 units per range, which fills the first five and leaves the last empty.
        final int[] even = evenRangeBounds(column.chunkCount(), ranges);
        assertEquals(even[ranges - 1], even[ranges], "the even cut's last range must be empty here");
        assertArrayEquals(new int[] {0, 2, 4, 6, 8, 10, 10}, even);

        final int[] chosen = column.weightedRangeBounds(ranges);
        assertArrayEquals(new int[] {0, 2, 4, 6, 8, sealed, sealed + 1}, chosen,
            "the open chunk must be isolated: sharing its range costs a block on top of its tails");
        assertEquals(2, rangePeak(chosen, sealed, openTails), "the chosen cut peaks at the open chunk");
        assertEquals(3, rangePeak(even, sealed, openTails),
            "the even cut peaks at the block sharing the open chunk's range, which the model must see");
        assertTrue(rangePeak(chosen, sealed, openTails) < rangePeak(even, sealed, openTails),
            "the split must pick the lower real peak");
      }
    } finally {
      writer.release();
    }
  }

  /**
   * The split is a total function over its documented domain. A column whose published mark is zero
   * spans no chunks at all, and asking it for a partition must hand back empty ranges rather than
   * dividing by an even cut of zero width. No production caller reaches this — the store asks for
   * more than one range only from {@code BLOOM_MANY_PARALLEL_MIN_CHUNKS} chunks up — so the helper's
   * own contract is the only thing holding the line.
   */
  @Test
  void theChunkRangeSplitPartitionsEmptyEvidenceInsteadOfDividingByZero() {
    final ProjectionBloomChunks.Writer writer = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        writer.finishChunks(storage, 0, COLUMN_KINDS);
        writer.publishManifests(storage, 0);
        wtx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 0);
        assertNotNull(evidence, "a published mark of zero is still readable evidence");
        final ProjectionBloomChunks.ColumnEvidence column = evidence[0];
        assertEquals(0, column.chunkCount(), "nothing is sealed and nothing is open");

        for (final int ranges : new int[] {1, 2, 4, 17}) {
          assertArrayEquals(new int[ranges + 1], column.weightedRangeBounds(ranges),
              "every range over empty evidence must come out empty, asked for " + ranges + " ranges");
        }
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        assertEquals(0, column.prune(0L, new long[0], 0, fetcher),
            "there is no leaf to prune and no evidence to prune it with");
      }
    } finally {
      writer.release();
    }
  }

  /** The plain index-even cut the weighted split is measured against. */
  private static int[] evenRangeBounds(final int count, final int ranges) {
    final int[] bounds = new int[ranges + 1];
    final int len = (count + ranges - 1) / ranges;
    for (int r = 1; r <= ranges; r++) {
      bounds[r] = Math.min(r * len, count);
    }
    return bounds;
  }

  /**
   * Page reads the heaviest range of {@code bounds} performs: one per sealed block it holds, plus
   * {@code openWeight} for the range holding the open chunk at index {@code sealed}.
   */
  private static int rangePeak(final int[] bounds, final int sealed, final int openWeight) {
    int peak = 0;
    for (int r = 0; r + 1 < bounds.length; r++) {
      final int from = bounds[r];
      final int to = bounds[r + 1];
      if (from >= to) {
        continue;
      }
      final int blocks = Math.max(0, Math.min(to, sealed) - from);
      peak = Math.max(peak, blocks + (from <= sealed && sealed < to
          ? openWeight
          : 0));
    }
    return peak;
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
    return Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup wideRowGroup(final byte[] kinds,
      final String firstValue) {
    final int columns = kinds.length;
    final String[] values = new String[columns];
    Arrays.fill(values, "right");
    values[0] = firstValue;
    final boolean[] present = new boolean[columns];
    Arrays.fill(present, true);
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
    page.appendRow(1L, new long[columns], new boolean[columns], values, present, new boolean[columns],
        new boolean[columns], new boolean[columns]);
    return Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
  }

  /**
   * One row group whose fingerprint is large enough to be stored as a referenced side page: at ~10
   * bits per distinct value, 500 values give an 8 192-bit filter, just over 1 KiB.
   */
  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup fatRowGroup() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone());
    for (int row = 0; row < 500; row++) {
      page.appendRow(row + 1L, new long[] {0L}, new boolean[] {false}, new String[] {"value-" + row},
          new boolean[] {true}, new boolean[] {false}, new boolean[] {false}, new boolean[] {false});
    }
    return Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup emptyEncodedRowGroup() {
    return Objects.requireNonNull(
        ProjectionIndexColumnSegmentCodec.encode(new ProjectionIndexRowGroupPage(COLUMN_KINDS.clone()).serialize()));
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
    return twoColumnRowGroup(kinds, 1L, left, right);
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup twoColumnRowGroup(final byte[] kinds,
      final long recordKey, final String left, final String right) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
    page.appendRow(recordKey, new long[] {0L, 0L}, new boolean[] {false, false}, new String[] {left, right},
        new boolean[] {true, true}, new boolean[] {false, false}, new boolean[] {false, false},
        new boolean[] {false, false});
    return Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
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
