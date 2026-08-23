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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage, publication, compatibility, and fail-open coverage for {@link ProjectionBloomChunks}.
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
        storage.removeBloomBlocks(COLUMN_KINDS.length);
        writer.publishManifests(storage, tailId);
        final byte[] manifest = storage.getBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0));
        assertTrue(ProjectionBloomChunks.isManifest(manifest, tailId));
        assertEquals(-1, ProjectionIndexColumnSegmentCodec.bloomBlockLeafCount(manifest),
            "an old reader must reject the unknown manifest and use the per-leaf chain");
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
        storage.removeBloomBlocks(COLUMN_KINDS.length);
        writer.publishManifests(storage, rowGroupCount);
        // Valid PIXB wrapper/hash, deliberately malformed PBLM payload: structural corruption must
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
        storage.tombstoneRowGroup(ProjectionBloomChunks.chunkSlotKey(0, 1));
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
        storage.removeBloomBlocks(1);
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
  void legacyContiguousBlockRemainsReadable() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("legacy");
    final byte[] bloom = bloomSegment(encoded);
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
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 1);
        assertNotNull(evidence);
        final long[] keep = prune(evidence[0], 1,
            ProjectionIndexColumnSegmentCodec.bloomHash("legacy".getBytes(StandardCharsets.UTF_8)),
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertKept(keep, 0);
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final byte[] oversized = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(new byte[][] {bloom, bloom}, 2);
        assertNotNull(oversized);
        storage.putBlob(ProjectionIndexHOTStorage.bloomBlockSlotKey(0), oversized);
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, 1),
            "legacy compatibility must require declared leafCount == metadata rowGroupCount");
      }
    }
  }

  @Test
  void referencedLegacyBlockIsDeferredAndRequiresExactDeclaredCount() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("legacy");
    final byte[] bloom = bloomSegment(encoded);
    final byte[][] perLeaf = new byte[ProjectionBloomChunks.CHUNK_LEAVES][];
    Arrays.fill(perLeaf, bloom);
    final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(perLeaf, perLeaf.length);
    assertNotNull(block);
    assertTrue(block.length > 512, "legacy fixture must use a referenced PIXB payload");
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
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
        final ProjectionBloomChunks.ColumnEvidence[] exact =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, perLeaf.length);
        assertNotNull(exact);
        final long[] keep = prune(exact[0], perLeaf.length,
            ProjectionIndexColumnSegmentCodec.bloomHash("legacy".getBytes(StandardCharsets.UTF_8)), fetcher);
        assertKept(keep, 0);
        assertKept(keep, perLeaf.length - 1);

        final ProjectionBloomChunks.ColumnEvidence[] stale =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, perLeaf.length - 1);
        assertNotNull(stale, "a referenced candidate is typed only after its deferred fetch");
        final long[] staleKeep = new long[(perLeaf.length - 1 + 63) >>> 6];
        Arrays.fill(staleKeep, -1L);
        assertEquals(-1, stale[0].prune(Long.MIN_VALUE, staleKeep, perLeaf.length - 1, fetcher),
            "declared legacy leaf count must equal metadata exactly");
        assertKept(staleKeep, 0, "rejected legacy evidence must not mutate keep bits");
      }
    }
  }

  @Test
  void reorderedEvidencePrunesLogicalRatherThanPhysicalLeafPositions() {
    final byte[][] perLeaf = {bloomSegment(encodedRowGroup("first")), bloomSegment(encodedRowGroup("second")),
        bloomSegment(encodedRowGroup("inserted")),};
    final byte[] block = ProjectionIndexColumnSegmentCodec.encodeBloomBlock(perLeaf, perLeaf.length);
    final ProjectionBloomChunks.ColumnEvidence[] physical =
        ProjectionBloomChunks.fromLegacyBlocks(new byte[][] {block}, perLeaf.length);
    assertNotNull(physical);
    final ProjectionBloomChunks.ColumnEvidence[] logical = ProjectionBloomChunks.reorder(physical, new int[] {1, 3, 2});
    final long hash = ProjectionIndexColumnSegmentCodec.bloomHash("second".getBytes(StandardCharsets.UTF_8));
    final long[] keep = prune(logical[0], perLeaf.length, hash, (offsets) -> new byte[offsets.length][]);

    assertDropped(keep, 0, "the physical first leaf is logical first");
    assertDropped(keep, 1, "the inserted physical third leaf is logical second");
    assertKept(keep, 2, "physical leaf two must map to logical leaf three");
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
        storage.removeBloomBlocks(1);
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
  void allEmptyRewriteTombstonesPriorChunk() {
    final ProjectionBloomChunks.Writer populated = new ProjectionBloomChunks.Writer();
    final ProjectionBloomChunks.Writer empty = new ProjectionBloomChunks.Writer();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        appendChunk(populated, encodedRowGroup("present"), storage);
        populated.finishChunks(storage, ProjectionBloomChunks.CHUNK_LEAVES, COLUMN_KINDS);
        storage.removeBloomBlocks(1);
        populated.publishManifests(storage, ProjectionBloomChunks.CHUNK_LEAVES);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        appendChunk(empty, emptyEncodedRowGroup(), storage);
        empty.finishChunks(storage, ProjectionBloomChunks.CHUNK_LEAVES, COLUMN_KINDS);
        storage.removeBloomBlocks(1);
        empty.publishManifests(storage, ProjectionBloomChunks.CHUNK_LEAVES);
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)),
            "an all-empty current span must tombstone, not inherit, the prior negative filter");
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
      populated.release();
      empty.release();
    }
  }

  @Test
  void shrinkingRebuildReclaimsOnlyPriorManifestTrailingChunks() {
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encodedRowGroup("present");
    final ProjectionBloomChunks.Writer large = new ProjectionBloomChunks.Writer();
    final ProjectionBloomChunks.Writer small = new ProjectionBloomChunks.Writer();
    final int largeCount = ProjectionBloomChunks.CHUNK_LEAVES * 3;
    final int smallCount = ProjectionBloomChunks.CHUNK_LEAVES;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= largeCount; rowGroupId++) {
          large.append(encoded, rowGroupId, storage);
        }
        large.finishChunks(storage, largeCount, COLUMN_KINDS);
        storage.removeBloomBlocks(1);
        large.publishManifests(storage, largeCount);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int rowGroupId = 1; rowGroupId <= smallCount; rowGroupId++) {
          small.append(encoded, rowGroupId, storage);
        }
        small.finishChunks(storage, smallCount, COLUMN_KINDS);
        storage.removeBloomBlocks(1);
        small.publishManifests(storage, smallCount);
        assertNotNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 0)));
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 1)),
            "first prior trailing chunk must be tombstoned");
        assertNull(storage.getBlob(ProjectionBloomChunks.chunkSlotKey(0, 2)),
            "last prior trailing chunk must be tombstoned");
        wtx.commit();
      }
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final ProjectionBloomChunks.ColumnEvidence[] evidence =
            ProjectionBloomChunks.read(rtx.getStorageEngineReader(), INDEX_NUMBER, COLUMN_KINDS, smallCount);
        assertNotNull(evidence);
        final long[] keep = prune(evidence[0], smallCount,
            ProjectionIndexColumnSegmentCodec.bloomHash("present".getBytes(StandardCharsets.UTF_8)),
            ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber()));
        assertKept(keep, 0);
        assertKept(keep, smallCount - 1);
      }
    } finally {
      large.release();
      small.release();
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
        storage.removeBloomBlocks(1);
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
    return ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(page.serialize());
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup emptyEncodedRowGroup() {
    return ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(
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
