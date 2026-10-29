/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.access.trx.page.StorageEpochTestSupport;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Projection maintenance across async-flush storage epochs must leave the same projection as the
 * same edits in one plain transaction.
 *
 * <p>
 * A {@link AfterCommitState#KEEP_OPEN_ASYNC_FLUSH} transaction rotates a storage epoch every
 * {@code MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT} modified record pages and runs the pending projection
 * maintenance before each rotation. That maintenance rewrites the fence chunk of the open leaf as a
 * referenced blob, which stays resident as an in-memory {@code OverflowPage} until its offset is
 * published one epoch later. The next pass read that chunk back and, when the chunk was already
 * full (32 entries, 7,808 bytes, so no growth copy was needed), used the page's own backing array
 * as its working buffer: every fence update then silently rewrote the still attached page under its
 * recorded hash, and the following pass failed {@code verifyPriorBlobForMutation} ("Blob at slot
 * ... failed length/hash verification") on the very first SH1 publication with a bounded
 * transaction. The fixture therefore holds more than 32 row groups, so the first fence chunk is
 * full, and every step removes the first record of a few row groups in that chunk: each removal
 * moves a fence, so every pass reads and rewrites the full chunk the previous pass left in memory.
 * The updates are spread over enough record pages to rotate several epochs per step, and the
 * appends grow the open tail.
 * </p>
 *
 * <p>
 * The twin runs the same script in a {@link AfterCommitState#KEEP_OPEN} transaction, which rotates
 * storage epochs only at the intent-log page bound and never at the async node-count epoch, so the
 * two runs interleave maintenance passes differently and must still agree row group by row group.
 * </p>
 *
 * <p>
 * A plain transaction whose intent log reaches its page bound rotates storage-only epochs and, like
 * the async-flush mode, runs the pending projection maintenance before each of them, while its
 * records are still in memory. Those passes partition the publication's dirty records differently
 * from the single commit-time pass of an unbounded transaction and must still produce the same
 * projection.
 * </p>
 */
final class AsyncFlushProjectionMaintenanceTest {

  private static final int INDEX = 0;

  /** More than one full fence chunk of row groups; updates land on many distinct record pages. */
  private static final int INITIAL_RECORDS = 40 * ProjectionIndexRowGroupPage.MAX_ROWS;

  /** One update per stride lands on a new record page: 16 of them rotate one storage epoch. */
  private static final int UPDATE_STRIDE = 701;

  private static final int STEPS = 6;
  private static final int UPDATES_PER_STEP = 24;

  /** Each step removes the current first record of this many row groups of the first fence chunk. */
  private static final int REMOVALS_PER_STEP = 4;
  private static final int APPENDS_PER_STEP = 40;

  @TempDir
  java.nio.file.Path directory;

  /** Data-file record reads counted when the last edited transaction began its commit. */
  private static long durableReadsBeforeCommit;

  @AfterEach
  void tearDown() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @AfterEach
  void restoreStorageEpochBound() {
    StorageEpochTestSupport.setPlainTransactionBound(0);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void asyncFlushEpochsPersistTheSameProjectionAsOnePlainTransaction() {
    twinRun(AfterCommitState.KEEP_OPEN_ASYNC_FLUSH, 0);
  }

  /**
   * A plain transaction at the import page bound rotates storage-only epochs during every step, each
   * with a maintenance pass over the records dirtied since the last one; the result must agree with
   * the unbounded twin's single commit-time pass.
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void plainTransactionEpochsPersistTheSameProjectionAsAnUnboundedTransaction() {
    twinRun(AfterCommitState.KEEP_OPEN, StorageEpochTestSupport.IMPORT_BOUND);
  }

  @Test
  void capturedRowsKeepTheirKindWithoutCursorReadsAndMutatedRootsMatchTheirTwin() {
    final var boundedPath = directory.resolve("capture-bounded");
    final var plainPath = directory.resolve("capture-plain");
    create(boundedPath);
    create(plainPath);
    try (final Database<JsonResourceSession> boundedDatabase = Databases.openJsonDatabase(boundedPath);
        final Database<JsonResourceSession> plainDatabase = Databases.openJsonDatabase(plainPath);
        final JsonResourceSession bounded = boundedDatabase.beginResourceSession("resource");
        final JsonResourceSession plain = plainDatabase.beginResourceSession("resource")) {
      final List<Long> keys = load(bounded);
      assertEquals(keys, load(plain));
      for (final JsonResourceSession session : List.of(bounded, plain)) {
        try (final JsonNodeTrx writer = session.beginNodeTrx();
            final PathSummaryReader paths = session.openPathSummary()) {
          final long key = keys.get(0);
          assertTrue(writer.moveTo(key) && writer.moveToFirstChild());
          writer.setNumberValue(123456);
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(definition(), paths);
          assertTrue(extractor.extractInto(writer, key));
          final ProjectionIndexRowExtractor.CapturedRow row = extractor.captureRow();
          if (session == bounded) {
            writer.getStorageEngineWriter().captureRecordForDeferredRead(key);
            writer.getStorageEngineWriter().asyncFlushStorageEpoch();
            writer.getStorageEngineWriter().awaitPendingAsyncFlush();
            extractor.useCapturedRows(record -> record == key
                ? row
                : null);
            assertTrue(writer.moveToDocumentRoot());
            final long saved = writer.getNodeKey();
            final long reads = StorageEpochTestSupport.durableDocumentRecordReads();
            assertTrue(extractor.extractInto(writer, key));
            assertEquals(saved, writer.getNodeKey());
            assertEquals(NodeKind.OBJECT, extractor.recordKind());
            assertTrue(extractor.extractInto(writer, key, new long[] {1L}));
            assertEquals(saved, writer.getNodeKey());
            assertEquals(NodeKind.OBJECT, extractor.recordKind());
            assertEquals(reads, StorageEpochTestSupport.durableDocumentRecordReads());
          }
          assertTrue(writer.moveTo(key));
          writer.insertObjectRecordAsFirstChild("extra", new StringValue("unindexed"));
          assertTrue(writer.moveTo(key));
          final long firstChild = writer.getFirstChildKey();
          final long children = writer.getChildCount();
          if (session == bounded) {
            writer.getStorageEngineWriter().asyncFlushStorageEpoch();
            writer.getStorageEngineWriter().awaitPendingAsyncFlush();
          }
          assertTrue(writer.moveToDocumentRoot() && writer.moveTo(key));
          assertEquals(children, writer.getChildCount());
          assertEquals(firstChild, writer.getFirstChildKey());
          writer.commit();
        }
      }
      try (final JsonNodeReadOnlyTrx left = bounded.beginNodeReadOnlyTrx();
          final JsonNodeReadOnlyTrx right = plain.beginNodeReadOnlyTrx()) {
        assertTrue(left.moveTo(keys.get(0)) && right.moveTo(keys.get(0)));
        assertEquals(right.getChildCount(), left.getChildCount());
        assertEquals(right.getFirstChildKey(), left.getFirstChildKey());
        final StorageEngineReader leftStorage = left.getStorageEngineReader();
        final StorageEngineReader rightStorage = right.getStorageEngineReader();
        final ProjectionIndexMetadata meta = ProjectionIndexMetadata.requireReady(leftStorage, INDEX);
        final ProjectionIndexMetadata twinMeta = ProjectionIndexMetadata.requireReady(rightStorage, INDEX);
        assertEquals(twinMeta.rowGroupCount(), meta.rowGroupCount());
        assertArrayEquals(
            projectionRows(rightStorage,
                ProjectionIndexFences.readPhysicalOrder(rightStorage, INDEX, twinMeta.rowGroupCount())),
            projectionRows(leftStorage,
                ProjectionIndexFences.readPhysicalOrder(leftStorage, INDEX, meta.rowGroupCount())));
      }
    }
  }

  /**
   * @param boundedMode the transaction mode of the bounded run
   * @param plainBound a positive value bounds the live intent log of the bounded run at that many
   *        pages (the twin stays unbounded); {@code 0} leaves the budget-derived bound
   */
  private void twinRun(final AfterCommitState boundedMode, final int plainBound) {
    final java.nio.file.Path boundedPath = directory.resolve("bounded");
    final java.nio.file.Path plainPath = directory.resolve("plain");
    create(boundedPath);
    create(plainPath);
    try (final Database<JsonResourceSession> boundedDatabase = Databases.openJsonDatabase(boundedPath);
        final Database<JsonResourceSession> plainDatabase = Databases.openJsonDatabase(plainPath);
        final JsonResourceSession bounded = boundedDatabase.beginResourceSession("resource");
        final JsonResourceSession plain = plainDatabase.beginResourceSession("resource")) {
      final List<Long> boundedKeys = load(bounded);
      final List<Long> plainKeys = load(plain);
      assertEquals(boundedKeys, plainKeys, "both loads assign the same node keys");

      StorageEpochTestSupport.setPlainTransactionBound(plainBound);
      StorageEpochTestSupport.resetRecordCaptureCounters();
      final int boundedEpochs = edit(bounded, boundedKeys, boundedMode);
      final long capturedHits = StorageEpochTestSupport.capturedRecordHits();
      final long commitDurableReads = StorageEpochTestSupport.durableDocumentRecordReads() - durableReadsBeforeCommit;
      StorageEpochTestSupport.setPlainTransactionBound(0);
      edit(plain, plainKeys, AfterCommitState.KEEP_OPEN);
      // Each step updates more distinct record pages than the bound holds, so a bounded run rotates
      // at least once per step; without that the twins would merely repeat each other.
      assertTrue(boundedEpochs >= STEPS, "the bounded run rotated " + boundedEpochs + " storage epochs");
      if (boundedMode == AfterCommitState.KEEP_OPEN) {
        // The commit-time pass re-reads every dirty record whose page an epoch wrote out; those reads
        // must be answered from the captures taken before the epochs, not from the data file.
        final String figures =
            "captured hits " + capturedHits + ", data-file reads during the commit " + commitDurableReads;
        assertTrue(capturedHits > 0, "the commit pass read flushed records from their captures: " + figures);
        assertEquals(0L, commitDurableReads, "the commit pass did not fall back to the data file: " + figures);
      }

      assertEquals(plain.getMostRecentRevisionNumber(), bounded.getMostRecentRevisionNumber(),
          "the bounded transaction publishes exactly one revision, like the plain one");
      try (final JsonNodeReadOnlyTrx boundedReader = bounded.beginNodeReadOnlyTrx();
          final JsonNodeReadOnlyTrx plainReader = plain.beginNodeReadOnlyTrx()) {
        final StorageEngineReader boundedStorage = boundedReader.getStorageEngineReader();
        final StorageEngineReader plainStorage = plainReader.getStorageEngineReader();
        final ProjectionIndexMetadata boundedMeta = ProjectionIndexMetadata.requireReady(boundedStorage, INDEX);
        final ProjectionIndexMetadata plainMeta = ProjectionIndexMetadata.requireReady(plainStorage, INDEX);
        assertEquals(plainMeta.rowGroupCount(), boundedMeta.rowGroupCount(), "row group count");
        final int[] plainOrder =
            ProjectionIndexFences.readPhysicalOrder(plainStorage, INDEX, plainMeta.rowGroupCount());
        final int[] boundedOrder =
            ProjectionIndexFences.readPhysicalOrder(boundedStorage, INDEX, boundedMeta.rowGroupCount());
        assertArrayEquals(plainOrder, boundedOrder, "the fence chain visits the same leaves in the same order");
        assertTrue(plainOrder.length > ProjectionIndexFences.CHUNK_LEAVES, "the fixture fills the first fence chunk");
        // Row contents are compared decoded, not byte for byte: the open tail row group may be encoded
        // differently depending on when maintenance last ran, while its rows must not.
        final long[][] documents = documentRows(plainReader);
        assertArrayEquals(documents, documentRows(boundedReader), "the documents hold the same values");
        assertArrayEquals(documents, projectionRows(plainStorage, plainOrder), "plain projection rows");
        assertArrayEquals(documents, projectionRows(boundedStorage, boundedOrder), "bounded projection rows");
      }
    }
    // Cold reopen: everything the bounded transaction committed must be readable from the data file
    // alone, in particular nothing may still point at a page an epoch only wrote to the spill file.
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> boundedDatabase = Databases.openJsonDatabase(boundedPath);
        final JsonResourceSession bounded = boundedDatabase.beginResourceSession("resource");
        final JsonNodeReadOnlyTrx boundedReader = bounded.beginNodeReadOnlyTrx()) {
      final StorageEngineReader boundedStorage = boundedReader.getStorageEngineReader();
      final ProjectionIndexMetadata boundedMeta = ProjectionIndexMetadata.requireReady(boundedStorage, INDEX);
      final int[] boundedOrder =
          ProjectionIndexFences.readPhysicalOrder(boundedStorage, INDEX, boundedMeta.rowGroupCount());
      final long[][] documents = documentRows(boundedReader);
      assertArrayEquals(documents, projectionRows(boundedStorage, boundedOrder),
          "bounded projection rows after a cold reopen");
    }
  }

  private static void create(final java.nio.file.Path path) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
    }
  }

  /** Load the initial array with the projection declared and return the record node keys in order. */
  private static List<Long> load(final JsonResourceSession session) {
    final List<Long> keys = new ArrayList<>(INITIAL_RECORDS);
    try (final JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(initialRecords()), JsonNodeTrx.Commit.NO);
      ((JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber())).createIndexes(
          Set.of(definition()), writer);
      assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), "the record array");
      if (writer.moveToFirstChild()) {
        do {
          keys.add(writer.getNodeKey());
        } while (writer.moveToRightSibling());
      }
      assertEquals(INITIAL_RECORDS, keys.size(), "the loaded record array");
      writer.commit();
    }
    return keys;
  }

  /**
   * The same deterministic edit script for both transactions: updates spread over many record pages
   * (so a bounded transaction rotates several storage epochs), removals, and appends that change the
   * fence chunks in every pass.
   */
  private static int edit(final JsonResourceSession session, final List<Long> recordKeys,
      final AfterCommitState afterCommitState) {
    try (final JsonNodeTrx writer = session.beginNodeTrx(afterCommitState)) {
      assertTrue(writer.moveToDocumentRoot() && writer.moveToFirstChild(), "the record array");
      final long arrayKey = writer.getNodeKey();
      final Set<Integer> removed = new HashSet<>();
      int appended = 0;
      for (int step = 0; step < STEPS; step++) {
        for (int update = 0; update < UPDATES_PER_STEP; update++) {
          int ordinal = ((step * UPDATES_PER_STEP + update) * UPDATE_STRIDE) % INITIAL_RECORDS;
          while (removed.contains(ordinal)) {
            ordinal = (ordinal + 1) % INITIAL_RECORDS;
          }
          assertTrue(writer.moveTo(recordKeys.get(ordinal)) && writer.moveToFirstChild(), "the record's field");
          writer.setNumberValue(1_000_000 + step * UPDATES_PER_STEP + update);
        }
        // The bulk build packs MAX_ROWS records per row group in document order, so record k * MAX_ROWS
        // opens row group k; removing it moves that group's lower fence.
        for (int removal = 0; removal < REMOVALS_PER_STEP; removal++) {
          final int ordinal = (step * REMOVALS_PER_STEP + removal) * ProjectionIndexRowGroupPage.MAX_ROWS;
          assertTrue(ordinal < ProjectionIndexFences.CHUNK_LEAVES * ProjectionIndexRowGroupPage.MAX_ROWS,
              "the removal stays in the first fence chunk");
          assertTrue(removed.add(ordinal));
          assertTrue(writer.moveTo(recordKeys.get(ordinal)), "the record to remove");
          writer.remove();
        }
        for (int append = 0; append < APPENDS_PER_STEP; append++) {
          assertTrue(writer.moveTo(arrayKey));
          writer.insertSubtreeAsLastChild(
              JsonShredder.createStringReader("{\"v\":" + (INITIAL_RECORDS + appended++) + "}"), JsonNodeTrx.Commit.NO);
        }
      }
      // The intent-log generation counts the storage epochs rotated so far in this transaction.
      final int epochs = StorageEpochTestSupport.rotatedEpochs(writer);
      // Reads that reach the data file during the edits are the transaction touching flushed pages
      // again; only the commit's reads are the maintenance pass's.
      durableReadsBeforeCommit = StorageEpochTestSupport.durableDocumentRecordReads();
      writer.commit();
      return epochs;
    }
  }

  /** Every record of the array as {record key, value}, sorted by record key. */
  private static long[][] documentRows(final JsonNodeReadOnlyTrx reader) {
    final List<long[]> rows = new ArrayList<>(INITIAL_RECORDS + STEPS * APPENDS_PER_STEP);
    assertTrue(reader.moveToDocumentRoot() && reader.moveToFirstChild() && reader.moveToFirstChild());
    do {
      final long record = reader.getNodeKey();
      assertTrue(reader.moveToFirstChild());
      rows.add(new long[] {record, reader.getNumberValue().longValue()});
      reader.moveTo(record);
    } while (reader.moveToRightSibling());
    assertEquals(INITIAL_RECORDS + STEPS * (APPENDS_PER_STEP - REMOVALS_PER_STEP), rows.size(), "record count");
    return sorted(rows);
  }

  /** Every projection row as {record key, value}, sorted by record key. */
  private static long[][] projectionRows(final StorageEngineReader storage, final int[] order) {
    final List<long[]> rows = new ArrayList<>(INITIAL_RECORDS + STEPS * APPENDS_PER_STEP);
    for (final int slot : order) {
      final byte[] bytes = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, slot);
      assertNotNull(bytes, "row group " + slot);
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(bytes);
      final long[] keys = page.recordKeys();
      final long[] values = page.numericColumn(0);
      final long[] presence = page.presenceColumnBits(0);
      for (int row = 0; row < page.getRowCount(); row++) {
        assertTrue((presence[row >>> 6] & (1L << (row & 63))) != 0,
            "row " + row + " of row group " + slot + " has a value");
        rows.add(new long[] {keys[row], values[row]});
      }
    }
    return sorted(rows);
  }

  private static long[][] sorted(final List<long[]> rows) {
    final long[][] array = rows.toArray(new long[0][]);
    Arrays.sort(array, (left, right) -> Long.compare(left[0], right[0]));
    return array;
  }

  private static IndexDef definition() {
    final List<Path<QNm>> fieldPaths = List.of(parse("/[]/v", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON), fieldPaths, List.of(Type.LON), INDEX,
        IndexDef.DbType.JSON);
  }

  private static String initialRecords() {
    final StringBuilder json = new StringBuilder(INITIAL_RECORDS * 12).append('[');
    for (int record = 0; record < INITIAL_RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"v\":").append(record).append('}');
    }
    return json.append(']').toString();
  }
}
