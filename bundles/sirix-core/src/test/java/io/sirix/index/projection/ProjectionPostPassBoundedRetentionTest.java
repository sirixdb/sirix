/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witnesses the explicit (post-pass) projection build's bounded-retention contract: the build rides
 * async-flush epochs, so the transaction intent log holds one epoch of pages instead of the entire
 * output. Without the intermediate flushes a 100M-row build pins ~20 GB of live frames and dies of
 * arena exhaustion — the bound is the fix, and the inverted arm proves the witness can say no.
 */
final class ProjectionPostPassBoundedRetentionTest {

  private static final String FLUSH_PROPERTY = "sirix.projection.buildIntermediateFlush";

  /**
   * 40 full row groups (1024 rows each) — enough leaves that an unbounded build's live intent-log
   * entry count separates from the epoch bound by a wide, drift-tolerant margin.
   */
  private static final int COMMITTED_RECORD_COUNT = 40 * 1024;

  /**
   * Crosses the flush boundary dozens of times with full rotation+cleanup cycles, so the
   * read-back-after-rotation contract is exercised for real, not just in the final epoch.
   */
  private static final int UNCOMMITTED_RECORD_COUNT = 40 * 1024;

  @TempDir
  Path temporaryDirectory;

  private Path databasePath;

  @BeforeEach
  void setUp() {
    databasePath = temporaryDirectory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
  }

  @AfterEach
  void tearDown() {
    System.clearProperty(FLUSH_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @Test
  void intermediateFlushesBoundTheIntentLogAndPreserveTheProjection() throws Exception {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      final int boundedLive = buildOverCommittedCorpus(database, "bounded", null);
      final int pinnedLive = buildOverCommittedCorpus(database, "pinned", "false");

      // The absolute contract: with flushes on, the log never exceeds one epoch of pages plus the
      // end-phase writes (Bloom/fence finish, header, dictionaries).
      assertTrue(boundedLive <= 64,
          "flushing build should retain at most one epoch of intent-log entries, held " + boundedLive);
      // The inverted arm is the positive witness that the counter can say no: the legacy build
      // pins every page it creates, so its live count must dwarf the flushing build's.
      assertTrue(pinnedLive > boundedLive * 3,
          "legacy build must pin the whole output (flushing=" + boundedLive + ", pinned=" + pinnedLive + ")");

      // Both arms must describe the same projection: identical leaf structure and column content.
      final long[] bounded = projectionRowsAndChecksum(database, "bounded");
      final long[] pinned = projectionRowsAndChecksum(database, "pinned");
      assertEquals(COMMITTED_RECORD_COUNT, bounded[0], "flushing build lost rows");
      assertEquals(bounded[0], pinned[0], "arms disagree on row count");
      assertEquals(bounded[1], pinned[1], "arms disagree on column content");
    }
  }

  @Test
  void buildInsideAMutatingTransactionFlushesAndKeepsEveryUncommittedRecord() throws Exception {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      final String resource = "uncommitted";
      createResource(database, resource);
      try (JsonResourceSession session = database.beginResourceSession(resource);
          JsonNodeTrx wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser(corpusJson(UNCOMMITTED_RECORD_COUNT))) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).build().call();
        // The document records are still uncommitted and live in the intent log — and the build
        // flushes anyway: a rotated-out page of the open revision resolves back from disk through
        // the log's recorded offsets, so the extraction reads every record across rotations. This
        // is the contract that lets a 100M build stay within one epoch of retained pages even
        // inside a mutating transaction.
        assertTrue(wtx.getStorageEngineWriter().getLog().liveEntryCount() > 0,
            "precondition: the corpus must still live in the intent log");
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        controller.createIndexes(Set.of(projectionDefinition(0)), wtx);
        final int liveEntries = wtx.getStorageEngineWriter().getLog().liveEntryCount();
        assertTrue(liveEntries <= 64,
            "the build must rotate the corpus and its own output out of the log, held " + liveEntries);
        wtx.commit();
      }
      final long[] result = projectionRowsAndChecksum(database, resource);
      assertEquals(UNCOMMITTED_RECORD_COUNT, result[0],
          "build inside a mutating transaction must keep every uncommitted record");
      assertEquals(expectedNumericChecksum(UNCOMMITTED_RECORD_COUNT), result[1],
          "extracted column content must be exact across rotations");
    }
  }

  /**
   * The checksum {@link #projectionRowsAndChecksum} computes, folded over the generated corpus
   * directly: records carry {@code a = 0..n-1} in insertion order.
   */
  private static long expectedNumericChecksum(final int recordCount) {
    long checksum = 1469598103934665603L;
    for (long value = 0; value < recordCount; value++) {
      checksum = (checksum ^ value) * 1099511628211L;
    }
    return checksum;
  }

  /**
   * Shreds and commits the corpus into a fresh resource, then builds the projection in a new
   * transaction, returning the intent log's live entry count captured right before the commit.
   */
  private int buildOverCommittedCorpus(final Database<JsonResourceSession> database, final String resource,
      final String flushProperty) throws Exception {
    createResource(database, resource);
    try (JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeTrx wtx = session.beginNodeTrx();
        var parser = JacksonJsonShredder.createStringParser(corpusJson(COMMITTED_RECORD_COUNT))) {
      new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    if (flushProperty == null) {
      System.clearProperty(FLUSH_PROPERTY);
    } else {
      System.setProperty(FLUSH_PROPERTY, flushProperty);
    }
    try (JsonResourceSession session = database.beginResourceSession(resource);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final JsonIndexController controller =
          (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      controller.createIndexes(Set.of(projectionDefinition(0)), wtx);
      final int liveEntries = wtx.getStorageEngineWriter().getLog().liveEntryCount();
      wtx.commit();
      return liveEntries;
    } finally {
      System.clearProperty(FLUSH_PROPERTY);
    }
  }

  /** Total row count and an order-sensitive checksum of the numeric column across every leaf. */
  private long[] projectionRowsAndChecksum(final Database<JsonResourceSession> database, final String resource) {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (JsonResourceSession session = database.beginResourceSession(resource);
        var rtx = session.beginNodeReadOnlyTrx()) {
      final IndexDef definition = projectionDefinition(0);
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.load(session, rtx.getRevisionNumber(), definition);
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(session,
          rtx.getRevisionNumber(), definition.getID(), handle.rowGroupCount()));
      long rows = 0;
      long checksum = 1469598103934665603L;
      for (final byte[] payload : leaves) {
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
        rows += page.getRowCount();
        final long[] column = page.numericColumn(0);
        for (int row = 0; row < page.getRowCount(); row++) {
          checksum = (checksum ^ column[row]) * 1099511628211L;
        }
      }
      return new long[] {rows, checksum};
    }
  }

  private static void createResource(final Database<JsonResourceSession> database, final String resource) {
    assertTrue(database.createResource(ResourceConfiguration.newBuilder(resource)
                                                            .storageType(StorageType.FILE_CHANNEL)
                                                            .hashKind(HashType.NONE)
                                                            .storeDiffs(false)
                                                            .buildPathSummary(true)
                                                            .buildPathStatistics(false)
                                                            .useDeweyIDs(true)
                                                            .build()));
  }

  private static IndexDef projectionDefinition(final int indexNumber) {
    return IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/a", PathParser.Type.JSON), parse("/[]/b", PathParser.Type.JSON)),
        List.of(Type.LON, Type.STR), indexNumber, IndexDef.DbType.JSON);
  }

  private static String corpusJson(final int recordCount) {
    final StringBuilder json = new StringBuilder(recordCount * 32).append('[');
    for (int i = 0; i < recordCount; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"a\":").append(i).append(",\"b\":\"s").append(i % 50).append("\"}");
    }
    return json.append(']').toString();
  }
}
