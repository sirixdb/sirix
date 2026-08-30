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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential gate for the parallel row-group directory walk
 * ({@link ProjectionIndexHOTStorage#readAllRowGroupDirectoriesFromColumnSegmentSlots(StorageEngineReader, int, int, ProjectionIndexHOTStorage.ParallelWalkReaders)})
 * against the serial cursor walk it replaces.
 *
 * <p>
 * The store is deliberately built past the ~160-row-group mark where the trie's topology order
 * stops agreeing with key order (see {@code DirectoryWalk}'s javadoc: a measured 196-leaf index
 * scanned as {@code 1..159, 192..196, 160..191}). That is the shape an order-sensitive reader gets
 * wrong, and the parallel walk's whole premise is that order does not matter.
 */
final class ProjectionParallelDirectoryWalkTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  /** Past the out-of-order-topology threshold, and wide enough to span many HOT leaves. */
  private static final int ROW_GROUPS = 400;

  /**
   * Twelve columns, not three: a row group occupies one descriptor slot plus one slot per segment
   * (KEYS, a BODY per column, a DICT per string column), and it is SLOTS — not row groups — that
   * decide how many HOT leaves the store spans. Three columns put all 400 row groups on five leaves,
   * which is narrower than the walk's fan-out floor and would have taken the serial route.
   */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
          ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
          ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static final int COLUMNS = KINDS.length;

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

  @Test
  void parallelWalkReproducesTheSerialWalkOverManyRowGroups() {
    writeRowGroups();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final int revision = session.getMostRecentRevisionNumber();

      Databases.getGlobalBufferManager().clearAllCaches();
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> serial;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        serial = ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, ROW_GROUPS);
      }
      assertNotNull(serial, "the serial walk must serve this store");
      assertEquals(ROW_GROUPS, serial.size());

      Databases.getGlobalBufferManager().clearAllCaches();
      final AtomicInteger leases = new AtomicInteger();
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> parallel;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final int partitions =
            ProjectionIndexHOTStorage.parallelWalkPartitionsForTest(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertTrue(partitions >= 8, "frontier of " + partitions + " references is too narrow to fan out — this store "
            + "would silently take the serial route and the test would prove nothing");
        parallel = ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
            rtx.getStorageEngineReader(), INDEX_NUMBER, ROW_GROUPS, worker -> {
              leases.incrementAndGet();
              try (JsonNodeReadOnlyTrx laneRtx = session.beginNodeReadOnlyTrx(revision)) {
                worker.accept(laneRtx.getStorageEngineReader());
              }
            });
      }
      assertTrue(leases.get() >= 2, "the parallel walk declined — only " + leases.get() + " worker leases were opened");
      assertDirectoriesEqual(serial, parallel);
    }
  }

  /**
   * A writer's reader resolves through a transaction intent log, so the parallel route must never
   * engage for it even when a factory is offered — the walk still has to serve.
   */
  @Test
  void writerContextKeepsTheSerialWalkEvenWhenOfferedWorkers() {
    writeRowGroups();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final int revision = session.getMostRecentRevisionNumber();
      Databases.getGlobalBufferManager().clearAllCaches();
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final StorageEngineReader writerReader = wtx.getStorageEngineWriter().getStorageEngineReader();
        assertTrue(writerReader.hasTrxIntentLog(), "a writer's reader must carry the intent log");
        final AtomicInteger leases = new AtomicInteger();
        final List<ProjectionIndexHOTStorage.RowGroupDirectory> served =
            ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(writerReader, INDEX_NUMBER,
                ROW_GROUPS, worker -> {
                  leases.incrementAndGet();
                  try (JsonNodeReadOnlyTrx laneRtx = session.beginNodeReadOnlyTrx(revision)) {
                    worker.accept(laneRtx.getStorageEngineReader());
                  }
                });
        assertEquals(0, leases.get(), "no worker lease may be opened against a writer context");
        assertNotNull(served, "the writer context must still be served by the serial walk");
        assertEquals(ROW_GROUPS, served.size());
      }
    }
  }

  private static void assertDirectoriesEqual(final List<ProjectionIndexHOTStorage.RowGroupDirectory> expected,
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> actual) {
    assertNotNull(actual, "the parallel walk must serve this store");
    assertEquals(expected.size(), actual.size(), "row group count");
    for (int i = 0; i < expected.size(); i++) {
      final ProjectionIndexHOTStorage.RowGroupDirectory want = expected.get(i);
      final ProjectionIndexHOTStorage.RowGroupDirectory got = actual.get(i);
      final String at = " at position " + i;
      assertEquals(want.rowGroupId(), got.rowGroupId(), "rowGroupId" + at);
      assertArrayEquals(want.descriptor(), got.descriptor(), "descriptor" + at);
      assertArrayEquals(want.columnSegmentIds(), got.columnSegmentIds(), "segment ids" + at);
      assertArrayEquals(want.columnSegmentOffsets(), got.columnSegmentOffsets(), "segment offsets" + at);
      for (int entry = 0; entry < want.columnSegmentIds().length; entry++) {
        assertArrayEquals(want.inlineBytesAt(entry), got.inlineBytesAt(entry), "inline payload of entry " + entry + at);
      }
    }
  }

  /**
   * {@link #ROW_GROUPS} row groups in one commit. Every fifth is stored referenced-only so both
   * capture branches — inline slot bytes and a referenced segment's durable offset — appear in the
   * directories being compared.
   */
  private static void writeRowGroups() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int id = 1; id <= ROW_GROUPS; id++) {
        final byte[] raw = rawRowGroup(id);
        storage.putRowGroupAsColumnSegmentSlots(id, id % 5 == 0
            ? ProjectionIndexColumnSegmentCodec.encode(raw)
            : ProjectionIndexColumnSegmentCodec.encode(raw));
      }
      wtx.commit();
    }
  }

  /** Deterministic row group over {@link #KINDS}; {@code id} seeds both the keys and the values. */
  private static byte[] rawRowGroup(final int id) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random rng = new Random(id);
    final long[] longs = new long[COLUMNS];
    final boolean[] bools = new boolean[COLUMNS];
    final String[] strings = new String[COLUMNS];
    final boolean[] present = new boolean[COLUMNS];
    final boolean[] unrepresentable = new boolean[COLUMNS];
    final boolean[] nonIntegral = new boolean[COLUMNS];
    Arrays.fill(present, true);
    long key = id * 100_000L;
    for (int row = 0; row < 48; row++) {
      key += 4 + rng.nextInt(5);
      for (int col = 0; col < COLUMNS; col++) {
        switch (KINDS[col]) {
          case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG -> longs[col] = 18 + rng.nextInt(48);
          case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> bools[col] = rng.nextBoolean();
          default -> strings[col] = DEPTS[rng.nextInt(DEPTS.length)] + "-" + col;
        }
      }
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrepresentable, nonIntegral));
    }
    return page.serialize();
  }
}
