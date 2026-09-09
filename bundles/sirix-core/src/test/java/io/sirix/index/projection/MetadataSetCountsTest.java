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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MetadataSetCountsTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};
  private static final byte[] MULTI_SET_KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};

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
  void metadataRoundTripsAnExplicitCapabilityWithoutInliningCounts() {
    final Map<Integer, Map<String, Long>> summaries = new LinkedHashMap<>();
    summaries.put(1, sampleGenres());
    final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(metadata(summaries, 1).serialize());

    assertNotNull(parsed);
    assertTrue(parsed.setValueRowCounts().containsKey(1));
    assertTrue(parsed.setValueRowCounts().get(1).isEmpty());
    assertNull(parsed.setValueRowCount(1, "Drama"));
    assertNull(parsed.setValueRowCount(0, "anything"));
  }

  @Test
  void emptySummaryRevivesAndHistoricalRevisionsStayExactAfterColdReopen() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final Map<Integer, Map<String, Long>> empty = new LinkedHashMap<>();
        empty.put(1, new LinkedHashMap<>());
        final Map<Integer, Map<String, Long>> capabilities = initializeSummaries(storage, KINDS, empty);
        assertEquals(0, storage.getBlob(ProjectionSetSummaryChunks.slotKey(1))[4]);
        storage.putBlob(0, metadata(capabilities, 1).serialize());
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionIndexMetadata prior = ProjectionIndexMetadata.parse(storage.getBlob(0));
        final ProjectionSetSummaryChunks.Accessor accessor =
            ProjectionSetSummaryChunks.open(storage, prior.setValueRowCounts());
        accessor.adjust(1, Map.of("Drama", 3L), 1L);
        final Map<Integer, Map<String, Long>> capabilities = accessor.flush(KINDS);
        storage.putBlob(0, metadata(capabilities, 2).serialize());
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
        final Map<Integer, Map<String, Long>> revision1 = readSummaries(r1);
        final Map<Integer, Map<String, Long>> revision2 = readSummaries(r2);
        assertTrue(revision1.get(1).isEmpty());
        assertEquals(0L, revision1.get(1).getOrDefault("Drama", 0L));
        assertEquals(3L, revision2.get(1).get("Drama"));
      }
    }
  }

  @Test
  void oversizedSummaryDeclinesWithoutPublishingCapability() {
    final Map<String, Long> many = new LinkedHashMap<>();
    for (int i = 0; i < 5_000; i++) {
      many.put("value-" + i, (long) i);
    }
    final Map<Integer, Map<String, Long>> summaries = new LinkedHashMap<>();
    summaries.put(1, many);
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      assertTrue(initializeSummaries(storage, KINDS, summaries).isEmpty());
      assertNull(ProjectionSetSummaryChunks.readAll(storage, Map.of()).get(1));
    }
  }

  @Test
  void streamingBuildDropsHighCardinalityStateButKeepsExactLowCardinalitySummaryAfterColdReopen() {
    final ProjectionSetSummaryChunks.BuildAccumulator summaries = new ProjectionSetSummaryChunks.BuildAccumulator();
    final int pages = 8;
    final Map<Integer, Map<String, Long>> capabilities;
    try {
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
          JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          for (int page = 0; page < pages; page++) {
            summaries.append(highAndLowCardinalityPage(page));
            assertTrue(summaries.peakRetainedValueCount(1) <= ProjectionSetSummaryChunks.maxValuesForTesting(),
                "a build may never retain more distinct values than one summary chunk can publish");
            if (page == 0) {
              assertTrue(summaries.disabled(1),
                  "the first high-cardinality leaf must fail the optional summary closed");
            }
            assertEquals(0, summaries.retainedValueCount(1),
                "disabled columns must release their retained strings immediately");
          }
          assertFalse(summaries.disabled(2));
          assertEquals(2, summaries.retainedValueCount(2));
          capabilities = summaries.writeAll(storage, MULTI_SET_KINDS);
          assertFalse(capabilities.containsKey(1));
          assertTrue(capabilities.containsKey(2));
          assertNull(storage.getBlob(ProjectionSetSummaryChunks.slotKey(1)));
          wtx.commit();
        }

        Databases.getGlobalBufferManager().clearAllCaches();
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final Map<Integer, Map<String, Long>> reopened =
              ProjectionSetSummaryChunks.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER, capabilities);
          assertEquals(pages * ProjectionIndexRowGroupPage.MAX_ROWS / 2L, reopened.get(2).get("Drama"));
          assertEquals(pages * ProjectionIndexRowGroupPage.MAX_ROWS / 2L, reopened.get(2).get("Comedy"));
          assertFalse(reopened.containsKey(1));
        }
      }
    } finally {
      summaries.release();
    }
  }

  @Test
  void maintenanceReadsAndWritesOnlyTheChangedSummaryChunk() {
    final Map<Integer, Map<String, Long>> capabilities = new LinkedHashMap<>();
    capabilities.put(1, new LinkedHashMap<>());
    capabilities.put(2, new LinkedHashMap<>());
    byte[] untouchedBefore;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final Map<Integer, Map<String, Long>> initial = new LinkedHashMap<>();
        initial.put(1, new LinkedHashMap<>(Map.of("Drama", 2L)));
        initial.put(2, new LinkedHashMap<>(Map.of("Comedy", 4L)));
        assertEquals(capabilities.keySet(), initializeSummaries(storage, MULTI_SET_KINDS, initial).keySet());
        untouchedBefore = storage.getBlob(ProjectionSetSummaryChunks.slotKey(2));
        assertNotNull(untouchedBefore);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        final ProjectionSetSummaryChunks.Accessor accessor = ProjectionSetSummaryChunks.open(storage, capabilities);
        accessor.adjust(1, Map.of("Drama", 1L), 1L);
        assertEquals(1, accessor.chunksRead());
        assertTrue(accessor.bytesRead() > 0);
        assertEquals(capabilities.keySet(), accessor.flush(MULTI_SET_KINDS).keySet());
        assertEquals(1, accessor.chunksWritten());
        assertTrue(accessor.bytesWritten() > 0);
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
        JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2)) {
      assertArrayEquals(untouchedBefore, ProjectionIndexHOTStorage.readBlob(r1.getStorageEngineReader(), INDEX_NUMBER,
          ProjectionSetSummaryChunks.slotKey(2)));
      assertArrayEquals(untouchedBefore, ProjectionIndexHOTStorage.readBlob(r2.getStorageEngineReader(), INDEX_NUMBER,
          ProjectionSetSummaryChunks.slotKey(2)));
      final Map<Integer, Map<String, Long>> revision1 =
          ProjectionSetSummaryChunks.readAll(r1.getStorageEngineReader(), INDEX_NUMBER, capabilities);
      final Map<Integer, Map<String, Long>> revision2 =
          ProjectionSetSummaryChunks.readAll(r2.getStorageEngineReader(), INDEX_NUMBER, capabilities);
      assertEquals(2L, revision1.get(1).get("Drama"));
      assertEquals(3L, revision2.get(1).get("Drama"));
      assertEquals(4L, revision2.get(2).get("Comedy"));
    }
  }

  private static Map<Integer, Map<String, Long>> initializeSummaries(final ProjectionIndexHOTStorage storage,
      final byte[] columnKinds, final Map<Integer, Map<String, Long>> summaries) {
    final ProjectionSetSummaryChunks.BuildAccumulator initializer = new ProjectionSetSummaryChunks.BuildAccumulator();
    try {
      initializer.append(new ProjectionIndexRowGroupPage(columnKinds.clone()));
      final Map<Integer, Map<String, Long>> capabilities = initializer.writeAll(storage, columnKinds);
      final ProjectionSetSummaryChunks.Accessor accessor = ProjectionSetSummaryChunks.open(storage, capabilities);
      for (final Map.Entry<Integer, Map<String, Long>> summary : summaries.entrySet()) {
        accessor.adjust(summary.getKey(), summary.getValue(), 1L);
      }
      return accessor.flush(columnKinds);
    } finally {
      initializer.release();
    }
  }

  private static ProjectionIndexMetadata metadata(final Map<Integer, Map<String, Long>> summaries, final int revision) {
    return new ProjectionIndexMetadata("/[]", new String[] {"/[]/year", "/[]/genres/[]"},
        new String[] {"year", "genres"}, KINDS, 0, revision, summaries);
  }

  private static Map<String, Long> sampleGenres() {
    final Map<String, Long> genres = new LinkedHashMap<>();
    genres.put("Drama", 1_349_952L);
    genres.put("Comedy", 1_007_808L);
    genres.put("Silent", 684_576L);
    return genres;
  }

  private static ProjectionIndexRowGroupPage highAndLowCardinalityPage(final int pageNumber) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(MULTI_SET_KINDS);
    final long[] longs = new long[MULTI_SET_KINDS.length];
    final boolean[] booleans = new boolean[MULTI_SET_KINDS.length];
    final String[] strings = new String[MULTI_SET_KINDS.length];
    final String[][] sets = new String[MULTI_SET_KINDS.length][];
    for (int row = 0; row < ProjectionIndexRowGroupPage.MAX_ROWS; row++) {
      sets[1] = new String[] {"unique-" + pageNumber + '-' + row};
      sets[2] = new String[] {(row & 1) == 0
          ? "Drama"
          : "Comedy"};
      final long recordKey = (long) pageNumber * ProjectionIndexRowGroupPage.MAX_ROWS + row + 1;
      assertTrue(page.appendRow(recordKey, longs, booleans, strings, sets, null, null, null, null));
    }
    return page;
  }

  private static Map<Integer, Map<String, Long>> readSummaries(final JsonNodeReadOnlyTrx rtx) {
    final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(
        ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0));
    return ProjectionSetSummaryChunks.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER, metadata.setValueRowCounts());
  }
}
