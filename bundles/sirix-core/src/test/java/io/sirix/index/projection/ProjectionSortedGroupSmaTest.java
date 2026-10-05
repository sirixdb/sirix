/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Regression: group reads must not enumerate an increasing number of physical chunks. */
@Isolated
final class ProjectionSortedGroupSmaTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void initialBuildRetainsOnlyItsOwnSmaPathWithoutReadingItBack(final VersioningType versioning) {
    final Path path = temporary.resolve("build-path-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int group = 0; group < 256; group++) {
      for (int row = 0; row < 4; row++)
        rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L + row, rows.size() + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = spy(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
        final ProjectionSortedDirectory.Builder builder =
            new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
        final byte[][] keys = rows.stream().map(SortedScanFixtures::key).toArray(byte[][]::new);
        Arrays.sort(keys, Arrays::compareUnsigned);
        for (int from = 0; from < keys.length; from += 4)
          builder.append(ProjectionSortedLeaf.encode(keys, null, from, 4));
        builder.finish();
        verify(storage, never()).getBlob(longThat(slot -> slot > ProjectionSortedGroupSma.HEADER_SLOT
            && slot <= ProjectionSortedGroupSma.HEADER_SLOT + Integer.MAX_VALUE));
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertEquals(SortedScanFixtures.expectedMin(rows, 16),
            ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 16, Order.MIN_ASC, 1, true));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void interiorEditsPreserveProofWithoutRefreshingDataEndpoints(final VersioningType versioning) {
    final Path path = temporary.resolve("interior-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int row = 0; row < 64; row++)
      rows.add(new SortedScanFixtures.Row("group", row, row + 1));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {16});
        trx.commit();
      }
      final int[] endpointReads = {0};
      ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(ignored -> endpointReads[0]++);
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = spy(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
        final ProjectionSortedDirectory.Editor editor = new ProjectionSortedDirectory.Editor(storage);
        final SortedScanFixtures.Row removed = rows.remove(20);
        editor.remove(SortedScanFixtures.key(removed));
        final SortedScanFixtures.Row inserted = new SortedScanFixtures.Row("group", 20, 1000);
        rows.add(inserted);
        editor.insert(SortedScanFixtures.key(inserted), new byte[0]);
        assertEquals(0, endpointReads[0], "strictly interior edits retain validated group extrema");
        verify(storage, never()).getBlob(longThat(slot -> slot > ProjectionSortedGroupSma.HEADER_SLOT
            && slot <= ProjectionSortedGroupSma.HEADER_SLOT + Integer.MAX_VALUE));
        verify(storage, never()).writeSlotValue(eq(ProjectionSortedGroupSma.HEADER_SLOT), any(byte[].class));
        trx.commit();
      } finally {
        ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
      }
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        final SortedScanFixtures.Row endpoint = rows.removeFirst();
        new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).remove(
            SortedScanFixtures.key(endpoint));
        trx.commit();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertNotNull(
            ProjectionIndexHOTStorage.readBlob(trx.getStorageEngineReader(), 0, ProjectionSortedGroupSma.HEADER_SLOT));
        assertEquals(SortedScanFixtures.expectedMin(rows, 3),
            ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 3, Order.MIN_ASC, 1, true));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void failedInteriorEditCannotCommitItsRetainedProof(final VersioningType versioning) {
    final Path path = temporary.resolve("interior-failure-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int row = 0; row < 64; row++) {
      rows.add(new SortedScanFixtures.Row("group", row, row + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {16});
        trx.commit();
      }
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        final ProjectionSortedDirectory.Editor editor =
            new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
        ProjectionSortedLeafStore.setWriteObserverForTesting(ignored -> {
          throw new IllegalStateException("failure after summary replacement");
        });
        try {
          assertThrows(IllegalStateException.class, () -> editor.remove(SortedScanFixtures.key(rows.get(20))));
          assertThrows(RuntimeException.class, trx::commit, "an incomplete edit must poison the transaction");
        } finally {
          ProjectionSortedLeafStore.setWriteObserverForTesting(null);
        }
        trx.rollback();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(1)) {
        assertEquals(SortedScanFixtures.expectedMin(rows, 3),
            ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 3, Order.MIN_ASC, 1, true));
        final byte[] expectedKey = SortedScanFixtures.key(rows.get(20));
        final ProjectionSortedDirectory.Accessor.Cursor cursor =
            ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0).seek(expectedKey);
        assertTrue(cursor.isValid());
        final byte[] actualKey = new byte[cursor.keyLength()];
        cursor.copyKeyTo(actualKey);
        assertArrayEquals(expectedKey, actualKey);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void groupMetadataWorkIsIndependentOfPhysicalChunkCount(final VersioningType versioning) {
    int firstReads = 0;
    for (final int chunks : new int[] {1, 4, 16}) {
      final Path path = temporary.resolve(versioning + "-" + chunks);
      final List<SortedScanFixtures.Row> rows = new ArrayList<>();
      for (int group = 0; group < 64; group++) {
        for (int row = 0; row < chunks * 8; row++)
          rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1_000_000L + row * (group + 1L),
              rows.size() + 1));
      }
      create(path, versioning);
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {8});
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final int[] metadata = {0};
        final int[] data = {0};
        final int queryDataReads;
        ProjectionSortedGroupSummary.setReadObserverForTesting(ignored -> metadata[0]++);
        ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(ignored -> data[0]++);
        try {
          assertEquals(SortedScanFixtures.expectedSpan(rows, 5, 3),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 5, Order.SPAN_DESC, 3));
          queryDataReads = data[0];
          assertEquals(0, queryDataReads, "group SMAs prune every data chunk");
          final ProjectionSortedDirectory.Accessor sorted =
              ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0);
          assertTrue(sorted.first().isValid());
        } finally {
          ProjectionSortedGroupSummary.setReadObserverForTesting(null);
          ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
        }
        assertTrue(metadata[0] > 0 && metadata[0] <= 10, "bounded, live metadata route including its header");
        if (chunks == 1)
          firstReads = metadata[0];
        else
          assertEquals(firstReads, metadata[0], "same groups must cost the same metadata work");
        assertEquals(SortedScanFixtures.expectedMin(rows, 5),
            ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 5, Order.MIN_ASC, 1, true));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void extremeEditsGroupSplitsDeletionInvalidationAndRollbackPreserveHistory(final VersioningType versioning) {
    final Path path = temporary.resolve("history-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    final List<List<SortedScanFixtures.Row>> revisions = new ArrayList<>();
    // Four rows per group in two-row chunks: 80 chunks for 40 groups keep the tree coarser than the
    // chunks through every edit below (the tree is declined once groups outnumber chunks).
    for (int group = 0; group < 40; group++) {
      for (int row = 0; row < 4; row++)
        rows.add(
            new SortedScanFixtures.Row("g" + (100 + group), group * 1000L + row * (group + 1L), group * 4 + row + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {2});
        trx.commit();
      }
      revisions.add(List.copyOf(rows));
      for (int operation = 0; operation < 12; operation++) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
          final SortedScanFixtures.Row removed = rows.removeFirst();
          editor.remove(SortedScanFixtures.key(removed));
          final SortedScanFixtures.Row inserted =
              new SortedScanFixtures.Row("z" + (100 + operation), operation * 999L, 1000 + operation);
          editor.insert(SortedScanFixtures.key(inserted), new byte[0]);
          rows.add(inserted);
          trx.commit();
        }
        revisions.add(List.copyOf(rows));
      }
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0).tombstoneBlob(
            ProjectionSortedGroupSummary.slot(1));
        trx.rollback();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertNotNull(
            ProjectionIndexHOTStorage.readBlob(trx.getStorageEngineReader(), 0, ProjectionSortedGroupSma.HEADER_SLOT));
      }
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0).tombstoneBlob(
            ProjectionSortedGroupSummary.slot(1));
        trx.commit();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertNull(
            ProjectionIndexHOTStorage.readBlob(trx.getStorageEngineReader(), 0, ProjectionSortedGroupSma.HEADER_SLOT));
        assertEquals(SortedScanFixtures.expectedSpan(rows, 5, 1),
            ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 5, Order.SPAN_DESC, 1));
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= revisions.size(); revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(SortedScanFixtures.expectedSpan(revisions.get(revision - 1), 5, 1),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 5, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void batchInsertsExtendGroupExtremaExactlyWithoutRescans(final VersioningType versioning) {
    final Path path = temporary.resolve("batch-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    final List<List<SortedScanFixtures.Row>> revisions = new ArrayList<>();
    // Four rows per group in two-row chunks: 80 chunks for 40 groups keep the tree coarser than the
    // chunks after the 35-row batch below (the tree is declined once groups outnumber chunks).
    for (int group = 0; group < 40; group++) {
      int row = 0;
      for (final long offset : new long[] {500, 550, 650, 700})
        rows.add(new SortedScanFixtures.Row("g" + (100 + group), group * 1000L + offset, group * 4 + ++row));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {2});
        trx.commit();
      }
      revisions.add(List.copyOf(rows));
      final int[] endpointReads = {0};
      ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(ignored -> endpointReads[0]++);
      try {
        // One batch: new minima for ten groups, new maxima for ten others, interior rows for ten,
        // and five brand-new groups. Insert-only, so every extremum is the exact combination of the
        // stored extrema and the inserted values: no data leaf may be read back.
        final List<SortedScanFixtures.Row> inserted = new ArrayList<>();
        for (int group = 0; group < 10; group++)
          inserted.add(new SortedScanFixtures.Row("g" + (100 + group), group * 1000L + 100, 1000 + group));
        for (int group = 10; group < 20; group++)
          inserted.add(new SortedScanFixtures.Row("g" + (100 + group), group * 1000L + 900, 1000 + group));
        for (int group = 20; group < 30; group++)
          inserted.add(new SortedScanFixtures.Row("g" + (100 + group), group * 1000L + 600, 1000 + group));
        for (int group = 0; group < 5; group++)
          inserted.add(new SortedScanFixtures.Row("n" + group, group * 77L, 2000 + group));
        final byte[][] keys =
            inserted.stream().map(SortedScanFixtures::key).sorted(Arrays::compareUnsigned).toArray(byte[][]::new);
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).apply(
              new byte[0][], 0, keys, null, keys.length);
          trx.commit();
        }
        rows.addAll(inserted);
        revisions.add(List.copyOf(rows));
        assertEquals(0, endpointReads[0], "insert-only batches combine extrema without reading data leaves");
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertEquals(SortedScanFixtures.expectedMin(rows, 12),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.MIN_ASC, 1, true));
          assertEquals(SortedScanFixtures.expectedSpan(rows, 12, 1),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.SPAN_DESC, 1));
        }
        // A batch that removes a current minimum must rescan that group; the others still combine.
        final SortedScanFixtures.Row removedMin = rows.stream()
                                                      .filter(row -> "g100".equals(row.group()))
                                                      .min((a, b) -> Long.compare(a.value(), b.value()))
                                                      .orElseThrow();
        final SortedScanFixtures.Row extra = new SortedScanFixtures.Row("g105", 105 * 1000L + 950, 3000);
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).apply(
              new byte[][] {SortedScanFixtures.key(removedMin)}, 1, new byte[][] {SortedScanFixtures.key(extra)}, null,
              1);
          trx.commit();
        }
        rows.remove(removedMin);
        rows.add(extra);
        revisions.add(List.copyOf(rows));
        assertTrue(endpointReads[0] > 0, "a removed extremum still triggers the exact rescan of its group");
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertEquals(SortedScanFixtures.expectedMin(rows, 12),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.MIN_ASC, 1, true));
          assertEquals(SortedScanFixtures.expectedSpan(rows, 12, 1),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.SPAN_DESC, 1));
        }
      } finally {
        ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= revisions.size(); revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(SortedScanFixtures.expectedMin(revisions.get(revision - 1), 12),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.MIN_ASC, 1, true));
          assertEquals(SortedScanFixtures.expectedSpan(revisions.get(revision - 1), 12, 1),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 12, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void nearSingletonGroupsDeclineTheTreeAtBuild(final VersioningType versioning) {
    final Path path = temporary.resolve("decline-build-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int group = 0; group < 200; group++) {
      rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L, group * 2 + 1));
      rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L + group + 1, group * 2 + 2));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        // 400 rows in four-row chunks: 100 chunks for 200 groups, so the group level is finer than
        // the chunk level and the tree is declined.
        SortedScanFixtures.build(trx, rows, new int[] {4});
        trx.commit();
      }
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = trx.getStorageEngineReader();
        assertEquals(-1L, ProjectionSortedGroupSma.publishedGroupCount(reader, 0),
            "200 groups over 100 chunks: no tree");
        for (int id = 1; id <= 64; id++) {
          assertNull(ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionSortedGroupSma.HEADER_SLOT + id),
              "no node blob survives a declined build");
        }
        assertEquals(SortedScanFixtures.expectedMin(rows, 5),
            ProjectionSortedGroupScan.topK(reader, 0, 5, Order.MIN_ASC, 1, true));
        assertEquals(SortedScanFixtures.expectedSpan(rows, 5, 1),
            ProjectionSortedGroupScan.topK(reader, 0, 5, Order.SPAN_DESC, 1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void groupGrowthPastTheChunkCountDropsTheTreeAndKeepsAnswersExact(final VersioningType versioning) {
    final Path path = temporary.resolve("decline-growth-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int group = 0; group < 16; group++) {
      for (int row = 0; row < 8; row++)
        rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L + row * (group + 1L), rows.size() + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {8}); // 16 chunks for 16 groups: kept
        trx.commit();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertEquals(16L, ProjectionSortedGroupSma.publishedGroupCount(trx.getStorageEngineReader(), 0));
      }
      final List<SortedScanFixtures.Row> revisionOne = List.copyOf(rows);
      // One batch of 100 singleton groups: 116 groups over a few dozen chunks, so the tree is dropped
      // whole and every grouped answer comes from the exact summary and bounds routes.
      final List<SortedScanFixtures.Row> inserted = new ArrayList<>();
      for (int i = 0; i < 100; i++)
        inserted.add(new SortedScanFixtures.Row(String.format("n%03d", i), 50_000L + i * 3L, 5000 + i));
      final byte[][] keys =
          inserted.stream().map(SortedScanFixtures::key).sorted(Arrays::compareUnsigned).toArray(byte[][]::new);
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).apply(
            new byte[0][], 0, keys, null, keys.length);
        trx.commit();
      }
      rows.addAll(inserted);
      Databases.clearGlobalCaches();
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = trx.getStorageEngineReader();
        assertEquals(-1L, ProjectionSortedGroupSma.publishedGroupCount(reader, 0), "116 groups outnumber the chunks");
        for (int id = 1; id <= 64; id++) {
          assertNull(ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionSortedGroupSma.HEADER_SLOT + id),
              "a dropped tree leaves no node blob behind");
        }
        assertEquals(SortedScanFixtures.expectedMin(rows, 12),
            ProjectionSortedGroupScan.topK(reader, 0, 12, Order.MIN_ASC, 1, true));
        assertEquals(SortedScanFixtures.expectedSpan(rows, 12, 1),
            ProjectionSortedGroupScan.topK(reader, 0, 12, Order.SPAN_DESC, 1));
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(1)) {
        final StorageEngineReader reader = trx.getStorageEngineReader();
        assertEquals(16L, ProjectionSortedGroupSma.publishedGroupCount(reader, 0), "revision 1 keeps its tree");
        assertEquals(SortedScanFixtures.expectedSpan(revisionOne, 12, 1),
            ProjectionSortedGroupScan.topK(reader, 0, 12, Order.SPAN_DESC, 1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void publishedGroupCountFollowsInsertsAndRemovals(final VersioningType versioning) {
    final Path path = temporary.resolve("count-" + versioning);
    create(path, versioning);
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int group = 0; group < 10; group++) {
      for (int row = 0; row < 4; row++)
        rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L + row * (group + 1L), rows.size() + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        SortedScanFixtures.build(trx, rows, new int[] {2}); // 20 chunks for 10 groups
        trx.commit();
      }
      final List<List<SortedScanFixtures.Row>> revisions = new ArrayList<>();
      revisions.add(List.copyOf(rows));
      // Remove every row of one group in one batch: the group leaves the tree.
      final List<SortedScanFixtures.Row> gone = rows.stream().filter(row -> "g1005".equals(row.group())).toList();
      final byte[][] removals =
          gone.stream().map(SortedScanFixtures::key).sorted(Arrays::compareUnsigned).toArray(byte[][]::new);
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).apply(
            removals, removals.length, new byte[0][], null, 0);
        trx.commit();
      }
      rows.removeAll(gone);
      revisions.add(List.copyOf(rows));
      // A brand-new group and one more row of an existing group: exactly one group joins.
      final List<SortedScanFixtures.Row> added =
          List.of(new SortedScanFixtures.Row("n0", 77L, 900), new SortedScanFixtures.Row("g1000", 5L, 901));
      final byte[][] insertions =
          added.stream().map(SortedScanFixtures::key).sorted(Arrays::compareUnsigned).toArray(byte[][]::new);
      try (JsonNodeTrx trx = session.beginNodeTrx()) {
        new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0)).apply(
            new byte[0][], 0, insertions, null, insertions.length);
        trx.commit();
      }
      rows.addAll(added);
      revisions.add(List.copyOf(rows));
      Databases.clearGlobalCaches();
      final long[] expectedCounts = {10, 9, 10};
      for (int revision = 1; revision <= revisions.size(); revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          assertEquals(expectedCounts[revision - 1], ProjectionSortedGroupSma.publishedGroupCount(reader, 0),
              "group count at revision " + revision);
          assertEquals(SortedScanFixtures.expectedMin(revisions.get(revision - 1), 6),
              ProjectionSortedGroupScan.topK(reader, 0, 6, Order.MIN_ASC, 1, true));
          assertEquals(SortedScanFixtures.expectedSpan(revisions.get(revision - 1), 6, 1),
              ProjectionSortedGroupScan.topK(reader, 0, 6, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  private static void create(final Path path, final VersioningType versioning) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
    }
  }
}
