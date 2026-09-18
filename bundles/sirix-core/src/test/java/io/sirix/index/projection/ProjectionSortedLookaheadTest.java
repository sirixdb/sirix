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
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.LookaheadStats;
import io.sirix.index.projection.SortedScanFixtures.Row;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lookahead windows of the Q4 bound walk and the Q5 best-first span scan must produce the
 * serial loop's results, tie outcomes and read-budget accounting exactly, for every window and
 * every versioning strategy, on deleted, inserted and reordered histories; their waste stays within
 * the documented bound.
 */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionSortedLookaheadTest {
  private static final int[] WINDOWS = {1, 2, 3, 8, 64};
  private static final int[] LIMITS = {1, 3, 17, 32};
  private static final long[] DIVISORS = {1, 7, 1000};

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyWindowMatchesTheSerialLoopOnReorderedDeletedAndInsertedHistory(final VersioningType versioning) {
    final Path path = directory.resolve("lookahead-history");
    final List<Row> original = SortedScanFixtures.history(78312);
    final List<Row> changed = new ArrayList<>(original);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, original, new int[] {1, 3, 127, 19, 256, 7});
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          for (final Row row : original) {
            if ("g1009".equals(row.group()) || "g1047".equals(row.group())) {
              editor.remove(SortedScanFixtures.key(row));
              changed.remove(row);
            }
          }
          for (int i = 0; i < 700; i++) {
            final Row row = new Row("a\0ß", -500_000_000L + i * 10_007L, original.size() + i + 1);
            editor.insert(SortedScanFixtures.key(row), new byte[0]);
            changed.add(row);
          }
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        final List<Row> rows = revision == 1
            ? original
            : changed;
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final ProjectionSortedDirectory.Accessor accessor = ProjectionSortedDirectory.open(reader, 0);
          assertNotNull(accessor);
          for (final int limit : LIMITS) {
            for (final long divisor : DIVISORS) {
              final List<Group> expected = SortedScanFixtures.expectedSpan(rows, limit, divisor);
              assertNotNull(expected, "fixture must exercise a successful accelerated result");
              final LookaheadStats serial = new LookaheadStats();
              assertEquals(expected, ProjectionSortedSpanScan.topK(reader, 0, accessor, limit, divisor, 1, serial));
              assertEquals(0, serial.fetched, "window 1 is the serial loop: nothing is fetched ahead");
              assertEquals(serial.charged, serial.charged);
              for (final int window : WINDOWS) {
                final LookaheadStats stats = new LookaheadStats();
                assertEquals(expected,
                    ProjectionSortedSpanScan.topK(reader, 0, accessor, limit, divisor, window, stats),
                    "span window " + window + " limit " + limit + " divisor " + divisor);
                assertEquals(serial.charged, stats.charged,
                    "the budget must see the serial loop's reads (window " + window + ")");
                assertTrue(stats.consumed <= stats.charged);
                assertTrue(stats.wasted() <= 2 * window - 1,
                    "waste " + stats.wasted() + " exceeds the bound for window " + window);
              }
            }
            final List<Group> expectedMin = SortedScanFixtures.expectedMin(rows, limit);
            final LookaheadStats serialMin = new LookaheadStats();
            final List<Group> serialBounds =
                ProjectionSortedGroupScan.topKFromBounds(reader, 0, accessor, limit, 1, serialMin);
            assertEquals(expectedMin, serialBounds, "the bound walk must match an independent fold");
            for (final int window : WINDOWS) {
              final LookaheadStats stats = new LookaheadStats();
              assertEquals(serialBounds,
                  ProjectionSortedGroupScan.topKFromBounds(reader, 0, accessor, limit, window, stats),
                  "bound window " + window + " limit " + limit);
              assertEquals(serialMin.charged, stats.charged, "bound walk reads (window " + window + ")");
              assertTrue(stats.wasted() <= window - 1, "waste " + stats.wasted() + " for window " + window);
            }
          }
        }
      }
    }
  }

  @Test
  void tiesBudgetsAndMissingSummariesBehaveIdenticallyForEveryWindow() {
    // Ties within and across the cut decline for every window.
    final Path ties = directory.resolve("lookahead-ties");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(ties)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(ties)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, List.of(new Row("a", -5, 1), new Row("a", 4, 2), new Row("b", 0, 3),
              new Row("b", 8, 4), new Row("c", -100, 5), new Row("c", 100, 6)), new int[] {1});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final ProjectionSortedDirectory.Accessor accessor =
              ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0);
          for (final int window : WINDOWS) {
            assertEquals(List.of(new Group("c", -100, 100)),
                ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 1, 3, window, null));
            assertNull(ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 2, 3, window, null));
            assertNull(ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 3, 3, window, null));
          }
          assertThrows(IllegalArgumentException.class,
              () -> ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 1, 3, 0, null));
          assertThrows(IllegalArgumentException.class,
              () -> ProjectionSortedGroupScan.topKFromBounds(trx.getStorageEngineReader(), 0, accessor, 1, 65, null));
        }
      }
    }
    // Unselective bounds exhaust the read budget at the same step for every window.
    final Path unselective = directory.resolve("lookahead-unselective");
    final List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 512; i++) {
      final long base = (i & 1) == 0
          ? -1_000_000_000_000L
          : 1_000_000_000_000L;
      rows.add(new Row("g" + (1000 + i), base, 2L * i + 1));
      rows.add(new Row("g" + (1000 + i), base + i + 1, 2L * i + 2));
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(unselective)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(unselective)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, rows, new int[] {4});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final ProjectionSortedDirectory.Accessor accessor =
              ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0);
          final LookaheadStats serial = new LookaheadStats();
          assertNull(ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 3, 1, 1, serial));
          assertTrue(serial.charged > 0);
          for (final int window : WINDOWS) {
            final LookaheadStats stats = new LookaheadStats();
            assertNull(ProjectionSortedSpanScan.topK(trx.getStorageEngineReader(), 0, accessor, 3, 1, window, stats));
            assertEquals(serial.charged, stats.charged, "budget exhaustion step for window " + window);
            assertTrue(stats.fetched <= serial.charged, "a batch must never fetch what the budget forbids");
          }
        }
      }
    }
    // A missing summary of a proven loser is never consumed; missing bounds decline before any scan.
    final Path pruning = directory.resolve("lookahead-pruning");
    final List<Row> pruned = List.of(new Row("a", 0, 1), new Row("a", 1000, 2), new Row("b", 1, 3),
        new Row("b", 901, 4), new Row("c", 2, 5), new Row("c", 802, 6), new Row("d", 400, 7), new Row("d", 401, 8),
        new Row("e", 400, 9), new Row("e", 401, 10));
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(pruning)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(pruning)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, pruned, new int[] {2});
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(5));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          ProjectionSortedLeafBounds.invalidate(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), 5);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx first = session.beginNodeReadOnlyTrx(1);
            JsonNodeReadOnlyTrx second = session.beginNodeReadOnlyTrx(2)) {
          final ProjectionSortedDirectory.Accessor firstAccessor =
              ProjectionSortedDirectory.open(first.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor secondAccessor =
              ProjectionSortedDirectory.open(second.getStorageEngineReader(), 0);
          for (final int window : WINDOWS) {
            assertEquals(List.of(new Group("a", 0, 1000)),
                ProjectionSortedSpanScan.topK(first.getStorageEngineReader(), 0, firstAccessor, 1, 1, window, null),
                "window " + window);
            assertNull(
                ProjectionSortedSpanScan.topK(second.getStorageEngineReader(), 0, secondAccessor, 1, 1, window, null));
            assertNull(ProjectionSortedGroupScan.topKFromBounds(second.getStorageEngineReader(), 0, secondAccessor, 1,
                window, null));
          }
        }
      }
    }
  }
}
