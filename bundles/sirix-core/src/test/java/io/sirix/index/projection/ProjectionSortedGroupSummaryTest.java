/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionSortedGroupSummaryTest {
  private static final String PROPERTY = "sirix.projection.sortedGroupSummaries";

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void parallelSummaryWindowsPreserveSplitGroupsAndHistoricalRevisions(final VersioningType versioning) {
    final Path path = directory.resolve("parallel-summary-windows");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          for (int row = 0; row < 2062; row += 2) {
            builder.append(leaf(new byte[][] {parallelKey(row), parallelKey(row + 1)}));
          }
          builder.finish();
          final StorageEngineReader reader = writer.getStorageEngineWriter();
          final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
          assertEquals(ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted, 3, Order.SPAN_DESC, 1, false),
              ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted, 3, Order.SPAN_DESC, 1, false, ignored -> {
                throw new AssertionError("uncommitted reads must not open committed workers");
              }, 4));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(parallelKey(0));
          editor.insert(key("g10001", -900_000_000, 9000), new byte[0]);
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        final int fixedRevision = revision;
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
          final AtomicInteger opened = new AtomicInteger();
          final AtomicInteger closed = new AtomicInteger();
          for (final Order order : Order.values()) {
            final List<Group> serial =
                ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted, 3, order, 1, false);
            assertNotNull(serial);
            assertEquals(serial,
                ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted, 3, order, 1, false, worker -> {
                  try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(fixedRevision)) {
                    opened.incrementAndGet();
                    worker.accept(lane.getStorageEngineReader());
                  } finally {
                    closed.incrementAndGet();
                  }
                }, 4));
            assertRoutesMatch(reader, order, false);
          }
          assertTrue(opened.get() > 4, "the data crosses a bounded read window");
          assertEquals(opened.get(), closed.get());
          final AtomicInteger failedOpened = new AtomicInteger();
          final AtomicInteger failedClosed = new AtomicInteger();
          assertThrows(IllegalStateException.class, () -> ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted,
              3, Order.SPAN_DESC, 1, false, worker -> {
                try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(fixedRevision)) {
                  if (failedOpened.incrementAndGet() == 2) {
                    throw new IllegalStateException("injected worker failure");
                  }
                  worker.accept(lane.getStorageEngineReader());
                } finally {
                  failedClosed.incrementAndGet();
                }
              }, 4));
          assertTrue(failedOpened.get() >= 2);
          assertEquals(failedOpened.get(), failedClosed.get(), "join and close every started reader before failing");
          if (revision == 1) {
            assertThrows(IllegalArgumentException.class, () -> ProjectionSortedGroupScan.topKFromSummaries(reader, 0,
                sorted, 3, Order.SPAN_DESC, 1, false, worker -> {
                  try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(2)) {
                    worker.accept(lane.getStorageEngineReader());
                  }
                }, 4));
          }
        }
      }
    }
  }

  private static byte[] parallelKey(final int row) {
    final int group = row / 7;
    return key("g" + (10000 + group), (group - 200L) * 1_000_000 + (row % 7) * (group + 1L), row);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void initialBoundsChunksAreAppendOnlyDuringBulkStaging(final VersioningType versioning) {
    for (final boolean accumulate : new boolean[] {false, true}) {
      final Path path = directory.resolve("staged-bounds-" + accumulate);
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                                .versioningApproach(versioning)
                                                                .maxNumberOfRevisionsToRestore(2)
                                                                .build()));
        try (JsonResourceSession session = database.beginResourceSession("resource")) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
            if (accumulate) {
              storage.beginBulkSlotAccumulation();
            }
            final ProjectionSortedDirectory.Builder builder =
                new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
            for (int id = 1; id <= 133; id++) {
              builder.append(leaf(new byte[][] {key("g" + (1000 + id), id, id)}));
            }
            builder.finish();
            if (accumulate) {
              storage.finalizeBulkSlotAccumulation();
            }
            writer.commit();
          }
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionSortedDirectory.Editor editor =
                new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
            editor.remove(key("g1133", 133, 133));
            editor.insert(key("g1133", -1, 134), new byte[0]);
            writer.commit();
          }
        }
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 2; revision++) {
          try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
            final StorageEngineReader reader = trx.getStorageEngineReader();
            final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
            assertNotNull(sorted);
            final ProjectionSortedLeafBounds.Candidates bounds = ProjectionSortedLeafBounds.read(reader, 0, sorted);
            assertNotNull(bounds, "full and partial chunks must both be published");
            assertEquals(sorted.dataLeafCount(), bounds.leafIds().length);
            assertRoutesMatch(reader, Order.MIN_ASC, true);
            final List<Group> minimum = ProjectionSortedGroupScan.topK(reader, 0, 1, Order.MIN_ASC, 1, true);
            assertNotNull(minimum);
            assertEquals(revision == 1
                ? 1
                : -1, minimum.getFirst().min());
          }
        }
      }
    }
  }

  @Test
  void codecKeepsExactGroupsAndSignedExtremaAndDeclinesOtherShapes() {
    final byte[][] keys = {key(null, Long.MIN_VALUE, 1), key(null, Long.MAX_VALUE, 2), key("", -10, 3), key("", -10, 4),
        key("a\0ß", -5, 5), key("a\0ß", 8, 6)};
    final ProjectionSortedLeaf summary =
        ProjectionSortedGroupSummary.encode(leaf(keys), SortedScanFixtures.GROUP_VALUE);
    assertNotNull(summary);
    final ProjectionSortedLeaf reopened = ProjectionSortedLeaf.open(summary.encodedBytes());
    assertEquals(3, reopened.rowCount());
    final byte[] payload = new byte[16];
    for (int row = 0; row < 3; row++) {
      final byte[] source = keys[row * 2];
      final int prefix = SortedScanFixtures.GROUP_VALUE.lastFieldOffset(source, source.length);
      assertArrayEquals(Arrays.copyOf(source, prefix), reopened.copyKey(row));
      reopened.copyPayloadTo(row, payload, 0);
      assertEquals(ProjectionSortedGroupScan.readOrderedLong(source, prefix + 1),
          ProjectionIndexRowGroupCodec.getLongLE(payload, 0));
      assertEquals(ProjectionSortedGroupScan.readOrderedLong(keys[row * 2 + 1], prefix + 1),
          ProjectionIndexRowGroupCodec.getLongLE(payload, 8));
    }
    assertNull(ProjectionSortedGroupSummary.encode(leaf(new byte[][] {{1, 2, 3}}), SortedScanFixtures.GROUP_VALUE));
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendMissing();
    writer.appendMissing();
    writer.appendRecordKey(1);
    assertNull(
        ProjectionSortedGroupSummary.encode(leaf(new byte[][] {writer.copyKey()}), SortedScanFixtures.GROUP_VALUE));
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void overflowSummariesSurviveSplitsAndExtremaEditsAcrossAsyncFlushes(final VersioningType versioning) {
    final Path path = directory.resolve("async-summaries");
    final byte[][] keys = new byte[ProjectionSortedLeaf.MAX_ROWS][];
    for (int row = 0; row < keys.length; row++) {
      final int group = row / 2;
      keys[row] = key("g" + (1000 + group), group * 10_000L + row % 2 * (group + 1), row);
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          builder.append(leaf(keys));
          builder.finish();
          trx.commit();
        }
        try (JsonNodeTrx trx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
          editor.insert(key("g1000", -1, 1000), new byte[0]);
          trx.getStorageEngineWriter().asyncFlush();
          trx.getStorageEngineWriter().awaitPendingAsyncFlush();
          assertRoutesMatch(trx.getStorageEngineWriter(), Order.MIN_ASC, false);
          assertRoutesMatch(trx.getStorageEngineWriter(), Order.MIN_ASC, true);

          final ProjectionSortedDirectory.Editor afterFlush =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(trx.getStorageEngineWriter(), 0));
          afterFlush.remove(keys[keys.length - 1]);
          trx.getStorageEngineWriter().asyncFlush();
          trx.getStorageEngineWriter().awaitPendingAsyncFlush();
          assertRoutesMatch(trx.getStorageEngineWriter(), Order.MAX_DESC, false);
          assertRoutesMatch(trx.getStorageEngineWriter(), Order.MIN_ASC, true);
          trx.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          for (final Order order : Order.values()) {
            assertRoutesMatch(reader, order, false);
          }
          assertRoutesMatch(reader, Order.MIN_ASC, true);
          final List<Group> minimum = ProjectionSortedGroupScan.topK(reader, 0, 1, Order.MIN_ASC, 1);
          final List<Group> maximum = ProjectionSortedGroupScan.topK(reader, 0, 1, Order.MAX_DESC, 1);
          assertNotNull(minimum);
          assertNotNull(maximum);
          assertEquals(revision == 1
              ? 0
              : -1, minimum.getFirst().min());
          assertEquals(revision == 1
              ? 1_270_128
              : 1_270_000, maximum.getFirst().max());
          assertTrue(ProjectionIndexHOTStorage.collectBlobLocators(reader, 0, ProjectionSortedGroupSummary.slot(1), 1)
                                              .offset(0) != Constants.NULL_ID_LONG);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void crossLeafGroupsSurviveExtremaWritesDeletionRollbackAndColdHistoricalReads(final VersioningType versioning) {
    final Path path = directory.resolve("history");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          for (int row = 0; row < 600; row += 2) {
            builder.append(leaf(new byte[][] {rowKey(row), rowKey(row + 1)}));
          }
          builder.finish();
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(rowKey(0));
          for (int row = 595; row < 600; row++) {
            editor.remove(rowKey(row));
          }
          editor.insert(key("", -25, 9000), new byte[0]);
          editor.insert(key("g060", 9_999_999, 9001), new byte[0]);
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(key("g060", 9_999_999, 9001));
          writer.rollback();
        }
      }
    }
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          for (final Order order : Order.values()) {
            assertRoutesMatch(reader, order, false);
          }
          assertRoutesMatch(reader, Order.MIN_ASC, true);
          final List<Group> minimum = ProjectionSortedGroupScan.topK(reader, 0, 1, Order.MIN_ASC, 1);
          assertNotNull(minimum);
          assertEquals(revision == 1
              ? null
              : "", minimum.getFirst().key());
          assertEquals(revision == 1
              ? 0
              : -25, minimum.getFirst().min());
          final List<Group> maximum = ProjectionSortedGroupScan.topK(reader, 0, 1, Order.MAX_DESC, 1);
          assertNotNull(maximum);
          assertEquals(revision == 1
              ? "g119"
              : "g060", maximum.getFirst().key());
        }
      }
    }
  }

  @Test
  void backfillIsRevisionedAndMissingSummariesFallBack() {
    final Path path = directory.resolve("backfill");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Builder builder =
              new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
          builder.append(leaf(new byte[][] {key("a", 1, 1), key("a", 10, 2)}));
          builder.append(leaf(new byte[][] {key("b", 2, 3), key("b", 30, 4)}));
          builder.finish();
          storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(1));
          storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(2));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(2, ProjectionSortedGroupScan.buildLeafSummaries(writer.getStorageEngineWriter(), 0));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).tombstoneBlob(
              ProjectionSortedGroupSummary.slot(2));
          writer.commit();
        }
        for (int revision = 1; revision <= 3; revision++) {
          try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
            final StorageEngineReader reader = trx.getStorageEngineReader();
            final ProjectionSortedDirectory.Accessor accessor = ProjectionSortedDirectory.open(reader, 0);
            assertNotNull(accessor);
            final List<Group> summarized =
                ProjectionSortedGroupScan.topKFromSummaries(reader, 0, accessor, 1, Order.SPAN_DESC, 1, false);
            if (revision == 2) {
              assertNotNull(summarized);
            } else {
              assertNull(summarized);
            }
            assertEquals(List.of(new Group("b", 2, 30)),
                ProjectionSortedGroupScan.topK(reader, 0, 1, Order.SPAN_DESC, 1));
          }
        }
      }
    }
  }

  @Test
  void malformedExtremaAreRejectedEvenInAHashValidBlob() {
    final Path path = directory.resolve("corrupt");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        final ProjectionSortedDirectory.Builder builder =
            new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
        builder.append(leaf(new byte[][] {key("a", 1, 1)}));
        builder.finish();
        final byte[] invalid = new byte[16];
        ProjectionIndexRowGroupCodec.putLongLEAt(invalid, 0, 5);
        ProjectionIndexRowGroupCodec.putLongLEAt(invalid, 8, -5);
        final ProjectionSortedLeaf malformed =
            ProjectionSortedLeaf.encode(new byte[][] {{1, 'a', 0, 0}}, new byte[][] {invalid}, 1);
        assertNotNull(malformed);
        storage.putBlob(ProjectionSortedGroupSummary.slot(1), malformed.encodedBytes());
        assertThrows(IllegalStateException.class,
            () -> ProjectionSortedGroupScan.topK(writer.getStorageEngineWriter(), 0, 1, Order.MIN_ASC, 1));
      }
    }
  }

  @Test
  void splittingThenDeletingAnEntireGroupKeepsEachRevisionExact() {
    final Path path = directory.resolve("split");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          final byte[][] keys = new byte[ProjectionSortedLeaf.MAX_ROWS][];
          for (int row = 0; row < keys.length; row++) {
            keys[row] = key("a", row, row);
          }
          builder.append(leaf(keys));
          builder.finish();
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.insert(key("a", -1, 1000), new byte[0]);
          editor.remove(key("a", 255, 255));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(key("a", -1, 1000));
          for (int row = 0; row < 255; row++) {
            editor.remove(key("a", row, row));
          }
          writer.commit();
        }
        for (int revision = 1; revision <= 3; revision++) {
          try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
            final ProjectionSortedDirectory.Accessor accessor =
                ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0);
            assertNotNull(accessor);
            assertEquals(revision == 3
                ? 0
                : revision, accessor.dataLeafCount());
            assertEquals(revision == 3
                ? List.of()
                : List.of(new Group("a", revision == 1
                    ? 0
                    : -1,
                    revision == 1
                        ? 255
                        : 254)),
                ProjectionSortedGroupScan.topKFromSummaries(trx.getStorageEngineReader(), 0, accessor, 1,
                    Order.SPAN_DESC, 1, false));
          }
        }
      }
    }
  }

  @Test
  void tiesWithinAndAcrossTheLimitStillDecline() {
    final Path path = directory.resolve("ties");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
        builder.append(leaf(new byte[][] {key("a", -5, 1), key("a", 4, 2)}));
        builder.append(leaf(new byte[][] {key("b", 0, 3), key("b", 8, 4)}));
        builder.finish();
        // Both spans are two after signed division of each endpoint by three. Dividing
        // the difference instead would incorrectly distinguish these groups.
        final ProjectionSortedDirectory.Accessor accessor =
            ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
        assertNotNull(accessor);
        for (final int limit : new int[] {1, 2}) {
          assertNull(ProjectionSortedGroupScan.topKFromSummaries(writer.getStorageEngineWriter(), 0, accessor, limit,
              Order.SPAN_DESC, 3, false));
          assertNull(ProjectionSortedGroupScan.topK(writer.getStorageEngineWriter(), 0, limit, Order.SPAN_DESC, 3));
        }
      }
    }
  }

  private static void assertRoutesMatch(final StorageEngineReader reader, final Order order, final boolean minOnly) {
    final ProjectionSortedDirectory.Accessor accessor = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(accessor);
    final List<Group> summarized =
        ProjectionSortedGroupScan.topKFromSummaries(reader, 0, accessor, 10, order, 1, minOnly);
    assertNotNull(summarized);
    if (minOnly) {
      assertEquals(summarized, ProjectionSortedGroupScan.topKFromBounds(reader, 0, accessor, 10));
    }
    final String prior = System.getProperty(PROPERTY);
    System.setProperty(PROPERTY, "false");
    try {
      assertEquals(ProjectionSortedGroupScan.topK(reader, 0, 10, order, 1, minOnly), summarized);
    } finally {
      if (prior == null) {
        System.clearProperty(PROPERTY);
      } else {
        System.setProperty(PROPERTY, prior);
      }
    }
  }

  @Test
  void boundsMergeRepeatedGroupsAcrossOutOfOrderCandidateLeaves() {
    final Path path = directory.resolve("bounds-random");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final Random random = new Random(426_123L);
        final byte[][] keys = new byte[480][];
        for (int group = 0; group < 120; group++) {
          final long minimum = random.nextInt(2000) - 1000L;
          for (int row = 0; row < 4; row++) {
            keys[group * 4 + row] = key("group" + group, minimum + row * 1000L, group * 4 + row);
          }
        }
        Arrays.sort(keys, Arrays::compareUnsigned);
        final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
        for (int from = 0; from < keys.length; from += 3) {
          builder.append(leaf(Arrays.copyOfRange(keys, from, from + 3)));
        }
        builder.finish();
        final ProjectionSortedDirectory.Accessor accessor =
            ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
        assertNotNull(accessor);
        assertEquals(160, accessor.dataLeafCount(), "the fixture must cross bound-chunk boundaries");
        for (final int limit : new int[] {1, 2, 3, 10, 32}) {
          assertEquals(
              ProjectionSortedGroupScan.topKFromSummaries(writer.getStorageEngineWriter(), 0, accessor, limit,
                  Order.MIN_ASC, 1, true),
              ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, limit));
        }
      }
    }
  }

  @Test
  void boundsSkipUncompetitiveSummariesButMissingBoundsKeepTheExactFallback() {
    final Path path = directory.resolve("bounds-skip");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        final ProjectionSortedDirectory.Builder builder =
            new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
        for (int row = 0; row < 3; row++) {
          builder.append(leaf(new byte[][] {key("g" + row, row * 10L, row)}));
        }
        builder.finish();
        final ProjectionSortedDirectory.Accessor accessor =
            ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
        assertNotNull(accessor);
        storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(3));
        final List<Group> expected = List.of(new Group("g0", 0, 0));
        assertEquals(expected,
            ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, 1),
            "the third summary must not be read after the cutline is proven");
        ProjectionSortedLeafBounds.invalidate(storage, 3);
        assertNull(ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, 1));
        assertEquals(expected,
            ProjectionSortedGroupScan.topK(writer.getStorageEngineWriter(), 0, 1, Order.MIN_ASC, 1, true),
            "an incomplete bounds index must fall back to source keys");
        assertEquals(3, ProjectionSortedGroupScan.buildLeafSummaries(writer.getStorageEngineWriter(), 0));
        assertEquals(expected,
            ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, 1));
        final byte[] malformed = storage.getBlob(ProjectionSortedLeafBounds.slot(3)).clone();
        // Corrupt an uncompetitive leaf's bound under a valid blob hash. Every bound is validated
        // before pruning, including bounds whose corresponding summary will not be visited.
        ProjectionIndexRowGroupCodec.putLongLEAt(malformed, 16 + 2 * 16, 9999);
        storage.putBlob(ProjectionSortedLeafBounds.slot(3), malformed);
        assertThrows(IllegalStateException.class,
            () -> ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, 1));
      }
    }
  }

  @Test
  void boundsRetainDistinctGroupsAndDeclineMinimumTies() {
    final Path path = directory.resolve("bounds-ties");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
        builder.append(leaf(new byte[][] {key("a", 7, 1)}));
        builder.append(leaf(new byte[][] {key("a", 8, 2), key("b", 7, 3)}));
        builder.append(leaf(new byte[][] {key("c", 9, 4)}));
        builder.finish();
        final ProjectionSortedDirectory.Accessor accessor =
            ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
        assertNotNull(accessor);
        for (final int limit : new int[] {1, 2, 3}) {
          assertNull(ProjectionSortedGroupScan.topKFromBounds(writer.getStorageEngineWriter(), 0, accessor, limit));
        }
      }
    }
  }

  private static ProjectionSortedLeaf leaf(final byte[][] keys) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, null, keys.length);
    assertNotNull(leaf);
    return leaf;
  }

  private static byte[] rowKey(final int row) {
    final int group = row / 5;
    final String name = group == 0
        ? null
        : group == 1
            ? ""
            : group == 2
                ? "a\0ß"
                : "g" + (group < 10
                    ? "00"
                    : group < 100
                        ? "0"
                        : "")
                    + group;
    return key(name, group * 10_000L + row % 5 * (group + 1L), row);
  }

  private static byte[] key(final String group, final long value, final long record) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    if (group == null) {
      writer.appendMissing();
    } else {
      final byte[] utf8 = group.getBytes(StandardCharsets.UTF_8);
      writer.appendUtf8(utf8, 0, utf8.length);
    }
    writer.appendLong(value);
    writer.appendRecordKey(record);
    return writer.copyKey();
  }
}
