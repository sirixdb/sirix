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
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionSortedSpanScanTest {
  @TempDir
  Path directory;

  private record Row(String group, long value, long record) {
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void splitGroupsReorderedPhysicalLeavesAndHistoryMatchAnIndependentFold(final VersioningType versioning) {
    final Path path = directory.resolve("span-history");
    final List<Row> original = new ArrayList<>();
    final Random random = new Random(78312);
    for (int group = 0; group < 48; group++) {
      final String name = group == 0
          ? null
          : group == 1
              ? ""
              : group == 2
                  ? "a\0ß"
                  : "g" + (1000 + group);
      final int count = group == 9
          ? 1600
          : 3 + random.nextInt(75);
      for (int row = 0; row < count; row++) {
        original.add(
            new Row(name, -99_000_000L + group * 100_003L + (long) row * (group * 7717 + 1), original.size() + 1));
      }
    }
    final List<Row> changed = new ArrayList<>(original);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, original, new int[] {1, 3, 127, 19, 256, 7});
          assertNull(bounded(writer.getStorageEngineWriter(), 3, 7), "writer-local reads keep the serial route");
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          for (final Row row : original) {
            if ("g1009".equals(row.group()) || "g1047".equals(row.group())) {
              editor.remove(key(row));
              changed.remove(row);
            }
          }
          // Split early logical leaves after high physical IDs already exist, and change an extreme.
          for (int i = 0; i < 700; i++) {
            final Row row = new Row("a\0ß", -500_000_000L + i * 10_007L, original.size() + i + 1);
            editor.insert(key(row), new byte[0]);
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
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          for (final long divisor : new long[] {1, 7, 1000}) {
            for (final int limit : new int[] {1, 3, 17, 32}) {
              final List<Group> expected = expected(revision == 1
                  ? original
                  : changed, limit, divisor);
              assertNotNull(expected, "fixture must exercise a successful accelerated result");
              assertEquals(expected, bounded(trx.getStorageEngineReader(), limit, divisor));
              assertEquals(expected,
                  ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, limit, Order.SPAN_DESC, divisor));
            }
          }
        }
      }
    }
  }

  @Test
  void strictCutoffKeepsTiesAndEndpointDivisionSemantics() {
    final Path path = directory.resolve("ties");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, List.of(new Row("a", -5, 1), new Row("a", 4, 2), new Row("b", 0, 3), new Row("b", 8, 4),
              new Row("c", -100, 5), new Row("c", 100, 6)), new int[] {1});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertEquals(List.of(new Group("c", -100, 100)), bounded(trx.getStorageEngineReader(), 1, 3));
          assertNull(bounded(trx.getStorageEngineReader(), 2, 3), "a/b tie crosses the cutoff");
          assertNull(bounded(trx.getStorageEngineReader(), 3, 3), "a/b tie is inside the winners");
        }
      }
    }
  }

  @Test
  void pruningSkipsMissingLosingSummariesAndMissingBoundsFallBack() {
    final Path path = directory.resolve("pruning");
    final List<Row> rows = List.of(new Row("a", 0, 1), new Row("a", 1000, 2), new Row("b", 1, 3), new Row("b", 901, 4),
        new Row("c", 2, 5), new Row("c", 802, 6), new Row("d", 400, 7), new Row("d", 401, 8), new Row("e", 400, 9),
        new Row("e", 401, 10));
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, rows, new int[] {2});
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
          assertEquals(List.of(new Group("a", 0, 1000)), bounded(first.getStorageEngineReader(), 1, 1),
              "the missing summary of a proven loser must not be read");
          assertNull(bounded(second.getStorageEngineReader(), 1, 1));
          assertEquals(List.of(new Group("a", 0, 1000)),
              ProjectionSortedGroupScan.topK(second.getStorageEngineReader(), 0, 1, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  @Test
  void unselectiveBoundsReturnToTheExistingScanAfterBoundedWork() {
    final Path path = directory.resolve("unselective");
    final List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 512; i++) {
      final long base = (i & 1) == 0
          ? -1_000_000_000_000L
          : 1_000_000_000_000L;
      rows.add(new Row("g" + (1000 + i), base, 2L * i + 1));
      rows.add(new Row("g" + (1000 + i), base + i + 1, 2L * i + 2));
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          // Each leaf mixes two distant groups, although each individual group's span is tiny.
          build(writer, rows, new int[] {4});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertNull(bounded(trx.getStorageEngineReader(), 3, 1));
          assertEquals(expected(rows, 3, 1),
              ProjectionSortedGroupScan.topK(trx.getStorageEngineReader(), 0, 3, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  @Test
  void boundOverflowSaturatesButExactGroupOverflowStillFails() {
    assertEquals(Long.MAX_VALUE, ProjectionSortedSpanScan.spanUpper(Long.MIN_VALUE, Long.MAX_VALUE, 1));
    final Path path = directory.resolve("overflow");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, List.of(new Row(null, Long.MIN_VALUE, 1), new Row(null, Long.MAX_VALUE, 2)), new int[] {1});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertThrows(IllegalStateException.class, () -> bounded(trx.getStorageEngineReader(), 1, 1));
          assertEquals(List.of(new Group(null, Long.MIN_VALUE, Long.MAX_VALUE)),
              bounded(trx.getStorageEngineReader(), 1, 3));
          assertThrows(IllegalArgumentException.class, () -> bounded(trx.getStorageEngineReader(), 1, 0));
        }
      }
    }
  }

  @Test
  void hashValidSummaryCorruptionIsRejected() {
    final Path path = directory.resolve("corrupt");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, List.of(new Row("a", 0, 1), new Row("a", 100, 2)), new int[] {2});
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedLeaf valid = ProjectionSortedGroupSummary.read(writer.getStorageEngineWriter(), 0, 1);
          final byte[] payload = new byte[16];
          ProjectionIndexRowGroupCodec.putLongLEAt(payload, 8, 99);
          final ProjectionSortedLeaf invalid =
              ProjectionSortedLeaf.encode(new byte[][] {valid.copyKey(0)}, new byte[][] {payload}, 1);
          storage.putBlob(ProjectionSortedGroupSummary.slot(1), invalid.encodedBytes());
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertThrows(IllegalStateException.class, () -> bounded(trx.getStorageEngineReader(), 1, 1));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void priorityAndDenseLookupMatchAnIndependentFoldAcrossLargeReorderedHistories(final VersioningType versioning) {
    final Path path = directory.resolve("large-priority-history");
    final List<Row> original = new ArrayList<>(2048);
    for (int group = 0; group < 1024; group++) {
      final String name = group == 0
          ? null
          : group == 1
              ? ""
              : group == 2
                  ? "a\0ß"
                  : "g" + (1000 + group);
      final long minimum = (group - 512L) * 10000;
      original.add(new Row(name, minimum, original.size() + 1));
      original.add(new Row(name, minimum + (group + 1L) * 777, original.size() + 1));
      if (group == 1023) {
        for (int extra = 0; extra < 1600; extra++) {
          original.add(new Row(name, minimum + (extra + 1L) * 1_000_000, original.size() + 1));
        }
      }
    }
    final List<Row> changed = new ArrayList<>(original);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, original, new int[] {1});
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          for (int i = 512; i < 552; i++) {
            final Row row = original.get(i);
            editor.remove(key(row));
            changed.remove(row);
          }
          for (int i = 0; i < 300; i++) {
            final Row row = new Row("g1003", 500_000_000L + i * 1001L, original.size() + i + 1);
            editor.insert(key(row), new byte[0]);
            changed.add(row);
          }
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    final String previous = System.getProperty("sirix.projection.heapSpanPriority");
    final String previousDense = System.getProperty("sirix.projection.denseSpanBounds");
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          assertTrue(ProjectionSortedDirectory.open(reader, 0).dataLeafCount() >= 1024);
          for (int mode = 0; mode < 4; mode++) {
            System.setProperty("sirix.projection.heapSpanPriority", Boolean.toString((mode & 1) != 0));
            System.setProperty("sirix.projection.denseSpanBounds", Boolean.toString((mode & 2) != 0));
            for (final long divisor : new long[] {1, 7, 100}) {
              for (final int limit : new int[] {1, 3, 17, 32}) {
                final List<Group> answer = expected(revision == 1
                    ? original
                    : changed, limit, divisor);
                assertNotNull(answer, "large priority fixture must have unique winning spans");
                assertEquals(answer, bounded(reader, limit, divisor));
              }
            }
          }
        }
      }
    } finally {
      if (previous == null)
        System.clearProperty("sirix.projection.heapSpanPriority");
      else
        System.setProperty("sirix.projection.heapSpanPriority", previous);
      if (previousDense == null)
        System.clearProperty("sirix.projection.denseSpanBounds");
      else
        System.setProperty("sirix.projection.denseSpanBounds", previousDense);
    }
  }

  @Test
  void heapKeepsTheStrictTieCutoffAndUnselectiveReadBudget() {
    final Path path = directory.resolve("large-priority-ties");
    final List<Row> rows = new ArrayList<>(2048);
    for (int group = 0; group < 1024; group++) {
      rows.add(new Row("group" + group, 0, rows.size() + 1));
      rows.add(new Row("group" + group, 777, rows.size() + 1));
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final String previous = System.getProperty("sirix.projection.heapSpanPriority");
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, rows, new int[] {1});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(1)) {
          for (final boolean heap : new boolean[] {false, true}) {
            System.setProperty("sirix.projection.heapSpanPriority", Boolean.toString(heap));
            assertNull(bounded(trx.getStorageEngineReader(), 3, 1));
          }
        }
      }
    } finally {
      if (previous == null)
        System.clearProperty("sirix.projection.heapSpanPriority");
      else
        System.setProperty("sirix.projection.heapSpanPriority", previous);
    }
  }

  private static List<Group> bounded(final StorageEngineReader reader, final int limit, final long divisor) {
    final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(sorted);
    return ProjectionSortedSpanScan.topK(reader, 0, sorted, limit, divisor);
  }

  private static void build(final JsonNodeTrx writer, final List<Row> rows, final int[] sizes) {
    final byte[][] keys = rows.stream().map(ProjectionSortedSpanScanTest::key).toArray(byte[][]::new);
    Arrays.sort(keys, Arrays::compareUnsigned);
    final ProjectionSortedDirectory.Builder builder =
        new ProjectionSortedDirectory.Builder(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
    for (int from = 0, batch = 0; from < keys.length; batch++) {
      final int to = Math.min(keys.length, from + sizes[batch % sizes.length]);
      final byte[][] leafKeys = Arrays.copyOfRange(keys, from, to);
      final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(leafKeys, null, leafKeys.length);
      assertNotNull(leaf);
      builder.append(leaf);
      from = to;
    }
    builder.finish();
  }

  private static byte[] key(final Row row) {
    final ProjectionSortKeyCodec.Writer key = new ProjectionSortKeyCodec.Writer();
    if (row.group() == null) {
      key.appendMissing();
    } else {
      final byte[] utf8 = row.group().getBytes(StandardCharsets.UTF_8);
      key.appendUtf8(utf8, 0, utf8.length);
    }
    key.appendLong(row.value());
    key.appendRecordKey(row.record());
    return key.copyKey();
  }

  private static List<Group> expected(final List<Row> rows, final int limit, final long divisor) {
    final Map<String, long[]> groups = new HashMap<>();
    for (final Row row : rows) {
      final long[] extrema =
          groups.computeIfAbsent(row.group(), ignored -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      extrema[0] = Math.min(extrema[0], row.value());
      extrema[1] = Math.max(extrema[1], row.value());
    }
    final List<Group> ordered = new ArrayList<>();
    groups.forEach((name, extrema) -> ordered.add(new Group(name, extrema[0], extrema[1])));
    final Comparator<Group> comparator =
        Comparator.comparingLong(group -> group.max() / divisor - group.min() / divisor);
    ordered.sort(comparator.reversed());
    for (int i = 1; i < Math.min(ordered.size(), limit + 1); i++) {
      if (comparator.compare(ordered.get(i - 1), ordered.get(i)) == 0) {
        return null;
      }
    }
    return List.copyOf(ordered.subList(0, Math.min(limit, ordered.size())));
  }
}
