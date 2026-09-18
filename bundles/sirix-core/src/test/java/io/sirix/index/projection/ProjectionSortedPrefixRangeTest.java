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
import io.sirix.index.projection.ProjectionIndexHOTStorage.ParallelWalkReaders;
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Equality-prefix ranges over a multi-leaf, four-field sorted view (two prefix fields, a string
 * group, a long value). Several prefixes each span several leaves, leaf boundaries fall inside
 * groups, and one group of prefix {@code (b, x)} crosses a boundary while holding that prefix's
 * minimum, maximum and widest span. Every route is compared against a brute-force fold.
 */
final class ProjectionSortedPrefixRangeTest {

  private static final ProjectionSortKeyCodec.Layout LAYOUT =
      new ProjectionSortKeyCodec.Layout(new byte[] {ProjectionSortKeyCodec.FIELD_STRING,
          ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_LONG});

  private static final String[][] PREFIXES = {{"a", "x"}, {"a", "y"}, {"b", "x"}, {"c", "x"}};

  private static final int[] LEAF_SIZES = {97, 256, 13, 200, 256, 64, 181, 7, 240};

  private record Row(String first, String second, @Nullable String group, long value, long record) {
  }

  @TempDir
  Path temporaryDirectory;

  @Test
  void everyRouteMatchesABruteForceFoldAcrossLeafBoundaries() {
    final List<Row> rows = rows();
    final byte[][] keys = new byte[rows.size()][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = key(rows.get(i));
    }
    Arrays.sort(keys, Arrays::compareUnsigned);
    final Path databasePath = temporaryDirectory.resolve("sorted-prefix-ranges");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), LAYOUT);
          for (int from = 0, leaf = 0; from < keys.length; leaf++) {
            final int to = Math.min(keys.length, from + LEAF_SIZES[leaf % LEAF_SIZES.length]);
            builder.append(ProjectionSortedLeaf.encode(keys, null, from, to - from));
            from = to;
          }
          builder.finish();
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, 0);
          assertNotNull(directory);
          assertTrue(directory.dataLeafCount() >= 8, "the fixture must span many leaves");
          final AtomicInteger lanes = new AtomicInteger();
          final ParallelWalkReaders workers = worker -> {
            try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(revision)) {
              lanes.incrementAndGet();
              worker.accept(lane.getStorageEngineReader());
            }
          };
          for (final String[] prefixFields : PREFIXES) {
            final byte[] prefix = prefix(prefixFields[0], prefixFields[1]);
            final byte[] upper = ProjectionSortKeyCodec.prefixUpperExclusive(prefix);
            assertRangeLeaves(directory, prefix, upper);
            final List<Row> selected = select(rows, prefixFields[0], prefixFields[1]);
            for (int limit = 1; limit <= 3; limit++) {
              assertRoutes(reader, directory, prefix, upper, selected, limit, workers);
            }
          }
          assertTrue(lanes.get() > 0, "parallel summary windows must have opened independent readers");
          final byte[] absent = prefix("b", "y");
          assertEquals(List.of(), ProjectionSortedGroupScan.topK(reader, 0, absent, 2, Order.MIN_ASC, 1, true, null));
          assertEquals(List.of(), ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, absent,
              ProjectionSortKeyCodec.prefixUpperExclusive(absent), 2, Order.MAX_DESC, 1, false, workers, 4));
          final byte[] crossing = prefix("b", "x");
          assertNull(directory.leafIds(crossing, ProjectionSortKeyCodec.prefixUpperExclusive(crossing), 1),
              "a range beyond the caller's maximum yields no ids");
        }
      }
    }
  }

  /** The range walk returns exactly the contiguous leaves that can hold the prefix. */
  private static void assertRangeLeaves(final ProjectionSortedDirectory.Accessor directory, final byte[] prefix,
      final byte[] upper) {
    final IntArrayList allLeaves = new IntArrayList();
    final IntArrayList holding = new IntArrayList();
    final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
    while (cursor.isValid()) {
      final int leaf = cursor.leafId();
      if (allLeaves.isEmpty() || allLeaves.getInt(allLeaves.size() - 1) != leaf) {
        allLeaves.add(leaf);
      }
      final byte[] key = cursor.copyKey();
      if (ProjectionSortKeyCodec.startsWith(key, key.length, prefix)
          && (holding.isEmpty() || holding.getInt(holding.size() - 1) != leaf)) {
        holding.add(leaf);
      }
      cursor.advance();
    }
    assertTrue(holding.size() >= 2, "every prefix must span several leaves");
    final int[] range = directory.leafIds(prefix, upper, Integer.MAX_VALUE);
    assertNotNull(range);
    final IntArrayList walked = new IntArrayList();
    final ProjectionSortedDirectory.Accessor.LeafCursor leaves = directory.leaves(prefix, upper);
    while (leaves.id() != 0) {
      walked.add(leaves.id());
      leaves.advance();
    }
    assertArrayEquals(range, walked.toIntArray());
    final int start = allLeaves.indexOf(range[0]);
    for (int i = 0; i < range.length; i++) {
      assertEquals(allLeaves.getInt(start + i), range[i], "a prefix range is one contiguous run of leaves");
    }
    assertEquals(holding.getInt(holding.size() - 1), range[range.length - 1]);
    final int firstHolding = allLeaves.indexOf(holding.getInt(0));
    assertTrue(start == firstHolding || start == firstHolding - 1,
        "the range starts at the first holding leaf or the leaf just below the prefix");
    assertEquals(range.length, directory.leafCount(prefix, upper, Integer.MAX_VALUE));
    assertEquals(1, directory.leafCount(prefix, upper, 1));
  }

  private static void assertRoutes(final StorageEngineReader reader, final ProjectionSortedDirectory.Accessor directory,
      final byte[] prefix, final byte[] upper, final List<Row> rows, final int limit,
      final ParallelWalkReaders workers) {
    final List<Group> minOnly = expected(rows, limit, Order.MIN_ASC, 1, true);
    final List<Group> minimum = expected(rows, limit, Order.MIN_ASC, 1, false);
    final List<Group> maximum = expected(rows, limit, Order.MAX_DESC, 1, false);
    assertEquals(minOnly,
        ProjectionSortedGroupScan.topKFromBounds(reader, 0, directory, prefix, upper, limit, 1, null));
    assertEquals(minOnly,
        ProjectionSortedGroupScan.topKFromBounds(reader, 0, directory, prefix, upper, limit, 4, null));
    for (final long divisor : new long[] {1, 7}) {
      final List<Group> span = expected(rows, limit, Order.SPAN_DESC, divisor, false);
      assertEquals(span, ProjectionSortedSpanScan.topK(reader, 0, directory, prefix, upper, limit, divisor, 1, null));
      assertEquals(span, ProjectionSortedSpanScan.topK(reader, 0, directory, prefix, upper, limit, divisor, 8, null));
      assertEquals(span, ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, prefix, upper, limit,
          Order.SPAN_DESC, divisor, false, null, 0));
      assertEquals(span, ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, prefix, upper, limit,
          Order.SPAN_DESC, divisor, false, workers, 4));
      assertEquals(span, inOrder(reader, prefix, limit, Order.SPAN_DESC, divisor, false));
    }
    for (final boolean parallel : new boolean[] {false, true}) {
      assertEquals(minOnly,
          ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, prefix, upper, limit, Order.MIN_ASC, 1,
              true, parallel
                  ? workers
                  : null,
              parallel
                  ? 4
                  : 0));
      assertEquals(minimum,
          ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, prefix, upper, limit, Order.MIN_ASC, 1,
              false, parallel
                  ? workers
                  : null,
              parallel
                  ? 4
                  : 0));
      assertEquals(maximum,
          ProjectionSortedGroupScan.topKFromSummaries(reader, 0, directory, prefix, upper, limit, Order.MAX_DESC, 1,
              false, parallel
                  ? workers
                  : null,
              parallel
                  ? 4
                  : 0));
    }
    assertEquals(minOnly, inOrder(reader, prefix, limit, Order.MIN_ASC, 1, true));
    assertEquals(minimum, inOrder(reader, prefix, limit, Order.MIN_ASC, 1, false));
    assertEquals(maximum, inOrder(reader, prefix, limit, Order.MAX_DESC, 1, false));
    assertEquals(minOnly, ProjectionSortedGroupScan.topK(reader, 0, prefix, limit, Order.MIN_ASC, 1, true, workers));
    assertEquals(maximum, ProjectionSortedGroupScan.topK(reader, 0, prefix, limit, Order.MAX_DESC, 1, false, workers));
  }

  /**
   * The full-key route: a prefix seek, then an in-order walk that stops at the first key outside it.
   */
  private static @Nullable List<Group> inOrder(final StorageEngineReader reader, final byte[] prefix, final int limit,
      final Order order, final long divisor, final boolean minOnly) {
    final String previous = System.getProperty("sirix.projection.sortedGroupSummaries");
    System.setProperty("sirix.projection.sortedGroupSummaries", "false");
    try {
      return ProjectionSortedGroupScan.topK(reader, 0, prefix, limit, order, divisor, minOnly, null);
    } finally {
      if (previous == null) {
        System.clearProperty("sirix.projection.sortedGroupSummaries");
      } else {
        System.setProperty("sirix.projection.sortedGroupSummaries", previous);
      }
    }
  }

  /** Independent fold; null when a tie falls within or at the cut, as every route then declines. */
  private static @Nullable List<Group> expected(final List<Row> rows, final int limit, final Order order,
      final long divisor, final boolean minOnly) {
    final Map<String, long[]> extrema = new HashMap<>();
    for (final Row row : rows) {
      final long[] range = extrema.computeIfAbsent(row.group(), ignored -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      range[0] = Math.min(range[0], row.value());
      range[1] = Math.max(range[1], row.value());
    }
    final List<Group> groups = new ArrayList<>(extrema.size());
    extrema.forEach((group, range) -> groups.add(new Group(group, range[0], minOnly
        ? range[0]
        : range[1])));
    final Comparator<Group> comparator = switch (order) {
      case MIN_ASC -> Comparator.comparingLong(Group::min);
      case MAX_DESC -> Comparator.comparingLong(Group::max).reversed();
      case SPAN_DESC ->
        Comparator.<Group>comparingLong(group -> group.max() / divisor - group.min() / divisor).reversed();
    };
    groups.sort(comparator);
    for (int i = 1; i < Math.min(groups.size(), limit + 1); i++) {
      if (comparator.compare(groups.get(i - 1), groups.get(i)) == 0) {
        return null;
      }
    }
    return List.copyOf(groups.subList(0, Math.min(limit, groups.size())));
  }

  private static List<Row> select(final List<Row> rows, final String first, final String second) {
    final List<Row> selected = new ArrayList<>();
    for (final Row row : rows) {
      if (row.first().equals(first) && row.second().equals(second)) {
        selected.add(row);
      }
    }
    return selected;
  }

  /**
   * About 1,500 rows. Each prefix holds a missing group, distinct random groups, and one group of 300
   * rows; in {@code (b, x)} that group owns the prefix's extreme minimum and maximum.
   */
  private static List<Row> rows() {
    final Random random = new Random(0x50AF1L);
    final List<Row> rows = new ArrayList<>();
    long record = 1;
    for (final String[] prefix : PREFIXES) {
      final boolean holdsExtremes = prefix[0].equals("b");
      for (int i = 0; i < 300; i++) {
        final long value = holdsExtremes
            ? -1_000_000_000L + i * 6_700_000L
            : 1_000L * i + random.nextInt(900);
        rows.add(new Row(prefix[0], prefix[1], "long-" + prefix[0] + prefix[1], value, record++));
      }
      for (int group = 0; group < 40; group++) {
        final int size = 1 + random.nextInt(4);
        final long base = random.nextInt(2_000_000) - 1_000_000L;
        for (int i = 0; i < size; i++) {
          rows.add(new Row(prefix[0], prefix[1], group == 0
              ? null
              : "g" + (100 + group), base + (long) i * (group + 3), record++));
        }
      }
    }
    return rows;
  }

  private static byte[] prefix(final String first, final String second) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, first);
    appendString(writer, second);
    return writer.copyKey();
  }

  private static byte[] key(final Row row) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, row.first());
    appendString(writer, row.second());
    if (row.group() == null) {
      writer.appendMissing();
    } else {
      appendString(writer, row.group());
    }
    writer.appendLong(row.value());
    writer.appendRecordKey(row.record());
    return writer.copyKey();
  }

  private static void appendString(final ProjectionSortKeyCodec.Writer writer, final String value) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
  }
}
