/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Sorted-projection fixtures shared by the lookahead equivalence tests (mirrors the span-scan test's). */
final class SortedScanFixtures {
  /** A view keyed by one string group field and one ordered long value field. */
  static final ProjectionSortKeyCodec.Layout GROUP_VALUE = new ProjectionSortKeyCodec.Layout(
      new byte[] {ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_LONG});

  record Row(@Nullable String group, long value, long record) {
  }

  private SortedScanFixtures() {}

  /** 48 groups of varied sizes with the three special group keys (missing, empty, embedded NUL). */
  static List<Row> history(final int seed) {
    final List<Row> original = new ArrayList<>();
    final Random random = new Random(seed);
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
    return original;
  }

  static void build(final JsonNodeTrx writer, final List<Row> rows, final int[] sizes) {
    final byte[][] keys = rows.stream().map(SortedScanFixtures::key).toArray(byte[][]::new);
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

  static byte[] key(final Row row) {
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

  /** Independent fold: the top-{@code limit} groups by span, {@code null} on a tie within or at the cut. */
  static @Nullable List<Group> expectedSpan(final List<Row> rows, final int limit, final long divisor) {
    final Comparator<Group> comparator =
        Comparator.comparingLong(group -> group.max() / divisor - group.min() / divisor);
    return expected(rows, limit, comparator.reversed());
  }

  /**
   * Independent fold: the top-{@code limit} groups by minimum, {@code null} on a tie within or at the
   * cut. The min-only routes report each group's minimum as both extrema, so the fold does too.
   */
  static @Nullable List<Group> expectedMin(final List<Row> rows, final int limit) {
    final List<Group> byMinimum = expected(rows, limit, Comparator.comparingLong(Group::min));
    if (byMinimum == null) {
      return null;
    }
    final List<Group> minOnly = new ArrayList<>(byMinimum.size());
    for (final Group group : byMinimum) {
      minOnly.add(new Group(group.key(), group.min(), group.min()));
    }
    return List.copyOf(minOnly);
  }

  private static @Nullable List<Group> expected(final List<Row> rows, final int limit, final Comparator<Group> order) {
    final Map<String, long[]> groups = new HashMap<>();
    for (final Row row : rows) {
      final long[] extrema =
          groups.computeIfAbsent(row.group(), ignored -> new long[] {Long.MAX_VALUE, Long.MIN_VALUE});
      extrema[0] = Math.min(extrema[0], row.value());
      extrema[1] = Math.max(extrema[1], row.value());
    }
    final List<Group> ordered = new ArrayList<>();
    groups.forEach((name, extrema) -> ordered.add(new Group(name, extrema[0], extrema[1])));
    ordered.sort(order);
    for (int i = 1; i < Math.min(ordered.size(), limit + 1); i++) {
      if (order.compare(ordered.get(i - 1), ordered.get(i)) == 0) {
        return null;
      }
    }
    return List.copyOf(ordered.subList(0, Math.min(limit, ordered.size())));
  }
}
