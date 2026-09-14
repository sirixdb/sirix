/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded grouped extrema over a revision's partial sorted projection. */
public final class ProjectionSortedGroupScan {

  public enum Order {
    MIN_ASC,
    MAX_DESC,
    SPAN_DESC
  }

  public record Group(@Nullable String key, long min, long max) {}

  private ProjectionSortedGroupScan() {}

  /**
   * Scan a two-field sorted view (string group, ordered long value), retaining at most {@code limit+1}
   * groups. Returns {@code null} when the key shape or an ordering tie cannot be proved exact. Ties
   * need the original document-first group ordinal, which a key-ordered page does not carry.
   */
  public static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber,
      final int limit, final Order order, final long spanDivisor) {
    return topK(reader, indexNumber, limit, order, spanDivisor, false);
  }

  /** {@code minOnly} permits jumping over every row after each group's first ordered value. */
  public static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber,
      final int limit, final Order order, final long spanDivisor, final boolean minOnly) {
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(order, "order");
    if (limit < 1 || limit > 32 || order == Order.SPAN_DESC && spanDivisor < 1
        || minOnly && order != Order.MIN_ASC) {
      throw new IllegalArgumentException("unsupported sorted group top-K request");
    }
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, indexNumber);
    if (directory == null) {
      return null;
    }
    if (!"false".equals(System.getProperty("sirix.projection.sortedGroupSummaries"))) {
      if (minOnly && !"false".equals(System.getProperty("sirix.projection.sortedMinBounds"))) {
        final List<Group> bounded = topKFromBounds(reader, indexNumber, directory, limit);
        if (bounded != null) {
          return bounded;
        }
      }
      final List<Group> summarized =
          topKFromSummaries(reader, indexNumber, directory, limit, order, spanDivisor, minOnly);
      if (summarized != null) {
        return summarized;
      }
    }
    final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
    if (!cursor.isValid()) {
      return List.of();
    }
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    int retained = 0;
    byte[] key = new byte[128];
    byte[] currentGroup = new byte[128];
    if (minOnly) {
      while (cursor.isValid()) {
        final int length = cursor.keyLength();
        if (length > key.length) {
          key = Arrays.copyOf(key, Math.max(length, key.length << 1));
        }
        cursor.copyKeyTo(key);
        final int groupLength = stringPrefixLength(key, length);
        if (groupLength < 0 || length != groupLength + 1 + Long.BYTES + Long.BYTES
            || key[groupLength] != 1) {
          return null;
        }
        final long value = readOrderedLong(key, groupLength + 1);
        retained = offer(key, groupLength, value, value, order, spanDivisor,
            winners, minimums, maximums, scores, retained);
        cursor.skipPrefix(key, groupLength);
      }
    } else {
      while (cursor.isValid()) {
        final int length = cursor.keyLength();
        if (length > key.length) {
          key = Arrays.copyOf(key, Math.max(length, key.length << 1));
        }
        cursor.copyKeyTo(key);
        final int groupLength = stringPrefixLength(key, length);
        if (groupLength < 0 || length != groupLength + 1 + Long.BYTES + Long.BYTES
            || key[groupLength] != 1) {
          return null;
        }
        final long groupMin = readOrderedLong(key, groupLength + 1);
        if (groupLength > currentGroup.length) {
          currentGroup = Arrays.copyOf(currentGroup, Math.max(groupLength, currentGroup.length << 1));
        }
        System.arraycopy(key, 0, currentGroup, 0, groupLength);
        cursor.skipPrefixCapturingLast(key, groupLength);
        if (cursor.lastSkippedKeyLength() != length) {
          return null;
        }
        cursor.copyLastSkippedKeyTo(key);
        if (key[groupLength] != 1) {
          return null;
        }
        final long groupMax = readOrderedLong(key, groupLength + 1);
        retained = offer(currentGroup, groupLength, groupMin, groupMax, order, spanDivisor,
            winners, minimums, maximums, scores, retained);
      }
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  /** Backfill this optional acceleration in the caller's transaction; commit remains caller-owned. */
  public static int buildLeafSummaries(final StorageEngineWriter writer, final int indexNumber) {
    Objects.requireNonNull(writer, "writer");
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(writer, indexNumber);
    if (directory == null) {
      return 0;
    }
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer, indexNumber);
    final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
    int written = 0;
    while (cursor.id() != 0) {
      final int id = cursor.id();
      final ProjectionSortedLeaf leaf = ProjectionSortedLeafStore.read(storage, id);
      if (leaf == null) {
        throw new IllegalStateException("missing sorted data leaf during summary build: " + id);
      }
      final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf);
      ProjectionSortedLeafBounds.invalidate(storage, id);
      storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(id));
      if (summary != null) {
        storage.putBlob(ProjectionSortedGroupSummary.slot(id), summary.encodedBytes());
        ProjectionSortedLeafBounds.write(storage, id, summary);
        written++;
      }
      cursor.advance();
    }
    return written;
  }

  /**
   * Visit summaries in ascending leaf-minimum order. Once an unseen leaf's minimum is greater than
   * the current (K+1)-th distinct group's minimum, it cannot change a winner or its tie cutline.
   * A group may cross many leaves, so each offer merges by exact encoded group identity.
   */
  static @Nullable List<Group> topKFromBounds(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit) {
    final ProjectionSortedLeafBounds.Candidates candidates = ProjectionSortedLeafBounds.read(reader, indexNumber,
        directory);
    if (candidates == null) {
      return null;
    }
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
    byte[] key = new byte[128];
    int retained = 0;
    for (final int candidate : candidates.order()) {
      final long boundMin = candidates.minimums()[candidate];
      if (retained == capacity && boundMin > scores[retained - 1]) {
        break;
      }
      final ProjectionSortedLeaf summary =
          ProjectionSortedGroupSummary.read(reader, indexNumber, candidates.leafIds()[candidate]);
      if (summary == null) {
        return null;
      }
      long actualMin = Long.MAX_VALUE;
      long actualMax = Long.MIN_VALUE;
      for (int row = 0; row < summary.rowCount(); row++) {
        final int length = summary.keyLength(row);
        if (length > key.length) {
          key = new byte[length];
        }
        summary.copyKeyTo(row, key);
        if (stringPrefixLength(key, length) != length || summary.payloadLength(row) != payload.length) {
          throw new IllegalStateException("invalid sorted group summary entry");
        }
        summary.copyPayloadTo(row, payload, 0);
        final long min = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
        final long max = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
        if (min > max) {
          throw new IllegalStateException("invalid sorted group summary extrema");
        }
        actualMin = Math.min(actualMin, min);
        actualMax = Math.max(actualMax, max);
        retained = offerDistinctMinimum(key, length, min, winners, minimums, maximums, scores, retained);
      }
      if (summary.rowCount() == 0 || actualMin != boundMin || actualMax != candidates.maximums()[candidate]) {
        throw new IllegalStateException("sorted leaf bounds disagree with their group summary");
      }
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  private static int offerDistinctMinimum(final byte[] group, final int length, final long minimum,
      final byte[][] winners, final long[] minimums, final long[] maximums, final long[] scores, int retained) {
    for (int i = 0; i < retained; i++) {
      if (winners[i].length == length && Arrays.equals(winners[i], 0, length, group, 0, length)) {
        if (minimum >= minimums[i]) {
          return retained;
        }
        // Reinsert an improved minimum in score order. Evicted groups can return later through
        // the normal offer, so no unbounded set of all previously seen groups is retained.
        for (int j = i; j + 1 < retained; j++) {
          winners[j] = winners[j + 1];
          minimums[j] = minimums[j + 1];
          maximums[j] = maximums[j + 1];
          scores[j] = scores[j + 1];
        }
        winners[--retained] = null;
        break;
      }
    }
    return offer(group, length, minimum, minimum, Order.MIN_ASC, 1, winners, minimums, maximums, scores, retained);
  }

  static @Nullable List<Group> topKFromSummaries(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final Order order,
      final long spanDivisor, final boolean minOnly) {
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
    byte[] key = new byte[128];
    byte[] currentGroup = new byte[128];
    int groupLength = 0;
    long groupMin = 0;
    long groupMax = 0;
    int retained = 0;
    final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
    while (cursor.id() != 0) {
      final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.read(reader, indexNumber, cursor.id());
      if (summary == null) {
        return null; // an old or partially backfilled revision still uses the full-key route
      }
      for (int row = 0; row < summary.rowCount(); row++) {
        final int length = summary.keyLength(row);
        if (length > key.length) {
          key = new byte[length];
        }
        summary.copyKeyTo(row, key);
        if (stringPrefixLength(key, length) != length || summary.payloadLength(row) != payload.length) {
          throw new IllegalStateException("invalid sorted group summary entry");
        }
        summary.copyPayloadTo(row, payload, 0);
        final long min = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
        final long max = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
        if (min > max) {
          throw new IllegalStateException("invalid sorted group summary extrema");
        }
        final int comparison = groupLength == 0 ? -1
            : Arrays.compareUnsigned(currentGroup, 0, groupLength, key, 0, length);
        if (comparison > 0) {
          throw new IllegalStateException("sorted group summaries are out of order");
        }
        if (comparison == 0) {
          groupMin = Math.min(groupMin, min);
          groupMax = Math.max(groupMax, max);
        } else {
          if (groupLength != 0) {
            retained = offer(currentGroup, groupLength, groupMin, minOnly ? groupMin : groupMax, order, spanDivisor,
                winners, minimums, maximums, scores, retained);
          }
          if (length > currentGroup.length) {
            currentGroup = new byte[length];
          }
          System.arraycopy(key, 0, currentGroup, 0, length);
          groupLength = length;
          groupMin = min;
          groupMax = max;
        }
      }
      cursor.advance();
    }
    if (groupLength != 0) {
      retained = offer(currentGroup, groupLength, groupMin, minOnly ? groupMin : groupMax, order, spanDivisor,
          winners, minimums, maximums, scores, retained);
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  private static @Nullable List<Group> finish(final byte[][] winners, final long[] minimums,
      final long[] maximums, final long[] scores, final int retained, final int limit) {
    // The interpreter's stable sort uses document-first group order for equal scores. The sorted
    // view orders groups by key, so decline a tie among winners or across the cut line.
    for (int i = 1, end = Math.min(retained, limit + 1); i < end; i++) {
      if (scores[i - 1] == scores[i]) {
        return null;
      }
    }
    final int count = Math.min(retained, limit);
    final ArrayList<Group> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      result.add(new Group(decodeString(winners[i]), minimums[i], maximums[i]));
    }
    return result;
  }

  private static int offer(final byte[] group, final int groupLength, final long min, final long max,
      final Order order, final long divisor, final byte[][] winners, final long[] minimums,
      final long[] maximums, final long[] scores, final int retained) {
    final long score;
    try {
      score = switch (order) {
        case MIN_ASC -> min;
        case MAX_DESC -> max;
        case SPAN_DESC -> Math.subtractExact(max / divisor, min / divisor);
      };
    } catch (final ArithmeticException e) {
      throw new IllegalStateException("sorted group span exceeds a long", e);
    }
    int at = 0;
    while (at < retained && (order == Order.MIN_ASC ? scores[at] <= score : scores[at] >= score)) {
      at++;
    }
    if (at == winners.length) {
      return retained;
    }
    final int next = Math.min(winners.length, retained + 1);
    for (int i = next - 1; i > at; i--) {
      winners[i] = winners[i - 1];
      minimums[i] = minimums[i - 1];
      maximums[i] = maximums[i - 1];
      scores[i] = scores[i - 1];
    }
    winners[at] = Arrays.copyOf(group, groupLength);
    minimums[at] = min;
    maximums[at] = max;
    scores[at] = score;
    return next;
  }

  static int stringPrefixLength(final byte[] key, final int length) {
    if (length < 1) {
      return -1;
    }
    if (key[0] == 0) {
      return 1;
    }
    if (key[0] != 1) {
      return -1;
    }
    for (int i = 1; i + 1 < length; i++) {
      if (key[i] == 0) {
        if (key[i + 1] == 0) {
          return i + 2;
        }
        if ((key[i + 1] & 0xFF) != 0xFF) {
          return -1;
        }
        i++;
      }
    }
    return -1;
  }

  static long readOrderedLong(final byte[] key, final int offset) {
    long value = 0;
    for (int i = 0; i < Long.BYTES; i++) {
      value = value << 8 | key[offset + i] & 0xFFL;
    }
    return value ^ Long.MIN_VALUE;
  }

  private static @Nullable String decodeString(final byte[] group) {
    if (group[0] == 0) {
      return null;
    }
    final byte[] utf8 = new byte[group.length - 3];
    int used = 0;
    for (int i = 1; i < group.length - 2; i++) {
      utf8[used++] = group[i];
      if (group[i] == 0) {
        i++;
      }
    }
    return new String(utf8, 0, used, StandardCharsets.UTF_8);
  }
}
