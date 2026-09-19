/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.projection.ProjectionIndexHOTStorage.ParallelWalkReaders;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/** Bounded grouped extrema over a key range of a revision's sorted projection view. */
public final class ProjectionSortedGroupScan {

  /**
   * Bound independent readers and their caches; the directory also requires 1,024 leaves per lane.
   */
  private static final boolean BATCH_SUMMARIES =
      Boolean.parseBoolean(System.getProperty("sirix.projection.batchSortedSummaries", "true"));
  private static final int MAX_SUMMARY_WORKERS =
      Math.max(1, Math.min(8, Integer.getInteger("sirix.projection.sortedSummaryMaxWorkers", 4)));
  /** Leaves of the queried key range each summary worker needs before a lane is opened. */
  private static final int LEAVES_PER_SUMMARY_WORKER = 1024;

  /**
   * Candidate summary leaves the bound walk (here) and the best-first span scan
   * ({@link ProjectionSortedSpanScan}) fetch together, in one batch, ahead of consuming them. The
   * candidate ORDER is fixed before the first read; only the stop point depends on the data, so the
   * next {@code window} candidates are known and their leaves can be in flight at once — cold, each
   * one otherwise waits its own device round trip. {@code 1} restores the one-read-per-step loop
   * exactly; at most {@code window - 1} leaves (twice that for run edges in the span scan) are
   * fetched past the stop point for nothing. Conservative default;
   * {@code -Dsirix.projection.sortedLookahead=N}.
   */
  static final int SORTED_LOOKAHEAD =
      Math.max(1, Math.min(64, Integer.getInteger("sirix.projection.sortedLookahead", 8)));

  /**
   * Lookahead accounting on stderr, only under {@code -Dsirix.projDiag=true}; never in timed runs.
   */
  static final boolean LOOKAHEAD_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Lookahead accounting for the focused equivalence tests and the diagnostic line: leaves fetched
   * ahead, leaves of those actually consumed, and reads charged (the count the serial loop would have
   * made, and for the span scan the count its budget saw).
   */
  static final class LookaheadStats {
    int fetched;
    int consumed;
    int charged;

    int wasted() {
      return fetched - consumed;
    }

    @Override
    public String toString() {
      return "fetched=" + fetched + " consumed=" + consumed + " wasted=" + wasted() + " charged=" + charged;
    }
  }

  /**
   * Whether a summaries-route decline also proves that the full-key walk cannot serve the range.
   *
   * <p>
   * It does when a leaf lying entirely inside the queried range has no group summary and the view's
   * header counts rows with no aggregate value: such a leaf then holds one of those rows, inside the
   * range, and the full-key walk would reach it and decline too. A leaf that only meets the range at
   * one of its two boundaries proves nothing, because the offending row may be one of the leaf's rows
   * outside the range — that walk still runs, exactly as it did before the count existed.
   * </p>
   */
  static final class SummaryDecline {
    private boolean proven;

    boolean proven() {
      return proven;
    }

    private void prove() {
      proven = true;
    }
  }

  public enum Order {
    MIN_ASC, MAX_DESC, SPAN_DESC
  }

  public record Group(@Nullable String key, long min, long max) {
  }

  /** No equality prefix: the whole view, for a view with exactly a group and a value field. */
  static final byte[] NO_PREFIX = ProjectionSortedDirectory.NO_BOUND;

  /** Scratch that has not held a key yet; every real key is longer, so the first use replaces it. */
  private static final byte[] NO_KEY = new byte[0];

  private ProjectionSortedGroupScan() {}

  /**
   * Scan a two-field sorted view (string group, ordered long value), retaining at most
   * {@code limit+1} groups. Returns {@code null} when the key shape or an ordering tie cannot be
   * proved exact. Ties need the original document-first group ordinal, which a key-ordered page does
   * not carry.
   */
  public static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber, final int limit,
      final Order order, final long spanDivisor) {
    return topK(reader, indexNumber, NO_PREFIX, limit, order, spanDivisor, false, null);
  }

  /** {@code minOnly} permits jumping over every row after each group's first ordered value. */
  public static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber, final int limit,
      final Order order, final long spanDivisor, final boolean minOnly) {
    return topK(reader, indexNumber, NO_PREFIX, limit, order, spanDivisor, minOnly, null);
  }

  /**
   * Grouped extrema over the rows whose leading key fields equal the encoded {@code prefix}: the next
   * key field groups and the last, ordered long field is aggregated. The prefix bounds every route to
   * one contiguous key range. Catalog-owned workers must open independent readers at the caller's
   * committed revision.
   */
  static @Nullable List<Group> topK(final StorageEngineReader reader, final int indexNumber, final byte[] prefix,
      final int limit, final Order order, final long spanDivisor, final boolean minOnly,
      final @Nullable ParallelWalkReaders workerReaders) {
    Objects.requireNonNull(reader, "reader");
    Objects.requireNonNull(prefix, "prefix");
    Objects.requireNonNull(order, "order");
    if (limit < 1 || limit > 32 || order == Order.SPAN_DESC && spanDivisor < 1 || minOnly && order != Order.MIN_ASC) {
      throw new IllegalArgumentException("unsupported sorted group top-K request");
    }
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, indexNumber);
    if (directory == null || directory.unencodableRows() > 0) {
      return null;
    }
    final ProjectionSortKeyCodec.Layout layout = directory.layout();
    final int fields = layout.fieldCount();
    if (fields < 2 || layout.field(fields - 2) != ProjectionSortKeyCodec.FIELD_STRING
        || layout.field(fields - 1) != ProjectionSortKeyCodec.FIELD_LONG) {
      return null;
    }
    if (layout.prefixEnd(prefix, prefix.length, fields - 2) != prefix.length) {
      throw new IllegalArgumentException("sorted group prefix does not encode exactly the leading key fields");
    }
    final byte[] upper = prefix.length == 0
        ? null
        : ProjectionSortKeyCodec.prefixUpperExclusive(prefix);
    if (!"false".equals(System.getProperty("sirix.projection.sortedGroupSummaries"))) {
      if (order == Order.SPAN_DESC && !reader.hasTrxIntentLog()
          && !"false".equals(System.getProperty("sirix.projection.sortedSpanBounds"))) {
        final List<Group> bounded = ProjectionSortedSpanScan.topK(reader, indexNumber, directory, prefix, upper, limit,
            spanDivisor, SORTED_LOOKAHEAD, null);
        if (bounded != null) {
          return bounded;
        }
      }
      if (minOnly && !"false".equals(System.getProperty("sirix.projection.sortedMinBounds"))) {
        final List<Group> bounded =
            topKFromBounds(reader, indexNumber, directory, prefix, upper, limit, SORTED_LOOKAHEAD, null);
        if (bounded != null) {
          return bounded;
        }
      }
      final int workers = workerReaders == null || reader.hasTrxIntentLog()
          || "false".equals(System.getProperty("sirix.projection.parallelSortedSummaries"))
              ? 0
              : Math.min(MAX_SUMMARY_WORKERS,
                  Math.min(Runtime.getRuntime().availableProcessors(),
                      directory.leafCount(prefix, upper, MAX_SUMMARY_WORKERS * LEAVES_PER_SUMMARY_WORKER)
                          / LEAVES_PER_SUMMARY_WORKER));
      // The header count is the cheap whole-view fact that enables the per-leaf proof below; while
      // it is zero or unknown the summaries route keeps every decision it had, at no added cost.
      final SummaryDecline decline = directory.holdsRowsWithoutAggregateValues()
          ? new SummaryDecline()
          : null;
      final List<Group> summarized = topKFromSummaries(reader, indexNumber, directory, prefix, upper, limit, order,
          spanDivisor, minOnly, workerReaders, workers, decline);
      if (summarized != null) {
        return summarized;
      }
      if (decline != null && decline.proven()) {
        // A leaf lying entirely inside the range has no summary, and the view holds rows with no
        // aggregate value, so that leaf holds one of them and it is inside the range. The full-key
        // walk would seek the prefix only to reach it and decline; the second walk buys nothing.
        return null;
      }
    }
    final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.seek(prefix);
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    int retained = 0;
    byte[] key = new byte[128];
    byte[] currentGroup = new byte[128];
    while (cursor.isValid()) {
      final int length = cursor.keyLength();
      if (length > key.length) {
        key = Arrays.copyOf(key, Math.max(length, key.length << 1));
      }
      cursor.copyKeyTo(key);
      if (!ProjectionSortKeyCodec.startsWith(key, length, prefix)) {
        break;
      }
      final int groupEnd = layout.fieldEnd(key, prefix.length, length - Long.BYTES, fields - 2);
      if (groupEnd < 0 || length != groupEnd + 1 + Long.BYTES + Long.BYTES
          || key[groupEnd] != ProjectionSortKeyCodec.PRESENT) {
        return null;
      }
      final long groupMin = readOrderedLong(key, groupEnd + 1);
      if (minOnly) {
        retained = offer(key, prefix.length, groupEnd, groupMin, groupMin, order, spanDivisor, winners, minimums,
            maximums, scores, retained);
        cursor.skipPrefix(key, groupEnd);
        continue;
      }
      if (groupEnd > currentGroup.length) {
        currentGroup = Arrays.copyOf(currentGroup, Math.max(groupEnd, currentGroup.length << 1));
      }
      System.arraycopy(key, 0, currentGroup, 0, groupEnd);
      cursor.skipPrefixCapturingLast(key, groupEnd);
      if (cursor.lastSkippedKeyLength() != length) {
        return null;
      }
      cursor.copyLastSkippedKeyTo(key);
      if (key[groupEnd] != ProjectionSortKeyCodec.PRESENT) {
        return null;
      }
      final long groupMax = readOrderedLong(key, groupEnd + 1);
      retained = offer(currentGroup, prefix.length, groupEnd, groupMin, groupMax, order, spanDivisor, winners, minimums,
          maximums, scores, retained);
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  /**
   * Backfill this optional acceleration in the caller's transaction; commit remains caller-owned.
   *
   * <p>
   * Every live leaf is visited, so the pass also publishes an exact count of the rows with no
   * aggregate value — upgrading a view written before that count existed, which until then keeps
   * attempting the routes those rows defeat.
   * </p>
   */
  public static int buildLeafSummaries(final StorageEngineWriter writer, final int indexNumber) {
    Objects.requireNonNull(writer, "writer");
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(writer, indexNumber);
    if (directory == null) {
      return 0;
    }
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer, indexNumber);
    final ProjectionSortedLeafBounds.Updater bounds = new ProjectionSortedLeafBounds.Updater(storage);
    final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
    final ProjectionSortKeyCodec.Layout layout = directory.layout();
    int written = 0;
    long missingAggregateRows = 0;
    while (cursor.id() != 0) {
      final int id = cursor.id();
      final ProjectionSortedLeaf leaf = ProjectionSortedLeafStore.read(storage, id);
      if (leaf == null) {
        throw new IllegalStateException("missing sorted data leaf during summary build: " + id);
      }
      final ProjectionSortedLeaf summary = ProjectionSortedGroupSummary.encode(leaf, layout);
      if (summary != null) {
        storage.putBlob(ProjectionSortedGroupSummary.slot(id), summary.encodedBytes());
        written++;
      } else {
        storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(id));
        missingAggregateRows += ProjectionSortedGroupSummary.countMissingLastField(leaf, layout);
      }
      bounds.set(id, summary);
      cursor.advance();
    }
    bounds.flush();
    if (layout.groupsByLastLong()) {
      new ProjectionSortedDirectory.Editor(storage).publishMissingAggregateRows(missingAggregateRows);
    }
    return written;
  }

  /**
   * Visit summaries in ascending leaf-minimum order. Once an unseen leaf's minimum is greater than
   * the current (K+1)-th distinct group's minimum, it cannot change a winner or its tie cutline. A
   * group may cross many leaves, so each offer merges by exact encoded group identity.
   */
  static @Nullable List<Group> topKFromBounds(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit) {
    return topKFromBounds(reader, indexNumber, directory, NO_PREFIX, null, limit, SORTED_LOOKAHEAD, null);
  }

  static @Nullable List<Group> topKFromBounds(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final int lookahead,
      final @Nullable LookaheadStats stats) {
    return topKFromBounds(reader, indexNumber, directory, NO_PREFIX, null, limit, lookahead, stats);
  }

  /**
   * {@code lookahead} candidates' summaries are fetched together ahead of consumption; the break
   * test, the missing-summary return and every validation run when a candidate is CONSUMED, exactly
   * where the one-read-per-step loop ran them, so the decision sequence and the result are its own.
   * {@code stats}, when given, receives the lookahead accounting. Only the leaves that can hold
   * {@code prefix} are candidates; a boundary leaf's bound also covers its other groups, which only
   * makes it a weaker, still valid lower bound.
   */
  static @Nullable List<Group> topKFromBounds(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final byte[] prefix, final byte @Nullable [] upper,
      final int limit, final int lookahead, final @Nullable LookaheadStats stats) {
    if (lookahead < 1 || lookahead > 64) {
      throw new IllegalArgumentException("unsupported sorted lookahead: " + lookahead);
    }
    final int[] leafIds = directory.leafIds(prefix, upper, ProjectionSortedLeafBounds.MAX_CANDIDATE_LEAVES);
    if (leafIds == null) {
      return null;
    }
    final ProjectionSortedLeafBounds.Candidates candidates =
        ProjectionSortedLeafBounds.read(reader, indexNumber, leafIds);
    if (candidates == null) {
      return null;
    }
    final ProjectionSortKeyCodec.Layout layout = directory.layout();
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
    byte[] key = new byte[128];
    int retained = 0;
    final int[] order = candidates.order();
    final LookaheadStats accounting = stats != null
        ? stats
        : LOOKAHEAD_DIAG
            ? new LookaheadStats()
            : null;
    // The next `window` candidates' summaries, fetched in one batch (k trie descents on one reader,
    // one coalesced payload read) and consumed in order. A failed batch falls back to per-candidate
    // reads for that window: a candidate the serial loop would never reach must not fail the scan,
    // and one it would reach fails identically from its own read.
    final int window = Math.min(lookahead, order.length);
    final int[] windowLeafIds = window > 1
        ? new int[window]
        : null;
    final ProjectionSortedLeaf[] staged = window > 1
        ? new ProjectionSortedLeaf[window]
        : null;
    int stagedFrom = 0;
    int stagedCount = 0;
    try {
      for (int position = 0; position < order.length; position++) {
        final int candidate = order[position];
        final long boundMin = candidates.minimums()[candidate];
        if (retained == capacity && boundMin > scores[retained - 1]) {
          break;
        }
        ProjectionSortedLeaf summary = null;
        boolean fromStage = false;
        if (window > 1) {
          if (position >= stagedFrom + stagedCount) {
            stagedFrom = position;
            stagedCount = Math.min(window, order.length - position);
            for (int i = 0; i < stagedCount; i++) {
              windowLeafIds[i] = candidates.leafIds()[order[position + i]];
            }
            try {
              ProjectionSortedGroupSummary.readBatch(reader, indexNumber, windowLeafIds, 0, stagedCount, staged);
              if (accounting != null) {
                accounting.fetched += stagedCount;
              }
            } catch (final RuntimeException batchFailure) {
              Arrays.fill(staged, null);
              stagedCount = 0;
            }
          }
          if (position < stagedFrom + stagedCount) {
            summary = staged[position - stagedFrom];
            staged[position - stagedFrom] = null;
            fromStage = true;
          }
        }
        if (!fromStage) {
          summary = ProjectionSortedGroupSummary.read(reader, indexNumber, candidates.leafIds()[candidate]);
        }
        if (accounting != null) {
          accounting.charged++;
          if (fromStage) {
            accounting.consumed++;
          }
        }
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
          if (!layout.isGroup(key, length) || summary.payloadLength(row) != payload.length) {
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
          if (ProjectionSortKeyCodec.startsWith(key, length, prefix)) {
            retained =
                offerDistinctMinimum(key, prefix.length, length, min, winners, minimums, maximums, scores, retained);
          }
        }
        if (summary.rowCount() == 0 || actualMin != boundMin || actualMax != candidates.maximums()[candidate]) {
          throw new IllegalStateException("sorted leaf bounds disagree with their group summary");
        }
      }
    } finally {
      if (LOOKAHEAD_DIAG && accounting != null) {
        System.err.println("[sortedLookahead] bounds window=" + window + " " + accounting);
      }
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  private static int offerDistinctMinimum(final byte[] group, final int from, final int to, final long minimum,
      final byte[][] winners, final long[] minimums, final long[] maximums, final long[] scores, int retained) {
    for (int i = 0; i < retained; i++) {
      if (Arrays.equals(winners[i], 0, winners[i].length, group, from, to)) {
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
    return offer(group, from, to, minimum, minimum, Order.MIN_ASC, 1, winners, minimums, maximums, scores, retained);
  }

  static @Nullable List<Group> topKFromSummaries(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final Order order, final long spanDivisor,
      final boolean minOnly) {
    return topKFromSummaries(reader, indexNumber, directory, NO_PREFIX, null, limit, order, spanDivisor, minOnly, null,
        0, null);
  }

  /** Explicit worker count keeps parallel/serial equivalence tests independent of machine size. */
  static @Nullable List<Group> topKFromSummaries(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final int limit, final Order order, final long spanDivisor,
      final boolean minOnly, final @Nullable ParallelWalkReaders workerReaders, final int workers) {
    return topKFromSummaries(reader, indexNumber, directory, NO_PREFIX, null, limit, order, spanDivisor, minOnly,
        workerReaders, workers, null);
  }

  static @Nullable List<Group> topKFromSummaries(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final byte[] prefix, final byte @Nullable [] upper,
      final int limit, final Order order, final long spanDivisor, final boolean minOnly,
      final @Nullable ParallelWalkReaders workerReaders, final int workers) {
    return topKFromSummaries(reader, indexNumber, directory, prefix, upper, limit, order, spanDivisor, minOnly,
        workerReaders, workers, null);
  }

  /**
   * Fold the per-leaf group summaries of the leaves that can hold {@code prefix}, in key order.
   * Entries outside the prefix occur only in the two boundary leaves and are skipped.
   *
   * <p>
   * {@code decline}, when given, receives whether a decline through a missing summary also proves
   * that the full-key walk cannot serve this range; see {@link SummaryDecline}. Passing it is what
   * asks the window for the per-leaf range test, so a caller that cannot use the proof pays nothing.
   * </p>
   */
  static @Nullable List<Group> topKFromSummaries(final StorageEngineReader reader, final int indexNumber,
      final ProjectionSortedDirectory.Accessor directory, final byte[] prefix, final byte @Nullable [] upper,
      final int limit, final Order order, final long spanDivisor, final boolean minOnly,
      final @Nullable ParallelWalkReaders workerReaders, final int workers,
      final @Nullable SummaryDecline decline) {
    final ProjectionSortKeyCodec.Layout layout = directory.layout();
    final int capacity = limit + 1;
    final byte[][] winners = new byte[capacity][];
    final long[] minimums = new long[capacity];
    final long[] maximums = new long[capacity];
    final long[] scores = new long[capacity];
    final byte[] payload = new byte[ProjectionSortedGroupSummary.PAYLOAD_BYTES];
    byte[] key = new byte[128];
    byte[] currentGroup = new byte[128];
    int groupLength = -1;
    long groupMin = 0;
    long groupMax = 0;
    int retained = 0;
    final SummaryWindow summaries = new SummaryWindow(reader, indexNumber, directory.leaves(prefix, upper),
        workerReaders, workers, decline == null
            ? null
            : prefix, upper);
    while (summaries.hasNext()) {
      final ProjectionSortedLeaf summary = summaries.next();
      if (summary == null) {
        // Otherwise an old or partially backfilled revision, which still uses the full-key route.
        if (decline != null && summaries.lastLeafWasEntirelyInsideRange()) {
          decline.prove();
        }
        return null;
      }
      for (int row = 0; row < summary.rowCount(); row++) {
        final int length = summary.keyLength(row);
        if (length > key.length) {
          key = new byte[length];
        }
        summary.copyKeyTo(row, key);
        if (!layout.isGroup(key, length) || summary.payloadLength(row) != payload.length) {
          throw new IllegalStateException("invalid sorted group summary entry");
        }
        summary.copyPayloadTo(row, payload, 0);
        final long min = ProjectionIndexRowGroupCodec.getLongLE(payload, 0);
        final long max = ProjectionIndexRowGroupCodec.getLongLE(payload, Long.BYTES);
        if (min > max) {
          throw new IllegalStateException("invalid sorted group summary extrema");
        }
        if (!ProjectionSortKeyCodec.startsWith(key, length, prefix)) {
          continue;
        }
        final int comparison = groupLength < 0
            ? -1
            : Arrays.compareUnsigned(currentGroup, 0, groupLength, key, 0, length);
        if (comparison > 0) {
          throw new IllegalStateException("sorted group summaries are out of order");
        }
        if (comparison == 0) {
          groupMin = Math.min(groupMin, min);
          groupMax = Math.max(groupMax, max);
        } else {
          if (groupLength >= 0) {
            retained = offer(currentGroup, prefix.length, groupLength, groupMin, minOnly
                ? groupMin
                : groupMax, order, spanDivisor, winners, minimums, maximums, scores, retained);
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
    }
    if (groupLength >= 0) {
      retained = offer(currentGroup, prefix.length, groupLength, groupMin, minOnly
          ? groupMin
          : groupMax, order, spanDivisor, winners, minimums, maximums, scores, retained);
    }
    return finish(winners, minimums, maximums, scores, retained, limit);
  }

  /** Read at most 1,024 summaries (64 MiB of encoded payload) before folding them in key order. */
  private static final class SummaryWindow {
    private final StorageEngineReader reader;
    private final int indexNumber;
    private final ProjectionSortedDirectory.Accessor.LeafCursor cursor;
    private final @Nullable ParallelWalkReaders workerReaders;
    private final int workers;
    private final int[] leafIds;
    private final ProjectionSortedLeaf[] summaries;
    /** Non-null only when the caller wants the per-leaf range test; then the queried lower bound. */
    private final byte @Nullable [] prefix;
    private final byte @Nullable [] upperExclusive;
    private final boolean @Nullable [] entirelyInside;
    private byte[] firstKey = NO_KEY;
    private boolean lastEntirelyInside;
    private int position;
    private int size;

    SummaryWindow(final StorageEngineReader reader, final int indexNumber,
        final ProjectionSortedDirectory.Accessor.LeafCursor cursor, final @Nullable ParallelWalkReaders workerReaders,
        final int requestedWorkers, final byte @Nullable [] prefix, final byte @Nullable [] upperExclusive) {
      if (requestedWorkers < 0 || requestedWorkers > 8) {
        throw new IllegalArgumentException("invalid sorted-summary worker count: " + requestedWorkers);
      }
      this.reader = reader;
      this.indexNumber = indexNumber;
      this.cursor = cursor;
      this.workerReaders = workerReaders;
      this.workers = workerReaders == null || reader.hasTrxIntentLog()
          ? 0
          : requestedWorkers;
      final int capacity = workers < 2
          ? 1
          : 1024;
      this.leafIds = new int[capacity];
      this.summaries = new ProjectionSortedLeaf[capacity];
      this.prefix = prefix;
      this.upperExclusive = upperExclusive;
      this.entirelyInside = prefix == null
          ? null
          : new boolean[capacity];
    }

    boolean hasNext() {
      return position < size || cursor.id() != 0;
    }

    @Nullable
    ProjectionSortedLeaf next() {
      if (position == size) {
        fill();
      }
      final ProjectionSortedLeaf summary = summaries[position];
      summaries[position] = null;
      lastEntirelyInside = entirelyInside != null && entirelyInside[position];
      position++;
      return summary;
    }

    /**
     * Whether every row of the leaf {@link #next()} last returned lies inside the queried range.
     * Answered from the fence keys the directory already holds, so it reads nothing, and it errs
     * towards {@code false}: a range's first and last leaf are treated as boundaries even when they
     * happen to hold no row outside it.
     */
    boolean lastLeafWasEntirelyInsideRange() {
      return lastEntirelyInside;
    }

    private void fill() {
      size = 0;
      position = 0;
      while (size < leafIds.length && cursor.id() != 0) {
        final int at = size;
        leafIds[size++] = cursor.id();
        // The leaf's own first key decides the lower end; that another leaf of the range follows it
        // decides the upper end, because that leaf's first key is below the range's upper bound.
        final boolean insideLower = entirelyInside == null || startsWithPrefix();
        final boolean more = cursor.advance();
        if (entirelyInside != null) {
          entirelyInside[at] = insideLower && (upperExclusive == null || more);
        }
      }
      if (size == 0) {
        throw new IllegalStateException("sorted summary window is exhausted");
      }
      if (workers < 2 || size == 1) {
        summaries[0] = ProjectionSortedGroupSummary.read(reader, indexNumber, leafIds[0]);
        return;
      }
      final int lanes = Math.min(workers, size);
      final int revision = reader.getRevisionNumber();
      final AtomicReference<Throwable> failed = new AtomicReference<>();
      IntStream.range(0, lanes).parallel().forEach(worker -> {
        if (failed.get() != null) {
          return;
        }
        try {
          workerReaders.runWithReader(lane -> {
            if (lane.hasTrxIntentLog() || lane.getRevisionNumber() != revision) {
              throw new IllegalArgumentException("summary workers must read the same committed revision");
            }
            final int from = worker * size / lanes;
            final int to = (worker + 1) * size / lanes;
            if (BATCH_SUMMARIES) {
              ProjectionSortedGroupSummary.readBatch(lane, indexNumber, leafIds, from, to, summaries);
            } else {
              for (int at = from; at < to; at++) {
                summaries[at] = ProjectionSortedGroupSummary.read(lane, indexNumber, leafIds[at]);
              }
            }
          });
        } catch (final RuntimeException | Error failure) {
          failed.compareAndSet(null, failure);
        }
      });
      // The parallel terminal operation joins every started lane, including its reader cleanup.
      final Throwable failure = failed.get();
      if (failure instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (failure instanceof Error error) {
        throw error;
      }
    }

    /** Whether the cursor's current leaf begins at or after the queried prefix. */
    private boolean startsWithPrefix() {
      if (prefix.length == 0) {
        return true;
      }
      final int length = cursor.firstKeyLength();
      if (length > firstKey.length) {
        firstKey = new byte[length];
      }
      cursor.copyFirstKeyTo(firstKey);
      return ProjectionSortKeyCodec.startsWith(firstKey, length, prefix);
    }
  }

  static @Nullable List<Group> finish(final byte[][] winners, final long[] minimums, final long[] maximums,
      final long[] scores, final int retained, final int limit) {
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

  /** Offer the group {@code group[from, to)}, which excludes the query's equality prefix. */
  static int offer(final byte[] group, final int from, final int to, final long min, final long max, final Order order,
      final long divisor, final byte[][] winners, final long[] minimums, final long[] maximums, final long[] scores,
      final int retained) {
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
    while (at < retained && (order == Order.MIN_ASC
        ? scores[at] <= score
        : scores[at] >= score)) {
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
    winners[at] = Arrays.copyOfRange(group, from, to);
    minimums[at] = min;
    maximums[at] = max;
    scores[at] = score;
    return next;
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
