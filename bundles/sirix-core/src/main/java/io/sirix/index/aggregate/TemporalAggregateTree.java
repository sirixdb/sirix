package io.sirix.index.aggregate;

import io.sirix.index.aggregate.TemporalAggregateSpec.Lane;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Sparse persistent segment tree with batched canonical-cover deltas. Each record is stored once.
 * Sum/count lanes use direct deltas; extrema lanes use value-frequency Patricia trees, sharing one
 * leaf among equal values. A commit rewrites each touched frequency subtree and segment once.
 */
public final class TemporalAggregateTree {
  /** Revision-bound slot storage. Implementations must provide read-your-writes. */
  public interface Store {
    byte @Nullable [] read(long segment, long prefix, int bit);

    void write(long segment, long prefix, int bit, byte[] value);
  }

  private static final int ROW_SLOT = -4;
  private static final int SEGMENT_SLOT = -3;
  private static final int EMPTY = -2;
  private static final int LEAF = -1;
  private static final int FORMAT = 2;
  private final Store store;
  private final int height;
  private final int lanes;
  private final boolean[] extrema;
  private final long sumMask;
  private final int summaryOffset;
  private final int segmentBytes;

  /** All operations, useful for callers supplying an explicit summary schema. */
  public TemporalAggregateTree(final Store store, final int height, final int lanes) {
    this(store, height, allExtrema(lanes), (1L << lanes) - 1);
  }

  public TemporalAggregateTree(final Store store, final int height, final List<Lane> lanes) {
    this(store, height, extrema(lanes), sumMask(lanes));
  }

  private TemporalAggregateTree(final Store store, final int height, final boolean[] extrema, final long sumMask) {
    this.store = Objects.requireNonNull(store);
    if (height < 2 || height > 62) {
      throw new IllegalArgumentException("Invalid aggregate tree height");
    }
    this.height = height;
    this.lanes = extrema.length;
    this.extrema = extrema;
    this.sumMask = sumMask;
    int roots = 0;
    for (final boolean enabled : extrema) {
      if (enabled) {
        roots++;
      }
    }
    summaryOffset = 28 + 12 * roots;
    segmentBytes = summaryOffset + Long.BYTES * (1 + 5 * lanes);
  }

  private static boolean[] allExtrema(final int lanes) {
    if (lanes < 0 || lanes > 16) {
      throw new IllegalArgumentException("Invalid aggregate lane count");
    }
    final boolean[] result = new boolean[lanes];
    Arrays.fill(result, true);
    return result;
  }

  private static boolean[] extrema(final List<Lane> lanes) {
    final boolean[] result = allExtrema(lanes.size());
    for (int i = 0; i < result.length; i++) {
      result[i] = lanes.get(i).hasExtrema();
    }
    return result;
  }

  private static long sumMask(final List<Lane> lanes) {
    long mask = 0;
    for (int i = 0; i < lanes.size(); i++) {
      if ((lanes.get(i).operations() & TemporalAggregateSpec.SUM) != 0) {
        mask |= 1L << i;
      }
    }
    return mask;
  }

  private record Row(long from, long to, long mask, long[] values) {
  }

  private record ValueDelta(long key, long delta) {
  }

  private record Ref(long prefix, int bit) {
  }

  private record Frequency(Ref ref, long first, long last, long count, @Nullable Ref left, @Nullable Ref right) {
  }

  private record Segment(long head, long left, long right, Ref[] roots, AggregateSummary summary,
      byte @Nullable [] encoded) {
  }

  private final class Change {
    private final Segment prior;
    private final Long2LongRBTreeMap[] values = new Long2LongRBTreeMap[lanes];

    private Change(final long segment) {
      prior = readSegment(segment);
    }

    private void adjust(final Row row, final int sign) {
      prior.summary().adjustRow(row.values(), row.mask(), sumMask, sign);
      for (int lane = 0; lane < lanes; lane++) {
        if (extrema[lane] && (row.mask() & (1L << lane)) != 0) {
          if (values[lane] == null) {
            values[lane] = new Long2LongRBTreeMap();
          }
          values[lane].addTo(row.values()[lane], sign);
        }
      }
    }
  }

  /** Commit-local final row replacements; persistence starts only in apply(). */
  public final class Batch {
    private final Long2ObjectRBTreeMap<Row> rows = new Long2ObjectRBTreeMap<>();
    private final Long2ObjectRBTreeMap<Change> changes = new Long2ObjectRBTreeMap<>();
    private boolean applied;

    /** Replace a record's interval and values. A null summary removes its previously stored row. */
    public void replace(final long recordKey, final long from, final long to,
        final @Nullable AggregateSummary value) {
      if (applied || recordKey < 0 || from < 0 || from >= to || to > (1L << height)
          || (value != null && (value.count() != 1 || value.lanes() != lanes))) {
        throw new IllegalArgumentException("Invalid temporal aggregate batch replacement");
      }
      if (value == null) {
        rows.put(recordKey, null);
        return;
      }
      final long[] values = new long[lanes];
      long mask = 0;
      for (int lane = 0; lane < lanes; lane++) {
        if (value.present(lane) != 0) {
          mask |= 1L << lane;
          values[lane] = value.sumLongExact(lane);
        }
      }
      rows.put(recordKey, new Row(from, to, mask, values));
    }

    private void cover(final Row row, final int sign) {
      final long base = 1L << height;
      long left = base + row.from();
      long right = base + row.to() - 1;
      while (left <= right) {
        if ((left & 1) != 0) {
          adjust(left++, row, sign);
        }
        if ((right & 1) == 0) {
          adjust(right--, row, sign);
        }
        left >>>= 1;
        right >>>= 1;
      }
    }

    private void adjust(final long segment, final Row row, final int sign) {
      Change change = changes.get(segment);
      if (change == null) {
        change = new Change(segment);
        changes.put(segment, change);
      }
      change.adjust(row, sign);
    }

    /** Sorted changes rebuild every touched subtree once, independent of event order. */
    public void apply() {
      if (applied) {
        throw new IllegalStateException("Temporal aggregate batch was already applied");
      }
      applied = true;
      for (final var entry : rows.long2ObjectEntrySet()) {
        final long key = entry.getLongKey();
        final Row old = readRow(key);
        final Row next = entry.getValue();
        if (old == null ? next == null : next != null && old.from() == next.from() && old.to() == next.to()
            && old.mask() == next.mask() && Arrays.equals(old.values(), next.values())) {
          continue;
        }
        if (old != null) {
          cover(old, -1);
        }
        if (next != null) {
          cover(next, 1);
        }
        writeRow(key, next);
      }
      final LongRBTreeSet pending = new LongRBTreeSet();
      for (final var entry : changes.long2ObjectEntrySet()) {
        final long segment = entry.getLongKey();
        final Change change = entry.getValue();
        for (int lane = 0; lane < lanes; lane++) {
          if (change.values[lane] != null) {
            final List<ValueDelta> edits = new ArrayList<>(change.values[lane].size());
            for (final var value : change.values[lane].long2LongEntrySet()) {
              if (value.getLongValue() != 0) {
                edits.add(new ValueDelta(value.getLongKey(), value.getLongValue()));
              }
            }
            if (!edits.isEmpty()) {
              final Ref prior = change.prior.roots()[lane];
              final Frequency root = merge(segment, lane, prior == null ? null : readFrequency(segment, lane, prior),
                  edits, 0, edits.size());
              if ((root == null ? 0 : root.count()) != change.prior.summary().present(lane)) {
                throw new IllegalStateException("Temporal aggregate frequency and presence counts differ");
              }
              change.prior.roots()[lane] = root == null ? null : root.ref();
              change.prior.summary().extrema(lane, root == null ? Long.MAX_VALUE : root.first(),
                  root == null ? Long.MIN_VALUE : root.last());
            }
          } else if (!extrema[lane]) {
            change.prior.summary().extrema(lane, 0, 0);
          }
        }
        pending.add(segment);
      }
      // Descendants have larger heap IDs, so every child is final before its parent is visited.
      // Parents store compressed child heads, not rolled-up summaries. A value-only change at a
      // stable head therefore requires no ancestor work; only a changed head propagates upward.
      while (!pending.isEmpty()) {
        final long id = pending.lastLong();
        pending.remove(id);
        final Change change = changes.get(id);
        final Segment previous = change == null ? readSegment(id) : change.prior;
        final boolean leaf = Long.SIZE - 1 - Long.numberOfLeadingZeros(id) == height;
        final long left = leaf ? 0 : readSegment(id << 1).head();
        final long right = leaf ? 0 : readSegment((id << 1) | 1).head();
        final long head = previous.summary().count() != 0 || (left != 0 && right != 0) ? id : left != 0 ? left : right;
        writeSegment(id, new Segment(head, left, right, previous.roots(), previous.summary(), previous.encoded()));
        if (id != 1 && head != previous.head()) {
          pending.add(id >>> 1);
        }
      }
    }
  }

  public Batch batch() {
    return new Batch();
  }

  /** Fold one compressed root-to-point path without reading any row or frequency tree. */
  public AggregateSummary point(final long point) {
    if (point < 0 || point >= (1L << height)) {
      throw new IllegalArgumentException("Aggregate point outside the domain");
    }
    final AggregateSummary result = new AggregateSummary(lanes);
    final byte[] root = store.read(1, 0, SEGMENT_SLOT);
    if (root == null) {
      return result;
    }
    long id = checked(root, segmentBytes).getLong();
    while (id != 0) {
      final int depth = Long.SIZE - 1 - Long.numberOfLeadingZeros(id);
      final int shift = height - depth;
      final long lower = (id - (1L << depth)) << shift;
      if (point < lower || point - lower >= (1L << shift)) {
        break;
      }
      final byte[] encoded = id == 1 ? root : Objects.requireNonNull(store.read(id, 0, SEGMENT_SLOT),
          "Missing temporal aggregate segment");
      final ByteBuffer node = checked(encoded, segmentBytes);
      node.getLong();
      final long left = node.getLong();
      final long right = node.getLong();
      node.position(summaryOffset);
      result.addEncoded(node);
      if (shift == 0) {
        break;
      }
      id = point - lower < (1L << (shift - 1)) ? left : right;
    }
    return result;
  }

  private @Nullable Frequency merge(final long segment, final int lane, final @Nullable Frequency old,
      final List<ValueDelta> edits, final int from, final int to) {
    if (from == to) {
      return old;
    }
    final long first = old == null ? edits.get(from).key() : Math.min(old.first(), edits.get(from).key());
    final long last = old == null ? edits.get(to - 1).key() : Math.max(old.last(), edits.get(to - 1).key());
    final int bit = Long.SIZE - 1 - Long.numberOfLeadingZeros(first ^ last);
    if (bit == LEAF) {
      final long count = Math.addExact(old == null ? 0 : old.count(), edits.get(from).delta());
      if (count < 0) {
        throw new IllegalStateException("Negative temporal aggregate value frequency");
      }
      if (count == 0) {
        return null;
      }
      final Frequency leaf = new Frequency(new Ref(first, LEAF), first, first, count, null, null);
      writeFrequency(segment, lane, leaf);
      return leaf;
    }
    // Signed order splits negative and nonnegative values at zero for the sign bit.
    final long prefix = bit == 63 ? 0 : first & (-1L << (bit + 1));
    final long boundary = bit == 63 ? 0 : prefix | (1L << bit);
    int lo = from;
    int hi = to;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (edits.get(mid).key() < boundary) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    final Frequency oldLeft;
    final Frequency oldRight;
    if (old != null && old.ref().bit() == bit) {
      oldLeft = readFrequency(segment, lane, Objects.requireNonNull(old.left()));
      oldRight = readFrequency(segment, lane, Objects.requireNonNull(old.right()));
    } else {
      oldLeft = old != null && old.first() < boundary ? old : null;
      oldRight = old != null && old.first() >= boundary ? old : null;
    }
    final Frequency left = merge(segment, lane, oldLeft, edits, from, lo);
    final Frequency right = merge(segment, lane, oldRight, edits, lo, to);
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    final Frequency combined = new Frequency(new Ref(prefix, bit), left.first(), right.last(),
        Math.addExact(left.count(), right.count()), left.ref(), right.ref());
    // References name stable logical slots, so changed descendants need no identical parent rewrite.
    if (combined.equals(old)) {
      return old;
    }
    writeFrequency(segment, lane, combined);
    return combined;
  }

  private @Nullable Row readRow(final long key) {
    final byte[] bytes = store.read(0, key, ROW_SLOT);
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    final ByteBuffer buffer = checked(bytes, 28 + 8 * lanes);
    final long from = buffer.getLong();
    final long to = buffer.getLong();
    final long mask = buffer.getLong();
    final long[] values = new long[lanes];
    for (int lane = 0; lane < lanes; lane++) {
      values[lane] = buffer.getLong();
    }
    return new Row(from, to, mask, values);
  }

  private void writeRow(final long key, final @Nullable Row row) {
    if (row == null) {
      store.write(0, key, ROW_SLOT, new byte[0]);
      return;
    }
    final ByteBuffer buffer = ByteBuffer.allocate(28 + 8 * lanes);
    buffer.putInt(FORMAT).putLong(row.from()).putLong(row.to()).putLong(row.mask());
    for (final long value : row.values()) {
      buffer.putLong(value);
    }
    store.write(0, key, ROW_SLOT, buffer.array());
  }

  private Segment readSegment(final long id) {
    final byte[] bytes = store.read(id, 0, SEGMENT_SLOT);
    if (bytes == null) {
      return new Segment(0, 0, 0, new Ref[lanes], new AggregateSummary(lanes), null);
    }
    final ByteBuffer buffer = checked(bytes, segmentBytes);
    final long head = buffer.getLong();
    final long left = buffer.getLong();
    final long right = buffer.getLong();
    final Ref[] roots = new Ref[lanes];
    for (int lane = 0; lane < lanes; lane++) {
      if (extrema[lane]) {
        roots[lane] = readRef(buffer);
      }
    }
    return new Segment(head, left, right, roots, AggregateSummary.read(buffer, lanes), bytes);
  }

  private void writeSegment(final long id, final Segment segment) {
    final ByteBuffer buffer = ByteBuffer.allocate(segmentBytes);
    buffer.putInt(FORMAT).putLong(segment.head()).putLong(segment.left()).putLong(segment.right());
    for (int lane = 0; lane < lanes; lane++) {
      if (extrema[lane]) {
        writeRef(buffer, segment.roots()[lane]);
      }
    }
    segment.summary().write(buffer);
    if (!Arrays.equals(segment.encoded(), buffer.array())) {
      store.write(id, 0, SEGMENT_SLOT, buffer.array());
    }
  }

  private Frequency readFrequency(final long segment, final int lane, final Ref ref) {
    final byte[] bytes = Objects.requireNonNull(store.read(segment, ref.prefix(), lane * 65 + ref.bit() + 1),
        "Missing temporal aggregate frequency node");
    final ByteBuffer buffer = checked(bytes, 52);
    return new Frequency(ref, buffer.getLong(), buffer.getLong(), buffer.getLong(), readRef(buffer), readRef(buffer));
  }

  private void writeFrequency(final long segment, final int lane, final Frequency node) {
    final ByteBuffer buffer = ByteBuffer.allocate(52);
    buffer.putInt(FORMAT).putLong(node.first()).putLong(node.last()).putLong(node.count());
    writeRef(buffer, node.left());
    writeRef(buffer, node.right());
    store.write(segment, node.ref().prefix(), lane * 65 + node.ref().bit() + 1, buffer.array());
  }

  private static ByteBuffer checked(final byte[] bytes, final int length) {
    if (bytes.length != length) {
      throw new IllegalStateException("Invalid temporal aggregate node length");
    }
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    if (buffer.getInt() != FORMAT) {
      throw new IllegalStateException("Unknown temporal aggregate node format");
    }
    return buffer;
  }

  private static @Nullable Ref readRef(final ByteBuffer buffer) {
    final long prefix = buffer.getLong();
    final int bit = buffer.getInt();
    if (bit < EMPTY || bit > 63) {
      throw new IllegalStateException("Invalid temporal aggregate frequency reference");
    }
    return bit == EMPTY ? null : new Ref(prefix, bit);
  }

  private static void writeRef(final ByteBuffer buffer, final @Nullable Ref ref) {
    buffer.putLong(ref == null ? 0 : ref.prefix()).putInt(ref == null ? EMPTY : ref.bit());
  }
}
