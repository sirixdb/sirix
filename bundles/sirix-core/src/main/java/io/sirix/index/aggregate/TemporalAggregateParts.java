package io.sirix.index.aggregate;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable group parts: endpoint prefix sums and compact, signed extrema-frequency vectors. */
public final class TemporalAggregateParts {
  public static final int CHUNK_BYTES = 2048;
  public static final int MAX_PARTS = 8;
  private static final VarHandle LONG_BE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final int MAGIC = 0x54415031;
  private static final int HEADER_BYTES = 32;
  private static final int CANDIDATE_LIMIT = 32;

  @FunctionalInterface
  public interface Store {
    byte[] read(long part, int chunk);
  }

  private final TemporalAggregateSpec spec;
  private final int height;
  private final int lanes;
  private final int sumsBytes;
  private final int nodeBytes;
  private final int[] frequencyOffsets;

  public TemporalAggregateParts(final TemporalAggregateSpec spec, final int height) {
    this.spec = Objects.requireNonNull(spec);
    if (height < 2 || height > 62) {
      throw new IllegalArgumentException("Invalid aggregate domain height");
    }
    this.height = height;
    lanes = spec.lanes().size();
    int sums = Long.BYTES;
    int node = 16;
    frequencyOffsets = new int[lanes];
    Arrays.fill(frequencyOffsets, -1);
    for (int lane = 0; lane < lanes; lane++) {
      sums += Long.BYTES;
      if (hasSum(lane)) {
        sums += 2 * Long.BYTES;
      }
      if (spec.lanes().get(lane).hasExtrema()) {
        frequencyOffsets[lane] = node;
        node += 16;
      }
    }
    sumsBytes = sums;
    nodeBytes = node;
  }

  private boolean hasSum(final int lane) {
    return (spec.lanes().get(lane).operations() & TemporalAggregateSpec.SUM) != 0;
  }

  /** Signed prefix arithmetic; only a final checkpoint or query result needs nonnegative counts. */
  private final class Sums {
    private final long[] data = new long[1 + 3 * lanes];

    private void row(final long[] values, final long mask, final int sign) {
      data[0] = Math.addExact(data[0], sign);
      for (int lane = 0; lane < lanes; lane++) {
        if ((mask & (1L << lane)) == 0) {
          continue;
        }
        final int offset = 1 + 3 * lane;
        data[offset] = Math.addExact(data[offset], sign);
        if (hasSum(lane)) {
          final long value = values[lane];
          addWide(offset + 1, sign > 0 ? value : -value,
              sign > 0 ? (value < 0 ? -1 : 0) : (value == Long.MIN_VALUE ? 0 : value > 0 ? -1 : 0));
        }
      }
    }

    private void addWide(final int offset, final long low, final long high) {
      final long previous = data[offset];
      data[offset] += low;
      data[offset + 1] += high + (Long.compareUnsigned(data[offset], previous) < 0 ? 1 : 0);
    }

    private void add(final Sums other, final boolean subtract) {
      data[0] = subtract ? Math.subtractExact(data[0], other.data[0]) : Math.addExact(data[0], other.data[0]);
      for (int lane = 0; lane < lanes; lane++) {
        final int offset = 1 + 3 * lane;
        data[offset] = subtract ? Math.subtractExact(data[offset], other.data[offset])
            : Math.addExact(data[offset], other.data[offset]);
        if (subtract) {
          final long low = -other.data[offset + 1];
          addWide(offset + 1, low, ~other.data[offset + 2] + (low == 0 ? 1 : 0));
        } else {
          addWide(offset + 1, other.data[offset + 1], other.data[offset + 2]);
        }
      }
    }

    private Sums copy() {
      final Sums result = new Sums();
      System.arraycopy(data, 0, result.data, 0, data.length);
      return result;
    }

    private void validate() {
      if (data[0] < 0) {
        throw new IllegalStateException("Negative aggregate checkpoint count");
      }
      for (int lane = 0; lane < lanes; lane++) {
        final long present = data[1 + 3 * lane];
        if (present < 0 || present > data[0]) {
          throw new IllegalStateException("Invalid aggregate checkpoint presence");
        }
      }
    }

    private void write(final ByteBuffer buffer) {
      buffer.putLong(data[0]);
      for (int lane = 0; lane < lanes; lane++) {
        final int offset = 1 + 3 * lane;
        buffer.putLong(data[offset]);
        if (hasSum(lane)) {
          buffer.putLong(data[offset + 1]).putLong(data[offset + 2]);
        }
      }
    }
  }

  private static final class Node {
    private final long id;
    private final Long2LongRBTreeMap[] frequencies;
    private long head;
    private long left;
    private long right;

    private Node(final long id, final int lanes) {
      this.id = id;
      frequencies = new Long2LongRBTreeMap[lanes];
    }

    private boolean hasValues() {
      for (final Long2LongRBTreeMap values : frequencies) {
        if (values != null && !values.isEmpty()) {
          return true;
        }
      }
      return false;
    }
  }

  /** Commit-local builder. All row changes are combined before any part is persisted. */
  public final class Builder {
    private final Long2ObjectRBTreeMap<Sums> endpoints = new Long2ObjectRBTreeMap<>();
    private final Long2ObjectRBTreeMap<Node> nodes = new Long2ObjectRBTreeMap<>();

    public void add(final long from, final long to, final long[] values, final long mask, final int sign) {
      if (from < 0 || from >= to || to > (1L << height) || values.length != lanes
          || (mask >>> lanes) != 0 || (sign != -1 && sign != 1)) {
        throw new IllegalArgumentException("Invalid temporal aggregate interval delta");
      }
      endpoint(from).row(values, mask, sign);
      endpoint(to).row(values, mask, -sign);
      boolean extrema = false;
      for (int lane = 0; lane < lanes; lane++) {
        extrema |= frequencyOffsets[lane] >= 0 && (mask & (1L << lane)) != 0;
      }
      if (!extrema) {
        return;
      }
      long left = (1L << height) + from;
      long right = (1L << height) + to - 1;
      while (left <= right) {
        if ((left & 1) != 0) {
          frequencies(left++, values, mask, sign);
        }
        if ((right & 1) == 0) {
          frequencies(right--, values, mask, sign);
        }
        left >>>= 1;
        right >>>= 1;
      }
    }

    private Sums endpoint(final long point) {
      Sums sums = endpoints.get(point);
      if (sums == null) {
        sums = new Sums();
        endpoints.put(point, sums);
      }
      return sums;
    }

    private Node node(final long id) {
      Node node = nodes.get(id);
      if (node == null) {
        node = new Node(id, lanes);
        nodes.put(id, node);
      }
      return node;
    }

    private void frequency(final Node node, final int lane, final long value, final long delta) {
      Long2LongRBTreeMap values = node.frequencies[lane];
      if (values == null) {
        values = new Long2LongRBTreeMap();
        node.frequencies[lane] = values;
      }
      final long count = Math.addExact(values.get(value), delta);
      if (count == 0) {
        values.remove(value);
      } else {
        values.put(value, count);
      }
    }

    private void frequencies(final long id, final long[] values, final long mask, final long delta) {
      final Node node = node(id);
      for (int lane = 0; lane < lanes; lane++) {
        if (frequencyOffsets[lane] >= 0 && (mask & (1L << lane)) != 0) {
          frequency(node, lane, values[lane], delta);
        }
      }
    }

    /** Fold an immutable prior part, used only when the bounded part list needs a checkpoint. */
    public void merge(final Store store, final long partId) {
      final Part part = new Part(store, partId);
      Sums previous = new Sums();
      for (int i = 0; i < part.endpointCount; i++) {
        final Sums current = part.sums(i);
        final Sums difference = current.copy();
        difference.add(previous, true);
        endpoint(part.point(i)).add(difference, false);
        previous = current;
      }
      for (int index = 0; index < part.nodeCount; index++) {
        final int offset = part.nodeOffset(index);
        final long id = part.longAt(offset);
        Node target = null;
        for (int lane = 0; lane < lanes; lane++) {
          if (frequencyOffsets[lane] < 0) {
            continue;
          }
          final Vector vector = part.vector(index, lane);
          if (vector == null) {
            continue;
          }
          if (target == null) {
            target = node(id);
          }
          long previousCount = 0;
          for (int at = 0; at < vector.size; at++) {
            final long prefix = vector.prefix(at);
            frequency(target, lane, vector.value(at), Math.subtractExact(prefix, previousCount));
            previousCount = prefix;
          }
        }
      }
    }

    /** Whether this publication cancels completely before any persistent work is needed. */
    public boolean isEmpty() {
      for (final Sums sums : endpoints.values()) {
        for (final long value : sums.data) {
          if (value != 0) {
            return false;
          }
        }
      }
      for (final Node node : nodes.values()) {
        if (node.hasValues()) {
          return false;
        }
      }
      return true;
    }

    public byte[] encode(final boolean checkpoint) {
      final List<Long> points = new ArrayList<>(endpoints.size());
      final List<Sums> prefixes = new ArrayList<>(endpoints.size());
      final Sums accumulated = new Sums();
      long[] previous = new long[accumulated.data.length];
      for (final var entry : endpoints.long2ObjectEntrySet()) {
        accumulated.add(entry.getValue(), false);
        if (checkpoint) {
          accumulated.validate();
        }
        if (!Arrays.equals(previous, accumulated.data)) {
          points.add(entry.getLongKey());
          final Sums prefix = accumulated.copy();
          prefixes.add(prefix);
          previous = prefix.data;
        }
      }
      final long[] existing = nodes.keySet().toLongArray();
      for (final long id : existing) {
        if (!nodes.get(id).hasValues()) {
          nodes.remove(id);
          continue;
        }
        for (long parent = id >>> 1; parent != 0; parent >>>= 1) {
          node(parent);
        }
      }
      final long[] ordered = nodes.keySet().toLongArray();
      for (int at = ordered.length - 1; at >= 0; at--) {
        final Node node = nodes.get(ordered[at]);
        final boolean leaf = Long.SIZE - 1 - Long.numberOfLeadingZeros(node.id) == height;
        final Node left = leaf ? null : nodes.get(node.id << 1);
        final Node right = leaf ? null : nodes.get((node.id << 1) | 1);
        node.left = left == null ? 0 : left.head;
        node.right = right == null ? 0 : right.head;
        node.head = node.hasValues() || (node.left != 0 && node.right != 0) ? node.id
            : node.left != 0 ? node.left : node.right;
      }
      final List<Node> compact = new ArrayList<>(nodes.size());
      final Long2IntOpenHashMap positions = new Long2IntOpenHashMap(nodes.size());
      positions.defaultReturnValue(-1);
      for (final Node node : nodes.values()) {
        if (node.head == node.id) {
          positions.put(node.id, compact.size());
          compact.add(node);
        }
      }
      final int nodeStart = Math.addExact(HEADER_BYTES, Math.multiplyExact(points.size(), Long.BYTES + sumsBytes));
      final int valuesStart = Math.addExact(nodeStart, Math.multiplyExact(compact.size(), nodeBytes));
      int length = valuesStart;
      final Map<FrequencyValues, Integer> unique = new HashMap<>();
      final FrequencyValues[][] vectors = new FrequencyValues[compact.size()][lanes];
      for (int index = 0; index < compact.size(); index++) {
        for (int lane = 0; lane < lanes; lane++) {
          final Long2LongRBTreeMap values = compact.get(index).frequencies[lane];
          if (values == null || values.isEmpty()) {
            continue;
          }
          final long[] encoded = new long[Math.multiplyExact(values.size(), 2)];
          int at = 0;
          long count = 0;
          for (final var entry : values.long2LongEntrySet()) {
            if (checkpoint && entry.getLongValue() < 0) {
              throw new IllegalStateException("Negative checkpoint value frequency");
            }
            count = Math.addExact(count, entry.getLongValue());
            encoded[at++] = entry.getLongKey();
            encoded[at++] = count;
          }
          final FrequencyValues vector = new FrequencyValues(encoded);
          vectors[index][lane] = vector;
          if (!unique.containsKey(vector)) {
            unique.put(vector, length);
            length = Math.addExact(length, Math.multiplyExact(encoded.length, Long.BYTES));
          }
        }
      }
      final ByteBuffer buffer = ByteBuffer.allocate(length);
      final Node root = nodes.get(1);
      buffer.putInt(MAGIC).putInt(height).putLong(length).putInt(points.size()).putInt(compact.size())
          .putInt(root == null ? -1 : positions.get(root.head)).putInt(lanes);
      for (final long point : points) {
        buffer.putLong(point);
      }
      for (final Sums prefix : prefixes) {
        prefix.write(buffer);
      }
      for (int index = 0; index < compact.size(); index++) {
        final Node node = compact.get(index);
        buffer.putLong(node.id).putInt(positions.get(node.left)).putInt(positions.get(node.right));
        for (int lane = 0; lane < lanes; lane++) {
          if (frequencyOffsets[lane] >= 0) {
            final FrequencyValues vector = vectors[index][lane];
            buffer.putLong(vector == null ? 0 : unique.get(vector)).putLong(vector == null ? 0 : vector.values.length / 2);
          }
        }
      }
      for (final var entry : unique.entrySet()) {
        buffer.position(entry.getValue());
        for (final long value : entry.getKey().values) {
          buffer.putLong(value);
        }
      }
      return buffer.array();
    }
  }

  private static final class FrequencyValues {
    private final long[] values;
    private final int hash;

    private FrequencyValues(final long[] values) {
      this.values = values;
      hash = Arrays.hashCode(values);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof FrequencyValues vector && Arrays.equals(values, vector.values);
    }
  }

  private final class Part {
    private final Store store;
    private final long id;
    private final Int2ObjectOpenHashMap<byte[]> chunks = new Int2ObjectOpenHashMap<>();
    private final int length;
    private final int endpointCount;
    private final int nodeCount;
    private final int nodeStart;
    private final int root;

    private Part(final Store store, final long id) {
      this.store = store;
      this.id = id;
      final byte[] first = Objects.requireNonNull(store.read(id, 0), "Missing temporal aggregate part");
      if (first.length < HEADER_BYTES) {
        throw new IllegalStateException("Truncated temporal aggregate part");
      }
      chunks.put(0, first);
      final ByteBuffer header = ByteBuffer.wrap(first);
      if (header.getInt() != MAGIC || header.getInt() != height) {
        throw new IllegalStateException("Invalid temporal aggregate part format/domain");
      }
      length = Math.toIntExact(header.getLong());
      endpointCount = header.getInt();
      nodeCount = header.getInt();
      root = header.getInt();
      if (header.getInt() != lanes || length < HEADER_BYTES || endpointCount < 0 || nodeCount < 0
          || root < -1 || root >= nodeCount || first.length != Math.min(length, CHUNK_BYTES)) {
        throw new IllegalStateException("Invalid temporal aggregate part dimensions");
      }
      nodeStart = Math.addExact(HEADER_BYTES, Math.multiplyExact(endpointCount, Long.BYTES + sumsBytes));
      if (Math.addExact(nodeStart, Math.multiplyExact(nodeCount, nodeBytes)) > length) {
        throw new IllegalStateException("Truncated temporal aggregate node directory");
      }
    }

    private byte[] at(final int offset, final int width) {
      if (offset < 0 || offset > length - width || (offset & (width - 1)) != 0) {
        throw new IllegalStateException("Invalid temporal aggregate part offset");
      }
      final int chunk = offset / CHUNK_BYTES;
      byte[] bytes = chunks.get(chunk);
      if (bytes == null) {
        bytes = Objects.requireNonNull(store.read(id, chunk), "Missing temporal aggregate chunk");
        if (bytes.length != Math.min(CHUNK_BYTES, length - chunk * CHUNK_BYTES)) {
          throw new IllegalStateException("Invalid temporal aggregate chunk length");
        }
        chunks.put(chunk, bytes);
      }
      return bytes;
    }

    private long longAt(final int offset) {
      return (long) LONG_BE.get(at(offset, Long.BYTES), offset % CHUNK_BYTES);
    }

    private int intAt(final int offset) {
      return (int) INT_BE.get(at(offset, Integer.BYTES), offset % CHUNK_BYTES);
    }

    private long point(final int index) {
      return longAt(HEADER_BYTES + index * Long.BYTES);
    }

    private Sums sums(final int index) {
      int offset = HEADER_BYTES + endpointCount * Long.BYTES + index * sumsBytes;
      final Sums result = new Sums();
      result.data[0] = longAt(offset);
      offset += Long.BYTES;
      for (int lane = 0; lane < lanes; lane++) {
        final int target = 1 + 3 * lane;
        result.data[target] = longAt(offset);
        offset += Long.BYTES;
        if (hasSum(lane)) {
          result.data[target + 1] = longAt(offset);
          result.data[target + 2] = longAt(offset + Long.BYTES);
          offset += 2 * Long.BYTES;
        }
      }
      return result;
    }

    private int nodeOffset(final int index) {
      if (index < 0 || index >= nodeCount) {
        throw new IllegalStateException("Invalid temporal aggregate node index");
      }
      return nodeStart + index * nodeBytes;
    }

    private @Nullable Vector vector(final int index, final int lane) {
      final int offset = nodeOffset(index) + frequencyOffsets[lane];
      final int start = Math.toIntExact(longAt(offset));
      final int size = Math.toIntExact(longAt(offset + Long.BYTES));
      if (size < 0 || (size > 0 && (start < nodeStart + nodeCount * nodeBytes
          || start > length - Math.multiplyExact(size, 16)))) {
        throw new IllegalStateException("Invalid temporal aggregate frequency vector");
      }
      return size == 0 ? null : new Vector(this, start, size);
    }

    private void point(final long point, final Sums sums, final List<List<Vector>> vectors) {
      int low = 0;
      int high = endpointCount;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        if (point(mid) <= point) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      if (low != 0) {
        sums.add(sums(low - 1), false);
      }
      int index = root;
      for (int visited = 0; index >= 0; visited++) {
        if (visited > height) {
          throw new IllegalStateException("Cyclic temporal aggregate part");
        }
        final int offset = nodeOffset(index);
        final long id = longAt(offset);
        final int depth = Long.SIZE - 1 - Long.numberOfLeadingZeros(id);
        final int shift = height - depth;
        if (id <= 0 || shift < 0) {
          throw new IllegalStateException("Invalid temporal aggregate segment identity");
        }
        final long lower = (id - (1L << depth)) << shift;
        if (point < lower || point - lower >= (1L << shift)) {
          return;
        }
        for (int lane = 0; lane < lanes; lane++) {
          if (frequencyOffsets[lane] >= 0) {
            final Vector vector = vector(index, lane);
            if (vector != null) {
              vectors.get(lane).add(vector);
            }
          }
        }
        if (shift == 0) {
          return;
        }
        index = intAt(offset + (point - lower < (1L << (shift - 1)) ? 8 : 12));
      }
    }
  }

  private record Vector(Part part, int start, int size) {
    private long value(final int index) {
      return part.longAt(start + 16 * index);
    }

    private long prefix(final int index) {
      return part.longAt(start + 16 * index + Long.BYTES);
    }

    private long frequency(final int index) {
      return Math.subtractExact(prefix(index), index == 0 ? 0 : prefix(index - 1));
    }

    private long through(final long value) {
      int low = 0;
      int high = size;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        if (value(mid) <= value) {
          low = mid + 1;
        } else {
          high = mid;
        }
      }
      return low == 0 ? 0 : prefix(low - 1);
    }
  }

  public Builder builder() {
    return new Builder();
  }

  public AggregateSummary point(final Store store, final long[] parts, final long point) {
    if (parts.length > MAX_PARTS || point < 0 || point >= (1L << height)) {
      throw new IllegalArgumentException("Invalid aggregate point or part count");
    }
    final Sums sums = new Sums();
    final List<List<Vector>> vectors = new ArrayList<>(lanes);
    for (int lane = 0; lane < lanes; lane++) {
      vectors.add(new ArrayList<>());
    }
    for (final long id : parts) {
      new Part(store, id).point(point, sums, vectors);
    }
    sums.validate();
    final long[] minimum = new long[lanes];
    final long[] maximum = new long[lanes];
    for (int lane = 0; lane < lanes; lane++) {
      final long present = sums.data[1 + 3 * lane];
      if (present == 0) {
        minimum[lane] = Long.MAX_VALUE;
        maximum[lane] = Long.MIN_VALUE;
      } else if (frequencyOffsets[lane] >= 0) {
        minimum[lane] = extreme(vectors.get(lane), present, false);
        maximum[lane] = extreme(vectors.get(lane), present, true);
      }
    }
    return AggregateSummary.fromPacked(sums.data, minimum, maximum);
  }

  private static long extreme(final List<Vector> vectors, final long present, final boolean maximum) {
    final int[] positions = new int[vectors.size()];
    if (maximum) {
      for (int i = 0; i < positions.length; i++) {
        positions[i] = vectors.get(i).size - 1;
      }
    }
    for (int attempt = 0; attempt < CANDIDATE_LIMIT; attempt++) {
      long candidate = maximum ? Long.MIN_VALUE : Long.MAX_VALUE;
      boolean found = false;
      for (int i = 0; i < positions.length; i++) {
        final Vector vector = vectors.get(i);
        if (positions[i] >= 0 && positions[i] < vector.size) {
          final long value = vector.value(positions[i]);
          candidate = maximum ? Math.max(candidate, value) : Math.min(candidate, value);
          found = true;
        }
      }
      if (!found) {
        throw new IllegalStateException("Missing live temporal aggregate extremum");
      }
      long count = 0;
      for (int i = 0; i < positions.length; i++) {
        final Vector vector = vectors.get(i);
        if (positions[i] >= 0 && positions[i] < vector.size && vector.value(positions[i]) == candidate) {
          count = Math.addExact(count, vector.frequency(positions[i]));
          positions[i] += maximum ? -1 : 1;
        }
      }
      if (count < 0) {
        throw new IllegalStateException("Negative temporal aggregate candidate frequency");
      }
      if (count != 0) {
        return candidate;
      }
    }
    // Long deletion runs use logarithmic weighted selection instead of scanning removed values.
    long low = Long.MAX_VALUE;
    long high = Long.MIN_VALUE;
    for (final Vector vector : vectors) {
      low = Math.min(low, vector.value(0));
      high = Math.max(high, vector.value(vector.size - 1));
    }
    final long rank = maximum ? present : 1;
    while (low < high) {
      final long mid = (low & high) + ((low ^ high) >> 1);
      long count = 0;
      for (final Vector vector : vectors) {
        count = Math.addExact(count, vector.through(mid));
      }
      if (count >= rank) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    return low;
  }
}
