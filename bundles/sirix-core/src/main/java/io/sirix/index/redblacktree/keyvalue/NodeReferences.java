package io.sirix.index.redblacktree.keyvalue;

import io.sirix.utils.ToStringHelper;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongConsumer;
import io.sirix.index.redblacktree.interfaces.References;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Set;

/**
 * Text node-ID references.
 *
 * <p>
 * Two internal representations, invisible through the API:
 * </p>
 * <ul>
 * <li><b>Bitmap-backed</b> — a {@link Roaring64Bitmap}, the historical form. Mutable, merge-
 * friendly; what the write path and the RB-tree backend build and serialize.</li>
 * <li><b>Compact</b> — a sorted-ascending {@code long[]} slice, produced by the HOT read path. The
 * average CAS posting list holds one or two node keys, and materializing a Roaring bitmap
 * (container tree + wrapper) per lookup result was the single largest remaining allocation on the
 * read path. A compact instance materializes its bitmap LAZILY on the first {@link #getNodeKeys()}
 * or mutation, so every existing consumer keeps working unchanged, while consumers using the
 * representation-independent accessors ({@link #nodeKeyIterator()},
 * {@link #forEachNodeKey(LongConsumer)}, {@link #cardinality()}, {@link #contains(long)}) never pay
 * for the bitmap at all.</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class NodeReferences implements References {
  /** Node keys as a bitmap; {@code null} while the compact representation is authoritative. */
  private @Nullable Roaring64Bitmap nodeKeys;

  /**
   * Compact representation: {@code compactKeys[0..compactCount)} sorted strictly ascending.
   * {@code null} when {@link #nodeKeys} is authoritative. Never exposed; adopted from
   * {@link #ofSortedArray(long[], int)} callers who hand over ownership.
   */
  private long @Nullable [] compactKeys;

  private int compactCount;

  /**
   * Default constructor.
   */
  public NodeReferences() {
    nodeKeys = new Roaring64Bitmap();
  }

  /**
   * Constructor taking a defensive copy of {@code nodeKeys}.
   *
   * <p>
   * Use this when the bitmap belongs to someone else — notably another {@code NodeReferences} reached
   * through {@link #getNodeKeys()}, which hands out the live set. Since {@link #addNodeKey} and
   * {@link #removeNodeKey} mutate in place, sharing the instance would let one reference set silently
   * rewrite another's.
   *
   * <p>
   * When the caller built the bitmap itself and nothing else can see it, the copy is pure waste on a
   * hot path — the index writer merges a reference set per indexed node, so an O(n) copy and a whole
   * duplicate bitmap allocation per merge is money burned. Use {@link #owning(Roaring64Bitmap)} there
   * instead.
   *
   * @param nodeKeys node keys, copied
   */
  public NodeReferences(final Roaring64Bitmap nodeKeys) {
    this(Objects.requireNonNull(nodeKeys, "nodeKeys"), true);
  }

  /**
   * Wrap a bitmap the caller is handing over, without copying it.
   *
   * <p>
   * The returned instance takes ownership: the caller must not retain the reference or mutate the
   * bitmap afterwards. Intended for freshly-built bitmaps that have not escaped — a deserialize
   * result, or a set merged together locally.
   *
   * @param nodeKeys node keys, adopted rather than copied
   * @return references over {@code nodeKeys}
   */
  public static NodeReferences owning(final Roaring64Bitmap nodeKeys) {
    return new NodeReferences(Objects.requireNonNull(nodeKeys, "nodeKeys"), false);
  }

  /**
   * Wrap a sorted run of node keys the caller is handing over, without building a bitmap.
   *
   * <p>
   * The returned instance takes ownership of {@code keys}: the caller must not retain or mutate the
   * array afterwards. {@code keys[0..count)} must be sorted strictly ascending — exactly what the HOT
   * chunk reassembly produces (chunkIdx-major, bit16-minor, duplicate-free by construction). The
   * bitmap view is created lazily only if some consumer insists on {@link #getNodeKeys()} or mutates
   * the set.
   *
   * @param keys the backing array, adopted; sorted strictly ascending in {@code [0, count)}
   * @param count number of live keys
   * @return references over the run
   */
  public static NodeReferences ofSortedArray(final long[] keys, final int count) {
    Objects.requireNonNull(keys, "keys");
    Objects.checkFromIndexSize(0, count, keys.length);
    final NodeReferences refs = new NodeReferences((Roaring64Bitmap) null, false);
    refs.compactKeys = keys;
    refs.compactCount = count;
    return refs;
  }

  private NodeReferences(final @Nullable Roaring64Bitmap nodeKeys, final boolean copy) {
    if (nodeKeys == null) {
      this.nodeKeys = null; // compact caller fills the array fields
    } else {
      this.nodeKeys = copy
          ? nodeKeys.clone()
          : nodeKeys;
    }
  }

  /**
   * The bitmap view, materializing (and caching) it from the compact representation on first call.
   * After materialization the bitmap is the single authoritative, MUTABLE set — the compact array is
   * dropped so the two can never diverge.
   */
  @Override
  public Roaring64Bitmap getNodeKeys() {
    Roaring64Bitmap bitmap = nodeKeys;
    if (bitmap == null) {
      bitmap = new Roaring64Bitmap();
      final long[] keys = compactKeys;
      for (int i = 0; i < compactCount; i++) {
        bitmap.add(keys[i]);
      }
      nodeKeys = bitmap;
      compactKeys = null;
      compactCount = 0;
    }
    return bitmap;
  }

  @Override
  public boolean isPresent(final long nodeKey) {
    return contains(nodeKey);
  }

  @Override
  public NodeReferences addNodeKey(final long nodeKey) {
    getNodeKeys().add(nodeKey);
    return this;
  }

  @Override
  public boolean removeNodeKey(long nodeKey) {
    final Roaring64Bitmap bitmap = getNodeKeys();
    boolean containsNodeKey = bitmap.contains(nodeKey);
    bitmap.removeLong(nodeKey);
    return containsNodeKey;
  }

  /** Number of referenced node keys, without materializing a bitmap. */
  public long cardinality() {
    final long[] keys = compactKeys;
    if (keys != null) {
      return compactCount;
    }
    return nodeKeys.getLongCardinality();
  }

  /**
   * Iterate the referenced node keys in ascending order, without materializing a bitmap for compact
   * instances. The representation-independent twin of {@code getNodeKeys().getLongIterator()}.
   */
  public LongIterator nodeKeyIterator() {
    final long[] keys = compactKeys;
    if (keys == null) {
      return nodeKeys.getLongIterator();
    }
    final int count = compactCount;
    return new LongIterator() {
      private int position;

      @Override
      public boolean hasNext() {
        return position < count;
      }

      @Override
      public long next() {
        return keys[position++];
      }

      @Override
      public LongIterator clone() {
        try {
          return (LongIterator) super.clone();
        } catch (CloneNotSupportedException e) {
          throw new IllegalStateException(e);
        }
      }
    };
  }

  /** Visit every referenced node key in ascending order, without materializing a bitmap. */
  public void forEachNodeKey(final LongConsumer consumer) {
    final long[] keys = compactKeys;
    if (keys != null) {
      final int count = compactCount;
      for (int i = 0; i < count; i++) {
        consumer.accept(keys[i]);
      }
      return;
    }
    final LongIterator iterator = nodeKeys.getLongIterator();
    while (iterator.hasNext()) {
      consumer.accept(iterator.next());
    }
  }

  @Override
  public int hashCode() {
    // Representation-independent: derived from the ascending key sequence, never from the
    // internal container structure, so a compact instance and its bitmap twin agree.
    long h = 1;
    final LongIterator iterator = nodeKeyIterator();
    while (iterator.hasNext()) {
      h = 31 * h + Long.hashCode(iterator.next());
    }
    return Long.hashCode(h);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (!(obj instanceof final NodeReferences refs)) {
      return false;
    }
    if (cardinality() != refs.cardinality()) {
      return false;
    }
    // Both iterate ascending, so paired iteration decides set equality without materializing.
    final LongIterator a = nodeKeyIterator();
    final LongIterator b = refs.nodeKeyIterator();
    while (a.hasNext()) {
      if (a.next() != b.next()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    final ToStringHelper helper = ToStringHelper.of(this);
    final LongIterator iterator = nodeKeyIterator();
    while (iterator.hasNext()) {
      final var nodeKey = iterator.next();
      helper.add("referenced node key", nodeKey);
    }
    return helper.toString();
  }

  @Override
  public boolean hasNodeKeys() {
    final long[] keys = compactKeys;
    if (keys != null) {
      return compactCount > 0;
    }
    return !nodeKeys.isEmpty();
  }

  @Override
  public boolean contains(long nodeKey) {
    final long[] keys = compactKeys;
    if (keys != null) {
      return Arrays.binarySearch(keys, 0, compactCount, nodeKey) >= 0;
    }
    return nodeKeys.contains(nodeKey);
  }
}
