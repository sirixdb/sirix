package io.sirix.index.redblacktree.keyvalue;

import io.sirix.index.redblacktree.interfaces.References;
import io.sirix.utils.ToStringHelper;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * Text node-ID references.
 *
 * <p>
 * Two internal representations, invisible through the API:
 * </p>
 * <ul>
 * <li><b>Bitmap-backed</b> — a {@link Roaring64Bitmap}. Mutable and merge-friendly; used by index
 * mutation/build paths and persisted posting lists.</li>
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
  /**
   * The one authoritative representation: a {@link Roaring64Bitmap} or an exactly-sized
   * {@code long[]} sorted strictly ascending. A SINGLE field, and {@code volatile}, so the two
   * representations can never be observed half-swapped: materializing the bitmap publishes it with a
   * release, and every accessor reads the field once into a local. (Two fields plus a plain write
   * would let another thread see the compact array already cleared while the bitmap write is still
   * invisible.) Reads stay a plain load on x86/ARM; materialization is once per instance at most.
   */
  private volatile Object refs;

  /**
   * Handle on {@link #refs}, for the lock-free compact-to-bitmap promotion in {@link #getNodeKeys()}.
   */
  private static final VarHandle REFS;

  static {
    try {
      REFS = MethodHandles.lookup().findVarHandle(NodeReferences.class, "refs", Object.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Default constructor.
   */
  public NodeReferences() {
    refs = new Roaring64Bitmap();
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
   * <p>
   * The array must be EXACTLY sized: its length is the cardinality, so there is no separate count to
   * publish and no branch that silently copies instead of adopting — the contract is unconditional,
   * which is what a caller handing over a buffer needs it to be.
   *
   * @param keys the backing array, adopted; sorted strictly ascending over its whole length
   * @return references over the run
   */
  public static NodeReferences ofSortedArray(final long[] keys) {
    Objects.requireNonNull(keys, "keys");
    // The precondition is what containsSorted's binary search rests on, and this factory is public
    // and ADOPTS the caller's array — so violating it yields a posting list that silently reports
    // present keys as absent, with no exception anywhere. Note the order is UNSIGNED: a caller who
    // sorted with Arrays.sort (signed) and holds any high-bit-set key would otherwise be accepted
    // and then misbehave only for those keys.
    requireAscending(keys);
    return new NodeReferences(keys);
  }

  /**
   * {@link #ofSortedArray} for a run the caller must KEEP: copies rather than adopts, so the caller's
   * array stays its own.
   *
   * <p>
   * The one caller that needs this is the HOT lookup cache's hit path, which holds a SHARED stored
   * posting list and has to copy before handing it to a mutable {@code NodeReferences} anyway.
   * </p>
   *
   * <p>
   * The ordering is NOT re-checked here, and that is the difference from {@link #ofSortedArray}. The
   * check is O(n) in a serial {@code Long.compareUnsigned} chain, and this method sits on a memoized
   * READ path: re-deriving a property of an immutable, already-validated array on hit number one
   * million produces the same answer it produced on hit number one, at up to
   * {@code MAX_CACHED_NODE_KEYS} comparisons a time in front of a vectorizable {@code clone()}. The
   * check moved to the ADMISSION side, where it runs once per array ever cached — see
   * {@link #isSortedAscending} and its caller in {@code AbstractHOTIndexReader.memoize}.
   * {@link #ofSortedArray}, which ADOPTS an arbitrary caller's array, keeps its own check.
   * </p>
   *
   * <p>
   * The name carries the {@code Unchecked} suffix because that is the only thing standing between a
   * future caller and silent wrong answers: this is {@code public}, it validates nothing beyond null,
   * and an out-of-order run makes {@link #contains}'s unsigned binary search report PRESENT node keys
   * as absent, with no exception anywhere. A caller that cannot point at the
   * {@link #isSortedAscending} check that already covered its array wants {@link #ofSortedArray}.
   * </p>
   *
   * @param keys a run this class itself produced (via {@link #toSortedArray()}) and validated on the
   *        way in; must be sorted strictly ascending in UNSIGNED order
   * @return references over a private copy of the run
   */
  public static NodeReferences copyOfSortedUnchecked(final long[] keys) {
    Objects.requireNonNull(keys, "keys");
    return new NodeReferences(keys.clone());
  }

  /**
   * The ordering contract of {@link #ofSortedArray}, ANSWERED rather than thrown, so a producer whose
   * admission is optional can decline instead of failing — {@code AbstractHOTIndexReader.memoize}
   * refuses to cache a run it cannot vouch for rather than failing a query that already has its
   * answer. Exposed so that check is paid ONCE, on the write side, for a run then handed to
   * {@link #copyOfSortedUnchecked} many times.
   *
   * <p>
   * An {@code assert} was not enough: assertions are off in production, so a caller that sorted with
   * {@code Arrays.sort} (SIGNED) and holds a high-bit-set key would be accepted, and
   * {@link #contains}'s unsigned binary search would then report present node keys as ABSENT with no
   * exception anywhere.
   * </p>
   *
   * @param keys the run to test
   * @return {@code true} iff {@code keys} is strictly ascending in unsigned order
   */
  public static boolean isSortedAscending(final long[] keys) {
    Objects.requireNonNull(keys, "keys");
    // Delegates rather than repeating the loop, so the throwing and answering entry points cannot
    // come to enforce different contracts — which is what firstNonAscendingIndex's own javadoc
    // promises and what a second copy of the scan would quietly break. A divergence here is silent:
    // a run this accepted and requireAscending rejected would be memoized by the lookup cache and
    // then make contains()'s unsigned binary search report present node keys as absent.
    return firstNonAscendingIndex(keys) < 0;
  }

  /**
   * ONE scan behind every entry point's rejection, so no two of them can come to enforce different
   * contracts.
   *
   * @param keys the array to check
   * @throws IllegalArgumentException if {@code keys} is not strictly ascending unsigned
   */
  private static void requireAscending(final long[] keys) {
    final int offender = firstNonAscendingIndex(keys);
    if (offender >= 0) {
      throw new IllegalArgumentException("keys must be sorted strictly ascending by unsigned order; index " + offender
          + " (" + Long.toUnsignedString(keys[offender]) + ") is not below index " + (offender + 1) + " ("
          + Long.toUnsignedString(keys[offender + 1]) + ")");
    }
  }

  /**
   * @param keys the array to scan
   * @return the index whose successor does not exceed it, or {@code -1} when strictly ascending
   */
  private static int firstNonAscendingIndex(final long[] keys) {
    for (int i = 1; i < keys.length; i++) {
      if (Long.compareUnsigned(keys[i - 1], keys[i]) >= 0) {
        return i - 1;
      }
    }
    return -1;
  }

  private NodeReferences(final long[] compactKeys) {
    this.refs = compactKeys;
  }

  private NodeReferences(final Roaring64Bitmap nodeKeys, final boolean copy) {
    this.refs = copy
        ? nodeKeys.clone()
        : nodeKeys;
  }

  /**
   * The bitmap view, materializing (and caching) it from the compact representation on first call.
   * After materialization the bitmap is the single authoritative, MUTABLE set — the compact array is
   * dropped so the two can never diverge.
   */
  @Override
  public Roaring64Bitmap getNodeKeys() {
    final Object current = refs;
    if (current instanceof Roaring64Bitmap bitmap) {
      return bitmap;
    }
    final Roaring64Bitmap materialized = new Roaring64Bitmap();
    for (final long key : (long[]) current) {
      materialized.add(key);
    }
    // Publish lock-free rather than under a monitor: the promotion happens at most once per
    // instance, so a monitor here is pure inflation risk on a read path. The loser of a race
    // discards its own copy and returns the winner's, which is what actually matters — every
    // caller must end up mutating THE bitmap, never an orphan. The only transition this field
    // ever makes is long[] -> Roaring64Bitmap, so a non-matching witness is always the bitmap.
    final Object witness = REFS.compareAndExchange(this, current, (Object) materialized);
    return witness == current
        ? materialized
        : (Roaring64Bitmap) witness;
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
    final Object current = refs;
    return current instanceof long[] keys
        ? keys.length
        : ((Roaring64Bitmap) current).getLongCardinality();
  }

  /**
   * Iterate the referenced node keys in ascending order, without materializing a bitmap for compact
   * instances. The representation-independent twin of {@code getNodeKeys().getLongIterator()}.
   */
  public LongIterator nodeKeyIterator() {
    final Object current = refs;
    if (!(current instanceof long[] keys)) {
      return ((Roaring64Bitmap) current).getLongIterator();
    }
    final int count = keys.length;
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

  /**
   * The referenced node keys as a fresh, exactly-sized {@code long[]} in ascending unsigned order —
   * the array form {@link #ofSortedArray} accepts back.
   *
   * <p>
   * ONE read of {@link #refs} decides both the size and the contents, which is the whole point:
   * draining from outside the class costs a {@link #cardinality()} read plus a
   * {@link #forEachNodeKey} read, and those two can disagree if the instance is mutated in between —
   * leaving the caller to size an array from one answer and fill it from another. The compact case,
   * which is what the HOT read path produces, is a single {@code clone()}; the bitmap case defers to
   * Roaring's own bulk export instead of a per-key callback.
   * </p>
   *
   * @return a fresh array the caller owns, ascending unsigned; empty when there are no references
   */
  public long[] toSortedArray() {
    final Object current = refs;
    return current instanceof long[] keys
        ? keys.clone()
        : ((Roaring64Bitmap) current).toArray();
  }

  /** Visit every referenced node key in ascending order, without materializing a bitmap. */
  public void forEachNodeKey(final LongConsumer consumer) {
    final Object current = refs;
    if (current instanceof long[] keys) {
      for (final long key : keys) {
        consumer.accept(key);
      }
      return;
    }
    final LongIterator iterator = ((Roaring64Bitmap) current).getLongIterator();
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
    // Named `other`, not `refs`: a pattern variable called `refs` would shadow the instance field of
    // that name, so a later edit meaning "my own representation" would silently read the argument's.
    if (!(obj instanceof final NodeReferences other)) {
      return false;
    }
    if (cardinality() != other.cardinality()) {
      return false;
    }
    // Both iterate ascending, so paired iteration decides set equality without materializing.
    final LongIterator a = nodeKeyIterator();
    final LongIterator b = other.nodeKeyIterator();
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
    final Object current = refs;
    return current instanceof long[] keys
        ? keys.length > 0
        : !((Roaring64Bitmap) current).isEmpty();
  }

  @Override
  public boolean contains(long nodeKey) {
    final Object current = refs;
    return current instanceof long[] keys
        ? containsSorted(keys, nodeKey)
        : ((Roaring64Bitmap) current).contains(nodeKey);
  }

  /**
   * Binary search over the compact run, UNSIGNED. The array is strictly ascending by construction —
   * {@code ChunkAccumulator} spills to a bitmap the moment an append would break that — but its
   * ordering guard is {@link Long#compareUnsigned}, so the search has to agree with it. Matching
   * {@link Arrays#binarySearch}'s signed order instead would silently mislocate any key with the high
   * bit set; today's chunk expansion tops out at 48 bits, but that is the caller's arithmetic, not an
   * invariant this class can hold, and {@link #ofSortedArray} is public.
   */
  private static boolean containsSorted(final long[] keys, final long nodeKey) {
    int low = 0;
    int high = keys.length - 1;
    while (low <= high) {
      final int mid = (low + high) >>> 1;
      final int cmp = Long.compareUnsigned(keys[mid], nodeKey);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return true;
      }
    }
    return false;
  }
}
