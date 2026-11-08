/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.compiler.translator;

import io.sirix.node.SegmentAccess;
import io.sirix.query.scan.DerivedCacheBudget;
import io.sirix.query.scan.MaskedColumns;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

/**
 * The kernels of the dense extrema join ({@link SirixJoinedGroupAggregateExpr}): one side's lanes
 * captured per leaf, the counting sort that packs a side by join key into one allocator slab, the
 * flat fold of overlapping pairs, the merge of two packed sides and the chained fold of streamed
 * lanes against a packed build side. Every read is a segment read through the typed handle
 * {@link #SLAB_LONG} at a base hoisted once per leaf or per bucket plus a linear offset, never a
 * heap copy; a full mask word (64 admitted rows) is read as a counted run so the compiler hoists
 * the segment checks, a partial word walks its bits.
 *
 * <p>
 * The handle is invoked directly at every read site, and this class is listed for build-time
 * initialization in the query module's {@code native-image.properties}, for two compilers' reasons.
 * HotSpot C2 compiles an inlined helper method ({@code SegmentAccess.getLong}) with the helper's
 * process-wide type profile, which heap-array segment reads elsewhere make bimorphic, so the
 * kernel's reads carry a type switch and an out-of-line call for the heap case (t250k warm Q10 +60
 * % against the direct invocation, whose own bytecode's profile is monomorphic for the slab). The
 * native image folds a {@code VarHandle} into a plain load only when it is a static final of a
 * class initialized at build time; a handle held, or aliased, by a class the image initializes at
 * run time is invoked through the generic method-handle path with a boxed allocation on every read
 * (105 ns against 2.3 ns, which cost t1M warm Q4 44 % and Q9 39 % natively while C2, which trusts
 * every static final, showed a win). Hence: this class holds the handle and nothing else, callers
 * name it as {@code JoinKernels.SLAB_LONG} (never an alias of their own), and
 * {@code SegmentHandleConstantsTest} keeps every other segment handle out of these packages and
 * this class in the configuration.
 *
 * <p>
 * Package-private so the kernel benchmark and the exactness test invoke the shipped code.
 */
final class JoinKernels {

  /**
   * The slab's long handle (native order; the unaligned layout because the slab aligns every value by
   * construction, so the aligned handle's check would only repeat per access). Invoked directly at
   * every read site of the kernels and of {@code SirixJoinedGroupAggregateExpr}, always as
   * {@code JoinKernels.SLAB_LONG}, never through a helper method or a static alias in another class:
   * see the class comment for the two compilers' reasons.
   */
  static final VarHandle SLAB_LONG = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();

  private JoinKernels() {}

  /**
   * {@code segment} as a fresh native view of the same bytes, for a kernel that hoists a lane per
   * leaf and reads it per row.
   *
   * <p>
   * Every read through a segment handle casts the segment to the JDK's abstract segment class and
   * calls three of its abstract methods (base, offset, alignment mask). C2 devirtualizes those from
   * the profile and the trap history of the JDK methods themselves, which every caller in the process
   * shares: once a heap-array segment ({@code MemorySegment.ofArray}) has been read through the same
   * path at a site compiled for native segments four times ({@code PerBytecodeTrapLimit}), the
   * speculation is gone for every kernel compiled afterwards, which then pays a subtype check and a
   * guarded virtual call per read (t250k Q4 lost 3 % to the heap-array KEYS reads of an unrelated
   * kernel this way). A view built here is a {@code new} native segment the compiler sees being
   * constructed, so its class is known statically at every read through the local that holds it, and
   * no profile is consulted. Costs one small object per leaf; a heap segment (the allocator-refused
   * key lanes) is returned as it is, since it has no address to view.
   * </p>
   *
   * <p>
   * The view keeps the segment's address and size, so every base and offset computed against the
   * original addresses the same byte. It is read-only in use and never released: the slab it views
   * stays mapped by the pin or ownership the kernel's caller already holds.
   * </p>
   */
  static MemorySegment nativeView(final MemorySegment segment) {
    if (!segment.isNative()) {
      return segment;
    }
    return MemorySegment.ofAddress(segment.address()).reinterpret(segment.byteSize());
  }

  /**
   * The permutation that orders {@code count} rows by {@code keyColumns} lexicographically: column 0
   * most significant, every column in ascending natural order of its longs, ties broken by the next
   * column and, when every column ties, by row index (a caller that wants a column descending stores
   * its values bitwise inverted, which reverses the order of every pair of longs without overflow).
   *
   * <p>
   * A least-significant-digit radix sort: for each column from the last, eight stable counting passes
   * over the key's bytes (the top byte with its sign bit flipped, so negative keys sort first), each
   * a histogram and a scatter of the row indexes, and a byte every row agrees on costs its histogram
   * only. The whole order is a few linear passes rather than a comparator call per comparison, which
   * is what the row-output join's ordering needs: a handful of long keys over thousands of rows,
   * where a comparator sort cost more than the join's reads. (fastutil's indirect radix sort is not
   * usable here: its "stable" variant breaks ties by row index, not by the incoming order, so passes
   * cannot be chained.)
   * </p>
   *
   * @param keyColumns the columns, each at least {@code count} long
   * @param count the rows to order
   * @return row indexes in order
   */
  static int[] sortRows(final long[][] keyColumns, final int count) {
    if (count < 0) {
      throw new IllegalArgumentException("negative row count " + count);
    }
    for (int column = 0; column < keyColumns.length; column++) {
      if (keyColumns[column].length < count) {
        throw new IllegalArgumentException(
            "key column " + column + " holds " + keyColumns[column].length + " of " + count + " rows");
      }
    }
    final int[] order = new int[count];
    for (int i = 0; i < count; i++) {
      order[i] = i;
    }
    if (count < 2) {
      return order;
    }
    final int[] counts = new int[257];
    int[] source = order;
    int[] target = new int[count];
    for (int column = keyColumns.length - 1; column >= 0; column--) {
      final long[] keys = keyColumns[column];
      for (int shift = 0; shift < 64; shift += 8) {
        final int signFlip = shift == 56
            ? 0x80
            : 0;
        Arrays.fill(counts, 0);
        for (int i = 0; i < count; i++) {
          counts[(((int) (keys[source[i]] >>> shift) & 0xFF) ^ signFlip) + 1]++;
        }
        if (counts[(((int) (keys[source[0]] >>> shift) & 0xFF) ^ signFlip) + 1] == count) {
          continue; // every row shares this byte: the pass would move nothing
        }
        for (int digit = 1; digit <= 256; digit++) {
          counts[digit] += counts[digit - 1];
        }
        for (int i = 0; i < count; i++) {
          final int row = source[i];
          target[counts[((int) (keys[row] >>> shift) & 0xFF) ^ signFlip]++] = row;
        }
        final int[] swap = source;
        source = target;
        target = swap;
      }
    }
    if (source != order) {
      System.arraycopy(source, 0, order, 0, count);
    }
    return order;
  }

  /** A bounded distinct domain: value {@code v} is bit {@code v - minimum} of {@code words} words. */
  record DistinctDomain(long minimum, int words) {
  }

  /**
   * The lanes of one side as segments, captured per leaf (segment and byte base of row 0 for the
   * join, from, to, margin, optional distinct and group lanes; a pruned leaf has no mask). The masks
   * are the side's own copies, so a packing task may outlive the query's view.
   */
  static final class SideLanes {
    final long[] @Nullable [] masks;
    final MemorySegment[] joinSeg;
    final long[] joinBase;
    final MemorySegment[] fromSeg;
    final long[] fromBase;
    final MemorySegment[] toSeg;
    final long[] toBase;
    final MemorySegment[] marginSeg;
    final long[] marginBase;
    final MemorySegment @Nullable [] distinctSeg;
    final long @Nullable [] distinctBase;
    final MemorySegment @Nullable [] groupSeg;
    final long @Nullable [] groupBase;
    /** Mask words with every row admitted, and with some: how often the counted run applies. */
    final int fullWords;
    final int partialWords;

    SideLanes(final long[] @Nullable [] masks, final MemorySegment[] joinSeg, final long[] joinBase,
        final MemorySegment[] fromSeg, final long[] fromBase, final MemorySegment[] toSeg, final long[] toBase,
        final MemorySegment[] marginSeg, final long[] marginBase, final MemorySegment @Nullable [] distinctSeg,
        final long @Nullable [] distinctBase, final MemorySegment @Nullable [] groupSeg,
        final long @Nullable [] groupBase) {
      this.masks = masks;
      this.joinSeg = joinSeg;
      this.joinBase = joinBase;
      this.fromSeg = fromSeg;
      this.fromBase = fromBase;
      this.toSeg = toSeg;
      this.toBase = toBase;
      this.marginSeg = marginSeg;
      this.marginBase = marginBase;
      this.distinctSeg = distinctSeg;
      this.distinctBase = distinctBase;
      this.groupSeg = groupSeg;
      this.groupBase = groupBase;
      int full = 0;
      int partial = 0;
      for (final long[] mask : masks) {
        if (mask == null) {
          continue;
        }
        for (final long word : mask) {
          if (word == -1L) {
            full++;
          } else if (word != 0L) {
            partial++;
          }
        }
      }
      this.fullWords = full;
      this.partialWords = partial;
    }

    static SideLanes capture(final MaskedColumns columns, final int fromSlot, final int toSlot, final int marginSlot,
        final int distinctSlot, final int groupSlot) {
      final int leaves = columns.leafCount();
      final long[][] masks = new long[leaves][];
      final MemorySegment[] joinSeg = new MemorySegment[leaves];
      final long[] joinBase = new long[leaves];
      final MemorySegment[] fromSeg = new MemorySegment[leaves];
      final long[] fromBase = new long[leaves];
      final MemorySegment[] toSeg = new MemorySegment[leaves];
      final long[] toBase = new long[leaves];
      final MemorySegment[] marginSeg = new MemorySegment[leaves];
      final long[] marginBase = new long[leaves];
      final MemorySegment[] distinctSeg = distinctSlot < 0
          ? null
          : new MemorySegment[leaves];
      final long[] distinctBase = distinctSlot < 0
          ? null
          : new long[leaves];
      final MemorySegment[] groupSeg = groupSlot < 0
          ? null
          : new MemorySegment[leaves];
      final long[] groupBase = groupSlot < 0
          ? null
          : new long[leaves];
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = columns.rowMask(leaf);
        if (mask == null) {
          continue;
        }
        masks[leaf] = mask.clone();
        joinSeg[leaf] = columns.laneSegment(0, leaf);
        joinBase[leaf] = columns.laneBase(0, leaf);
        fromSeg[leaf] = columns.laneSegment(fromSlot, leaf);
        fromBase[leaf] = columns.laneBase(fromSlot, leaf);
        toSeg[leaf] = columns.laneSegment(toSlot, leaf);
        toBase[leaf] = columns.laneBase(toSlot, leaf);
        marginSeg[leaf] = columns.laneSegment(marginSlot, leaf);
        marginBase[leaf] = columns.laneBase(marginSlot, leaf);
        if (distinctSeg != null) {
          distinctSeg[leaf] = columns.laneSegment(distinctSlot, leaf);
          distinctBase[leaf] = columns.laneBase(distinctSlot, leaf);
        }
        if (groupSeg != null) {
          groupSeg[leaf] = columns.laneSegment(groupSlot, leaf);
          groupBase[leaf] = columns.laneBase(groupSlot, leaf);
        }
      }
      return new SideLanes(masks, joinSeg, joinBase, fromSeg, fromBase, toSeg, toBase, marginSeg, marginBase,
          distinctSeg, distinctBase, groupSeg, groupBase);
    }
  }

  /**
   * One side of the dense extrema kernel, bucketed by join key in one allocator slab: a counting sort
   * over the build side's dense key domain places every key's rows contiguously, so the join is a
   * merge over keys that reads both sides sequentially. The slab holds {@code slots + 1} bucket
   * starts, then 64-byte aligned records of {@value #STRIDE} longs (from, to, margin, group index or
   * 0, distinct operand or 0), so bucketing touches one line per row. Timestamps keep their persisted
   * unit (epoch seconds on both sides), which orders exactly like the millisecond residuals of the
   * generic kernel. Rows whose key lies outside the domain cannot join and are dropped while
   * bucketing. Immutable once built, so a revision's side is reusable.
   *
   * <p>
   * Packing a large side costs a first touch of its whole record region plus one scattered write per
   * row, which is why a process-cold query never packs its probe side on the critical path: it
   * streams the lanes against the bucketed build side instead ({@link #foldChained}) while the
   * packing runs in the background for the next execution.
   */
  static final class BucketedSide {
    static final int STRIDE = 5;
    static final long STRIDE_BYTES = 8L * STRIDE;
    static final long FROM = 0L;
    static final long TO = 8L;
    static final long MARGIN = 16L;
    /** The group index on the side carrying the group key, zero on the other. */
    static final long META = 24L;
    static final long DISTINCT = 32L;

    final MemorySegment slab;
    final long rowsBase;
    final int count;
    final int slots;

    private BucketedSide(final MemorySegment slab, final long rowsBase, final int count, final int slots) {
      this.slab = slab;
      this.rowsBase = rowsBase;
      this.count = count;
      this.slots = slots;
    }

    /** Slab bytes for {@code slots} buckets and {@code count} records. */
    static long slabBytes(final int slots, final int count) {
      return DerivedCacheBudget.saturatedAdd(DerivedCacheBudget.alignUp(4L * (slots + 1L)),
          DerivedCacheBudget.saturatedMultiply(count, STRIDE_BYTES));
    }

    /** Count the rows whose key lies in the domain into {@code bucketStart[slot + 1]}; the total. */
    static int count(final SideLanes lanes, final long keyMinimum, final int slots, final int[] bucketStart) {
      if (slots < 0 || bucketStart.length != slots + 1) {
        throw new IllegalArgumentException("one bucket start per slot and one past the end");
      }
      int total = 0;
      for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        final MemorySegment join = lanes.joinSeg[leaf];
        final long joinBase = lanes.joinBase[leaf];
        for (int w = 0; w < mask.length; w++) {
          final long word = mask[w];
          if (word == 0L) {
            continue;
          }
          final long wordBase = joinBase + ((long) w << 9);
          if (word == -1L) {
            for (int r = 0; r < 64; r++) {
              final long slot = (long) SLAB_LONG.get(join, wordBase + ((long) r << 3)) - keyMinimum;
              if (slot >= 0 && slot < slots) {
                bucketStart[(int) slot + 1]++;
                total++;
              }
            }
          } else {
            long bits = word;
            while (bits != 0L) {
              final int bit = Long.numberOfTrailingZeros(bits);
              bits &= bits - 1L;
              final long slot = (long) SLAB_LONG.get(join, wordBase + ((long) bit << 3)) - keyMinimum;
              if (slot >= 0 && slot < slots) {
                bucketStart[(int) slot + 1]++;
                total++;
              }
            }
          }
        }
      }
      return total;
    }

    /**
     * Write the bucket starts and scatter the records into {@code slab}, sized by {@link #slabBytes};
     * {@code bucketStart} holds the per-bucket counts from {@link #count} and leaves as the prefix
     * sums.
     */
    static BucketedSide pack(final MemorySegment slab, final SideLanes lanes, final long keyMinimum, final int slots,
        final long groupMinimum, final int[] bucketStart, final int total) {
      if (slots < 0 || bucketStart.length != slots + 1 || total < 0) {
        throw new IllegalArgumentException("one bucket start per slot and one past the end");
      }
      final long rowsBase = DerivedCacheBudget.alignUp(4L * (slots + 1L));
      if (rowsBase + total * STRIDE_BYTES > slab.byteSize()) {
        throw new IllegalArgumentException("slab smaller than the packed side");
      }
      for (int slot = 0; slot < slots; slot++) {
        bucketStart[slot + 1] += bucketStart[slot];
      }
      if (bucketStart[slots] != total) {
        throw new IllegalArgumentException("bucket counts do not sum to the record total");
      }
      for (int slot = 0; slot <= slots; slot++) {
        SegmentAccess.putInt(slab, 4L * slot, bucketStart[slot]);
      }
      final int[] fill = Arrays.copyOf(bucketStart, slots);
      for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        fillLeaf(slab, rowsBase, lanes, leaf, mask, fill, keyMinimum, slots, groupMinimum);
      }
      return new BucketedSide(slab, rowsBase, total, slots);
    }

    private static void fillLeaf(final MemorySegment slab, final long rowsBase, final SideLanes lanes, final int leaf,
        final long[] mask, final int[] fill, final long keyMinimum, final int slots, final long groupMinimum) {
      final MemorySegment join = lanes.joinSeg[leaf];
      final long joinBase = lanes.joinBase[leaf];
      final MemorySegment from = lanes.fromSeg[leaf];
      final long fromBase = lanes.fromBase[leaf];
      final MemorySegment to = lanes.toSeg[leaf];
      final long toBase = lanes.toBase[leaf];
      final MemorySegment margin = lanes.marginSeg[leaf];
      final long marginBase = lanes.marginBase[leaf];
      final boolean hasDistinct = lanes.distinctSeg != null;
      final MemorySegment distinct = hasDistinct
          ? lanes.distinctSeg[leaf]
          : MemorySegment.NULL;
      final long distinctBase = hasDistinct
          ? lanes.distinctBase[leaf]
          : 0L;
      final boolean hasGroup = lanes.groupSeg != null;
      final MemorySegment group = hasGroup
          ? lanes.groupSeg[leaf]
          : MemorySegment.NULL;
      final long groupBase = hasGroup
          ? lanes.groupBase[leaf]
          : 0L;
      for (int w = 0; w < mask.length; w++) {
        final long word = mask[w];
        if (word == 0L) {
          continue;
        }
        final long wordAt = (long) w << 9;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            final long at8 = wordAt + ((long) r << 3);
            final long slot = (long) SLAB_LONG.get(join, joinBase + at8) - keyMinimum;
            if (slot < 0 || slot >= slots) {
              continue;
            }
            final long at = rowsBase + fill[(int) slot]++ * STRIDE_BYTES;
            putRecord(slab, at, from, fromBase, to, toBase, margin, marginBase, hasDistinct, distinct, distinctBase,
                hasGroup, group, groupBase, groupMinimum, at8);
          }
        } else {
          long bits = word;
          while (bits != 0L) {
            final int bit = Long.numberOfTrailingZeros(bits);
            bits &= bits - 1L;
            final long at8 = wordAt + ((long) bit << 3);
            final long slot = (long) SLAB_LONG.get(join, joinBase + at8) - keyMinimum;
            if (slot < 0 || slot >= slots) {
              continue;
            }
            final long at = rowsBase + fill[(int) slot]++ * STRIDE_BYTES;
            putRecord(slab, at, from, fromBase, to, toBase, margin, marginBase, hasDistinct, distinct, distinctBase,
                hasGroup, group, groupBase, groupMinimum, at8);
          }
        }
      }
    }

    private static void putRecord(final MemorySegment slab, final long at, final MemorySegment from,
        final long fromBase, final MemorySegment to, final long toBase, final MemorySegment margin,
        final long marginBase, final boolean hasDistinct, final MemorySegment distinct, final long distinctBase,
        final boolean hasGroup, final MemorySegment group, final long groupBase, final long groupMinimum,
        final long at8) {
      SLAB_LONG.set(slab, at + FROM, (long) SLAB_LONG.get(from, fromBase + at8));
      SLAB_LONG.set(slab, at + TO, (long) SLAB_LONG.get(to, toBase + at8));
      SLAB_LONG.set(slab, at + MARGIN, (long) SLAB_LONG.get(margin, marginBase + at8));
      SLAB_LONG.set(slab, at + META, hasGroup
          ? (long) SLAB_LONG.get(group, groupBase + at8) - groupMinimum
          : 0L);
      SLAB_LONG.set(slab, at + DISTINCT, hasDistinct
          ? (long) SLAB_LONG.get(distinct, distinctBase + at8)
          : 0L);
    }

    /** The first record ordinal of {@code slot}; {@code slot == slots} is the end. */
    int bucketStart(final int slot) {
      if (slot < 0 || slot > slots) {
        throw new IndexOutOfBoundsException("slot " + slot + " of " + slots);
      }
      return SegmentAccess.getInt(slab, 4L * slot);
    }
  }

  /**
   * The flat fold state of the dense extrema kernel: per-group minimum/maximum of the residual and
   * distinct values per group as one value-major bitmap or, when the distinct domain is unbounded,
   * one set per group. {@link #pair} folds one probe row against the build records of its key, read
   * from the build side's slab through the slab handle.
   */
  static final class ExtremaFold {
    final long[] groupMinimum;
    final long[] groupMaximum;
    final long[] distinctCounts;
    final long @Nullable [] bits;
    final LongOpenHashSet @Nullable [] sets;
    final MemorySegment buildSlab;
    final long buildRows;
    final int groups;
    final long domainMinimum;
    final boolean distinctBuild;
    final boolean buildMinusProbe;

    ExtremaFold(final int groups, final @Nullable DistinctDomain domain, final BucketedSide build,
        final boolean distinctBuild, final boolean buildMinusProbe) {
      if (groups < 0) {
        throw new IllegalArgumentException("negative group count " + groups);
      }
      this.groups = groups;
      this.buildSlab = build.slab;
      this.buildRows = build.rowsBase;
      this.distinctBuild = distinctBuild;
      this.buildMinusProbe = buildMinusProbe;
      groupMinimum = new long[groups];
      groupMaximum = new long[groups];
      distinctCounts = new long[groups];
      Arrays.fill(groupMinimum, Long.MAX_VALUE);
      Arrays.fill(groupMaximum, Long.MIN_VALUE);
      domainMinimum = domain == null
          ? 0L
          : domain.minimum();
      // Value-major bit layout: the bits of one distinct value's groups are adjacent.
      bits = domain == null
          ? null
          : new long[Math.multiplyExact(groups, domain.words())];
      sets = domain == null
          ? new LongOpenHashSet[groups]
          : null;
    }

    /**
     * Fold one probe row against the build records {@code [buildBegin, buildEnd)} (record ordinals),
     * the chained fold's order: a loop over record offsets, which the kernel benchmark measured as the
     * fastest handle form on C2 (an ordinal-indexed loop and a vector load per record were slower).
     */
    void pair(final long from, final long to, final long margin, final long probeValue, final int buildBegin,
        final int buildEnd) {
      final MemorySegment slab = this.buildSlab;
      final long begin = this.buildRows + buildBegin * BucketedSide.STRIDE_BYTES;
      for (long b = this.buildRows + buildEnd * BucketedSide.STRIDE_BYTES - BucketedSide.STRIDE_BYTES; b >= begin; b -=
          BucketedSide.STRIDE_BYTES) {
        if (from >= (long) SLAB_LONG.get(slab, b + BucketedSide.TO)
            || (long) SLAB_LONG.get(slab, b + BucketedSide.FROM) >= to) {
          continue;
        }
        final int group = (int) (long) SLAB_LONG.get(slab, b + BucketedSide.META);
        final long buildMargin = (long) SLAB_LONG.get(slab, b + BucketedSide.MARGIN);
        final long value = buildMinusProbe
            ? Math.subtractExact(buildMargin, margin)
            : Math.subtractExact(margin, buildMargin);
        if (value < groupMinimum[group]) {
          groupMinimum[group] = value;
        }
        if (value > groupMaximum[group]) {
          groupMaximum[group] = value;
        }
        final long distinctValue = distinctBuild
            ? (long) SLAB_LONG.get(slab, b + BucketedSide.DISTINCT)
            : probeValue;
        if (bits != null) {
          final long bitIndex = (distinctValue - domainMinimum) * groups + group;
          final int bitWord = (int) (bitIndex >>> 6);
          final long bit = 1L << bitIndex;
          final long prior = bits[bitWord];
          if ((prior & bit) == 0L) {
            bits[bitWord] = prior | bit;
            distinctCounts[group]++;
          }
        } else {
          addToSet(group, distinctValue);
        }
      }
    }

    /**
     * Fold one bucket of the merge: every build record {@code [buildBegin, buildEnd)} (record ordinals)
     * against every probe record in {@code [probeBegin, probeEnd)} (byte offsets of the probe slab).
     * The build record's five values are read once; a probe record costs two reads unless it overlaps,
     * then its margin and, when the probe carries it, its distinct operand.
     */
    void bucket(final int buildBegin, final int buildEnd, final MemorySegment probeSlab, final long probeBegin,
        final long probeEnd) {
      final MemorySegment slab = this.buildSlab;
      final long rows = this.buildRows;
      final boolean distinctBuild = this.distinctBuild;
      final boolean buildMinusProbe = this.buildMinusProbe;
      final int groups = this.groups;
      for (int i = buildBegin; i < buildEnd; i++) {
        final long b = rows + i * BucketedSide.STRIDE_BYTES;
        final long buildFrom = (long) SLAB_LONG.get(slab, b + BucketedSide.FROM);
        final long buildTo = (long) SLAB_LONG.get(slab, b + BucketedSide.TO);
        final long buildMargin = (long) SLAB_LONG.get(slab, b + BucketedSide.MARGIN);
        final int group = (int) (long) SLAB_LONG.get(slab, b + BucketedSide.META);
        final long buildDistinct = distinctBuild
            ? (long) SLAB_LONG.get(slab, b + BucketedSide.DISTINCT)
            : 0L;
        for (long p = probeBegin; p < probeEnd; p += BucketedSide.STRIDE_BYTES) {
          if ((long) SLAB_LONG.get(probeSlab, p + BucketedSide.FROM) >= buildTo
              || buildFrom >= (long) SLAB_LONG.get(probeSlab, p + BucketedSide.TO)) {
            continue;
          }
          final long margin = (long) SLAB_LONG.get(probeSlab, p + BucketedSide.MARGIN);
          final long value = buildMinusProbe
              ? Math.subtractExact(buildMargin, margin)
              : Math.subtractExact(margin, buildMargin);
          if (value < groupMinimum[group]) {
            groupMinimum[group] = value;
          }
          if (value > groupMaximum[group]) {
            groupMaximum[group] = value;
          }
          final long distinctValue = distinctBuild
              ? buildDistinct
              : (long) SLAB_LONG.get(probeSlab, p + BucketedSide.DISTINCT);
          if (bits != null) {
            final long bitIndex = (distinctValue - domainMinimum) * groups + group;
            final int bitWord = (int) (bitIndex >>> 6);
            final long bit = 1L << bitIndex;
            final long prior = bits[bitWord];
            if ((prior & bit) == 0L) {
              bits[bitWord] = prior | bit;
              distinctCounts[group]++;
            }
          } else {
            addToSet(group, distinctValue);
          }
        }
      }
    }

    /**
     * The unbounded-domain fallback, kept out of {@link #pair} and {@link #bucket} so the hot folds
     * stay small enough to inline.
     */
    @SuppressWarnings("NullAway") // Called only when bits is null, which proves sets is non-null.
    private void addToSet(final int group, final long distinctValue) {
      LongOpenHashSet set = sets[group];
      if (set == null) {
        set = new LongOpenHashSet(16);
        sets[group] = set;
      }
      set.add(distinctValue);
    }

    @SuppressWarnings("NullAway") // The constructor allocates sets exactly when bits is null.
    long distinctCount(final int group) {
      return bits != null
          ? distinctCounts[group]
          : sets[group] == null
              ? 0L
              : sets[group].size();
    }
  }

  /**
   * Merge two bucketed sides key by key: buckets are contiguous and ascending in both slabs, so every
   * key's records are one range per side. Each build record of a bucket is read once into locals and
   * the bucket's probe records stream past it ({@link ExtremaFold#bucket}): two reads decide a
   * non-overlapping pair, four an overlapping one, against the four-plus-four of a probe-major order.
   * The fold is commutative (minimum, maximum, set union), so the pair order is free.
   */
  static void foldMerge(final ExtremaFold fold, final BucketedSide build, final BucketedSide probe, final int slots) {
    if (build.slots != slots || probe.slots != slots) {
      throw new IllegalArgumentException("both sides must be bucketed over the same key domain");
    }
    final MemorySegment buildSlab = build.slab;
    final MemorySegment probeSlab = probe.slab;
    final long probeRows = probe.rowsBase;
    int buildStart = SegmentAccess.getInt(buildSlab, 0L);
    int probeStart = SegmentAccess.getInt(probeSlab, 0L);
    for (int slot = 0; slot < slots; slot++) {
      final int buildNext = SegmentAccess.getInt(buildSlab, 4L * (slot + 1));
      final int probeNext = SegmentAccess.getInt(probeSlab, 4L * (slot + 1));
      if (buildStart != buildNext && probeStart != probeNext) {
        fold.bucket(buildStart, buildNext, probeSlab, probeRows + probeStart * BucketedSide.STRIDE_BYTES,
            probeRows + probeNext * BucketedSide.STRIDE_BYTES);
      }
      buildStart = buildNext;
      probeStart = probeNext;
    }
  }

  /**
   * Stream the probe lanes in row order against the bucketed build side: no probe record is written,
   * so a process-cold execution pays only the lane reads, with the same fold state as
   * {@link #foldMerge}. Returns the admitted probe rows (those whose key lies in the domain).
   */
  static int foldChained(final ExtremaFold fold, final BucketedSide build, final SideLanes lanes, final long keyMinimum,
      final int slots) {
    if (build.slots != slots) {
      throw new IllegalArgumentException("the build side must be bucketed over the probe's key domain");
    }
    final MemorySegment buildSlab = build.slab;
    final boolean hasDistinct = lanes.distinctSeg != null;
    int admitted = 0;
    for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
      final long[] mask = lanes.masks[leaf];
      if (mask == null) {
        continue;
      }
      final MemorySegment join = lanes.joinSeg[leaf];
      final long joinBase = lanes.joinBase[leaf];
      final MemorySegment from = lanes.fromSeg[leaf];
      final long fromBase = lanes.fromBase[leaf];
      final MemorySegment to = lanes.toSeg[leaf];
      final long toBase = lanes.toBase[leaf];
      final MemorySegment margin = lanes.marginSeg[leaf];
      final long marginBase = lanes.marginBase[leaf];
      final MemorySegment distinct = hasDistinct
          ? lanes.distinctSeg[leaf]
          : MemorySegment.NULL;
      final long distinctBase = hasDistinct
          ? lanes.distinctBase[leaf]
          : 0L;
      for (int w = 0; w < mask.length; w++) {
        final long word = mask[w];
        if (word == 0L) {
          continue;
        }
        final long wordAt = (long) w << 9;
        if (word == -1L) {
          for (int r = 0; r < 64; r++) {
            admitted += chainRow(fold, buildSlab, join, joinBase, from, fromBase, to, toBase, margin, marginBase,
                hasDistinct, distinct, distinctBase, keyMinimum, slots, wordAt + ((long) r << 3));
          }
        } else {
          long bits = word;
          while (bits != 0L) {
            final int bit = Long.numberOfTrailingZeros(bits);
            bits &= bits - 1L;
            admitted += chainRow(fold, buildSlab, join, joinBase, from, fromBase, to, toBase, margin, marginBase,
                hasDistinct, distinct, distinctBase, keyMinimum, slots, wordAt + ((long) bit << 3));
          }
        }
      }
    }
    return admitted;
  }

  /** One probe row of {@link #foldChained}: 1 when its key lies in the domain, else 0. */
  private static int chainRow(final ExtremaFold fold, final MemorySegment buildSlab, final MemorySegment join,
      final long joinBase, final MemorySegment from, final long fromBase, final MemorySegment to, final long toBase,
      final MemorySegment margin, final long marginBase, final boolean hasDistinct, final MemorySegment distinct,
      final long distinctBase, final long keyMinimum, final int slots, final long at8) {
    final long slot = (long) SLAB_LONG.get(join, joinBase + at8) - keyMinimum;
    if (slot < 0 || slot >= slots) {
      return 0;
    }
    final int buildBegin = SegmentAccess.getInt(buildSlab, 4L * (int) slot);
    final int buildEnd = SegmentAccess.getInt(buildSlab, 4L * ((int) slot + 1));
    if (buildBegin != buildEnd) {
      fold.pair((long) SLAB_LONG.get(from, fromBase + at8), (long) SLAB_LONG.get(to, toBase + at8),
          (long) SLAB_LONG.get(margin, marginBase + at8), hasDistinct
              ? (long) SLAB_LONG.get(distinct, distinctBase + at8)
              : 0L,
          buildBegin, buildEnd);
    }
    return 1;
  }
}
