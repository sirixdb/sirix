/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Zero-copy scan over serialised {@link ProjectionIndexLeafPage} byte[]s.
 * Does not materialise the leaf's column arrays — reads primitives
 * directly from the payload via {@link VarHandle}, eliminating the
 * per-leaf allocation storm that {@link ProjectionIndexScan} pays
 * through {@link ProjectionIndexLeafPage#deserialize}.
 *
 * <p>Measured on a synthetic 1M-row stream (977 leaves), {@code deserialize}
 * accounts for ~50% of the scan cost on numeric predicates and ~90–98% on
 * boolean / string-EQ predicates (where the kernel itself is
 * sub-nanosecond). This reader skips it entirely: one small scratch
 * long[] per scan-thread (reused across leaves), otherwise zero allocs
 * on the hot path.
 *
 * <h2>Iter#02 note — why {@link VarHandle} beat {@code sun.misc.Unsafe}</h2>
 *
 * An early iter#02 attempt replaced the {@code byteArrayViewVarHandle} reads
 * with {@code sun.misc.Unsafe.getInt/getLong(byte[], base+off)} on the
 * hypothesis that the {@code VarHandleGuards.guard_LI_I} / access-mode checks
 * visible in a contaminated CPU profile were real per-call overhead. A clean
 * A/B/C comparison (3 runs each of varhandle, MemorySegment via FFM, Unsafe
 * direct — cold 100M scale bench, load ≤ 2) found:
 * <ul>
 *   <li>varhandle median wall <b>5.29 s</b>, projection build 2,567 ms</li>
 *   <li>msegment median wall 5.53 s (+4.5%), build 2,659 ms</li>
 *   <li>unsafe   median wall 5.56 s (+5.1%), build 2,729 ms</li>
 * </ul>
 * HotSpot's C2 already inlines and intrinsifies {@code VarHandle.get} on
 * static-final byte-array view handles to the same raw MOVL/MOVQ that
 * {@code Unsafe} emits; the "guard" frames only appear when the VarHandle
 * is not proven monomorphic at the call site. Swapping to Unsafe also meant
 * an {@code --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED} dependency
 * that would have pulled a deprecated-for-removal API onto the hot path for
 * zero measured benefit, so the swap was reverted. This javadoc records the
 * result so future attempts know to skip the detour.
 *
 * <h2>Layout decoded</h2>
 * The format is defined by {@link ProjectionIndexLeafPage#serialize}:
 * <pre>
 *   0:   int rowCount
 *   4:   int columnCount
 *   8:   long firstRecordKey
 *   16:  long lastRecordKey
 *   24:  byte[columnCount] kinds
 *   24+columnCount: long[rowCount] recordKeys    (if rowCount > 0)
 *   then per column in order:
 *     long min, long max
 *     NUMERIC_LONG:  long[rowCount] values
 *     BOOLEAN:       long[ceil(rowCount/64)] packed bits
 *     STRING_DICT:   int dictSize
 *                    int[dictSize] lengths
 *                    byte[Σ lengths] concatenated UTF-8
 *                    int[rowCount] packed dict-ids
 * </pre>
 *
 * <p>All multi-byte integers are little-endian.
 */
public final class ProjectionIndexByteScan {

  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  /**
   * SIMD species for the numeric-compare fast path. {@code SPECIES_PREFERRED}
   * adapts to the runtime CPU: 8 lanes on AVX-512, 4 on AVX2, 2 on SSE.
   */
  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int LANES = LONG_SPECIES.length();

  /**
   * Read a little-endian {@code int} from {@code b} at byte offset {@code off}.
   * Forwards to {@link #INT_LE} — wrapped in a named helper so call-sites are
   * easier to grep and the small unchecked-cast-from-Object boilerplate lives
   * in one place. HotSpot intrinsifies the VarHandle call to a single
   * {@code MOVL} after warm-up.
   */
  private static int getIntLE(final byte[] b, final int off) {
    return (int) INT_LE.get(b, off);
  }

  /**
   * Read a little-endian {@code long} from {@code b} at byte offset {@code off}.
   * Intrinsified to a single {@code MOVQ} by HotSpot.
   */
  private static long getLongLE(final byte[] b, final int off) {
    return (long) LONG_LE.get(b, off);
  }

  /**
   * Thread-local scan scratch. Hoisted out of {@link #conjunctiveCount} and
   * {@link #conjunctiveCountByGroup}'s per-call allocation: ~8.5 KB per
   * invocation × 20 worker threads × N queries = tens of MB/s of GC churn
   * at sustained analytical load. Reuse across calls; grown on demand if a
   * wider projection shows up. Single instance per thread is safe because
   * each conjunctiveCount call is one-shot on its own worker-thread stack —
   * no re-entrancy.
   */
  private static final class ScanScratch {
    int[] columnDataOff = new int[16];
    int[] columnMinMaxOff = new int[16];
    /**
     * End offset of the column data stream of the leaf most recently
     * processed by {@link #evaluateLeafMask} — i.e. where the presence tail
     * would start. Lets the group/aggregate kernels locate presence bitmaps
     * with an EXACT boundary check instead of trusting the footer alone.
     */
    int leafDataEnd;
    final long[] numericScratch = new long[ProjectionIndexLeafPage.MAX_ROWS];
    final long[] mask = new long[(ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6];
    final long[] colMask = new long[(ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6];
    // Lazily-sized dict byte-offset cache + String cache for the group-by
    // variant. null on threads that only do conjunctiveCount.
    String[] dictCache;
    int[] dictByteOff;
    // Per-thread intern: 64-bit FNV-1a hash of group-value bytes →
    // canonical String. Zero allocation on the lookup hot path
    // (hash + Long2ObjectMap.get is primitive-keyed, no autoboxing), one
    // String decode per distinct group value per thread per scan.
    // 64-bit hash collision probability at 10M distinct values ~10⁻¹⁹ —
    // negligible for analytical groupby cardinalities.
    it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<String> stringIntern;
    // iter#10 dense group-by remap: per-leaf dictId -> canonId.
    // Pre-allocated to 64, grown on demand for leaves with larger dicts.
    int[] dictRemap;
  }

  private static final ThreadLocal<ScanScratch> SCRATCH = ThreadLocal.withInitial(ScanScratch::new);

  private ProjectionIndexByteScan() {
  }

  /** Raw row count — parses the header only. Identical semantics to the materialising variant. */
  public static long countRows(final Iterable<byte[]> leafPayloads) {
    long total = 0;
    for (final byte[] payload : leafPayloads) {
      total += getIntLE(payload, 0);
    }
    return total;
  }

  /**
   * Count rows satisfying the conjunctive {@code predicates}. Predicate-free
   * calls throw — callers should use {@link #countRows(Iterable)} for
   * unconditional counts.
   */
  public static long conjunctiveCount(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates) {
    if (predicates == null || predicates.length == 0) {
      throw new IllegalArgumentException("use countRows for unconditional counts");
    }
    // Thread-local scratch: one allocation per worker thread amortised
    // across all analytical queries on that thread. See {@link ScanScratch}.
    final ScanScratch s = SCRATCH.get();
    long total = 0;
    for (final byte[] payload : leafPayloads) {
      // Grow scratch if a wider leaf shows up (rare — projection indexes
      // are built column-set-at-a-time, so the width is consistent within
      // a single handle).
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      total += countLeaf(payload, predicates, s);
    }
    return total;
  }

  /**
   * Probe the first {@code probeLeaves} leaves to collect the union of
   * UTF-8 byte-slices present in the {@code groupColumn} dictionary.
   * Returns the resulting canonical dictionary ({@code byte[][]}, one
   * entry per distinct UTF-8 value in insertion order), or {@code null}
   * when any of the following hold:
   *
   * <ul>
   *   <li>{@code leafPayloads} is empty,</li>
   *   <li>{@code groupColumn} is out of range on the first leaf,</li>
   *   <li>the group column's kind is not
   *       {@link ProjectionIndexLeafPage#COLUMN_KIND_STRING_DICT},</li>
   *   <li>the observed cardinality exceeds {@code cardLimit}.</li>
   * </ul>
   *
   * <p>Used by {@link ProjectionIndexRegistry.Handle#canonicalDict} to
   * decide eligibility for the dense group-by path
   * ({@link #conjunctiveCountByGroupDense}).
   *
   * <p>HFT-grade: bounded scan depth; one {@code ArrayList<byte[]>} for
   * the probe result; no per-leaf dict string allocation (values are
   * carried as slices copied into fresh {@code byte[]}).
   *
   * @param leafPayloads ordered leaf byte[] list — typically
   *                     {@link ProjectionIndexRegistry.Handle#leafPayloads}.
   * @param groupColumn  target column index.
   * @param probeLeaves  max number of leaves to probe, {@code > 0}.
   * @param cardLimit    max tolerable cardinality; caller-specific
   *                     bound (e.g. {@code long[]} budget per worker).
   * @return immutable canonical dict (caller must not mutate), or
   *         {@code null} if ineligible.
   */
  public static byte[][] probeCanonicalDict(final List<byte[]> leafPayloads,
      final int groupColumn, final int probeLeaves, final int cardLimit) {
    if (leafPayloads == null || leafPayloads.isEmpty()) return null;
    if (probeLeaves <= 0 || cardLimit <= 0) return null;
    final byte[] firstLeaf = leafPayloads.get(0);
    if (firstLeaf == null) return null;
    final int columnCount = columnCountOf(firstLeaf);
    if (groupColumn < 0 || groupColumn >= columnCount) return null;
    if (firstLeaf[24 + groupColumn] != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) return null;

    // Seed the canonical dict from the first leaf's dict.
    final ArrayList<byte[]> canon = new ArrayList<>(Math.min(cardLimit, 64));
    final int scanUpTo = Math.min(probeLeaves, leafPayloads.size());
    for (int li = 0; li < scanUpTo; li++) {
      final byte[] payload = leafPayloads.get(li);
      if (payload == null) continue;
      if (columnCountOf(payload) != columnCount) continue;
      if (payload[24 + groupColumn] != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) return null;
      final int groupBase = columnDataOffFor(payload, groupColumn);
      if (groupBase < 0) return null;
      final int dictSize = getIntLE(payload, groupBase);
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      int running = concatOff;
      for (int i = 0; i < dictSize; i++) {
        final int len = getIntLE(payload, lenHeaderOff + i * 4);
        // Dedup against canonical dict (linear probe, small N in practice).
        final int canonSize = canon.size();
        boolean present = false;
        for (int c = 0; c < canonSize; c++) {
          if (bytesEqualAt(payload, running, len, canon.get(c))) { present = true; break; }
        }
        if (!present) {
          if (canonSize >= cardLimit) return null;   // cardinality exceeded
          final byte[] copy = new byte[len];
          if (len > 0) System.arraycopy(payload, running, copy, 0, len);
          canon.add(copy);
        }
        running += len;
      }
    }
    return canon.toArray(new byte[0][]);
  }

  /**
   * Compute the starting byte-offset of {@code groupColumn}'s data block
   * inside {@code payload} without populating the full per-column offset
   * cache. Returns {@code -1} on any structural inconsistency (caller
   * falls back to the hashmap path). Mirrors the offset-walk logic in
   * {@link #evaluateLeafMask} but stops at the target column.
   */
  private static int columnDataOffFor(final byte[] payload, final int groupColumn) {
    final int rowCount = getIntLE(payload, 0);
    if (rowCount == 0) return -1;
    final int columnCount = getIntLE(payload, 4);
    if (groupColumn < 0 || groupColumn >= columnCount) return -1;
    final int kindsOff = 24;
    int cursor = kindsOff + columnCount + rowCount * 8;  // recordKeysOff + rowCount*8
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;  // per-column min/max
      if (c == groupColumn) return cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE -> cursor += rowCount * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        default -> { return -1; }
      }
    }
    return -1;  // not found (shouldn't happen — guarded by columnCount check)
  }

  /**
   * Dense group-by-count: per matching row, increments
   * {@code counts[canonId]} where {@code canonId} is the position of the
   * leaf's dict-value in {@code canonicalDict}. Values NOT in
   * {@code canonicalDict} fall back to the hashmap path
   * ({@link #conjunctiveCountByGroup}) for the offending leaf only.
   *
   * <p>Hot path: one {@code int[]} dictId→canonId remap per leaf (cost:
   * {@code dictSize} × {@code canonLen} byte comparisons; both tiny for
   * bounded-cardinality group columns), then a single {@code counts[remap[dictId]]++}
   * per matching row. Zero hashmap ops, zero String.equals, zero
   * FNV-1a hashing on the per-row path.
   *
   * <p>HFT-grade: caller-allocated {@code counts}; per-leaf remap uses a
   * thread-local scratch {@code int[]}; no boxing, no virtual dispatch.
   *
   * @param leafPayloads   leaves to scan.
   * @param predicates     conjunctive predicate list (may be empty).
   * @param groupColumn    STRING_DICT column index.
   * @param canonicalDict  immutable canonical dict (length = count array size).
   * @param counts         output array, pre-zeroed by caller, length ≥ canonicalDict.length.
   * @param fallbackOut    optional hashmap that receives counts for any
   *                       leaf whose dict contains a value NOT in
   *                       {@code canonicalDict}. Non-null required when
   *                       a full fallback may happen (i.e. when caller
   *                       did not prove the canonical dict is complete).
   *                       Pass a non-null empty map and merge it back on
   *                       the caller side.
   */
  public static void conjunctiveCountByGroupDense(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int groupColumn,
      final byte[][] canonicalDict,
      final long[] counts,
      final Object2LongOpenHashMap<String> fallbackOut) {
    conjunctiveCountByGroupDense(leafPayloads, predicates, groupColumn, canonicalDict, counts, fallbackOut, null);
  }

  /**
   * Sparse-aware variant — matching rows missing the group field (
   * presence bit clear) count into {@code missingOut[0]}; {@code null}
   * keeps the dense behavior. See
   * {@link #conjunctiveCountByGroup(Iterable, ProjectionIndexScan.ColumnPredicate[], int, Object2LongOpenHashMap, long[])}.
   */
  public static void conjunctiveCountByGroupDense(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int groupColumn,
      final byte[][] canonicalDict,
      final long[] counts,
      final Object2LongOpenHashMap<String> fallbackOut,
      final long[] missingOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (canonicalDict == null) {
      throw new IllegalArgumentException("canonicalDict must not be null");
    }
    if (counts == null || counts.length < canonicalDict.length) {
      throw new IllegalArgumentException("counts[] too small for canonicalDict");
    }
    final int canonLen = canonicalDict.length;
    final ScanScratch s = SCRATCH.get();
    // Reuse the per-thread dict remap scratch. Legacy field
    // dictByteOff is a per-leaf byte-offset cache; we co-opt
    // dictCache's sibling slot by adding a new scratch field.
    int[] remap = s.dictRemap;
    if (remap == null || remap.length < 64) {
      remap = new int[64];
      s.dictRemap = remap;
    }
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn
            + " is not STRING_DICT (kind=" + groupKind + ")");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int dictSize = getIntLE(payload, groupBase);
      if (remap.length < dictSize) {
        remap = new int[Math.max(remap.length * 2, dictSize)];
        s.dictRemap = remap;
      }
      // Per-leaf dictId → canonId remap. -1 marks "not in canonical
      // dict", forcing fallback for this leaf.
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      int running = concatOff;
      boolean needsFallback = false;
      for (int i = 0; i < dictSize; i++) {
        final int len = getIntLE(payload, lenHeaderOff + i * 4);
        int hit = -1;
        for (int c = 0; c < canonLen; c++) {
          if (bytesEqualAt(payload, running, len, canonicalDict[c])) { hit = c; break; }
        }
        remap[i] = hit;
        if (hit < 0) needsFallback = true;
        running += len;
      }
      final int idsOff = running;

      final int tailStart = missingOut != null ? presenceTailStart(payload, s.leafDataEnd) : -1;
      final int presOff = tailStart >= 0 ? presenceWordsOff(payload, tailStart, groupColumn) : -1;

      if (needsFallback) {
        // Fallback: run the standard hashmap path on this single leaf.
        // The caller merges fallbackOut back into the final aggregate.
        if (fallbackOut == null) {
          throw new IllegalStateException(
              "canonical dict missing value and no fallback provided for leaf with dictSize=" + dictSize);
        }
        conjunctiveCountByGroupSingleLeaf(payload, rowCount, s.mask,
            groupBase, dictSize, lenHeaderOff, concatOff, idsOff, s, fallbackOut, presOff, missingOut);
        continue;
      }

      // Dense hot loop: counts[remap[dictId]]++ per matching row.
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long presWord = presOff >= 0 ? getLongLE(payload, presOff + w * 8) : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          if (presOff >= 0 && (presWord & (1L << bit)) == 0L) {
            missingOut[0]++;
            continue;
          }
          final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
          counts[remap[dictId]]++;
        }
      }
    }
  }

  /**
   * Per-leaf fallback path for {@link #conjunctiveCountByGroupDense}:
   * when a leaf's dict contains a value NOT in the canonical dict, we
   * fall back to the original hashmap accumulator for that one leaf.
   *
   * <p>This is structurally the same as the inner loop of
   * {@link #conjunctiveCountByGroup} (intern by FNV-1a64 hash, bump
   * {@link Object2LongOpenHashMap#addTo} per row), hoisted into a
   * helper so the dense path can invoke it without duplicating the
   * mask-iteration state. Payload offsets are passed in pre-computed
   * since the dense path already walked them.
   */
  private static void conjunctiveCountByGroupSingleLeaf(final byte[] payload,
      final int rowCount, final long[] scanMask, final int groupBase,
      final int dictSize, final int lenHeaderOff, final int concatOff, final int idsOff,
      final ScanScratch s, final Object2LongOpenHashMap<String> out,
      final int presOff, final long[] missingOut) {
    if (s.dictCache == null || s.dictCache.length < dictSize) {
      s.dictCache = new String[Math.max(64, dictSize)];
      s.dictByteOff = new int[s.dictCache.length];
    } else {
      // Clear the prefix we'll populate.
      for (int i = 0; i < dictSize; i++) s.dictCache[i] = null;
    }
    if (s.stringIntern == null) {
      s.stringIntern = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>(32);
    }
    final String[] dictCache = s.dictCache;
    final int[] dictByteOff = s.dictByteOff;
    int running = concatOff;
    for (int i = 0; i < dictSize; i++) {
      dictByteOff[i] = running;
      running += getIntLE(payload, lenHeaderOff + i * 4);
    }
    final var intern = s.stringIntern;
    final int stride = (rowCount + 63) >>> 6;
    for (int w = 0; w < stride; w++) {
      long word = scanMask[w];
      final long presWord = presOff >= 0 ? getLongLE(payload, presOff + w * 8) : -1L;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int rowIdx = (w << 6) + bit;
        if (rowIdx >= rowCount) break;
        if (presOff >= 0 && (presWord & (1L << bit)) == 0L) {
          missingOut[0]++;
          continue;
        }
        final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
        String gv = dictCache[dictId];
        if (gv == null) {
          final int byteOff = dictByteOff[dictId];
          final int len = getIntLE(payload, lenHeaderOff + dictId * 4);
          final long h = fnv1a64(payload, byteOff, len);
          gv = intern.get(h);
          if (gv == null) {
            gv = new String(payload, byteOff, len, StandardCharsets.UTF_8);
            intern.put(h, gv);
          }
          dictCache[dictId] = gv;
        }
        out.addTo(gv, 1L);
      }
    }
  }

  /**
   * Conjunctive filter + numeric aggregate over a {@link ProjectionIndexLeafPage#COLUMN_KIND_NUMERIC_LONG}
   * column: for every row matching {@code predicates}, folds the column value
   * into {@code acc} = {@code [count, sum, min, max]}. The column sweep is a
   * straight {@code long[rowCount]} read per leaf — memory-bandwidth bound,
   * the same shape a column store executes.
   */
  public static void conjunctiveAggregateNumeric(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int numericColumn,
      final long[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final byte kind = payload[24 + numericColumn];
      if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
        throw new IllegalStateException("aggregate column " + numericColumn
            + " is not NUMERIC_LONG (kind=" + kind + ")");
      }
      final int base = s.columnDataOff[numericColumn];
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      // Sparse semantics: rows on which the aggregated field is MISSING
      // contribute nothing (the interpreter's deref yields the empty
      // sequence there) — AND the match mask with the column's presence.
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart >= 0) {
        final int presOff = presenceWordsOff(payload, tailStart, numericColumn);
        for (int w = 0; w < stride; w++) {
          scanMask[w] &= getLongLE(payload, presOff + w * 8);
        }
      }
      long count = acc[0];
      long sum = acc[1];
      long min = acc[2];
      long max = acc[3];
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        // Fast path: a fully-set word covering valid rows — sweep 64 values
        // without per-bit branching.
        final int rowBase = w << 6;
        if (word == -1L && rowBase + 64 <= rowCount) {
          for (int i = 0; i < 64; i++) {
            final long v = getLongLE(payload, base + (rowBase + i) * 8);
            count++;
            sum += v;
            if (v < min) min = v;
            if (v > max) max = v;
          }
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) break;
          final long v = getLongLE(payload, base + rowIdx * 8);
          count++;
          sum += v;
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * {@link #conjunctiveAggregateNumeric} for {@link ProjectionIndexLeafPage#COLUMN_KIND_NUMERIC_DOUBLE}
   * columns: cells hold the order-preserving transform ({@link ProjectionDoubleEncoding}), so
   * the sweep decodes each matching value (two-op inverse) and folds into
   * {@code acc} = {@code [count, sum, min, max]} as doubles ({@code count} is exact well past
   * any reachable row count). Callers initialise {@code acc} to
   * {@code {0, 0, +Infinity, -Infinity}} and, for run-to-run determinism, merge per-leaf
   * partials in ascending leaf order (double addition is not associative).
   */
  public static void conjunctiveAggregateNumericDouble(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int numericColumn,
      final double[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final byte kind = payload[24 + numericColumn];
      if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        throw new IllegalStateException("aggregate column " + numericColumn
            + " is not NUMERIC_DOUBLE (kind=" + kind + ")");
      }
      final int base = s.columnDataOff[numericColumn];
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart >= 0) {
        final int presOff = presenceWordsOff(payload, tailStart, numericColumn);
        for (int w = 0; w < stride; w++) {
          scanMask[w] &= getLongLE(payload, presOff + w * 8);
        }
      }
      double count = acc[0];
      double sum = acc[1];
      double min = acc[2];
      double max = acc[3];
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final int rowBase = w << 6;
        if (word == -1L && rowBase + 64 <= rowCount) {
          for (int i = 0; i < 64; i++) {
            final double v = ProjectionDoubleEncoding.decode(getLongLE(payload, base + (rowBase + i) * 8));
            count++;
            sum += v;
            if (v < min) min = v;
            if (v > max) max = v;
          }
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) break;
          final double v = ProjectionDoubleEncoding.decode(getLongLE(payload, base + rowIdx * 8));
          count++;
          sum += v;
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Multi-key conjunctive group-by-count over {@code groupColumns} (each MUST be
   * {@link ProjectionIndexLeafPage#COLUMN_KIND_STRING_DICT}). For every matching
   * row the composite key over all group columns is counted. Keys in {@code out}
   * use the executor's typed composite encoding — one {@code 's<len>:<utf8>'}
   * segment per column (or {@code 'm'} for a MISSING field, mirroring the typed
   * slot-walk kernel's missing bucket), in {@code groupColumns} order — so
   * callers can decode group records with the same machinery as the typed
   * slot-walk kernel.
   *
   * <p>Sparse semantics: when the leaf carries a presence tail, rows on
   * which a group field is missing contribute the {@code 'm'} segment instead
   * of the stored default. Leaves without a readable presence tail keep the dense
   * behavior — callers must gate via {@link #probeSparseEvidence} when the
   * data may be sparse.
   *
   * <p>Per-leaf, per-cell lazy compose: the composite string for a
   * (dictIdA, dictIdB, ...) cell is built at most once per leaf via a packed-id
   * cache, so the hot loop is one array/hash probe + one {@code addTo} per row.
   */
  public static void conjunctiveCountByGroupMulti(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] groupColumns,
      final Object2LongOpenHashMap<String> out) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int m = groupColumns.length;
    final ScanScratch s = SCRATCH.get();
    final it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<String> cellCache =
        new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>(64);
    final int[] dictSizes = new int[m];
    final int[] lenHeaderOffs = new int[m];
    final int[] idsOffs = new int[m];
    final int[] presOffs = new int[m];
    final int[][] dictByteOffs = new int[m][];
    final StringBuilder kb = new StringBuilder(32);
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      long cellStride = 1L;
      for (int g = 0; g < m; g++) {
        final int col = groupColumns[g];
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("groupColumn " + col + " is not STRING_DICT (kind=" + kind + ")");
        }
        final int base = s.columnDataOff[col];
        final int dictSize = getIntLE(payload, base);
        dictSizes[g] = dictSize;
        lenHeaderOffs[g] = base + 4;
        final int concatOff = lenHeaderOffs[g] + dictSize * 4;
        int[] offs = dictByteOffs[g];
        if (offs == null || offs.length < dictSize) {
          offs = new int[Math.max(64, dictSize)];
          dictByteOffs[g] = offs;
        }
        int running = concatOff;
        for (int i = 0; i < dictSize; i++) {
          offs[i] = running;
          running += getIntLE(payload, lenHeaderOffs[g] + i * 4);
        }
        idsOffs[g] = running;
        presOffs[g] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, col) : -1;
        // Radix per column is dictSize + 1: the extra symbol encodes MISSING.
        cellStride *= dictSize + 1L;
      }
      // Per-leaf cell cache only valid for THIS leaf's dict ids.
      cellCache.clear();
      final boolean packable = cellStride <= (1L << 62) && m <= 8;
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          long cell = 0L;
          for (int g = 0; g < m; g++) {
            final boolean missing = presOffs[g] >= 0
                && (getLongLE(payload, presOffs[g] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) == 0L;
            final int id = missing ? dictSizes[g] : getIntLE(payload, idsOffs[g] + rowIdx * 4);
            cell = cell * (dictSizes[g] + 1L) + id;
          }
          String key = packable ? cellCache.get(cell) : null;
          if (key == null) {
            kb.setLength(0);
            long rem = cell;
            // decode ids back out in reverse, then emit in order
            final int[] ids = s.dictRemap != null && s.dictRemap.length >= m ? s.dictRemap : (s.dictRemap = new int[Math.max(8, m)]);
            for (int g = m - 1; g >= 0; g--) {
              ids[g] = (int) (rem % (dictSizes[g] + 1L));
              rem /= dictSizes[g] + 1L;
            }
            for (int g = 0; g < m; g++) {
              final int id = ids[g];
              if (id == dictSizes[g]) {
                // Missing field — same segment the typed slot-walk kernel emits.
                kb.append('m');
                continue;
              }
              final int len = getIntLE(payload, lenHeaderOffs[g] + id * 4);
              final int off = dictByteOffs[g][id];
              // Composite segments carry CHAR counts (the decoder substrings by
              // chars) — byte length diverges for non-ASCII dictionary values.
              final String v = new String(payload, off, len, java.nio.charset.StandardCharsets.UTF_8);
              kb.append('s').append(v.length()).append(':').append(v);
            }
            key = kb.toString();
            if (packable) cellCache.put(cell, key);
          }
          out.addTo(key, 1L);
        }
      }
    }
  }

  /**
   * Dense multi-key conjunctive group-by-count: the composite-key analog of
   * {@link #conjunctiveCountByGroupDense}. Instead of materialising a
   * composite {@code String} key per cell and paying two hash probes per
   * matching row ({@link #conjunctiveCountByGroupMulti}), each group column
   * gets a per-leaf {@code dictId → canonicalId} remap, and every matching
   * row does exactly one mixed-radix array increment:
   * {@code counts[((idA) * (lenB+1) + idB) ...]++} where index
   * {@code canonLen_g} encodes MISSING for column {@code g}.
   *
   * <p>Leaves whose dictionary carries a value absent from the canonical
   * dict fall back to the composite hashmap path
   * ({@link #conjunctiveCountByGroupMulti}) for that leaf only, into
   * {@code fallbackOut} — the caller merges both accumulators (decode the
   * counts array with the same mixed radix, then {@code addTo}).
   *
   * <p>HFT-grade: zero hashmap ops, zero String materialisation and zero
   * hashing on the per-row path; per-leaf remap cost is
   * {@code dictSize × canonLen} tiny byte-compares per column.
   *
   * @param counts caller-zeroed accumulator of length
   *               {@code prod(canonicalDicts[g].length + 1)}, mixed-radix
   *               ordered with column 0 as the most significant digit.
   */
  public static void conjunctiveCountByGroupMultiDense(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] groupColumns,
      final byte[][][] canonicalDicts,
      final long[] counts,
      final Object2LongOpenHashMap<String> fallbackOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int m = groupColumns.length;
    if (canonicalDicts == null || canonicalDicts.length != m) {
      throw new IllegalArgumentException("canonicalDicts must be per-group-column");
    }
    long expectedSize = 1L;
    for (int g = 0; g < m; g++) {
      expectedSize *= canonicalDicts[g].length + 1L;
    }
    if (counts == null || counts.length < expectedSize) {
      throw new IllegalArgumentException("counts[] too small for canonical dict product");
    }
    final ScanScratch s = SCRATCH.get();
    final int[][] remaps = new int[m][];
    final int[] dictSizes = new int[m];
    final int[] idsOffs = new int[m];
    final int[] presOffs = new int[m];
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      boolean needsFallback = false;
      for (int g = 0; g < m; g++) {
        final int col = groupColumns[g];
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("groupColumn " + col + " is not STRING_DICT (kind=" + kind + ")");
        }
        final int base = s.columnDataOff[col];
        final int dictSize = getIntLE(payload, base);
        dictSizes[g] = dictSize;
        final int lenHeaderOff = base + 4;
        final int concatOff = lenHeaderOff + dictSize * 4;
        int[] remap = remaps[g];
        if (remap == null || remap.length < dictSize) {
          remap = new int[Math.max(64, dictSize)];
          remaps[g] = remap;
        }
        final byte[][] canon = canonicalDicts[g];
        final int canonLen = canon.length;
        int running = concatOff;
        for (int i = 0; i < dictSize; i++) {
          final int len = getIntLE(payload, lenHeaderOff + i * 4);
          int hit = -1;
          for (int c = 0; c < canonLen; c++) {
            if (bytesEqualAt(payload, running, len, canon[c])) { hit = c; break; }
          }
          remap[i] = hit;
          if (hit < 0) needsFallback = true;
          running += len;
        }
        idsOffs[g] = running;
        presOffs[g] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, col) : -1;
      }
      if (needsFallback) {
        // A dict value outside the canonical dict — run the composite
        // hashmap kernel on this single leaf; caller merges fallbackOut.
        if (fallbackOut == null) {
          throw new IllegalStateException("canonical dict missing value and no fallback provided");
        }
        conjunctiveCountByGroupMulti(List.of(payload), predicates, groupColumns, fallbackOut);
        continue;
      }
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          int idx = 0;
          for (int g = 0; g < m; g++) {
            final boolean missing = presOffs[g] >= 0
                && (getLongLE(payload, presOffs[g] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) == 0L;
            final int id = missing ? canonicalDicts[g].length : remaps[g][getIntLE(payload, idsOffs[g] + rowIdx * 4)];
            idx = idx * (canonicalDicts[g].length + 1) + id;
          }
          counts[idx]++;
        }
      }
    }
  }

  /**
   * Exact distinct PRESENT string values of {@code groupColumn} across all
   * leaves — the count-distinct fast path. Exploits a structural invariant
   * of the leaf format: every non-empty dictionary entry was interned by an
   * actual row of that leaf (missing/unrepresentable rows intern only the
   * {@code ""} default), so with the column gated sparse-clean the distinct
   * set is simply the UNION of the per-leaf dictionaries — no per-row work
   * at all, except to disambiguate a {@code ""} dictionary entry, which may
   * be a phantom from missing rows: on a fully-present leaf it is real; on a
   * leaf with missing rows the packed ids are scanned for a present row
   * referencing it (early exit on first hit).
   *
   * <p>Valid ONLY for the unpredicated case (every row counts) and for
   * sparse-clean columns — callers gate on both.
   *
   * @param cardLimit bail-out bound: exceeding it returns {@code null}
   *                  (caller falls back to the group-counting path, which
   *                  handles any cardinality).
   * @return distinct present values as UTF-8 byte arrays (a {@code ""}
   *         value is included as a zero-length entry when real), or
   *         {@code null} when the kernel cannot serve (kind mismatch,
   *         malformed leaf, missing presence tail, cardinality exceeded).
   */
  public static ArrayList<byte[]> distinctPresentStrings(
      final List<byte[]> leafPayloads, final int groupColumn, final int cardLimit) {
    if (leafPayloads == null) return null;
    final ArrayList<byte[]> distinct = new ArrayList<>(16);
    boolean emptyReal = false;
    for (final byte[] payload : leafPayloads) {
      if (payload == null) return null;
      final int rowCount = getIntLE(payload, 0);
      if (rowCount == 0) continue;
      final int columnCount = getIntLE(payload, 4);
      if (groupColumn < 0 || groupColumn >= columnCount) return null;
      if (payload[24 + groupColumn] != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) return null;
      final int groupBase = columnDataOffFor(payload, groupColumn);
      if (groupBase < 0) return null;
      final int dictSize = getIntLE(payload, groupBase);
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      final int dataEnd = leafDataEnd(payload);
      final int tailStart = dataEnd < 0 ? -1 : presenceTailStart(payload, dataEnd);
      if (tailStart < 0) return null;
      final int presOff = presenceWordsOff(payload, tailStart, groupColumn);
      int emptyId = -1;
      int running = concatOff;
      for (int i = 0; i < dictSize; i++) {
        final int len = getIntLE(payload, lenHeaderOff + i * 4);
        if (len == 0) {
          emptyId = i;
        } else {
          boolean present = false;
          final int n = distinct.size();
          for (int c = 0; c < n; c++) {
            if (bytesEqualAt(payload, running, len, distinct.get(c))) { present = true; break; }
          }
          if (!present) {
            if (distinct.size() >= cardLimit) return null;
            final byte[] copy = new byte[len];
            System.arraycopy(payload, running, copy, 0, len);
            distinct.add(copy);
          }
        }
        running += len;
      }
      if (emptyId >= 0 && !emptyReal) {
        final int idsOff = running;
        final int presWords = (rowCount + 63) >>> 6;
        boolean allPresent = true;
        for (int w = 0; w < presWords; w++) {
          final long expect = w == presWords - 1 && (rowCount & 63) != 0
              ? (1L << (rowCount & 63)) - 1
              : -1L;
          if ((getLongLE(payload, presOff + w * 8) & expect) != expect) {
            allPresent = false;
            break;
          }
        }
        if (allPresent) {
          // Every row is present, so the "" entry was interned by a present row.
          emptyReal = true;
        } else {
          for (int r = 0; r < rowCount; r++) {
            if ((getLongLE(payload, presOff + (r >>> 6) * 8) & (1L << (r & 63))) == 0L) continue;
            if (getIntLE(payload, idsOff + r * 4) == emptyId) {
              emptyReal = true;
              break;
            }
          }
        }
      }
    }
    if (emptyReal) {
      distinct.add(new byte[0]);
    }
    return distinct;
  }

  /**
   * Conjunctive filter + group-by-count: walks {@code leafPayloads} with the
   * supplied {@code predicates}, then for every matching row reads the
   * {@code groupColumn}'s UTF-8 string value and increments the matching
   * group counter in {@code out}. The group column MUST be
   * {@link ProjectionIndexLeafPage#COLUMN_KIND_STRING_DICT}.
   *
   * <p>Per-leaf dict decode is lazy: each dict-id referenced by a matching
   * row is decoded at most once per leaf via a small {@code String[]}
   * cache. Group-counter updates use {@link Object2LongOpenHashMap#addTo}
   * — one hashmap op per match, no box-on-insert.
   */
  public static void conjunctiveCountByGroup(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int groupColumn,
      final Object2LongOpenHashMap<String> out) {
    conjunctiveCountByGroup(leafPayloads, predicates, groupColumn, out, null);
  }

  /**
   * Sparse-aware variant of {@link #conjunctiveCountByGroup(Iterable,
   * ProjectionIndexScan.ColumnPredicate[], int, Object2LongOpenHashMap)}:
   * matching rows on which the group field is MISSING (presence bit clear)
   * are counted into {@code missingOut[0]} instead of polluting the string
   * groups with the stored default. {@code missingOut == null} keeps the
   * dense behavior; leaves without a readable presence tail always use it.
   */
  public static void conjunctiveCountByGroup(final Iterable<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int groupColumn,
      final Object2LongOpenHashMap<String> out,
      final long[] missingOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    // Thread-local scratch + long-hash string intern. The intern map
    // reduces the 8 dept values shared across 97 K leaves to 8 String
    // allocations total (per thread, per scan), not 776 K. Keying by
    // long-hash keeps the lookup fully primitive — no ByteKey / String
    // object alloc on lookup. The per-leaf dictCache/dictByteOff
    // buffers are hoisted here too.
    final ScanScratch s = SCRATCH.get();
    if (s.dictCache == null) {
      s.dictCache = new String[64];
      s.dictByteOff = new int[64];
    }
    if (s.stringIntern == null) {
      s.stringIntern = new it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<>(32);
    }
    String[] dictCache = s.dictCache;
    int[] dictByteOff = s.dictByteOff;
    final it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<String> intern = s.stringIntern;
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn
            + " is not STRING_DICT (kind=" + groupKind + ")");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int dictSize = getIntLE(payload, groupBase);
      if (dictCache.length < dictSize) {
        final int newSize = Math.max(dictCache.length * 2, dictSize);
        dictCache = new String[newSize];
        dictByteOff = new int[newSize];
        s.dictCache = dictCache;
        s.dictByteOff = dictByteOff;
      } else {
        for (int i = 0; i < dictSize; i++) dictCache[i] = null;
      }
      // Layout: [int dictSize][int[dictSize] lengths][concat bytes][int[rowCount] ids]
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      // Prefix-sum the lengths to get per-dict-id byte offsets (and the
      // ids-array base as a side product). One pass, no re-scan on miss.
      int running = concatOff;
      for (int i = 0; i < dictSize; i++) {
        dictByteOff[i] = running;
        running += getIntLE(payload, lenHeaderOff + i * 4);
      }
      final int idsOff = running;
      // Sparse semantics: matching rows missing the group field go to the
      // caller's missing bucket instead of the stored default's group.
      final int tailStart = missingOut != null ? presenceTailStart(payload, s.leafDataEnd) : -1;
      final int presOff = tailStart >= 0 ? presenceWordsOff(payload, tailStart, groupColumn) : -1;

      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long presWord = presOff >= 0 ? getLongLE(payload, presOff + w * 8) : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          if (presOff >= 0 && (presWord & (1L << bit)) == 0L) {
            missingOut[0]++;
            continue;
          }
          final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
          String gv = dictCache[dictId];
          if (gv == null) {
            final int byteOff = dictByteOff[dictId];
            final int len = getIntLE(payload, lenHeaderOff + dictId * 4);
            // Lookup by 64-bit FNV-1a hash — zero-alloc hit path.
            // Collision rate at N=10^7 distinct values ≈ 10⁻¹⁹.
            final long h = fnv1a64(payload, byteOff, len);
            gv = intern.get(h);
            if (gv == null) {
              gv = new String(payload, byteOff, len, StandardCharsets.UTF_8);
              intern.put(h, gv);
            }
            dictCache[dictId] = gv;
          }
          out.addTo(gv, 1L);
        }
      }
    }
  }

  private static int columnCountOf(final byte[] payload) {
    return getIntLE(payload, 4);
  }

  // ------------------------------------------------------------------
  // Presence tail (sparse-field correctness). Layout appended AFTER the
  // column stream — see ProjectionIndexLeafPage's class javadoc:
  //   byte[columnCount] columnFlags        (bit0 = unrepresentable seen,
  //                                         bit1 = non-integral seen)
  //   long[presWords] presence per column  (only when rowCount > 0)
  //   int tailLen; byte version; int magic = "PIX1"
  // ------------------------------------------------------------------

  /**
   * Start offset of the presence tail given the EXACT end of the column
   * data stream, or {@code -1} when the trailing bytes don't form a valid
   * tail (malformed payload). The boundary equality check makes false
   * positives impossible — a truncated payload can never be misread.
   */
  static int presenceTailStart(final byte[] payload, final int dataEnd) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int presWords = rowCount > 0 ? (rowCount + 63) >>> 6 : 0;
    final int tailLen = columnCount + columnCount * presWords * 8;
    if (payload.length != dataEnd + tailLen + 9) return -1;
    if (getIntLE(payload, payload.length - 4) != ProjectionIndexLeafPage.PRESENCE_TAIL_MAGIC) return -1;
    if (payload[payload.length - 5] != ProjectionIndexLeafPage.PRESENCE_TAIL_VERSION) return -1;
    if (getIntLE(payload, payload.length - 9) != tailLen) return -1;
    return dataEnd;
  }

  /** Byte offset of {@code column}'s presence words inside a leaf whose tail starts at {@code tailStart}. */
  private static int presenceWordsOff(final byte[] payload, final int tailStart, final int column) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int presWords = (rowCount + 63) >>> 6;
    return tailStart + columnCount + column * presWords * 8;
  }

  /**
   * End offset of the column data stream — header + recordKeys + all
   * column bodies. Walks the column directory; used by the one-shot evidence
   * probe (the hot kernels get the boundary from {@link #evaluateLeafMask}).
   * Returns {@code -1} on structural inconsistency.
   */
  static int leafDataEnd(final byte[] payload) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    if (rowCount == 0) return kindsOff + columnCount;
    int cursor = kindsOff + columnCount + rowCount * 8;
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE -> cursor += rowCount * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        default -> { return -1; }
      }
    }
    return cursor;
  }

  /** Sparse-evidence status: not yet probed. */
  public static final byte SPARSE_STATUS_UNKNOWN = 0;
  /** Every leaf carries presence data and the column never saw an unrepresentable value. */
  public static final byte SPARSE_STATUS_CLEAN = 1;
  /** Some leaf lacks a valid presence tail (malformed) or saw an unrepresentable value — fail closed. */
  public static final byte SPARSE_STATUS_DIRTY = 2;

  /**
   * One-shot probe over ALL leaves: per column, decide whether sparse-correct
   * semantics can be served from the projection. A column is CLEAN iff every
   * leaf carries a valid presence tail AND never flags the column as
   * unrepresentable (JSON null / object / array / kind-mismatch values poison
   * the column — a present row's stored default is not the real value).
   * Anything else is DIRTY — consumers must fall back (typed scan kernels /
   * generic pipeline), which is always correct.
   *
   * @return per-column status array of {@link #SPARSE_STATUS_CLEAN} /
   *         {@link #SPARSE_STATUS_DIRTY}, sized {@code columnCount};
   *         zero-length when the leaf list is empty.
   */
  public static byte[] probeSparseEvidence(final List<byte[]> leafPayloads) {
    if (leafPayloads == null || leafPayloads.isEmpty()) return new byte[0];
    final byte[] first = leafPayloads.get(0);
    if (first == null) return new byte[0];
    final int columnCount = columnCountOf(first);
    final byte[] status = new byte[columnCount];
    Arrays.fill(status, SPARSE_STATUS_CLEAN);
    for (final byte[] payload : leafPayloads) {
      if (payload == null || columnCountOf(payload) != columnCount) {
        Arrays.fill(status, SPARSE_STATUS_DIRTY);
        return status;
      }
      final int dataEnd = leafDataEnd(payload);
      final int tailStart = dataEnd < 0 ? -1 : presenceTailStart(payload, dataEnd);
      if (tailStart < 0) {
        // Malformed leaf — no trustworthy presence info; every column is dirty.
        Arrays.fill(status, SPARSE_STATUS_DIRTY);
        return status;
      }
      for (int c = 0; c < columnCount; c++) {
        if ((payload[tailStart + c] & ProjectionIndexLeafPage.COLUMN_FLAG_UNREPRESENTABLE) != 0) {
          status[c] = SPARSE_STATUS_DIRTY;
        }
      }
    }
    return status;
  }

  /**
   * One-shot probe over ALL leaves: recover per-column NUMERIC_LONG
   * integrality provenance from the persisted bytes. A column's flag is the
   * OR of {@link ProjectionIndexLeafPage#COLUMN_FLAG_NON_INTEGRAL} across
   * every leaf — {@code true} means some cell was truncated from a
   * non-integral number and value-exact consumers must decline the column.
   *
   * <p>Returns {@code null} — integrality UNKNOWN, consumers must fail
   * closed — when any leaf lacks a valid presence tail (malformed payload):
   * fabricating "integral" for such leaves would let aggregates return
   * truncated sums. This is the persistence-safe
   * counterpart of {@code ProjectionIndexBuilder#numericColumnNonIntegralFlags()}:
   * it lets a re-opened resource re-derive the flags that previously lived
   * only in builder memory.
   *
   * @return per-column non-integral flags sized {@code columnCount}, or
   *         {@code null} when any leaf lacks the presence tail; zero-length
   *         when the leaf list is empty.
   */
  public static boolean[] probeNumericNonIntegral(final List<byte[]> leafPayloads) {
    if (leafPayloads == null || leafPayloads.isEmpty()) return new boolean[0];
    final byte[] first = leafPayloads.get(0);
    if (first == null || first.length < 8) return null;
    final int columnCount = columnCountOf(first);
    if (columnCount < 0) return null;
    final boolean[] nonIntegral = new boolean[columnCount];
    try {
      for (final byte[] payload : leafPayloads) {
        if (payload == null || payload.length < 8 || columnCountOf(payload) != columnCount) return null;
        final int dataEnd = leafDataEnd(payload);
        final int tailStart = dataEnd < 0 ? -1 : presenceTailStart(payload, dataEnd);
        if (tailStart < 0) return null;
        for (int c = 0; c < columnCount; c++) {
          if ((payload[tailStart + c] & ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL) != 0) {
            nonIntegral[c] = true;
          }
        }
      }
    } catch (final IndexOutOfBoundsException truncated) {
      // Malformed / truncated payload — fail closed per the contract.
      return null;
    }
    return nonIntegral;
  }

  private static long countLeaf(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final ScanScratch s) {
    final int rowCount = evaluateLeafMask(payload, predicates, s);
    if (rowCount <= 0) return 0L;
    final int stride = (rowCount + 63) >>> 6;
    long result = 0;
    final long[] mask = s.mask;
    for (int i = 0; i < stride; i++) result += Long.bitCount(mask[i]);
    return result;
  }

  /**
   * Parse leaf offsets, apply zone-map pruning, and compute the final
   * conjunctive predicate mask into {@code s.mask}. Returns the leaf's
   * {@code rowCount} (possibly with a zeroed-out mask when zone-map
   * rules out the page), or {@code 0} for empty leaves / zone-map
   * skips — callers should treat {@code 0} as "nothing to do".
   *
   * <p>Sparse-field semantics: when the leaf carries a presence tail,
   * every predicate's match bits are AND-ed with the predicate column's
   * presence bitmap — a comparison/EBV over a MISSING field evaluates over
   * the empty sequence and is FALSE in XQuery, never "matches the stored
   * default". Leaves without a readable tail keep the all-present
   * behavior; sparse-correct callers must gate on
   * {@link #probeSparseEvidence} before trusting them.
   *
   * <p>The mask is sized to {@code ceil(MAX_ROWS/64)}; only the first
   * {@code ceil(rowCount/64)} words are populated. As a side effect
   * {@code s.leafDataEnd} records where the column stream ends.
   */
  private static int evaluateLeafMask(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final ScanScratch s) {
    final int[] columnDataOff = s.columnDataOff;
    final int[] columnMinMaxOff = s.columnMinMaxOff;
    final long[] numericScratch = s.numericScratch;
    final long[] mask = s.mask;
    final long[] colMask = s.colMask;
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    final int recordKeysOff = kindsOff + columnCount;
    if (rowCount == 0) {
      s.leafDataEnd = recordKeysOff;
      return 0;
    }

    // Compute column offsets in one pass. Each column starts with
    // (min, max) 16 bytes, then its kind-specific data.
    int cursor = recordKeysOff + rowCount * 8;
    for (int c = 0; c < columnCount; c++) {
      columnMinMaxOff[c] = cursor;
      cursor += 16;
      columnDataOff[c] = cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE -> cursor += rowCount * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
    }
    s.leafDataEnd = cursor;

    // Zone-map prune — numeric columns only (same policy as the
    // materialising variant). Zone maps fold in only PRESENT,
    // representable values, so an all-missing leaf prunes outright.
    for (final var p : predicates) {
      final byte kind = payload[kindsOff + p.column];
      if (!ProjectionIndexLeafPage.isNumericKind(kind)) continue;
      final long min = getLongLE(payload, columnMinMaxOff[p.column]);
      final long max = getLongLE(payload, columnMinMaxOff[p.column] + 8);
      if (min > max) return 0;  // no present value in the column at all
      if (zoneSkip(p, min, max)) return 0;
    }

    final int tailStart = presenceTailStart(payload, cursor);

    // Build the conjunctive mask over the caller-provided buffers.
    final int stride = (rowCount + 63) >>> 6;
    fillAllTrue(mask, rowCount);
    for (final var p : predicates) {
      // Only clear the live prefix of colMask — tail words beyond
      // `stride` are never read.
      Arrays.fill(colMask, 0, stride, 0L);
      final byte kind = payload[kindsOff + p.column];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE -> evalNumericBytes(
            payload, columnDataOff[p.column], rowCount, p.op, p.longLit, p.highLit, numericScratch, colMask);
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> evalBooleanBytes(
            payload, columnDataOff[p.column], rowCount, p.boolLit, colMask);
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> evalStringEqBytes(
            payload, columnDataOff[p.column], rowCount, p.stringLitBytes, colMask);
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
      if (tailStart >= 0) {
        // Missing field ⇒ predicate is false — AND with the column's presence.
        final int presOff = presenceWordsOff(payload, tailStart, p.column);
        for (int i = 0; i < stride; i++) {
          mask[i] &= colMask[i] & getLongLE(payload, presOff + i * 8);
        }
      } else {
        for (int i = 0; i < stride; i++) mask[i] &= colMask[i];
      }
    }
    return rowCount;
  }

  private static boolean zoneSkip(final ProjectionIndexScan.ColumnPredicate p,
      final long min, final long max) {
    return switch (p.op) {
      case GT -> max <= p.longLit;
      case LT -> min >= p.longLit;
      case GE -> max < p.longLit;
      case LE -> min > p.longLit;
      case EQ -> p.longLit < min || p.longLit > max;
      // BETWEEN zone-skip: OR of the two single-bound zone-skip conditions.
      // Strictly no more pessimistic than two independent predicates — see
      // iter07-range-fusion-analysis.md for the semantics derivation.
      case BETWEEN_GT_LT -> max <= p.longLit || min >= p.highLit;
      case BETWEEN_GT_LE -> max <= p.longLit || min > p.highLit;
      case BETWEEN_GE_LT -> max < p.longLit || min >= p.highLit;
      case BETWEEN_GE_LE -> max < p.longLit || min > p.highLit;
    };
  }

  /**
   * 64-bit FNV-1a hash over a byte slice. Stable across JVMs; adequate
   * collision resistance for the group-by intern ({@code ~10⁻¹⁹} at 10M
   * distinct values). Matches the hash used in
   * {@link io.sirix.page.pax.StringRegion.Encoder} so encoder / decoder
   * agree on key space when interop is needed.
   */
  private static long fnv1a64(final byte[] data, final int off, final int len) {
    long h = 0xcbf29ce484222325L;
    final int end = off + len;
    for (int i = off; i < end; i++) {
      h ^= data[i] & 0xFF;
      h *= 0x100000001b3L;
    }
    return h;
  }

  /**
   * SIMD-accelerated numeric compare. Hybrid scalar-load + SIMD-eval:
   * copies the numeric column out of the byte[] payload into a reusable
   * {@code long[]} scratch via the byte-array {@link VarHandle} (HotSpot
   * fully intrinsifies to a tight MOVQ loop), then runs the compare via
   * {@link LongVector#fromArray} + {@link LongVector#compare(VectorOperators.Comparison, long)}
   * + {@link VectorMask#toLong()} and OR's the bits into the output
   * packed-bit mask at the correct position.
   *
   * <p>This detour avoids {@link LongVector#fromMemorySegment}, which
   * funnels through {@code ScopedMemoryAccess.loadFromMemorySegmentScopedInternal}
   * and does per-leaf session/scope validation — checks that aren't
   * hoisted out of the hot loop and which prevented the intrinsic path
   * from kicking in at all in profiling.
   */
  private static void evalNumericBytes(final byte[] payload,
      final int baseOff, final int rowCount, final ProjectionIndexScan.Op op,
      final long lit, final long highLit, final long[] scratch, final long[] out) {
    // 1) Load column into scratch — fully intrinsified (MOVQ per lane).
    //    Shared between single-bound and BETWEEN paths: the fused range
    //    eliminates the *second* load that the un-fused pair would do,
    //    which is the dominant saving (~8 KB/leaf × 97,657 leaves).
    for (int k = 0; k < rowCount; k++) {
      scratch[k] = getLongLE(payload, baseOff + k * 8);
    }
    // BETWEEN range ops fuse two opposing-direction compares into one
    // SIMD pass. Kept as a distinct branch so the single-bound op arm
    // stays a single MOVM (one compare + mask store).
    switch (op) {
      case BETWEEN_GT_LT -> evalBetween(scratch, rowCount, VectorOperators.GT, lit,
          VectorOperators.LT, highLit, out);
      case BETWEEN_GT_LE -> evalBetween(scratch, rowCount, VectorOperators.GT, lit,
          VectorOperators.LE, highLit, out);
      case BETWEEN_GE_LT -> evalBetween(scratch, rowCount, VectorOperators.GE, lit,
          VectorOperators.LT, highLit, out);
      case BETWEEN_GE_LE -> evalBetween(scratch, rowCount, VectorOperators.GE, lit,
          VectorOperators.LE, highLit, out);
      default -> {
        // iter#15: scalar broadword loop replaces the Vector-API SIMD body.
        // Same rationale as evalBetween: on Oracle GraalVM 25.0.2 with
        // -XX:-UseJVMCICompiler, Long256Vector/Long256Mask and their
        // long[]/boolean[] backings do not escape-analyse even after
        // 200 prewarm iters × 1.9M tier-4 invocations, producing a large
        // fraction of the total cold-100M allocation budget.
        // Dispatch on op kind once, outside the tight loop — the switch
        // is hoisted by C2 at tier-4 and each arm becomes a pure numeric
        // predicate-into-bitmask loop with no virtual dispatch.
        switch (op) {
          case GT -> {
            for (int k = 0; k < rowCount; k++) {
              if (scratch[k] > lit) out[k >>> 6] |= 1L << (k & 63);
            }
          }
          case LT -> {
            for (int k = 0; k < rowCount; k++) {
              if (scratch[k] < lit) out[k >>> 6] |= 1L << (k & 63);
            }
          }
          case GE -> {
            for (int k = 0; k < rowCount; k++) {
              if (scratch[k] >= lit) out[k >>> 6] |= 1L << (k & 63);
            }
          }
          case LE -> {
            for (int k = 0; k < rowCount; k++) {
              if (scratch[k] <= lit) out[k >>> 6] |= 1L << (k & 63);
            }
          }
          case EQ -> {
            for (int k = 0; k < rowCount; k++) {
              if (scratch[k] == lit) out[k >>> 6] |= 1L << (k & 63);
            }
          }
          default -> throw new IllegalStateException("unreachable: BETWEEN handled above");
        }
      }
    }
  }

  /**
   * Fused BETWEEN evaluator: one SIMD-load pass produces the
   * {@code (lowCmp, lowLit) AND (highCmp, highLit)} bitmap. The scratch
   * buffer is expected to hold the column values (caller populates).
   *
   * <p>Cost-per-leaf (AVX-512, LANES=8, 1024 rows):
   * 128 iters × {2 vector compares, 1 AND, 1 OR-into-mask} ≈ 300 ns
   * compute + ~1 µs scratch-load shared with the single-bound path.
   * The un-fused pair would pay <b>two</b> scratch loads (~2 µs) for
   * the same result.
   *
   * <p>HFT invariants: no allocation, no virtual dispatch, {@code final}
   * on all parameters, tableswitch folded by C2 at the call site.
   */
  private static void evalBetween(final long[] scratch, final int rowCount,
      final VectorOperators.Comparison lowCmp, final long lowLit,
      final VectorOperators.Comparison highCmp, final long highLit, final long[] out) {
    // iter#15: scalar broadword loop replaces the Vector-API SIMD body. The
    // jdk.incubator.vector path allocated Long256Vector, Long256Mask plus
    // backing long[]/boolean[] per call (7.72 GB / 62.9% of total alloc per
    // cold-100M run, per iter#14 alloc profile) because escape-analysis fails
    // on Oracle GraalVM 25.0.2 with -XX:-UseJVMCICompiler. HotSpot C2
    // auto-vectorises this predicate-into-bitmask pattern at tier-4; even
    // if it doesn't, the per-cell cost is amortised by the elimination of
    // ~7.72 GB of allocation over the bench.
    final boolean lowIncl = lowCmp == VectorOperators.GE;
    final boolean highIncl = highCmp == VectorOperators.LE;
    for (int k = 0; k < rowCount; k++) {
      final long v = scratch[k];
      final boolean loOk = lowIncl ? (v >= lowLit) : (v > lowLit);
      final boolean hiOk = highIncl ? (v <= highLit) : (v < highLit);
      if (loOk & hiOk) {
        out[k >>> 6] |= 1L << (k & 63);
      }
    }
  }

  private static void evalBooleanBytes(final byte[] payload, final int baseOff,
      final int rowCount, final boolean wantTrue, final long[] out) {
    final int stride = (rowCount + 63) >>> 6;
    if (wantTrue) {
      for (int i = 0; i < stride; i++) {
        out[i] = getLongLE(payload, baseOff + i * 8);
      }
    } else {
      for (int i = 0; i < stride; i++) {
        out[i] = ~getLongLE(payload, baseOff + i * 8);
      }
      final int tail = rowCount & 63;
      if (tail != 0) out[stride - 1] &= (1L << tail) - 1L;
    }
  }

  private static void evalStringEqBytes(final byte[] payload, final int baseOff,
      final int rowCount, final byte[] literal, final long[] out) {
    // Dict header: [int dictSize][int[dictSize] lengths][concat bytes][int[rowCount] ids]
    final int dictSize = getIntLE(payload, baseOff);
    int concatOff = baseOff + 4 + dictSize * 4;
    int targetDictId = -1;
    for (int i = 0; i < dictSize; i++) {
      final int len = getIntLE(payload, baseOff + 4 + i * 4);
      if (bytesEqualAt(payload, concatOff, len, literal)) {
        targetDictId = i;
        // Don't break — still need concatOff to advance past all dict entries
        // to find the ids region. Take shortcut: we know ids start at
        // concatOff + totalLengthsRemaining from here.
      }
      concatOff += len;
    }
    if (targetDictId < 0) return;
    final int idsOff = concatOff;
    for (int i = 0; i < rowCount; i++) {
      if (getIntLE(payload, idsOff + i * 4) == targetDictId) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
  }

  private static boolean bytesEqualAt(final byte[] a, final int aOff, final int len, final byte[] b) {
    if (len != b.length) return false;
    for (int i = 0; i < len; i++) if (a[aOff + i] != b[i]) return false;
    return true;
  }

  private static void fillAllTrue(final long[] mask, final int rowCount) {
    final int fullWords = rowCount >>> 6;
    for (int i = 0; i < fullWords; i++) mask[i] = -1L;
    final int tail = rowCount & 63;
    if (tail != 0) mask[fullWords] = (1L << tail) - 1L;
  }
}
