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

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

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
    // Per-row 0/1 compare flags — target of the SuperWord-vectorised compare
    // pass in evalNumericBytes, packed into colMask afterwards.
    final long[] numericFlags = new long[ProjectionIndexLeafPage.MAX_ROWS];
    final long[] mask = new long[(ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6];
    final long[] colMask = new long[(ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6];
    // Lazily-sized dict byte-offset cache + String cache for the group-by
    // variant. null on threads that only do conjunctiveCount.
    String[] dictCache;
    int[] dictByteOff;
    // Per-aggregate-column presence/value offsets for the group-aggregate
    // kernel; lazily sized to the aggregate column count.
    int[] groupAggPresOff;
    int[] groupAggValOff;
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
            // Double.compare total order, NOT IEEE < / >: the interpreter's min/max
            // (MinMaxAggregator via Atomic.cmp) distinguishes -0.0 < 0.0, and served
            // results must pick the identical winner. Ties keep the first-seen value on
            // both pipelines.
            if (Double.compare(v, min) < 0) min = v;
            if (Double.compare(v, max) > 0) max = v;
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
          if (Double.compare(v, min) < 0) min = v;
          if (Double.compare(v, max) > 0) max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Pull-cursor over the predicate-matched, presence-filtered cells of one NUMERIC_DOUBLE
   * column, decoded to plain doubles in document order (ascending leaf, ascending row) —
   * the §11-8 serving bridge: the executor wraps this in a brackit {@code Sequence} and
   * feeds ONE continuous stream through brackit's own {@code SumAvgAggregator}, so served
   * sum/avg reproduce the interpreter's exact association order (seeding, batching, SIMD
   * reduction) by construction instead of imitating it.
   *
   * <p>Single-threaded use only: the cursor borrows the calling thread's scan scratch, so
   * no other kernel/probe call may interleave on the same thread between {@link #advance()}
   * calls (the executor drains the aggregator synchronously, which satisfies this).
   */
  public static final class MatchingDoubleCursor {

    private final List<byte[]> leafPayloads;
    private final ProjectionIndexScan.ColumnPredicate[] predicates;
    private final int column;
    private final ScanScratch s = SCRATCH.get();

    private int leafIdx;
    private byte[] payload;
    private int rowCount;
    private int base;
    private int stride;
    private int wordIdx;
    private long word;
    private double current;

    public MatchingDoubleCursor(final List<byte[]> leafPayloads,
        final ProjectionIndexScan.ColumnPredicate[] predicates, final int column) {
      if (predicates == null) {
        throw new IllegalArgumentException("predicates must not be null");
      }
      this.leafPayloads = leafPayloads;
      this.predicates = predicates;
      this.column = column;
    }

    /** Advance to the next matching cell; {@code false} = stream exhausted. */
    public boolean advance() {
      while (true) {
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = ((wordIdx - 1) << 6) + bit;
          if (rowIdx >= rowCount) break;
          current = ProjectionDoubleEncoding.decode(getLongLE(payload, base + rowIdx * 8));
          return true;
        }
        if (payload != null && wordIdx < stride) {
          word = s.mask[wordIdx++];
          continue;
        }
        if (leafIdx >= leafPayloads.size()) {
          return false;
        }
        payload = leafPayloads.get(leafIdx++);
        final int columnCount = columnCountOf(payload);
        if (s.columnDataOff.length < columnCount) {
          s.columnDataOff = new int[columnCount];
          s.columnMinMaxOff = new int[columnCount];
        }
        rowCount = evaluateLeafMask(payload, predicates, s);
        if (rowCount <= 0) {
          payload = null;
          continue;
        }
        final byte kind = payload[24 + column];
        if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE) {
          throw new IllegalStateException("cursor column " + column
              + " is not NUMERIC_DOUBLE (kind=" + kind + ")");
        }
        base = s.columnDataOff[column];
        stride = (rowCount + 63) >>> 6;
        final int tailStart = presenceTailStart(payload, s.leafDataEnd);
        if (tailStart >= 0) {
          final int presOff = presenceWordsOff(payload, tailStart, column);
          for (int w = 0; w < stride; w++) {
            s.mask[w] &= getLongLE(payload, presOff + w * 8);
          }
        }
        wordIdx = 0;
        word = 0L;
      }
    }

    /** The matched cell decoded to its double value; valid after a true {@link #advance()}. */
    public double value() {
      return current;
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

  /**
   * Accumulator slot count for {@link #conjunctiveAggregateByGroup}: per group,
   * {@code [0]=matching rows, [1]=first-seen ordinal, then per aggregate column
   * [count, sum, min, max]}. First-seen ordinals ({@code leafIndex << 20 | rowIdx} —
   * rowIdx bounded by {@link ProjectionIndexLeafPage#MAX_ROWS}) let callers emit groups
   * in DOCUMENT first-appearance order, the interpreter's grouping order.
   */
  public static int groupAggSlots(final int aggColumns) {
    return 2 + 4 * aggColumns;
  }

  /** Fresh per-group accumulator with fold-ready min/max seeds. */
  public static long[] newGroupAggAcc(final int aggColumns, final long firstSeen) {
    final long[] acc = new long[groupAggSlots(aggColumns)];
    acc[1] = firstSeen;
    for (int a = 0; a < aggColumns; a++) {
      acc[2 + 4 * a + 2] = Long.MAX_VALUE;
      acc[2 + 4 * a + 3] = Long.MIN_VALUE;
    }
    return acc;
  }

  /**
   * Per-group NUMERIC_LONG aggregates (P5b stage 7a): the group-by twin of the plain
   * {@link #conjunctiveAggregateNumeric} — matching rows fold {@code [count, sum, min,
   * max]} PER GROUP for every aggregate column, with each aggregate column's own presence
   * AND (missing cells contribute nothing, exactly like the interpreter's per-group
   * {@code sum($r.f)} over records lacking {@code f}). Matching rows whose GROUP field is
   * missing fold into {@code missingAcc} (the null-key group) instead of a string group.
   * Accumulator layout: {@link #groupAggSlots}. Dict interning mirrors
   * {@link #conjunctiveCountByGroup}.
   */
  public static void conjunctiveAggregateByGroup(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int groupColumn,
      final int[] aggColumns,
      final Object2ObjectOpenHashMap<String, long[]> out,
      final long[] missingAcc,
      final int leafIndexBase) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
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
    for (int leaf = 0; leaf < leafPayloads.size(); leaf++) {
      final byte[] payload = leafPayloads.get(leaf);
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
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      int running = concatOff;
      for (int i = 0; i < dictSize; i++) {
        dictByteOff[i] = running;
        running += getIntLE(payload, lenHeaderOff + i * 4);
      }
      final int idsOff = running;
      // Presence: the GROUP column decides null-key routing; every AGGREGATE column
      // gates its own fold. A leaf without a readable tail is malformed for sparse
      // serving — callers gate on columnSparseClean, so treat as dense here.
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int groupPresOff = tailStart >= 0 ? presenceWordsOff(payload, tailStart, groupColumn) : -1;
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggColumns.length
          ? s.groupAggPresOff : (s.groupAggPresOff = new int[Math.max(4, aggColumns.length)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggColumns.length
          ? s.groupAggValOff : (s.groupAggValOff = new int[Math.max(4, aggColumns.length)]);
      for (int a = 0; a < aggColumns.length; a++) {
        // Same fail-loud per-leaf kind check the group column gets: a leaf whose kind
        // byte drifted from the handle metadata must never be folded as longs silently.
        final byte aggKind = payload[24 + aggColumns[a]];
        if (aggKind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
          throw new IllegalStateException("aggColumn " + aggColumns[a]
              + " is not NUMERIC_LONG (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, aggColumns[a]) : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final long leafOrdinalBase = ((long) (leafIndexBase + leaf)) << 20;
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long groupPresWord = groupPresOff >= 0 ? getLongLE(payload, groupPresOff + w * 8) : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          final long[] acc;
          if (groupPresOff >= 0 && (groupPresWord & (1L << bit)) == 0L) {
            acc = missingAcc;
            if (acc[0] == 0) {
              // First missing-group row of this chunk — record its ordinal so the
              // null-key group emits at its document first-appearance position.
              acc[1] = leafOrdinalBase | rowIdx;
            }
          } else {
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
            long[] existing = out.get(gv);
            if (existing == null) {
              existing = newGroupAggAcc(aggColumns.length, leafOrdinalBase | rowIdx);
              out.put(gv, existing);
            }
            acc = existing;
          }
          acc[0]++;
          for (int a = 0; a < aggColumns.length; a++) {
            if (aggPresOff[a] >= 0
                && (getLongLE(payload, aggPresOff[a] + w * 8) & (1L << bit)) == 0L) {
              continue;
            }
            final long v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            final int base = 2 + 4 * a;
            acc[base]++;
            // Exact sum or DECLINE: the interpreter promotes an overflowing xs:integer
            // sum to exact decimal — a wrapped long would silently serve a wrong total.
            acc[base + 1] = Math.addExact(acc[base + 1], v);
            if (v < acc[base + 2]) acc[base + 2] = v;
            if (v > acc[base + 3]) acc[base + 3] = v;
          }
        }
      }
    }
  }

  /**
   * Maximum group columns for {@link #conjunctiveAggregateByGroupMulti}: composite keys
   * pack one 11-bit component per column ({@link ProjectionIndexLeafPage#MAX_ROWS} caps
   * dict ids at 1024, sentinel {@link #GROUP_ID_MISSING} = 2047) into one long — 5 × 11 =
   * 55 bits. Callers must decline shapes with more keys.
   */
  public static final int MAX_GROUP_COLUMNS = 5;

  /** Packed-component sentinel for "group field missing on this row" (11-bit max). */
  private static final int GROUP_ID_MISSING = 0x7FF;

  /**
   * Immutable composite group key for multi-key group-by: the per-key string values in
   * record-entry order, {@code null} components marking rows where that group field is
   * MISSING (the interpreter groups them under the empty-sequence key). Hash is
   * precomputed — the key is a hash-map key on the hot merge path.
   */
  public static final class GroupKey {
    private final String[] parts;
    private final int hash;

    GroupKey(final String[] parts) {
      this.parts = parts;
      this.hash = Arrays.hashCode(parts);
    }

    /** Number of key components. */
    public int size() {
      return parts.length;
    }

    /** Component {@code i}; {@code null} = the missing-field (empty-sequence) key. */
    public String part(final int i) {
      return parts[i];
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      return o instanceof GroupKey other && hash == other.hash
          && Arrays.equals(parts, other.parts);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public String toString() {
      return Arrays.toString(parts);
    }
  }

  /**
   * MULTI-KEY per-group NUMERIC_LONG aggregates (gap item 1a): the N-key generalization of
   * {@link #conjunctiveAggregateByGroup}. Matching rows fold {@code [count, sum, min,
   * max]} per aggregate column into the accumulator of their COMPOSITE group key — the
   * tuple of the row's group-column string values, with {@code null} components for
   * missing cells (so the all-missing row lands in the all-null group, exactly like the
   * interpreter's grouping over empty let-bound keys). Accumulator layout and first-seen
   * ordinals: {@link #groupAggSlots} / {@link #newGroupAggAcc}.
   *
   * <p>Hot-path shape: per leaf, every group column's dictionary is decoded ONCE into a
   * {@code String[]}; per row the group ids pack into one long (11 bits per component) and
   * resolve through a per-leaf combo cache, so the steady-state row cost is one packed-long
   * hash probe — no string work, no allocation.
   */
  public static void conjunctiveAggregateByGroupMulti(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] groupColumns,
      final int[] aggColumns,
      final Object2ObjectOpenHashMap<GroupKey, long[]> out,
      final int leafIndexBase) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int keyCount = groupColumns.length;
    if (keyCount < 2 || keyCount > MAX_GROUP_COLUMNS) {
      throw new IllegalArgumentException("groupColumns must have 2.." + MAX_GROUP_COLUMNS
          + " entries, got " + keyCount);
    }
    final ScanScratch s = SCRATCH.get();
    // Per-leaf combo cache: packed group ids -> the SAME long[] accumulator instance
    // stored under the composite key in {@code out}. Cleared per leaf (ids are per-leaf).
    final Long2ObjectOpenHashMap<long[]> comboCache = new Long2ObjectOpenHashMap<>(64);
    final String[][] dicts = new String[keyCount][];
    final int[] groupIdsOff = new int[keyCount];
    final int[] groupPresOff = new int[keyCount];
    final int[] dictSizes = new int[keyCount];
    final String[] keyScratch = new String[keyCount];
    for (int leaf = 0; leaf < leafPayloads.size(); leaf++) {
      final byte[] payload = leafPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      for (int g = 0; g < keyCount; g++) {
        final int col = groupColumns[g];
        if (col < 0 || col >= columnCount) {
          throw new IllegalStateException("groupColumn " + col
              + " out of range [0, " + columnCount + ")");
        }
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("groupColumn " + col
              + " is not STRING_DICT (kind=" + kind + ")");
        }
        final int base = s.columnDataOff[col];
        final int dictSize = getIntLE(payload, base);
        if (dictSize < 0 || dictSize >= GROUP_ID_MISSING) {
          // The 11-bit lane must hold every dict id PLUS the missing sentinel. Today
          // dictSize <= MAX_ROWS = 1024 < 2047; a corrupt header (or a future MAX_ROWS
          // bump past the lane width) must fail loud, never alias groups silently.
          throw new IllegalStateException("group dict size " + dictSize
              + " exceeds the packed-key lane (max " + (GROUP_ID_MISSING - 1) + ")");
        }
        final int lenHeaderOff = base + 4;
        final String[] dict = new String[dictSize];
        int running = lenHeaderOff + dictSize * 4;
        for (int i = 0; i < dictSize; i++) {
          final int len = getIntLE(payload, lenHeaderOff + i * 4);
          dict[i] = new String(payload, running, len, StandardCharsets.UTF_8);
          running += len;
        }
        dicts[g] = dict;
        dictSizes[g] = dictSize;
        groupIdsOff[g] = running;
        groupPresOff[g] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, col) : -1;
      }
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggColumns.length
          ? s.groupAggPresOff : (s.groupAggPresOff = new int[Math.max(4, aggColumns.length)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggColumns.length
          ? s.groupAggValOff : (s.groupAggValOff = new int[Math.max(4, aggColumns.length)]);
      for (int a = 0; a < aggColumns.length; a++) {
        if (aggColumns[a] < 0 || aggColumns[a] >= columnCount) {
          throw new IllegalStateException("aggColumn " + aggColumns[a]
              + " out of range [0, " + columnCount + ")");
        }
        final byte aggKind = payload[24 + aggColumns[a]];
        if (aggKind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
          throw new IllegalStateException("aggColumn " + aggColumns[a]
              + " is not NUMERIC_LONG (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, aggColumns[a]) : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      comboCache.clear();
      final long leafOrdinalBase = ((long) (leafIndexBase + leaf)) << 20;
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          long packed = 0L;
          for (int g = 0; g < keyCount; g++) {
            final int comp;
            if (groupPresOff[g] >= 0
                && (getLongLE(payload, groupPresOff[g] + w * 8) & (1L << bit)) == 0L) {
              comp = GROUP_ID_MISSING;
            } else {
              comp = getIntLE(payload, groupIdsOff[g] + rowIdx * 4);
              if (comp < 0 || comp >= dictSizes[g]) {
                // A corrupt id must fail loud here: 2047 would alias the MISSING
                // sentinel and >= 2048 would bleed into the adjacent key's lane —
                // both silently wrong groups, which fail-soft callers would never see.
                throw new IllegalStateException("group dict id " + comp
                    + " out of range [0, " + dictSizes[g] + ")");
              }
            }
            packed = (packed << 11) | comp;
          }
          long[] acc = comboCache.get(packed);
          if (acc == null) {
            long p = packed;
            for (int g = keyCount - 1; g >= 0; g--) {
              final int comp = (int) (p & 0x7FF);
              p >>>= 11;
              keyScratch[g] = comp == GROUP_ID_MISSING ? null : dicts[g][comp];
            }
            final GroupKey key = new GroupKey(keyScratch.clone());
            acc = out.get(key);
            if (acc == null) {
              acc = newGroupAggAcc(aggColumns.length, leafOrdinalBase | rowIdx);
              out.put(key, acc);
            }
            comboCache.put(packed, acc);
          }
          acc[0]++;
          for (int a = 0; a < aggColumns.length; a++) {
            if (aggPresOff[a] >= 0
                && (getLongLE(payload, aggPresOff[a] + w * 8) & (1L << bit)) == 0L) {
              continue;
            }
            final long v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            final int base = 2 + 4 * a;
            acc[base]++;
            // Exact sum or DECLINE: the interpreter promotes an overflowing xs:integer
            // sum to exact decimal — a wrapped long would silently serve a wrong total.
            acc[base + 1] = Math.addExact(acc[base + 1], v);
            if (v < acc[base + 2]) acc[base + 2] = v;
            if (v > acc[base + 3]) acc[base + 3] = v;
          }
        }
      }
    }
  }

  /** Postfix opcodes for {@link #conjunctiveAggregateComputed} (mirror the detection stage). */
  public static final int COMPUTED_OP_ADD = -1;
  public static final int COMPUTED_OP_SUB = -2;
  public static final int COMPUTED_OP_MUL = -3;

  /** Code slots {@code >= COMPUTED_CONST_BASE} push {@code consts[slot - COMPUTED_CONST_BASE]}. */
  public static final int COMPUTED_CONST_BASE = 1 << 20;

  /**
   * COMPUTED-EXPRESSION aggregate fold (gap item 2): for every predicate-matching row on
   * which ALL operand columns are PRESENT, evaluate the postfix {@code code} program over
   * the row's NUMERIC_LONG operand values ({@code slot < COMPUTED_CONST_BASE} pushes
   * {@code operandColumns[slot]}'s value, {@code slot >= COMPUTED_CONST_BASE} pushes
   * {@code consts[slot - COMPUTED_CONST_BASE]}, negative slots apply
   * ADD/SUB/MUL popping two operands) and fold {@code [count, sum, min, max]} into
   * {@code acc}. Rows missing ANY operand contribute nothing — the interpreter's
   * arithmetic over the empty sequence is empty, so those rows add no item.
   *
   * <p>ALL arithmetic (program ops AND the running sum) is {@code Math.*Exact}: an
   * overflow throws {@link ArithmeticException}, which callers treat as a DECLINE — the
   * interpreter promotes overflowing integer math to exact decimal, so only the generic
   * pipeline can answer those digit-exactly.
   *
   * <p>{@code acc} layout {@code [count, sum, min, max]} with min/max seeded to
   * {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE}. The program is caller-validated
   * (balanced, depth ≤ {@code stack.length}); this kernel re-checks only bounds that
   * protect memory safety.
   */
  public static void conjunctiveAggregateComputed(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] operandColumns,
      final int[] code,
      final long[] consts,
      final long[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int nOps = operandColumns.length;
    if (nOps < 1) {
      throw new IllegalArgumentException("operandColumns must not be empty");
    }
    final ScanScratch s = SCRATCH.get();
    final int[] valOff = new int[nOps];
    final int[] presOff = new int[nOps];
    final long[] operand = new long[nOps];
    final long[] stack = new long[code.length];
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      for (int c = 0; c < nOps; c++) {
        if (operandColumns[c] < 0 || operandColumns[c] >= columnCount) {
          throw new IllegalStateException("operand column " + operandColumns[c]
              + " out of range [0, " + columnCount + ")");
        }
        final byte kind = payload[24 + operandColumns[c]];
        if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
          throw new IllegalStateException("operand column " + operandColumns[c]
              + " is not NUMERIC_LONG (kind=" + kind + ")");
        }
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      for (int c = 0; c < nOps; c++) {
        valOff[c] = s.columnDataOff[operandColumns[c]];
        presOff[c] = tailStart >= 0 ? presenceWordsOff(payload, tailStart, operandColumns[c]) : -1;
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
          boolean anyMissing = false;
          for (int c = 0; c < nOps; c++) {
            if (presOff[c] >= 0
                && (getLongLE(payload, presOff[c] + w * 8) & (1L << bit)) == 0L) {
              anyMissing = true;
              break;
            }
          }
          if (anyMissing) {
            continue;
          }
          for (int c = 0; c < nOps; c++) {
            operand[c] = getLongLE(payload, valOff[c] + rowIdx * 8);
          }
          int sp = 0;
          for (final int op : code) {
            if (op >= COMPUTED_CONST_BASE) {
              stack[sp++] = consts[op - COMPUTED_CONST_BASE];
            } else if (op >= 0) {
              stack[sp++] = operand[op];
            } else {
              final long b = stack[--sp];
              final long a = stack[--sp];
              stack[sp++] = switch (op) {
                case COMPUTED_OP_ADD -> Math.addExact(a, b);
                case COMPUTED_OP_SUB -> Math.subtractExact(a, b);
                case COMPUTED_OP_MUL -> Math.multiplyExact(a, b);
                default -> throw new IllegalStateException("unknown computed opcode " + op);
              };
            }
          }
          final long v = stack[0];
          acc[0]++;
          acc[1] = Math.addExact(acc[1], v);
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
        }
      }
    }
  }

  /**
   * Collect {@code (sortValue[0..k-1], recordKey)} TUPLES for every predicate-matching row
   * on which ALL {@code k} NUMERIC_LONG sort columns are PRESENT (P5b stage 7b sorted-scan
   * serving; gap item 1b generalized the single column to {@code k}). Values append
   * ROW-MAJOR into {@code valuesOut} (stride {@code sortColumns.length}). Rows missing ANY
   * sort column are counted into {@code missingKeysOut} instead — the interpreter sorts
   * empty order keys per the empty-least/greatest mode (no error), a placement this
   * long-tuple collector cannot represent, so callers decline when it is non-empty. Outputs
   * append in DOCUMENT order, so a stable by-tuple sort of the rows reproduces the
   * interpreter's stable {@code order by}.
   */
  public static void collectMatchingSortTuples(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] sortColumns,
      final LongArrayList valuesOut,
      final LongArrayList keysOut,
      final LongArrayList missingKeysOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int keyCount = sortColumns.length;
    if (keyCount < 1) {
      throw new IllegalArgumentException("sortColumns must not be empty");
    }
    final ScanScratch s = SCRATCH.get();
    final int[] valOff = new int[keyCount];
    final int[] presOff = new int[keyCount];
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      for (int k = 0; k < keyCount; k++) {
        final int sortColumn = sortColumns[k];
        if (sortColumn < 0 || sortColumn >= columnCount) {
          throw new IllegalStateException("sortColumn " + sortColumn
              + " out of range [0, " + columnCount + ")");
        }
        final byte sortKind = payload[24 + sortColumn];
        if (sortKind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
          throw new IllegalStateException("sortColumn " + sortColumn
              + " is not NUMERIC_LONG (kind=" + sortKind + ")");
        }
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int recordKeysOff = 24 + columnCount;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart < 0) {
        // Fail closed: without a valid presence tail, missing cells would sort as their
        // phantom stored defaults. Executor gates (columnSparseClean) make this
        // unreachable; this guard keeps the PUBLIC method safe for any future caller.
        throw new IllegalStateException("leaf without a valid presence tail — sorted "
            + "collection requires presence truth");
      }
      for (int k = 0; k < keyCount; k++) {
        valOff[k] = s.columnDataOff[sortColumns[k]];
        presOff[k] = presenceWordsOff(payload, tailStart, sortColumns[k]);
      }
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        if (word == 0L) {
          continue;
        }
        // Hoist the AND of every key's presence word ONCE per 64-row word (the
        // single-key kernel's pattern) — the row loop below then tests bits only.
        long presAll = -1L;
        for (int k = 0; k < keyCount; k++) {
          presAll &= presOff[k] >= 0 ? getLongLE(payload, presOff[k] + w * 8) : -1L;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
          final long recordKey = getLongLE(payload, recordKeysOff + rowIdx * 8);
          if ((presAll & (1L << bit)) == 0L) {
            missingKeysOut.add(recordKey);
            continue;
          }
          for (int k = 0; k < keyCount; k++) {
            valuesOut.add(getLongLE(payload, valOff[k] + rowIdx * 8));
          }
          keysOut.add(recordKey);
        }
      }
    }
  }

  /**
   * Per-row sink for {@link #materializeMatchingRows}. The arrays are REUSED across rows
   * (thread-confined flyweights) — consumers must copy what they keep. {@code present[i]}
   * false ⇒ the field is missing on this row ({@code longVals}/{@code stringVals} slots
   * are then meaningless); numeric/boolean columns fill {@code longVals[i]} (booleans as
   * 0/1, doubles in transform domain), STRING_DICT columns fill {@code stringVals[i]}.
   */
  @FunctionalInterface
  public interface RowSink {
    void row(long[] longVals, String[] stringVals, boolean[] present);
  }

  /**
   * COVERED-ROW materialization (P5b stage 7c): stream every predicate-matching row's
   * requested column values straight from the raw leaves, in document order — no document
   * -store touch. Column kinds are validated per leaf (fail loud on drift); presence is
   * per-column truth (fail closed on a missing tail). Dict interning mirrors
   * {@link #conjunctiveCountByGroup}.
   */
  public static void materializeMatchingRows(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] cols,
      final byte[] expectedKinds, final RowSink sink) {
    if (predicates == null || cols == null || expectedKinds == null || sink == null) {
      throw new IllegalArgumentException("predicates, cols, expectedKinds and sink must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    final int nCols = cols.length;
    final long[] longVals = new long[nCols];
    final String[] stringVals = new String[nCols];
    final boolean[] present = new boolean[nCols];
    final int[] valOff = new int[nCols];
    final int[] presOff = new int[nCols];
    final byte[] kinds = new byte[nCols];
    final int[] dictLenHeaderOff = new int[nCols];
    final int[] dictIdsOff = new int[nCols];
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates, s);
      if (rowCount <= 0) continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart < 0) {
        throw new IllegalStateException("leaf without a valid presence tail — covered-row "
            + "materialization requires presence truth");
      }
      // Per-COLUMN dict byte offsets: multiple string columns each need their own prefix
      // sums (a shared cache would be clobbered). Allocated per leaf per string column —
      // dictionaries are canonical-cardinality small, amortized over the leaf's rows.
      final int[][] dictOffs = new int[nCols][];
      final String[][] dictStrings = new String[nCols][];
      for (int c = 0; c < nCols; c++) {
        final int col = cols[c];
        if (col < 0 || col >= columnCount) {
          throw new IllegalStateException("column " + col + " out of range [0, " + columnCount + ")");
        }
        kinds[c] = payload[24 + col];
        if (kinds[c] != expectedKinds[c]) {
          // Fail loud on kind drift between handle metadata and leaf truth — the sibling
          // kernels' policy; silent drift here would serve raw wrong data.
          throw new IllegalStateException("column " + col + " kind drift: leaf says "
              + kinds[c] + ", handle says " + expectedKinds[c]);
        }
        valOff[c] = s.columnDataOff[col];
        presOff[c] = presenceWordsOff(payload, tailStart, col);
        if (kinds[c] == ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
          final int base = valOff[c];
          final int dictSize = getIntLE(payload, base);
          dictLenHeaderOff[c] = base + 4;
          // Decode the whole (canonical-cardinality small) dictionary once per leaf per
          // column — no global hash-keyed interning: row-level strings would make a
          // 64-bit-hash intern both collision-prone (silent wrong data) and unbounded.
          final int[] offs = new int[dictSize];
          int running = base + 4 + dictSize * 4;
          for (int i = 0; i < dictSize; i++) {
            offs[i] = running;
            running += getIntLE(payload, dictLenHeaderOff[c] + i * 4);
          }
          final String[] decoded = new String[dictSize];
          for (int i = 0; i < dictSize; i++) {
            decoded[i] = new String(payload, offs[i],
                getIntLE(payload, dictLenHeaderOff[c] + i * 4), StandardCharsets.UTF_8);
          }
          dictStrings[c] = decoded;
          dictOffs[c] = offs;
          dictIdsOff[c] = running;
        } else if (!ProjectionIndexLeafPage.isNumericKind(kinds[c])
            && kinds[c] != ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN) {
          throw new IllegalStateException("column " + col + " has unsupported kind " + kinds[c]);
        }
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
          for (int c = 0; c < nCols; c++) {
            final boolean p =
                (getLongLE(payload, presOff[c] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) != 0L;
            present[c] = p;
            if (!p) continue;
            switch (kinds[c]) {
              case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
                final int dictId = getIntLE(payload, dictIdsOff[c] + rowIdx * 4);
                stringVals[c] = dictStrings[c][dictId];
              }
              case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> {
                final long bw = getLongLE(payload, valOff[c] + (rowIdx >>> 6) * 8);
                longVals[c] = (bw >>> (rowIdx & 63)) & 1L;
              }
              default -> longVals[c] = getLongLE(payload, valOff[c] + rowIdx * 8);
            }
          }
          sink.row(longVals, stringVals, present);
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

  /**
   * Per-column pure-double-source evidence (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-8):
   * {@code result[c]} is {@code true} iff column {@code c} is NUMERIC_DOUBLE and EVERY leaf's
   * presence tail asserts {@link ProjectionIndexLeafPage#COLUMN_FLAG_PURE_DOUBLE_SOURCE} —
   * the aggregation direction is AND, the opposite of the sticky-poison probes: purity is a
   * positive claim that one silent leaf (old bytes, impure sources) must be able to veto.
   * Returns {@code null} on any malformed payload — consumers fail closed.
   */
  public static boolean[] probeDoublePureSource(final List<byte[]> leafPayloads) {
    if (leafPayloads == null || leafPayloads.isEmpty()) return new boolean[0];
    final byte[] first = leafPayloads.get(0);
    if (first == null || first.length < 8) return null;
    final int columnCount = columnCountOf(first);
    if (columnCount < 0) return null;
    final boolean[] pure = new boolean[columnCount];
    try {
      for (int c = 0; c < columnCount; c++) {
        pure[c] = first[24 + c] == ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE;
      }
      for (final byte[] payload : leafPayloads) {
        if (payload == null || payload.length < 8 || columnCountOf(payload) != columnCount) return null;
        final int dataEnd = leafDataEnd(payload);
        final int tailStart = dataEnd < 0 ? -1 : presenceTailStart(payload, dataEnd);
        if (tailStart < 0) return null;
        for (int c = 0; c < columnCount; c++) {
          if ((payload[tailStart + c] & ProjectionIndexLeafPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE) == 0) {
            pure[c] = false;
          }
        }
      }
    } catch (final IndexOutOfBoundsException truncated) {
      // Malformed / truncated payload (including a first payload shorter than its declared
      // kinds array) — fail closed per the contract, never propagate.
      return null;
    }
    return pure;
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
    final long[] numericFlags = s.numericFlags;
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
            payload, columnDataOff[p.column], rowCount, p.op, p.longLit, p.highLit,
            numericScratch, numericFlags, colMask);
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

  /** Package-private: the SINGLE zone-skip authority, shared with the column kernels. */
  static boolean zoneSkip(final ProjectionIndexScan.ColumnPredicate p,
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
   * SIMD numeric compare, two-pass so the compare auto-vectorises without any
   * heap allocation:
   *
   * <ol>
   *   <li><b>Load</b> the numeric column out of the {@code byte[]} payload into
   *       a reusable {@code long[]} scratch via the byte-array {@link VarHandle}
   *       (HotSpot intrinsifies to a tight MOVQ loop).</li>
   *   <li><b>Compare</b> each row against the bound(s) and write a branch-free
   *       {@code 0}/{@code 1} flag into {@code flags} — independent stores with
   *       no loop-carried dependency, so C2 SuperWord vectorises it to
   *       {@code VPCMPGTQ}+select over {@code LANES} rows at a time.</li>
   *   <li><b>Pack</b> 64 flags into each output bitmask word ({@link #packFlags}).</li>
   * </ol>
   *
   * <p>The earlier {@code jdk.incubator.vector} body produced the packed mask in
   * one {@code compare(...).toLong()} pass, but the {@code VectorMask} temporary
   * boxes on this runtime — measured ~26 KB/leaf, HotSpot C2 vector-box
   * elimination does not fire for the {@code toLong()} pattern (nor did it on
   * Oracle GraalVM 25.0.2, the original iter#15 trigger). This two-pass form is
   * allocation-free <em>and</em> ~2.9× faster than the previous scalar
   * predicate-into-bitmask loop, because the packed-bit store's loop-carried
   * dependency (64 rows write the same word) is confined to the cheap pack pass
   * and no longer blocks vectorisation of the compare.
   */
  private static void evalNumericBytes(final byte[] payload,
      final int baseOff, final int rowCount, final ProjectionIndexScan.Op op,
      final long lit, final long highLit, final long[] scratch, final long[] flags,
      final long[] out) {
    // 1) Load column into scratch — fully intrinsified (MOVQ per lane).
    for (int k = 0; k < rowCount; k++) {
      scratch[k] = getLongLE(payload, baseOff + k * 8);
    }
    // 2) Vectorisable compare pass. Each arm is a single relational op (or a
    //    non-short-circuit AND of two, for the fused BETWEEN range) applied
    //    branch-free into `flags` — the shape C2 SuperWord turns into a packed
    //    vector compare. Dispatch is hoisted out of the loop by the tableswitch.
    switch (op) {
      case GT -> { for (int k = 0; k < rowCount; k++) flags[k] = (scratch[k] >  lit) ? 1L : 0L; }
      case LT -> { for (int k = 0; k < rowCount; k++) flags[k] = (scratch[k] <  lit) ? 1L : 0L; }
      case GE -> { for (int k = 0; k < rowCount; k++) flags[k] = (scratch[k] >= lit) ? 1L : 0L; }
      case LE -> { for (int k = 0; k < rowCount; k++) flags[k] = (scratch[k] <= lit) ? 1L : 0L; }
      case EQ -> { for (int k = 0; k < rowCount; k++) flags[k] = (scratch[k] == lit) ? 1L : 0L; }
      case BETWEEN_GT_LT ->
          { for (int k = 0; k < rowCount; k++) { final long v = scratch[k]; flags[k] = ((v >  lit) & (v <  highLit)) ? 1L : 0L; } }
      case BETWEEN_GT_LE ->
          { for (int k = 0; k < rowCount; k++) { final long v = scratch[k]; flags[k] = ((v >  lit) & (v <= highLit)) ? 1L : 0L; } }
      case BETWEEN_GE_LT ->
          { for (int k = 0; k < rowCount; k++) { final long v = scratch[k]; flags[k] = ((v >= lit) & (v <  highLit)) ? 1L : 0L; } }
      case BETWEEN_GE_LE ->
          { for (int k = 0; k < rowCount; k++) { final long v = scratch[k]; flags[k] = ((v >= lit) & (v <= highLit)) ? 1L : 0L; } }
      default -> throw new IllegalStateException("Unknown numeric op " + op);
    }
    // 3) Pack the per-row flags into the packed-bit output mask.
    packFlags(flags, rowCount, out);
  }

  /**
   * Pack per-row flags into a packed-bit mask: bit {@code (w*64 + b)} of
   * {@code out} is set iff {@code flags[w*64 + b] != 0}. OR's into {@code out}
   * (the caller pre-clears the live prefix), matching the former in-place bit-set
   * semantics. The inner reduction into a register-local {@code word} keeps the
   * loop-carried write off the hot compare path.
   *
   * <p>A flag is normalised to a single bit ({@code != 0 ? 1 : 0}) before the
   * shift, so the contract holds for any non-zero truth encoding — e.g. the
   * all-ones {@code -1L} a SIMD lane-mask naturally produces — not only the
   * {@code 0}/{@code 1} the current compare pass writes.
   */
  private static void packFlags(final long[] flags, final int rowCount, final long[] out) {
    final int stride = (rowCount + 63) >>> 6;
    for (int w = 0; w < stride; w++) {
      final int base = w << 6;
      final int n = Math.min(64, rowCount - base);
      long word = 0L;
      for (int b = 0; b < n; b++) {
        // Normalise to one bit: `flags[i] << b` would smear a multi-bit or
        // all-ones truth value across neighbouring output bits.
        word |= (flags[base + b] != 0 ? 1L : 0L) << b;
      }
      out[w] |= word;
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
