/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

/**
 * Zero-copy scan over serialised {@link ProjectionIndexRowGroupPage} byte[]s. Does not materialise
 * the leaf's column arrays — reads primitives directly from the payload via {@link VarHandle},
 * eliminating the per-leaf allocation storm that {@link ProjectionIndexScan} pays through
 * {@link ProjectionIndexRowGroupPage#deserialize}.
 *
 * <p>
 * Measured on a synthetic 1M-row stream (977 leaves), {@code deserialize} accounts for ~50% of the
 * scan cost on numeric predicates and ~90–98% on boolean / string-EQ predicates (where the kernel
 * itself is sub-nanosecond). This reader skips it entirely: one small scratch long[] per
 * scan-thread (reused across leaves), otherwise zero allocs on the hot path.
 *
 * <h2>Iter#02 note — why {@link VarHandle} beat {@code sun.misc.Unsafe}</h2>
 *
 * An early iter#02 attempt replaced the {@code byteArrayViewVarHandle} reads with
 * {@code sun.misc.Unsafe.getInt/getLong(byte[], base+off)} on the hypothesis that the
 * {@code VarHandleGuards.guard_LI_I} / access-mode checks visible in a contaminated CPU profile
 * were real per-call overhead. A clean A/B/C comparison (3 runs each of varhandle, MemorySegment
 * via FFM, Unsafe direct — cold 100M scale bench, load ≤ 2) found:
 * <ul>
 * <li>varhandle median wall <b>5.29 s</b>, projection build 2,567 ms</li>
 * <li>msegment median wall 5.53 s (+4.5%), build 2,659 ms</li>
 * <li>unsafe median wall 5.56 s (+5.1%), build 2,729 ms</li>
 * </ul>
 * HotSpot's C2 already inlines and intrinsifies {@code VarHandle.get} on static-final byte-array
 * view handles to the same raw MOVL/MOVQ that {@code Unsafe} emits; the "guard" frames only appear
 * when the VarHandle is not proven monomorphic at the call site. Swapping to Unsafe also meant an
 * {@code --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED} dependency that would have pulled a
 * deprecated-for-removal API onto the hot path for zero measured benefit, so the swap was reverted.
 * This javadoc records the result so future attempts know to skip the detour.
 *
 * <h2>Layout decoded</h2> The format is defined by {@link ProjectionIndexRowGroupPage#serialize}:
 * 
 * <pre>
 *   0:   int rowCount
 *   4:   int columnCount
 *   8:   long firstRecordKey
 *   16:  long lastRecordKey
 *   24:  byte[columnCount] kinds
 *   24+columnCount: long[rowCount] recordKeys    (if rowCount > 0)
 *   then byte orderExceptionKind and, only for DENSE, ceil(rowCount/64) bitmap words
 *   then int orderLabelByteLength, int[rowCount+1] orderLabelOffsets, byte[orderLabelByteLength]
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
 * <p>
 * All multi-byte integers are little-endian.
 *
 * <h2>Group-by kernels: two families, never interchangeable</h2>
 *
 * The STRING_DICT kernels ({@link #conjunctiveCountByGroup} and friends) and the NUMERIC_LONG
 * kernels ({@link #conjunctiveCountByGroupNumeric} and friends) are siblings — each rejects the
 * other's column kind loudly. The numeric family exists because a numeric key deletes the entire
 * canonical-dictionary apparatus the string family needs: dict ids are LEAF-LOCAL and cannot be
 * summed across leaves, whereas values are globally comparable by construction. So the numeric
 * kernels carry no canonical-dict probe, no per-leaf {@code dictId → canonId} remap, no per-leaf
 * fallback map, no FNV-1a intern and no {@code String} at all; the dense arm indexes by
 * {@code value - base} and merges by elementwise sum with no decode step. See
 * {@link #numericZoneUnion} for where {@code base} comes from (metadata, no row touches).
 */
public final class ProjectionIndexByteScan {

  /** Aggregate operand is numeric, not a string-length transform. */
  public static final byte STRING_LENGTH_NONE = 0;
  /** Aggregate operand is {@code fn:string-length}: count Unicode code points. */
  public static final byte STRING_LENGTH_CODE_POINTS = 1;
  /** Aggregate operand is {@code jn:utf8-length}: count encoded UTF-8 bytes. */
  public static final byte STRING_LENGTH_UTF8_BYTES = 2;

  private static final VarHandle INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  /**
   * Manual little-endian byte assembly instead of the {@link VarHandle} view (same switch and default
   * as {@link ProjectionIndexRowGroupCodec}). The VarHandle folds to a single
   * {@code MOVL}/{@code MOVQ} only once C2 has compiled the call-site; a COLD one-shot never gets
   * there, and a wall-clock profile of cold S3 put the un-elided access-mode machinery
   * ({@code checkAccessModeThenIsDirect} / {@code VarForm.getMemberName} / {@code guard_LI_J}) at ~55
   * % of all busy samples — more than the scan kernel itself. Manual assembly has no access-mode
   * check to elide, so it costs the same interpreted, in C1 and in C2.
   */
  private static final boolean MANUAL_LE = !"false".equals(System.getProperty("sirix.projection.manualLE"));

  /**
   * Read a little-endian {@code int} from {@code b} at byte offset {@code off}. Wrapped in a named
   * helper so call-sites are easier to grep and the encoding choice lives in one place.
   */
  private static int getIntLE(final byte[] b, final int off) {
    if (MANUAL_LE) {
      return (b[off] & 0xFF) | (b[off + 1] & 0xFF) << 8 | (b[off + 2] & 0xFF) << 16 | b[off + 3] << 24;
    }
    return (int) INT_LE.get(b, off);
  }

  /** Read a little-endian {@code long} from {@code b} at byte offset {@code off}. */
  private static long getLongLE(final byte[] b, final int off) {
    if (MANUAL_LE) {
      return (b[off] & 0xFFL) | (b[off + 1] & 0xFFL) << 8 | (b[off + 2] & 0xFFL) << 16 | (b[off + 3] & 0xFFL) << 24
          | (b[off + 4] & 0xFFL) << 32 | (b[off + 5] & 0xFFL) << 40 | (b[off + 6] & 0xFFL) << 48
          | (b[off + 7] & 0xFFL) << 56;
    }
    return (long) LONG_LE.get(b, off);
  }

  /**
   * Thread-local scan scratch. Hoisted out of {@link #conjunctiveCount} and
   * {@link #conjunctiveCountByGroup}'s per-call allocation: ~8.5 KB per invocation × 20 worker
   * threads × N queries = tens of MB/s of GC churn at sustained analytical load. Reuse across calls;
   * grown on demand if a wider projection shows up. Single instance per thread is safe because each
   * conjunctiveCount call is one-shot on its own worker-thread stack — no re-entrancy.
   */
  private static final class ScanScratch {
    int[] columnDataOff = new int[16];
    int[] columnMinMaxOff = new int[16];
    /**
     * End offset of the column data stream of the leaf most recently processed by
     * {@link #evaluateRowGroupMask} — i.e. where the presence tail would start. Lets the
     * group/aggregate kernels locate presence bitmaps with an EXACT boundary check instead of trusting
     * the footer alone.
     */
    int leafDataEnd;
    final long[] numericScratch = new long[ProjectionIndexRowGroupPage.MAX_ROWS];
    // Per-row 0/1 compare flags — target of the SuperWord-vectorised compare
    // pass in evalNumericBytes, packed into colMask afterwards.
    final long[] numericFlags = new long[ProjectionIndexRowGroupPage.MAX_ROWS];
    final long[] mask = new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6];
    final long[] colMask = new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6];
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
    // Flat string group-by: per-leaf dictId -> (FNV hash, cached table slot base). The base is
    // validated against the table key on every use (rehash-safe), so stale bases self-heal.
    long[] dictHashCache;
    int[] dictSlotBase;
    // COUNT(DISTINCT) over a STRING_DICT operand: per-leaf dictId -> payload byte range of the
    // entry plus its content hash (0 = not yet hashed). Separate from dictByteOff/dictHashCache,
    // which the GROUP key owns — the distinct column may well be the group column.
    int[] cdDictOff;
    int[] cdDictLen;
    long[] cdDictHash;
    // TREE mask builder: one mask per predicate-tree leaf (program stack depth <= MAX_LEAVES).
    long[][] treeMaskStack;
  }

  private static final ThreadLocal<ScanScratch> SCRATCH = ThreadLocal.withInitial(ScanScratch::new);

  private ProjectionIndexByteScan() {}

  /** Raw row count — parses the header only. Identical semantics to the materialising variant. */
  public static long countRows(final Iterable<byte[]> rowGroupPayloads) {
    long total = 0;
    for (final byte[] payload : rowGroupPayloads) {
      total += getIntLE(payload, 0);
    }
    return total;
  }

  /**
   * Count rows satisfying the conjunctive {@code predicates}. Predicate-free calls throw — callers
   * should use {@link #countRows(Iterable)} for unconditional counts.
   */
  public static long conjunctiveCount(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates) {
    if (predicates == null || predicates.length == 0) {
      throw new IllegalArgumentException("use countRows for unconditional counts");
    }
    // Thread-local scratch: one allocation per worker thread amortised
    // across all analytical queries on that thread. See {@link ScanScratch}.
    final ScanScratch s = SCRATCH.get();
    long total = 0;
    for (final byte[] payload : rowGroupPayloads) {
      // Grow scratch if a wider leaf shows up (rare — projection indexes
      // are built column-set-at-a-time, so the width is consistent within
      // a single handle).
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      total += countRowGroup(payload, predicates, s);
    }
    return total;
  }

  /**
   * Probe the first {@code probeLeaves} leaves to collect the union of UTF-8 byte-slices present in
   * the {@code groupColumn} dictionary. Returns the resulting canonical dictionary ({@code byte[][]},
   * one entry per distinct UTF-8 value in insertion order), or {@code null} when any of the following
   * hold:
   *
   * <ul>
   * <li>{@code rowGroupPayloads} is empty,</li>
   * <li>{@code groupColumn} is out of range on the first leaf,</li>
   * <li>the group column's kind is not
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT},</li>
   * <li>the observed cardinality exceeds {@code cardLimit}.</li>
   * </ul>
   *
   * <p>
   * Used by {@link ProjectionIndexRegistry.Handle#canonicalDict} to decide eligibility for the dense
   * group-by path ({@link #conjunctiveCountByGroupDense}).
   *
   * <p>
   * HFT-grade: bounded scan depth; one {@code ArrayList<byte[]>} for the probe result; no per-leaf
   * dict string allocation (values are carried as slices copied into fresh {@code byte[]}).
   *
   * @param rowGroupPayloads ordered leaf byte[] list — typically
   *        {@link ProjectionIndexRegistry.Handle#rowGroupPayloads}.
   * @param groupColumn target column index.
   * @param probeLeaves max number of leaves to probe, {@code > 0}.
   * @param cardLimit max tolerable cardinality; caller-specific bound (e.g. {@code long[]} budget per
   *        worker).
   * @return immutable canonical dict (caller must not mutate), or {@code null} if ineligible.
   */
  public static byte[][] probeCanonicalDict(final List<byte[]> rowGroupPayloads, final int groupColumn,
      final int probeLeaves, final int cardLimit) {
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty())
      return null;
    if (probeLeaves <= 0 || cardLimit <= 0)
      return null;
    final byte[] firstRowGroup = rowGroupPayloads.get(0);
    if (firstRowGroup == null)
      return null;
    final int columnCount = columnCountOf(firstRowGroup);
    if (groupColumn < 0 || groupColumn >= columnCount)
      return null;
    if (firstRowGroup[24 + groupColumn] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT)
      return null;

    // Seed the canonical dict from the first leaf's dict.
    final ArrayList<byte[]> canon = new ArrayList<>(Math.min(cardLimit, 64));
    final int scanUpTo = Math.min(probeLeaves, rowGroupPayloads.size());
    for (int li = 0; li < scanUpTo; li++) {
      final byte[] payload = rowGroupPayloads.get(li);
      if (payload == null)
        continue;
      if (columnCountOf(payload) != columnCount)
        continue;
      if (payload[24 + groupColumn] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT)
        return null;
      final int groupBase = columnDataOffFor(payload, groupColumn);
      if (groupBase < 0)
        return null;
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
          if (bytesEqualAt(payload, running, len, canon.get(c))) {
            present = true;
            break;
          }
        }
        if (!present) {
          if (canonSize >= cardLimit)
            return null; // cardinality exceeded
          final byte[] copy = new byte[len];
          if (len > 0)
            System.arraycopy(payload, running, copy, 0, len);
          canon.add(copy);
        }
        running += len;
      }
    }
    return canon.toArray(new byte[0][]);
  }

  /**
   * Compute the starting byte-offset of {@code groupColumn}'s data block inside {@code payload}
   * without populating the full per-column offset cache. Returns {@code -1} on any structural
   * inconsistency (caller falls back to the hashmap path). Mirrors the offset-walk logic in
   * {@link #evaluateRowGroupMask} but stops at the target column.
   */
  private static int columnDataOffFor(final byte[] payload, final int groupColumn) {
    final int rowCount = getIntLE(payload, 0);
    if (rowCount == 0)
      return -1;
    final int columnCount = getIntLE(payload, 4);
    if (groupColumn < 0 || groupColumn >= columnCount)
      return -1;
    final int kindsOff = 24;
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      cursor += 16; // per-column min/max
      if (c == groupColumn)
        return cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          // Dict, then per-row counts, then the flat element run whose length is their sum.
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
        }
        default -> {
          return -1;
        }
      }
    }
    return -1; // not found (shouldn't happen — guarded by columnCount check)
  }

  /**
   * Dense group-by-count: per matching row, increments {@code counts[canonId]} where {@code canonId}
   * is the position of the leaf's dict-value in {@code canonicalDict}. Values NOT in
   * {@code canonicalDict} fall back to the hashmap path ({@link #conjunctiveCountByGroup}) for the
   * offending leaf only.
   *
   * <p>
   * Hot path: one {@code int[]} dictId→canonId remap per leaf (cost: {@code dictSize} ×
   * {@code canonLen} byte comparisons; both tiny for bounded-cardinality group columns), then a
   * single {@code counts[remap[dictId]]++} per matching row. Zero hashmap ops, zero String.equals,
   * zero FNV-1a hashing on the per-row path.
   *
   * <p>
   * HFT-grade: caller-allocated {@code counts}; per-leaf remap uses a thread-local scratch
   * {@code int[]}; no boxing, no virtual dispatch.
   *
   * @param rowGroupPayloads leaves to scan.
   * @param predicates conjunctive predicate list (may be empty).
   * @param groupColumn STRING_DICT column index.
   * @param canonicalDict immutable canonical dict (length = count array size).
   * @param counts output array, pre-zeroed by caller, length ≥ canonicalDict.length.
   * @param fallbackOut optional hashmap that receives counts for any leaf whose dict contains a value
   *        NOT in {@code canonicalDict}. Non-null required when a full fallback may happen (i.e. when
   *        caller did not prove the canonical dict is complete). Pass a non-null empty map and merge
   *        it back on the caller side.
   */
  public static void conjunctiveCountByGroupDense(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final byte[][] canonicalDict,
      final long[] counts, final Object2LongOpenHashMap<String> fallbackOut) {
    conjunctiveCountByGroupDense(rowGroupPayloads, predicates, groupColumn, canonicalDict, counts, fallbackOut, null);
  }

  /**
   * Sparse-aware variant — matching rows missing the group field ( presence bit clear) count into
   * {@code missingOut[0]}; {@code null} keeps the dense behavior. See
   * {@link #conjunctiveCountByGroup(Iterable, ProjectionIndexScan.ColumnPredicate[], int, Object2LongOpenHashMap, long[])}.
   */
  public static void conjunctiveCountByGroupDense(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final byte[][] canonicalDict,
      final long[] counts, final Object2LongOpenHashMap<String> fallbackOut, final long[] missingOut) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn + " is not STRING_DICT (kind=" + groupKind + ")");
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
          if (bytesEqualAt(payload, running, len, canonicalDict[c])) {
            hit = c;
            break;
          }
        }
        remap[i] = hit;
        if (hit < 0)
          needsFallback = true;
        running += len;
      }
      final int idsOff = running;

      final int tailStart = missingOut != null
          ? presenceTailStart(payload, s.leafDataEnd)
          : -1;
      final int presOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;

      if (needsFallback) {
        // Fallback: run the standard hashmap path on this single leaf.
        // The caller merges fallbackOut back into the final aggregate.
        if (fallbackOut == null) {
          throw new IllegalStateException(
              "canonical dict missing value and no fallback provided for leaf with dictSize=" + dictSize);
        }
        conjunctiveCountByGroupSingleRowGroup(payload, rowCount, s.mask, groupBase, dictSize, lenHeaderOff, concatOff,
            idsOff, s, fallbackOut, presOff, missingOut);
        continue;
      }

      // Dense hot loop: counts[remap[dictId]]++ per matching row.
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long presWord = presOff >= 0
            ? getLongLE(payload, presOff + w * 8)
            : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount)
            break;
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
   * Per-leaf fallback path for {@link #conjunctiveCountByGroupDense}: when a leaf's dict contains a
   * value NOT in the canonical dict, we fall back to the original hashmap accumulator for that one
   * leaf.
   *
   * <p>
   * This is structurally the same as the inner loop of {@link #conjunctiveCountByGroup} (intern by
   * FNV-1a64 hash, bump {@link Object2LongOpenHashMap#addTo} per row), hoisted into a helper so the
   * dense path can invoke it without duplicating the mask-iteration state. Payload offsets are passed
   * in pre-computed since the dense path already walked them.
   */
  private static void conjunctiveCountByGroupSingleRowGroup(final byte[] payload, final int rowCount,
      final long[] scanMask, final int groupBase, final int dictSize, final int lenHeaderOff, final int concatOff,
      final int idsOff, final ScanScratch s, final Object2LongOpenHashMap<String> out, final int presOff,
      final long[] missingOut) {
    if (s.dictCache == null || s.dictCache.length < dictSize) {
      s.dictCache = new String[Math.max(64, dictSize)];
      s.dictByteOff = new int[s.dictCache.length];
    } else {
      // Clear the prefix we'll populate.
      for (int i = 0; i < dictSize; i++)
        s.dictCache[i] = null;
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
      final long presWord = presOff >= 0
          ? getLongLE(payload, presOff + w * 8)
          : -1L;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int rowIdx = (w << 6) + bit;
        if (rowIdx >= rowCount)
          break;
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
   * Conjunctive filter + numeric aggregate over a
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_LONG} column: for every row matching
   * {@code predicates}, folds the column value into {@code acc} = {@code [count, sum, min, max]}. The
   * column sweep is a straight {@code long[rowCount]} read per leaf — memory-bandwidth bound, the
   * same shape a column store executes.
   *
   * <p>
   * Exact sum or DECLINE, the same contract as {@link #conjunctiveAggregateByGroup}:
   * {@code xs:integer} is arbitrary precision and the interpreter promotes an overflowing total to
   * exact decimal, so a wrapped {@code long} would be a silently wrong answer (a column of 1e18-scale
   * ids wraps after a few dozen rows). The check is a per-value {@link Math#addExact} rather than the
   * pre-flight zone-map bound the SIMD fold kernel uses: this is a SCALAR walk, where an exact add
   * costs one never-taken branch and — unlike a lanewise fold — can see every carry, so it declines
   * only on a real overflow instead of on a conservative bound. (The zone map is reachable here too,
   * but only per leaf as {@code evaluateRowGroupMask} resolves it, and the payloads arrive as an
   * {@link Iterable} this method must not walk twice.)
   *
   * @throws ArithmeticException on overflow — callers treat it as a DECLINE
   */
  /**
   * {@link #conjunctiveAggregateNumeric} with a 128-BIT SUM — the overflow fallback for columns whose
   * exact long sum cannot fit (64-bit id columns; the interpreter promotes to big-integer arithmetic
   * there, so declining to the row path was pure cost). {@code acc} layout:
   * {@code [count, sumHi, sumLo(unsigned), min, max]}; the two sum lanes add with carry and merge
   * associatively, so the parallel chunk merge stays exact.
   */
  public static void conjunctiveAggregateNumeric128(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int numericColumn, final long[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final byte kind = payload[24 + numericColumn];
      if (!ProjectionIndexRowGroupPage.isOrderedLongKind(kind)) {
        throw new IllegalStateException(
            "aggregate column " + numericColumn + " is not NUMERIC_LONG or temporal (kind=" + kind + ")");
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
      long count = acc[0];
      long sumHi = acc[1];
      long sumLo = acc[2];
      long min = acc[3];
      long max = acc[4];
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          final long v = getLongLE(payload, base + rowIdx * 8);
          count++;
          final long lo = sumLo + v;
          // Unsigned carry-out of lo += v, plus the sign extension of v into the high lane.
          sumHi += (v >> 63) + (((sumLo & v) | ((sumLo | v) & ~lo)) >>> 63);
          sumLo = lo;
          if (v < min) {
            min = v;
          }
          if (v > max) {
            max = v;
          }
        }
      }
      acc[0] = count;
      acc[1] = sumHi;
      acc[2] = sumLo;
      acc[3] = min;
      acc[4] = max;
    }
  }

  public static void conjunctiveAggregateNumeric(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int numericColumn, final long[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final byte kind = payload[24 + numericColumn];
      if (!ProjectionIndexRowGroupPage.isOrderedLongKind(kind)) {
        throw new IllegalStateException(
            "aggregate column " + numericColumn + " is not NUMERIC_LONG or temporal (kind=" + kind + ")");
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
            sum = Math.addExact(sum, v);
            if (v < min)
              min = v;
            if (v > max)
              max = v;
          }
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount)
            break;
          final long v = getLongLE(payload, base + rowIdx * 8);
          count++;
          sum = Math.addExact(sum, v);
          if (v < min)
            min = v;
          if (v > max)
            max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * {@link #conjunctiveAggregateNumeric} for
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_DOUBLE} columns: cells hold the
   * order-preserving transform ({@link ProjectionDoubleEncoding}), so the sweep decodes each matching
   * value (two-op inverse) and folds into {@code acc} = {@code [count, sum, min, max]} as doubles
   * ({@code count} is exact well past any reachable row count). Callers initialise {@code acc} to
   * {@code {0, 0, +Infinity, -Infinity}} and, for run-to-run determinism, merge per-leaf partials in
   * ascending leaf order (double addition is not associative).
   */
  public static void conjunctiveAggregateNumericDouble(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int numericColumn, final double[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final byte kind = payload[24 + numericColumn];
      if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        throw new IllegalStateException(
            "aggregate column " + numericColumn + " is not NUMERIC_DOUBLE (kind=" + kind + ")");
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
            if (Double.compare(v, min) < 0)
              min = v;
            if (Double.compare(v, max) > 0)
              max = v;
          }
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount)
            break;
          final double v = ProjectionDoubleEncoding.decode(getLongLE(payload, base + rowIdx * 8));
          count++;
          sum += v;
          if (Double.compare(v, min) < 0)
            min = v;
          if (Double.compare(v, max) > 0)
            max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Pull-cursor over the predicate-matched, presence-filtered cells of one NUMERIC_DOUBLE column,
   * decoded to plain doubles in document order (ascending leaf, ascending row) — the §11-8 serving
   * bridge: the executor wraps this in a brackit {@code Sequence} and feeds ONE continuous stream
   * through brackit's own {@code SumAvgAggregator}, so served sum/avg reproduce the interpreter's
   * exact association order (seeding, batching, SIMD reduction) by construction instead of imitating
   * it.
   *
   * <p>
   * Single-threaded use only: the cursor borrows the calling thread's scan scratch, so no other
   * kernel/probe call may interleave on the same thread between {@link #advance()} calls (the
   * executor drains the aggregator synchronously, which satisfies this).
   */
  public static final class MatchingDoubleCursor {

    private final List<byte[]> rowGroupPayloads;
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

    public MatchingDoubleCursor(final List<byte[]> rowGroupPayloads,
        final ProjectionIndexScan.ColumnPredicate[] predicates, final int column) {
      if (predicates == null) {
        throw new IllegalArgumentException("predicates must not be null");
      }
      this.rowGroupPayloads = rowGroupPayloads;
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
          if (rowIdx >= rowCount)
            break;
          current = ProjectionDoubleEncoding.decode(getLongLE(payload, base + rowIdx * 8));
          return true;
        }
        if (payload != null && wordIdx < stride) {
          word = s.mask[wordIdx++];
          continue;
        }
        if (leafIdx >= rowGroupPayloads.size()) {
          return false;
        }
        payload = rowGroupPayloads.get(leafIdx++);
        final int columnCount = columnCountOf(payload);
        if (s.columnDataOff.length < columnCount) {
          s.columnDataOff = new int[columnCount];
          s.columnMinMaxOff = new int[columnCount];
        }
        rowCount = evaluateRowGroupMask(payload, predicates, s);
        if (rowCount <= 0) {
          payload = null;
          continue;
        }
        final byte kind = payload[24 + column];
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
          throw new IllegalStateException("cursor column " + column + " is not NUMERIC_DOUBLE (kind=" + kind + ")");
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
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT}). For every matching row the
   * composite key over all group columns is counted. Keys in {@code out} use the executor's typed
   * composite encoding — one {@code 's<len>:<utf8>'} segment per column (or {@code 'm'} for a MISSING
   * field, mirroring the typed slot-walk kernel's missing bucket), in {@code groupColumns} order — so
   * callers can decode group records with the same machinery as the typed slot-walk kernel.
   *
   * <p>
   * Sparse semantics: when the leaf carries a presence tail, rows on which a group field is missing
   * contribute the {@code 'm'} segment instead of the stored default. Leaves without a readable
   * presence tail keep the dense behavior — callers must gate via {@link #probeSparseEvidence} when
   * the data may be sparse.
   *
   * <p>
   * Per-leaf, per-cell lazy compose: the composite string for a (dictIdA, dictIdB, ...) cell is built
   * at most once per leaf via a packed-id cache, so the hot loop is one array/hash probe + one
   * {@code addTo} per row.
   */
  public static void conjunctiveCountByGroupMulti(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns,
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      long cellStride = 1L;
      for (int g = 0; g < m; g++) {
        final int col = groupColumns[g];
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
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
        presOffs[g] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, col)
            : -1;
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
          if (rowIdx >= rowCount)
            break;
          long cell = 0L;
          for (int g = 0; g < m; g++) {
            final boolean missing = presOffs[g] >= 0
                && (getLongLE(payload, presOffs[g] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) == 0L;
            final int id = missing
                ? dictSizes[g]
                : getIntLE(payload, idsOffs[g] + rowIdx * 4);
            cell = cell * (dictSizes[g] + 1L) + id;
          }
          String key = packable
              ? cellCache.get(cell)
              : null;
          if (key == null) {
            kb.setLength(0);
            long rem = cell;
            // decode ids back out in reverse, then emit in order
            final int[] ids = s.dictRemap != null && s.dictRemap.length >= m
                ? s.dictRemap
                : (s.dictRemap = new int[Math.max(8, m)]);
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
            if (packable)
              cellCache.put(cell, key);
          }
          out.addTo(key, 1L);
        }
      }
    }
  }

  /**
   * Dense multi-key conjunctive group-by-count: the composite-key analog of
   * {@link #conjunctiveCountByGroupDense}. Instead of materialising a composite {@code String} key
   * per cell and paying two hash probes per matching row ({@link #conjunctiveCountByGroupMulti}),
   * each group column gets a per-leaf {@code dictId → canonicalId} remap, and every matching row does
   * exactly one mixed-radix array increment: {@code counts[((idA) * (lenB+1) + idB) ...]++} where
   * index {@code canonLen_g} encodes MISSING for column {@code g}.
   *
   * <p>
   * Leaves whose dictionary carries a value absent from the canonical dict fall back to the composite
   * hashmap path ({@link #conjunctiveCountByGroupMulti}) for that leaf only, into {@code fallbackOut}
   * — the caller merges both accumulators (decode the counts array with the same mixed radix, then
   * {@code addTo}).
   *
   * <p>
   * HFT-grade: zero hashmap ops, zero String materialisation and zero hashing on the per-row path;
   * per-leaf remap cost is {@code dictSize × canonLen} tiny byte-compares per column.
   *
   * @param counts caller-zeroed accumulator of length {@code prod(canonicalDicts[g].length + 1)},
   *        mixed-radix ordered with column 0 as the most significant digit.
   */
  public static void conjunctiveCountByGroupMultiDense(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final byte[][][] canonicalDicts,
      final long[] counts, final Object2LongOpenHashMap<String> fallbackOut) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      boolean needsFallback = false;
      for (int g = 0; g < m; g++) {
        final int col = groupColumns[g];
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
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
            if (bytesEqualAt(payload, running, len, canon[c])) {
              hit = c;
              break;
            }
          }
          remap[i] = hit;
          if (hit < 0)
            needsFallback = true;
          running += len;
        }
        idsOffs[g] = running;
        presOffs[g] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, col)
            : -1;
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
          if (rowIdx >= rowCount)
            break;
          int idx = 0;
          for (int g = 0; g < m; g++) {
            final boolean missing = presOffs[g] >= 0
                && (getLongLE(payload, presOffs[g] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) == 0L;
            final int id = missing
                ? canonicalDicts[g].length
                : remaps[g][getIntLE(payload, idsOffs[g] + rowIdx * 4)];
            idx = idx * (canonicalDicts[g].length + 1) + id;
          }
          counts[idx]++;
        }
      }
    }
  }

  /**
   * Exact distinct PRESENT string values of {@code groupColumn} across all leaves — the
   * count-distinct fast path. Exploits a structural invariant of the leaf format: every non-empty
   * dictionary entry was interned by an actual row of that leaf (missing/unrepresentable rows intern
   * only the {@code ""} default), so with the column gated sparse-clean the distinct set is simply
   * the UNION of the per-leaf dictionaries — no per-row work at all, except to disambiguate a
   * {@code ""} dictionary entry, which may be a phantom from missing rows: on a fully-present leaf it
   * is real; on a leaf with missing rows the packed ids are scanned for a present row referencing it
   * (early exit on first hit).
   *
   * <p>
   * Valid ONLY for the unpredicated case (every row counts) and for sparse-clean columns — callers
   * gate on both.
   *
   * @param cardLimit bail-out bound: exceeding it returns {@code null} (caller falls back to the
   *        group-counting path, which handles any cardinality).
   * @return distinct present values as UTF-8 byte arrays (a {@code ""} value is included as a
   *         zero-length entry when real), or {@code null} when the kernel cannot serve (kind
   *         mismatch, malformed leaf, missing presence tail, cardinality exceeded).
   */
  public static ArrayList<byte[]> distinctPresentStrings(final List<byte[]> rowGroupPayloads, final int groupColumn,
      final int cardLimit) {
    if (rowGroupPayloads == null)
      return null;
    final ArrayList<byte[]> distinct = new ArrayList<>(16);
    boolean emptyReal = false;
    for (final byte[] payload : rowGroupPayloads) {
      if (payload == null)
        return null;
      final int rowCount = getIntLE(payload, 0);
      if (rowCount == 0)
        continue;
      final int columnCount = getIntLE(payload, 4);
      if (groupColumn < 0 || groupColumn >= columnCount)
        return null;
      if (payload[24 + groupColumn] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT)
        return null;
      final int groupBase = columnDataOffFor(payload, groupColumn);
      if (groupBase < 0)
        return null;
      final int dictSize = getIntLE(payload, groupBase);
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      final int dataEnd = leafDataEnd(payload);
      final int tailStart = dataEnd < 0
          ? -1
          : presenceTailStart(payload, dataEnd);
      if (tailStart < 0)
        return null;
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
            if (bytesEqualAt(payload, running, len, distinct.get(c))) {
              present = true;
              break;
            }
          }
          if (!present) {
            if (distinct.size() >= cardLimit)
              return null;
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
            if ((getLongLE(payload, presOff + (r >>> 6) * 8) & (1L << (r & 63))) == 0L)
              continue;
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
   * Conjunctive filter + group-by-count: walks {@code rowGroupPayloads} with the supplied
   * {@code predicates}, then for every matching row reads the {@code groupColumn}'s UTF-8 string
   * value and increments the matching group counter in {@code out}. The group column MUST be
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_DICT}.
   *
   * <p>
   * Per-leaf dict decode is lazy: each dict-id referenced by a matching row is decoded at most once
   * per leaf via a small {@code String[]} cache. Group-counter updates use
   * {@link Object2LongOpenHashMap#addTo} — one hashmap op per match, no box-on-insert.
   */
  public static void conjunctiveCountByGroup(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn,
      final Object2LongOpenHashMap<String> out) {
    conjunctiveCountByGroup(rowGroupPayloads, predicates, groupColumn, out, null);
  }

  /**
   * Sparse-aware variant of
   * {@link #conjunctiveCountByGroup(Iterable, ProjectionIndexScan.ColumnPredicate[], int, Object2LongOpenHashMap)}:
   * matching rows on which the group field is MISSING (presence bit clear) are counted into
   * {@code missingOut[0]} instead of polluting the string groups with the stored default.
   * {@code missingOut == null} keeps the dense behavior; leaves without a readable presence tail
   * always use it.
   */
  public static void conjunctiveCountByGroup(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn,
      final Object2LongOpenHashMap<String> out, final long[] missingOut) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn + " is not STRING_DICT (kind=" + groupKind + ")");
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
        for (int i = 0; i < dictSize; i++)
          dictCache[i] = null;
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
      final int tailStart = missingOut != null
          ? presenceTailStart(payload, s.leafDataEnd)
          : -1;
      final int presOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;

      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long presWord = presOff >= 0
            ? getLongLE(payload, presOff + w * 8)
            : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount)
            break;
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
   * [count, sum, min, max]}. First-seen ordinals ({@code rowGroupId << 20 | rowIdx} — rowIdx bounded
   * by {@link ProjectionIndexRowGroupPage#MAX_ROWS}) let callers emit groups in DOCUMENT
   * first-appearance order, the interpreter's grouping order.
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
   * max]} PER GROUP for every aggregate column, with each aggregate column's own presence AND
   * (missing cells contribute nothing, exactly like the interpreter's per-group {@code sum($r.f)}
   * over records lacking {@code f}). Matching rows whose GROUP field is missing fold into
   * {@code missingAcc} (the null-key group) instead of a string group. Accumulator layout:
   * {@link #groupAggSlots}. Dict interning mirrors {@link #conjunctiveCountByGroup}.
   */
  public static void conjunctiveAggregateByGroup(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final Object2ObjectOpenHashMap<String, long[]> out, final long[] missingAcc, final int leafIndexBase) {
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
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn + " is not STRING_DICT (kind=" + groupKind + ")");
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
        for (int i = 0; i < dictSize; i++)
          dictCache[i] = null;
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
      final int groupPresOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggColumns.length
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggColumns.length)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggColumns.length
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggColumns.length)]);
      for (int a = 0; a < aggColumns.length; a++) {
        // Same fail-loud per-leaf kind check the group column gets: a leaf whose kind
        // byte drifted from the handle metadata must never be folded as longs silently.
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final long leafOrdinalBase = ((long) (leafIndexBase + leaf)) << 20;
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long groupPresWord = groupPresOff >= 0
            ? getLongLE(payload, groupPresOff + w * 8)
            : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount)
            break;
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
            if (aggPresOff[a] >= 0 && (getLongLE(payload, aggPresOff[a] + w * 8) & (1L << bit)) == 0L) {
              continue;
            }
            final long v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            final int base = 2 + 4 * a;
            acc[base]++;
            // Exact sum or DECLINE: the interpreter promotes an overflowing xs:integer
            // sum to exact decimal — a wrapped long would silently serve a wrong total.
            acc[base + 1] = Math.addExact(acc[base + 1], v);
            if (v < acc[base + 2])
              acc[base + 2] = v;
            if (v > acc[base + 3])
              acc[base + 3] = v;
          }
        }
      }
    }
  }

  // ==================================================================
  // NUMERIC_LONG group-by kernels.
  // ==================================================================

  /**
   * Byte offsets of every column's zone-map {@code (min, max)} pair, written into {@code out} (which
   * must hold at least {@code columnCount} entries). Header walk only — the row payload is never
   * touched, and a STRING_DICT/STRING_SET column costs one read of its length header.
   *
   * @return the leaf's {@code rowCount}; {@code 0} for a rowless leaf, which carries NO per-column
   *         zone pair at all (so {@code out} is left untouched); {@code -1} on an unknown column kind
   */
  private static int columnZonePairOffsets(final byte[] payload, final int[] out) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    if (rowCount == 0) {
      return 0;
    }
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      out[c] = cursor;
      cursor += 16;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
        }
        default -> {
          return -1;
        }
      }
    }
    return rowCount;
  }

  /** Grow the per-thread offset scratch in lockstep so the two arrays never disagree in length. */
  private static void ensureOffsetScratch(final ScanScratch s, final int columnCount) {
    if (s.columnDataOff.length < columnCount) {
      s.columnDataOff = new int[columnCount];
      s.columnMinMaxOff = new int[columnCount];
    }
  }

  /**
   * Union of a NUMERIC_LONG column's per-leaf zone maps: {@code out3} receives
   * {@code {gMin, gMax, totalRows}}. The EAGER-handle twin of
   * {@link ProjectionColumnStore#columnZoneRange} — leaf HEADERS only, no row payload, no I/O.
   *
   * <p>
   * The union is what makes an index-by-subtraction group accumulator safe with no probe pass: zone
   * maps fold in only PRESENT, representable values, so under the caller's sparse-clean gate every
   * value the kernel will read is provably inside {@code [gMin, gMax]}.
   *
   * <p>
   * Leaves whose pair reads {@code min > max} (the all-missing marker) contribute rows but no range —
   * folding their sentinels in would produce an inverted union. A leaf whose range cannot be read at
   * all makes the whole union UNKNOWN ({@code false}); skipping it would under-report the range and
   * the dense arm would then address out of bounds.
   *
   * @return {@code false} — range UNKNOWN, caller must take the hash arm — on an empty leaf list, a
   *         null payload, a column index out of range, a kind that is not NUMERIC_LONG on some leaf,
   *         a structurally unwalkable leaf, or zero present rows across the whole index
   */
  public static boolean numericZoneUnion(final List<byte[]> rowGroupPayloads, final int col, final long[] out3) {
    if (out3 == null || out3.length < 3) {
      throw new IllegalArgumentException("out3 must hold at least three longs");
    }
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty() || col < 0) {
      return false;
    }
    final ScanScratch s = SCRATCH.get();
    long gMin = Long.MAX_VALUE;
    long gMax = Long.MIN_VALUE;
    long rows = 0;
    boolean anyRange = false;
    for (final byte[] payload : rowGroupPayloads) {
      if (payload == null) {
        return false;
      }
      final int columnCount = columnCountOf(payload);
      if (col >= columnCount) {
        return false;
      }
      if (!ProjectionIndexRowGroupPage.isOrderedLongKind(payload[24 + col])) {
        return false;
      }
      ensureOffsetScratch(s, columnCount);
      final int rowCount = columnZonePairOffsets(payload, s.columnMinMaxOff);
      if (rowCount < 0) {
        return false;
      }
      rows += rowCount;
      if (rowCount == 0) {
        continue;
      }
      final int off = s.columnMinMaxOff[col];
      final long min = getLongLE(payload, off);
      final long max = getLongLE(payload, off + 8);
      if (min > max) {
        continue;
      }
      if (min < gMin) {
        gMin = min;
      }
      if (max > gMax) {
        gMax = max;
      }
      anyRange = true;
    }
    if (!anyRange) {
      return false;
    }
    out3[0] = gMin;
    out3[1] = gMax;
    out3[2] = rows;
    return true;
  }

  /**
   * Zone-map pre-flight for per-group sums: declines BEFORE a single row is read when the aggregate
   * columns cannot be summed exactly in a {@code long}. Port of
   * {@code ProjectionColumnSegmentFoldScan#requireSumFitsLong} to the whole-leaf path.
   *
   * <p>
   * A single WHOLE-COLUMN bound covers every group, because each group's sum is a sub-multiset of the
   * column's — so one {@code O(leaves)} metadata check replaces an {@code O(rows)} scan that was
   * going to be thrown away. {@link Math#absExact} supplies the {@link Long#MIN_VALUE} guard for
   * free.
   *
   * <p>
   * The bound is CONSERVATIVE (predicates and presence only ever remove contributions), so this may
   * decline a shape whose actual sums would have fit — a slower correct answer instead of a wrapped
   * wrong one. It is an EARLY-decline optimization layered over the per-row {@link Math#addExact} in
   * the kernels, which remains the authority: on any leaf shape this walk cannot decode it abandons
   * the pre-flight rather than reporting a partial (and therefore too-small) bound.
   *
   * @throws ArithmeticException naming the column — callers treat it as a DECLINE
   */
  public static void requireGroupSumsFitLong(final List<byte[]> rowGroupPayloads, final int[] aggColumns) {
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty() || aggColumns == null || aggColumns.length == 0) {
      return;
    }
    final ScanScratch s = SCRATCH.get();
    final long[] bounds = new long[aggColumns.length];
    int failedColumn = -1;
    try {
      for (final byte[] payload : rowGroupPayloads) {
        if (payload == null) {
          return;
        }
        final int columnCount = columnCountOf(payload);
        ensureOffsetScratch(s, columnCount);
        final int rowCount = columnZonePairOffsets(payload, s.columnMinMaxOff);
        if (rowCount <= 0) {
          if (rowCount < 0) {
            return;
          }
          continue;
        }
        for (int a = 0; a < aggColumns.length; a++) {
          final int col = aggColumns[a];
          if (col < 0 || col >= columnCount) {
            return;
          }
          final int off = s.columnMinMaxOff[col];
          final long min = getLongLE(payload, off);
          final long max = getLongLE(payload, off + 8);
          if (min > max) {
            continue;
          }
          failedColumn = col;
          final long magnitude = Math.max(Math.absExact(min), Math.absExact(max));
          bounds[a] = Math.addExact(bounds[a], Math.multiplyExact((long) rowCount, magnitude));
        }
      }
    } catch (final ArithmeticException overflow) {
      throw new ArithmeticException("Projection column " + failedColumn
          + " cannot be summed exactly in a long: the zone-map magnitude bound over " + rowGroupPayloads.size()
          + " row groups leaves the signed 64-bit range");
    }
  }

  /**
   * Per-leaf preamble shared by the three numeric group kernels: verify the column kind, then locate
   * the group column's presence words.
   *
   * <p>
   * Presence is MANDATORY here, unlike in the string kernels. Those tolerate a missing tail and fall
   * back to dense semantics, which for a dict id merely mis-groups; for a VALUE the stored default of
   * a missing row (typically {@code 0}) is generally outside the zone-map range and would trip the
   * dense arm's range guard — or, worse, land inside it and fabricate a phantom group {@code 0}.
   *
   * @return the leaf's validated presence-tail start
   * @throws IllegalStateException on kind drift, an out-of-range column, or an unreadable presence
   *         tail — callers catch it as a DECLINE
   */
  private static int numericGroupTailStart(final byte[] payload, final int groupColumn, final ScanScratch s) {
    final int columnCount = columnCountOf(payload);
    if (groupColumn < 0 || groupColumn >= columnCount) {
      throw new IllegalStateException("groupColumn " + groupColumn + " out of range [0, " + columnCount + ")");
    }
    final byte kind = payload[24 + groupColumn];
    // STRING_GLOBAL joins NUMERIC_LONG: its cells are resource-wide dictionary ids stored in the
    // same long lane, and grouping by an id IS grouping by the value it stands for — that identity
    // is the whole point of the kind. What must NOT join is NUMERIC_DOUBLE, whose cells carry the
    // order-preserving transform rather than the value.
    // A temporal column joins for the same reason a global id does: its cell IS the group identity,
    // exactly and across leaves. Only the winner's TEXT has to be rendered, and that happens on
    // emission, where the caller knows the column's kind.
    if (!ProjectionIndexRowGroupPage.isOrderedLongKind(kind)
        && kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
      throw new IllegalStateException(
          "groupColumn " + groupColumn + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + kind + ")");
    }
    final int tailStart = presenceTailStart(payload, s.leafDataEnd);
    if (tailStart < 0) {
      throw new IllegalStateException("groupColumn " + groupColumn
          + " has no readable presence tail — a numeric group key must never fall back to the stored default");
    }
    return tailStart;
  }

  /**
   * Bit mask selecting the VALID rows of mask word {@code w} — the once-per-word form of the
   * {@code rowIdx >= rowCount} bound check the dict kernels re-run per set bit. Equivalent (the mask
   * builder already clears the final word's tail bits), cheaper, and it is what lets the hot loop
   * tally MISSING rows with one {@link Long#bitCount} instead of branching per row.
   */
  /**
   * Composite missing-component sentinel — part of the key identity, shared with the sliced kernels.
   */
  static final long MISSING_COMPONENT_HASH = 0x9E3779B97F4A7C15L;

  /**
   * Identity written under a set presence-mask bit for a component with no value at all; the mask bit
   * is what disambiguates it from a stored value that happens to encode the same way.
   */
  private static final long MISSING_COMPONENT_IDENTITY = 0L;

  /** Identity for a conditional else branch that carries no substitution literal. */
  private static final long ABSENT_ELSE_LITERAL_IDENTITY = 1L;
  /** FNV-1a chain seed/prime — shared so the sliced composite kernel cannot drift. */
  static final long FNV_SEED = 0xcbf29ce484222325L;
  static final long FNV_PRIME = 0x100000001b3L;

  /**
   * Per-call CROSS-LEAF regex transform cache: raw entry FNV → transformed-key FNV. Dict hashes reset
   * per leaf, but dictionary content repeats heavily ACROSS leaves — without this, a regex-keyed
   * 1M-row scan runs ~1M Matcher+replaceAll+getBytes rounds against ~100k distinct strings. Keying on
   * the raw 64-bit FNV is the same trust level the group identity already ships. One instance per
   * kernel call (worker × query) — a regex/replacement pair never outlives its query.
   */
  static final class RegexHashCache {
    final it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap map =
        new it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap(256);
    java.util.regex.Matcher matcher;

    RegexHashCache() {
      map.defaultReturnValue(Long.MIN_VALUE);
    }
  }

  /** The transformed-key hash of one dict entry, served from {@code cache} across leaves. */
  static long transformedKeyHash(final RegexHashCache cache, final java.util.regex.Pattern keyRegex,
      final String keyRegexRepl, final byte[] bytes, final int off, final int len) {
    final long raw = fnv1a64(bytes, off, len);
    final long cached = cache.map.get(raw);
    if (cached != Long.MIN_VALUE) {
      return cached;
    }
    final String src = new String(bytes, off, len, StandardCharsets.UTF_8);
    java.util.regex.Matcher m = cache.matcher;
    if (m == null) {
      m = cache.matcher = keyRegex.matcher(src);
    } else {
      m.reset(src);
    }
    final byte[] tb = m.replaceAll(keyRegexRepl).getBytes(StandardCharsets.UTF_8);
    final long h = fnv1a64(tb, 0, tb.length);
    cache.map.put(raw, h);
    return h;
  }

  static long validRowsMask(final int w, final int stride, final int rowCount) {
    final int lastBits = rowCount & 63;
    return lastBits != 0 && w == stride - 1
        ? -1L >>> 64 - lastBits
        : -1L;
  }

  /**
   * Conjunctive filter + group-by-count over a
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_NUMERIC_LONG} column — the HASH arm. Every
   * matching row bumps {@code out} under its primitive value; matching rows whose group field is
   * MISSING count into {@code missingOut[0]} (the null-key group), never under the stored default.
   *
   * <p>
   * HFT-grade: one 8-byte load and one {@link Long2LongOpenHashMap#addTo} probe per matching row. No
   * boxing (primitive key), no interning, no {@code String}, no per-leaf dictionary work, zero
   * allocation on the hot path.
   *
   * <p>
   * Counts cannot overflow and are deliberately unchecked: rows are bounded by
   * {@code leaves × MAX_ROWS ≤ 2^31 × 2^10 = 2^41}.
   *
   * @param out caller-supplied accumulator with {@code defaultReturnValue(0L)}
   * @param missingOut one-element sink for the missing-field group; must be non-null — presence is
   *        mandatory on this path (see {@link #numericGroupPresenceOff})
   * @throws IllegalStateException on column-kind drift or an unreadable presence tail — a DECLINE
   */
  public static void conjunctiveCountByGroupNumeric(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final Long2LongOpenHashMap out,
      final long[] missingOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null) {
      throw new IllegalArgumentException("out must not be null");
    }
    if (missingOut == null || missingOut.length < 1) {
      throw new IllegalArgumentException("missingOut is mandatory — the numeric group path requires presence");
    }
    final ScanScratch s = SCRATCH.get();
    long missing = 0;
    for (final byte[] payload : rowGroupPayloads) {
      ensureOffsetScratch(s, columnCountOf(payload));
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int presOff = presenceWordsOff(payload, numericGroupTailStart(payload, groupColumn, s), groupColumn);
      final int valOff = s.columnDataOff[groupColumn];
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        final long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final long presWord = getLongLE(payload, presOff + (w << 3));
        missing += Long.bitCount(word & ~presWord);
        long live = word & presWord;
        final int rowBase = w << 6;
        while (live != 0L) {
          final int bit = Long.numberOfTrailingZeros(live);
          live &= live - 1L;
          out.addTo(getLongLE(payload, valOff + (rowBase + bit) * 8), 1L);
        }
      }
    }
    missingOut[0] += missing;
  }

  /**
   * DENSE arm of {@link #conjunctiveCountByGroupNumeric}: the group index IS the value, offset by
   * {@code base}, so a matching row costs one 8-byte load and one array increment — no hash probe.
   * The caller sizes {@code counts} from the zone-map union ({@link #numericZoneUnion}) and passes
   * {@code base = gMin}.
   *
   * <p>
   * There is no {@code fallbackOut} counterpart to the string dense kernel's: values are globally
   * comparable, so "outside the accumulator" is not a late-arriving value, it is CORRUPTION (a zone
   * map disagreeing with its own column). The range guard converts it into a decline rather than an
   * {@link ArrayIndexOutOfBoundsException} or, worse, a silently wrong bucket. Keep the branch: it is
   * perfectly predicted (always false) and it is the only thing standing between a corrupt leaf and a
   * wrong answer.
   *
   * @param counts caller-zeroed accumulator; its LENGTH defines the addressable range, so it cannot
   *        drift from the guard
   * @throws IllegalStateException on kind drift, an unreadable presence tail, or a value outside
   *         {@code [base, base + counts.length)} — all DECLINES
   */
  public static void conjunctiveCountByGroupNumericDense(final Iterable<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final long base,
      final long[] counts, final long[] missingOut) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (counts == null || counts.length == 0) {
      throw new IllegalArgumentException("counts must be a non-empty caller-zeroed accumulator");
    }
    if (missingOut == null || missingOut.length < 1) {
      throw new IllegalArgumentException("missingOut is mandatory — the numeric group path requires presence");
    }
    final ScanScratch s = SCRATCH.get();
    final int cells = counts.length;
    long missing = 0;
    for (final byte[] payload : rowGroupPayloads) {
      ensureOffsetScratch(s, columnCountOf(payload));
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int presOff = presenceWordsOff(payload, numericGroupTailStart(payload, groupColumn, s), groupColumn);
      final int valOff = s.columnDataOff[groupColumn];
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        final long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final long presWord = getLongLE(payload, presOff + (w << 3));
        missing += Long.bitCount(word & ~presWord);
        long live = word & presWord;
        final int rowBase = w << 6;
        while (live != 0L) {
          final int bit = Long.numberOfTrailingZeros(live);
          live &= live - 1L;
          final long v = getLongLE(payload, valOff + (rowBase + bit) * 8);
          final long d = v - base;
          if (d < 0 || d >= cells) {
            throw new IllegalStateException("group value " + v + " outside zone range [" + base + ", " + (base + cells)
                + ") on column " + groupColumn);
          }
          counts[(int) d]++;
        }
      }
    }
    missingOut[0] += missing;
  }

  /**
   * Per-group NUMERIC_LONG aggregates keyed by a NUMERIC_LONG group column — the numeric twin of
   * {@link #conjunctiveAggregateByGroup}. Accumulator layout, per-aggregate-column presence AND,
   * exact-sum discipline and first-seen ordinals ({@code (leafIndexBase + leaf) << 20 | rowIdx},
   * min-on-merge) are unchanged: the ordinals are what make the served emission order match the
   * interpreter's document first-appearance order.
   *
   * <p>
   * Hash only, no dense arm: a per-group accumulator is {@code 2 + 4·aggCols} longs, so a dense array
   * of them is {@code long[cells × slots]} and explodes; and at the cardinality where per-group
   * aggregates actually appear the map is L1-resident and the probe is invisible against the per-row
   * presence + value loads.
   *
   * @param missingAcc accumulator for matching rows whose GROUP field is missing (the null-key group)
   * @throws ArithmeticException on a per-group sum overflow — a DECLINE
   * @throws IllegalStateException on kind drift or an unreadable presence tail — a DECLINE
   */
  public static void conjunctiveAggregateByGroupNumeric(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final Long2ObjectOpenHashMap<long[]> out, final long[] missingAcc, final int leafIndexBase) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null || missingAcc == null || aggColumns == null) {
      throw new IllegalArgumentException("out, missingAcc and aggColumns must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    final int aggCount = aggColumns.length;
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      final byte[] payload = rowGroupPayloads.get(leaf);
      ensureOffsetScratch(s, columnCountOf(payload));
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int tailStart = numericGroupTailStart(payload, groupColumn, s);
      final int groupPresOff = presenceWordsOff(payload, tailStart, groupColumn);
      final int groupValOff = s.columnDataOff[groupColumn];
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        // Same fail-loud per-leaf kind check the group column gets: a leaf whose kind byte
        // drifted from the handle metadata must never be folded as longs silently.
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = presenceWordsOff(payload, tailStart, aggColumns[a]);
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final long leafOrdinalBase = (long) (leafIndexBase + leaf) << 20;
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final long groupPresWord = getLongLE(payload, groupPresOff + (w << 3));
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          final long[] acc;
          if ((groupPresWord & 1L << bit) == 0L) {
            acc = missingAcc;
            if (acc[0] == 0) {
              // First missing-group row of this chunk — record its ordinal so the
              // null-key group emits at its document first-appearance position.
              acc[1] = leafOrdinalBase | rowIdx;
            }
          } else {
            final long gv = getLongLE(payload, groupValOff + rowIdx * 8);
            long[] existing = out.get(gv);
            if (existing == null) {
              existing = newGroupAggAcc(aggCount, leafOrdinalBase | rowIdx);
              out.put(gv, existing);
            }
            acc = existing;
          }
          acc[0]++;
          for (int a = 0; a < aggCount; a++) {
            if ((getLongLE(payload, aggPresOff[a] + (w << 3)) & 1L << bit) == 0L) {
              continue;
            }
            final long v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            final int base = 2 + 4 * a;
            acc[base]++;
            // Exact sum or DECLINE: the interpreter promotes an overflowing xs:integer
            // sum to exact decimal — a wrapped long would silently serve a wrong total.
            acc[base + 1] = Math.addExact(acc[base + 1], v);
            if (v < acc[base + 2]) {
              acc[base + 2] = v;
            }
            if (v > acc[base + 3]) {
              acc[base + 3] = v;
            }
          }
        }
      }
    }
  }

  /**
   * {@link #conjunctiveAggregateByGroupNumeric} writing into a {@link NumericGroupAggTable} instead
   * of a boxed map — identical fold, identical accumulator layout, identical exact-sum and
   * first-seen-ordinal discipline. The flat table removes the per-group {@code long[]} allocation and
   * the boxed probe, which the profile shows dominating the high-cardinality kernel once the
   * downstream sort is gone.
   */
  public static void conjunctiveAggregateByGroupNumericFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final NumericGroupAggTable out, final long[] missingAcc, final int leafIndexBase) {
    conjunctiveAggregateByGroupNumericFlat(rowGroupPayloads, predicates, groupColumn, aggColumns, out, missingAcc,
        leafIndexBase, -1, null, null, null, null, null, false, null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupNumericFlat} with an optional COUNT(DISTINCT) lane: agg block
   * {@code distinctBlock} is not folded — instead each present value enters the group's
   * {@link LongOpenHashSet} in {@code distinctOut} (keyed by the group value; the missing-key group's
   * set is {@code distinctMissing}). {@code budget} is {@code [remaining, exceededFlag]}: once
   * remaining goes negative the flag is set and the scan stops — the CALLER declines, a budget stop
   * must never look like an answer. The block's lanes stay zero; the caller writes the exact set size
   * into its sum lane after the partition-wise union (merging partial SIZES would double-count values
   * shared between threads).
   */
  public static void conjunctiveAggregateByGroupNumericFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final NumericGroupAggTable out, final long[] missingAcc, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final GroupDistinctAccumulator.Sink distinctMissing,
      final long[] budget, final ProjectionIndexScan.PredicateTree treeOrNull, final byte[] stringLengthModes,
      final boolean cdStringDict, final int[][] globalLengthTables) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null || missingAcc == null || aggColumns == null) {
      throw new IllegalArgumentException("out, missingAcc and aggColumns must not be null");
    }
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query that
    // asks only for min/max/count can never decline on an overflow no answer depends on —
    // see NumericGroupAggTable#sumsExact for the rule the partition merge obeys too.
    final long sumExactMask = out.sumExactMask();
    if (cdStringDict && distinctBlock < 0) {
      throw new IllegalArgumentException("cdStringDict without a distinct block");
    }
    final ScanScratch s = SCRATCH.get();
    final int aggCount = aggColumns.length;
    validateStringLengthModes(stringLengthModes, aggCount);
    // COUNT-ONLY: no aggregate lanes and no distinct set, so a row is one increment into a
    // [key, count, firstSeen] stripe. Decided ONCE, never per row.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    // Per-aggregate length caches for transformed string operands (one int per dictionary entry,
    // per leaf). Codepoint mode counts non-continuation bytes; UTF-8 mode reuses the wire length.
    final int[][] stringLengths = stringLengthModes != null
        ? new int[aggCount][]
        : null;
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines; nothing here is an answer
      }
      final byte[] payload = rowGroupPayloads.get(leaf);
      ensureOffsetScratch(s, columnCountOf(payload));
      final int rowCount = treeOrNull != null
          ? evaluateRowGroupMaskTree(payload, treeOrNull, s)
          : evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int tailStart = numericGroupTailStart(payload, groupColumn, s);
      final int groupPresOff = presenceWordsOff(payload, tailStart, groupColumn);
      final int groupValOff = s.columnDataOff[groupColumn];
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        final byte aggKind = payload[24 + aggColumns[a]];
        final byte lengthMode = stringLengthModes == null
            ? STRING_LENGTH_NONE
            : stringLengthModes[a];
        if (lengthMode != STRING_LENGTH_NONE) {
          if (globalLengthTables != null && globalLengthTables[a] != null) {
            // GLOBAL operand: the per-QUERY id→length table replaces the per-leaf per-entry pass —
            // the string work happened once per distinct value, rows read an int by id.
            if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
              throw new IllegalStateException(
                  "strlen aggColumn " + aggColumns[a] + " is not STRING_GLOBAL (kind=" + aggKind + ")");
            }
            aggValOff[a] = s.columnDataOff[aggColumns[a]];
            aggPresOff[a] = presenceWordsOff(payload, tailStart, aggColumns[a]);
            continue;
          }
          if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
            throw new IllegalStateException(
                "strlen aggColumn " + aggColumns[a] + " is not STRING_DICT (kind=" + aggKind + ")");
          }
          final int base = s.columnDataOff[aggColumns[a]];
          final int dictSize = getIntLE(payload, base);
          final int lenHeaderOff = base + 4;
          int[] lengths = stringLengths[a];
          if (lengths == null || lengths.length < dictSize) {
            stringLengths[a] = lengths = new int[Math.max(64, dictSize)];
          }
          int off = lenHeaderOff + dictSize * 4;
          for (int i = 0; i < dictSize; i++) {
            final int len = getIntLE(payload, lenHeaderOff + i * 4);
            int result = len;
            if (lengthMode == STRING_LENGTH_CODE_POINTS) {
              result = 0;
              for (int b = off; b < off + len; b++) {
                if ((payload[b] & 0xC0) != 0x80) {
                  result++;
                }
              }
            }
            lengths[i] = result;
            off += len;
          }
          aggValOff[a] = off; // ids region
          aggPresOff[a] = presenceWordsOff(payload, tailStart, aggColumns[a]);
          continue;
        }
        if (cdStringDict && a == distinctBlock) {
          if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
            throw new IllegalStateException(
                "distinct aggColumn " + aggColumns[a] + " is not STRING_DICT (kind=" + aggKind + ")");
          }
          aggValOff[a] = prepareCdDict(s, payload, s.columnDataOff[aggColumns[a]]); // ids region
          aggPresOff[a] = presenceWordsOff(payload, tailStart, aggColumns[a]);
          continue;
        }
        // A STRING_GLOBAL column is admitted in exactly ONE lane: the distinct operand, where its
        // cells are folded into a set of longs. Resource-wide ids need no dictionary and no content
        // hash — they ARE the identity — so the plain long lane below serves them unchanged. Every
        // other lane keeps the NUMERIC_LONG assert, because summing or averaging an id is a wrong
        // answer, not a slow one.
        final boolean globalDistinctOperand =
            a == distinctBlock && !cdStringDict && aggKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
        if (!globalDistinctOperand && !ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = presenceWordsOff(payload, tailStart, aggColumns[a]);
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      // Read AFTER the loop: prepareCdDict may have grown the scratch arrays.
      final int[] cdOff = cdStringDict
          ? s.cdDictOff
          : null;
      final int[] cdLen = cdStringDict
          ? s.cdDictLen
          : null;
      final long[] cdHashes = cdStringDict
          ? s.cdDictHash
          : null;
      final long leafOrdinalBase = (long) (leafIndexBase + leaf) << 20;
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      if (countOnly) {
        for (int w = 0; w < stride; w++) {
          long word = scanMask[w] & validRowsMask(w, stride, rowCount);
          final long groupPresWord = getLongLE(payload, groupPresOff + (w << 3));
          final int rowBase = w << 6;
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final int rowIdx = rowBase + bit;
            if ((groupPresWord & 1L << bit) == 0L) {
              if (missingAcc[0] == 0) {
                missingAcc[1] = leafOrdinalBase | rowIdx;
              }
              missingAcc[0]++;
            } else {
              final long gv = getLongLE(payload, groupValOff + rowIdx * 8);
              if (gv == 0L) {
                out.acquireZero(leafOrdinalBase | rowIdx)[0]++;
              } else {
                // Resolve storage AFTER acquire: growth can move the stripe to another chunk.
                final int handle = out.acquire(gv, leafOrdinalBase | rowIdx);
                final long[] block = out.storageAtAccBase(handle);
                block[out.offsetAtAccBase(handle)]++;
              }
            }
          }
        }
        continue;
      }
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final long groupPresWord = getLongLE(payload, groupPresOff + (w << 3));
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          final long[] slotArr;
          final int base;
          GroupDistinctAccumulator.Sink dset = null;
          if ((groupPresWord & 1L << bit) == 0L) {
            slotArr = missingAcc;
            base = 0;
            if (slotArr[0] == 0) {
              slotArr[1] = leafOrdinalBase | rowIdx;
            }
            dset = distinctMissing;
          } else {
            final long gv = getLongLE(payload, groupValOff + rowIdx * 8);
            if (gv == 0L) {
              slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
              base = 0;
            } else {
              // Order matters: a growing acquire can move the stripe, so resolve it AFTER.
              final int handle = out.acquire(gv, leafOrdinalBase | rowIdx);
              slotArr = out.storageAtAccBase(handle);
              base = out.offsetAtAccBase(handle);
            }
            if (distinctBlock >= 0) {
              dset = distinctOut.sinkFor(gv);
            }
          }
          slotArr[base]++;
          for (int a = 0; a < aggCount; a++) {
            final boolean stringLengthAgg = stringLengthModes != null && stringLengthModes[a] != STRING_LENGTH_NONE;
            final boolean present = (getLongLE(payload, aggPresOff[a] + (w << 3)) & 1L << bit) != 0L;
            if (!present && !stringLengthAgg) {
              continue;
            }
            // fn:string-length(()) is 0, never empty: a row MISSING the operand still
            // contributes 0 — skipping it would shrink counts and shift averages.
            final long v;
            if (stringLengthAgg) {
              if (globalLengthTables != null && globalLengthTables[a] != null) {
                // fn:string-length(()) is 0 — a missing operand still contributes 0 (same rule as
                // the dict arm). A present cell's id indexes the query-wide table; an id outside it
                // would be corruption and the bounds check keeps it loud.
                v = present
                    ? globalLengthTables[a][(int) getLongLE(payload, aggValOff[a] + rowIdx * 8)]
                    : 0L;
              } else {
                v = present
                    ? stringLengths[a][getIntLE(payload, aggValOff[a] + rowIdx * 4)]
                    : 0L;
              }
            } else if (cdStringDict && a == distinctBlock) {
              // STRING distinct operand: dict ids are LEAF-LOCAL, so the set member is the
              // entry's 64-bit content hash — exact up to a hash collision, the SAME standard
              // the composite group-key identity already accepts. Hashed once per entry per leaf.
              final int cdId = getIntLE(payload, aggValOff[a] + rowIdx * 4);
              long h = cdHashes[cdId];
              if (h == 0L) {
                h = fnv1a64(payload, cdOff[cdId], cdLen[cdId]);
                cdHashes[cdId] = h;
              }
              v = h;
            } else {
              v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            }
            if (a == distinctBlock) {
              dset.add(v); // exact and bounded inside the shared accumulator; its overrun declines the arm
              continue;
            }
            final int aggBase = base + 2 + 4 * a;
            slotArr[aggBase]++;
            // Exact sum or DECLINE — the interpreter promotes an overflowing xs:integer sum. A lane
            // no sum/avg reads is not folded at all, so a min-only query cannot decline on an
            // overflow nothing in its answer depends on.
            if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
              slotArr[aggBase + 1] = Math.addExact(slotArr[aggBase + 1], v);
            }
            if (v < slotArr[aggBase + 2]) {
              slotArr[aggBase + 2] = v;
            }
            if (v > slotArr[aggBase + 3]) {
              slotArr[aggBase + 3] = v;
            }
          }
        }
      }
    }
  }

  /**
   * {@link #conjunctiveAggregateByGroup} without Strings: groups are keyed by the 64-bit FNV-1a hash
   * of their bytes — the SAME identity the string kernels' intern table already trusts — into a flat
   * {@link NumericGroupAggTable} whose aux lane records {@code (leaf << 20) | dictId} of each group's
   * first sighting, so a caller materializes Strings for WINNING groups only (via
   * {@link #stringDictColumnBase}/{@link #dictEntryString}). Per row the fold is: dict id → cached
   * slot base (validated by key match, so table growth self-heals) → inline accumulate; a String is
   * never built and the per-distinct work is one hash + one probe per leaf.
   */
  public static void conjunctiveAggregateByGroupStringFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final NumericGroupAggTable out, final long[] missingAcc, final int leafIndexBase) {
    conjunctiveAggregateByGroupStringFlat(rowGroupPayloads, predicates, groupColumn, aggColumns, out, missingAcc,
        leafIndexBase, -1, null, null, null, null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupStringFlat} with the optional COUNT(DISTINCT) lane — the
   * string twin of the numeric overload's contract, with the group's set keyed by the SAME 64-bit
   * hash the table keys on.
   */
  public static void conjunctiveAggregateByGroupStringFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final NumericGroupAggTable out, final long[] missingAcc, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final GroupDistinctAccumulator.Sink distinctMissing,
      final long[] budget, final ProjectionIndexScan.PredicateTree treeOrNull) {
    conjunctiveAggregateByGroupStringFlat(rowGroupPayloads, predicates, groupColumn, aggColumns, out, missingAcc,
        leafIndexBase, distinctBlock, distinctOut, distinctMissing, budget, treeOrNull, null, null, null, null, false,
        null, null, null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupStringFlat} with the REGEX key transform and/or string-length
   * aggregate operands. {@code keyRegex} groups on the TRANSFORMED string — hashed once per
   * dictionary entry per leaf; a matched row MISSING the key field sets {@code regexDecline[0]}
   * (fn:replace over the empty sequence is {@code ""}, a REAL key the missing-key arm must not
   * absorb) and the caller declines. {@code stringLengthModes[a]} selects per-dictionary-entry
   * codepoint or UTF-8-byte counts, both with missing-is-0 semantics. {@code cdStringDict} marks the
   * distinct block's operand as STRING_DICT — see {@link #foldRowDistinct} for the leaf-local-id →
   * content-hash identity it feeds the set.
   */
  public static void conjunctiveAggregateByGroupStringFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int[] aggColumns,
      final NumericGroupAggTable out, final long[] missingAcc, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final GroupDistinctAccumulator.Sink distinctMissing,
      final long[] budget, final ProjectionIndexScan.PredicateTree treeOrNull, final Pattern keyRegex,
      final String keyRegexRepl, final long[] regexDecline, final byte[] stringLengthModes, final boolean cdStringDict,
      final GlobalValueDictionary.ReadView groupGlobalView, final int[][] globalLengthTables,
      final long @Nullable [] globalKeyHashes) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null || missingAcc == null || aggColumns == null) {
      throw new IllegalArgumentException("out, missingAcc and aggColumns must not be null");
    }
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query that
    // asks only for min/max/count can never decline on an overflow no answer depends on —
    // see NumericGroupAggTable#sumsExact for the rule the partition merge obeys too.
    final long sumExactMask = out.sumExactMask();
    if (cdStringDict && distinctBlock < 0) {
      throw new IllegalArgumentException("cdStringDict without a distinct block");
    }
    final ScanScratch s = SCRATCH.get();
    if (s.dictByteOff == null) {
      s.dictByteOff = new int[64];
    }
    if (s.dictHashCache == null) {
      s.dictHashCache = new long[64];
      s.dictSlotBase = new int[64];
    }
    int[] dictByteOff = s.dictByteOff;
    long[] dictHash = s.dictHashCache;
    int[] dictBase = s.dictSlotBase;
    final RegexHashCache regexCache = keyRegex != null
        ? new RegexHashCache()
        : null;
    // Global-group caches, valid across leaves because the ids are resource-wide: transformed-key
    // hash per id, and the id's resolved accumulator base (the dictBase twin, rehash-validated the
    // same way). With a PRECOMPUTED hash table (id-indexed, built by one sequential sweep of the
    // dictionary before the row loop) the per-worker lazy memo is skipped entirely — its first-sight
    // resolutions are RANDOM point reads, and at a scale where the decoded dictionary exceeds the
    // record cache each one is a cache-missing block decode (measured: q29 16 s → 330 s at 100M).
    final Long2LongOpenHashMap gidHash = groupGlobalView != null && globalKeyHashes == null
        ? new Long2LongOpenHashMap()
        : null;
    final Long2IntOpenHashMap gidBase = groupGlobalView != null && globalKeyHashes == null
        ? new Long2IntOpenHashMap()
        : null;
    if (gidBase != null) {
      gidBase.defaultReturnValue(-1);
    }
    final int aggCount = aggColumns.length;
    validateStringLengthModes(stringLengthModes, aggCount);
    // COUNT-ONLY: no aggregate lanes and no distinct set, so the fold is one increment into a
    // [key, count, firstSeen, aux] stripe. Loop-invariant — the dict cache below is the group
    // identity and stays in both shapes.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    final int[][] stringLengths = stringLengthModes != null
        ? new int[aggCount][]
        : null;
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines
      }
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = treeOrNull != null
          ? evaluateRowGroupMaskTree(payload, treeOrNull, s)
          : evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final byte groupKind = payload[24 + groupColumn];
      final boolean globalGroup = groupGlobalView != null;
      if (globalGroup) {
        // Resource-wide id group key: the ONLY admitted shape is the regex-transformed one — an
        // untransformed global key is an integer group-by and never routes here. The transformed
        // hash is cached per id ACROSS leaves (ids are global), so the regex runs once per
        // distinct value per worker, not once per row.
        if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "groupColumn " + groupColumn + " is not STRING_GLOBAL (kind=" + groupKind + ")");
        }
        if (keyRegex == null) {
          throw new IllegalStateException("a global group column reaches the string kernel only regex-transformed");
        }
      } else if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn + " is not STRING_DICT (kind=" + groupKind + ")");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int dictSize = globalGroup
          ? 0
          : getIntLE(payload, groupBase);
      if (dictByteOff.length < dictSize) {
        final int newSize = Math.max(dictByteOff.length * 2, dictSize);
        dictByteOff = s.dictByteOff = new int[newSize];
      }
      if (dictHash.length < dictSize) {
        final int newSize = Math.max(dictHash.length * 2, dictSize);
        dictHash = s.dictHashCache = new long[newSize];
        dictBase = s.dictSlotBase = new int[newSize];
      }
      final int lenHeaderOff = groupBase + 4;
      final int concatOff = lenHeaderOff + dictSize * 4;
      int running = concatOff;
      for (int i = 0; i < dictSize; i++) {
        dictByteOff[i] = running;
        running += getIntLE(payload, lenHeaderOff + i * 4);
        dictBase[i] = -1; // unresolved for THIS leaf
      }
      final int idsOff = globalGroup
          ? groupBase
          : running;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int groupPresOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        final byte aggKind = payload[24 + aggColumns[a]];
        final byte lengthMode = stringLengthModes == null
            ? STRING_LENGTH_NONE
            : stringLengthModes[a];
        if (lengthMode != STRING_LENGTH_NONE) {
          if (globalLengthTables != null && globalLengthTables[a] != null) {
            // GLOBAL operand: the per-query id table replaces the per-leaf entry pass entirely.
            if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
              throw new IllegalStateException(
                  "strlen aggColumn " + aggColumns[a] + " is not STRING_GLOBAL (kind=" + aggKind + ")");
            }
            aggValOff[a] = s.columnDataOff[aggColumns[a]];
            aggPresOff[a] = tailStart >= 0
                ? presenceWordsOff(payload, tailStart, aggColumns[a])
                : -1;
            continue;
          }
          if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
            throw new IllegalStateException(
                "strlen aggColumn " + aggColumns[a] + " is not STRING_DICT (kind=" + aggKind + ")");
          }
          final int aBase = s.columnDataOff[aggColumns[a]];
          final int aDictSize = getIntLE(payload, aBase);
          final int aLensOff = aBase + 4;
          int[] lengths = stringLengths[a];
          if (lengths == null || lengths.length < aDictSize) {
            stringLengths[a] = lengths = new int[Math.max(64, aDictSize)];
          }
          int aOff = aLensOff + aDictSize * 4;
          for (int i = 0; i < aDictSize; i++) {
            final int len = getIntLE(payload, aLensOff + i * 4);
            int result = len;
            if (lengthMode == STRING_LENGTH_CODE_POINTS) {
              result = 0;
              for (int b = aOff; b < aOff + len; b++) {
                if ((payload[b] & 0xC0) != 0x80) {
                  result++;
                }
              }
            }
            lengths[i] = result;
            aOff += len;
          }
          aggValOff[a] = aOff; // ids region
          aggPresOff[a] = tailStart >= 0
              ? presenceWordsOff(payload, tailStart, aggColumns[a])
              : -1;
          continue;
        }
        if (cdStringDict && a == distinctBlock) {
          if (aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
            throw new IllegalStateException(
                "distinct aggColumn " + aggColumns[a] + " is not STRING_DICT (kind=" + aggKind + ")");
          }
          aggValOff[a] = prepareCdDict(s, payload, s.columnDataOff[aggColumns[a]]); // ids region
          aggPresOff[a] = tailStart >= 0
              ? presenceWordsOff(payload, tailStart, aggColumns[a])
              : -1;
          continue;
        }
        // See the numeric-flat twin: STRING_GLOBAL is admitted in the distinct-operand lane only,
        // where its resource-wide ids go straight into a set of longs. This is the JSONBench Q2
        // shape — a per-leaf-dict GROUP key with a high-cardinality global distinct operand.
        final boolean globalDistinctOperand =
            a == distinctBlock && !cdStringDict && aggKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
        if (!globalDistinctOperand && !ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      // Read AFTER the loop: prepareCdDict may have grown the scratch arrays.
      final int[] cdOff = cdStringDict
          ? s.cdDictOff
          : null;
      final int[] cdLen = cdStringDict
          ? s.cdDictLen
          : null;
      final long[] cdHashes = cdStringDict
          ? s.cdDictHash
          : null;
      final long leafOrdinalBase = (long) (leafIndexBase + leaf) << 20;
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long groupPresWord = groupPresOff >= 0
            ? getLongLE(payload, groupPresOff + w * 8)
            : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          final long[] slotArr;
          final int base;
          GroupDistinctAccumulator.Sink dset = null;
          if (groupPresOff >= 0 && (groupPresWord & 1L << bit) == 0L) {
            if (keyRegex != null) {
              // fn:replace over the missing field's empty sequence is "" — a REAL key.
              regexDecline[0] = 1;
              return;
            }
            slotArr = missingAcc;
            base = 0;
            if (slotArr[0] == 0) {
              slotArr[1] = leafOrdinalBase | rowIdx;
            }
            dset = distinctMissing;
          } else if (globalGroup) {
            final long gid = getLongLE(payload, idsOff + rowIdx * 8);
            final long ordinal = leafOrdinalBase | rowIdx;
            // With PRECOMPUTED hashes the per-worker gid→base memo is a net LOSS: it is a
            // multi-million-key map probed per row (DRAM miss each), against a group-table acquire
            // whose probe touches a table sized by GROUPS. Skip it and acquire directly.
            int cached = globalKeyHashes != null
                ? -1
                : gidBase.get(gid);
            long h;
            if (globalKeyHashes != null) {
              // Precomputed by one sequential sweep — same function (utf8Hash of the TRANSFORMED
              // string), so the hash domain matches pass 2 and the winner rebuild identically.
              h = globalKeyHashes[(int) gid];
            } else if (cached >= 0) {
              h = gidHash.get(gid);
              if (out.keyAtAccBase(cached) != h) {
                cached = -1; // table rehashed since this id resolved — re-acquire below
              }
            } else {
              h = gidHash.get(gid); // 0 = not yet transformed
            }
            if (cached < 0) {
              if (h == 0L && globalKeyHashes == null) {
                // Regex applied ONCE per distinct id per worker; the hash domain is identical to
                // the per-leaf arm's (utf8Hash of the TRANSFORMED string) so pass 2 and the
                // winner rebuild match either arm's groups.
                final String transformed =
                    keyRegex.matcher(groupGlobalView.valueAsString((int) gid)).replaceAll(keyRegexRepl);
                h = utf8Hash(transformed.getBytes(StandardCharsets.UTF_8));
                gidHash.put(gid, h);
              }
              if (h == 0L) {
                final boolean fresh = !out.hasZeroKey();
                final long[] zero = out.acquireZero(ordinal);
                if (fresh) {
                  out.setZeroAux(gid);
                }
                if (countOnly) {
                  zero[0]++;
                } else if (distinctBlock < 0) {
                  foldRow(zero, 0, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, stringLengthModes,
                      stringLengths, globalLengthTables, sumExactMask);
                } else {
                  foldRowDistinct(zero, 0, payload, aggPresOff, aggValOff, aggCount, distinctBlock,
                      distinctOut.sinkFor(0L), budget, w, bit, rowIdx, stringLengthModes, stringLengths,
                      globalLengthTables, cdOff, cdLen, cdHashes, sumExactMask);
                }
                continue;
              }
              cached = out.acquire(h, ordinal);
              final long[] cachedStorage = out.storageAtAccBase(cached);
              final int cachedOffset = out.offsetAtAccBase(cached);
              if (cachedStorage[cachedOffset] == 0L) {
                // Aux carries the GLOBAL ID itself — resource-wide, so no (leaf, dictId) packing;
                // the executor's winner materialization resolves it through the dictionary.
                out.setAuxAtAccBase(cached, gid);
              }
              if (globalKeyHashes == null) {
                gidBase.put(gid, cached);
              }
            }
            if (distinctBlock >= 0) {
              dset = distinctOut.sinkFor(h);
            }
            slotArr = out.storageAtAccBase(cached);
            base = out.offsetAtAccBase(cached);
          } else {
            final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
            final long ordinal = leafOrdinalBase | rowIdx;
            int cached = dictBase[dictId];
            long h;
            if (cached >= 0) {
              h = dictHash[dictId];
              // Rehash-safe validation: keys are unique, so a key match IS the group. The key
              // sits one lane BELOW the cached block base — same stripe, same cache line.
              if (out.keyAtAccBase(cached) != h) {
                cached = -1;
              }
            } else {
              h = 0L;
            }
            if (cached < 0) {
              if (h == 0L) {
                if (keyRegex != null) {
                  h = transformedKeyHash(regexCache, keyRegex, keyRegexRepl, payload, dictByteOff[dictId],
                      getIntLE(payload, lenHeaderOff + dictId * 4));
                } else {
                  h = fnv1a64(payload, dictByteOff[dictId], getIntLE(payload, lenHeaderOff + dictId * 4));
                }
                dictHash[dictId] = h;
              }
              if (h == 0L) {
                // A value whose FNV hash IS the empty-bucket sentinel: zero side slot.
                final boolean fresh = !out.hasZeroKey();
                final long[] zero = out.acquireZero(ordinal);
                if (fresh) {
                  out.setZeroAux(leafOrdinalBase | dictId);
                }
                if (countOnly) {
                  zero[0]++;
                } else if (distinctBlock < 0) {
                  foldRow(zero, 0, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, stringLengthModes,
                      stringLengths, globalLengthTables, sumExactMask);
                } else {
                  foldRowDistinct(zero, 0, payload, aggPresOff, aggValOff, aggCount, distinctBlock,
                      distinctOut.sinkFor(0L), budget, w, bit, rowIdx, stringLengthModes, stringLengths,
                      globalLengthTables, cdOff, cdLen, cdHashes, sumExactMask);
                }
                continue;
              }
              cached = out.acquire(h, ordinal);
              // Every acquire is followed by a fold, so count 0 means the entry was created
              // just now — stamp the source reference exactly once.
              final long[] cachedStorage = out.storageAtAccBase(cached);
              final int cachedOffset = out.offsetAtAccBase(cached);
              if (cachedStorage[cachedOffset] == 0L) {
                out.setAuxAtAccBase(cached, leafOrdinalBase | dictId);
              }
              dictBase[dictId] = cached;
            }
            if (distinctBlock >= 0) {
              dset = distinctOut.sinkFor(h);
            }
            slotArr = out.storageAtAccBase(cached);
            base = out.offsetAtAccBase(cached);
          }
          if (countOnly) {
            slotArr[base]++;
          } else if (distinctBlock < 0) {
            foldRow(slotArr, base, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, stringLengthModes,
                stringLengths, globalLengthTables, sumExactMask);
          } else {
            foldRowDistinct(slotArr, base, payload, aggPresOff, aggValOff, aggCount, distinctBlock, dset, budget, w,
                bit, rowIdx, stringLengthModes, stringLengths, globalLengthTables, cdOff, cdLen, cdHashes,
                sumExactMask);
          }
        }
      }
    }
  }

  /**
   * Fold EVERY matching row of every leaf into ONE accumulator block ({@link #newGroupAggAcc} layout;
   * the ordinal lane is unused) — the single-group kernel behind {@code group by
   * <constant>}: N aggregates over their operand columns in one pass, no group column read at all.
   */
  public static void conjunctiveAggregateAllNumeric(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] aggColumns, final long[] acc) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (acc == null || aggColumns == null) {
      throw new IllegalArgumentException("acc and aggColumns must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    final int aggCount = aggColumns.length;
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          foldRow(acc, 0, payload, aggPresOff, aggValOff, aggCount, w, bit, rowBase + bit, -1L);
        }
      }
    }
  }

  /**
   * COMPOSITE-key flat group aggregation: 2..5 key components, each NUMERIC_LONG or STRING_DICT,
   * combined into one 64-bit identity by chaining LEAF-INDEPENDENT component hashes (numeric:
   * {@code HashCommon.mix(value)}; dict: the FNV-1a of the entry BYTES, cached per leaf entry;
   * missing component: a fixed sentinel — a missing part is PART of the key, per the multi-key
   * contract, never a separate null-key group). Identity is hash-only — the same assumption the
   * string kernels' intern already trusts. The table's aux lane records the FIRST-SEEN row
   * ({@code (leaf << 20) | rowIdx}), so a caller materializes the K winning keys by re-reading their
   * component values from that one row each — no per-group key materialization during the scan at
   * all.
   */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase) {
    conjunctiveAggregateByGroupCompositeFlat(rowGroupPayloads, predicates, groupColumns, aggColumns, out, leafIndexBase,
        -1, null, null, null, null, null, null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupCompositeFlat} with the COUNT(DISTINCT) lane, keyed by the
   * same composite hash the table keys on (no missing-key side set: a missing component is part of
   * the key here). Same budget-decline contract as the single-key overloads.
   */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget) {
    conjunctiveAggregateByGroupCompositeFlat(rowGroupPayloads, predicates, groupColumns, aggColumns, out, leafIndexBase,
        distinctBlock, distinctOut, budget, null, null, null, null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupCompositeFlat} with per-component KEY TRANSFORMS:
   * {@code keyOffsets[k]} shifts a numeric component ({@code $r.f + c} keys — grouping happens on the
   * SHIFTED value; a non-injective transform grouped raw would over-partition), and
   * {@code keySubstr[2k, 2k+1]} applies {@code xs:integer(substring(entry, start, len))} to a dict
   * component, evaluated once per dictionary entry per leaf. {@code declineFlag[0] != 0} signals a
   * case the interpreter RAISES on (missing operand under a substring transform, a lexically invalid
   * slice, an overflowing shift) — the caller declines, and the generic pipeline raises identically.
   */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull) {
    conjunctiveAggregateByGroupCompositeFlat(rowGroupPayloads, predicates, groupColumns, aggColumns, out, leafIndexBase,
        distinctBlock, distinctOut, budget, keyOffsets, keySubstr, declineFlag, treeOrNull, null, null, null, null,
        null);
  }

  /**
   * {@link #conjunctiveAggregateByGroupCompositeFlat} with the CONDITIONAL key transform
   * ({@code if ($r.c1 = L1 [and $r.c2 = L2]) then $r.f else "lit"} — the Q39 CASE WHEN port):
   * {@code keyCondCols[2k]} ({@code -1} = key {@code k} unconditional) and {@code 2k+1} name the
   * NUMERIC condition columns, {@code keyCondLits} the literals, {@code keyCondElse[k]} the else
   * literal's UTF-8 bytes. Condition truth per row is {@code present AND value == literal} — the
   * general comparison's existential (missing ⇒ false). The then-branch reads the dict component
   * exactly like an untransformed key (missing field ⇒ the empty-sequence key); the else branch
   * hashes the literal bytes in the SAME FNV domain as dict entries, so a stored value equal to the
   * literal merges into the interpreter's group.
   *
   * <p>
   * {@code keyCondElse[k]} with NO condition column ({@code keyCondCols[2k] < 0}) is the
   * MISSING-value substitution instead — the {@code fn:string($r.f)} key, whose only difference from
   * the bare deref is that an absent field reads as {@code ""}. Same FNV domain, for the same reason:
   * a stored {@code ""} must land in the group the interpreter puts it in.
   *
   * <p>
   * {@code keyDivMod} carries {@code ($r.f idiv D) mod M} per key ({@code 2k} the divisor,
   * {@code 2k+1} the modulus, {@code 0} = not applied): integer date-part extraction over a
   * NUMERIC_LONG component, applied AFTER {@code keyOffsets} and grouped on the transformed value.
   * Java's {@code /} and {@code %} are XQuery's {@code idiv} and {@code mod} for every input, so no
   * case declines.
   */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int[] keyCondCols, final long[] keyCondLits, final byte[][] keyCondElse, final long[] keyDivMod) {
    conjunctiveAggregateByGroupCompositeFlat(rowGroupPayloads, predicates, groupColumns, aggColumns, out, leafIndexBase,
        distinctBlock, distinctOut, budget, keyOffsets, keySubstr, declineFlag, treeOrNull, keyCondCols, keyCondLits,
        keyCondElse, keyDivMod, null);
  }

  /** Global-string substring-cast capable final overload; views are aligned to group components. */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int[] keyCondCols, final long[] keyCondLits, final byte[][] keyCondElse, final long[] keyDivMod,
      final GlobalValueDictionary.ReadView[] globalKeyViews) {
    conjunctiveAggregateByGroupCompositeFlat(rowGroupPayloads, predicates, groupColumns, aggColumns, out, leafIndexBase,
        distinctBlock, distinctOut, budget, keyOffsets, keySubstr, declineFlag, treeOrNull, keyCondCols, keyCondLits,
        keyCondElse, keyDivMod, globalKeyViews, null, null);
  }

  /**
   * Final overload, additionally PROVING that per-leaf dictionary string components are identified
   * exactly rather than merely fingerprinted. See
   * {@link ProjectionColumnGroupScan#aggregateByGroupCompositeFlat} for the argument.
   *
   * @param identityRegistry shared across this scan's workers, or {@code null} when every component
   *        is numeric or substring-cast and therefore already exact in one lane
   */
  public static void conjunctiveAggregateByGroupCompositeFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final NumericGroupAggTable out, final int leafIndexBase, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int[] keyCondCols, final long[] keyCondLits, final byte[][] keyCondElse, final long[] keyDivMod,
      final GlobalValueDictionary.ReadView[] globalKeyViews, final ProjectionStringIdentityRegistry identityRegistry,
      final long[] globalCondElseIds) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null || aggColumns == null || groupColumns == null) {
      throw new IllegalArgumentException("out, aggColumns and groupColumns must not be null");
    }
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query that
    // asks only for min/max/count can never decline on an overflow no answer depends on —
    // see NumericGroupAggTable#sumsExact for the rule the partition merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final ScanScratch s = SCRATCH.get();
    final int keyCount = groupColumns.length;
    final int aggCount = aggColumns.length;
    // COUNT-ONLY: no aggregate lanes and no distinct set, so the fold is one increment into a
    // [key, count, firstSeen, aux] stripe. Loop-invariant, decided once per call.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    // Per-component per-leaf dict caches (entry hash by dict id); components are <= 5.
    final long[][] compDictHash = new long[keyCount][];
    // Identity lanes per dictionary entry — see CompositeGroupIdentity for why the composite probe
    // hash cannot double as the group's identity.
    final long[][] compDictIdA = new long[keyCount][];
    final long[][] compDictIdB = new long[keyCount][];
    // Lazy identity proof: a dictionary entry is proven on its FIRST use by a surviving row of the
    // leaf, never for the whole dictionary — the canonical set is exactly the group keys the answer
    // holds, not every string the visited leaves store.
    final int[][] compDictOff = new int[keyCount][];
    final long[][] compDictProven = new long[keyCount][];
    final int[] compDictLenOff = new int[keyCount];
    final boolean[] compNeedsProof = new boolean[keyCount];
    final int[] compPresOff = new int[keyCount];
    final int[] compValOff = new int[keyCount];
    final byte[] compKind = new byte[keyCount];
    final long[] condElseHash = keyCondCols != null
        ? new long[keyCount]
        : null;
    final long[] condElseIdA = keyCondCols != null
        ? new long[keyCount]
        : null;
    final long[] condElseIdB = keyCondCols != null
        ? new long[keyCount]
        : null;
    // A projection column carries ONE kind across every leaf (ProjectionColumnStore#columnKind:
    // "every leaf carries the same shape"), so the identity layout is fixed once from the first
    // payload; the per-leaf pass below re-asserts it rather than trusting it silently.
    final byte[] identityKinds = new byte[keyCount];
    if (!rowGroupPayloads.isEmpty()) {
      final byte[] first = rowGroupPayloads.get(0);
      for (int k = 0; k < keyCount; k++) {
        identityKinds[k] = first[24 + groupColumns[k]];
      }
    }
    if (identityRegistry == null && CompositeGroupIdentity.hasFingerprintedComponent(identityKinds, keySubstr)) {
      // FAIL CLOSED. A dictionary-string component is identified by a fingerprint pair, which
      // discriminates but does not identify; only ProjectionStringIdentityRegistry's canonical byte
      // comparison makes it exact. The older public overloads delegate here with a null registry,
      // so without this guard they would silently serve a probabilistic identity — the exact defect
      // the registry exists to remove. Numeric and substring-cast keys are unaffected: they carry
      // their raw or cast value in an exact lane and need no registry.
      throw new IllegalArgumentException("composite key has a dictionary-string component and therefore requires a "
          + "ProjectionStringIdentityRegistry; the registry-less overload cannot identify it exactly");
    }
    final int[] idLane = CompositeGroupIdentity.laneOffsets(identityKinds, keySubstr);
    final int identityWidth = idLane[keyCount];
    if (out.idWidth() != identityWidth) {
      throw new IllegalArgumentException(
          "group table identity width " + out.idWidth() + " does not match the composite key's " + identityWidth);
    }
    final long[] identity = new long[identityWidth];
    final boolean[] twoLane = new boolean[keyCount];
    for (int k = 0; k < keyCount; k++) {
      twoLane[k] = idLane[k + 1] - idLane[k] == 2;
    }
    // Pre-proven components carry their lanes as exact identity (their column's strings are
    // memoized pairwise distinct under this registry's fingerprint); eager mode proves every
    // dictionary entry in the dictionary pass — the sliced kernel's contract, mirrored exactly.
    final boolean[] compPreProven = new boolean[keyCount];
    boolean anyProof = false;
    for (int k = 0; k < keyCount; k++) {
      if (identityRegistry != null && twoLane[k]) {
        compPreProven[k] = identityRegistry.preProven(k);
        anyProof |= !compPreProven[k];
      }
    }
    final boolean eagerProof = anyProof && identityRegistry.proveEveryEntry();
    final ProjectionStringIdentityRegistry.LocalProofCache proofCache = anyProof
        ? new ProjectionStringIdentityRegistry.LocalProofCache(keyCount)
        : null;
    final int[] condPresOff = keyCondCols != null
        ? new int[2 * keyCount]
        : null;
    final int[] condValOff = keyCondCols != null
        ? new int[2 * keyCount]
        : null;
    if (keyCondCols != null) {
      for (int k = 0; k < keyCount; k++) {
        // Both roles of the literal — the conditional else branch and the missing-value
        // substitution — hash once here, in the dictionary's own domain.
        if (keyCondElse[k] != null && identityKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          // GLOBAL then-branch: identity space is the id space, so the else literal is its
          // RESOLVED id — a then-row holding the same value merges exactly. An uninterned literal
          // means no stored row can equal it, and the caller's sentinel (-2, below every real id
          // and outside the presence-marked constants) keeps the else group separate.
          final long elseId = globalCondElseIds != null
              ? globalCondElseIds[k]
              : Long.MIN_VALUE;
          if (elseId == Long.MIN_VALUE) {
            throw new IllegalStateException(
                "global conditional component " + k + " reached the kernel without a resolved else id");
          }
          condElseHash[k] = HashCommon.mix(elseId);
          condElseIdA[k] = elseId;
          condElseIdB[k] = 0L;
          continue;
        }
        if (keyCondElse[k] != null) {
          condElseHash[k] = fnv1a64(keyCondElse[k], 0, keyCondElse[k].length);
          condElseIdA[k] = condElseHash[k];
          condElseIdB[k] = GlobalValueDictionary.secondaryValueHash(keyCondElse[k], 0, keyCondElse[k].length);
          if (identityRegistry != null && twoLane[k]) {
            if (compPreProven[k]) {
              throw new IllegalStateException(
                  "composite key component " + k + " is pre-proven but carries an else literal");
            }
            final byte[] lit = keyCondElse[k];
            final long a = identityRegistry.laneA(lit, 0, lit.length, condElseHash[k]);
            final long b = identityRegistry.laneB(lit, 0, lit.length);
            // Same seam as a dictionary entry: the literal's PROBE contribution follows lane A too.
            condElseHash[k] = a;
            condElseIdA[k] = a;
            condElseIdB[k] = b;
            if (!proofCache.prove(identityRegistry, k, a, b, lit, 0, lit.length)) {
              return;
            }
          }
        }
      }
    }
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines
      }
      if (declineFlag != null && declineFlag[0] != 0) {
        return; // a transform case the interpreter raises on — the caller declines
      }
      if (identityRegistry != null && !identityRegistry.identityProven()) {
        return; // string identity cannot be proven — the caller declines
      }
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = treeOrNull != null
          ? evaluateRowGroupMaskTree(payload, treeOrNull, s)
          : evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      for (int k = 0; k < keyCount; k++) {
        final int col = groupColumns[k];
        final byte kind = payload[24 + col];
        if (kind != identityKinds[k]) {
          throw new IllegalStateException("composite key component " + col + " changes kind across leaves ("
              + identityKinds[k] + " -> " + kind + "), which the identity layout cannot describe");
        }
        compKind[k] = kind;
        compPresOff[k] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, col)
            : -1;
        if (ProjectionIndexRowGroupPage.isOrderedLongKind(kind)
            || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
              && (globalKeyViews == null || globalKeyViews.length != keyCount || globalKeyViews[k] == null)) {
            // Untransformed and conditional global components fold the ID itself (the id IS the
            // exact identity); only the substring cast reads value bytes, and it does so through
            // this same view. Either way the dictionary must be readable.
            throw new IllegalStateException("global composite key component requires a readable dictionary view");
          }
          compValOff[k] = s.columnDataOff[col];
        } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          // Pre-hash the WHOLE dictionary once per leaf (<= 1024 entries): per-row cost is
          // then one id load + one array index per component. Under a substring transform the
          // per-entry value IS the transformed integer (hashed), with Long.MIN_VALUE marking a
          // slice the cast would raise on — referenced by a row, that declines the serve.
          final int groupBase = s.columnDataOff[col];
          final int dictSize = getIntLE(payload, groupBase);
          final int lenHeaderOff = groupBase + 4;
          long[] hashes = compDictHash[k];
          if (hashes == null || hashes.length < dictSize) {
            hashes = compDictHash[k] = new long[Math.max(64, dictSize)];
          }
          long[] idA = compDictIdA[k];
          if (idA == null || idA.length < dictSize) {
            idA = compDictIdA[k] = new long[Math.max(64, dictSize)];
          }
          // POSITIVE start = the xs:integer(substring(...)) cast; 0 = none. The STRING
          // substring variant (negative start) never reaches this kernel — it routes to the
          // packed single-key arm, and the executor gate declines it for composites.
          final int subStart = keySubstr != null
              ? keySubstr[2 * k]
              : 0;
          final int subLen = keySubstr != null
              ? keySubstr[2 * k + 1]
              : 0;
          long[] idB = null;
          if (subStart <= 0) {
            idB = compDictIdB[k];
            if (idB == null || idB.length < dictSize) {
              idB = compDictIdB[k] = new long[Math.max(64, dictSize)];
            }
          }
          int off = lenHeaderOff + dictSize * 4;
          int[] dictOff = compDictOff[k];
          if (dictOff == null || dictOff.length < dictSize) {
            dictOff = compDictOff[k] = new int[Math.max(64, dictSize)];
          }
          compDictLenOff[k] = lenHeaderOff;
          final boolean proveEntries = identityRegistry != null && subStart <= 0 && !compPreProven[k];
          final boolean proveEagerly = proveEntries && eagerProof;
          compNeedsProof[k] = proveEntries && !eagerProof;
          if (compNeedsProof[k]) {
            final int words = dictSize + 63 >>> 6;
            long[] proven = compDictProven[k];
            if (proven == null || proven.length < words) {
              proven = compDictProven[k] = new long[Math.max(1, words)];
            }
            Arrays.fill(proven, 0, words, 0L);
          }
          for (int i = 0; i < dictSize; i++) {
            final int len = getIntLE(payload, lenHeaderOff + i * 4);
            if (subStart > 0) {
              final long tv = xsIntegerOfSubstring(payload, off, len, subStart, subLen);
              hashes[i] = tv == Long.MIN_VALUE
                  ? Long.MIN_VALUE
                  : HashCommon.mix(tv);
              idA[i] = tv;
            } else {
              final long primary = fnv1a64(payload, off, len);
              if (identityRegistry == null) {
                hashes[i] = primary;
                idA[i] = primary;
                idB[i] = GlobalValueDictionary.secondaryValueHash(payload, off, len);
              } else {
                final long a = identityRegistry.laneA(payload, off, len, primary);
                final long b = identityRegistry.laneB(payload, off, len);
                // Probe hash follows lane A — identical in production, and the only way an injected
                // fingerprint can emulate a TRUE simultaneous collision rather than half of one.
                hashes[i] = a;
                idA[i] = a;
                idB[i] = b;
                // Eager: every entry is proven here. Lazy: the byte proof waits for the first
                // surviving row that names this entry.
                if (proveEagerly && !proofCache.prove(identityRegistry, k, a, b, payload, off, len)) {
                  return; // fingerprint collision or exhausted budget — the caller declines
                }
              }
            }
            dictOff[i] = off;
            off += len;
          }
          compValOff[k] = off; // ids region
        } else {
          throw new IllegalStateException("composite key component " + col + " has unsupported kind " + kind);
        }
      }
      if (keyCondCols != null) {
        for (int c2 = 0; c2 < 2 * keyCount; c2++) {
          final int cc = keyCondCols[c2];
          if (cc >= 0) {
            final byte condKind = payload[24 + cc];
            if (condKind != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
              throw new IllegalStateException(
                  "condition column " + cc + " is not NUMERIC_LONG (kind=" + condKind + ")");
            }
            condPresOff[c2] = tailStart >= 0
                ? presenceWordsOff(payload, tailStart, cc)
                : -1;
            condValOff[c2] = s.columnDataOff[cc];
          }
        }
      }
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final long leafOrdinalBase = (long) (leafIndexBase + leaf) << 20;
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          long h = 0xcbf29ce484222325L;
          long presenceMask = 0L;
          for (int k = 0; k < keyCount; k++) {
            final boolean subTransformed = keySubstr != null && keySubstr[2 * k] > 0;
            final int lane = idLane[k];
            final long compHash;
            if (keyCondCols != null && keyCondCols[2 * k] >= 0) {
              boolean condTrue = true;
              for (int j = 0; j < 2 && condTrue; j++) {
                final int cc = keyCondCols[2 * k + j];
                if (cc < 0) {
                  continue;
                }
                if (condPresOff[2 * k + j] >= 0
                    && (getLongLE(payload, condPresOff[2 * k + j] + (w << 3)) & 1L << bit) == 0L) {
                  condTrue = false; // missing condition operand: the comparison is false
                } else if (getLongLE(payload, condValOff[2 * k + j] + rowIdx * 8) != keyCondLits[2 * k + j]) {
                  condTrue = false;
                }
              }
              if (!condTrue) {
                compHash = condElseHash[k];
                if (keyCondElse[k] != null) {
                  identity[lane] = condElseIdA[k];
                  if (twoLane[k]) {
                    identity[lane + 1] = condElseIdB[k];
                  }
                } else {
                  presenceMask |= 1L << k;
                  identity[lane] = ABSENT_ELSE_LITERAL_IDENTITY;
                  if (twoLane[k]) {
                    identity[lane + 1] = 0L;
                  }
                }
              } else if (compPresOff[k] >= 0 && (getLongLE(payload, compPresOff[k] + (w << 3)) & 1L << bit) == 0L) {
                compHash = 0x9E3779B97F4A7C15L; // then-branch over a missing field: empty-sequence key
                presenceMask |= 1L << k;
                identity[lane] = MISSING_COMPONENT_IDENTITY;
                if (twoLane[k]) {
                  identity[lane + 1] = 0L;
                }
              } else if (compKind[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
                // Conditional THEN over a global component: the id is the exact identity, in the
                // same lane domain the resolved else id lives in.
                final long gid = getLongLE(payload, compValOff[k] + rowIdx * 8);
                compHash = HashCommon.mix(gid);
                identity[lane] = gid;
              } else {
                final int dictId = getIntLE(payload, compValOff[k] + rowIdx * 4);
                if (compNeedsProof[k] && !proveOnFirstUse(identityRegistry, proofCache, k, compDictProven[k], dictId,
                    compDictIdA[k][dictId], compDictIdB[k][dictId], payload, compDictOff[k][dictId],
                    getIntLE(payload, compDictLenOff[k] + dictId * 4))) {
                  return; // fingerprint collision or exhausted budget — the caller declines
                }
                compHash = compDictHash[k][dictId];
                identity[lane] = compDictIdA[k][dictId];
                if (twoLane[k]) {
                  identity[lane + 1] = compDictIdB[k][dictId];
                }
              }
            } else if (compPresOff[k] >= 0 && (getLongLE(payload, compPresOff[k] + (w << 3)) & 1L << bit) == 0L) {
              if (subTransformed) {
                // xs:integer(substring((), s, l)) = xs:integer("") — the interpreter RAISES.
                declineFlag[0] = 1;
                return;
              }
              // fn:string(()) is "" — the substitution literal's hash, in the dict domain, so the
              // group merges with a stored empty string exactly as the interpreter's does.
              // Otherwise a fixed sentinel: part of the identity, not a side group.
              compHash = condElseHash != null && keyCondElse[k] != null
                  ? condElseHash[k]
                  : MISSING_COMPONENT_HASH;
              if (condElseHash != null && keyCondElse[k] != null) {
                identity[lane] = condElseIdA[k];
                if (twoLane[k]) {
                  identity[lane + 1] = condElseIdB[k];
                }
              } else {
                presenceMask |= 1L << k;
                identity[lane] = MISSING_COMPONENT_IDENTITY;
                if (twoLane[k]) {
                  identity[lane + 1] = 0L;
                }
              }
            } else if (ProjectionIndexRowGroupPage.isOrderedLongKind(compKind[k])) {
              long v = getLongLE(payload, compValOff[k] + rowIdx * 8);
              if (keyOffsets != null && keyOffsets[k] != 0L) {
                final long shifted = v + keyOffsets[k];
                if (((v ^ shifted) & (keyOffsets[k] ^ shifted)) < 0) {
                  declineFlag[0] = 1; // overflow: the interpreter promotes to decimal
                  return;
                }
                v = shifted;
              }
              v = applyDivMod(v, keyDivMod, k);
              compHash = HashCommon.mix(v);
              identity[lane] = v;
            } else if (compKind[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
              if (subTransformed) {
                final int id = Math.toIntExact(getLongLE(payload, compValOff[k] + rowIdx * 8));
                final long transformed =
                    globalKeyViews[k].xsIntegerOfSubstring(id, keySubstr[2 * k], keySubstr[2 * k + 1]);
                if (transformed == Long.MIN_VALUE) {
                  declineFlag[0] = 1;
                  return;
                }
                compHash = HashCommon.mix(transformed);
                identity[lane] = transformed;
              } else {
                // Untransformed global component: the id IS the exact identity — no dictionary
                // bytes read, no content hash, one lane.
                final long gid = getLongLE(payload, compValOff[k] + rowIdx * 8);
                compHash = HashCommon.mix(gid);
                identity[lane] = gid;
              }
            } else {
              final int dictId = getIntLE(payload, compValOff[k] + rowIdx * 4);
              if (compNeedsProof[k] && !proveOnFirstUse(identityRegistry, proofCache, k, compDictProven[k], dictId,
                  compDictIdA[k][dictId], compDictIdB[k][dictId], payload, compDictOff[k][dictId],
                  getIntLE(payload, compDictLenOff[k] + dictId * 4))) {
                return; // fingerprint collision or exhausted budget — the caller declines
              }
              compHash = compDictHash[k][dictId];
              if (subTransformed && compHash == Long.MIN_VALUE) {
                declineFlag[0] = 1; // a row references a slice the cast raises on
                return;
              }
              identity[lane] = compDictIdA[k][dictId];
              if (twoLane[k]) {
                identity[lane + 1] = compDictIdB[k][dictId];
              }
            }
            h = h * 0x100000001b3L ^ compHash;
          }
          identity[0] = presenceMask;
          final int handle = out.acquireExact(h, leafOrdinalBase | rowIdx, identity, 0);
          final long[] slotArr = out.storageAtAccBase(handle);
          final int base = out.offsetAtAccBase(handle);
          if (slotArr[base] == 0L) {
            out.setAuxAtAccBase(handle, leafOrdinalBase | rowIdx);
          }
          if (countOnly) {
            slotArr[base]++;
          } else if (distinctBlock < 0) {
            foldRow(slotArr, base, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, sumExactMask);
          } else {
            foldRowDistinct(slotArr, base, payload, aggPresOff, aggValOff, aggCount, distinctBlock,
                distinctOut.sinkFor(h), budget, w, bit, rowIdx, sumExactMask);
          }
        }
      }
    }
  }

  /**
   * Materialize the composite key parts of ONE row — the winner-side companion of
   * {@link #conjunctiveAggregateByGroupCompositeFlat}: {@code outStrings[k]} for a present
   * STRING_DICT component, {@code outLongs[k]} (with {@code outIsLong[k]}) for a present NUMERIC_LONG
   * one, neither for a missing component ({@code outPresent[k] == false}). {@code columnDataOff} must
   * be this payload's offsets from {@link #columnOffsets}.
   */
  public static void readRowKeyParts(final byte[] payload, final int[] columnDataOff, final int leafDataEnd,
      final int[] groupColumns, final int rowIdx, final String[] outStrings, final long[] outLongs,
      final boolean[] outPresent, final boolean[] outIsLong) {
    readRowKeyParts(payload, columnDataOff, leafDataEnd, groupColumns, rowIdx, outStrings, outLongs, outPresent,
        outIsLong, null, null);
  }

  /**
   * {@link #readRowKeyParts} applying the same key transforms the composite kernel groups by —
   * winners must emit the TRANSFORMED value. A raise-case cannot appear here: the kernel already
   * declined the serve before any winner existed.
   */
  public static void readRowKeyParts(final byte[] payload, final int[] columnDataOff, final int leafDataEnd,
      final int[] groupColumns, final int rowIdx, final String[] outStrings, final long[] outLongs,
      final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets, final int[] keySubstr) {
    readRowKeyParts(payload, columnDataOff, leafDataEnd, groupColumns, rowIdx, outStrings, outLongs, outPresent,
        outIsLong, keyOffsets, keySubstr, null, null, null, null);
  }

  /**
   * {@link #readRowKeyParts} with the CONDITIONAL key transform — the winner-side companion of the
   * kernel's conditional arm: the same per-row condition picks the dict entry or the else literal, so
   * the emitted key part is byte-for-byte the value the row grouped under.
   */
  public static void readRowKeyParts(final byte[] payload, final int[] columnDataOff, final int leafDataEnd,
      final int[] groupColumns, final int rowIdx, final String[] outStrings, final long[] outLongs,
      final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets, final int[] keySubstr,
      final int[] keyCondCols, final long[] keyCondLits, final String[] keyCondElse, final long[] keyDivMod) {
    readRowKeyParts(payload, columnDataOff, leafDataEnd, groupColumns, rowIdx, outStrings, outLongs, outPresent,
        outIsLong, keyOffsets, keySubstr, keyCondCols, keyCondLits, keyCondElse, keyDivMod, null);
  }

  /** Global-string substring-cast capable winner materialisation; views align to key components. */
  public static void readRowKeyParts(final byte[] payload, final int[] columnDataOff, final int leafDataEnd,
      final int[] groupColumns, final int rowIdx, final String[] outStrings, final long[] outLongs,
      final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets, final int[] keySubstr,
      final int[] keyCondCols, final long[] keyCondLits, final String[] keyCondElse, final long[] keyDivMod,
      final GlobalValueDictionary.ReadView[] globalKeyViews) {
    final int tailStart = presenceTailStart(payload, leafDataEnd);
    for (int k = 0; k < groupColumns.length; k++) {
      final int col = groupColumns[k];
      if (keyCondCols != null && keyCondCols[2 * k] >= 0) {
        boolean condTrue = true;
        for (int j = 0; j < 2 && condTrue; j++) {
          final int cc = keyCondCols[2 * k + j];
          if (cc < 0) {
            continue;
          }
          if (tailStart >= 0) {
            final int cPresOff = presenceWordsOff(payload, tailStart, cc);
            if ((getLongLE(payload, cPresOff + (rowIdx >>> 6) * 8) & 1L << (rowIdx & 63)) == 0L) {
              condTrue = false;
              continue;
            }
          }
          if (getLongLE(payload, columnDataOff[cc] + rowIdx * 8) != keyCondLits[2 * k + j]) {
            condTrue = false;
          }
        }
        if (!condTrue) {
          outPresent[k] = true;
          outIsLong[k] = false;
          outStrings[k] = keyCondElse[k];
          continue;
        }
        // Condition holds: fall through to the plain dict read below (missing ⇒ absent part).
      }
      if (tailStart >= 0) {
        final int presOff = presenceWordsOff(payload, tailStart, col);
        if ((getLongLE(payload, presOff + (rowIdx >>> 6) * 8) & 1L << (rowIdx & 63)) == 0L) {
          // fn:string over a missing field emits the substitution literal the kernel grouped it
          // under; a conditional key (handled above) keeps the empty-sequence part.
          if (keyCondElse != null && keyCondElse[k] != null && (keyCondCols == null || keyCondCols[2 * k] < 0)) {
            outPresent[k] = true;
            outIsLong[k] = false;
            outStrings[k] = keyCondElse[k];
          } else {
            outPresent[k] = false;
          }
          continue;
        }
      }
      outPresent[k] = true;
      final byte kind = payload[24 + col];
      if (ProjectionIndexRowGroupPage.isOrderedLongKind(kind)) {
        outIsLong[k] = true;
        outLongs[k] = applyDivMod(getLongLE(payload, columnDataOff[col] + rowIdx * 8) + (keyOffsets != null
            ? keyOffsets[k]
            : 0L), keyDivMod, k);
      } else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        if (globalKeyViews == null || globalKeyViews.length != groupColumns.length || globalKeyViews[k] == null) {
          throw new IllegalStateException("global composite winner requires a readable dictionary view");
        }
        if (keySubstr != null && keySubstr[2 * k] > 0) {
          outIsLong[k] = true;
          outLongs[k] = globalKeyViews[k].xsIntegerOfSubstring(
              Math.toIntExact(getLongLE(payload, columnDataOff[col] + rowIdx * 8)), keySubstr[2 * k],
              keySubstr[2 * k + 1]);
        } else {
          // Untransformed (or conditional-then) global component: the winner's key part is the
          // interned value itself — one dictionary read per winner.
          outIsLong[k] = false;
          outStrings[k] =
              globalKeyViews[k].valueAsString(Math.toIntExact(getLongLE(payload, columnDataOff[col] + rowIdx * 8)));
        }
      } else {
        outIsLong[k] = false;
        final int groupBase = columnDataOff[col];
        final int dictSize = getIntLE(payload, groupBase);
        final int lenHeaderOff = groupBase + 4;
        int off = lenHeaderOff + dictSize * 4;
        int total = 0;
        for (int i = 0; i < dictSize; i++) {
          total += getIntLE(payload, lenHeaderOff + i * 4);
        }
        final int idsOff = off + total;
        final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
        int entryOff = off;
        for (int i = 0; i < dictId; i++) {
          entryOff += getIntLE(payload, lenHeaderOff + i * 4);
        }
        final int entryLen = getIntLE(payload, lenHeaderOff + dictId * 4);
        if (keySubstr != null && keySubstr[2 * k] > 0) {
          outIsLong[k] = true;
          outLongs[k] = xsIntegerOfSubstring(payload, entryOff, entryLen, keySubstr[2 * k], keySubstr[2 * k + 1]);
        } else {
          outStrings[k] = new String(payload, entryOff, entryLen, StandardCharsets.UTF_8);
        }
      }
    }
  }

  /**
   * {@code (v idiv D) mod M} for the group key's divmod transform — {@code keyDivMod[2k]} the divisor
   * and {@code 2k+1} the modulus, {@code 0} meaning the operation is absent.
   *
   * <p>
   * Exact, not approximate: Java's {@code /} truncates toward zero exactly as XQuery's {@code idiv}
   * does and Java's {@code %} takes the dividend's sign exactly as XQuery's {@code mod} does, so
   * every input — negatives included — yields the interpreter's value. The caller admits only
   * POSITIVE constants, which is also what rules out the two arithmetic traps: a zero divisor
   * (FOAR0001 in the interpreter) and {@code Long.MIN_VALUE / -1} (overflow).
   */
  static long applyDivMod(final long v, final long[] keyDivMod, final int k) {
    if (keyDivMod == null) {
      return v;
    }
    long out = v;
    final long divisor = keyDivMod[2 * k];
    if (divisor > 0L) {
      out /= divisor;
    }
    final long modulus = keyDivMod[2 * k + 1];
    if (modulus > 0L) {
      out %= modulus;
    }
    return out;
  }

  /**
   * {@code xs:integer(substring(entry, start, len))} as a long — the transform the composite kernel
   * groups by. {@code Long.MIN_VALUE} = a case the CAST raises on (empty slice, non-digit, beyond
   * long) or a non-ASCII entry (fn:substring counts CODEPOINTS; declining beats an off-by-index
   * answer). fn:substring clamps out-of-range windows to the empty string.
   */
  static long xsIntegerOfSubstring(final byte[] bytes, final int off, final int len, final int start,
      final int subLen) {
    // fn:substring is 1-based, so start < 1 addresses BEFORE the value. Unguarded, `start - 1` went
    // negative and the window opened at off - 1, reading a byte of the previous packed value.
    // Negative lengths and windows computed in int arithmetic have the same shape of bug for an
    // extreme start, so the window is derived in long arithmetic and clamped.
    if (start < 1 || subLen < 0) {
      return Long.MIN_VALUE;
    }
    for (int i = off; i < off + len; i++) {
      if (bytes[i] < 0) {
        return Long.MIN_VALUE;
      }
    }
    final long s0 = (long) start - 1L;
    int from = off + (int) Math.min(s0, len);
    int end = off + (int) Math.min((long) len, s0 + subLen);
    // xs:integer's whiteSpace facet collapses surrounding whitespace before the cast.
    while (from < end && (bytes[from] == ' ' || bytes[from] == '\t' || bytes[from] == '\n' || bytes[from] == '\r')) {
      from++;
    }
    while (end > from
        && (bytes[end - 1] == ' ' || bytes[end - 1] == '\t' || bytes[end - 1] == '\n' || bytes[end - 1] == '\r')) {
      end--;
    }
    if (from >= end) {
      return Long.MIN_VALUE;
    }
    boolean neg = false;
    if (bytes[from] == '+' || bytes[from] == '-') {
      neg = bytes[from] == '-';
      from++;
    }
    if (from >= end) {
      return Long.MIN_VALUE;
    }
    long v = 0;
    for (int i = from; i < end; i++) {
      final int d = bytes[i] - '0';
      if (d < 0 || d > 9) {
        return Long.MIN_VALUE;
      }
      if (v > Long.MAX_VALUE / 10L || (v == Long.MAX_VALUE / 10L && d > Long.MAX_VALUE % 10L)) {
        return Long.MIN_VALUE; // beyond long: the interpreter goes to big-integer — decline
      }
      v = v * 10 + d;
    }
    return neg
        ? -v
        : v;
  }

  /**
   * Column offsets + data end for one payload — the winner-side offset computation (headers only, no
   * mask work); cache the result per leaf when materializing many winners.
   *
   * @return {@code [columnDataOff.., leafDataEnd]} — the last slot carries the data end
   */
  public static int[] columnOffsets(final byte[] payload) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    final int[] result = new int[columnCount + 1];
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;
      result[c] = cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += (rowCount + 63 >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
        }
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
    }
    result[columnCount] = cursor;
    return result;
  }

  /**
   * MIN or MAX over a STRING_DICT column, from the dictionaries: per leaf, mark the entries a PRESENT
   * row references (a dictionary may hold phantom entries interned by missing rows — an unreferenced
   * {@code ""} must never win a MIN), then compare only those. Collation follows the interpreter's
   * {@code Str#cmp}: unsigned byte order unless either side carries a 4-byte UTF-8 lead, then decoded
   * {@code String.compareTo}. Cost is the ids sweep plus at most dictSize comparisons per leaf — no
   * row value is ever materialized.
   *
   * @return the extremum, or {@code null} when no present row carries the field anywhere (the caller
   *         emits the empty sequence or falls back)
   */
  public static String stringDictMinMax(final List<byte[]> rowGroupPayloads, final int column, final boolean min) {
    final ScanScratch s = SCRATCH.get();
    byte[] bestPayload = null;
    int bestOff = 0;
    int bestLen = 0;
    long[] referenced = null;
    for (final byte[] payload : rowGroupPayloads) {
      final int rowCount = getIntLE(payload, 0);
      if (rowCount <= 0) {
        continue;
      }
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      // Offsets + presence tail via the conjunctive builder with zero predicates.
      evaluateRowGroupMask(payload, NO_PREDICATES, s);
      final byte kind = payload[24 + column];
      if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("column " + column + " is not STRING_DICT (kind=" + kind + ")");
      }
      final int groupBase = s.columnDataOff[column];
      final int dictSize = getIntLE(payload, groupBase);
      if (dictSize <= 0) {
        continue;
      }
      final int lenHeaderOff = groupBase + 4;
      int concatOff = lenHeaderOff + dictSize * 4;
      int total = 0;
      for (int i = 0; i < dictSize; i++) {
        total += getIntLE(payload, lenHeaderOff + i * 4);
      }
      final int idsOff = concatOff + total;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int presOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, column)
          : -1;
      final int refWords = dictSize + 63 >>> 6;
      if (referenced == null || referenced.length < refWords) {
        referenced = new long[Math.max(16, refWords)];
      } else {
        Arrays.fill(referenced, 0, refWords, 0L);
      }
      for (int r = 0; r < rowCount; r++) {
        if (presOff >= 0 && (getLongLE(payload, presOff + (r >>> 6) * 8) & 1L << (r & 63)) == 0L) {
          continue;
        }
        final int id = getIntLE(payload, idsOff + r * 4);
        referenced[id >>> 6] |= 1L << (id & 63);
      }
      int off = concatOff;
      for (int i = 0; i < dictSize; i++) {
        final int len = getIntLE(payload, lenHeaderOff + i * 4);
        if ((referenced[i >>> 6] & 1L << (i & 63)) != 0L) {
          if (bestPayload == null || compareStrSlices(payload, off, len, bestPayload, bestOff, bestLen) * (min
              ? 1
              : -1) < 0) {
            bestPayload = payload;
            bestOff = off;
            bestLen = len;
          }
        }
        off += len;
      }
    }
    return bestPayload == null
        ? null
        : new String(bestPayload, bestOff, bestLen, StandardCharsets.UTF_8);
  }

  /**
   * PASS 2 of the deferred string-aggregate route: fold {@code min}/{@code max} over STRING_DICT
   * operand columns for the K WINNING groups only. Pass 1 (numeric kernel + top-K) selected the
   * winners; an aggregate that appears only in the emission record cannot change which groups win or
   * their order, so its exact value is computed afterwards for K groups instead of all of them. Group
   * membership matches pass 1's identity exactly: the FNV-64 of the key entry bytes
   * ({@code winnerHashes}), plus the missing-key group as slot {@code winnerHashes.length} when
   * {@code winnerMissingKey}. Comparison authority: {@link #compareStrSlices} — the collation every
   * dict kernel shares. Best-so-far is held as (payload, off, len) slices, so a row costs one
   * early-exit byte compare against its group's current best; rows whose operand is MISSING
   * contribute nothing (min over present values only, the interpreter's fn:min semantics).
   *
   * <p>
   * {@code bestOut[a][slot]} receives the materialized best value, or stays {@code null} when no row
   * of the group carries the operand (min over the empty sequence). Thread-partials merge caller-side
   * via {@link String#compareTo} — the identical collation on materialized values.
   */
  public static void stringAggForWinnerGroups(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int groupColumn, final int[] stringAggColumns, final boolean[] aggIsMin, final long[] winnerHashes,
      final boolean winnerMissingKey, final String[][] bestOut) {
    stringAggForWinnerGroups(rowGroupPayloads, predicates, treeOrNull, groupColumn, stringAggColumns, aggIsMin,
        winnerHashes, winnerMissingKey, bestOut, null, null);
  }

  /**
   * {@link #stringAggForWinnerGroups} under a REGEX-transformed key: row-to-winner matching hashes
   * the TRANSFORMED entry — the same identity pass 1 grouped on.
   */
  public static void stringAggForWinnerGroups(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int groupColumn, final int[] stringAggColumns, final boolean[] aggIsMin, final long[] winnerHashes,
      final boolean winnerMissingKey, final String[][] bestOut, final Pattern keyRegex, final String keyRegexRepl) {
    stringAggForWinnerGroups(rowGroupPayloads, predicates, treeOrNull, groupColumn, stringAggColumns, aggIsMin,
        winnerHashes, winnerMissingKey, bestOut, keyRegex, keyRegexRepl, null, null, null);
  }

  /**
   * {@link #stringAggForWinnerGroups} with GLOBAL operand columns admitted: {@code aggGlobalViews} is
   * index-aligned with {@code stringAggColumns}, a non-null entry marking that column as
   * {@code STRING_GLOBAL} and supplying the dictionary view its ids resolve through. The best value
   * of a global operand is tracked as an ID under {@link GlobalValueDictionary.ReadView#compareIds} —
   * the same UTF-16 collation {@code compareStrSlices} gives the per-leaf entries — and materialized
   * once per winning group at the end. Rows whose id repeats the incumbent skip without a dictionary
   * touch, which on low-cardinality operands is nearly every row.
   *
   * <p>
   * The view is a PER-WORKER object (its slice caches are not thread-safe); callers running fold
   * partials in parallel hand each worker its own.
   */
  public static void stringAggForWinnerGroups(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final ProjectionIndexScan.PredicateTree treeOrNull,
      final int groupColumn, final int[] stringAggColumns, final boolean[] aggIsMin, final long[] winnerHashes,
      final boolean winnerMissingKey, final String[][] bestOut, final Pattern keyRegex, final String keyRegexRepl,
      final GlobalValueDictionary.ReadView[] aggGlobalViews, final GlobalValueDictionary.ReadView groupGlobalView,
      final long @Nullable [] globalKeyHashes) {
    if (predicates == null || stringAggColumns == null || aggIsMin == null || winnerHashes == null || bestOut == null) {
      throw new IllegalArgumentException(
          "predicates, stringAggColumns, aggIsMin, winnerHashes and bestOut must not be null");
    }
    final ScanScratch s = SCRATCH.get();
    final RegexHashCache regexCache2 = keyRegex != null
        ? new RegexHashCache()
        : null;
    // Row-to-winner slot per GLOBAL id, valid across leaves (ids are resource-wide): -1 = not a
    // winner, computed once per distinct id per call.
    final Long2IntOpenHashMap gidWinnerSlot = groupGlobalView != null && globalKeyHashes == null
        ? new Long2IntOpenHashMap()
        : null;
    if (gidWinnerSlot != null) {
      gidWinnerSlot.defaultReturnValue(-2);
    }
    // With PRECOMPUTED key hashes the row-to-winner map keys by the hash instead of the gid: a
    // #winners-sized map probed per row, against a multi-million-key gid map. First-wins on a
    // (astronomically unlikely) duplicate winner hash matches the linear scan it replaces.
    final Long2IntOpenHashMap winnerSlotByHash;
    if (globalKeyHashes != null && groupGlobalView != null) {
      winnerSlotByHash = new Long2IntOpenHashMap(winnerHashes.length);
      winnerSlotByHash.defaultReturnValue(-1);
      for (int wi = 0; wi < winnerHashes.length; wi++) {
        winnerSlotByHash.putIfAbsent(winnerHashes[wi], wi);
      }
    } else {
      winnerSlotByHash = null;
    }
    final int aggCount = stringAggColumns.length;
    final int slots = winnerHashes.length + (winnerMissingKey
        ? 1
        : 0);
    final byte[][][] bestPayload = new byte[aggCount][slots][];
    final int[][] bestOff = new int[aggCount][slots];
    final int[][] bestLen = new int[aggCount][slots];
    final int[][] bestGlobalId = new int[aggCount][];
    for (int a = 0; a < aggCount; a++) {
      if (aggGlobalViews != null && aggGlobalViews[a] != null) {
        bestGlobalId[a] = new int[slots];
        Arrays.fill(bestGlobalId[a], -1);
      }
    }
    final int[][] aggEntryOff = new int[aggCount][];
    final int[] aggIdsOff = new int[aggCount];
    final int[] aggPresOff = new int[aggCount];
    final int[] aggLensOff = new int[aggCount];
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = treeOrNull != null
          ? evaluateRowGroupMaskTree(payload, treeOrNull, s)
          : evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final byte groupKind = payload[24 + groupColumn];
      final boolean globalGroup = groupGlobalView != null;
      if (globalGroup) {
        // Pass 2 under a GLOBAL regex-transformed key: row-to-winner matching hashes the
        // TRANSFORMED value per distinct ID — the identical domain pass 1 grouped on.
        if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "groupColumn " + groupColumn + " is not STRING_GLOBAL (kind=" + groupKind + ")");
        }
        if (keyRegex == null) {
          throw new IllegalStateException("a global group column reaches pass 2 only regex-transformed");
        }
      } else if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn + " is not STRING_DICT (kind=" + groupKind + ")");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int dictSize = globalGroup
          ? 0
          : getIntLE(payload, groupBase);
      if (s.dictByteOff == null || s.dictByteOff.length < dictSize) {
        s.dictByteOff = new int[Math.max(64, dictSize)];
      }
      if (s.dictSlotBase == null || s.dictSlotBase.length < dictSize) {
        s.dictSlotBase = new int[Math.max(64, dictSize)];
      }
      final int[] dictByteOff = s.dictByteOff;
      final int[] winnerSlotOfDict = s.dictSlotBase;
      final int lenHeaderOff = groupBase + 4;
      int running = lenHeaderOff + dictSize * 4;
      for (int i = 0; i < dictSize; i++) {
        dictByteOff[i] = running;
        running += getIntLE(payload, lenHeaderOff + i * 4);
        winnerSlotOfDict[i] = -2; // unresolved for THIS leaf
      }
      final int idsOff = globalGroup
          ? groupBase
          : running;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int groupPresOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;
      for (int a = 0; a < aggCount; a++) {
        final int col = stringAggColumns[a];
        final byte kind = payload[24 + col];
        if (bestGlobalId[a] != null) {
          if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
            throw new IllegalStateException("stringAggColumn " + col + " is not STRING_GLOBAL (kind=" + kind + ")");
          }
          aggIdsOff[a] = s.columnDataOff[col];
          aggPresOff[a] = tailStart >= 0
              ? presenceWordsOff(payload, tailStart, col)
              : -1;
          continue;
        }
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("stringAggColumn " + col + " is not STRING_DICT (kind=" + kind + ")");
        }
        final int base = s.columnDataOff[col];
        final int aDictSize = getIntLE(payload, base);
        aggLensOff[a] = base + 4;
        int[] offs = aggEntryOff[a];
        if (offs == null || offs.length < aDictSize) {
          aggEntryOff[a] = offs = new int[Math.max(64, aDictSize)];
        }
        int run = aggLensOff[a] + aDictSize * 4;
        for (int i = 0; i < aDictSize; i++) {
          offs[i] = run;
          run += getIntLE(payload, aggLensOff[a] + i * 4);
        }
        aggIdsOff[a] = run;
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, col)
            : -1;
      }
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        final long groupPresWord = groupPresOff >= 0
            ? getLongLE(payload, groupPresOff + w * 8)
            : -1L;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          int slot;
          if ((groupPresWord & 1L << bit) == 0L) {
            if (!winnerMissingKey) {
              continue;
            }
            slot = winnerHashes.length;
          } else if (globalGroup) {
            final long gid = getLongLE(payload, idsOff + rowIdx * 8);
            if (winnerSlotByHash != null) {
              slot = winnerSlotByHash.get(globalKeyHashes[(int) gid]);
              if (slot < 0) {
                continue;
              }
            } else {
              slot = gidWinnerSlot.get(gid);
              if (slot == -2) {
                // Same hash domain as pass 1's global arm: utf8Hash of the regex-TRANSFORMED value.
                final long h;
                if (globalKeyHashes != null) {
                  h = globalKeyHashes[(int) gid]; // precomputed sweep — identical hash domain
                } else {
                  final String transformed =
                      keyRegex.matcher(groupGlobalView.valueAsString((int) gid)).replaceAll(keyRegexRepl);
                  h = utf8Hash(transformed.getBytes(StandardCharsets.UTF_8));
                }
                slot = -1;
                for (int wi = 0; wi < winnerHashes.length; wi++) {
                  if (winnerHashes[wi] == h) {
                    slot = wi;
                    break;
                  }
                }
                gidWinnerSlot.put(gid, slot);
              }
              if (slot < 0) {
                continue;
              }
            }
          } else {
            final int dictId = getIntLE(payload, idsOff + rowIdx * 4);
            slot = winnerSlotOfDict[dictId];
            if (slot == -2) {
              final long h;
              if (keyRegex != null) {
                h = transformedKeyHash(regexCache2, keyRegex, keyRegexRepl, payload, dictByteOff[dictId],
                    getIntLE(payload, lenHeaderOff + dictId * 4));
              } else {
                h = fnv1a64(payload, dictByteOff[dictId], getIntLE(payload, lenHeaderOff + dictId * 4));
              }
              slot = -1;
              for (int wi = 0; wi < winnerHashes.length; wi++) {
                if (winnerHashes[wi] == h) {
                  slot = wi;
                  break;
                }
              }
              winnerSlotOfDict[dictId] = slot;
            }
            if (slot < 0) {
              continue;
            }
          }
          for (int a = 0; a < aggCount; a++) {
            if (aggPresOff[a] >= 0
                && (getLongLE(payload, aggPresOff[a] + (rowIdx >>> 6) * 8) & 1L << (rowIdx & 63)) == 0L) {
              continue; // operand missing on this row — contributes nothing
            }
            final int[] globalBest = bestGlobalId[a];
            if (globalBest != null) {
              final long gid = getLongLE(payload, aggIdsOff[a] + rowIdx * 8);
              if (gid < 1 || gid > Integer.MAX_VALUE) {
                continue; // no valid id on this row — nothing to fold
              }
              final int candidate = (int) gid;
              final int incumbent = globalBest[slot];
              if (incumbent == candidate) {
                continue;
              }
              if (incumbent < 0) {
                globalBest[slot] = candidate;
                continue;
              }
              final int cmp = aggGlobalViews[a].compareIds(candidate, incumbent);
              if (aggIsMin[a]
                  ? cmp < 0
                  : cmp > 0) {
                globalBest[slot] = candidate;
              }
              continue;
            }
            final int id = getIntLE(payload, aggIdsOff[a] + rowIdx * 4);
            final int off = aggEntryOff[a][id];
            final int len = getIntLE(payload, aggLensOff[a] + id * 4);
            final byte[] cur = bestPayload[a][slot];
            if (cur == null) {
              bestPayload[a][slot] = payload;
              bestOff[a][slot] = off;
              bestLen[a][slot] = len;
              continue;
            }
            if (cur == payload && bestOff[a][slot] == off) {
              continue; // the group's best IS this dict entry
            }
            final int cmp = compareStrSlices(payload, off, len, cur, bestOff[a][slot], bestLen[a][slot]);
            if (aggIsMin[a]
                ? cmp < 0
                : cmp > 0) {
              bestPayload[a][slot] = payload;
              bestOff[a][slot] = off;
              bestLen[a][slot] = len;
            }
          }
        }
      }
    }
    for (int a = 0; a < aggCount; a++) {
      final int[] globalBest = bestGlobalId[a];
      for (int sl = 0; sl < slots; sl++) {
        if (globalBest != null) {
          bestOut[a][sl] = globalBest[sl] < 0
              ? null
              : aggGlobalViews[a].valueAsString(globalBest[sl]);
        } else {
          bestOut[a][sl] = bestPayload[a][sl] == null
              ? null
              : new String(bestPayload[a][sl], bestOff[a][sl], bestLen[a][sl], StandardCharsets.UTF_8);
        }
      }
    }
  }

  /**
   * FNV-64 of arbitrary UTF-8 bytes in the SAME domain the flat string kernels key on — for
   * rebuilding a REGEX-transformed winner's hash from its materialized value.
   */
  public static long utf8Hash(final byte[] utf8) {
    return fnv1a64(utf8, 0, utf8.length);
  }

  static void validateStringLengthModes(final byte[] modes, final int aggregateCount) {
    if (modes == null) {
      return;
    }
    if (modes.length < aggregateCount) {
      throw new IllegalArgumentException(
          "stringLengthModes has " + modes.length + " entries for " + aggregateCount + " aggregates");
    }
    for (int i = 0; i < aggregateCount; i++) {
      final byte mode = modes[i];
      if (mode != STRING_LENGTH_NONE && mode != STRING_LENGTH_CODE_POINTS && mode != STRING_LENGTH_UTF8_BYTES) {
        throw new IllegalArgumentException("unknown string length mode " + mode + " at aggregate " + i);
      }
    }
  }

  /**
   * FNV-64 of ONE dict entry's bytes — the flat string kernels' group identity, exposed so the
   * executor can rebuild a winner's hash from its {@code (leaf, dictId)} source reference.
   */
  public static long dictEntryHash(final byte[] payload, final int columnBase, final int dictId) {
    final int dictSize = getIntLE(payload, columnBase);
    final int lenHeaderOff = columnBase + 4;
    int off = lenHeaderOff + dictSize * 4;
    for (int i = 0; i < dictId; i++) {
      off += getIntLE(payload, lenHeaderOff + i * 4);
    }
    return fnv1a64(payload, off, getIntLE(payload, lenHeaderOff + dictId * 4));
  }

  /**
   * Slice comparison under the interpreter's collation ({@code Str#cmp} = UTF-16 code units):
   * unsigned bytes unless either side carries a 4-byte UTF-8 lead, then decoded compareTo.
   */
  static int compareStrSlices(final byte[] a, final int aOff, final int aLen, final byte[] b, final int bOff,
      final int bLen) {
    if (ProjectionIndexScan.hasFourByteUtf8(a, aOff, aLen) || ProjectionIndexScan.hasFourByteUtf8(b, bOff, bLen)) {
      return new String(a, aOff, aLen, StandardCharsets.UTF_8).compareTo(
          new String(b, bOff, bLen, StandardCharsets.UTF_8));
    }
    return Arrays.compareUnsigned(a, aOff, aOff + aLen, b, bOff, bOff + bLen);
  }

  /**
   * Order-preserving digit pack of an ISO-minute substring window: the {@code len == 16} window
   * {@code dddd-dd-ddTdd:dd} packs its 12 digits as {@code yyyyMMddHHmm + 1} (the bias keeps the
   * degenerate all-zero string off the table's zero sentinel). Lexicographic order over the validated
   * set equals numeric order over the packs — the property that lets ORD_KEY serve {@code order by}
   * on the substring. {@code Long.MIN_VALUE} = the window fails the shape (including the {@code ""} a
   * MISSING field produces, a REAL group key the interpreter emits) — ONE such entry referenced by a
   * row corrupts the packed order, so the caller declines.
   */
  static long packIsoMinuteSubstring(final byte[] bytes, final int off, final int len, final int start,
      final int subLen) {
    if (subLen != 16) {
      return Long.MIN_VALUE;
    }
    // Same 1-based guard as the integer cast: start < 1 would put the 16-byte window before the
    // value. The comparison is in long arithmetic so an extreme start cannot wrap into a window that
    // looks in range.
    if (start < 1) {
      return Long.MIN_VALUE;
    }
    final long s0 = (long) start - 1L;
    if (s0 + 16L > len) {
      return Long.MIN_VALUE; // fn:substring clamps to a SHORTER window — not the ISO shape
    }
    final int b = off + (int) s0;
    if (bytes[b + 4] != '-' || bytes[b + 7] != '-' || bytes[b + 10] != 'T' || bytes[b + 13] != ':') {
      return Long.MIN_VALUE;
    }
    long v = 0;
    for (final int i : ISO_MINUTE_DIGITS) {
      final int d = bytes[b + i] - '0';
      if (d < 0 || d > 9) {
        return Long.MIN_VALUE;
      }
      v = v * 10 + d;
    }
    return v + 1;
  }

  private static final int[] ISO_MINUTE_DIGITS = {0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15};

  /**
   * Single STRING_DICT group key transformed by a bare {@code substring(f, s, 16)} over ISO
   * timestamps: groups AND orders on {@link #packIsoMinuteSubstring}'s long (an ORD_KEY the heap
   * compares directly), with the aux lane carrying {@code (leaf << 20) | dictId} so the K winners
   * emit the ORIGINAL substring bytes. Structure mirrors the numeric flat kernel; a single unpackable
   * entry REFERENCED BY A MATCHING ROW sets {@code declineFlag} and aborts.
   */
  public static void conjunctiveAggregateByGroupPackedSubstringFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int subStart,
      final int subLen, final int[] aggColumns, final NumericGroupAggTable out, final int leafIndexBase,
      final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull) {
    conjunctiveAggregateByGroupPackedSubstringFlat(rowGroupPayloads, predicates, groupColumn, subStart, subLen,
        aggColumns, out, leafIndexBase, declineFlag, treeOrNull, null);
  }

  /** Global-string capable packed-substring overload; a non-null view resolves global-id cells. */
  public static void conjunctiveAggregateByGroupPackedSubstringFlat(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int groupColumn, final int subStart,
      final int subLen, final int[] aggColumns, final NumericGroupAggTable out, final int leafIndexBase,
      final long[] declineFlag, final ProjectionIndexScan.PredicateTree treeOrNull,
      final GlobalValueDictionary.ReadView globalDictionaryView) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    if (out == null || aggColumns == null || declineFlag == null) {
      throw new IllegalArgumentException("out, aggColumns and declineFlag must not be null");
    }
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query that
    // asks only for min/max/count can never decline on an overflow no answer depends on —
    // see NumericGroupAggTable#sumsExact for the rule the partition merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final ScanScratch s = SCRATCH.get();
    if (s.dictHashCache == null) {
      s.dictHashCache = new long[64];
      s.dictSlotBase = new int[64];
    }
    long[] dictPacked = s.dictHashCache;
    final int aggCount = aggColumns.length;
    // COUNT-ONLY: no aggregate lanes, so the fold is one increment into the group's stripe.
    final boolean countOnly = aggCount == 0;
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      if (declineFlag[0] != 0) {
        return;
      }
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = treeOrNull != null
          ? evaluateRowGroupMaskTree(payload, treeOrNull, s)
          : evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0) {
        continue;
      }
      final byte groupKind = payload[24 + groupColumn];
      final boolean global = groupKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
      if (groupKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT && !global) {
        throw new IllegalStateException(
            "groupColumn " + groupColumn + " is not STRING_DICT or STRING_GLOBAL (kind=" + groupKind + ")");
      }
      if (global && globalDictionaryView == null) {
        throw new IllegalStateException("STRING_GLOBAL packed substring needs a dictionary read view");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int idsOff;
      if (global) {
        idsOff = groupBase;
      } else {
        final int dictSize = getIntLE(payload, groupBase);
        if (dictPacked.length < dictSize) {
          dictPacked = s.dictHashCache = new long[Math.max(dictPacked.length * 2, dictSize)];
        }
        final int lenHeaderOff = groupBase + 4;
        int off = lenHeaderOff + dictSize * 4;
        for (int i = 0; i < dictSize; i++) {
          final int len = getIntLE(payload, lenHeaderOff + i * 4);
          dictPacked[i] = packIsoMinuteSubstring(payload, off, len, subStart, subLen);
          off += len;
        }
        idsOff = off;
      }
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      final int groupPresOff = tailStart >= 0
          ? presenceWordsOff(payload, tailStart, groupColumn)
          : -1;
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggCount
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggCount)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggCount
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggCount)]);
      for (int a = 0; a < aggCount; a++) {
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
        aggValOff[a] = s.columnDataOff[aggColumns[a]];
      }
      final long leafOrdinalBase = (long) (leafIndexBase + leaf) << 20;
      final int stride = rowCount + 63 >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w] & validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresOff >= 0
            ? getLongLE(payload, groupPresOff + (w << 3))
            : -1L;
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (groupPresOff >= 0 && (groupPresWord & 1L << bit) == 0L) {
            // substring((), s, l) is "" — a REAL group key that fails the ISO shape: decline,
            // never the null-key group (the interpreter emits "" here, not JSON null).
            declineFlag[0] = 1;
            return;
          }
          final int dictId = global
              ? Math.toIntExact(getLongLE(payload, idsOff + rowIdx * 8))
              : getIntLE(payload, idsOff + rowIdx * 4);
          final long packed = global
              ? globalDictionaryView.packIsoMinuteSubstring(dictId, subStart, subLen)
              : dictPacked[dictId];
          if (packed == Long.MIN_VALUE) {
            declineFlag[0] = 1;
            return;
          }
          final int handle = out.acquire(packed, leafOrdinalBase | rowIdx);
          final long[] slotArr = out.storageAtAccBase(handle);
          final int base = out.offsetAtAccBase(handle);
          if (slotArr[base] == 0L) {
            out.setAuxAtAccBase(handle, global
                ? dictId
                : leafOrdinalBase | dictId);
          }
          if (countOnly) {
            slotArr[base]++;
          } else {
            foldRow(slotArr, base, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, sumExactMask);
          }
        }
      }
    }
  }

  /** Shared per-row accumulate for the flat group kernels. */
  private static void foldRow(final long[] slotArr, final int base, final byte[] payload, final int[] aggPresOff,
      final int[] aggValOff, final int aggCount, final int w, final int bit, final int rowIdx,
      final long sumExactMask) {
    foldRow(slotArr, base, payload, aggPresOff, aggValOff, aggCount, w, bit, rowIdx, null, null, null, sumExactMask);
  }

  /**
   * {@link #foldRow} with string-length operand modes: a transformed string aggregate folds the
   * entry's precomputed codepoint or UTF-8-byte count, and a missing operand folds zero.
   */
  private static void foldRow(final long[] slotArr, final int base, final byte[] payload, final int[] aggPresOff,
      final int[] aggValOff, final int aggCount, final int w, final int bit, final int rowIdx,
      final byte[] stringLengthModes, final int[][] stringLengths, final int[][] globalLengthTables,
      final long sumExactMask) {
    slotArr[base]++;
    for (int a = 0; a < aggCount; a++) {
      final boolean stringLengthAgg = stringLengthModes != null && stringLengthModes[a] != STRING_LENGTH_NONE;
      final boolean present = aggPresOff[a] < 0 || (getLongLE(payload, aggPresOff[a] + (w << 3)) & 1L << bit) != 0L;
      if (!present && !stringLengthAgg) {
        continue;
      }
      // A GLOBAL length operand reads its per-query id table over the 8-byte id lane; a per-leaf
      // dict operand reads its per-leaf entry table over the 4-byte id lane. Missing folds 0 in
      // both — fn:string-length(()) is 0, never empty.
      final long v = stringLengthAgg
          ? (present
              ? (globalLengthTables != null && globalLengthTables[a] != null
                  ? globalLengthTables[a][(int) getLongLE(payload, aggValOff[a] + rowIdx * 8)]
                  : stringLengths[a][getIntLE(payload, aggValOff[a] + rowIdx * 4)])
              : 0L)
          : getLongLE(payload, aggValOff[a] + rowIdx * 8);
      final int aggBase = base + 2 + 4 * a;
      slotArr[aggBase]++;
      // Exact sum or DECLINE — but only for a lane some sum/avg reads; an unread lane is not
      // folded, so a min-only query cannot decline on an overflow its answer never touches.
      if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
        slotArr[aggBase + 1] = Math.addExact(slotArr[aggBase + 1], v);
      }
      if (v < slotArr[aggBase + 2]) {
        slotArr[aggBase + 2] = v;
      }
      if (v > slotArr[aggBase + 3]) {
        slotArr[aggBase + 3] = v;
      }
    }
  }

  /** {@link #foldRowDistinct} without string operand modes — every agg block is NUMERIC_LONG. */
  private static void foldRowDistinct(final long[] slotArr, final int base, final byte[] payload,
      final int[] aggPresOff, final int[] aggValOff, final int aggCount, final int distinctBlock,
      final GroupDistinctAccumulator.Sink dset, final long[] budget, final int w, final int bit, final int rowIdx,
      final long sumExactMask) {
    foldRowDistinct(slotArr, base, payload, aggPresOff, aggValOff, aggCount, distinctBlock, dset, budget, w, bit,
        rowIdx, null, null, null, null, null, null, sumExactMask);
  }

  /**
   * {@link #foldRow} with block {@code distinctBlock} feeding the group's distinct SET instead of the
   * fold lanes; its lanes stay zero until the caller writes the merged set size.
   *
   * <p>
   * {@code cdHash != null} marks the distinct operand as STRING_DICT: dict ids are LEAF-LOCAL, so the
   * set member is the entry's 64-bit content hash, computed once per referenced entry per leaf
   * ({@code 0} = not yet hashed). Identity is therefore exact up to a 64-bit hash collision — the
   * SAME standard the composite group-key identity already accepts.
   */
  private static void foldRowDistinct(final long[] slotArr, final int base, final byte[] payload,
      final int[] aggPresOff, final int[] aggValOff, final int aggCount, final int distinctBlock,
      final GroupDistinctAccumulator.Sink dset, final long[] budget, final int w, final int bit, final int rowIdx,
      final byte[] stringLengthModes, final int[][] stringLengths, final int[][] globalLengthTables, final int[] cdOff,
      final int[] cdLen, final long[] cdHash, final long sumExactMask) {
    slotArr[base]++;
    for (int a = 0; a < aggCount; a++) {
      final boolean stringLengthAgg = stringLengthModes != null && stringLengthModes[a] != STRING_LENGTH_NONE;
      final boolean present = aggPresOff[a] < 0 || (getLongLE(payload, aggPresOff[a] + (w << 3)) & 1L << bit) != 0L;
      if (!present && !stringLengthAgg) {
        continue;
      }
      final long v;
      if (stringLengthAgg) {
        // Both supported length transforms map the empty sequence to zero; see foldRow for the
        // global-vs-per-leaf table split.
        v = present
            ? (globalLengthTables != null && globalLengthTables[a] != null
                ? globalLengthTables[a][(int) getLongLE(payload, aggValOff[a] + rowIdx * 8)]
                : stringLengths[a][getIntLE(payload, aggValOff[a] + rowIdx * 4)])
            : 0L;
      } else if (cdHash != null && a == distinctBlock) {
        final int cdId = getIntLE(payload, aggValOff[a] + rowIdx * 4);
        long h = cdHash[cdId];
        if (h == 0L) {
          h = fnv1a64(payload, cdOff[cdId], cdLen[cdId]);
          cdHash[cdId] = h;
        }
        v = h;
      } else {
        v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
      }
      if (a == distinctBlock) {
        dset.add(v); // exact and bounded inside the shared accumulator; its overrun declines the arm
        continue;
      }
      final int aggBase = base + 2 + 4 * a;
      slotArr[aggBase]++;
      // Exact sum or DECLINE — but only for a lane some sum/avg reads; an unread lane is not
      // folded, so a min-only query cannot decline on an overflow its answer never touches.
      if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
        slotArr[aggBase + 1] = Math.addExact(slotArr[aggBase + 1], v);
      }
      if (v < slotArr[aggBase + 2]) {
        slotArr[aggBase + 2] = v;
      }
      if (v > slotArr[aggBase + 3]) {
        slotArr[aggBase + 3] = v;
      }
    }
  }

  /**
   * Per-leaf memo for a COUNT(DISTINCT) operand over a STRING_DICT column: each dict entry's payload
   * byte range, hashes reset to the "unhashed" sentinel, and the returned offset is the column's IDS
   * region (what {@code aggValOff} must carry for that block). The hashes themselves are computed
   * lazily on first reference in {@link #foldRowDistinct} — a selective predicate must not pay for
   * entries no matching row names. Grows the scratch arrays, so re-read them after.
   */
  private static int prepareCdDict(final ScanScratch s, final byte[] payload, final int columnBase) {
    final int dictSize = getIntLE(payload, columnBase);
    if (s.cdDictOff == null || s.cdDictOff.length < dictSize) {
      final int n = Math.max(64, dictSize);
      s.cdDictOff = new int[n];
      s.cdDictLen = new int[n];
      s.cdDictHash = new long[n];
    }
    final int[] offs = s.cdDictOff;
    final int[] lens = s.cdDictLen;
    final long[] hashes = s.cdDictHash;
    final int lenHeaderOff = columnBase + 4;
    int off = lenHeaderOff + dictSize * 4;
    for (int i = 0; i < dictSize; i++) {
      final int len = getIntLE(payload, lenHeaderOff + i * 4);
      offs[i] = off;
      lens[i] = len;
      hashes[i] = 0L;
      off += len;
    }
    return off;
  }

  /** The group's distinct set, created on first sight. */

  /**
   * Data-stream base of a STRING_DICT column, walking the header of one leaf payload — the
   * winner-materialization companion of {@link #conjunctiveAggregateByGroupStringFlat} (cache the
   * result per leaf; the walk prices every column before {@code column}).
   */
  public static int stringDictColumnBase(final byte[] payload, final int column) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;
      if (c == column) {
        if (payload[kindsOff + c] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("column " + column + " is not STRING_DICT");
        }
        return cursor;
      }
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += (rowCount + 63 >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
        }
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
    }
    throw new IllegalStateException("column " + column + " out of range");
  }

  /**
   * Dict entry {@code dictId} of the STRING_DICT column whose data stream starts at
   * {@code columnBase} (from {@link #stringDictColumnBase}), decoded as a String.
   */
  public static String dictEntryString(final byte[] payload, final int columnBase, final int dictId) {
    final int dictSize = getIntLE(payload, columnBase);
    if (dictId < 0 || dictId >= dictSize) {
      throw new IllegalStateException("dictId " + dictId + " out of range 0.." + (dictSize - 1));
    }
    final int lenHeaderOff = columnBase + 4;
    int off = lenHeaderOff + dictSize * 4;
    for (int i = 0; i < dictId; i++) {
      off += getIntLE(payload, lenHeaderOff + i * 4);
    }
    final int len = getIntLE(payload, lenHeaderOff + dictId * 4);
    return new String(payload, off, len, StandardCharsets.UTF_8);
  }

  /**
   * Maximum group columns for {@link #conjunctiveAggregateByGroupMulti}: composite keys pack one
   * 11-bit component per column ({@link ProjectionIndexRowGroupPage#MAX_ROWS} caps dict ids at 1024,
   * sentinel {@link #GROUP_ID_MISSING} = 2047) into one long — 5 × 11 = 55 bits. Callers must decline
   * shapes with more keys.
   */
  public static final int MAX_GROUP_COLUMNS = 5;

  /** Packed-component sentinel for "group field missing on this row" (11-bit max). */
  private static final int GROUP_ID_MISSING = 0x7FF;

  /**
   * Immutable composite group key for multi-key group-by: the per-key string values in record-entry
   * order, {@code null} components marking rows where that group field is MISSING (the interpreter
   * groups them under the empty-sequence key). Hash is precomputed — the key is a hash-map key on the
   * hot merge path.
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
      return o instanceof GroupKey other && hash == other.hash && Arrays.equals(parts, other.parts);
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
   * max]} per aggregate column into the accumulator of their COMPOSITE group key — the tuple of the
   * row's group-column string values, with {@code null} components for missing cells (so the
   * all-missing row lands in the all-null group, exactly like the interpreter's grouping over empty
   * let-bound keys). Accumulator layout and first-seen ordinals: {@link #groupAggSlots} /
   * {@link #newGroupAggAcc}.
   *
   * <p>
   * Hot-path shape: per leaf, every group column's dictionary is decoded ONCE into a
   * {@code String[]}; per row the group ids pack into one long (11 bits per component) and resolve
   * through a per-leaf combo cache, so the steady-state row cost is one packed-long hash probe — no
   * string work, no allocation.
   */
  public static void conjunctiveAggregateByGroupMulti(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] groupColumns, final int[] aggColumns,
      final Object2ObjectOpenHashMap<GroupKey, long[]> out, final int leafIndexBase) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final int keyCount = groupColumns.length;
    if (keyCount < 2 || keyCount > MAX_GROUP_COLUMNS) {
      throw new IllegalArgumentException(
          "groupColumns must have 2.." + MAX_GROUP_COLUMNS + " entries, got " + keyCount);
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
    for (int leaf = 0; leaf < rowGroupPayloads.size(); leaf++) {
      final byte[] payload = rowGroupPayloads.get(leaf);
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      for (int g = 0; g < keyCount; g++) {
        final int col = groupColumns[g];
        if (col < 0 || col >= columnCount) {
          throw new IllegalStateException("groupColumn " + col + " out of range [0, " + columnCount + ")");
        }
        final byte kind = payload[24 + col];
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          throw new IllegalStateException("groupColumn " + col + " is not STRING_DICT (kind=" + kind + ")");
        }
        final int base = s.columnDataOff[col];
        final int dictSize = getIntLE(payload, base);
        if (dictSize < 0 || dictSize >= GROUP_ID_MISSING) {
          // The 11-bit lane must hold every dict id PLUS the missing sentinel. Today
          // dictSize <= MAX_ROWS = 1024 < 2047; a corrupt header (or a future MAX_ROWS
          // bump past the lane width) must fail loud, never alias groups silently.
          throw new IllegalStateException(
              "group dict size " + dictSize + " exceeds the packed-key lane (max " + (GROUP_ID_MISSING - 1) + ")");
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
        groupPresOff[g] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, col)
            : -1;
      }
      final int[] aggPresOff = s.groupAggPresOff != null && s.groupAggPresOff.length >= aggColumns.length
          ? s.groupAggPresOff
          : (s.groupAggPresOff = new int[Math.max(4, aggColumns.length)]);
      final int[] aggValOff = s.groupAggValOff != null && s.groupAggValOff.length >= aggColumns.length
          ? s.groupAggValOff
          : (s.groupAggValOff = new int[Math.max(4, aggColumns.length)]);
      for (int a = 0; a < aggColumns.length; a++) {
        if (aggColumns[a] < 0 || aggColumns[a] >= columnCount) {
          throw new IllegalStateException("aggColumn " + aggColumns[a] + " out of range [0, " + columnCount + ")");
        }
        final byte aggKind = payload[24 + aggColumns[a]];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(aggKind)
            && aggKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          throw new IllegalStateException(
              "aggColumn " + aggColumns[a] + " is not NUMERIC_LONG, temporal or STRING_GLOBAL (kind=" + aggKind + ")");
        }
        aggPresOff[a] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, aggColumns[a])
            : -1;
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
          if (rowIdx >= rowCount)
            break;
          long packed = 0L;
          for (int g = 0; g < keyCount; g++) {
            final int comp;
            if (groupPresOff[g] >= 0 && (getLongLE(payload, groupPresOff[g] + w * 8) & (1L << bit)) == 0L) {
              comp = GROUP_ID_MISSING;
            } else {
              comp = getIntLE(payload, groupIdsOff[g] + rowIdx * 4);
              if (comp < 0 || comp >= dictSizes[g]) {
                // A corrupt id must fail loud here: 2047 would alias the MISSING
                // sentinel and >= 2048 would bleed into the adjacent key's lane —
                // both silently wrong groups, which fail-soft callers would never see.
                throw new IllegalStateException("group dict id " + comp + " out of range [0, " + dictSizes[g] + ")");
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
              keyScratch[g] = comp == GROUP_ID_MISSING
                  ? null
                  : dicts[g][comp];
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
            if (aggPresOff[a] >= 0 && (getLongLE(payload, aggPresOff[a] + w * 8) & (1L << bit)) == 0L) {
              continue;
            }
            final long v = getLongLE(payload, aggValOff[a] + rowIdx * 8);
            final int base = 2 + 4 * a;
            acc[base]++;
            // Exact sum or DECLINE: the interpreter promotes an overflowing xs:integer
            // sum to exact decimal — a wrapped long would silently serve a wrong total.
            acc[base + 1] = Math.addExact(acc[base + 1], v);
            if (v < acc[base + 2])
              acc[base + 2] = v;
            if (v > acc[base + 3])
              acc[base + 3] = v;
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
   * COMPUTED-EXPRESSION aggregate fold (gap item 2): for every predicate-matching row on which ALL
   * operand columns are PRESENT, evaluate the postfix {@code code} program over the row's
   * NUMERIC_LONG operand values ({@code slot < COMPUTED_CONST_BASE} pushes
   * {@code operandColumns[slot]}'s value, {@code slot >= COMPUTED_CONST_BASE} pushes
   * {@code consts[slot - COMPUTED_CONST_BASE]}, negative slots apply ADD/SUB/MUL popping two
   * operands) and fold {@code [count, sum, min, max]} into {@code acc}. Rows missing ANY operand
   * contribute nothing — the interpreter's arithmetic over the empty sequence is empty, so those rows
   * add no item.
   *
   * <p>
   * ALL arithmetic (program ops AND the running sum) is {@code Math.*Exact}: an overflow throws
   * {@link ArithmeticException}, which callers treat as a DECLINE — the interpreter promotes
   * overflowing integer math to exact decimal, so only the generic pipeline can answer those
   * digit-exactly.
   *
   * <p>
   * {@code acc} layout {@code [count, sum, min, max]} with min/max seeded to
   * {@link Long#MAX_VALUE}/{@link Long#MIN_VALUE}. The program is caller-validated (balanced, depth ≤
   * {@code stack.length}); this kernel re-checks only bounds that protect memory safety.
   */
  public static void conjunctiveAggregateComputed(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] operandColumns, final int[] code,
      final long[] consts, final long[] acc) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      for (int c = 0; c < nOps; c++) {
        if (operandColumns[c] < 0 || operandColumns[c] >= columnCount) {
          throw new IllegalStateException(
              "operand column " + operandColumns[c] + " out of range [0, " + columnCount + ")");
        }
        final byte kind = payload[24 + operandColumns[c]];
        if (kind != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
          throw new IllegalStateException(
              "operand column " + operandColumns[c] + " is not NUMERIC_LONG (kind=" + kind + ")");
        }
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      for (int c = 0; c < nOps; c++) {
        valOff[c] = s.columnDataOff[operandColumns[c]];
        presOff[c] = tailStart >= 0
            ? presenceWordsOff(payload, tailStart, operandColumns[c])
            : -1;
      }
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount)
            break;
          boolean anyMissing = false;
          for (int c = 0; c < nOps; c++) {
            if (presOff[c] >= 0 && (getLongLE(payload, presOff[c] + w * 8) & (1L << bit)) == 0L) {
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
          if (v < acc[2])
            acc[2] = v;
          if (v > acc[3])
            acc[3] = v;
        }
      }
    }
  }

  /**
   * Collect {@code (sortValue[0..k-1], recordKey)} TUPLES for every predicate-matching row on which
   * ALL {@code k} NUMERIC_LONG sort columns are PRESENT (P5b stage 7b sorted-scan serving; gap item
   * 1b generalized the single column to {@code k}). Values append ROW-MAJOR into {@code valuesOut}
   * (stride {@code sortColumns.length}). Rows missing ANY sort column are counted into
   * {@code missingKeysOut} instead — the interpreter sorts empty order keys per the
   * empty-least/greatest mode (no error), a placement this long-tuple collector cannot represent, so
   * callers decline when it is non-empty. Outputs append in DOCUMENT order, so a stable by-tuple sort
   * of the rows reproduces the interpreter's stable {@code order by}.
   */
  public static void collectMatchingSortTuples(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] sortColumns, final LongArrayList valuesOut,
      final LongArrayList keysOut, final LongArrayList missingKeysOut) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      for (int k = 0; k < keyCount; k++) {
        final int sortColumn = sortColumns[k];
        if (sortColumn < 0 || sortColumn >= columnCount) {
          throw new IllegalStateException("sortColumn " + sortColumn + " out of range [0, " + columnCount + ")");
        }
        final byte sortKind = payload[24 + sortColumn];
        if (!ProjectionIndexRowGroupPage.isOrderedLongKind(sortKind)) {
          throw new IllegalStateException(
              "sortColumn " + sortColumn + " is not NUMERIC_LONG or temporal (kind=" + sortKind + ")");
        }
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int recordKeysOff = 24 + columnCount;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart < 0) {
        // Fail closed: without a valid presence tail, missing cells would sort as their
        // phantom stored defaults. Executor gates (columnSparseClean) make this
        // unreachable; this guard keeps the PUBLIC method safe for any future caller.
        throw new IllegalStateException(
            "leaf without a valid presence tail — sorted " + "collection requires presence truth");
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
          presAll &= presOff[k] >= 0
              ? getLongLE(payload, presOff[k] + w * 8)
              : -1L;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount)
            break;
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
   * (thread-confined flyweights) — consumers must copy what they keep. {@code present[i]} false ⇒ the
   * field is missing on this row ({@code longVals}/{@code stringVals} slots are then meaningless);
   * numeric/boolean columns fill {@code longVals[i]} (booleans as 0/1, doubles in transform domain),
   * STRING_DICT columns fill {@code stringVals[i]}.
   */
  @FunctionalInterface
  public interface RowSink {
    void row(long[] longVals, String[] stringVals, boolean[] present);
  }

  /**
   * COVERED-ROW materialization (P5b stage 7c): stream every predicate-matching row's requested
   * column values straight from the raw leaves, in document order — no document -store touch. Column
   * kinds are validated per leaf (fail loud on drift); presence is per-column truth (fail closed on a
   * missing tail). Dict interning mirrors {@link #conjunctiveCountByGroup}.
   */
  public static void materializeMatchingRows(final List<byte[]> rowGroupPayloads,
      final ProjectionIndexScan.ColumnPredicate[] predicates, final int[] cols, final byte[] expectedKinds,
      final RowSink sink) {
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
    for (final byte[] payload : rowGroupPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateRowGroupMask(payload, predicates, s);
      if (rowCount <= 0)
        continue;
      final int tailStart = presenceTailStart(payload, s.leafDataEnd);
      if (tailStart < 0) {
        throw new IllegalStateException(
            "leaf without a valid presence tail — covered-row " + "materialization requires presence truth");
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
          throw new IllegalStateException(
              "column " + col + " kind drift: leaf says " + kinds[c] + ", handle says " + expectedKinds[c]);
        }
        valOff[c] = s.columnDataOff[col];
        presOff[c] = presenceWordsOff(payload, tailStart, col);
        if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
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
            decoded[i] =
                new String(payload, offs[i], getIntLE(payload, dictLenHeaderOff[c] + i * 4), StandardCharsets.UTF_8);
          }
          dictStrings[c] = decoded;
          dictOffs[c] = offs;
          dictIdsOff[c] = running;
        } else if (!ProjectionIndexRowGroupPage.isNumericKind(kinds[c])
            && !ProjectionIndexRowGroupPage.isTemporalKind(kinds[c])
            && kinds[c] != ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
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
          if (rowIdx >= rowCount)
            break;
          for (int c = 0; c < nCols; c++) {
            final boolean p = (getLongLE(payload, presOff[c] + (rowIdx >>> 6) * 8) & (1L << (rowIdx & 63))) != 0L;
            present[c] = p;
            if (!p)
              continue;
            switch (kinds[c]) {
              case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
                final int dictId = getIntLE(payload, dictIdsOff[c] + rowIdx * 4);
                stringVals[c] = dictStrings[c][dictId];
              }
              case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> {
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

  /** End of KEYS in the raw V0 leaf; NONE is the allocation-free one-byte common case. */
  private static int columnStreamStart(final byte[] payload, final int rowCount, final int columnCount) {
    final long markerOffsetLong = 24L + columnCount + (long) rowCount * Long.BYTES;
    if (rowCount < 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS || columnCount < 0 || markerOffsetLong < 0
        || markerOffsetLong >= payload.length) {
      throw new IllegalStateException("truncated projection KEYS stream");
    }
    final int markerOffset = (int) markerOffsetLong;
    final byte kind = payload[markerOffset];
    final long orderLabelsOffset;
    if (kind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE) {
      orderLabelsOffset = markerOffsetLong + 1L;
    } else if (kind == ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE && rowCount > 0) {
      orderLabelsOffset = markerOffsetLong + 1L + ((rowCount + 63L) >>> 6) * Long.BYTES;
    } else {
      throw new IllegalStateException("unknown projection order-exception kind " + kind);
    }
    final long offsetsEnd = orderLabelsOffset + Integer.BYTES + Math.multiplyExact((long) rowCount + 1L, Integer.BYTES);
    if (offsetsEnd > payload.length) {
      throw new IllegalStateException("truncated projection Dewey order-label metadata");
    }
    final int labelByteLength = getIntLE(payload, (int) orderLabelsOffset);
    if (labelByteLength < 0 || labelByteLength > ProjectionIndexRowGroupPage.MAX_ORDER_LABEL_BYTES
        || offsetsEnd + labelByteLength > payload.length) {
      throw new IllegalStateException("invalid projection Dewey order-label byte length " + labelByteLength);
    }
    final int firstOffset = getIntLE(payload, (int) orderLabelsOffset + Integer.BYTES);
    final int finalOffset =
        getIntLE(payload, (int) orderLabelsOffset + Integer.BYTES + Math.multiplyExact(rowCount, Integer.BYTES));
    if (firstOffset != 0 || finalOffset != labelByteLength) {
      throw new IllegalStateException("invalid projection Dewey order-label offsets");
    }
    return Math.toIntExact(offsetsEnd + labelByteLength);
  }

  // ------------------------------------------------------------------
  // Presence tail (sparse-field correctness). Layout appended AFTER the
  // column stream — see ProjectionIndexRowGroupPage's class javadoc:
  // byte[columnCount] columnFlags (bit0 = unrepresentable seen,
  // bit1 = non-integral seen)
  // long[presWords] presence per column (only when rowCount > 0)
  // int tailLen; byte version; int magic = "PIX1"
  // ------------------------------------------------------------------

  /**
   * Start offset of the presence tail given the EXACT end of the column data stream, or {@code -1}
   * when the trailing bytes don't form a valid tail (malformed payload). The boundary equality check
   * makes false positives impossible — a truncated payload can never be misread.
   */
  static int presenceTailStart(final byte[] payload, final int dataEnd) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int presWords = rowCount > 0
        ? (rowCount + 63) >>> 6
        : 0;
    final int tailLen = columnCount + columnCount * presWords * 8;
    if (payload.length != dataEnd + tailLen + 9)
      return -1;
    if (getIntLE(payload, payload.length - 4) != ProjectionIndexRowGroupPage.PRESENCE_TAIL_MAGIC)
      return -1;
    if (payload[payload.length - 5] != ProjectionIndexRowGroupPage.PRESENCE_TAIL_VERSION)
      return -1;
    if (getIntLE(payload, payload.length - 9) != tailLen)
      return -1;
    return dataEnd;
  }

  /**
   * Byte offset of {@code column}'s presence words inside a leaf whose tail starts at
   * {@code tailStart}.
   */
  private static int presenceWordsOff(final byte[] payload, final int tailStart, final int column) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int presWords = (rowCount + 63) >>> 6;
    return tailStart + columnCount + column * presWords * 8;
  }

  /**
   * End offset of the column data stream — header + recordKeys + all column bodies. Walks the column
   * directory; used by the one-shot evidence probe (the hot kernels get the boundary from
   * {@link #evaluateRowGroupMask}). Returns {@code -1} on structural inconsistency.
   */
  static int leafDataEnd(final byte[] payload) {
    final int rowCount = getIntLE(payload, 0);
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    if (rowCount == 0)
      return columnStreamStart(payload, rowCount, columnCount);
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          // Dict, then per-row counts, then the flat element run whose length is their sum.
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
        }
        default -> {
          return -1;
        }
      }
    }
    return cursor;
  }

  /** Sparse-evidence status: not yet probed. */
  public static final byte SPARSE_STATUS_UNKNOWN = 0;
  /** Every leaf carries presence data and the column never saw an unrepresentable value. */
  public static final byte SPARSE_STATUS_CLEAN = 1;
  /**
   * Some leaf lacks a valid presence tail (malformed) or saw an unrepresentable value — fail closed.
   */
  public static final byte SPARSE_STATUS_DIRTY = 2;

  /**
   * One-shot probe over ALL leaves: per column, decide whether sparse-correct semantics can be served
   * from the projection. A column is CLEAN iff every leaf carries a valid presence tail AND never
   * flags the column as unrepresentable (JSON null / object / array / kind-mismatch values poison the
   * column — a present row's stored default is not the real value). Anything else is DIRTY —
   * consumers must fall back (typed scan kernels / generic pipeline), which is always correct.
   *
   * @return per-column status array of {@link #SPARSE_STATUS_CLEAN} / {@link #SPARSE_STATUS_DIRTY},
   *         sized {@code columnCount}; zero-length when the leaf list is empty.
   */
  public static byte[] probeSparseEvidence(final List<byte[]> rowGroupPayloads) {
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty())
      return new byte[0];
    final byte[] first = rowGroupPayloads.get(0);
    if (first == null)
      return new byte[0];
    final int columnCount = columnCountOf(first);
    final byte[] status = new byte[columnCount];
    Arrays.fill(status, SPARSE_STATUS_CLEAN);
    for (final byte[] payload : rowGroupPayloads) {
      if (payload == null || columnCountOf(payload) != columnCount) {
        Arrays.fill(status, SPARSE_STATUS_DIRTY);
        return status;
      }
      final int dataEnd = leafDataEnd(payload);
      final int tailStart = dataEnd < 0
          ? -1
          : presenceTailStart(payload, dataEnd);
      if (tailStart < 0) {
        // Malformed leaf — no trustworthy presence info; every column is dirty.
        Arrays.fill(status, SPARSE_STATUS_DIRTY);
        return status;
      }
      for (int c = 0; c < columnCount; c++) {
        if ((payload[tailStart + c] & ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE) != 0) {
          status[c] = SPARSE_STATUS_DIRTY;
        }
      }
    }
    return status;
  }

  /**
   * One-shot probe over ALL leaves: recover per-column NUMERIC_LONG integrality provenance from the
   * persisted bytes. A column's flag is the OR of
   * {@link ProjectionIndexRowGroupPage#COLUMN_FLAG_NON_INTEGRAL} across every leaf — {@code true}
   * means some cell was truncated from a non-integral number and value-exact consumers must decline
   * the column.
   *
   * <p>
   * Returns {@code null} — integrality UNKNOWN, consumers must fail closed — when any leaf lacks a
   * valid presence tail (malformed payload): fabricating "integral" for such leaves would let
   * aggregates return truncated sums. This is the persistence-safe counterpart of
   * {@code ProjectionIndexBuilder#numericColumnNonIntegralFlags()}: it lets a re-opened resource
   * re-derive the flags that previously lived only in builder memory.
   *
   * @return per-column non-integral flags sized {@code columnCount}, or {@code null} when any leaf
   *         lacks the presence tail; zero-length when the leaf list is empty.
   */
  public static boolean[] probeNumericNonIntegral(final List<byte[]> rowGroupPayloads) {
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty())
      return new boolean[0];
    final byte[] first = rowGroupPayloads.get(0);
    if (first == null || first.length < 8)
      return null;
    final int columnCount = columnCountOf(first);
    if (columnCount < 0)
      return null;
    final boolean[] nonIntegral = new boolean[columnCount];
    try {
      for (final byte[] payload : rowGroupPayloads) {
        if (payload == null || payload.length < 8 || columnCountOf(payload) != columnCount)
          return null;
        final int dataEnd = leafDataEnd(payload);
        final int tailStart = dataEnd < 0
            ? -1
            : presenceTailStart(payload, dataEnd);
        if (tailStart < 0)
          return null;
        for (int c = 0; c < columnCount; c++) {
          if ((payload[tailStart + c] & ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL) != 0) {
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
   * presence tail asserts {@link ProjectionIndexRowGroupPage#COLUMN_FLAG_PURE_DOUBLE_SOURCE} — the
   * aggregation direction is AND, the opposite of the sticky-poison probes: purity is a positive
   * claim that one silent leaf (old bytes, impure sources) must be able to veto. Returns {@code null}
   * on any malformed payload — consumers fail closed.
   */
  public static boolean[] probeDoublePureSource(final List<byte[]> rowGroupPayloads) {
    if (rowGroupPayloads == null || rowGroupPayloads.isEmpty())
      return new boolean[0];
    final byte[] first = rowGroupPayloads.get(0);
    if (first == null || first.length < 8)
      return null;
    final int columnCount = columnCountOf(first);
    if (columnCount < 0)
      return null;
    final boolean[] pure = new boolean[columnCount];
    try {
      for (int c = 0; c < columnCount; c++) {
        pure[c] = first[24 + c] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE;
      }
      for (final byte[] payload : rowGroupPayloads) {
        if (payload == null || payload.length < 8 || columnCountOf(payload) != columnCount)
          return null;
        final int dataEnd = leafDataEnd(payload);
        final int tailStart = dataEnd < 0
            ? -1
            : presenceTailStart(payload, dataEnd);
        if (tailStart < 0)
          return null;
        for (int c = 0; c < columnCount; c++) {
          if ((payload[tailStart + c] & ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE) == 0) {
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

  private static long countRowGroup(final byte[] payload, final ProjectionIndexScan.ColumnPredicate[] predicates,
      final ScanScratch s) {
    final int rowCount = evaluateRowGroupMask(payload, predicates, s);
    if (rowCount <= 0)
      return 0L;
    final int stride = (rowCount + 63) >>> 6;
    long result = 0;
    final long[] mask = s.mask;
    for (int i = 0; i < stride; i++)
      result += Long.bitCount(mask[i]);
    return result;
  }

  /**
   * Parse leaf offsets, apply zone-map pruning, and compute the final conjunctive predicate mask into
   * {@code s.mask}. Returns the leaf's {@code rowCount} (possibly with a zeroed-out mask when
   * zone-map rules out the page), or {@code 0} for empty leaves / zone-map skips — callers should
   * treat {@code 0} as "nothing to do".
   *
   * <p>
   * Sparse-field semantics: when the leaf carries a presence tail, every predicate's match bits are
   * AND-ed with the predicate column's presence bitmap — a comparison/EBV over a MISSING field
   * evaluates over the empty sequence and is FALSE in XQuery, never "matches the stored default".
   * Leaves without a readable tail keep the all-present behavior; sparse-correct callers must gate on
   * {@link #probeSparseEvidence} before trusting them.
   *
   * <p>
   * The mask is sized to {@code ceil(MAX_ROWS/64)}; only the first {@code ceil(rowCount/64)} words
   * are populated. As a side effect {@code s.leafDataEnd} records where the column stream ends.
   */
  /**
   * Evaluate ONE predicate leaf into {@code s.colMask} (presence NOT yet applied) — the shared
   * per-leaf body of the conjunctive and TREE mask builders, so their semantics cannot drift.
   */
  private static void evalPredicateLeafMask(final byte[] payload, final ProjectionIndexScan.ColumnPredicate p,
      final int rowCount, final ScanScratch s) {
    final int kindsOff = 24;
    final byte kind = payload[kindsOff + p.column];
    switch (kind) {
      // A temporal predicate arrives already expressed in the column's own units: the executor maps
      // the string literal onto an exact value or a half-open epoch range before building it.
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
        evalNumericBytes(payload, s.columnDataOff[p.column], rowCount, p.op, p.longLit, p.highLit, s.numericScratch,
            s.numericFlags, s.colMask);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> {
        // Two forms reach a global column. A pre-evaluated VERDICT (containment, ordering — any
        // per-value string question) tests each row's id against the caller-built bitset: the
        // string work happened once per distinct id, the row sweep is pure integer work. An
        // id-resolved EQ/NE evaluates as the integers ids are. A predicate still carrying its
        // string bytes WITHOUT a verdict never reached either translation, and comparing bytes
        // against ids would answer a different question with a plausible number — throw.
        if (p.globalIdVerdict != null) {
          evalGlobalVerdictBytes(payload, s.columnDataOff[p.column], rowCount, p.globalIdVerdict,
              p.globalIdVerdictCount, s.colMask);
        } else if (p.stringLitBytes != null) {
          throw new IllegalStateException("column " + p.column + " is STRING_GLOBAL, but the " + p.op
              + " predicate still carries a string literal — it was never resolved to a dictionary id");
        } else {
          evalNumericBytes(payload, s.columnDataOff[p.column], rowCount, p.op, p.longLit, p.highLit, s.numericScratch,
              s.numericFlags, s.colMask);
        }
      }
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
        evalBooleanBytes(payload, s.columnDataOff[p.column], rowCount, p.boolLit, s.colMask);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
        // Per-entry two-phase evaluation: NE is an ordinary per-entry outcome now (no
        // complement pass, no tail-mask hazard), and ordering/contains ride the same path.
        evalStringDictBytes(payload, s.columnDataOff[p.column], rowCount, p, s.colMask);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
        if (p.op != ProjectionIndexScan.Op.EQ) {
          // `!= lit` over a SET is not the complement of membership — the interpreter compares a
          // SEQUENCE to a literal, which is an existential over the elements, so its negation is
          // not "does not contain". Decline rather than answer a different question.
          throw new IllegalStateException("STRING_SET column supports containment (EQ) only, got " + p.op);
        }
        evalStringSetContainsBytes(payload, s.columnDataOff[p.column], rowCount, p.stringLitBytes, s.colMask);
      }
      default -> throw new IllegalStateException("Unknown column kind " + kind);
    }
  }

  /** Zero-length predicate array for the tree evaluator's offset-filling pre-pass. */
  private static final ProjectionIndexScan.ColumnPredicate[] NO_PREDICATES = new ProjectionIndexScan.ColumnPredicate[0];

  /**
   * TREE mask builder — the disjunction-capable twin of {@link #evaluateRowGroupMask}: each tree leaf
   * evaluates into its OWN mask with its OWN column's presence ANDed in (missing ⇒ false per leaf —
   * the tree type's contract, which is what makes OR over sparse fields sound), then the postfix
   * program combines masks with AND/OR. No zone pruning: a leaf's zone evidence cannot prune the
   * GROUP under a disjunction, so the tree path conservatively reads every leaf.
   */
  private static int evaluateRowGroupMaskTree(final byte[] payload, final ProjectionIndexScan.PredicateTree tree,
      final ScanScratch s) {
    // The conjunctive builder with zero predicates fills the column offsets, leafDataEnd and the
    // tail-masked all-true mask; its zone loop no-ops.
    final int rowCount = evaluateRowGroupMask(payload, NO_PREDICATES, s);
    if (rowCount <= 0) {
      return rowCount;
    }
    final int stride = rowCount + 63 >>> 6;
    final int tailStart = presenceTailStart(payload, s.leafDataEnd);
    long[][] stack = s.treeMaskStack;
    if (stack == null || stack[0].length < stride) {
      stack = s.treeMaskStack =
          new long[ProjectionIndexScan.PredicateTree.MAX_LEAVES][(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6];
    }
    int top = 0;
    final long[] colMask = s.colMask;
    for (final byte op : tree.program) {
      if (op >= 0) {
        final ProjectionIndexScan.ColumnPredicate p = tree.leaves[op];
        Arrays.fill(colMask, 0, stride, 0L);
        evalPredicateLeafMask(payload, p, rowCount, s);
        final long[] dst = stack[top++];
        if (tailStart >= 0) {
          final int presOff = presenceWordsOff(payload, tailStart, p.column);
          for (int i = 0; i < stride; i++) {
            dst[i] = colMask[i] & getLongLE(payload, presOff + i * 8);
          }
        } else {
          System.arraycopy(colMask, 0, dst, 0, stride);
        }
      } else if (op == ProjectionIndexScan.PredicateTree.OP_AND) {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] &= b[i];
        }
      } else if (op == ProjectionIndexScan.PredicateTree.OP_NOT) {
        // Complement of (present AND matches) = missing OR (present AND !matches) — exactly
        // fn:not over the child. Tail garbage past rowCount is cleared by the final AND with
        // the tail-masked base mask below.
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] = ~a[i];
        }
      } else {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] |= b[i];
        }
      }
    }
    final long[] result = stack[0];
    final long[] mask = s.mask;
    for (int i = 0; i < stride; i++) {
      mask[i] &= result[i];
    }
    return rowCount;
  }

  private static int evaluateRowGroupMask(final byte[] payload, final ProjectionIndexScan.ColumnPredicate[] predicates,
      final ScanScratch s) {
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
      s.leafDataEnd = columnStreamStart(payload, rowCount, columnCount);
      return 0;
    }

    // Compute column offsets in one pass. Each column starts with
    // (min, max) 16 bytes, then its kind-specific data.
    int cursor = columnStreamStart(payload, rowCount, columnCount);
    for (int c = 0; c < columnCount; c++) {
      columnMinMaxOff[c] = cursor;
      cursor += 16;
      columnDataOff[c] = cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
            ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP,
            ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
          cursor += rowCount * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> {
          final int dictSize = getIntLE(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += getIntLE(payload, cursor + 4 + i * 4);
          }
          // Dict, then per-row counts, then the flat element run whose length is their sum.
          final int countsOff = cursor + 4 + dictSize * 4 + lenTotal;
          int elemTotal = 0;
          for (int r = 0; r < rowCount; r++) {
            elemTotal += getIntLE(payload, countsOff + r * 4);
          }
          cursor = countsOff + rowCount * 4 + elemTotal * 4;
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
      if (!ProjectionIndexRowGroupPage.isNumericKind(kind) && !ProjectionIndexRowGroupPage.isTemporalKind(kind))
        continue;
      final long min = getLongLE(payload, columnMinMaxOff[p.column]);
      final long max = getLongLE(payload, columnMinMaxOff[p.column] + 8);
      if (min > max)
        return 0; // no present value in the column at all
      if (zoneSkip(p, min, max))
        return 0;
    }

    final int tailStart = presenceTailStart(payload, cursor);

    // Build the conjunctive mask over the caller-provided buffers.
    final int stride = (rowCount + 63) >>> 6;
    fillAllTrue(mask, rowCount);
    for (final var p : predicates) {
      // Only clear the live prefix of colMask — tail words beyond
      // `stride` are never read.
      Arrays.fill(colMask, 0, stride, 0L);
      evalPredicateLeafMask(payload, p, rowCount, s);
      if (tailStart >= 0) {
        // Missing field ⇒ predicate is false — AND with the column's presence.
        final int presOff = presenceWordsOff(payload, tailStart, p.column);
        for (int i = 0; i < stride; i++) {
          mask[i] &= colMask[i] & getLongLE(payload, presOff + i * 8);
        }
      } else {
        for (int i = 0; i < stride; i++)
          mask[i] &= colMask[i];
      }
    }
    return rowCount;
  }

  /**
   * Flip the first {@code rowCount} bits of {@code mask} in place, leaving every bit at or beyond
   * {@code rowCount} clear.
   *
   * <p>
   * The tail matters: masks are sized in whole 64-bit words, so the last word carries up to 63 bits
   * that name no row. Complementing them would light rows that do not exist, and the callers AND
   * these masks together — one stray bit survives into the answer.
   */
  private static void invertMaskRows(final long[] mask, final int rowCount) {
    final int stride = (rowCount + 63) >>> 6;
    for (int i = 0; i < stride; i++) {
      mask[i] = ~mask[i];
    }
    final int tailBits = rowCount & 63;
    if (tailBits != 0) {
      mask[stride - 1] &= (1L << tailBits) - 1;
    }
  }

  /** Package-private: the SINGLE zone-skip authority, shared with the column kernels. */
  static boolean zoneSkip(final ProjectionIndexScan.ColumnPredicate p, final long min, final long max) {
    // A STRING predicate can never be zone-skipped, whatever its op. A string column's zone map
    // holds min/max DICTIONARY IDS — which say nothing about the values' order or content — while
    // the numeric arms below test p.longLit, which a string predicate does not set.
    //
    // The STR_* ops opt out below for exactly this reason, but EQ and NE did not, and were safe
    // only by accident: per-leaf dict ids start at 0, so `longLit == 0` fell inside every zone and
    // the test never fired. Global dictionary ids start at 1, at which point the same unsound
    // comparison starts pruning leaves that hold matches — a silent wrong answer, and one that
    // shows up as a missing count rather than an error.
    if (p.stringLitBytes != null) {
      return false;
    }
    return switch (p.op) {
      case GT -> max <= p.longLit;
      case LT -> min >= p.longLit;
      case GE -> max < p.longLit;
      case LE -> min > p.longLit;
      case EQ -> p.longLit < min || p.longLit > max;
      // A leaf can only be skipped for NE when EVERY value equals the literal, i.e. the zone
      // collapses onto it. Common enough to be worth the test: a column that is constant 0 across a
      // row group skips entirely for `!= 0`.
      case NE -> min == max && min == p.longLit;
      // BETWEEN zone-skip: OR of the two single-bound zone-skip conditions.
      // Strictly no more pessimistic than two independent predicates — see
      // iter07-range-fusion-analysis.md for the semantics derivation.
      case BETWEEN_GT_LT -> max <= p.longLit || min >= p.highLit;
      case BETWEEN_GT_LE -> max <= p.longLit || min > p.highLit;
      case BETWEEN_GE_LT -> max < p.longLit || min >= p.highLit;
      case BETWEEN_GE_LE -> max < p.longLit || min > p.highLit;
      // NEVER skip on string ops: a STRING_DICT column's zone map holds min/max DICT IDS, which
      // say nothing about the values' order or content — pruning here drops matching leaves.
      case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> false;
    };
  }

  /**
   * 64-bit FNV-1a hash over a byte slice. Stable across JVMs; adequate collision resistance for the
   * group-by intern ({@code ~10⁻¹⁹} at 10M distinct values). Matches the hash used in
   * {@link io.sirix.page.pax.StringRegion.Encoder} so encoder / decoder agree on key space when
   * interop is needed.
   */
  /**
   * THE group-identity hash — shared by the byte and sliced kernel families so the domains can never
   * drift (table keys, winner hashes, distinct-set keys all ride it).
   */
  static long fnv1a64(final byte[] data, final int off, final int len) {
    long h = 0xcbf29ce484222325L;
    final int end = off + len;
    for (int i = off; i < end; i++) {
      h ^= data[i] & 0xFF;
      h *= 0x100000001b3L;
    }
    return h;
  }

  /**
   * SIMD numeric compare, two-pass so the compare auto-vectorises without any heap allocation:
   *
   * <ol>
   * <li><b>Load</b> the numeric column out of the {@code byte[]} payload into a reusable
   * {@code long[]} scratch via the byte-array {@link VarHandle} (HotSpot intrinsifies to a tight MOVQ
   * loop).</li>
   * <li><b>Compare</b> each row against the bound(s) and write a branch-free {@code 0}/{@code 1} flag
   * into {@code flags} — independent stores with no loop-carried dependency, so C2 SuperWord
   * vectorises it to {@code VPCMPGTQ}+select over {@code LANES} rows at a time.</li>
   * <li><b>Pack</b> 64 flags into each output bitmask word ({@link #packFlags}).</li>
   * </ol>
   *
   * <p>
   * The earlier {@code jdk.incubator.vector} body produced the packed mask in one
   * {@code compare(...).toLong()} pass, but the {@code VectorMask} temporary boxes on this runtime —
   * measured ~26 KB/leaf, HotSpot C2 vector-box elimination does not fire for the {@code toLong()}
   * pattern (nor did it on Oracle GraalVM 25.0.2, the original iter#15 trigger). This two-pass form
   * is allocation-free <em>and</em> ~2.9× faster than the previous scalar predicate-into-bitmask
   * loop, because the packed-bit store's loop-carried dependency (64 rows write the same word) is
   * confined to the cheap pack pass and no longer blocks vectorisation of the compare.
   */
  /**
   * Row sweep for a pre-evaluated global-dictionary verdict: row {@code k} matches iff its id lane
   * holds a valid id whose verdict bit is set. Ids outside {@code 1 .. idCount} — the missing-cell 0
   * and anything malformed — never match, which is exactly the missing ⇒ false leaf contract.
   */
  private static void evalGlobalVerdictBytes(final byte[] payload, final int baseOff, final int rowCount,
      final long[] verdict, final int idCount, final long[] out) {
    for (int k = 0; k < rowCount; k++) {
      final long id = getLongLE(payload, baseOff + k * 8);
      if (id >= 1 && id <= idCount && (verdict[(int) (id >>> 6)] & 1L << (id & 63)) != 0L) {
        out[k >>> 6] |= 1L << (k & 63);
      }
    }
  }

  private static void evalNumericBytes(final byte[] payload, final int baseOff, final int rowCount,
      final ProjectionIndexScan.Op op, final long lit, final long highLit, final long[] scratch, final long[] flags,
      final long[] out) {
    // 1) Load column into scratch — fully intrinsified (MOVQ per lane).
    for (int k = 0; k < rowCount; k++) {
      scratch[k] = getLongLE(payload, baseOff + k * 8);
    }
    // 2) Vectorisable compare pass. Each arm is a single relational op (or a
    // non-short-circuit AND of two, for the fused BETWEEN range) applied
    // branch-free into `flags` — the shape C2 SuperWord turns into a packed
    // vector compare. Dispatch is hoisted out of the loop by the tableswitch.
    switch (op) {
      case GT -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] > lit)
              ? 1L
              : 0L;
      }
      case LT -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] < lit)
              ? 1L
              : 0L;
      }
      case GE -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] >= lit)
              ? 1L
              : 0L;
      }
      case LE -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] <= lit)
              ? 1L
              : 0L;
      }
      case EQ -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] == lit)
              ? 1L
              : 0L;
      }
      case NE -> {
        for (int k = 0; k < rowCount; k++)
          flags[k] = (scratch[k] != lit)
              ? 1L
              : 0L;
      }
      case BETWEEN_GT_LT -> {
        for (int k = 0; k < rowCount; k++) {
          final long v = scratch[k];
          flags[k] = ((v > lit) & (v < highLit))
              ? 1L
              : 0L;
        }
      }
      case BETWEEN_GT_LE -> {
        for (int k = 0; k < rowCount; k++) {
          final long v = scratch[k];
          flags[k] = ((v > lit) & (v <= highLit))
              ? 1L
              : 0L;
        }
      }
      case BETWEEN_GE_LT -> {
        for (int k = 0; k < rowCount; k++) {
          final long v = scratch[k];
          flags[k] = ((v >= lit) & (v < highLit))
              ? 1L
              : 0L;
        }
      }
      case BETWEEN_GE_LE -> {
        for (int k = 0; k < rowCount; k++) {
          final long v = scratch[k];
          flags[k] = ((v >= lit) & (v <= highLit))
              ? 1L
              : 0L;
        }
      }
      default -> throw new IllegalStateException("Unknown numeric op " + op);
    }
    // 3) Pack the per-row flags into the packed-bit output mask.
    packFlags(flags, rowCount, out);
  }

  /**
   * Pack per-row flags into a packed-bit mask: bit {@code (w*64 + b)} of {@code out} is set iff
   * {@code flags[w*64 + b] != 0}. OR's into {@code out} (the caller pre-clears the live prefix),
   * matching the former in-place bit-set semantics. The inner reduction into a register-local
   * {@code word} keeps the loop-carried write off the hot compare path.
   *
   * <p>
   * A flag is normalised to a single bit ({@code != 0 ? 1 : 0}) before the shift, so the contract
   * holds for any non-zero truth encoding — e.g. the all-ones {@code -1L} a SIMD lane-mask naturally
   * produces — not only the {@code 0}/{@code 1} the current compare pass writes.
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
        word |= (flags[base + b] != 0
            ? 1L
            : 0L) << b;
      }
      out[w] |= word;
    }
  }

  private static void evalBooleanBytes(final byte[] payload, final int baseOff, final int rowCount,
      final boolean wantTrue, final long[] out) {
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
      if (tail != 0)
        out[stride - 1] &= (1L << tail) - 1L;
    }
  }

  /**
   * Set membership over the raw leaf bytes: mark rows whose set holds {@code literal}.
   *
   * <p>
   * Mirror of {@link #evalStringEqBytes} for a variable-length column. The literal resolves against
   * the leaf's dictionary once, and a literal the dictionary does not hold leaves the mask untouched
   * — every row in the leaf is ruled out without reading one element.
   */
  private static void evalStringSetContainsBytes(final byte[] payload, final int baseOff, final int rowCount,
      final byte[] literal, final long[] out) {
    // [int dictSize][int[dictSize] lengths][concat bytes][int[rowCount] counts][int[total] elems]
    final int dictSize = getIntLE(payload, baseOff);
    int concatOff = baseOff + 4 + dictSize * 4;
    int targetDictId = -1;
    for (int i = 0; i < dictSize; i++) {
      final int len = getIntLE(payload, baseOff + 4 + i * 4);
      if (bytesEqualAt(payload, concatOff, len, literal)) {
        targetDictId = i;
      }
      concatOff += len; // must advance past every entry to reach the counts region
    }
    if (targetDictId < 0) {
      return;
    }
    final int countsOff = concatOff;
    final int elemsOff = countsOff + rowCount * 4;
    int cursor = 0;
    for (int r = 0; r < rowCount; r++) {
      final int n = getIntLE(payload, countsOff + r * 4);
      for (int k = 0; k < n; k++) {
        if (getIntLE(payload, elemsOff + (cursor + k) * 4) == targetDictId) {
          out[r >>> 6] |= 1L << (r & 63);
          break; // an existential needs one witness
        }
      }
      cursor += n; // advance regardless, or the run desynchronises
    }
  }

  private static void evalStringDictBytes(final byte[] payload, final int baseOff, final int rowCount,
      final ProjectionIndexScan.ColumnPredicate p, final long[] out) {
    // Dict header: [int dictSize][int[dictSize] lengths][concat bytes][int[rowCount] ids].
    // Two-phase: the predicate evaluates ONCE per dict entry into an id bitset — a walk this
    // method already paid to locate the ids region — then the row sweep is one bit test each.
    // All op semantics live in ProjectionIndexScan#stringDictEntryMatches, shared with the
    // hydrated and sliced kernels.
    final int dictSize = getIntLE(payload, baseOff);
    final long[] idBits = new long[dictSize + 63 >>> 6];
    boolean any = false;
    final byte[] lit = p.stringLitBytes;
    final boolean litHasSupplementary = ProjectionIndexScan.hasFourByteUtf8(lit, 0, lit.length);
    int concatOff = baseOff + 4 + dictSize * 4;
    for (int i = 0; i < dictSize; i++) {
      final int len = getIntLE(payload, baseOff + 4 + i * 4);
      if (ProjectionIndexScan.stringDictEntryMatches(payload, concatOff, len, p.op, lit, litHasSupplementary)) {
        idBits[i >>> 6] |= 1L << (i & 63);
        any = true;
      }
      concatOff += len;
    }
    if (!any) {
      return;
    }
    final int idsOff = concatOff;
    for (int i = 0; i < rowCount; i++) {
      final int id = getIntLE(payload, idsOff + i * 4);
      if ((idBits[id >>> 6] & 1L << (id & 63)) != 0L) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
  }

  private static boolean bytesEqualAt(final byte[] a, final int aOff, final int len, final byte[] b) {
    if (len != b.length)
      return false;
    for (int i = 0; i < len; i++)
      if (a[aOff + i] != b[i])
        return false;
    return true;
  }

  private static void fillAllTrue(final long[] mask, final int rowCount) {
    final int fullWords = rowCount >>> 6;
    for (int i = 0; i < fullWords; i++)
      mask[i] = -1L;
    final int tail = rowCount & 63;
    if (tail != 0)
      mask[fullWords] = (1L << tail) - 1L;
  }

  /**
   * Prove a per-leaf dictionary entry's identity the FIRST time a surviving row names it, and never
   * again for that leaf: {@code proven} is the leaf's bitset over dictionary ids. The registry then
   * retains exactly the strings that become group keys — the answer's own vocabulary — instead of
   * every string the visited leaves store, which is what let a 1M-row corpus exhaust the budget on a
   * query whose answer held a few thousand keys. A collision or budget refusal latches in the
   * registry; the executor's post-join check declines the serve whatever this leaf saw.
   *
   * @return {@code false} when the scan must decline
   */
  static boolean proveOnFirstUse(final ProjectionStringIdentityRegistry registry,
      final ProjectionStringIdentityRegistry.LocalProofCache cache, final int component, final long[] proven,
      final int dictId, final long laneA, final long laneB, final byte[] utf8, final int off, final int len) {
    final int word = dictId >>> 6;
    final long bit = 1L << (dictId & 63);
    if ((proven[word] & bit) != 0L) {
      return true;
    }
    if (!cache.prove(registry, component, laneA, laneB, utf8, off, len)) {
      return false;
    }
    proven[word] |= bit;
    return true;
  }
}
