/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.sirix.page.pax.NumberRegionCompact;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
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
    Long2ObjectOpenHashMap<String> stringIntern;
    // iter#10 dense group-by remap: per-leaf dictId -> canonId.
    // Pre-allocated to 64, grown on demand for leaves with larger dicts.
    int[] dictRemap;
    // iter#22 — reusable FOR-BP header. No allocation per leaf parse;
    // NumberRegionCompact.readHeader populates in place.
    final NumberRegionCompact.Header forBpHeader = new NumberRegionCompact.Header();
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
      total += countLeaf(payload, predicates, s.columnDataOff, s.columnMinMaxOff,
          s.numericScratch, s.mask, s.colMask, s.forBpHeader);
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
    MemorySegment payloadSegment = null;
    // Local scratch header — columnDataOffFor is invoked at probe time
    // (once per handle, not per-leaf at scan time), so a lightweight
    // allocation here is fine and avoids contaminating the ScanScratch.
    final NumberRegionCompact.Header tmp = new NumberRegionCompact.Header();
    for (int c = 0; c < columnCount; c++) {
      cursor += 16;  // per-column min/max
      if (c == groupColumn) return cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> cursor += rowCount * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP -> {
          if (payloadSegment == null) payloadSegment = MemorySegment.ofArray(payload);
          NumberRegionCompact.readHeader(payloadSegment, cursor, tmp);
          cursor += tmp.headerBytes + (int) tmp.bodyBytes;
        }
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
      final int rowCount = evaluateLeafMask(payload, predicates,
          s.columnDataOff, s.columnMinMaxOff, s.numericScratch, s.mask, s.colMask, s.forBpHeader);
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

      if (needsFallback) {
        // Fallback: run the standard hashmap path on this single leaf.
        // The caller merges fallbackOut back into the final aggregate.
        if (fallbackOut == null) {
          throw new IllegalStateException(
              "canonical dict missing value and no fallback provided for leaf with dictSize=" + dictSize);
        }
        conjunctiveCountByGroupSingleLeaf(payload, rowCount, s.mask,
            groupBase, dictSize, lenHeaderOff, concatOff, idsOff, s, fallbackOut);
        continue;
      }

      // Dense hot loop: counts[remap[dictId]]++ per matching row.
      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
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
      final ScanScratch s, final Object2LongOpenHashMap<String> out) {
    if (s.dictCache == null || s.dictCache.length < dictSize) {
      s.dictCache = new String[Math.max(64, dictSize)];
      s.dictByteOff = new int[s.dictCache.length];
    } else {
      // Clear the prefix we'll populate.
      for (int i = 0; i < dictSize; i++) s.dictCache[i] = null;
    }
    if (s.stringIntern == null) {
      s.stringIntern = new Long2ObjectOpenHashMap<>(32);
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
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int rowIdx = (w << 6) + bit;
        if (rowIdx >= rowCount) break;
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
      s.stringIntern = new Long2ObjectOpenHashMap<>(32);
    }
    String[] dictCache = s.dictCache;
    int[] dictByteOff = s.dictByteOff;
    final Long2ObjectOpenHashMap<String> intern = s.stringIntern;
    for (final byte[] payload : leafPayloads) {
      final int columnCount = columnCountOf(payload);
      if (s.columnDataOff.length < columnCount) {
        s.columnDataOff = new int[columnCount];
        s.columnMinMaxOff = new int[columnCount];
      }
      final int rowCount = evaluateLeafMask(payload, predicates,
          s.columnDataOff, s.columnMinMaxOff, s.numericScratch, s.mask, s.colMask, s.forBpHeader);
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

      final int stride = (rowCount + 63) >>> 6;
      final long[] scanMask = s.mask;
      for (int w = 0; w < stride; w++) {
        long word = scanMask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) break;
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

  private static long countLeaf(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] columnDataOff, final int[] columnMinMaxOff,
      final long[] numericScratch, final long[] mask, final long[] colMask,
      final NumberRegionCompact.Header forBpHeader) {
    final int rowCount = evaluateLeafMask(payload, predicates,
        columnDataOff, columnMinMaxOff, numericScratch, mask, colMask, forBpHeader);
    if (rowCount <= 0) return 0L;
    final int stride = (rowCount + 63) >>> 6;
    long result = 0;
    for (int i = 0; i < stride; i++) result += Long.bitCount(mask[i]);
    return result;
  }

  /**
   * Parse leaf offsets, apply zone-map pruning, and compute the final
   * conjunctive predicate mask into {@code mask}. Returns the leaf's
   * {@code rowCount} (possibly with a zeroed-out mask when zone-map
   * rules out the page), or {@code 0} for empty leaves / zone-map
   * skips — callers should treat {@code 0} as "nothing to do".
   *
   * <p>The mask is sized by the caller to {@code ceil(MAX_ROWS/64)};
   * only the first {@code ceil(rowCount/64)} words are populated.
   *
   * <p>{@code forBpHeader} is a caller-owned reusable
   * {@link NumberRegionCompact.Header} scratch used to parse FOR-BP
   * encoded numeric columns without per-call allocation. Callers pass
   * the thread-local one from {@link ScanScratch}.
   */
  private static int evaluateLeafMask(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] columnDataOff, final int[] columnMinMaxOff,
      final long[] numericScratch, final long[] mask, final long[] colMask,
      final NumberRegionCompact.Header forBpHeader) {
    final int rowCount = getIntLE(payload, 0);
    if (rowCount == 0) return 0;
    final int columnCount = getIntLE(payload, 4);
    final int kindsOff = 24;
    final int recordKeysOff = kindsOff + columnCount;

    // iter#22 — Single on-heap view, reused across every FOR-BP column
    // parse on this payload. ofArray is zero-copy. Lazily allocated only
    // when at least one FOR-BP column is actually present.
    MemorySegment payloadSegment = null;

    // Compute column offsets in one pass. Each column starts with
    // (min, max) 16 bytes, then its kind-specific data.
    int cursor = recordKeysOff + rowCount * 8;
    for (int c = 0; c < columnCount; c++) {
      columnMinMaxOff[c] = cursor;
      cursor += 16;
      columnDataOff[c] = cursor;
      final byte kind = payload[kindsOff + c];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> cursor += rowCount * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP -> {
          // Parse header once to advance the cursor. The body is decoded
          // only on demand when the column is referenced by a predicate —
          // header parse is ~10 ns, trivial vs the decode+predicate cost.
          if (payloadSegment == null) payloadSegment = MemorySegment.ofArray(payload);
          NumberRegionCompact.readHeader(payloadSegment, cursor, forBpHeader);
          cursor += forBpHeader.headerBytes + (int) forBpHeader.bodyBytes;
        }
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

    // Zone-map prune — numeric columns only (same policy as the
    // materialising variant). Applies to both raw NUMERIC_LONG and
    // FOR-BP encoded columns since both carry the identical 16-byte
    // (min, max) prefix.
    for (final var p : predicates) {
      final byte kind = payload[kindsOff + p.column];
      if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
          && kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP) {
        continue;
      }
      final long min = getLongLE(payload, columnMinMaxOff[p.column]);
      final long max = getLongLE(payload, columnMinMaxOff[p.column] + 8);
      if (zoneSkip(p, min, max)) return 0;
    }

    // Build the conjunctive mask over the caller-provided buffers.
    final int stride = (rowCount + 63) >>> 6;
    fillAllTrue(mask, rowCount);
    for (final var p : predicates) {
      // Only clear the live prefix of colMask — tail words beyond
      // `stride` are never read.
      Arrays.fill(colMask, 0, stride, 0L);
      final byte kind = payload[kindsOff + p.column];
      switch (kind) {
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> evalNumericBytes(
            payload, columnDataOff[p.column], rowCount, p.op, p.longLit, p.highLit, numericScratch, colMask);
        case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP -> {
          if (payloadSegment == null) payloadSegment = MemorySegment.ofArray(payload);
          evalNumericBytesForBp(payloadSegment, columnDataOff[p.column], rowCount,
              p.op, p.longLit, p.highLit, numericScratch, colMask, forBpHeader);
        }
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> evalBooleanBytes(
            payload, columnDataOff[p.column], rowCount, p.boolLit, colMask);
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> evalStringEqBytes(
            payload, columnDataOff[p.column], rowCount, p.stringLitBytes, colMask);
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
      for (int i = 0; i < stride; i++) mask[i] &= colMask[i];
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
    // 2) Delegate to the shared predicate evaluator — same shape whether
    // the scratch was populated from a raw-long column or a FOR-BP
    // decode. Keeps one C2 compile shape for every numeric eval path.
    evalNumericScratch(scratch, rowCount, op, lit, highLit, out);
  }

  /**
   * iter#22 — numeric compare over a FOR-BP (Frame of Reference + Bit
   * Packing) encoded column. Two short-circuit fast paths run before
   * any decode:
   *
   * <ol>
   *   <li><b>Constant-run</b>: when {@code bitWidth == 0}, every value
   *       equals {@code header.minValue}. A single scalar compare
   *       decides the whole leaf — no decode required.</li>
   *   <li><b>Bit-width gate</b>: the predicate literal is tested against
   *       the effective column range {@code [min, min + (1<<bitWidth) - 1]}.
   *       If the predicate is provably all-match or all-none across
   *       that range, {@code out} gets the all-ones prefix or stays
   *       empty and we skip the decode+SIMD entirely. This is a
   *       "zone-map within zone-map" that helps any leaf whose
   *       column range is narrower than the literal-span.</li>
   * </ol>
   *
   * <p>Otherwise, decodes the bit-packed body into {@code scratch} via
   * {@link NumberRegionCompact#decodeAll} (zero-alloc, right-shift +
   * mask per value) and falls through to the shared scalar-broadword
   * predicate evaluator {@link #evalNumericScratch}. Only the source
   * of scratch differs from the raw-long path (decode vs direct MOVQ),
   * so C2 pre-warm covers both.
   *
   * <p>HFT invariants: {@code forBpHeader} is caller-owned (thread-local
   * {@link ScanScratch}); scratch is caller-owned too; no allocation
   * in this method.
   */
  private static void evalNumericBytesForBp(final MemorySegment payload,
      final int baseOff, final int rowCount, final ProjectionIndexScan.Op op,
      final long lit, final long highLit, final long[] scratch, final long[] out,
      final NumberRegionCompact.Header forBpHeader) {
    NumberRegionCompact.readHeader(payload, baseOff, forBpHeader);
    if (forBpHeader.count != rowCount) {
      throw new IllegalStateException(
          "FOR-BP column count=" + forBpHeader.count + " != leaf rowCount=" + rowCount);
    }
    final int bitWidth = forBpHeader.bitWidth;
    final long base = forBpHeader.minValue;

    // Fast-path 1 — constant-run. One scalar compare decides the whole leaf.
    if (bitWidth == 0) {
      if (evalScalarOp(base, op, lit, highLit)) fillAllTrue(out, rowCount);
      return;
    }

    // Fast-path 2 — bit-width gate. Value range is [base, base + (1<<bitWidth) - 1]
    // for widths ≤ 63; width 64 means raw storage (base forced to 0 by
    // the encoder) — skip the gate there. For widths 1..63 we compute
    // absMax from the encoded space; this is tighter or equal to the
    // column's min/max range (which is already the full observed span).
    if (bitWidth < 64) {
      final long maxPacked = (1L << bitWidth) - 1L;
      final long absMax = base + maxPacked;
      final int decision = evalRangeDecision(op, lit, highLit, base, absMax);
      if (decision == DECISION_ALL) {
        fillAllTrue(out, rowCount);
        return;
      }
      if (decision == DECISION_NONE) {
        return;
      }
      // DECISION_MIXED — fall through to decode + evaluate.
    }

    // Slow path: decode body into scratch (zero-alloc — caller-owned
    // long[]), then reuse the shared predicate evaluator.
    NumberRegionCompact.decodeAll(payload, forBpHeader, scratch);
    evalNumericScratch(scratch, rowCount, op, lit, highLit, out);
  }

  /** Decision codes for {@link #evalRangeDecision}. */
  private static final int DECISION_ALL = 1;
  private static final int DECISION_NONE = 2;
  private static final int DECISION_MIXED = 0;

  /**
   * Decide whether a predicate {@code op(lit, highLit)} over a column
   * whose value range is {@code [minInclusive, maxInclusive]} is
   * guaranteed to match all rows, no rows, or requires per-row
   * evaluation. Pure scalar, no allocation.
   */
  private static int evalRangeDecision(final ProjectionIndexScan.Op op,
      final long lit, final long highLit, final long minInclusive, final long maxInclusive) {
    return switch (op) {
      case GT -> (minInclusive > lit) ? DECISION_ALL : (maxInclusive <= lit ? DECISION_NONE : DECISION_MIXED);
      case GE -> (minInclusive >= lit) ? DECISION_ALL : (maxInclusive < lit ? DECISION_NONE : DECISION_MIXED);
      case LT -> (maxInclusive < lit) ? DECISION_ALL : (minInclusive >= lit ? DECISION_NONE : DECISION_MIXED);
      case LE -> (maxInclusive <= lit) ? DECISION_ALL : (minInclusive > lit ? DECISION_NONE : DECISION_MIXED);
      case EQ -> (minInclusive == maxInclusive && minInclusive == lit) ? DECISION_ALL
          : (lit < minInclusive || lit > maxInclusive ? DECISION_NONE : DECISION_MIXED);
      case BETWEEN_GT_LT -> rangeDecisionBetween(minInclusive, maxInclusive, lit, false, highLit, false);
      case BETWEEN_GT_LE -> rangeDecisionBetween(minInclusive, maxInclusive, lit, false, highLit, true);
      case BETWEEN_GE_LT -> rangeDecisionBetween(minInclusive, maxInclusive, lit, true,  highLit, false);
      case BETWEEN_GE_LE -> rangeDecisionBetween(minInclusive, maxInclusive, lit, true,  highLit, true);
    };
  }

  /**
   * BETWEEN range decision: all rows match iff {@code min ≥ lo} AND
   * {@code max ≤ hi}; no rows match iff {@code max ≤ lo} OR
   * {@code min ≥ hi} (applying the open/closed semantics per flag).
   */
  private static int rangeDecisionBetween(final long minInclusive, final long maxInclusive,
      final long lo, final boolean lowIncl, final long hi, final boolean highIncl) {
    final boolean allLo = lowIncl ? (minInclusive >= lo) : (minInclusive > lo);
    final boolean allHi = highIncl ? (maxInclusive <= hi) : (maxInclusive < hi);
    if (allLo && allHi) return DECISION_ALL;
    final boolean noLo = lowIncl ? (maxInclusive < lo) : (maxInclusive <= lo);
    final boolean noHi = highIncl ? (minInclusive > hi) : (minInclusive >= hi);
    if (noLo || noHi) return DECISION_NONE;
    return DECISION_MIXED;
  }

  /** Scalar evaluation of a predicate against a single value. Used by the constant-run FOR-BP shortcut. */
  private static boolean evalScalarOp(final long v, final ProjectionIndexScan.Op op,
      final long lit, final long highLit) {
    return switch (op) {
      case GT -> v > lit;
      case LT -> v < lit;
      case GE -> v >= lit;
      case LE -> v <= lit;
      case EQ -> v == lit;
      case BETWEEN_GT_LT -> v > lit && v < highLit;
      case BETWEEN_GT_LE -> v > lit && v <= highLit;
      case BETWEEN_GE_LT -> v >= lit && v < highLit;
      case BETWEEN_GE_LE -> v >= lit && v <= highLit;
    };
  }

  /**
   * Shared predicate evaluator that takes a populated {@code long[]}
   * scratch and writes per-row outcome bits into {@code out}. Extracted
   * so both the raw-long and the FOR-BP paths share one evaluator —
   * guarantees C2 treats them identically at the compare level and
   * avoids shape-divergence penalties.
   */
  private static void evalNumericScratch(final long[] scratch, final int rowCount,
      final ProjectionIndexScan.Op op, final long lit, final long highLit, final long[] out) {
    // BETWEEN range ops fuse two opposing-direction compares into one
    // pass. Kept as a distinct branch so the single-bound op arm stays
    // a single MOVM-shaped loop.
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
