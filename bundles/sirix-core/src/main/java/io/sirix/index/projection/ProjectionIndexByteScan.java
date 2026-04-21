/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
    it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap<String> stringIntern;
  }

  private static final ThreadLocal<ScanScratch> SCRATCH = ThreadLocal.withInitial(ScanScratch::new);

  private ProjectionIndexByteScan() {
  }

  /** Raw row count — parses the header only. Identical semantics to the materialising variant. */
  public static long countRows(final Iterable<byte[]> leafPayloads) {
    long total = 0;
    for (final byte[] payload : leafPayloads) {
      total += (int) INT_LE.get(payload, 0);
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
          s.numericScratch, s.mask, s.colMask);
    }
    return total;
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
      final int rowCount = evaluateLeafMask(payload, predicates,
          s.columnDataOff, s.columnMinMaxOff, s.numericScratch, s.mask, s.colMask);
      if (rowCount <= 0) continue;
      final byte groupKind = payload[24 + groupColumn];
      if (groupKind != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
        throw new IllegalStateException("groupColumn " + groupColumn
            + " is not STRING_DICT (kind=" + groupKind + ")");
      }
      final int groupBase = s.columnDataOff[groupColumn];
      final int dictSize = (int) INT_LE.get(payload, groupBase);
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
        running += (int) INT_LE.get(payload, lenHeaderOff + i * 4);
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
          final int dictId = (int) INT_LE.get(payload, idsOff + rowIdx * 4);
          String gv = dictCache[dictId];
          if (gv == null) {
            final int byteOff = dictByteOff[dictId];
            final int len = (int) INT_LE.get(payload, lenHeaderOff + dictId * 4);
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
    return (int) INT_LE.get(payload, 4);
  }

  private static long countLeaf(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] columnDataOff, final int[] columnMinMaxOff,
      final long[] numericScratch, final long[] mask, final long[] colMask) {
    final int rowCount = evaluateLeafMask(payload, predicates,
        columnDataOff, columnMinMaxOff, numericScratch, mask, colMask);
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
   */
  private static int evaluateLeafMask(final byte[] payload,
      final ProjectionIndexScan.ColumnPredicate[] predicates,
      final int[] columnDataOff, final int[] columnMinMaxOff,
      final long[] numericScratch, final long[] mask, final long[] colMask) {
    final int rowCount = (int) INT_LE.get(payload, 0);
    if (rowCount == 0) return 0;
    final int columnCount = (int) INT_LE.get(payload, 4);
    final int kindsOff = 24;
    final int recordKeysOff = kindsOff + columnCount;

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
        case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> cursor += ((rowCount + 63) >>> 6) * 8;
        case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> {
          final int dictSize = (int) INT_LE.get(payload, cursor);
          int lenTotal = 0;
          for (int i = 0; i < dictSize; i++) {
            lenTotal += (int) INT_LE.get(payload, cursor + 4 + i * 4);
          }
          cursor += 4 + dictSize * 4 + lenTotal + rowCount * 4;
        }
        default -> throw new IllegalStateException("Unknown column kind " + kind);
      }
    }

    // Zone-map prune — numeric columns only (same policy as the
    // materialising variant).
    for (final var p : predicates) {
      final byte kind = payload[kindsOff + p.column];
      if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) continue;
      final long min = (long) LONG_LE.get(payload, columnMinMaxOff[p.column]);
      final long max = (long) LONG_LE.get(payload, columnMinMaxOff[p.column] + 8);
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
            payload, columnDataOff[p.column], rowCount, p.op, p.longLit, numericScratch, colMask);
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
      final long lit, final long[] scratch, final long[] out) {
    // 1) Load column into scratch — fully intrinsified (MOVQ per lane).
    for (int k = 0; k < rowCount; k++) {
      scratch[k] = (long) LONG_LE.get(payload, baseOff + k * 8);
    }
    final VectorOperators.Comparison cmp = switch (op) {
      case GT -> VectorOperators.GT;
      case LT -> VectorOperators.LT;
      case GE -> VectorOperators.GE;
      case LE -> VectorOperators.LE;
      case EQ -> VectorOperators.EQ;
    };
    // 2) SIMD-eval over scratch. LongVector.fromArray(long[]) is a
    // pure intrinsic — no scope checks, bounds folds into the trip
    // count test.
    int i = 0;
    final int simdEnd = rowCount - (rowCount % LANES);
    for (; i < simdEnd; i += LANES) {
      final LongVector v = LongVector.fromArray(LONG_SPECIES, scratch, i);
      final VectorMask<Long> m = v.compare(cmp, lit);
      final long bits = m.toLong();
      if (bits != 0L) {
        final int wordIdx = i >>> 6;
        final int bitOffset = i & 63;
        // LANES ∈ {2,4,8} divides 64, so bits never straddle two output
        // words — one shift + OR is sufficient.
        out[wordIdx] |= bits << bitOffset;
      }
    }
    // Scalar tail — 0..LANES-1 values remain.
    for (; i < rowCount; i++) {
      final long v = scratch[i];
      final boolean match = switch (op) {
        case GT -> v > lit;
        case LT -> v < lit;
        case GE -> v >= lit;
        case LE -> v <= lit;
        case EQ -> v == lit;
      };
      if (match) out[i >>> 6] |= 1L << (i & 63);
    }
  }

  private static void evalBooleanBytes(final byte[] payload, final int baseOff,
      final int rowCount, final boolean wantTrue, final long[] out) {
    final int stride = (rowCount + 63) >>> 6;
    if (wantTrue) {
      for (int i = 0; i < stride; i++) {
        out[i] = (long) LONG_LE.get(payload, baseOff + i * 8);
      }
    } else {
      for (int i = 0; i < stride; i++) {
        out[i] = ~(long) LONG_LE.get(payload, baseOff + i * 8);
      }
      final int tail = rowCount & 63;
      if (tail != 0) out[stride - 1] &= (1L << tail) - 1L;
    }
  }

  private static void evalStringEqBytes(final byte[] payload, final int baseOff,
      final int rowCount, final byte[] literal, final long[] out) {
    // Dict header: [int dictSize][int[dictSize] lengths][concat bytes][int[rowCount] ids]
    final int dictSize = (int) INT_LE.get(payload, baseOff);
    int concatOff = baseOff + 4 + dictSize * 4;
    int targetDictId = -1;
    for (int i = 0; i < dictSize; i++) {
      final int len = (int) INT_LE.get(payload, baseOff + 4 + i * 4);
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
      if ((int) INT_LE.get(payload, idsOff + i * 4) == targetDictId) {
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
