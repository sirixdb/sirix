package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * GROUP-AGGREGATE kernels over {@link ColumnSlice}s — the column-sliced twins of
 * {@link ProjectionIndexByteScan}'s flat group kernels. The byte kernels consume WHOLE-LEAF
 * payloads, which forces the executor to assemble every leaf (decode + re-serialize of all
 * columns — the cold-start whale); these read only the queried columns' decoded slices.
 *
 * <p>
 * <b>Identity parity is structural, not replicated:</b> group hashes go through the ONE
 * {@link ProjectionIndexByteScan#fnv1a64} both kernel families share, and the payload's dict
 * entry bytes are written from the same decoded DICT segment a slice exposes — both sides hash
 * identical byte content. Aux references use ABSOLUTE leaf indices ({@code (long) leaf << 20 |
 * rowIdx}), exactly what the executor's winner decoders already assume.
 *
 * <p>
 * V1 scope: the NUMERIC single-key flat kernel (conjunctive predicates; COUNT(DISTINCT) lane
 * included; strlen aggregate mode and predicate TREES stay on the whole-leaf path — the caller
 * gates). Missing-key semantics mirror the byte kernel exactly: a row without the group field
 * folds into {@code missingAcc} with a first-seen ordinal stamp; group value {@code 0} takes the
 * table's zero side slot.
 */
public final class ProjectionColumnGroupScan {

  private ProjectionColumnGroupScan() {
  }

  /** Per-thread predicate mask over one leaf (MAX_ROWS bound — same bound the scans use). */
  private static final ThreadLocal<long[]> MASK =
      ThreadLocal.withInitial(() -> new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6]);

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupNumericFlat}. Leaf
   * range {@code [fromLeaf, toLeaf)} in ABSOLUTE store indices; the caller resolves every column
   * ONCE before the parallel fan-out (twenty workers racing the first fill would multiply the
   * I/O) and hands the shared immutable slice arrays in. {@code aggStrlen[a]} marks string-length
   * operands: their slices are STRING_DICT and fold per-dict-entry codepoint counts with
   * fn:string-length's missing-is-0 semantics (null = all-numeric aggregates).
   */
  public static void aggregateByGroupNumericFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final boolean[] aggStrlen, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final long[] missingAcc, final int distinctBlock,
      final Long2ObjectOpenHashMap<LongOpenHashSet> distinctOut, final LongOpenHashSet distinctMissing,
      final long[] budget) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc and aggCols must not be null");
    }
    final long[] mask = MASK.get();
    final DictScratch ds = DICT_SCRATCH.get();
    final int aggCount = aggCols.length;
    if (aggStrlen != null && (ds.strlenCp == null || ds.strlenCp.length < aggCount)) {
      ds.strlenCp = new int[Math.max(4, aggCount)][];
    }
    // Hoisted per leaf — the record accessors and double indirection must stay out of the
    // per-row loop (1M-row group scans pay every load in it).
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    final int[][] aggIds = new int[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines; nothing here is an answer
      }
      final int rowCount = treeOrNull != null
          ? ProjectionColumnScan.evaluateMaskTree(treeOrNull, treeCols, leaf, store.rowCount(leaf), mask)
          : ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice group = groupCol[leaf];
      final long[] groupValues = group.numericValues();
      final long[] groupPresence = group.presenceWords();
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggPresence[a] = agg.presenceWords();
        if (aggStrlen != null && aggStrlen[a]) {
          precomputeStrlen(ds, a, agg);
          aggIds[a] = agg.stringDictIds();
          aggValues[a] = null;
        } else {
          aggValues[a] = agg.numericValues();
          aggIds[a] = null;
        }
      }
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          final long[] slotArr;
          final int base;
          LongOpenHashSet dset = null;
          if ((groupPresWord & 1L << bit) == 0L) {
            slotArr = missingAcc;
            base = 0;
            if (slotArr[0] == 0) {
              slotArr[1] = leafOrdinalBase | rowIdx;
            }
            dset = distinctMissing;
          } else {
            final long gv = groupValues[rowIdx];
            if (gv == 0L) {
              slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
              base = 0;
            } else {
              base = out.acquire(gv, leafOrdinalBase | rowIdx);
              slotArr = out.slotsArray();
            }
            if (distinctBlock >= 0) {
              dset = distinctSetFor(distinctOut, gv);
            }
          }
          foldSliced(slotArr, base, aggValues, aggPresence, aggIds, ds.strlenCp, aggStrlen, aggCount, w, bit, rowIdx,
              distinctBlock, dset, budget);
        }
      }
    }
  }

  /** Per-thread per-leaf dict caches for the string kernel (hash + resolved slot base). */
  private static final class DictScratch {
    long[] hash = new long[64];
    int[] base = new int[64];
    int[][] strlenCp; // per agg block: codepoint count per dict entry (lazy)

    void ensure(final int dictSize) {
      if (hash.length < dictSize) {
        final int n = Math.max(hash.length * 2, dictSize);
        hash = new long[n];
        base = new int[n];
      }
    }
  }

  private static final ThreadLocal<DictScratch> DICT_SCRATCH = ThreadLocal.withInitial(DictScratch::new);

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupStringFlat} (v1:
   * conjunctive predicates only — the caller gates trees to the whole-leaf path). Group identity
   * is the FNV-64 of the dict entry's bytes — the slice's dict entries hold the SAME decoded
   * content the payload's dict region holds, so both kernel families produce identical hashes.
   * The aux lane carries {@code (leaf << 20) | dictId} so the caller materializes winner strings
   * from the group column's slice dict directly.
   *
   * <p>
   * {@code keyRegex} groups on the TRANSFORMED entry (hashed once per dict entry per leaf); a
   * matched row MISSING the key field sets {@code regexDecline[0]} — fn:replace over the empty
   * sequence is {@code ""}, a REAL key the missing-key arm must not absorb. {@code aggStrlen[a]}
   * folds per-dict-entry CODEPOINT counts with fn:string-length's missing-is-0 semantics.
   */
  public static void aggregateByGroupStringFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final boolean[] aggStrlen, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final long[] missingAcc, final int distinctBlock,
      final Long2ObjectOpenHashMap<LongOpenHashSet> distinctOut, final LongOpenHashSet distinctMissing,
      final long[] budget, final Pattern keyRegex, final String keyRegexRepl, final long[] regexDecline) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc and aggCols must not be null");
    }
    final long[] mask = MASK.get();
    final DictScratch ds = DICT_SCRATCH.get();
    final ProjectionIndexByteScan.RegexHashCache regexCache = keyRegex != null
        ? new ProjectionIndexByteScan.RegexHashCache()
        : null;
    final int aggCount = aggCols.length;
    if (aggStrlen != null && (ds.strlenCp == null || ds.strlenCp.length < aggCount)) {
      ds.strlenCp = new int[Math.max(4, aggCount)][];
    }
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    final int[][] aggIds = new int[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines
      }
      final int rowCount = treeOrNull != null
          ? ProjectionColumnScan.evaluateMaskTree(treeOrNull, treeCols, leaf, store.rowCount(leaf), mask)
          : ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice group = groupCol[leaf];
      final byte[][] dict = group.stringDict();
      final int[] ids = group.stringDictIds();
      final long[] groupPresence = group.presenceWords();
      final int dictSize = dict == null
          ? 0
          : dict.length;
      ds.ensure(dictSize);
      final long[] dictHash = ds.hash;
      final int[] dictBase = ds.base;
      for (int i = 0; i < dictSize; i++) {
        dictHash[i] = 0L;
        dictBase[i] = -1; // unresolved for THIS leaf
      }
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggPresence[a] = agg.presenceWords();
        if (aggStrlen != null && aggStrlen[a]) {
          precomputeStrlen(ds, a, agg);
          aggIds[a] = agg.stringDictIds();
          aggValues[a] = null;
        } else {
          aggValues[a] = agg.numericValues();
          aggIds[a] = null;
        }
      }
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          final long[] slotArr;
          final int base;
          LongOpenHashSet dset = null;
          if ((groupPresWord & 1L << bit) == 0L) {
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
          } else {
            final int dictId = ids[rowIdx];
            final long ordinal = leafOrdinalBase | rowIdx;
            int cached = dictBase[dictId];
            long h;
            if (cached >= 0) {
              h = dictHash[dictId];
              // Rehash-safe validation: keys are unique, so a key match IS the group.
              if (out.keyAtSlotBase(cached) != h) {
                cached = -1;
              }
            } else {
              h = 0L;
            }
            if (cached < 0) {
              if (h == 0L) {
                final byte[] entry = dict[dictId];
                if (keyRegex != null) {
                  h = ProjectionIndexByteScan.transformedKeyHash(regexCache, keyRegex, keyRegexRepl, entry, 0,
                      entry.length);
                } else {
                  h = ProjectionIndexByteScan.fnv1a64(entry, 0, entry.length);
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
                foldSliced(zero, 0, aggValues, aggPresence, aggIds, ds.strlenCp, aggStrlen, aggCount, w, bit, rowIdx,
                    distinctBlock, distinctBlock >= 0
                        ? distinctSetFor(distinctOut, 0L)
                        : null,
                    budget);
                continue;
              }
              cached = out.acquire(h, ordinal);
              // Every acquire is followed by a fold, so count 0 means the entry was created
              // just now — stamp the source reference exactly once.
              if (out.slotsArray()[cached] == 0L) {
                out.setAuxAtBase(cached, leafOrdinalBase | dictId);
              }
              dictBase[dictId] = cached;
            }
            if (distinctBlock >= 0) {
              dset = distinctSetFor(distinctOut, h);
            }
            slotArr = out.slotsArray();
            base = cached;
          }
          foldSliced(slotArr, base, aggValues, aggPresence, aggIds, ds.strlenCp, aggStrlen, aggCount, w, bit, rowIdx,
              distinctBlock, dset, budget);
        }
      }
    }
  }

  /**
   * Per-dict-entry CODEPOINT counts for a string-length operand column (UTF-8 codepoints =
   * non-continuation bytes, fn:string-length's codePointCount contract for every plane). Stops at
   * the dict's null-padded tail (codec floor of 16) — no row id references it.
   */
  private static void precomputeStrlen(final DictScratch ds, final int a, final ColumnSlice agg) {
    final byte[][] aDict = agg.stringDict();
    final int aDictSize = aDict == null
        ? 0
        : aDict.length;
    int[] cp = ds.strlenCp[a];
    if (cp == null || cp.length < aDictSize) {
      ds.strlenCp[a] = cp = new int[Math.max(64, aDictSize)];
    }
    for (int i = 0; i < aDictSize; i++) {
      final byte[] e = aDict[i];
      if (e == null) {
        break; // null-padded dict tail (codec floor 16) — no id references it
      }
      int cnt = 0;
      for (final byte b : e) {
        if ((b & 0xC0) != 0x80) {
          cnt++;
        }
      }
      cp[i] = cnt;
    }
  }

  /** One row's aggregate fold over hoisted slice arrays — the byte kernel's foldRow twin. */
  private static void foldSliced(final long[] slotArr, final int base, final long[][] aggValues,
      final long[][] aggPresence, final int[][] aggIds, final int[][] strlenCp, final boolean[] aggStrlen,
      final int aggCount, final int w, final int bit, final int rowIdx, final int distinctBlock,
      final LongOpenHashSet dset, final long[] budget) {
    slotArr[base]++;
    for (int a = 0; a < aggCount; a++) {
      final boolean strlenAgg = aggStrlen != null && aggStrlen[a];
      final boolean present = (aggPresence[a][w] & 1L << bit) != 0L;
      if (!present && !strlenAgg) {
        continue;
      }
      // fn:string-length(()) is 0, never empty: a row MISSING the operand still contributes 0.
      final long v = strlenAgg
          ? (present
              ? strlenCp[a][aggIds[a][rowIdx]]
              : 0L)
          : aggValues[a][rowIdx];
      if (a == distinctBlock) {
        if (dset.add(v) && --budget[0] < 0) {
          budget[1] = 1;
        }
        continue;
      }
      final int aggBase = base + 2 + 4 * a;
      slotArr[aggBase]++;
      // Exact sum or DECLINE — the interpreter promotes an overflowing xs:integer sum.
      slotArr[aggBase + 1] = Math.addExact(slotArr[aggBase + 1], v);
      if (v < slotArr[aggBase + 2]) {
        slotArr[aggBase + 2] = v;
      }
      if (v > slotArr[aggBase + 3]) {
        slotArr[aggBase + 3] = v;
      }
    }
  }

  private static LongOpenHashSet distinctSetFor(final Long2ObjectOpenHashMap<LongOpenHashSet> out, final long key) {
    LongOpenHashSet s = out.get(key);
    if (s == null) {
      s = new LongOpenHashSet();
      out.put(key, s);
    }
    return s;
  }

  /**
   * Descriptor-only twin of {@link ProjectionIndexByteScan#requireGroupSumsFitLong}: bound every
   * summed column's whole-column magnitude by {@code Σ_leaf rowCount × max(|min|,|max|)} from the
   * zone descriptors — ZERO segment I/O. Overflow of the bound throws {@link ArithmeticException},
   * the same decline signal the byte pre-flight uses; a column without zone evidence abandons its
   * pre-flight silently (the per-row {@code Math.addExact} in the kernel stays the authority), and
   * {@code min > max} means an all-missing leaf and is skipped. Kind is asserted: a dict column's
   * zone lanes hold DICT IDS, and bounding on those would be reading id-range as value-range.
   */
  public static void requireGroupSumsFitLong(final ProjectionColumnStore store, final int[] summedCols) {
    final long[] range = new long[2];
    for (final int col : summedCols) {
      if (store.columnKind(col) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        throw new IllegalStateException("summed column " + col + " is not NUMERIC_LONG");
      }
      long bound = 0;
      for (int leaf = 0, n = store.rowGroupCount(); leaf < n; leaf++) {
        if (!store.columnZoneRange(leaf, col, range)) {
          bound = -1; // no zone evidence — abandon this column's pre-flight silently
          break;
        }
        if (range[0] > range[1]) {
          continue; // all-missing leaf
        }
        final long mag = Math.max(Math.absExact(range[0]), Math.absExact(range[1]));
        bound = Math.addExact(bound, Math.multiplyExact((long) store.rowCount(leaf), mag));
      }
    }
  }
  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupCompositeFlat}
   * (v1: conjunctive predicates; the caller gates trees to the whole-leaf path). Components
   * chain leaf-independently into ONE 64-bit identity through the SHARED FNV seed/prime, dict
   * entries pre-hash once per leaf from the slice dict (a substring-cast component hashes the
   * TRANSFORMED integer; {@code Long.MIN_VALUE} marks a raise-case entry — referenced by a row,
   * the serve DECLINES), and the aux lane carries each group's first-seen
   * {@code (leaf << 20) | rowIdx} so winners re-read key parts from slices.
   */
  public static void aggregateByGroupCompositeFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[][] keyCols,
      final byte[] keyKinds, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final int distinctBlock,
      final Long2ObjectOpenHashMap<LongOpenHashSet> distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final int[] keyCondCols, final ColumnSlice[][] condCols,
      final long[] keyCondLits, final byte[][] keyCondElseBytes) {
    if (predicates == null || out == null || aggCols == null || keyCols == null) {
      throw new IllegalArgumentException("predicates, out, aggCols and keyCols must not be null");
    }
    final long[] mask = MASK.get();
    final int keyCount = keyCols.length;
    final int aggCount = aggCols.length;
    final long[][] compDictHash = new long[keyCount][];
    final long[][] compValues = new long[keyCount][];
    final int[][] compIds = new int[keyCount][];
    final long[][] compPresence = new long[keyCount][];
    final long[] condElseHash = keyCondCols != null
        ? new long[keyCount]
        : null;
    final long[][] condValues = keyCondCols != null
        ? new long[2 * keyCount][]
        : null;
    final long[][] condPresence = keyCondCols != null
        ? new long[2 * keyCount][]
        : null;
    if (keyCondCols != null) {
      for (int k = 0; k < keyCount; k++) {
        if (keyCondCols[2 * k] >= 0) {
          condElseHash[k] = ProjectionIndexByteScan.fnv1a64(keyCondElseBytes[k], 0, keyCondElseBytes[k].length);
        }
      }
    }
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines
      }
      if (declineFlag != null && declineFlag[0] != 0) {
        return; // a transform case the interpreter raises on — the caller declines
      }
      final int rowCount = treeOrNull != null
          ? ProjectionColumnScan.evaluateMaskTree(treeOrNull, treeCols, leaf, store.rowCount(leaf), mask)
          : ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      for (int k = 0; k < keyCount; k++) {
        final ColumnSlice slice = keyCols[k][leaf];
        compPresence[k] = slice.presenceWords();
        if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
          compValues[k] = slice.numericValues();
          compIds[k] = null;
        } else {
          final byte[][] dict = slice.stringDict();
          int dictSize = dict == null
              ? 0
              : dict.length;
          while (dictSize > 0 && dict[dictSize - 1] == null) {
            dictSize--; // null-padded dict tail (codec floor of 16)
          }
          long[] hashes = compDictHash[k];
          if (hashes == null || hashes.length < dictSize) {
            hashes = compDictHash[k] = new long[Math.max(64, dictSize)];
          }
          final int subStart = keySubstr != null
              ? keySubstr[2 * k]
              : 0;
          final int subLen = keySubstr != null
              ? keySubstr[2 * k + 1]
              : 0;
          for (int i = 0; i < dictSize; i++) {
            final byte[] entry = dict[i];
            if (subStart > 0) {
              final long tv = ProjectionIndexByteScan.xsIntegerOfSubstring(entry, 0, entry.length, subStart, subLen);
              hashes[i] = tv == Long.MIN_VALUE
                  ? Long.MIN_VALUE
                  : HashCommon.mix(tv);
            } else {
              hashes[i] = ProjectionIndexByteScan.fnv1a64(entry, 0, entry.length);
            }
          }
          compIds[k] = slice.stringDictIds();
          compValues[k] = null;
        }
      }
      if (keyCondCols != null) {
        for (int c2 = 0; c2 < 2 * keyCount; c2++) {
          if (keyCondCols[c2] >= 0) {
            final ColumnSlice cs = condCols[c2][leaf];
            condValues[c2] = cs.numericValues();
            condPresence[c2] = cs.presenceWords();
          }
        }
      }
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggValues[a] = agg.numericValues();
        aggPresence[a] = agg.presenceWords();
      }
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      // NO-TRANSFORM fast loop (the common suite shape): no conditional keys, no substring
      // casts, no shifts — per row each component is one presence test + one load + one mix,
      // no per-row flag re-derivation. Transform-bearing queries take the general loop below
      // (extract BRANCHES, not emissions — the per-row flag checks were 17.8% of cold CPU's
      // biggest frame).
      if (keyCondCols == null && keySubstr == null && keyOffsets == null) {
        for (int w = 0; w < stride; w++) {
          long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
          final int rowBase = w << 6;
          while (word != 0L) {
            final int bit = Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final int rowIdx = rowBase + bit;
            long h = ProjectionIndexByteScan.FNV_SEED;
            for (int k = 0; k < keyCount; k++) {
              final long compHash;
              if ((compPresence[k][w] & 1L << bit) == 0L) {
                compHash = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
              } else if (compValues[k] != null) {
                compHash = HashCommon.mix(compValues[k][rowIdx]);
              } else {
                compHash = compDictHash[k][compIds[k][rowIdx]];
              }
              h = h * ProjectionIndexByteScan.FNV_PRIME ^ compHash;
            }
            final long[] slotArr;
            final int base;
            if (h == 0L) {
              final boolean fresh = !out.hasZeroKey();
              slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
              base = 0;
              if (fresh) {
                out.setZeroAux(leafOrdinalBase | rowIdx);
              }
            } else {
              base = out.acquire(h, leafOrdinalBase | rowIdx);
              slotArr = out.slotsArray();
              if (slotArr[base] == 0L) {
                out.setAuxAtBase(base, leafOrdinalBase | rowIdx);
              }
            }
            foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx,
                distinctBlock, distinctBlock >= 0
                    ? distinctSetFor(distinctOut, h)
                    : null,
                budget);
          }
        }
        continue;
      }
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          long h = ProjectionIndexByteScan.FNV_SEED;
          for (int k = 0; k < keyCount; k++) {
            final boolean subTransformed = keySubstr != null && keySubstr[2 * k] > 0;
            final long compHash;
            if (keyCondCols != null && keyCondCols[2 * k] >= 0) {
              boolean condTrue = true;
              for (int j = 0; j < 2 && condTrue; j++) {
                final int c2 = 2 * k + j;
                if (keyCondCols[c2] < 0) {
                  continue;
                }
                if ((condPresence[c2][w] & 1L << bit) == 0L) {
                  condTrue = false; // missing condition operand: the comparison is false
                } else if (condValues[c2][rowIdx] != keyCondLits[c2]) {
                  condTrue = false;
                }
              }
              if (!condTrue) {
                compHash = condElseHash[k];
              } else if ((compPresence[k][w] & 1L << bit) == 0L) {
                // then-branch over a missing field: empty-sequence key
                compHash = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
              } else {
                compHash = compDictHash[k][compIds[k][rowIdx]];
              }
            } else if ((compPresence[k][w] & 1L << bit) == 0L) {
              if (subTransformed) {
                // xs:integer(substring((), s, l)) = xs:integer("") — the interpreter RAISES.
                declineFlag[0] = 1;
                return;
              }
              // Missing component: a fixed sentinel — part of the identity, not a side group.
              compHash = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
            } else if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
              long v = compValues[k][rowIdx];
              if (keyOffsets != null && keyOffsets[k] != 0L) {
                final long shifted = v + keyOffsets[k];
                if (((v ^ shifted) & (keyOffsets[k] ^ shifted)) < 0) {
                  declineFlag[0] = 1; // overflow: the interpreter promotes to decimal
                  return;
                }
                v = shifted;
              }
              compHash = HashCommon.mix(v);
            } else {
              compHash = compDictHash[k][compIds[k][rowIdx]];
              if (subTransformed && compHash == Long.MIN_VALUE) {
                declineFlag[0] = 1; // a row references a slice the cast raises on
                return;
              }
            }
            h = h * ProjectionIndexByteScan.FNV_PRIME ^ compHash;
          }
          final long[] slotArr;
          final int base;
          if (h == 0L) {
            final boolean fresh = !out.hasZeroKey();
            slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
            base = 0;
            if (fresh) {
              out.setZeroAux(leafOrdinalBase | rowIdx);
            }
          } else {
            base = out.acquire(h, leafOrdinalBase | rowIdx);
            slotArr = out.slotsArray();
            if (slotArr[base] == 0L) {
              out.setAuxAtBase(base, leafOrdinalBase | rowIdx);
            }
          }
          foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx,
              distinctBlock, distinctBlock >= 0
                  ? distinctSetFor(distinctOut, h)
                  : null,
              budget);
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#readRowKeyParts}: materialize ONE winner row's
   * key parts from slices, applying the SAME transforms the kernel grouped by (winners emit the
   * TRANSFORMED value; a raise-case cannot appear — the kernel declined before a winner existed).
   */
  public static void readRowKeyPartsSliced(final ColumnSlice[][] keyCols, final byte[] keyKinds,
      final ColumnSlice[][] condCols, final int leaf, final int rowIdx, final String[] outStrings,
      final long[] outLongs, final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets,
      final int[] keySubstr, final int[] keyCondCols, final long[] keyCondLits, final String[] keyCondElse) {
    for (int k = 0; k < keyCols.length; k++) {
      final ColumnSlice slice = keyCols[k][leaf];
      if (keyCondCols != null && keyCondCols[2 * k] >= 0) {
        boolean condTrue = true;
        for (int j = 0; j < 2 && condTrue; j++) {
          final int c2 = 2 * k + j;
          if (keyCondCols[c2] < 0) {
            continue;
          }
          final ColumnSlice cs = condCols[c2][leaf];
          if ((cs.presenceWords()[rowIdx >>> 6] & 1L << (rowIdx & 63)) == 0L) {
            condTrue = false;
          } else if (cs.numericValues()[rowIdx] != keyCondLits[c2]) {
            condTrue = false;
          }
        }
        if (!condTrue) {
          outPresent[k] = true;
          outIsLong[k] = false;
          outStrings[k] = keyCondElse[k];
          continue;
        }
        // Condition holds: fall through to the plain read below (missing => absent part).
      }
      if ((slice.presenceWords()[rowIdx >>> 6] & 1L << (rowIdx & 63)) == 0L) {
        outPresent[k] = false;
        continue;
      }
      outPresent[k] = true;
      if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        outIsLong[k] = true;
        outLongs[k] = slice.numericValues()[rowIdx] + (keyOffsets != null
            ? keyOffsets[k]
            : 0L);
      } else {
        final byte[] entry = slice.stringDict()[slice.stringDictIds()[rowIdx]];
        if (keySubstr != null && keySubstr[2 * k] > 0) {
          outIsLong[k] = true;
          outLongs[k] =
              ProjectionIndexByteScan.xsIntegerOfSubstring(entry, 0, entry.length, keySubstr[2 * k],
                  keySubstr[2 * k + 1]);
        } else {
          outIsLong[k] = false;
          outStrings[k] = new String(entry, StandardCharsets.UTF_8);
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupPackedSubstringFlat}
   * (v1: conjunctive predicates — trees stay whole-leaf). Groups AND orders on the SHARED
   * {@link ProjectionIndexByteScan#packIsoMinuteSubstring} long (pack-domain parity is
   * structural — both families pack the same decoded entry bytes); aux carries
   * {@code (leaf << 20) | dictId} so winners decode originals from the slice dict. Decline
   * points verbatim: a matching row MISSING the key field (substring of the empty sequence is
   * {@code ""}, a REAL key failing the ISO shape) and a row referencing an entry the pack
   * validator rejected.
   */
  public static void aggregateByGroupPackedSubstringFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf, final int subStart, final int subLen,
      final NumericGroupAggTable out, final long[] declineFlag) {
    if (predicates == null || out == null || aggCols == null || declineFlag == null) {
      throw new IllegalArgumentException("predicates, out, aggCols and declineFlag must not be null");
    }
    final long[] mask = MASK.get();
    final DictScratch ds = DICT_SCRATCH.get();
    final int aggCount = aggCols.length;
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (declineFlag[0] != 0) {
        return;
      }
      final int rowCount = treeOrNull != null
          ? ProjectionColumnScan.evaluateMaskTree(treeOrNull, treeCols, leaf, store.rowCount(leaf), mask)
          : ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice group = groupCol[leaf];
      final byte[][] dict = group.stringDict();
      final int[] ids = group.stringDictIds();
      final long[] groupPresence = group.presenceWords();
      int dictSize = dict == null
          ? 0
          : dict.length;
      while (dictSize > 0 && dict[dictSize - 1] == null) {
        dictSize--; // null-padded dict tail (codec floor of 16)
      }
      ds.ensure(dictSize);
      final long[] dictPacked = ds.hash;
      for (int i = 0; i < dictSize; i++) {
        final byte[] entry = dict[i];
        dictPacked[i] = ProjectionIndexByteScan.packIsoMinuteSubstring(entry, 0, entry.length, subStart, subLen);
      }
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggValues[a] = agg.numericValues();
        aggPresence[a] = agg.presenceWords();
      }
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if ((groupPresWord & 1L << bit) == 0L) {
            // substring((), s, l) is "" — a REAL group key failing the ISO shape: decline,
            // never the null-key group (the interpreter emits "" here, not JSON null).
            declineFlag[0] = 1;
            return;
          }
          final int dictId = ids[rowIdx];
          final long packed = dictPacked[dictId];
          if (packed == Long.MIN_VALUE) {
            declineFlag[0] = 1;
            return;
          }
          final int base = out.acquire(packed, leafOrdinalBase | rowIdx);
          final long[] slotArr = out.slotsArray();
          if (slotArr[base] == 0L) {
            out.setAuxAtBase(base, leafOrdinalBase | dictId);
          }
          foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx, -1, null,
              null);
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#stringAggForWinnerGroups} — deferred string
   * extrema (pass 2): fold min/max over string operand columns for the WINNER groups only,
   * matching rows to winners by the SAME transformed-key hash pass 1 grouped on (a regex key
   * hashes the TRANSFORMED entry — hashing raw would silently mismatch every winner and return
   * all-null extrema). Best-so-far entries are plain {@code byte[]} refs (slices are immutable,
   * store-lifetime); collation authority is the byte kernel's own comparator.
   */
  public static void stringAggForWinnerGroupsSliced(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] stringAggCols, final boolean[] aggIsMin, final long[] winnerHashes,
      final boolean winnerMissingKey, final String[][] bestOut, final Pattern keyRegex, final String keyRegexRepl,
      final int fromLeaf, final int toLeaf) {
    if (predicates == null || stringAggCols == null || aggIsMin == null || winnerHashes == null || bestOut == null) {
      throw new IllegalArgumentException(
          "predicates, stringAggCols, aggIsMin, winnerHashes and bestOut must not be null");
    }
    final long[] mask = MASK.get();
    final ProjectionIndexByteScan.RegexHashCache regexCache = keyRegex != null
        ? new ProjectionIndexByteScan.RegexHashCache()
        : null;
    final int aggCount = stringAggCols.length;
    final int slots = winnerHashes.length + (winnerMissingKey
        ? 1
        : 0);
    final byte[][][] best = new byte[aggCount][slots][];
    // OWN winner-slot memo (sentinel -2 = unresolved) — DictScratch.base uses -1 and sharing
    // two sentinel conventions in one array is a foot-gun.
    int[] winnerSlotOfDict = new int[64];
    final byte[][][] aggDicts = new byte[aggCount][][];
    final int[][] aggIds = new int[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = treeOrNull != null
          ? ProjectionColumnScan.evaluateMaskTree(treeOrNull, treeCols, leaf, store.rowCount(leaf), mask)
          : ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice group = groupCol[leaf];
      final byte[][] dict = group.stringDict();
      final int[] ids = group.stringDictIds();
      final long[] groupPresence = group.presenceWords();
      final int dictSize = dict == null
          ? 0
          : dict.length;
      if (winnerSlotOfDict.length < dictSize) {
        winnerSlotOfDict = new int[Math.max(winnerSlotOfDict.length * 2, dictSize)];
      }
      Arrays.fill(winnerSlotOfDict, 0, dictSize, -2);
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = stringAggCols[a][leaf];
        aggDicts[a] = agg.stringDict();
        aggIds[a] = agg.stringDictIds();
        aggPresence[a] = agg.presenceWords();
      }
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          int slot;
          if ((groupPresWord & 1L << bit) == 0L) {
            if (!winnerMissingKey) {
              continue;
            }
            slot = winnerHashes.length;
          } else {
            final int dictId = ids[rowIdx];
            slot = winnerSlotOfDict[dictId];
            if (slot == -2) {
              final byte[] entry = dict[dictId];
              final long h = keyRegex != null
                  ? ProjectionIndexByteScan.transformedKeyHash(regexCache, keyRegex, keyRegexRepl, entry, 0,
                      entry.length)
                  : ProjectionIndexByteScan.fnv1a64(entry, 0, entry.length);
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
            if ((aggPresence[a][w] & 1L << bit) == 0L) {
              continue; // operand missing on this row — contributes nothing
            }
            final byte[] entry = aggDicts[a][aggIds[a][rowIdx]];
            final byte[] cur = best[a][slot];
            if (cur == null) {
              best[a][slot] = entry;
              continue;
            }
            if (cur == entry) {
              continue; // the group's best IS this dict entry (same leaf + id = same array)
            }
            final int cmp = ProjectionIndexByteScan.compareStrSlices(entry, 0, entry.length, cur, 0, cur.length);
            if (aggIsMin[a]
                ? cmp < 0
                : cmp > 0) {
              best[a][slot] = entry;
            }
          }
        }
      }
    }
    for (int a = 0; a < aggCount; a++) {
      for (int sl = 0; sl < slots; sl++) {
        bestOut[a][sl] = best[a][sl] == null
            ? null
            : new String(best[a][sl], StandardCharsets.UTF_8);
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateAllNumeric}: fold every
   * matching row into ONE accumulator block ({@code newGroupAggAcc} layout, ordinal lane
   * unused) — the single-group kernel behind {@code group by <constant>}. No group column is
   * read at all; the caller merges per-worker accumulators.
   */
  public static void aggregateAllNumericFlat(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSlice[][] predCols, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final long[] acc) {
    if (predicates == null || acc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, acc and aggCols must not be null");
    }
    final long[] mask = MASK.get();
    final int aggCount = aggCols.length;
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggValues[a] = agg.numericValues();
        aggPresence[a] = agg.presenceWords();
      }
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          foldSliced(acc, 0, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowBase + bit, -1, null,
              null);
        }
      }
    }
  }

}
