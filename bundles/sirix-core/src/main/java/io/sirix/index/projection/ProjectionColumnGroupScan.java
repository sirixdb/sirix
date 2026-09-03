package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * GROUP-AGGREGATE kernels over {@link ColumnSlice}s — the column-sliced twins of
 * {@link ProjectionIndexByteScan}'s flat group kernels. The byte kernels consume WHOLE-LEAF
 * payloads, which forces the executor to assemble every leaf (decode + re-serialize of all columns
 * — the cold-start whale); these read only the queried columns' decoded slices.
 *
 * <p>
 * <b>Identity parity is structural, not replicated:</b> group hashes go through the ONE
 * {@link ProjectionIndexByteScan#fnv1a64} both kernel families share, and the payload's dict entry
 * bytes are written from the same decoded DICT segment a slice exposes — both sides hash identical
 * byte content. Aux references use ABSOLUTE leaf indices ({@code (long) leaf << 20 |
 * rowIdx}), exactly what the executor's winner decoders already assume.
 *
 * <p>
 * V1 scope: the NUMERIC single-key flat kernel (conjunctive predicates; COUNT(DISTINCT) lane
 * included; strlen aggregate mode and predicate TREES stay on the whole-leaf path — the caller
 * gates). Missing-key semantics mirror the byte kernel exactly: a row without the group field folds
 * into {@code missingAcc} with a first-seen ordinal stamp; group value {@code 0} takes the table's
 * zero side slot.
 */
public final class ProjectionColumnGroupScan {

  private ProjectionColumnGroupScan() {}

  /** Per-thread predicate mask over one leaf (MAX_ROWS bound — same bound the scans use). */
  private static final ThreadLocal<long[]> MASK =
      ThreadLocal.withInitial(() -> new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6]);

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupNumericFlat}. Leaf range
   * {@code [fromLeaf, toLeaf)} in ABSOLUTE store indices; the caller resolves every column ONCE
   * before the parallel fan-out (twenty workers racing the first fill would multiply the I/O) and
   * hands the shared immutable slice arrays in. {@code stringLengthModes[a]} selects codepoint or
   * UTF-8-byte length over STRING_DICT operands (null = all-numeric aggregates); a STRING_GLOBAL
   * length operand instead supplies {@code globalLengthTables[a]}, the per-query id → length table the
   * fold indexes with the row's id lane — no dictionary bytes are touched per leaf (the same table the
   * whole-leaf twin and the composite arm consume). {@code cdStringDict} marks the distinct block's
   * operand as STRING_DICT — see {@link #foldSliced} for the leaf-local-id → content-hash identity it
   * feeds the set.
   */
  public static void aggregateByGroupNumericFlat(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSlice[][] predCols, final ProjectionIndexScan.PredicateTree treeOrNull,
      final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol, final ColumnSlice[][] aggCols,
      final byte[] stringLengthModes, final int fromLeaf, final int toLeaf, final NumericGroupAggTable out,
      final long[] missingAcc, final int distinctBlock, final GroupDistinctAccumulator.Worker distinctOut,
      final GroupDistinctAccumulator.Sink distinctMissing, final long[] budget, final boolean cdStringDict,
      final int[][] globalLengthTables) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc and aggCols must not be null");
    }
    if (cdStringDict && distinctBlock < 0) {
      throw new IllegalArgumentException("cdStringDict without a distinct block");
    }
    if (globalLengthTables != null && (stringLengthModes == null || globalLengthTables.length < aggCols.length)) {
      throw new IllegalArgumentException("globalLengthTables needs a string-length mode per aggregate");
    }
    final long[] mask = MASK.get();
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query
    // that asks only for min/max/count can never decline on an overflow no answer depends
    // on — see NumericGroupAggTable#sumsExact for the rule the merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final DictScratch ds = DICT_SCRATCH.get();
    final int aggCount = aggCols.length;
    ProjectionIndexByteScan.validateStringLengthModes(stringLengthModes, aggCount);
    // COUNT-ONLY: no aggregate lanes and no distinct set, so a row is one increment into a
    // [key, count, firstSeen] stripe. Decided ONCE, never per row.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    if (stringLengthModes != null && (ds.stringLengths == null || ds.stringLengths.length < aggCount)) {
      ds.stringLengths = new int[Math.max(4, aggCount)][];
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
      byte[] cdDictBytes = null;
      int[] cdDictOffsets = null;
      long[] cdHash = null;
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggPresence[a] = agg.presenceWords();
        if (stringLengthModes != null && stringLengthModes[a] != ProjectionIndexByteScan.STRING_LENGTH_NONE) {
          if (globalLengthTables != null && globalLengthTables[a] != null) {
            // GLOBAL operand: the per-query id→length table replaces the per-leaf entry pass —
            // the fold reads table[(int) idLane[row]] and no dictionary bytes are touched.
            aggValues[a] = agg.numericValues();
            aggIds[a] = null;
          } else {
            precomputeStringLengths(ds, a, agg, stringLengthModes[a]);
            aggIds[a] = agg.stringDictIds();
            aggValues[a] = null;
          }
        } else if (cdStringDict && a == distinctBlock) {
          cdHash = cdHashesFor(ds, agg);
          cdDictBytes = agg.dictHashes() != null
              ? null
              : agg.dictBytes();
          cdDictOffsets = agg.dictHashes() != null
              ? null
              : agg.dictOffsets();
          aggIds[a] = agg.stringDictIds();
          aggValues[a] = null;
        } else {
          aggValues[a] = agg.numericValues();
          aggIds[a] = null;
        }
      }
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      if (countOnly) {
        for (int w = 0; w < stride; w++) {
          long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
          final long groupPresWord = groupPresence[w];
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
              final long gv = groupValues[rowIdx];
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
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
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
            final long gv = groupValues[rowIdx];
            if (gv == 0L) {
              slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
              base = 0;
            } else {
              final int handle = out.acquire(gv, leafOrdinalBase | rowIdx);
              slotArr = out.storageAtAccBase(handle);
              base = out.offsetAtAccBase(handle);
            }
            if (distinctBlock >= 0) {
              dset = distinctOut.sinkFor(gv);
            }
          }
          foldSliced(slotArr, base, aggValues, aggPresence, aggIds, ds.stringLengths, stringLengthModes, aggCount, w,
              bit, rowIdx, distinctBlock, dset, budget, cdDictBytes, cdDictOffsets, cdHash, null, null, sumExactMask,
              globalLengthTables);
        }
      }
    }
  }

  /**
   * {@link #aggregateByGroupNumericFlat} over a DENSE GLOBAL id key: same predicate evaluation, same
   * missing-key semantics, same accumulator layout — but the group's block is addressed by the id
   * instead of probed for, and every worker folds into the ONE shared
   * {@link DenseGlobalGroupAggTable} rather than into a private table the executor then has to merge.
   *
   * <p>
   * Scope is deliberately the shape the dense table pays off on: numeric aggregates over a
   * global-dictionary group key, no COUNT(DISTINCT) block and no string-length operand (both need
   * per-leaf dictionary state that the hash arm already carries). The MISSING-key accumulator stays
   * thread-confined and merges exactly as before — one accumulator per worker, no atomics on it.
   *
   * @param out the shared dense table, fully initialized ({@link DenseGlobalGroupAggTable#initIds})
   * @param missingAcc this worker's accumulator for rows whose group field is absent
   * @param roster this worker's created-id list, which the selection walks instead of the id space
   * @param decline set to {@code 1} when a row carries an id the table was not sized for
   */
  public static void aggregateByGroupNumericDense(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSlice[][] predCols, final ProjectionIndexScan.PredicateTree treeOrNull,
      final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol, final ColumnSlice[][] aggCols, final int fromLeaf,
      final int toLeaf, final DenseGlobalGroupAggTable out, final long[] missingAcc,
      final DenseGlobalGroupAggTable.IdRoster roster, final long[] decline) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null || roster == null
        || decline == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc, aggCols, roster and decline must not be null");
    }
    final long[] mask = MASK.get();
    final long sumExactMask = out.sumExactMask();
    final int aggCount = aggCols.length;
    // Hoisted per leaf, exactly as the hash kernel hoists them: the record accessors and the double
    // indirection must stay out of the per-row loop.
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (decline[0] != 0L) {
        return; // an id outside the sized range — the caller declines; nothing here is an answer
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
        aggValues[a] = agg.numericValues();
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
            if (missingAcc[0] == 0) {
              missingAcc[1] = leafOrdinalBase | rowIdx;
            }
            foldNumericPlain(missingAcc, 0, aggValues, aggPresence, aggCount, w, bit, rowIdx, sumExactMask);
          } else {
            out.fold(groupValues[rowIdx], leafOrdinalBase | rowIdx, aggValues, aggPresence, aggCount, w, bit, rowIdx,
                roster, decline);
          }
        }
      }
    }
  }

  /**
   * {@link #foldSliced} restricted to plain numeric aggregate lanes — the shape the missing-key
   * accumulator of the dense arm needs, where no distinct set, no string-length operand and no
   * dictionary state can occur. Kept beside its atomic twin ({@link DenseGlobalGroupAggTable#fold})
   * so the lane order of the two is read together.
   */
  private static void foldNumericPlain(final long[] acc, final int base, final long[][] aggValues,
      final long[][] aggPresence, final int aggCount, final int w, final int bit, final int rowIdx,
      final long sumExactMask) {
    acc[base]++;
    for (int a = 0; a < aggCount; a++) {
      if ((aggPresence[a][w] & 1L << bit) == 0L) {
        continue;
      }
      final long v = aggValues[a][rowIdx];
      final int aggBase = base + 2 + 4 * a;
      acc[aggBase]++;
      if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
        acc[aggBase + 1] = Math.addExact(acc[aggBase + 1], v);
      }
      if (v < acc[aggBase + 2]) {
        acc[aggBase + 2] = v;
      }
      if (v > acc[aggBase + 3]) {
        acc[aggBase + 3] = v;
      }
    }
  }

  /**
   * Zero the per-leaf hash memo for a STRING_DICT count-distinct operand and hand it back: one
   * {@code long} per dict entry, hashed lazily on first reference by {@link #foldSliced}.
   */
  /**
   * The distinct operand's {@code dictId -> content hash} table for ONE leaf.
   *
   * <p>
   * A slice filled in DISTINCT-IDENTITY mode carries the table already — read straight off the leaf's
   * {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_DICT_HASHES} segment, so no dictionary byte was
   * fetched, decoded or hashed for it. Its array is SHARED and must stay read-only, which is what the
   * {@code cdDictBytes == null} guard in {@link #foldSliced} enforces: with no bytes to hash from,
   * there is nothing to memoize back. Otherwise the per-thread scratch carries the same hashes,
   * filled lazily per referenced entry.
   */
  private static long[] cdHashesFor(final DictScratch ds, final ColumnSlice agg) {
    final long[] precomputed = agg.dictHashes();
    return precomputed != null
        ? precomputed
        : resetCdHashes(ds, agg.dictSize());
  }

  private static long[] resetCdHashes(final DictScratch ds, final int dictSize) {
    final long[] cdHash = ds.ensureCd(dictSize);
    Arrays.fill(cdHash, 0, dictSize, 0L);
    return cdHash;
  }

  /** Per-thread per-leaf dict caches for the string kernel (hash + resolved slot base). */
  private static final class DictScratch {
    long[] hash = new long[64];
    int[] base = new int[64];
    int[][] stringLengths; // per aggregate block: transformed length per dictionary entry (lazy)
    /** COUNT(DISTINCT) over a STRING_DICT operand: per-leaf dictId → content hash (0 = unhashed). */
    long[] cdHash = new long[64];

    void ensure(final int dictSize) {
      if (hash.length < dictSize) {
        final int n = Math.max(hash.length * 2, dictSize);
        hash = new long[n];
        base = new int[n];
      }
    }

    long[] ensureCd(final int dictSize) {
      if (cdHash.length < dictSize) {
        cdHash = new long[Math.max(cdHash.length * 2, dictSize)];
      }
      return cdHash;
    }
  }

  private static final ThreadLocal<DictScratch> DICT_SCRATCH = ThreadLocal.withInitial(DictScratch::new);

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupStringFlat} (v1:
   * conjunctive predicates only — the caller gates trees to the whole-leaf path). Group identity is
   * the FNV-64 of the dict entry's bytes — the slice's dict entries hold the SAME decoded content the
   * payload's dict region holds, so both kernel families produce identical hashes. The aux lane
   * carries {@code (leaf << 20) | dictId} so the caller materializes winner strings from the group
   * column's slice dict directly.
   *
   * <p>
   * {@code keyRegex} groups on the TRANSFORMED entry (hashed once per dict entry per leaf); a matched
   * row MISSING the key field sets {@code regexDecline[0]} — fn:replace over the empty sequence is
   * {@code ""}, a REAL key the missing-key arm must not absorb. {@code stringLengthModes[a]} selects
   * codepoint or UTF-8-byte counts with missing-is-zero semantics. {@code cdStringDict} marks the
   * distinct block's operand as STRING_DICT — see {@link #foldSliced} for the leaf-local-id →
   * content-hash identity it feeds the set.
   */
  public static void aggregateByGroupStringFlat(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSlice[][] predCols, final ProjectionIndexScan.PredicateTree treeOrNull,
      final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol, final ColumnSlice[][] aggCols,
      final byte[] stringLengthModes, final int fromLeaf, final int toLeaf, final NumericGroupAggTable out,
      final long[] missingAcc, final int distinctBlock, final GroupDistinctAccumulator.Worker distinctOut,
      final GroupDistinctAccumulator.Sink distinctMissing, final long[] budget, final boolean cdStringDict, final Pattern keyRegex,
      final String keyRegexRepl, final long[] regexDecline, final GroupDistinctBitmaps distinctBitmaps,
      final int[][] globalLengthTables, final long[] globalKeyHashes) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc and aggCols must not be null");
    }
    if (cdStringDict && distinctBlock < 0) {
      throw new IllegalArgumentException("cdStringDict without a distinct block");
    }
    final long[] mask = MASK.get();
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query
    // that asks only for min/max/count can never decline on an overflow no answer depends
    // on — see NumericGroupAggTable#sumsExact for the rule the merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final DictScratch ds = DICT_SCRATCH.get();
    // Thread-confined: resolving a group's shared bitmap is a per-(worker, group) cost, so the
    // per-row path stays a fastutil lookup on a primitive key and never boxes.
    final Long2ObjectOpenHashMap<long[]> localWords = distinctBitmaps == null
        ? null
        : new Long2ObjectOpenHashMap<>();
    final ProjectionIndexByteScan.RegexHashCache regexCache = keyRegex != null
        ? new ProjectionIndexByteScan.RegexHashCache()
        : null;
    // GLOBAL group key (regex-transformed): the slice's long lane holds resource-wide ids and the
    // caller precomputed the transformed-key hash PER ID in one sequential sweep — the same table
    // and hash domain the whole-leaf kernel and the winner rebuild consume, so the arms' groups
    // match exactly. No per-leaf dictionary exists and none is read.
    final boolean globalGroup = globalKeyHashes != null;
    final int aggCount = aggCols.length;
    ProjectionIndexByteScan.validateStringLengthModes(stringLengthModes, aggCount);
    // COUNT-ONLY: no aggregate lanes and no distinct set, so the fold is one increment into a
    // [key, count, firstSeen, aux] stripe. Loop-invariant — the dict cache below is the group
    // identity and stays in both shapes.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    if (stringLengthModes != null && (ds.stringLengths == null || ds.stringLengths.length < aggCount)) {
      ds.stringLengths = new int[Math.max(4, aggCount)][];
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
      final long[] globalGids = globalGroup
          ? group.numericValues()
          : null;
      if (globalGroup && globalGids == null) {
        throw new IllegalStateException("global regex group column has no id lane in leaf " + leaf);
      }
      final byte[] dictBytes = globalGroup
          ? null
          : group.dictBytes();
      final int[] dictOffsets = globalGroup
          ? null
          : group.dictOffsets();
      final int[] ids = globalGroup
          ? null
          : group.stringDictIds();
      final long[] groupPresence = group.presenceWords();
      final int dictSize = globalGroup
          ? 0
          : group.dictSize();
      ds.ensure(dictSize);
      final long[] dictHash = ds.hash;
      final int[] dictBase = ds.base;
      for (int i = 0; i < dictSize; i++) {
        dictHash[i] = 0L;
        dictBase[i] = -1; // unresolved for THIS leaf
      }
      byte[] cdDictBytes = null;
      int[] cdDictOffsets = null;
      long[] cdHash = null;
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        aggPresence[a] = agg.presenceWords();
        if (stringLengthModes != null && stringLengthModes[a] != ProjectionIndexByteScan.STRING_LENGTH_NONE) {
          if (globalLengthTables != null && globalLengthTables[a] != null) {
            // GLOBAL operand: the per-query id→length table replaces the per-leaf entry pass —
            // the fold reads table[(int) idLane[row]] and no dictionary bytes are touched.
            aggValues[a] = agg.numericValues();
            aggIds[a] = null;
          } else {
            precomputeStringLengths(ds, a, agg, stringLengthModes[a]);
            aggIds[a] = agg.stringDictIds();
            aggValues[a] = null;
          }
        } else if (cdStringDict && a == distinctBlock) {
          // The operand's own dict — a SEPARATE memo from the group key's (ds.hash), because the
          // distinct column may well BE the group column.
          cdHash = cdHashesFor(ds, agg);
          cdDictBytes = agg.dictHashes() != null
              ? null
              : agg.dictBytes();
          cdDictOffsets = agg.dictHashes() != null
              ? null
              : agg.dictOffsets();
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
          GroupDistinctAccumulator.Sink dset = null;
          long[] dwords = null;
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
            if (localWords != null) {
              // The missing-key group is a real group with a real distinct count; give it the same
              // bitmap treatment under a reserved key no dictionary id can collide with.
              dwords = wordsFor(localWords, distinctBitmaps, Long.MIN_VALUE, budget);
              if (dwords == null) {
                return;
              }
            }
          } else if (globalGroup) {
            final long gid = globalGids[rowIdx];
            final long ordinal = leafOrdinalBase | rowIdx;
            // One array load per row; the per-leaf dict memo does not apply (ids are resource-wide
            // and the table IS the memo). Hash 0 = the transformed key hashed to the empty-bucket
            // sentinel: zero side slot, exactly the whole-leaf kernel's contract.
            final long h = globalKeyHashes[(int) gid];
            if (h == 0L) {
              final boolean fresh = !out.hasZeroKey();
              final long[] zero = out.acquireZero(ordinal);
              if (fresh) {
                out.setZeroAux(gid);
              }
              if (countOnly) {
                zero[0]++;
              } else {
                long[] zeroWords = null;
                if (distinctBlock >= 0 && localWords != null) {
                  zeroWords = wordsFor(localWords, distinctBitmaps, 0L, budget);
                  if (zeroWords == null) {
                    return;
                  }
                }
                foldSliced(zero, 0, aggValues, aggPresence, aggIds, ds.stringLengths, stringLengthModes, aggCount, w,
                    bit, rowIdx, distinctBlock, distinctBlock >= 0 && localWords == null
                        ? distinctOut.sinkFor(0L)
                        : null,
                    budget, cdDictBytes, cdDictOffsets, cdHash, distinctBitmaps, zeroWords, sumExactMask,
                    globalLengthTables);
              }
              continue;
            }
            final int cached = out.acquire(h, ordinal);
            final long[] cachedStorage = out.storageAtAccBase(cached);
            final int cachedOffset = out.offsetAtAccBase(cached);
            if (cachedStorage[cachedOffset] == 0L) {
              // Aux carries the GLOBAL ID itself — resource-wide, so no (leaf, dictId) packing;
              // the executor's winner materialization resolves it through the dictionary.
              out.setAuxAtAccBase(cached, gid);
            }
            if (distinctBlock >= 0) {
              if (localWords != null) {
                dwords = wordsFor(localWords, distinctBitmaps, h, budget);
                if (dwords == null) {
                  return;
                }
              } else {
                dset = distinctOut.sinkFor(h);
              }
            }
            slotArr = cachedStorage;
            base = cachedOffset;
          } else {
            final int dictId = ids[rowIdx];
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
                final int off = dictOffsets[dictId];
                final int len = dictOffsets[dictId + 1] - off;
                if (keyRegex != null) {
                  h = ProjectionIndexByteScan.transformedKeyHash(regexCache, keyRegex, keyRegexRepl, dictBytes, off,
                      len);
                } else {
                  h = ProjectionIndexByteScan.fnv1a64(dictBytes, off, len);
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
                } else {
                  long[] zeroWords = null;
                  if (distinctBlock >= 0 && localWords != null) {
                    zeroWords = wordsFor(localWords, distinctBitmaps, 0L, budget);
                    if (zeroWords == null) {
                      return;
                    }
                  }
                  foldSliced(zero, 0, aggValues, aggPresence, aggIds, ds.stringLengths, stringLengthModes, aggCount, w,
                      bit, rowIdx, distinctBlock, distinctBlock >= 0 && localWords == null
                          ? distinctOut.sinkFor(0L)
                          : null,
                      budget, cdDictBytes, cdDictOffsets, cdHash, distinctBitmaps, zeroWords, sumExactMask, globalLengthTables);
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
              if (localWords != null) {
                dwords = wordsFor(localWords, distinctBitmaps, h, budget);
                if (dwords == null) {
                  return;
                }
              } else {
                dset = distinctOut.sinkFor(h);
              }
            }
            slotArr = out.storageAtAccBase(cached);
            base = out.offsetAtAccBase(cached);
          }
          if (countOnly) {
            slotArr[base]++;
          } else {
            foldSliced(slotArr, base, aggValues, aggPresence, aggIds, ds.stringLengths, stringLengthModes, aggCount, w,
                bit, rowIdx, distinctBlock, dset, budget, cdDictBytes, cdDictOffsets, cdHash, distinctBitmaps, dwords,
                sumExactMask, globalLengthTables);
          }
        }
      }
    }
  }

  /** Precompute codepoint or UTF-8-byte counts for one string aggregate's dictionary. */
  private static void precomputeStringLengths(final DictScratch ds, final int a, final ColumnSlice agg,
      final byte lengthMode) {
    final byte[] aDictBytes = agg.dictBytes();
    final int[] aDictOffsets = agg.dictOffsets();
    final int aDictSize = agg.dictSize();
    int[] lengths = ds.stringLengths[a];
    if (lengths == null || lengths.length < aDictSize) {
      ds.stringLengths[a] = lengths = new int[Math.max(64, aDictSize)];
    }
    for (int i = 0; i < aDictSize; i++) {
      final int start = aDictOffsets[i];
      final int end = aDictOffsets[i + 1];
      int result = end - start;
      if (lengthMode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS) {
        result = 0;
        for (int b = start; b < end; b++) {
          if ((aDictBytes[b] & 0xC0) != 0x80) {
            result++;
          }
        }
      }
      lengths[i] = result;
    }
  }

  /**
   * One row's aggregate fold over hoisted slice arrays — the byte kernel's foldRow twin.
   *
   * <p>
   * {@code cdHash != null} marks the distinct block's operand as STRING_DICT: dict ids are
   * LEAF-LOCAL, so the set member is the entry's 64-bit content hash. Either the leaf's DICT_HASHES
   * segment supplied the whole table ({@code cdDictBytes == null} — read-only, nothing to compute),
   * or it is built once per referenced entry per leaf into a per-thread scratch ({@code 0} = not yet
   * hashed; an entry whose hash IS 0 re-hashes per row, correct and vanishingly rare). Both fill it
   * with the same function over the same bytes, so the two are interchangeable within one column.
   * Identity is exact up to a 64-bit hash collision — the SAME standard the composite group-key
   * identity already accepts.
   */
  private static void foldSliced(final long[] slotArr, final int base, final long[][] aggValues,
      final long[][] aggPresence, final int[][] aggIds, final int[][] stringLengths, final byte[] stringLengthModes,
      final int aggCount, final int w, final int bit, final int rowIdx, final int distinctBlock,
      final GroupDistinctAccumulator.Sink dset, final long[] budget, final byte[] cdDictBytes, final int[] cdDictOffsets,
      final long[] cdHash, final GroupDistinctBitmaps bitmaps, final long[] dwords, final long sumExactMask,
      final int[][] globalLengthTables) {
    slotArr[base]++;
    for (int a = 0; a < aggCount; a++) {
      final boolean stringLengthAgg =
          stringLengthModes != null && stringLengthModes[a] != ProjectionIndexByteScan.STRING_LENGTH_NONE;
      final boolean present = (aggPresence[a][w] & 1L << bit) != 0L;
      if (!present && !stringLengthAgg) {
        continue;
      }
      // fn:string-length(()) is 0, never empty: a row MISSING the operand still contributes 0.
      final long v;
      if (stringLengthAgg) {
        // A GLOBAL operand's lengths live in the per-query id table, indexed by the row's id lane;
        // a per-leaf dict operand's in the precomputed per-entry pass, indexed by its dict id.
        v = !present
            ? 0L
            : globalLengthTables != null && globalLengthTables[a] != null
                ? globalLengthTables[a][(int) aggValues[a][rowIdx]]
                : stringLengths[a][aggIds[a][rowIdx]];
      } else if (cdHash != null && a == distinctBlock) {
        final int cdId = aggIds[a][rowIdx];
        long h = cdHash[cdId];
        // cdDictBytes == null marks a PRECOMPUTED table (the leaf's DICT_HASHES segment): the value
        // read is already the answer, and the array is shared across workers — never written here.
        if (h == 0L && cdDictBytes != null) {
          final int off = cdDictOffsets[cdId];
          h = ProjectionIndexByteScan.fnv1a64(cdDictBytes, off, cdDictOffsets[cdId + 1] - off);
          cdHash[cdId] = h;
        }
        v = h;
      } else {
        v = aggValues[a][rowIdx];
      }
      if (a == distinctBlock) {
        // Dense global ids go to the shared per-group bitmap, which is exact at any cardinality and
        // therefore spends no budget; everything else keeps the per-worker exact set and its ceiling.
        if (dwords != null) {
          if (!bitmaps.set(dwords, v)) {
            budget[1] = 1; // id outside the sized range — decline; a dropped id is a low count
          }
        } else dset.add(v); // exact and bounded inside the shared accumulator; its overrun declines the arm
        continue;
      }
      final int aggBase = base + 2 + 4 * a;
      slotArr[aggBase]++;
      // Exact sum or DECLINE — the interpreter promotes an overflowing xs:integer sum. A lane no
      // sum/avg reads is not folded at all: declining a min-only query over an unread sum would
      // refuse an answer the accumulator's fixed [count, sum, min, max] shape never needed.
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
   * The shared bitmap for {@code groupKey}, resolved through the worker's thread-confined cache so
   * the per-row path never consults the concurrent map. A {@code null} return means the bitmap budget
   * is spent and the caller must decline — signalled through the same {@code budget[1]} flag the
   * exact sets use, so both ceilings surface to the executor as one decline.
   */
  private static long[] wordsFor(final Long2ObjectOpenHashMap<long[]> local, final GroupDistinctBitmaps shared,
      final long groupKey, final long[] budget) {
    long[] words = local.get(groupKey);
    if (words == null) {
      words = shared.acquire(groupKey);
      if (words == null) {
        budget[1] = 1;
        return null;
      }
      local.put(groupKey, words);
    }
    return words;
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
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupCompositeFlat} (v1:
   * conjunctive predicates; the caller gates trees to the whole-leaf path). Components chain
   * leaf-independently into ONE 64-bit identity through the SHARED FNV seed/prime, dict entries
   * pre-hash once per leaf from the slice dict (a substring-cast component hashes the TRANSFORMED
   * integer; {@code Long.MIN_VALUE} marks a raise-case entry — referenced by a row, the serve
   * DECLINES), and the aux lane carries each group's first-seen {@code (leaf << 20) | rowIdx} so
   * winners re-read key parts from slices.
   */
  /**
   * Identity written under a set presence-mask bit for a component with no value at all. The mask
   * bit is what makes it unambiguous — a real value may encode to any bit pattern, but never with
   * its mask bit set.
   */
  private static final long MISSING_COMPONENT_IDENTITY = 0L;

  /** Identity for a conditional else branch that carries no substitution literal. */
  private static final long ABSENT_ELSE_LITERAL_IDENTITY = 1L;

  public static void aggregateByGroupCompositeFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[][] keyCols,
      final byte[] keyKinds, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final int[] keyCondCols, final ColumnSlice[][] condCols,
      final long[] keyCondLits, final byte[][] keyCondElseBytes, final long[] keyDivMod) {
    aggregateByGroupCompositeFlat(store, predicates, predCols, treeOrNull, treeCols, keyCols, keyKinds, aggCols,
        fromLeaf, toLeaf, out, distinctBlock, distinctOut, budget, keyOffsets, keySubstr, declineFlag, keyCondCols,
        condCols, keyCondLits, keyCondElseBytes, keyDivMod, null);
  }

  /** Global-string substring-cast capable final overload; views align to key components. */
  public static void aggregateByGroupCompositeFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[][] keyCols,
      final byte[] keyKinds, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final int[] keyCondCols, final ColumnSlice[][] condCols,
      final long[] keyCondLits, final byte[][] keyCondElseBytes, final long[] keyDivMod,
      final GlobalValueDictionary.ReadView[] globalKeyViews) {
    aggregateByGroupCompositeFlat(store, predicates, predCols, treeOrNull, treeCols, keyCols, keyKinds, aggCols,
        fromLeaf, toLeaf, out, distinctBlock, distinctOut, budget, keyOffsets, keySubstr, declineFlag, keyCondCols,
        condCols, keyCondLits, keyCondElseBytes, keyDivMod, globalKeyViews, null, null);
  }

  /**
   * Final overload, additionally PROVING that per-leaf dictionary string components are identified
   * exactly.
   *
   * <p>
   * The identity lanes of a string component are a fingerprint pair, which discriminates but does
   * not identify: two distinct strings sharing both fingerprints would produce equal lanes, so
   * {@link NumericGroupAggTable#acquireExact} would fold them and never report a probe-key
   * collision. {@code identityRegistry} closes that hole by comparing canonical BYTES for every
   * fingerprint it has seen before — in this per-leaf dictionary pass, where the bytes are already
   * in hand and already in cache from hashing them, never per row. A scan whose registry cannot
   * prove identity returns early and the caller declines.
   *
   * @param identityRegistry shared across this scan's workers, or {@code null} when every component
   *        is numeric or substring-cast and therefore already exact in one lane
   * @param globalCondElseIds per-component RESOLVED global ids of conditional else literals
   *        ({@link Long#MIN_VALUE} = not a global conditional component), or {@code null} when no
   *        global component carries one — mirrors the whole-leaf kernel's contract exactly
   */
  public static void aggregateByGroupCompositeFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[][] keyCols,
      final byte[] keyKinds, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final NumericGroupAggTable out, final int distinctBlock,
      final GroupDistinctAccumulator.Worker distinctOut, final long[] budget, final long[] keyOffsets,
      final int[] keySubstr, final long[] declineFlag, final int[] keyCondCols, final ColumnSlice[][] condCols,
      final long[] keyCondLits, final byte[][] keyCondElseBytes, final long[] keyDivMod,
      final GlobalValueDictionary.ReadView[] globalKeyViews,
      final ProjectionStringIdentityRegistry identityRegistry, final long[] globalCondElseIds) {
    if (predicates == null || out == null || aggCols == null || keyCols == null) {
      throw new IllegalArgumentException("predicates, out, aggCols and keyCols must not be null");
    }
    final long[] mask = MASK.get();
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query
    // that asks only for min/max/count can never decline on an overflow no answer depends
    // on — see NumericGroupAggTable#sumsExact for the rule the merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final int keyCount = keyCols.length;
    final int aggCount = aggCols.length;
    // COUNT-ONLY: no aggregate lanes and no distinct set, so the fold is one increment into a
    // [key, count, firstSeen, aux] stripe — the composite shape behind `group by a, b` with
    // count(*) alone. Loop-invariant, so the check never re-derives anything per row.
    final boolean countOnly = aggCount == 0 && distinctBlock < 0;
    final long[][] compDictHash = new long[keyCount][];
    // Identity lanes per dictionary entry, filled in the same per-leaf pass that hashes it:
    // lane A is the FNV-1a primary (or the cast integer, for a cast component), lane B the xxh3
    // secondary. See CompositeGroupIdentity for why the probe hash cannot double as identity.
    final long[][] compDictIdA = new long[keyCount][];
    final long[][] compDictIdB = new long[keyCount][];
    // Lazy identity proof — see ProjectionIndexByteScan.proveOnFirstUse.
    final byte[][] compDictBytes = new byte[keyCount][];
    final int[][] compDictOffsets = new int[keyCount][];
    final long[][] compDictProven = new long[keyCount][];
    final boolean[] compNeedsProof = new boolean[keyCount];
    final long[][] compValues = new long[keyCount][];
    final int[][] compIds = new int[keyCount][];
    final long[][] compPresence = new long[keyCount][];
    if (identityRegistry == null && CompositeGroupIdentity.hasFingerprintedComponent(keyKinds, keySubstr)) {
      // FAIL CLOSED. A dictionary-string component is identified by a fingerprint pair, which
      // discriminates but does not identify; only ProjectionStringIdentityRegistry's canonical byte
      // comparison makes it exact. The older public overloads delegate here with a null registry,
      // so without this guard they would silently serve a probabilistic identity — the exact defect
      // the registry exists to remove. Numeric and substring-cast keys are unaffected: they carry
      // their raw or cast value in an exact lane and need no registry.
      throw new IllegalArgumentException(
          "composite key has a dictionary-string component and therefore requires a "
              + "ProjectionStringIdentityRegistry; the registry-less overload cannot identify it exactly");
    }
    final int[] idLane = CompositeGroupIdentity.laneOffsets(keyKinds, keySubstr);
    final int identityWidth = idLane[keyCount];
    if (out.idWidth() != identityWidth) {
      throw new IllegalArgumentException(
          "group table identity width " + out.idWidth() + " does not match the composite key's " + identityWidth);
    }
    // One row-sized scratch, hoisted: the kernel writes it per row and acquireExact copies out of
    // it, so a composite group-by allocates nothing per row.
    final long[] identity = new long[identityWidth];
    // Which components own a second identity lane, so a row that leaves one unwritten cannot
    // inherit the previous row's secondary hash.
    final boolean[] twoLane = new boolean[keyCount];
    for (int k = 0; k < keyCount; k++) {
      twoLane[k] = idLane[k + 1] - idLane[k] == 2;
    }
    // A component the executor marked PRE-PROVEN (its column's strings are memoized pairwise
    // distinct under this registry's fingerprint) carries its lanes as exact identity: no proof,
    // no cache, no canonical bytes. In EAGER mode the dictionary pass proves every entry it hashes
    // — the full-coverage precondition for that memo — so the row loop has nothing left to prove.
    final boolean[] compPreProven = new boolean[keyCount];
    boolean anyProof = false;
    for (int k = 0; k < keyCount; k++) {
      if (identityRegistry != null && twoLane[k]) {
        compPreProven[k] = identityRegistry.preProven(k);
        anyProof |= !compPreProven[k];
      }
    }
    final boolean eagerProof = anyProof && identityRegistry.proveEveryEntry();
    // Worker-local, bounded: the same string recurs in most leaves, so this keeps the shared
    // registry off the per-dictionary-entry path while still comparing canonical bytes on every hit.
    // Built only when some component still has something to prove: a fully pre-proven key pays
    // neither its arena nor a single registry probe.
    final ProjectionStringIdentityRegistry.LocalProofCache proofCache = anyProof
        ? new ProjectionStringIdentityRegistry.LocalProofCache(keyCount)
        : null;
    final long[] condElseHash = keyCondCols != null
        ? new long[keyCount]
        : null;
    final long[] condElseIdA = keyCondCols != null
        ? new long[keyCount]
        : null;
    final long[] condElseIdB = keyCondCols != null
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
        if (keyCondElseBytes[k] != null && keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          // GLOBAL then-branch: identity space is the id space, so the else literal is its
          // RESOLVED id — a then-row holding the same value merges exactly. An uninterned literal
          // means no stored row can equal it, and the caller's sentinel (-2, below every real id
          // and outside the presence-marked constants) keeps the else group separate.
          final long elseId = globalCondElseIds != null
              ? globalCondElseIds[k]
              : Long.MIN_VALUE;
          if (elseId == Long.MIN_VALUE) {
            throw new IllegalStateException(
                "global conditional component " + k + " reached the sliced kernel without a resolved else id");
          }
          condElseHash[k] = HashCommon.mix(elseId);
          condElseIdA[k] = elseId;
          condElseIdB[k] = 0L;
          continue;
        }
        // Both roles of the literal — the conditional else branch and the fn:string
        // missing-value substitution — hash once here, in the dictionary's own domain.
        if (keyCondElseBytes[k] != null) {
          condElseHash[k] = ProjectionIndexByteScan.fnv1a64(keyCondElseBytes[k], 0, keyCondElseBytes[k].length);
          condElseIdA[k] = condElseHash[k];
          condElseIdB[k] = GlobalValueDictionary.secondaryValueHash(keyCondElseBytes[k], 0, keyCondElseBytes[k].length);
          if (identityRegistry != null && twoLane[k]) {
            // The substitution literal is a value in the component's own domain and can collide
            // with a stored one, so it is proven exactly like a dictionary entry — once, up front.
            // A pre-proven component has no registry entries to prove it against: the executor
            // never marks a literal-bearing component, and a kernel must not paper over it.
            if (compPreProven[k]) {
              throw new IllegalStateException(
                  "composite key component " + k + " is pre-proven but carries an else literal");
            }
            final byte[] lit = keyCondElseBytes[k];
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
    final long[][] aggValues = new long[aggCount][];
    final long[][] aggPresence = new long[aggCount][];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines
      }
      if (declineFlag != null && declineFlag[0] != 0) {
        return; // a transform case the interpreter raises on — the caller declines
      }
      if (identityRegistry != null && !identityRegistry.identityProven()) {
        return; // string identity cannot be proven — the caller declines
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
        // A temporal key rides the numeric lane like any other ordered long: the epoch IS the group
        // identity, and the winner's text is rendered from it when the group is emitted.
        if (ProjectionIndexRowGroupPage.isOrderedLongKind(keyKinds[k])
            || keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          // A global component in EVERY shape (substring-cast, conditional, untransformed) needs a
          // readable dictionary view: the substring shape transforms through it per distinct id,
          // and the other two materialize winners from it. The id lane itself is the identity.
          if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
              && (globalKeyViews == null || globalKeyViews.length != keyCount || globalKeyViews[k] == null)) {
            throw new IllegalStateException("global composite key requires a readable dictionary view");
          }
          compValues[k] = slice.numericValues();
          compIds[k] = null;
        } else {
          final byte[] dictBytes = slice.dictBytes();
          final int[] dictOffsets = slice.dictOffsets();
          final int dictSize = slice.dictSize();
          long[] hashes = compDictHash[k];
          if (hashes == null || hashes.length < dictSize) {
            hashes = compDictHash[k] = new long[Math.max(64, dictSize)];
          }
          long[] idA = compDictIdA[k];
          if (idA == null || idA.length < dictSize) {
            idA = compDictIdA[k] = new long[Math.max(64, dictSize)];
          }
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
          compDictBytes[k] = dictBytes;
          compDictOffsets[k] = dictOffsets;
          // Entries need proving unless the component is pre-proven; eagerly (every entry, here)
          // when the executor demanded full coverage, lazily (on first row use) otherwise.
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
            final int off = dictOffsets[i];
            final int len = dictOffsets[i + 1] - off;
            if (subStart > 0) {
              final long tv = ProjectionIndexByteScan.xsIntegerOfSubstring(dictBytes, off, len, subStart, subLen);
              hashes[i] = tv == Long.MIN_VALUE
                  ? Long.MIN_VALUE
                  : HashCommon.mix(tv);
              // A cast component groups on the cast result, so THAT is its exact identity.
              idA[i] = tv;
            } else {
              final long primary = ProjectionIndexByteScan.fnv1a64(dictBytes, off, len);
              if (identityRegistry == null) {
                hashes[i] = primary;
                idA[i] = primary;
                idB[i] = GlobalValueDictionary.secondaryValueHash(dictBytes, off, len);
              } else {
                final long a = identityRegistry.laneA(dictBytes, off, len, primary);
                final long b = identityRegistry.laneB(dictBytes, off, len);
                // The PROBE hash is fed from lane A, not from the raw FNV. In production these are
                // the same value, so nothing changes. Under an injected fingerprint they are not,
                // and feeding the raw FNV here would leave the probe hash — and therefore the
                // bucket — still distinguishing strings whose identity lanes were forced equal.
                // The injected adversary would then emulate only HALF a collision and could never
                // make two groups actually merge, which is precisely the failure being guarded.
                hashes[i] = a;
                idA[i] = a;
                idB[i] = b;
                // Eager: every entry is proven here, whether or not a row names it. Lazy: the byte
                // proof waits for the first surviving row that names this entry.
                if (proveEagerly && !proofCache.prove(identityRegistry, k, a, b, dictBytes, off, len)) {
                  return; // fingerprint collision or exhausted budget — the caller declines
                }
              }
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
            long presenceMask = 0L;
            for (int k = 0; k < keyCount; k++) {
              final long compHash;
              final int lane = idLane[k];
              if ((compPresence[k][w] & 1L << bit) == 0L) {
                compHash = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
                presenceMask |= 1L << k;
                identity[lane] = MISSING_COMPONENT_IDENTITY;
                if (twoLane[k]) {
                  identity[lane + 1] = 0L;
                }
              } else if (compValues[k] != null) {
                final long v = compValues[k][rowIdx];
                compHash = HashCommon.mix(v);
                identity[lane] = v;
              } else {
                final int dictId = compIds[k][rowIdx];
                if (compNeedsProof[k] && !ProjectionIndexByteScan.proveOnFirstUse(identityRegistry, proofCache, k,
                    compDictProven[k], dictId, compDictIdA[k][dictId], compDictIdB[k][dictId], compDictBytes[k],
                    compDictOffsets[k][dictId], compDictOffsets[k][dictId + 1] - compDictOffsets[k][dictId])) {
                  return; // fingerprint collision or exhausted budget — the caller declines
                }
                compHash = compDictHash[k][dictId];
                identity[lane] = compDictIdA[k][dictId];
                identity[lane + 1] = compDictIdB[k][dictId];
              }
              h = h * ProjectionIndexByteScan.FNV_PRIME ^ compHash;
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
            } else {
              foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx,
                  distinctBlock, distinctBlock >= 0
                      ? distinctOut.sinkFor(h)
                      : null,
                  budget, null, null, null, null, null, sumExactMask, null);
            }
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
          long presenceMask = 0L;
          for (int k = 0; k < keyCount; k++) {
            final boolean subTransformed = keySubstr != null && keySubstr[2 * k] > 0;
            final int lane = idLane[k];
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
                if (keyCondElseBytes[k] != null) {
                  identity[lane] = condElseIdA[k];
                  if (twoLane[k]) {
                    identity[lane + 1] = condElseIdB[k];
                  }
                } else {
                  // No else literal: the branch produces its own key, distinct from BOTH a stored
                  // value and an absent field, so it gets its own discriminator under the mask bit.
                  presenceMask |= 1L << k;
                  identity[lane] = ABSENT_ELSE_LITERAL_IDENTITY;
                  if (twoLane[k]) {
                    identity[lane + 1] = 0L;
                  }
                }
              } else if ((compPresence[k][w] & 1L << bit) == 0L) {
                // then-branch over a missing field: empty-sequence key
                compHash = ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
                presenceMask |= 1L << k;
                identity[lane] = MISSING_COMPONENT_IDENTITY;
                if (twoLane[k]) {
                  identity[lane + 1] = 0L;
                }
              } else if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
                // Conditional THEN over a global component: the id is the exact identity, in the
                // same lane domain the resolved else id lives in.
                final long gid = compValues[k][rowIdx];
                compHash = HashCommon.mix(gid);
                identity[lane] = gid;
              } else {
                final int dictId = compIds[k][rowIdx];
                if (compNeedsProof[k] && !ProjectionIndexByteScan.proveOnFirstUse(identityRegistry, proofCache, k,
                    compDictProven[k], dictId, compDictIdA[k][dictId], compDictIdB[k][dictId], compDictBytes[k],
                    compDictOffsets[k][dictId], compDictOffsets[k][dictId + 1] - compDictOffsets[k][dictId])) {
                  return; // fingerprint collision or exhausted budget — the caller declines
                }
                compHash = compDictHash[k][dictId];
                identity[lane] = compDictIdA[k][dictId];
                if (twoLane[k]) {
                  identity[lane + 1] = compDictIdB[k][dictId];
                }
              }
            } else if ((compPresence[k][w] & 1L << bit) == 0L) {
              if (subTransformed) {
                // xs:integer(substring((), s, l)) = xs:integer("") — the interpreter RAISES.
                declineFlag[0] = 1;
                return;
              }
              // fn:string(()) is "": the substitution literal's hash, in the dict domain, so the
              // group merges with a stored empty string exactly as the interpreter's does.
              // Otherwise a fixed sentinel — part of the identity, not a side group.
              compHash = condElseHash != null && keyCondElseBytes[k] != null
                  ? condElseHash[k]
                  : ProjectionIndexByteScan.MISSING_COMPONENT_HASH;
              if (condElseHash != null && keyCondElseBytes[k] != null) {
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
            } else if (ProjectionIndexRowGroupPage.isOrderedLongKind(keyKinds[k])) {
              long v = compValues[k][rowIdx];
              if (keyOffsets != null && keyOffsets[k] != 0L) {
                final long shifted = v + keyOffsets[k];
                if (((v ^ shifted) & (keyOffsets[k] ^ shifted)) < 0) {
                  declineFlag[0] = 1; // overflow: the interpreter promotes to decimal
                  return;
                }
                v = shifted;
              }
              v = ProjectionIndexByteScan.applyDivMod(v, keyDivMod, k);
              compHash = HashCommon.mix(v);
              identity[lane] = v;
            } else if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
              if (subTransformed) {
                final long transformed = globalKeyViews[k].xsIntegerOfSubstring(Math.toIntExact(compValues[k][rowIdx]),
                    keySubstr[2 * k], keySubstr[2 * k + 1]);
                if (transformed == Long.MIN_VALUE) {
                  declineFlag[0] = 1;
                  return;
                }
                compHash = HashCommon.mix(transformed);
                identity[lane] = transformed;
              } else {
                // Untransformed global component: the id IS the exact identity — no dictionary
                // bytes read, no content hash, one lane.
                final long gid = compValues[k][rowIdx];
                compHash = HashCommon.mix(gid);
                identity[lane] = gid;
              }
            } else {
              final int dictId = compIds[k][rowIdx];
              if (compNeedsProof[k] && !ProjectionIndexByteScan.proveOnFirstUse(identityRegistry, proofCache, k,
                  compDictProven[k], dictId, compDictIdA[k][dictId], compDictIdB[k][dictId], compDictBytes[k],
                  compDictOffsets[k][dictId], compDictOffsets[k][dictId + 1] - compDictOffsets[k][dictId])) {
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
            h = h * ProjectionIndexByteScan.FNV_PRIME ^ compHash;
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
          } else {
            foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx, distinctBlock,
                distinctBlock >= 0
                    ? distinctOut.sinkFor(h)
                    : null,
                budget, null, null, null, null, null, sumExactMask, null);
          }
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#readRowKeyParts}: materialize ONE winner row's key
   * parts from slices, applying the SAME transforms the kernel grouped by (winners emit the
   * TRANSFORMED value; a raise-case cannot appear — the kernel declined before a winner existed).
   */
  public static void readRowKeyPartsSliced(final ColumnSlice[][] keyCols, final byte[] keyKinds,
      final ColumnSlice[][] condCols, final int leaf, final int rowIdx, final String[] outStrings,
      final long[] outLongs, final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets,
      final int[] keySubstr, final int[] keyCondCols, final long[] keyCondLits, final String[] keyCondElse,
      final long[] keyDivMod) {
    readRowKeyPartsSliced(keyCols, keyKinds, condCols, leaf, rowIdx, outStrings, outLongs, outPresent, outIsLong,
        keyOffsets, keySubstr, keyCondCols, keyCondLits, keyCondElse, keyDivMod, null);
  }

  /** Global-string substring-cast capable winner materialisation; views align to key components. */
  public static void readRowKeyPartsSliced(final ColumnSlice[][] keyCols, final byte[] keyKinds,
      final ColumnSlice[][] condCols, final int leaf, final int rowIdx, final String[] outStrings,
      final long[] outLongs, final boolean[] outPresent, final boolean[] outIsLong, final long[] keyOffsets,
      final int[] keySubstr, final int[] keyCondCols, final long[] keyCondLits, final String[] keyCondElse,
      final long[] keyDivMod, final GlobalValueDictionary.ReadView[] globalKeyViews) {
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
      outPresent[k] = true;
      if (ProjectionIndexRowGroupPage.isOrderedLongKind(keyKinds[k])) {
        outIsLong[k] = true;
        outLongs[k] = ProjectionIndexByteScan.applyDivMod(slice.numericValues()[rowIdx] + (keyOffsets != null
            ? keyOffsets[k]
            : 0L), keyDivMod, k);
      } else if (keyKinds[k] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        if (globalKeyViews == null || globalKeyViews.length != keyCols.length || globalKeyViews[k] == null) {
          throw new IllegalStateException("global composite winner requires a readable dictionary view");
        }
        if (keySubstr != null && keySubstr[2 * k] > 0) {
          outIsLong[k] = true;
          outLongs[k] = globalKeyViews[k].xsIntegerOfSubstring(Math.toIntExact(slice.numericValues()[rowIdx]),
              keySubstr[2 * k], keySubstr[2 * k + 1]);
        } else {
          // Untransformed (or conditional-then) global component: the winner's key part is the
          // interned value itself — one dictionary read per winner.
          outIsLong[k] = false;
          outStrings[k] = globalKeyViews[k].valueAsString(Math.toIntExact(slice.numericValues()[rowIdx]));
        }
      } else {
        final int dictId = slice.stringDictIds()[rowIdx];
        if (keySubstr != null && keySubstr[2 * k] > 0) {
          outIsLong[k] = true;
          outLongs[k] = ProjectionIndexByteScan.xsIntegerOfSubstring(slice.dictBytes(), slice.dictOffset(dictId),
              slice.dictLength(dictId), keySubstr[2 * k], keySubstr[2 * k + 1]);
        } else {
          outIsLong[k] = false;
          outStrings[k] = slice.dictString(dictId);
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupPackedSubstringFlat}
   * (v1: conjunctive predicates — trees stay whole-leaf). Groups AND orders on the SHARED
   * {@link ProjectionIndexByteScan#packIsoMinuteSubstring} long (pack-domain parity is structural —
   * both families pack the same decoded entry bytes); aux carries {@code (leaf << 20) | dictId} so
   * winners decode originals from the slice dict. Decline points verbatim: a matching row MISSING the
   * key field (substring of the empty sequence is {@code ""}, a REAL key failing the ISO shape) and a
   * row referencing an entry the pack validator rejected.
   */
  public static void aggregateByGroupPackedSubstringFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf, final int subStart, final int subLen,
      final NumericGroupAggTable out, final long[] declineFlag) {
    aggregateByGroupPackedSubstringFlat(store, predicates, predCols, treeOrNull, treeCols, groupCol, aggCols, fromLeaf,
        toLeaf, subStart, subLen, out, declineFlag, null);
  }

  /** Global-string capable packed-substring overload; a non-null view selects global-id cells. */
  public static void aggregateByGroupPackedSubstringFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols,
      final ProjectionIndexScan.PredicateTree treeOrNull, final ColumnSlice[][] treeCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf, final int subStart, final int subLen,
      final NumericGroupAggTable out, final long[] declineFlag,
      final GlobalValueDictionary.ReadView globalDictionaryView) {
    if (predicates == null || out == null || aggCols == null || declineFlag == null) {
      throw new IllegalArgumentException("predicates, out, aggCols and declineFlag must not be null");
    }
    final long[] mask = MASK.get();
    // The SUM lanes the query actually reads. Every other lane goes unfolded, so a query
    // that asks only for min/max/count can never decline on an overflow no answer depends
    // on — see NumericGroupAggTable#sumsExact for the rule the merge obeys too.
    final long sumExactMask = out.sumExactMask();
    final DictScratch ds = DICT_SCRATCH.get();
    final int aggCount = aggCols.length;
    // COUNT-ONLY: no aggregate lanes, so the fold is one increment into the group's stripe.
    final boolean countOnly = aggCount == 0;
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
      final boolean global = globalDictionaryView != null;
      final byte[] dictBytes = global
          ? null
          : group.dictBytes();
      final int[] dictOffsets = global
          ? null
          : group.dictOffsets();
      final int[] ids = global
          ? null
          : group.stringDictIds();
      final long[] globalIds = global
          ? group.numericValues()
          : null;
      final long[] groupPresence = group.presenceWords();
      final long[] dictPacked;
      if (global) {
        dictPacked = null;
      } else {
        final int dictSize = group.dictSize();
        ds.ensure(dictSize);
        dictPacked = ds.hash;
        for (int i = 0; i < dictSize; i++) {
          final int off = dictOffsets[i];
          dictPacked[i] = ProjectionIndexByteScan.packIsoMinuteSubstring(dictBytes, off,
              dictOffsets[i + 1] - off, subStart, subLen);
        }
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
          final int dictId = global
              ? Math.toIntExact(globalIds[rowIdx])
              : ids[rowIdx];
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
            foldSliced(slotArr, base, aggValues, aggPresence, null, null, null, aggCount, w, bit, rowIdx, -1, null,
                null, null, null, null, null, null, sumExactMask, null);
          }
        }
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#stringAggForWinnerGroups} — deferred string extrema
   * (pass 2): fold min/max over string operand columns for the WINNER groups only, matching rows to
   * winners by the SAME transformed-key hash pass 1 grouped on (a regex key hashes the TRANSFORMED
   * entry — hashing raw would silently mismatch every winner and return all-null extrema).
   * Best-so-far entries are plain {@code byte[]} refs (slices are immutable, store-lifetime);
   * collation authority is the byte kernel's own comparator.
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
    // Best-so-far per (agg, slot) as a RANGE into whichever leaf's flat dictionary currently wins;
    // slices are immutable and store-lifetime, so the reference stays valid across leaves.
    final byte[][][] bestBytes = new byte[aggCount][slots][];
    final int[][] bestOff = new int[aggCount][slots];
    final int[][] bestLen = new int[aggCount][slots];
    // OWN winner-slot memo (sentinel -2 = unresolved) — DictScratch.base uses -1 and sharing
    // two sentinel conventions in one array is a foot-gun.
    int[] winnerSlotOfDict = new int[64];
    final byte[][] aggDictBytes = new byte[aggCount][];
    final int[][] aggDictOffsets = new int[aggCount][];
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
      final byte[] dictBytes = group.dictBytes();
      final int[] dictOffsets = group.dictOffsets();
      final int[] ids = group.stringDictIds();
      final long[] groupPresence = group.presenceWords();
      final int dictSize = group.dictSize();
      if (winnerSlotOfDict.length < dictSize) {
        winnerSlotOfDict = new int[Math.max(winnerSlotOfDict.length * 2, dictSize)];
      }
      Arrays.fill(winnerSlotOfDict, 0, dictSize, -2);
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = stringAggCols[a][leaf];
        aggDictBytes[a] = agg.dictBytes();
        aggDictOffsets[a] = agg.dictOffsets();
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
              final int off = dictOffsets[dictId];
              final int len = dictOffsets[dictId + 1] - off;
              final long h = keyRegex != null
                  ? ProjectionIndexByteScan.transformedKeyHash(regexCache, keyRegex, keyRegexRepl, dictBytes, off, len)
                  : ProjectionIndexByteScan.fnv1a64(dictBytes, off, len);
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
            final int aggDictId = aggIds[a][rowIdx];
            final byte[] entryBytes = aggDictBytes[a];
            final int off = aggDictOffsets[a][aggDictId];
            final int len = aggDictOffsets[a][aggDictId + 1] - off;
            final byte[] cur = bestBytes[a][slot];
            if (cur == null) {
              bestBytes[a][slot] = entryBytes;
              bestOff[a][slot] = off;
              bestLen[a][slot] = len;
              continue;
            }
            if (cur == entryBytes && bestOff[a][slot] == off) {
              continue; // the group's best IS this dict entry (same leaf + id = same range)
            }
            final int cmp =
                ProjectionIndexByteScan.compareStrSlices(entryBytes, off, len, cur, bestOff[a][slot], bestLen[a][slot]);
            if (aggIsMin[a]
                ? cmp < 0
                : cmp > 0) {
              bestBytes[a][slot] = entryBytes;
              bestOff[a][slot] = off;
              bestLen[a][slot] = len;
            }
          }
        }
      }
    }
    for (int a = 0; a < aggCount; a++) {
      for (int sl = 0; sl < slots; sl++) {
        bestOut[a][sl] = bestBytes[a][sl] == null
            ? null
            : new String(bestBytes[a][sl], bestOff[a][sl], bestLen[a][sl], StandardCharsets.UTF_8);
      }
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateAllNumeric}: fold every
   * matching row into ONE accumulator block ({@code newGroupAggAcc} layout, ordinal lane unused) —
   * the single-group kernel behind {@code group by <constant>}. No group column is read at all; the
   * caller merges per-worker accumulators.
   *
   * <p>
   * Lane-at-a-time, not row-at-a-time: the generic per-row {@code foldSliced} (a bit walk plus an
   * {@code addExact} per value) cost 16 ns/row on a resident column — 100M q29 spent 0.2 s hot on
   * ONE column. Each aggregate lane is folded over the matched-and-present words with a
   * straight-line loop on full words and a bit walk on partial ones, into leaf-local
   * {@code [count, sum, min, max]}; exactness is decided ONCE per leaf from the fold's own extrema
   * (every partial sum is bounded by {@code count × max|v|}), and only a leaf that could have wrapped
   * is re-summed with {@code addExact} — the interpreter promotes an overflowing sum, so the arm must
   * decline rather than wrap, exactly as before.
   * </p>
   */
  public static void aggregateAllNumericFlat(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSlice[][] predCols, final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf,
      final long[] acc) {
    if (predicates == null || acc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, acc and aggCols must not be null");
    }
    final long[] mask = MASK.get();
    final int aggCount = aggCols.length;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final int stride = (rowCount + 63) >>> 6;
      long matched = 0L;
      for (int w = 0; w < stride; w++) {
        matched += Long.bitCount(mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount));
      }
      if (matched == 0L) {
        continue;
      }
      acc[0] += matched;
      for (int a = 0; a < aggCount; a++) {
        final ColumnSlice agg = aggCols[a][leaf];
        final long[] values = agg.numericValues();
        if (values == null) {
          throw new IllegalStateException("const-group aggregate over a column without a numeric lane on leaf " + leaf);
        }
        foldNumericLaneFlat(acc, 2 + 4 * a, values, agg.presenceWords(), mask, stride, rowCount);
      }
    }
  }

  /** Values whose magnitude is below this never wrap a 64-row straight-line block, whatever the mask. */
  private static final long EXACT_SUM_MAGNITUDE = 1L << 62;

  /**
   * One aggregate lane of one leaf into {@code acc[aggBase..aggBase+3]} = {@code [count, sum, min,
   * max]}: rows are those set in {@code mask} (already clipped to the leaf's rows) AND present in the
   * lane. Plain adds throughout; the leaf's contribution is proven exact from its own extrema before
   * it is committed, otherwise re-summed with {@code addExact}.
   */
  private static void foldNumericLaneFlat(final long[] acc, final int aggBase, final long[] values,
      final long @Nullable [] presence, final long[] mask, final int stride, final int rowCount) {
    long cnt = 0L;
    long sum = 0L;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int w = 0; w < stride; w++) {
      long bits = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
      if (presence != null) {
        bits &= presence[w];
      }
      if (bits == 0L) {
        continue;
      }
      final int rowBase = w << 6;
      if (bits == -1L) {
        // Full word: 64 consecutive values, four independent sum lanes so the adds pipeline.
        long s0 = 0L;
        long s1 = 0L;
        long s2 = 0L;
        long s3 = 0L;
        for (int i = rowBase; i < rowBase + 64; i += 4) {
          final long v0 = values[i];
          final long v1 = values[i + 1];
          final long v2 = values[i + 2];
          final long v3 = values[i + 3];
          s0 += v0;
          s1 += v1;
          s2 += v2;
          s3 += v3;
          min = Math.min(min, Math.min(Math.min(v0, v1), Math.min(v2, v3)));
          max = Math.max(max, Math.max(Math.max(v0, v1), Math.max(v2, v3)));
        }
        sum += s0 + s1 + s2 + s3;
        cnt += 64L;
        continue;
      }
      while (bits != 0L) {
        final int bit = Long.numberOfTrailingZeros(bits);
        bits &= bits - 1L;
        final long v = values[rowBase + bit];
        sum += v;
        min = Math.min(min, v);
        max = Math.max(max, v);
        cnt++;
      }
    }
    if (cnt == 0L) {
      return;
    }
    // Every partial sum is bounded by cnt × max|v|: below 2^62 the plain adds cannot have wrapped.
    final long bound = EXACT_SUM_MAGNITUDE / cnt;
    if (min <= -bound || max >= bound) {
      sum = exactSumOfLane(values, presence, mask, stride, rowCount);
    }
    acc[aggBase] += cnt;
    acc[aggBase + 1] = Math.addExact(acc[aggBase + 1], sum);
    if (min < acc[aggBase + 2]) {
      acc[aggBase + 2] = min;
    }
    if (max > acc[aggBase + 3]) {
      acc[aggBase + 3] = max;
    }
  }

  /** The rare exact re-sum of a leaf whose extrema could not prove the plain fold wrap-free. */
  private static long exactSumOfLane(final long[] values, final long @Nullable [] presence, final long[] mask,
      final int stride, final int rowCount) {
    long sum = 0L;
    for (int w = 0; w < stride; w++) {
      long bits = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
      if (presence != null) {
        bits &= presence[w];
      }
      final int rowBase = w << 6;
      while (bits != 0L) {
        final int bit = Long.numberOfTrailingZeros(bits);
        bits &= bits - 1L;
        sum = Math.addExact(sum, values[rowBase + bit]);
      }
    }
    return sum;
  }

}
