/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * Vectorized executor that bridges Brackit's VectorizedExecutor interface
 * to SirixDB's parallel page-scan path.
 */
package io.sirix.query.scan;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.HyperLogLogSketch;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberRegionSimd;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;

import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * Vectorized executor backed by a Sirix {@link JsonResourceSession}.
 *
 * <p>Implements Brackit's {@link VectorizedExecutor} interface so that
 * Brackit's optimizer can dispatch eligible queries (pure aggregates,
 * filtered counts, group-bys) to a parallel page-scan path that bypasses
 * the per-record JsonItem/Trx ceremony.
 *
 * <p>Pattern: each worker opens its own {@link JsonNodeReadOnlyTrx} +
 * {@code StorageEngineReader}, walks a chunk of leaf pages, examines
 * populated slots, and accumulates per-thread results that are merged
 * at the end.
 *
 * <p>Phase 1 (this commit): {@link #executeAggregate} for sum/avg/min/max
 * over a single numeric field. Filter-count and group-by-count to follow
 * once the abstraction is proven on aggregates.
 */
public final class SirixVectorizedExecutor implements VectorizedExecutor {

  /** Node kind id for an OBJECT_KEY entry — the named child of an object. */
  private static final int OBJECT_KEY_KIND = 26;

  private final JsonResourceSession session;
  private final int revision;
  private final int threads;

  /** Kill switch to bypass the PAX fast path for regression bisection. */
  private static final boolean PAX_FAST_PATH_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.pax.scan", "true"));

  /**
   * Lightweight scan-path diagnostics. Toggled via {@code -Dsirix.scan.diag=true}.
   * When {@code false}, all counter increments are dead-code-eliminated by the
   * JIT because this field is {@code static final} and read as a compile-time
   * constant at class-init time. Zero steady-state overhead when disabled.
   */
  static final boolean DIAGNOSTICS_ENABLED = Boolean.getBoolean("sirix.scan.diag");

  /**
   * Per-query scan counters — attributes where the hot loop spent its pages and
   * records. Only populated when {@link #DIAGNOSTICS_ENABLED} is {@code true}.
   * Per-thread slots are contiguous in a single array; no contention, no
   * false-sharing mitigation (one int per thread at 64-byte stride would be the
   * next step if this becomes hot, but counter increments in-lambda are
   * register-resident for the JIT's duration).
   */
  static final class ScanDiagnostics {
    // Page-level outcomes (one increment per leaf page visited).
    long pagesVisited;         // every getRecordPage result that yielded a KeyValueLeafPage
    long pagesNullHeader;      // PAX NumberRegion header absent — slow path
    long pagesTagMissing;      // header present, but lookupTag(fieldKey) returned -1
    long pagesZoneSkippedAll;  // zone-map ruled out every row in the tag range
    long pagesZoneAllPass;     // zone-map proved every row passes → no decode needed
    long pagesDecoded;         // PAX fast-path decode loop actually ran
    long pagesSlowPath;        // fell through to moveTo + getNumberValue path

    // Record-level outcomes (sum across decoded pages).
    long recordsFastPath;      // records processed inside the PAX decode loop
    long recordsSlowPath;      // records processed via rtx.moveTo + firstChild
    long recordsMatched;       // records that passed the predicate

    // GroupBy-specific: which path the value-read took.
    long groupByDirectSuccess;  // childPk == pk AND readObjectStringValueBytesFromSlot returned > 0
    long groupByCrossPage;      // childPk != pk (value lives on a different page)
    long groupByDirectNeg;      // direct read returned <= 0 (FSST/oversize/etc.)
    long groupByRtxFallback;    // rtx.moveTo path actually ran

    // GroupBy StringRegion fast-path stats.
    long stringRegionHits;          // pages where the BtrBlocks/Umbra-style StringRegion fast-path ran
    long stringRegionMissesNoHeader; // header was null (region absent — page hadn't built one)
    long stringRegionMissesNoTag;    // header present but no tag for this fieldKey
    long stringRegionMissesDictBig;  // local dict > 256 entries — exceeds scratch

    /** Merge {@code other} into this accumulator. Called during per-thread fan-in. */
    void mergeFrom(final ScanDiagnostics other) {
      this.pagesVisited += other.pagesVisited;
      this.pagesNullHeader += other.pagesNullHeader;
      this.pagesTagMissing += other.pagesTagMissing;
      this.pagesZoneSkippedAll += other.pagesZoneSkippedAll;
      this.pagesZoneAllPass += other.pagesZoneAllPass;
      this.pagesDecoded += other.pagesDecoded;
      this.pagesSlowPath += other.pagesSlowPath;
      this.recordsFastPath += other.recordsFastPath;
      this.recordsSlowPath += other.recordsSlowPath;
      this.recordsMatched += other.recordsMatched;
      this.groupByDirectSuccess += other.groupByDirectSuccess;
      this.groupByCrossPage += other.groupByCrossPage;
      this.groupByDirectNeg += other.groupByDirectNeg;
      this.groupByRtxFallback += other.groupByRtxFallback;
      this.stringRegionHits += other.stringRegionHits;
      this.stringRegionMissesNoHeader += other.stringRegionMissesNoHeader;
      this.stringRegionMissesNoTag += other.stringRegionMissesNoTag;
      this.stringRegionMissesDictBig += other.stringRegionMissesDictBig;
    }

    void print(final String label) {
      final long totalPagesWithHeader = pagesDecoded + pagesZoneSkippedAll + pagesZoneAllPass;
      final long totalRecords = recordsFastPath + recordsSlowPath;
      System.err.printf("[scan-diag %s] pages visited=%,d  [fast-path: header-null=%,d, tag-missing=%,d, "
              + "zone-skip=%,d, zone-all=%,d, decoded=%,d] slow-path=%,d%n",
          label, pagesVisited, pagesNullHeader, pagesTagMissing, pagesZoneSkippedAll, pagesZoneAllPass,
          pagesDecoded, pagesSlowPath);
      System.err.printf("[scan-diag %s] records: fast-path=%,d (decoded %,d pages), slow-path=%,d "
              + "(via moveTo), matched=%,d, total=%,d%n",
          label, recordsFastPath, totalPagesWithHeader, recordsSlowPath, recordsMatched, totalRecords);
      if (groupByDirectSuccess + groupByCrossPage + groupByDirectNeg + groupByRtxFallback > 0) {
        System.err.printf("[scan-diag %s] groupBy paths: direct-success=%,d, cross-page=%,d, "
                + "direct-returned-neg=%,d, rtx-fallback=%,d%n",
            label, groupByDirectSuccess, groupByCrossPage, groupByDirectNeg, groupByRtxFallback);
      }
      if (stringRegionHits + stringRegionMissesNoHeader + stringRegionMissesNoTag + stringRegionMissesDictBig > 0) {
        System.err.printf("[scan-diag %s] StringRegion: hits=%,d, miss-no-header=%,d, "
                + "miss-no-tag=%,d, miss-dict-too-big=%,d%n",
            label, stringRegionHits, stringRegionMissesNoHeader, stringRegionMissesNoTag,
            stringRegionMissesDictBig);
      }
    }
  }
  /** Cached max node key — Sirix-internal, fixed for a given revision. */
  private volatile long cachedMaxNodeKey = -1;
  private final ExecutorService workerPool;
  /** Cached field-name → int nameKey resolution. Keyed once per executor lifetime. */
  private final ConcurrentHashMap<String, Integer> fieldKeyCache = new ConcurrentHashMap<>();

  /**
   * Per-field cache of the {@code {count, sum, min, max}} tuple produced by
   * {@link #parallelAggregate(String)}. The executor is scoped to a single
   * (session, revision), so the result is stable for the executor's lifetime —
   * a closed-form invalidation is therefore not required. This exists so that
   * query shapes like {@code {"min": min(...x), "max": max(...x)}} — which
   * Brackit compiles to two separate aggregate calls on the same field — pay
   * the parallel scan only once. For a 100M-record dataset that's a 8 s
   * saving on the {@code minMaxAge} query shape (one scan vs two).
   */
  private final ConcurrentHashMap<String, long[]> aggregateCache = new ConcurrentHashMap<>();

  /**
   * Per-predicate cache of the {@code long} count produced by
   * {@link #parallelFilterCount(String,String,long)} /
   * {@link #parallelFilterCountRange(String,String,long,String,long)}. Key is
   * the canonicalized predicate signature (field + op + value); value is the
   * scan result. Same validity argument as {@link #aggregateCache} — the
   * executor is bound to a single (session, revision). Realistic dashboards
   * re-issue the same filter-count after small delays (e.g. 5 s polling); the
   * cache keeps the repeat sub-millisecond instead of a full parallel scan.
   */
  private final ConcurrentHashMap<String, Long> filterCountCache = new ConcurrentHashMap<>();

  /**
   * Per-field cache of the Brackit {@link Sequence} produced by
   * {@link #parallelGroupByCount(String)}. The Sequence is a {@code DArray} of
   * immutable {@code ArrayObject}s — safe to share across callers. Same
   * validity argument as the aggregate/filter caches: the executor is bound
   * to one (session, revision).
   */
  private final ConcurrentHashMap<String, Sequence> groupByCountCache = new ConcurrentHashMap<>();

  /**
   * Per-(group, filter) cache of filtered-group-by-count. Same validity
   * argument as the other caches — executor is bound to one (session,
   * revision). Key is {@code groupField|filterField|filterOp|filterValue}.
   */
  private final ConcurrentHashMap<String, Sequence> filteredGroupByCountCache = new ConcurrentHashMap<>();

  public SirixVectorizedExecutor(JsonResourceSession session, int revision) {
    this(session, revision, defaultThreadCount());
  }

  /**
   * Default worker-thread count, resolved from {@code -Dsirix.vec.threads=N}
   * or the number of available CPUs. Lower thread counts reduce concurrent
   * page-load pressure on the page-cache evictor, which matters when the
   * working set approaches the buffer budget and the ClockSweeper struggles
   * to keep up with allocations.
   */
  private static int defaultThreadCount() {
    final String prop = System.getProperty("sirix.vec.threads");
    if (prop != null && !prop.isEmpty()) {
      try {
        final int n = Integer.parseInt(prop.trim());
        if (n > 0) return n;
      } catch (NumberFormatException ignored) {
      }
    }
    return Runtime.getRuntime().availableProcessors();
  }

  public SirixVectorizedExecutor(JsonResourceSession session, int revision, int threads) {
    this.session = session;
    this.revision = revision;
    this.threads = threads;
    final ThreadFactory tf = r -> {
      Thread t = new Thread(r, "sirix-vec-exec");
      t.setDaemon(true);
      return t;
    };
    this.workerPool = Executors.newFixedThreadPool(threads, tf);
  }

  /** Release per-thread shared trxes and shut down the worker pool. */
  public void close() {
    // Sirix's session manages the per-thread shared trx pool via
    // getOrCreateSharedReadOnlyTrx; closeSharedReadOnlyTrxs releases all
    // entries for our revision. Bounded count = workerThreads + 1.
    try {
      session.closeSharedReadOnlyTrxs(revision);
    } catch (Exception ignored) {
    }
    workerPool.shutdown();
  }

  /** Borrow this thread's shared read-only trx — reused across calls, no per-call alloc. */
  private JsonNodeReadOnlyTrx workerTrx() {
    return session.getOrCreateSharedReadOnlyTrx(revision);
  }

  @Override
  public boolean canExecute(QueryContext ctx) {
    return session != null;
  }

  @Override
  public Sequence executeGroupByCount(QueryContext ctx, String groupField) throws QueryException {
    try {
      Sequence cached = groupByCountCache.get(groupField);
      if (cached == null) {
        final Sequence fresh = parallelGroupByCount(groupField);
        cached = groupByCountCache.putIfAbsent(groupField, fresh);
        if (cached == null) cached = fresh;
      }
      return cached;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized group-by-count failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Generic predicate-tree count, Umbra/DuckDB/ClickHouse-style. The tree is
   * compiled once to a flat {@link CompiledPredicate} form and the hot-loop
   * walks it record-by-record, evaluating leaves via primitive scratch arrays.
   *
   * <p>This is the single entry point for all filter-count queries; the
   * legacy shape-specific kernels (executeFilterCount, executeFilterCount2,
   * executeFilterCountAndBool, executeFilterCount2AndBool) are kept only for
   * backward compatibility with executors that haven't migrated.
   */
  @Override
  public Sequence executePredicateCount(QueryContext ctx, PredicateNode predicate) throws QueryException {
    if (predicate == null) return null;
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) return new Int64(0L);
      if (predicate instanceof PredicateNode.AlwaysTrue) {
        final Sequence fast = tryPathSummaryStats(null, "count");
        if (fast != null) return fast;
        return null;
      }
      final String cacheKey = predicateCacheKey(predicate);
      Long cached = filterCountCache.get(cacheKey);
      if (cached == null) {
        final long fresh = parallelGenericPredicateCount(predicate);
        cached = filterCountCache.putIfAbsent(cacheKey, fresh);
        if (cached == null) cached = fresh;
      }
      return new Int64(cached);
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized predicate-count failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Generic predicate-tree group-by-count. Applies {@code predicate} to every
   * record; for those that pass, reads the {@code groupField} string value and
   * increments the count for that group. Returns a {@link Sequence} of
   * {@code {"groupField": value, "count": n}} objects — same shape as
   * {@link #executeGroupByCount}.
   */
  @Override
  public Sequence executePredicateGroupByCount(QueryContext ctx, PredicateNode predicate, String groupField)
      throws QueryException {
    if (predicate == null || groupField == null) return null;
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) {
        return new DArray(new ArrayList<>());
      }
      final String cacheKey = "gb:" + groupField + "#" + predicateCacheKey(predicate);
      Sequence cached = filteredGroupByCountCache.get(cacheKey);
      if (cached != null) return cached;
      final Sequence fresh = parallelGenericPredicateGroupByCount(predicate, groupField);
      cached = filteredGroupByCountCache.putIfAbsent(cacheKey, fresh);
      return cached == null ? fresh : cached;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized predicate-group-by-count failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Walk every leaf page in parallel; per-record evaluate the compiled
   * predicate; on match, bump the per-group counter. The group field is
   * compiled into the same {@link CompiledPredicate} via an extra field slot
   * so the per-record load pass covers both predicate fields and the group
   * field in one sweep.
   */
  private Sequence parallelGenericPredicateGroupByCount(final PredicateNode predicate, final String groupField)
      throws Exception {
    // Augment the predicate's field set with the group field so `loadFields`
    // picks it up in the same sweep over the record's children.
    final CompiledPredicate cp = compileWithExtraField(predicate, groupField);
    // The anchor is still the predicate's first field — the group field is
    // not necessarily selective, so we don't drive from it.
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) return new DArray(new ArrayList<>());
    final int groupFieldIdx = indexOfStr(cp.fieldNames, groupField);
    if (groupFieldIdx < 0 || cp.fieldNameKeys[groupFieldIdx] == -1) {
      return new DArray(new ArrayList<>());
    }

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    @SuppressWarnings("unchecked")
    final Map<String, long[]>[] perThread = new Map[eff];

    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
      final HashMap<String, long[]> local = new HashMap<>();
      for (long pk = s; pk < e; pk++) {
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
        for (final int slot : matches) {
          final long anchorObjectKey = base + slot;
          if (!rtx.moveTo(anchorObjectKey)) continue;
          if (!rtx.moveToParent()) continue;
          loadFields(rtx, cp, scratch);
          if (!evalCompiled(cp, 0, scratch)) continue;
          // Read the group field's value from the same scratch.
          if (scratch.fieldKind[groupFieldIdx] != 3) continue;  // require string group
          final String gv = scratch.strVals[groupFieldIdx];
          if (gv == null) continue;
          long[] counter = local.get(gv);
          if (counter == null) {
            counter = new long[] { 0L };
            local.put(gv, counter);
          }
          counter[0]++;
        }
      }
      perThread[idx] = local;
    });

    // Merge per-thread maps.
    final HashMap<String, Long> merged = new HashMap<>();
    for (final Map<String, long[]> m : perThread) {
      if (m == null) continue;
      for (final Map.Entry<String, long[]> e : m.entrySet()) {
        merged.merge(e.getKey(), e.getValue()[0], Long::sum);
      }
    }

    // Build Brackit output: array of {"groupField": key, "count": n}.
    final ArrayList<Item> out = new ArrayList<>(merged.size());
    final QNm keyQnm = new QNm(groupField);
    final QNm cntQnm = new QNm("count");
    for (final Map.Entry<String, Long> e : merged.entrySet()) {
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { new Str(e.getKey()), new Int64(e.getValue()) };
      out.add(new ArrayObject(keys, vals));
    }
    return new DArray(out);
  }

  /**
   * Compile a predicate tree with an extra {@code groupField} appended to its
   * field set. This ensures {@code loadFields} also reads the group field in
   * the same per-record sweep, so we don't need a second pass.
   */
  private CompiledPredicate compileWithExtraField(final PredicateNode root, final String extraField) {
    final CompiledPredicate base = compile(root);
    // Already present? Then just return as-is.
    for (int i = 0; i < base.fieldNames.length; i++) {
      if (base.fieldNames[i].equals(extraField)) return base;
    }
    final int n = base.fieldNames.length + 1;
    final String[] newNames = new String[n];
    final int[] newKeys = new int[n];
    System.arraycopy(base.fieldNames, 0, newNames, 0, base.fieldNames.length);
    System.arraycopy(base.fieldNameKeys, 0, newKeys, 0, base.fieldNameKeys.length);
    newNames[n - 1] = extraField;
    newKeys[n - 1] = resolveFieldKey(extraField);
    base.fieldNames = newNames;
    base.fieldNameKeys = newKeys;
    return base;
  }

  private static int indexOfStr(final String[] arr, final String s) {
    for (int i = 0; i < arr.length; i++) if (arr[i].equals(s)) return i;
    return -1;
  }

  /**
   * Structural, allocation-light cache key for a {@link PredicateNode} tree.
   * Equal trees produce equal keys. Uses a StringBuilder so the same field
   * names / literals don't each create a new interned string — matters for
   * dashboards that re-issue the same predicate.
   */
  private static String predicateCacheKey(final PredicateNode p) {
    final StringBuilder sb = new StringBuilder(64);
    appendKey(sb, p);
    return sb.toString();
  }

  private static void appendKey(final StringBuilder sb, final PredicateNode p) {
    switch (p) {
      case PredicateNode.NumCmp nc -> sb.append('N').append(nc.field()).append(':').append(nc.op()).append(':')
                                        .append(nc.value()).append(';');
      case PredicateNode.StrEq se -> sb.append('S').append(se.field()).append(':').append(se.value()).append(';');
      case PredicateNode.BoolRef br -> sb.append('B').append(br.field()).append(';');
      case PredicateNode.Not n -> { sb.append("!("); appendKey(sb, n.child()); sb.append(");"); }
      case PredicateNode.And a -> {
        sb.append("&(");
        for (PredicateNode c : a.children()) appendKey(sb, c);
        sb.append(");");
      }
      case PredicateNode.Or o -> {
        sb.append("|(");
        for (PredicateNode c : o.children()) appendKey(sb, c);
        sb.append(");");
      }
      case PredicateNode.AlwaysTrue t -> sb.append('T').append(';');
      case PredicateNode.AlwaysFalse f -> sb.append('F').append(';');
    }
  }

  /**
   * Compiled form of a {@link PredicateNode} tree: operator opcodes as a byte
   * array, ancillary operands in parallel arrays. Interpreting this in the
   * hot loop is branch-predictable int-dispatch, no Object allocation, no
   * String comparison, no Integer/Long/Boolean boxing.
   *
   * <p>Encoding (one entry per tree node, stored post-order / bottom-up is
   * not used — we recurse via child indices):
   * <pre>
   *   opcode        payload
   *   ───────────────────────────────────────────
   *   OP_ALWAYS_T   -
   *   OP_ALWAYS_F   -
   *   OP_NOT        childIdx[0]
   *   OP_AND        childCount, childIdx[0..n-1]
   *   OP_OR         same as AND
   *   OP_NUM_CMP    fieldIdx, cmpOp, longVal
   *   OP_STR_EQ     fieldIdx, strIdx (index into strLiterals)
   *   OP_BOOL_REF   fieldIdx
   * </pre>
   * The flat array layout was chosen because JIT-inlining of the hot predicate
   * evaluator requires a small, type-stable representation; record-dispatch
   * over {@link PredicateNode} reuses the switch but pays one indirection +
   * a virtual call per node.
   */
  private static final class CompiledPredicate {
    static final byte OP_ALWAYS_T = 0;
    static final byte OP_ALWAYS_F = 1;
    static final byte OP_NOT = 2;
    static final byte OP_AND = 3;
    static final byte OP_OR = 4;
    static final byte OP_NUM_CMP = 5;
    static final byte OP_STR_EQ = 6;
    static final byte OP_BOOL_REF = 7;

    /** Per-node opcode. Index 0 is the root. */
    byte[] ops;
    /** Child indices for AND/OR/NOT; -1 terminator. Packed contiguously. */
    int[] children;
    /** Per-node start index into {@link #children}. */
    int[] childStart;
    /** Per-node length in {@link #children}. */
    int[] childCount;
    /** Per-node fieldIdx (into {@link #fieldNameKeys}), -1 if N/A. */
    int[] fieldIdx;
    /** Per-node cmpOp (see OP_EQ/OP_GT/...), 0 if N/A. */
    int[] cmpOp;
    /** Per-node long literal (for NumCmp), 0 if N/A. */
    long[] longLit;
    /** Per-node string literal index (for StrEq), -1 if N/A. */
    int[] strIdx;

    /** Distinct field names referenced by the predicate, in first-seen order. */
    String[] fieldNames;
    /** Resolved nameKeys in the same order as {@link #fieldNames}; -1 = not found in document. */
    int[] fieldNameKeys;
    /** String literals used by StrEq leaves, de-duplicated by reference equality pre-intern. */
    String[] strLiterals;
    /** Bytes of each string literal in UTF-8 (for fast MemorySegment compare later). */
    byte[][] strLiteralBytes;
  }

  /**
   * Compile a {@link PredicateNode} tree to the flat {@link CompiledPredicate}
   * form. Called once per query; the compiled form is then consumed by the
   * hot loop below without any allocation.
   */
  private CompiledPredicate compile(final PredicateNode root) {
    // First pass: collect unique fields and string literals in deterministic order.
    final LinkedHashSet<String> fieldSet = new LinkedHashSet<>();
    final LinkedHashSet<String> strSet = new LinkedHashSet<>();
    collectLiterals(root, fieldSet, strSet);

    final CompiledPredicate cp = new CompiledPredicate();
    cp.fieldNames = fieldSet.toArray(new String[0]);
    cp.fieldNameKeys = new int[cp.fieldNames.length];
    for (int i = 0; i < cp.fieldNames.length; i++) {
      cp.fieldNameKeys[i] = resolveFieldKey(cp.fieldNames[i]);
    }
    cp.strLiterals = strSet.toArray(new String[0]);
    cp.strLiteralBytes = new byte[cp.strLiterals.length][];
    for (int i = 0; i < cp.strLiterals.length; i++) {
      cp.strLiteralBytes[i] = cp.strLiterals[i].getBytes(StandardCharsets.UTF_8);
    }

    // Second pass: flatten the tree into the parallel arrays.
    final ArrayList<Byte> ops = new ArrayList<>();
    final ArrayList<Integer> children = new ArrayList<>();
    final ArrayList<Integer> childStart = new ArrayList<>();
    final ArrayList<Integer> childCount = new ArrayList<>();
    final ArrayList<Integer> fieldIdx = new ArrayList<>();
    final ArrayList<Integer> cmpOp = new ArrayList<>();
    final ArrayList<Long> longLit = new ArrayList<>();
    final ArrayList<Integer> strIdx = new ArrayList<>();
    flatten(root, cp.fieldNames, cp.strLiterals, ops, children, childStart, childCount, fieldIdx, cmpOp, longLit,
            strIdx);

    cp.ops = new byte[ops.size()];
    for (int i = 0; i < ops.size(); i++) cp.ops[i] = ops.get(i);
    cp.children = new int[children.size()];
    for (int i = 0; i < children.size(); i++) cp.children[i] = children.get(i);
    cp.childStart = new int[childStart.size()];
    for (int i = 0; i < childStart.size(); i++) cp.childStart[i] = childStart.get(i);
    cp.childCount = new int[childCount.size()];
    for (int i = 0; i < childCount.size(); i++) cp.childCount[i] = childCount.get(i);
    cp.fieldIdx = new int[fieldIdx.size()];
    for (int i = 0; i < fieldIdx.size(); i++) cp.fieldIdx[i] = fieldIdx.get(i);
    cp.cmpOp = new int[cmpOp.size()];
    for (int i = 0; i < cmpOp.size(); i++) cp.cmpOp[i] = cmpOp.get(i);
    cp.longLit = new long[longLit.size()];
    for (int i = 0; i < longLit.size(); i++) cp.longLit[i] = longLit.get(i);
    cp.strIdx = new int[strIdx.size()];
    for (int i = 0; i < strIdx.size(); i++) cp.strIdx[i] = strIdx.get(i);
    return cp;
  }

  private static void collectLiterals(final PredicateNode n, final LinkedHashSet<String> fields,
      final LinkedHashSet<String> strs) {
    switch (n) {
      case PredicateNode.NumCmp nc -> fields.add(nc.field());
      case PredicateNode.StrEq se -> { fields.add(se.field()); strs.add(se.value()); }
      case PredicateNode.BoolRef br -> fields.add(br.field());
      case PredicateNode.Not nt -> collectLiterals(nt.child(), fields, strs);
      case PredicateNode.And a -> { for (PredicateNode c : a.children()) collectLiterals(c, fields, strs); }
      case PredicateNode.Or o -> { for (PredicateNode c : o.children()) collectLiterals(c, fields, strs); }
      case PredicateNode.AlwaysTrue t -> { }
      case PredicateNode.AlwaysFalse f -> { }
    }
  }

  /** Returns index of the appended node in {@code ops}. */
  private static int flatten(final PredicateNode n, final String[] fieldNames, final String[] strLits,
      final ArrayList<Byte> ops, final ArrayList<Integer> children, final ArrayList<Integer> childStart,
      final ArrayList<Integer> childCount, final ArrayList<Integer> fieldIdx, final ArrayList<Integer> cmpOp,
      final ArrayList<Long> longLit, final ArrayList<Integer> strIdx) {
    final int myIdx = ops.size();
    // Reserve slot.
    ops.add((byte) 0);
    childStart.add(0);
    childCount.add(0);
    fieldIdx.add(-1);
    cmpOp.add(0);
    longLit.add(0L);
    strIdx.add(-1);

    switch (n) {
      case PredicateNode.AlwaysTrue t -> ops.set(myIdx, CompiledPredicate.OP_ALWAYS_T);
      case PredicateNode.AlwaysFalse f -> ops.set(myIdx, CompiledPredicate.OP_ALWAYS_F);
      case PredicateNode.NumCmp nc -> {
        ops.set(myIdx, CompiledPredicate.OP_NUM_CMP);
        fieldIdx.set(myIdx, indexOf(fieldNames, nc.field()));
        cmpOp.set(myIdx, encodeOp(nc.op()));
        longLit.set(myIdx, nc.value());
      }
      case PredicateNode.StrEq se -> {
        ops.set(myIdx, CompiledPredicate.OP_STR_EQ);
        fieldIdx.set(myIdx, indexOf(fieldNames, se.field()));
        strIdx.set(myIdx, indexOf(strLits, se.value()));
      }
      case PredicateNode.BoolRef br -> {
        ops.set(myIdx, CompiledPredicate.OP_BOOL_REF);
        fieldIdx.set(myIdx, indexOf(fieldNames, br.field()));
      }
      case PredicateNode.Not nt -> {
        ops.set(myIdx, CompiledPredicate.OP_NOT);
        final int childIdx = flatten(nt.child(), fieldNames, strLits, ops, children, childStart, childCount, fieldIdx,
                                     cmpOp, longLit, strIdx);
        childStart.set(myIdx, children.size());
        children.add(childIdx);
        childCount.set(myIdx, 1);
      }
      case PredicateNode.And a -> {
        ops.set(myIdx, CompiledPredicate.OP_AND);
        final int start = children.size();
        // Reserve placeholder slots so child-indexing isn't re-entrant with `children`.
        for (int i = 0; i < a.children().size(); i++) children.add(-1);
        for (int i = 0; i < a.children().size(); i++) {
          children.set(start + i, flatten(a.children().get(i), fieldNames, strLits, ops, children, childStart,
                                          childCount, fieldIdx, cmpOp, longLit, strIdx));
        }
        childStart.set(myIdx, start);
        childCount.set(myIdx, a.children().size());
      }
      case PredicateNode.Or o -> {
        ops.set(myIdx, CompiledPredicate.OP_OR);
        final int start = children.size();
        for (int i = 0; i < o.children().size(); i++) children.add(-1);
        for (int i = 0; i < o.children().size(); i++) {
          children.set(start + i, flatten(o.children().get(i), fieldNames, strLits, ops, children, childStart,
                                          childCount, fieldIdx, cmpOp, longLit, strIdx));
        }
        childStart.set(myIdx, start);
        childCount.set(myIdx, o.children().size());
      }
    }
    return myIdx;
  }

  private static int indexOf(final String[] arr, final String s) {
    for (int i = 0; i < arr.length; i++) if (arr[i].equals(s)) return i;
    return -1;
  }

  /**
   * Scratch space consumed by the hot evaluator — pre-allocated per worker,
   * reused across every record. All arrays are indexed by fieldIdx.
   *
   * <p>{@code longVals} / {@code boolVals} / {@code strVals} hold the per-record
   * decoded value; {@code present} marks whether the field exists on the
   * record. The loader fills them by a single pass over the object's child
   * OBJECT_KEY slots, so we avoid one rtx.moveTo per field (N fields ⇒ 1 pass).
   */
  private static final class EvalScratch {
    long[] longVals;
    boolean[] boolVals;
    String[] strVals;
    byte[] fieldKind;  // 0 = missing, 1 = num, 2 = bool, 3 = str
    /** Work stack for iterative AND/OR short-circuit evaluation. */
    int[] stackNode;
    boolean[] stackRes;

    EvalScratch(final int nFields, final int nNodes) {
      longVals = new long[nFields];
      boolVals = new boolean[nFields];
      strVals = new String[nFields];
      fieldKind = new byte[nFields];
      stackNode = new int[Math.max(16, nNodes)];
      stackRes = new boolean[Math.max(16, nNodes)];
    }
  }

  /**
   * Generic per-record predicate evaluator. Walks every leaf page in parallel;
   * for each record it descends to each referenced field via the rtx cursor
   * and evaluates the {@link PredicateNode} tree recursively. Much slower than
   * the specialized PAX kernels but handles arbitrary shapes (OR, NOT,
   * 3+ conjuncts, StrEq, cross-field ANDs).
   *
   * <p>Hot-loop design:
   * <ul>
   *   <li>Compile the predicate once into {@link CompiledPredicate} — flat
   *       arrays, no allocations per record.</li>
   *   <li>Pre-resolve nameKeys once at compile time.</li>
   *   <li>Anchor on the first referenced field; iterate its OBJECT_KEY slots
   *       via {@code getObjectKeySlotsForNameKey}.</li>
   *   <li>Per record: single pass over child OBJECT_KEYs loads all needed
   *       field values into primitive scratch arrays. Then the evaluator
   *       walks the compiled predicate using an iterative stack — zero
   *       allocation, no boxing.</li>
   * </ul>
   */
  private long parallelGenericPredicateCount(final PredicateNode predicate) throws Exception {
    final CompiledPredicate cp = compile(predicate);
    if (cp.fieldNames.length == 0) return 0L;
    // Anchor on the first field — its nameKey drives slot iteration.
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) return 0L;

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final long[] perThread = new long[eff];

    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
      long localCount = 0;
      for (long pk = s; pk < e; pk++) {
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
        for (final int slot : matches) {
          final long anchorObjectKey = base + slot;
          if (!rtx.moveTo(anchorObjectKey)) continue;
          if (!rtx.moveToParent()) continue;  // enclosing object
          loadFields(rtx, cp, scratch);
          if (evalCompiled(cp, 0, scratch)) localCount++;
        }
      }
      perThread[idx] = localCount;
    });
    long total = 0;
    for (final long c : perThread) total += c;
    return total;
  }

  /**
   * Single pass over the current object's child OBJECT_KEYs; for each nameKey
   * we recognize, load its value into {@code scratch} as the appropriate
   * primitive. Missing fields stay {@code fieldKind=0}.
   *
   * <p>Assumes rtx is positioned on the enclosing OBJECT node.
   */
  private static void loadFields(final JsonNodeReadOnlyTrx rtx, final CompiledPredicate cp,
      final EvalScratch scratch) {
    for (int i = 0; i < scratch.fieldKind.length; i++) scratch.fieldKind[i] = 0;
    if (!rtx.moveToFirstChild()) return;
    final int[] wantKeys = cp.fieldNameKeys;
    do {
      final int nk = rtx.getNameKey();
      int fi = -1;
      for (int i = 0; i < wantKeys.length; i++) if (wantKeys[i] == nk) { fi = i; break; }
      if (fi < 0) continue;
      if (!rtx.moveToFirstChild()) { rtx.moveToParent(); continue; }
      final NodeKind kind = rtx.getKind();
      switch (kind) {
        case OBJECT_NUMBER_VALUE, NUMBER_VALUE -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            scratch.longVals[fi] = n.longValue();
            scratch.fieldKind[fi] = 1;
          }
        }
        case OBJECT_BOOLEAN_VALUE, BOOLEAN_VALUE -> {
          scratch.boolVals[fi] = rtx.getBooleanValue();
          scratch.fieldKind[fi] = 2;
        }
        case OBJECT_STRING_VALUE, STRING_VALUE -> {
          scratch.strVals[fi] = rtx.getValue();
          scratch.fieldKind[fi] = 3;
        }
        default -> { /* unsupported field type — leave kind=0 (missing) */ }
      }
      rtx.moveToParent();
    } while (rtx.moveToRightSibling());
  }

  /**
   * Recursive evaluator over the compiled predicate. Tail-recursive in the
   * AND/OR cases; the JIT inlines this heavily because it's shape-monomorphic
   * per query.
   */
  private static boolean evalCompiled(final CompiledPredicate cp, final int nodeIdx, final EvalScratch scratch) {
    final byte op = cp.ops[nodeIdx];
    switch (op) {
      case CompiledPredicate.OP_ALWAYS_T: return true;
      case CompiledPredicate.OP_ALWAYS_F: return false;
      case CompiledPredicate.OP_NOT:
        return !evalCompiled(cp, cp.children[cp.childStart[nodeIdx]], scratch);
      case CompiledPredicate.OP_AND: {
        final int start = cp.childStart[nodeIdx];
        final int n = cp.childCount[nodeIdx];
        for (int i = 0; i < n; i++) {
          if (!evalCompiled(cp, cp.children[start + i], scratch)) return false;
        }
        return true;
      }
      case CompiledPredicate.OP_OR: {
        final int start = cp.childStart[nodeIdx];
        final int n = cp.childCount[nodeIdx];
        for (int i = 0; i < n; i++) {
          if (evalCompiled(cp, cp.children[start + i], scratch)) return true;
        }
        return false;
      }
      case CompiledPredicate.OP_NUM_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        if (scratch.fieldKind[fi] != 1) return false;
        return scalarEval(scratch.longVals[fi], cp.cmpOp[nodeIdx], cp.longLit[nodeIdx]);
      }
      case CompiledPredicate.OP_STR_EQ: {
        final int fi = cp.fieldIdx[nodeIdx];
        if (scratch.fieldKind[fi] != 3) return false;
        return cp.strLiterals[cp.strIdx[nodeIdx]].equals(scratch.strVals[fi]);
      }
      case CompiledPredicate.OP_BOOL_REF: {
        final int fi = cp.fieldIdx[nodeIdx];
        if (scratch.fieldKind[fi] != 2) return false;
        return scratch.boolVals[fi];
      }
      default:
        throw new IllegalStateException("bad opcode " + op);
    }
  }

  @Override
  public Sequence executeCountDistinct(QueryContext ctx, String field) throws QueryException {
    try {
      // Only honour the short-circuit when the resource maintains an HLL per path.
      if (!session.getResourceConfig().withPathStatistics) {
        return null;
      }
      try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
        final var summary = rtx.getResourceSession().openPathSummary(revision);
        try {
          final var paths = summary.findPathsByLocalName(field);
          if (paths.isEmpty()) {
            return null;
          }
          HyperLogLogSketch union = null;
          for (final var p : paths) {
            final var hll = p.getHllSketch();
            if (hll == null) {
              continue;
            }
            if (union == null) {
              union = new HyperLogLogSketch();
            }
            union.union(hll);
          }
          return new Int64(union == null ? 0L : union.estimate());
        } finally {
          summary.close();
        }
      }
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized count-distinct failed: %s",
                               e.getMessage());
    }
  }

  @Override
  public Sequence executeAggregate(QueryContext ctx, String func, String field) throws QueryException {
    try {
      // PathStatistics short-circuit: when the resource maintains per-path stats, an
      // unfiltered aggregate over a single field resolves directly from the PathSummary
      // — microseconds instead of a parallel scan of every data page.
      final Sequence fast = tryPathSummaryStats(field, func);
      if (fast != null) {
        return fast;
      }
      // The executor is bound to a single (session, revision) — any aggregate we
      // computed earlier for this field is still valid. ComputeIfAbsent keeps
      // the scan exactly-once even under concurrent callers.
      long[] stats = aggregateCache.get(field);
      if (stats == null) {
        final long[] fresh = parallelAggregate(field);
        stats = aggregateCache.putIfAbsent(field, fresh);
        if (stats == null) stats = fresh;
      }
      long count = stats[0], sum = stats[1], min = stats[2], max = stats[3];
      return switch (func) {
        case "count" -> new Int64(count);
        case "sum" -> new Int64(sum);
        case "avg" -> count == 0 ? new Int64(0) : new Dbl((double) sum / (double) count);
        case "min" -> count == 0 ? new Int64(0) : new Int64(min);
        case "max" -> count == 0 ? new Int64(0) : new Int64(max);
        default -> null;  // unknown func → fall back
      };
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized aggregate failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Try to answer an unfiltered aggregate ({@code sum | avg | min | max | count}) over
   * a single field via the PathSummary's per-path statistics. Returns {@code null} to
   * signal the caller should fall back to a full parallel scan when:
   * <ul>
   *   <li>the resource doesn't maintain path statistics,</li>
   *   <li>no PathNode matches the requested field's local name,</li>
   *   <li>the requested min/max bound is marked dirty (a prior delete may have
   *       tightened it — a rescan is correct, skip the fast path), or</li>
   *   <li>the function is not one of the supported aggregates.</li>
   * </ul>
   */
  private Sequence tryPathSummaryStats(final String field, final String func) {
    if (!session.getResourceConfig().withPathStatistics) {
      return null;
    }
    switch (func) {
      case "count", "sum", "avg", "min", "max" -> { /* supported */ }
      default -> { return null; }
    }

    try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final PathSummaryReader summary = rtx.getResourceSession().openPathSummary(revision);
      try {
        final List<PathNode> paths = summary.findPathsByLocalName(field);
        if (paths.isEmpty()) {
          return null;
        }
        long totalCount = 0L;
        long totalSum = 0L;
        long totalMin = Long.MAX_VALUE;
        long totalMax = Long.MIN_VALUE;
        for (final PathNode p : paths) {
          if (("min".equals(func) || "avg".equals(func)) && p.isStatsMinDirty()) {
            return null;
          }
          if ("max".equals(func) && p.isStatsMaxDirty()) {
            return null;
          }
          totalCount += p.getStatsValueCount();
          totalSum += p.getStatsSum();
          if (p.hasNumericStats()) {
            if (p.getStatsMin() < totalMin) {
              totalMin = p.getStatsMin();
            }
            if (p.getStatsMax() > totalMax) {
              totalMax = p.getStatsMax();
            }
          }
        }
        return switch (func) {
          case "count" -> new Int64(totalCount);
          case "sum"   -> new Int64(totalSum);
          case "avg"   -> totalCount == 0 ? new Int64(0) : new Dbl((double) totalSum / (double) totalCount);
          case "min"   -> totalCount == 0 ? new Int64(0) : new Int64(totalMin);
          case "max"   -> totalCount == 0 ? new Int64(0) : new Int64(totalMax);
          default      -> null;
        };
      } finally {
        summary.close();
      }
    }
  }

  // ==================== Implementation ====================

  /**
   * Resolve the int nameKey for {@code field} once. We walk the file looking
   * for the first OBJECT_KEY whose local name equals {@code field}, then
   * cache the resulting int key. Subsequent inner-loop comparisons are pure
   * int compares — orders of magnitude faster than {@code String.equals}.
   *
   * @return the nameKey, or {@code -1} if no such field exists in the data
   */
  private int resolveFieldKey(String field) {
    Integer cached = fieldKeyCache.get(field);
    if (cached != null) return cached;
    synchronized (fieldKeyCache) {
      cached = fieldKeyCache.get(field);
      if (cached != null) return cached;
      int[] found = { -1 };
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        var reader = rtx.getStorageEngineReader();
        long totalPages = (getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
        for (long pk = 0; pk < totalPages && found[0] < 0; pk++) {
          var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
          if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
          long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
          kv.forEachPopulatedSlot(slot -> {
            if (kv.getSlotNodeKindId(slot) != OBJECT_KEY_KIND) return true;
            if (!rtx.moveTo(base + slot)) return true;
            var name = rtx.getName();
            if (name != null && field.equals(name.getLocalName())) {
              found[0] = rtx.getNameKey();
              return false;  // stop iterating
            }
            return true;
          });
        }
      }
      int foundKey = found[0];
      fieldKeyCache.put(field, foundKey);
      return foundKey;
    }
  }

  /**
   * Walk all leaf pages in parallel, find every OBJECT_KEY slot whose nameKey
   * matches the pre-resolved key for {@code field}, descend to its numeric
   * value via the page's slot relationships (skipping {@code moveTo} +
   * {@code Number} boxing), and accumulate.
   *
   * @return {@code [count, sum, min, max]}
   */
  private long[] parallelAggregate(String field) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey < 0) return new long[] { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };

    long maxNodeKey = getMaxNodeKey();
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, Math.max(1, totalPages));
    long ppt = (totalPages + eff - 1) / eff;

    long[][] perThread = new long[eff][4];  // [count, sum, min, max]
    for (int i = 0; i < eff; i++) {
      perThread[i][2] = Long.MAX_VALUE;
      perThread[i][3] = Long.MIN_VALUE;
    }

    parallel(eff, i -> {
      long[] acc = perThread[i];
      // Per-worker scratch for the SIMD aggregate kernel — out[0]=sum, out[1]=min, out[2]=max.
      // One allocation per worker, reused across every page.
      final long[] simdAggOut = new long[3];
      // Reuse one IndexLogKey across all pages for this worker — eliminates the per-page
      // allocation in the hot loop. Safe because getRecordPage() only reads the fields
      // and does not retain a reference to the key object.
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      long s = (long) i * ppt;
      long e = Math.min(s + ppt, totalPages);
      JsonNodeReadOnlyTrx rtx = workerTrx();
      var reader = rtx.getStorageEngineReader();
      final int numValKindId = KeyValueLeafPage.objectNumberValueKindId();
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        // PAX fast path: jump straight to the target field's value range via
        // per-tag directory. SIMD aggregate kernel computes sum/min/max in one
        // vector pass — single memory sweep, hardware-pipelined reductions.
        if (PAX_FAST_PATH_ENABLED) {
          final NumberRegion.Header hdr = kv.getNumberRegionHeader();
          if (hdr != null) {
            final int tag = NumberRegion.lookupTag(hdr, fieldKey);
            if (tag >= 0) {
              final int start = hdr.tagStart[tag];
              final int tagN = hdr.tagCount[tag];
              final int end = start + tagN;
              final MemorySegment payloadSeg = kv.getNumberRegionPayloadSegment();
              if (NumberRegionSimd.aggregateRange(payloadSeg, hdr, start, end, simdAggOut)) {
                acc[0] += tagN;
                acc[1] += simdAggOut[0];
                if (simdAggOut[1] < acc[2]) acc[2] = simdAggOut[1];
                if (simdAggOut[2] > acc[3]) acc[3] = simdAggOut[2];
                continue;
              }
              // SIMD fallback: bit-width > 56 or other unsupported encoding.
              final byte[] payload = kv.getNumberRegionPayload();
              for (int idx = start; idx < end; idx++) {
                final long v = NumberRegion.decodeValueAt(payload, hdr, idx);
                acc[0]++;
                acc[1] += v;
                if (v < acc[2]) acc[2] = v;
                if (v > acc[3]) acc[3] = v;
              }
              continue;
            }
          }
        }

        // Slow path: walk OBJECT_KEY slots matching the nameKey, descend to
        // firstChild. Used when the page has no number region (e.g. first-time
        // commit before Phase-1 writer landed) or the target field isn't in
        // the region's parent-nameKey dictionary.
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        for (final int slot : matches) {
          final long objectKeyNodeKey = base + slot;
          // Direct slot path: read firstChild's nodeKey straight off the page
          // (no cursor), then read the numeric value straight off the firstChild
          // slot. Skips moveTo + singleton bind + Number boxing per match.
          // Only works when firstChild is on the same page (common case for
          // flat JSON-array records where key + value land side-by-side).
          final long fcKey = kv.getObjectKeyFirstChildKeyFromSlot(slot, objectKeyNodeKey);
          final long fcPk = fcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
          long v;
          if (fcPk == pk) {
            final int fcSlot = (int) (fcKey - base);
            if (kv.getSlotNodeKindId(fcSlot) != numValKindId) continue;
            v = kv.getNumberValueLongFromSlot(fcSlot);
            if (v == Long.MIN_VALUE) {
              // Payload is float/double/BigDecimal — fall back to full path.
              if (!rtx.moveTo(fcKey)) continue;
              final Number n = rtx.getNumberValue();
              if (n == null) continue;
              v = n.longValue();
            }
          } else {
            // Cross-page first child — fall back to full cursor path.
            if (!rtx.moveTo(fcKey)) continue;
            final Number n = rtx.getNumberValue();
            if (n == null) continue;
            v = n.longValue();
          }
          acc[0]++;
          acc[1] += v;
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
        }
      }
    });

    long count = 0, sum = 0, min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (long[] a : perThread) {
      count += a[0];
      sum += a[1];
      if (a[0] > 0) {
        if (a[2] < min) min = a[2];
        if (a[3] > max) max = a[3];
      }
    }
    return new long[] { count, sum, min, max };
  }

  /**
   * Parallel filter-count with two AND-conjoined predicates on the same field.
   * Uses {@link NumberRegionSimd#countMatchingRange} to apply both predicates in
   * a single SIMD pass per tag range, eliminating the Brackit post-filter that a
   * serial two-step filter would pay (scan with predicate 1, then iterate matches
   * applying predicate 2).
   *
   * <p>Typical use: {@code count(... where age > 30 and age < 50 ...)} — two
   * comparators on the same field, yielding a range filter.
   */
  private long parallelFilterCountRange(String field, String op1str, long val1, String op2str, long val2)
      throws Exception {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey < 0) return 0;
    final int op1 = encodeOp(op1str);
    final int op2 = encodeOp(op2str);
    final VectorOperators.Comparison vop1 = vectorOp(op1);
    final VectorOperators.Comparison vop2 = vectorOp(op2);
    if (vop1 == null || vop2 == null) return 0; // unsupported op → caller falls back

    long maxNodeKey = getMaxNodeKey();
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, Math.max(1, totalPages));
    long ppt = (totalPages + eff - 1) / eff;
    long[] perThread = new long[eff];

    parallel(eff, i -> {
      long[] acc = perThread;
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      long s = (long) i * ppt;
      long e = Math.min(s + ppt, totalPages);
      JsonNodeReadOnlyTrx rtx = workerTrx();
      var reader = rtx.getStorageEngineReader();
      long localCount = 0;
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        if (!PAX_FAST_PATH_ENABLED) continue;
        final NumberRegion.Header hdr = kv.getNumberRegionHeader();
        if (hdr == null) continue;
        final int tag = NumberRegion.lookupTag(hdr, fieldKey);
        if (tag < 0) continue;
        final int start = hdr.tagStart[tag];
        final int tagN = hdr.tagCount[tag];
        if (tagN == 0) continue;
        final long tagMin = hdr.tagMinOrGlobal(tag);
        final long tagMax = hdr.tagMaxOrGlobal(tag);
        // Zone-map: both predicates must admit at least one value.
        final int o1 = zoneMapOutcome(op1, val1, tagMin, tagMax);
        final int o2 = zoneMapOutcome(op2, val2, tagMin, tagMax);
        if (o1 == ZONE_NONE || o2 == ZONE_NONE) continue;
        if (o1 == ZONE_ALL && o2 == ZONE_ALL) {
          localCount += tagN;
          continue;
        }
        final MemorySegment payloadSeg = kv.getNumberRegionPayloadSegment();
        final long matched = NumberRegionSimd.countMatchingRange(
            payloadSeg, hdr, start, start + tagN, vop1, val1, vop2, val2);
        if (matched >= 0) {
          localCount += matched;
        } else {
          // SIMD fallback — scalar.
          final byte[] payload = kv.getNumberRegionPayload();
          for (int idx = start; idx < start + tagN; idx++) {
            final long v = NumberRegion.decodeValueAt(payload, hdr, idx);
            if (scalarEval(v, op1, val1) && scalarEval(v, op2, val2)) localCount++;
          }
        }
      }
      acc[i] = localCount;
    });

    long total = 0;
    for (long c : perThread) total += c;
    return total;
  }

  private static boolean scalarEval(final long v, final int op, final long t) {
    return switch (op) {
      case OP_GT -> v > t;
      case OP_LT -> v < t;
      case OP_GE -> v >= t;
      case OP_LE -> v <= t;
      case OP_EQ -> v == t;
      default -> true;
    };
  }

  /**
   * Parallel filter-count: walks all leaf pages, finds OBJECT_KEY slots whose
   * nameKey matches the pre-resolved key, descends to numeric value, and
   * counts those satisfying {@code value OP threshold}.
   */
  private long parallelFilterCount(String filterField, String filterOp, long threshold) throws Exception {
    final int fieldKey = resolveFieldKey(filterField);
    if (fieldKey < 0) return 0;

    long maxNodeKey = getMaxNodeKey();
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, Math.max(1, totalPages));
    long ppt = (totalPages + eff - 1) / eff;
    long[] perThread = new long[eff];

    // Encode op once as int — int compare in hot loop instead of String.equals.
    final int op = encodeOp(filterOp);

    final ScanDiagnostics[] perThreadDiag = DIAGNOSTICS_ENABLED ? new ScanDiagnostics[eff] : null;

    parallel(eff, i -> {
      long[] acc = perThread;
      // Reuse one IndexLogKey across all pages for this worker (zero per-page alloc).
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      long s = (long) i * ppt;
      long e = Math.min(s + ppt, totalPages);
      JsonNodeReadOnlyTrx rtx = workerTrx();
      var reader = rtx.getStorageEngineReader();
      long localCount = 0;
      final ScanDiagnostics diag = DIAGNOSTICS_ENABLED ? new ScanDiagnostics() : null;
      if (DIAGNOSTICS_ENABLED) perThreadDiag[i] = diag;
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        if (DIAGNOSTICS_ENABLED) diag.pagesVisited++;
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        long localTotal = 0;

        // PAX fast path: iterate only the target field's range.
        if (PAX_FAST_PATH_ENABLED) {
          final NumberRegion.Header hdr = kv.getNumberRegionHeader();
          if (hdr != null) {
            final int tag = NumberRegion.lookupTag(hdr, fieldKey);
            if (tag >= 0) {
              final int tagN = hdr.tagCount[tag];
              // Zone-map pre-check: skip or shortcut the whole tag range if the
              // predicate is unsatisfiable / always-satisfied against [min,max].
              final long tagMin = hdr.tagMinOrGlobal(tag);
              final long tagMax = hdr.tagMaxOrGlobal(tag);
              final int outcome = zoneMapOutcome(op, threshold, tagMin, tagMax);
              if (outcome == ZONE_NONE) {
                if (DIAGNOSTICS_ENABLED) diag.pagesZoneSkippedAll++;
                continue;
              }
              if (outcome == ZONE_ALL) {
                localCount += tagN;
                if (DIAGNOSTICS_ENABLED) {
                  diag.pagesZoneAllPass++;
                  diag.recordsMatched += tagN;
                }
                continue;
              }
              final byte[] payload = kv.getNumberRegionPayload();
              final int start = hdr.tagStart[tag];
              final int end = start + tagN;

              // SIMD path: try to vectorize the decode + compare loop.
              // Falls back to scalar if the encoding/bit-width isn't supported
              // (NumberRegionSimd.countMatching returns -1 in that case).
              final VectorOperators.Comparison vecOp = vectorOp(op);
              long matched = -1L;
              if (vecOp != null) {
                // Use the cached MemorySegment view rather than ofArray(payload) — same bytes,
                // zero per-page wrapper allocation.
                final MemorySegment payloadSeg = kv.getNumberRegionPayloadSegment();
                matched = NumberRegionSimd.countMatching(
                    payloadSeg, hdr, start, end, vecOp, threshold);
              }
              if (matched < 0L) {
                // Scalar fallback — preserved verbatim.
                for (int idx = start; idx < end; idx++) {
                  final long v = NumberRegion.decodeValueAt(payload, hdr, idx);
                  final boolean pass = switch (op) {
                    case OP_GT -> v > threshold;
                    case OP_LT -> v < threshold;
                    case OP_GE -> v >= threshold;
                    case OP_LE -> v <= threshold;
                    case OP_EQ -> v == threshold;
                    default -> true;
                  };
                  if (pass) localTotal++;
                }
              } else {
                localTotal += matched;
              }
              localCount += localTotal;
              if (DIAGNOSTICS_ENABLED) {
                diag.pagesDecoded++;
                diag.recordsFastPath += tagN;
                diag.recordsMatched += localTotal;
              }
              continue;
            }
            if (DIAGNOSTICS_ENABLED) diag.pagesTagMissing++;
          } else {
            if (DIAGNOSTICS_ENABLED) diag.pagesNullHeader++;
          }
        }

        if (DIAGNOSTICS_ENABLED) diag.pagesSlowPath++;
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        for (final int slot : matches) {
          if (DIAGNOSTICS_ENABLED) diag.recordsSlowPath++;
          if (!rtx.moveTo(base + slot)) continue;
          if (!rtx.moveToFirstChild()) continue;
          Number n = rtx.getNumberValue();
          if (n == null) continue;
          long v = n.longValue();
          boolean pass = switch (op) {
            case OP_GT -> v > threshold;
            case OP_LT -> v < threshold;
            case OP_GE -> v >= threshold;
            case OP_LE -> v <= threshold;
            case OP_EQ -> v == threshold;
            default -> true;
          };
          if (pass) localTotal++;
        }
        localCount += localTotal;
        if (DIAGNOSTICS_ENABLED) diag.recordsMatched += localTotal;
      }
      acc[i] = localCount;
    });

    long total = 0;
    for (long c : perThread) total += c;

    if (DIAGNOSTICS_ENABLED && perThreadDiag != null) {
      final ScanDiagnostics merged = new ScanDiagnostics();
      for (final ScanDiagnostics d : perThreadDiag) if (d != null) merged.mergeFrom(d);
      merged.print("filterCount(" + filterField + " " + filterOp + " " + threshold + ")");
    }

    return total;
  }

  /**
   * Numeric-predicate filter fused with a boolean-field "is true" conjunct.
   * Handles the common {@code where $u.F OP V AND $u.B} shape the walker
   * surfaces via {@link io.brackit.query.compiler.optimizer.VectorizedScanAnnotation#FILTER_BOOL_FIELD}.
   *
   * <p><b>HFT-grade hot path.</b> Same shape as {@link #parallelFilterCount}
   * for the numeric side: NumberRegion zone-map shortcut + direct-slot
   * {@code getNumberValueLongFromSlot} read. On pass, the boolean is read
   * by navigating to the record's boolField OBJECT_KEY via bitmap + direct
   * slot node-kind check ({@code OBJECT_BOOLEAN_TRUE_VALUE_KIND_ID}); we
   * only fall back to rtx when the OBJECT_KEY or its first child is on a
   * different page. Per-worker scratch is one IndexLogKey.
   */
  private long parallelFilterCountAndBool(final String filterField, final String filterOp, final long threshold,
      final String boolField) throws Exception {
    final int fieldKey = resolveFieldKey(filterField);
    final int boolKey = resolveFieldKey(boolField);
    // resolveFieldKey uses -1 as the "not found" sentinel; the nameKey hash
    // itself can be any int — for e.g. "active" the hash is negative. Earlier
    // code checked {@code < 0} which silently returned zero for any field
    // whose hash happened to land in the negative half of int space.
    if (fieldKey == -1 || boolKey == -1) return 0;

    final int op = encodeOp(filterOp);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final long[] perThread = new long[eff];

    parallel(eff, i -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) i * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final int SLOT_MASK = Constants.NDP_NODE_COUNT - 1;
      long localCount = 0;

      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        final int[] filterMatches = kv.getObjectKeySlotsForNameKey(fieldKey);
        if (filterMatches.length == 0) continue;
        final int[] boolMatches = kv.getObjectKeySlotsForNameKey(boolKey);

        // Zone-map shortcut same as parallelFilterCount.
        final NumberRegion.Header nhdr = kv.getNumberRegionHeader();
        boolean zoneAll = false;
        if (nhdr != null) {
          final int tag = NumberRegion.lookupTag(nhdr, fieldKey);
          if (tag >= 0) {
            final long tagMin = nhdr.tagMinOrGlobal(tag);
            final long tagMax = nhdr.tagMaxOrGlobal(tag);
            final int outcome = zoneMapOutcome(op, threshold, tagMin, tagMax);
            if (outcome == ZONE_NONE) continue;
            zoneAll = outcome == ZONE_ALL;
          }
        }

        // Fast path: bitmap-order OBJECT_KEY slots for filter and bool
        // fields correspond to the same record at the same index when every
        // record on the page has both fields. Avoids the rtx moveToParent +
        // moveToFirstChild + walk-siblings dance per passing record.
        final boolean aligned = boolMatches.length == filterMatches.length;

        for (int m = 0; m < filterMatches.length; m++) {
          final int filterSlot = filterMatches[m];
          // Numeric predicate unless zone-map already proved all pass.
          if (!zoneAll) {
            final long filterKeyNodeKey = base + filterSlot;
            final long filterFcKey = kv.getObjectKeyFirstChildKeyFromSlot(filterSlot, filterKeyNodeKey);
            final long filterFcPk = filterFcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
            long filterVal;
            if (filterFcPk == pk) {
              final int fcSlot = (int) (filterFcKey & SLOT_MASK);
              filterVal = kv.getNumberValueLongFromSlot(fcSlot);
              if (filterVal == Long.MIN_VALUE) {
                if (!rtx.moveTo(filterFcKey)) continue;
                final Number n = rtx.getNumberValue();
                if (n == null) continue;
                filterVal = n.longValue();
              }
            } else {
              if (!rtx.moveTo(filterFcKey)) continue;
              final Number n = rtx.getNumberValue();
              if (n == null) continue;
              filterVal = n.longValue();
            }
            final boolean pass = switch (op) {
              case OP_GT -> filterVal > threshold;
              case OP_LT -> filterVal < threshold;
              case OP_GE -> filterVal >= threshold;
              case OP_LE -> filterVal <= threshold;
              case OP_EQ -> filterVal == threshold;
              default -> true;
            };
            if (!pass) continue;
          }

          // Numeric predicate passed — evaluate boolean conjunct.
          boolean boolVal;
          if (aligned) {
            // Direct by-index + direct-slot lookup — no rtx navigation.
            // getObjectBooleanValueFromSlot reads the value byte straight off
            // the slotted page (two memory loads). Falls back to rtx only
            // when the boolean value lives on a different page (rare for
            // flat-JSON datasets where key+value are adjacent slots).
            final int boolSlot = boolMatches[m];
            final long boolFcKey = kv.getObjectKeyFirstChildKeyFromSlot(boolSlot, base + boolSlot);
            final long boolFcPk = boolFcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
            if (boolFcPk == pk) {
              final int fcSlot = (int) (boolFcKey & SLOT_MASK);
              boolVal = kv.getObjectBooleanValueFromSlot(fcSlot);
            } else {
              if (!rtx.moveTo(boolFcKey)) continue;
              boolVal = rtx.getBooleanValue();
            }
          } else {
            // Fallback: walk parent object's children for boolField.
            final long filterKeyNodeKey = base + filterSlot;
            if (!rtx.moveTo(filterKeyNodeKey)) continue;
            if (!rtx.moveToParent()) continue;
            if (!rtx.moveToFirstChild()) continue;
            boolean matched = false;
            do {
              if (rtx.getNameKey() == boolKey) {
                if (rtx.moveToFirstChild()) matched = rtx.getBooleanValue();
                break;
              }
            } while (rtx.moveToRightSibling());
            boolVal = matched;
          }
          if (boolVal) localCount++;
        }
      }
      perThread[i] = localCount;
    });

    long total = 0;
    for (long c : perThread) total += c;
    return total;
  }

  /**
   * Same-field two-predicate numeric range AND a boolean-field "is true"
   * conjunct. Mirrors {@link #parallelFilterCountAndBool} but applies both
   * op1/value1 and op2/value2 scalar tests before the boolean navigation
   * (boolean read uses the direct-slot accessor; rtx fallback for cross-page).
   */
  private long parallelFilterCountRangeAndBool(final String field, final String op1str, final long v1,
      final String op2str, final long v2, final String boolField) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    final int boolKey = resolveFieldKey(boolField);
    if (fieldKey == -1 || boolKey == -1) return 0;

    final int op1 = encodeOp(op1str);
    final int op2 = encodeOp(op2str);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final long[] perThread = new long[eff];

    parallel(eff, i -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) i * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final int SLOT_MASK = Constants.NDP_NODE_COUNT - 1;
      long localCount = 0;

      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        final int[] filterMatches = kv.getObjectKeySlotsForNameKey(fieldKey);
        if (filterMatches.length == 0) continue;
        final int[] boolMatches = kv.getObjectKeySlotsForNameKey(boolKey);
        final boolean aligned = boolMatches.length == filterMatches.length;

        for (int m = 0; m < filterMatches.length; m++) {
          final int filterSlot = filterMatches[m];
          // Two-predicate numeric check.
          final long filterKeyNodeKey = base + filterSlot;
          final long filterFcKey = kv.getObjectKeyFirstChildKeyFromSlot(filterSlot, filterKeyNodeKey);
          final long filterFcPk = filterFcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
          long v;
          if (filterFcPk == pk) {
            final int fcSlot = (int) (filterFcKey & SLOT_MASK);
            v = kv.getNumberValueLongFromSlot(fcSlot);
            if (v == Long.MIN_VALUE) {
              if (!rtx.moveTo(filterFcKey)) continue;
              final Number n = rtx.getNumberValue();
              if (n == null) continue;
              v = n.longValue();
            }
          } else {
            if (!rtx.moveTo(filterFcKey)) continue;
            final Number n = rtx.getNumberValue();
            if (n == null) continue;
            v = n.longValue();
          }
          if (!scalarEval(v, op1, v1) || !scalarEval(v, op2, v2)) continue;

          // Boolean conjunct — direct-slot read on same-page, rtx fallback otherwise.
          boolean boolVal;
          if (aligned) {
            final int boolSlot = boolMatches[m];
            final long boolFcKey = kv.getObjectKeyFirstChildKeyFromSlot(boolSlot, base + boolSlot);
            final long boolFcPk = boolFcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
            if (boolFcPk == pk) {
              final int fcSlot = (int) (boolFcKey & SLOT_MASK);
              boolVal = kv.getObjectBooleanValueFromSlot(fcSlot);
            } else {
              if (!rtx.moveTo(boolFcKey)) continue;
              boolVal = rtx.getBooleanValue();
            }
          } else {
            if (!rtx.moveTo(filterKeyNodeKey)) continue;
            if (!rtx.moveToParent()) continue;
            if (!rtx.moveToFirstChild()) continue;
            boolean matched = false;
            do {
              if (rtx.getNameKey() == boolKey) {
                if (rtx.moveToFirstChild()) matched = rtx.getBooleanValue();
                break;
              }
            } while (rtx.moveToRightSibling());
            boolVal = matched;
          }
          if (boolVal) localCount++;
        }
      }
      perThread[i] = localCount;
    });

    long total = 0;
    for (long c : perThread) total += c;
    return total;
  }

  /**
   * Parallel group-by-count: walks all leaf pages, finds OBJECT_KEY slots whose
   * nameKey matches the pre-resolved key, descends to value, accumulates counts
   * per unique value into a 1BRC-style open-addressing byte-key hash map
   * (per-thread, then merged). Returns a Brackit array of
   * {@code {"groupField": value, "count": N}} objects.
   */
  private Sequence parallelGroupByCount(String groupField) throws Exception {
    final int fieldKey = resolveFieldKey(groupField);
    if (fieldKey < 0) return new DArray(List.of());

    long maxNodeKey = getMaxNodeKey();
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    // Per-page work on the StringRegion fast path is tiny (one 64-bit unaligned
    // load + shift + mask + array store per record). At 20 threads on a 10K-page
    // scan the per-thread workload is ~550 pages ≈ 50 µs — which is comparable
    // to Executor.submit + Future.get + FutureTask allocation on the submit
    // fan-out. Bench: 20 threads → min 35.8 ms / 4 threads → min 6.3 ms at 1 M
    // records (6× win). Floor the split at ~2.5 K pages/thread so small scans
    // don't pay for fan-out they can't amortize; large scans still reach the
    // configured thread count once they have enough work to justify it.
    final int MIN_PAGES_PER_THREAD = 2500;
    final long maxEffByWork = Math.max(1L, (totalPages + MIN_PAGES_PER_THREAD - 1) / MIN_PAGES_PER_THREAD);
    int eff = (int) Math.min(Math.min(threads, Math.max(1, totalPages)), maxEffByWork);
    long ppt = (totalPages + eff - 1) / eff;

    final ScanResult.GroupByResult[] perThread = new ScanResult.GroupByResult[eff];
    for (int i = 0; i < eff; i++) perThread[i] = new ScanResult.GroupByResult();
    final ScanDiagnostics[] perThreadDiag = DIAGNOSTICS_ENABLED ? new ScanDiagnostics[eff] : null;

    parallel(eff, i -> {
      ScanResult.GroupByResult acc = perThread[i];
      // Reuse one IndexLogKey across all pages for this worker (zero per-page alloc).
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      long s = (long) i * ppt;
      long e = Math.min(s + ppt, totalPages);
      JsonNodeReadOnlyTrx rtx = workerTrx();
      var reader = rtx.getStorageEngineReader();
      final ScanDiagnostics diag = DIAGNOSTICS_ENABLED ? new ScanDiagnostics() : null;
      if (DIAGNOSTICS_ENABLED) perThreadDiag[i] = diag;
      // Per-thread scratch for direct-slot value reads. Handles FSST-compressed
      // values inline; cross-page children / oversized values fall back to rtx.
      final byte[] scratch = new byte[256];
      final int SLOT_MASK = Constants.NDP_NODE_COUNT - 1;
      // Per-page scratch for the StringRegion fast path: counts per local
      // dict-id (≤ 256 supported by 8-bit lane width). Reset per page.
      final long[] localDictCounts = new long[256];
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        if (DIAGNOSTICS_ENABLED) {
          diag.pagesVisited++;
          diag.pagesSlowPath++;
        }
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        // Fast path: if the page exposes a StringRegion, count locally per
        // dict-id (8-entry bump per page) then emit one aggregated add per
        // local dict id (8 hash inserts/page instead of 90). Saves the per-
        // record byte-hash + insert that the slot-walk path pays.
        final StringRegion.Header sh = kv.getStringRegionHeader();
        if (sh == null) {
          if (DIAGNOSTICS_ENABLED) diag.stringRegionMissesNoHeader++;
        } else {
          final int tag = StringRegion.lookupTag(sh, fieldKey);
          if (tag < 0) {
            if (DIAGNOSTICS_ENABLED) diag.stringRegionMissesNoTag++;
          } else {
            final byte[] payload = kv.getStringRegionPayload();
            final int dictSize = sh.tagStringDictSize[tag];
            if (dictSize > localDictCounts.length) {
              if (DIAGNOSTICS_ENABLED) diag.stringRegionMissesDictBig++;
            } else {
              if (DIAGNOSTICS_ENABLED) diag.stringRegionHits++;
              // Reset only what we'll touch.
              for (int d = 0; d < dictSize; d++) localDictCounts[d] = 0;
              final int start = sh.tagStart[tag];
              final int n = sh.tagCount[tag];
              for (int idx = 0; idx < n; idx++) {
                final int dictId =
                    StringRegion.decodeDictIdAt(payload, sh, start + idx);
                localDictCounts[dictId]++;
              }
              for (int d = 0; d < dictSize; d++) {
                final long c = localDictCounts[d];
                if (c == 0) continue;
                final int off = StringRegion.decodeStringOffset(payload, sh, tag, d);
                final int len = StringRegion.decodeStringLength(payload, sh, tag, d);
                acc.addN(payload, off, len, c);
                if (DIAGNOSTICS_ENABLED) diag.recordsMatched += c;
              }
              continue;
            }
          }
        }

        // Slow path: walk OBJECT_KEY slots, descend to value, hash-insert.
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        for (final int slot : matches) {
          if (DIAGNOSTICS_ENABLED) diag.recordsSlowPath++;

          final long firstChildKey = kv.getObjectKeyFirstChildKeyFromSlot(slot, base + slot);
          final long childPk = firstChildKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
          if (childPk == pk) {
            final int valueSlot = (int) (firstChildKey & SLOT_MASK);
            final int n = kv.readObjectStringValueBytesFromSlot(valueSlot, scratch);
            if (n > 0) {
              acc.add(scratch, 0, n);
              if (DIAGNOSTICS_ENABLED) {
                diag.groupByDirectSuccess++;
                diag.recordsMatched++;
              }
              continue;
            }
            if (DIAGNOSTICS_ENABLED) diag.groupByDirectNeg++;
          } else {
            if (DIAGNOSTICS_ENABLED) diag.groupByCrossPage++;
          }

          if (DIAGNOSTICS_ENABLED) diag.groupByRtxFallback++;
          if (!rtx.moveTo(base + slot)) continue;
          if (!rtx.moveToFirstChild()) continue;
          byte[] valueBytes = rtx.getValueBytes();
          if (valueBytes != null && valueBytes.length > 0) {
            acc.add(valueBytes, 0, valueBytes.length);
            if (DIAGNOSTICS_ENABLED) diag.recordsMatched++;
          }
        }
      }
    });

    // Merge per-thread maps into one
    ScanResult.GroupByResult merged = perThread[0];
    for (int i = 1; i < eff; i++) merged.merge(perThread[i]);

    if (DIAGNOSTICS_ENABLED && perThreadDiag != null) {
      final ScanDiagnostics mergedDiag = new ScanDiagnostics();
      for (final ScanDiagnostics d : perThreadDiag) if (d != null) mergedDiag.mergeFrom(d);
      mergedDiag.print("groupByCount(" + groupField + ")");
    }

    // Build Brackit result: array of {groupField: value, "count": N}
    List<Item> items = new ArrayList<>(merged.size());
    final QNm groupFieldQNm = new QNm(groupField);
    final QNm countQNm = new QNm("count");
    merged.forEach((key, count) -> {
      QNm[] fields = { groupFieldQNm, countQNm };
      Sequence[] values = { new Str(new String(key, StandardCharsets.UTF_8)), new Int64(count) };
      items.add(new ArrayObject(fields, values));
    });
    return new DArray(items);
  }

  /**
   * Correct filter-then-group-by for a numeric predicate on {@code filterField}
   * and a string group-key {@code groupField}.
   *
   * <p><b>HFT-grade hot path.</b>
   * <ul>
   *   <li>Per-worker scratch buffers are allocated <i>once</i> outside the
   *       page loop and reused across every page: one
   *       {@link io.sirix.cache.IndexLogKey}, one {@code byte[256]} for the
   *       group-value direct-slot read, and the per-thread
   *       {@link ScanResult.GroupByResult} accumulator.</li>
   *   <li>Filter value is read directly off the slotted page via
   *       {@link KeyValueLeafPage#getNumberValueLongFromSlot}; we fall back
   *       to {@link JsonNodeReadOnlyTrx#moveTo}/{@code getNumberValue} only
   *       for cross-page first-children or values stored as float/double
   *       (signalled by {@code Long.MIN_VALUE} sentinel).</li>
   *   <li>Group value is read via
   *       {@link KeyValueLeafPage#readObjectStringValueBytesFromSlot} into
   *       the per-worker scratch, then {@link ScanResult.GroupByResult#add}
   *       copies on insert (the one unavoidable alloc per <i>new</i>
   *       dictionary key); no cursor walk when the record's entire object
   *       is on-page — which is the common flat-JSON case.</li>
   *   <li>Fan-out is capped at {@code MIN_PAGES_PER_THREAD=2500} so small
   *       scans don't pay the fixed executor submit+Future.get overhead
   *       (same rationale as {@link #parallelGroupByCount}).</li>
   * </ul>
   */
  private Sequence parallelFilteredGroupByCount(
      final String groupField, final String filterField,
      final String filterOp, final long filterValue) throws Exception {
    final int groupKey = resolveFieldKey(groupField);
    final int filterKey = resolveFieldKey(filterField);
    if (groupKey < 0 || filterKey < 0) return new DArray(List.of());

    final int op = encodeOp(filterOp);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int MIN_PAGES_PER_THREAD = 2500;
    final long maxEffByWork = Math.max(1L,
        (totalPages + MIN_PAGES_PER_THREAD - 1) / MIN_PAGES_PER_THREAD);
    final int eff = (int) Math.min(Math.min(threads, Math.max(1, totalPages)), maxEffByWork);
    final long ppt = (totalPages + eff - 1) / eff;

    final ScanResult.GroupByResult[] perThread = new ScanResult.GroupByResult[eff];
    for (int i = 0; i < eff; i++) perThread[i] = new ScanResult.GroupByResult();

    parallel(eff, i -> {
      final ScanResult.GroupByResult acc = perThread[i];
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final byte[] gScratch = new byte[256];
      // Per-tag dict-id count buffer reused across all pages. 8-bit wide is
      // plenty for benchmark group-field dictionaries (8 depts, 8 cities, etc.);
      // tag-dict-size bigger than this falls through to the rtx path.
      final long[] localDictCounts = new long[256];
      final long s = (long) i * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final int SLOT_MASK = Constants.NDP_NODE_COUNT - 1;
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        // PAX fast path: NumberRegion filter tag + StringRegion group tag are both
        // grouped by record-order per tag. When their tag counts match we can
        // correlate by index — record i's filter value is at (nTagStart+i) and its
        // group dict-id is at (sTagStart+i). Per-page cost then collapses to an
        // index-aligned decode+predicate+dict-bump loop, no cursor navigation.
        final NumberRegion.Header nhdr = kv.getNumberRegionHeader();
        final StringRegion.Header shdr = kv.getStringRegionHeader();
        if (nhdr != null && shdr != null) {
          final int nTag = NumberRegion.lookupTag(nhdr, filterKey);
          final int sTag = StringRegion.lookupTag(shdr, groupKey);
          if (nTag >= 0 && sTag >= 0) {
            final int nTagN = nhdr.tagCount[nTag];
            final int sTagN = shdr.tagCount[sTag];
            final int sDictSize = shdr.tagStringDictSize[sTag];
            if (nTagN == sTagN && sDictSize <= localDictCounts.length) {
              for (int d = 0; d < sDictSize; d++) localDictCounts[d] = 0;

              final byte[] numPayload = kv.getNumberRegionPayload();
              final byte[] strPayload = kv.getStringRegionPayload();
              final int nStart = nhdr.tagStart[nTag];
              final int sStart = shdr.tagStart[sTag];

              final long tagMin = nhdr.tagMinOrGlobal(nTag);
              final long tagMax = nhdr.tagMaxOrGlobal(nTag);
              final int outcome = zoneMapOutcome(op, filterValue, tagMin, tagMax);
              if (outcome == ZONE_ALL) {
                // Every record on this page passes — bump every dict-id at its position.
                for (int idx = 0; idx < nTagN; idx++) {
                  final int dictId = StringRegion.decodeDictIdAt(strPayload, shdr, sStart + idx);
                  localDictCounts[dictId]++;
                }
              } else if (outcome != ZONE_NONE) {
                // Per-record: decode filter value, evaluate predicate, bump group dict on pass.
                for (int idx = 0; idx < nTagN; idx++) {
                  final long v = NumberRegion.decodeValueAt(numPayload, nhdr, nStart + idx);
                  final boolean pass = switch (op) {
                    case OP_GT -> v > filterValue;
                    case OP_LT -> v < filterValue;
                    case OP_GE -> v >= filterValue;
                    case OP_LE -> v <= filterValue;
                    case OP_EQ -> v == filterValue;
                    default -> true;
                  };
                  if (pass) {
                    final int dictId = StringRegion.decodeDictIdAt(strPayload, shdr, sStart + idx);
                    localDictCounts[dictId]++;
                  }
                }
              }
              // ZONE_NONE: nothing to count, localDictCounts stays zero, emit skipped.

              // Emit the page's per-dict-id aggregate.
              for (int d = 0; d < sDictSize; d++) {
                final long c = localDictCounts[d];
                if (c == 0) continue;
                final int off = StringRegion.decodeStringOffset(strPayload, shdr, sTag, d);
                final int len = StringRegion.decodeStringLength(strPayload, shdr, sTag, d);
                acc.addN(strPayload, off, len, c);
              }
              continue;
            }
          }
        }

        // Slow fallback: walk the filter field's OBJECT_KEY slots — one per record.
        final int[] filterMatches = kv.getObjectKeySlotsForNameKey(filterKey);
        if (filterMatches.length == 0) continue;

        // Same zone-map shortcut as above but for the rtx path.
        if (nhdr != null) {
          final int tag = NumberRegion.lookupTag(nhdr, filterKey);
          if (tag >= 0) {
            final long tagMin = nhdr.tagMinOrGlobal(tag);
            final long tagMax = nhdr.tagMaxOrGlobal(tag);
            final int outcome = zoneMapOutcome(op, filterValue, tagMin, tagMax);
            if (outcome == ZONE_NONE) continue;
            // ZONE_ALL falls through: still need per-record group lookup.
          }
        }

        for (final int filterSlot : filterMatches) {
          final long filterKeyNodeKey = base + filterSlot;
          final long filterFcKey = kv.getObjectKeyFirstChildKeyFromSlot(filterSlot, filterKeyNodeKey);
          final long filterFcPk = filterFcKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
          long filterVal;
          if (filterFcPk == pk) {
            final int fcSlot = (int) (filterFcKey & SLOT_MASK);
            filterVal = kv.getNumberValueLongFromSlot(fcSlot);
            if (filterVal == Long.MIN_VALUE) {
              // Non-long value (double/BigDecimal) — fall back to rtx once.
              if (!rtx.moveTo(filterFcKey)) continue;
              final Number n = rtx.getNumberValue();
              if (n == null) continue;
              filterVal = n.longValue();
            }
          } else {
            if (!rtx.moveTo(filterFcKey)) continue;
            final Number n = rtx.getNumberValue();
            if (n == null) continue;
            filterVal = n.longValue();
          }

          final boolean pass = switch (op) {
            case OP_GT -> filterVal > filterValue;
            case OP_LT -> filterVal < filterValue;
            case OP_GE -> filterVal >= filterValue;
            case OP_LE -> filterVal <= filterValue;
            case OP_EQ -> filterVal == filterValue;
            default -> true;
          };
          if (!pass) continue;

          // Passed predicate — navigate to this record's group-field OBJECT_KEY.
          if (!rtx.moveTo(filterKeyNodeKey)) continue;
          if (!rtx.moveToParent()) continue;
          if (!rtx.moveToFirstChild()) continue;
          do {
            if (rtx.getNameKey() == groupKey) {
              if (rtx.moveToFirstChild()) {
                final long gNodeKey = rtx.getNodeKey();
                final long gPk = gNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
                if (gPk == pk) {
                  // Same-page group value — direct-slot read into scratch, no alloc
                  // except the one on new-key insertion inside GroupByResult.add.
                  final int gSlot = (int) (gNodeKey & SLOT_MASK);
                  final int n = kv.readObjectStringValueBytesFromSlot(gSlot, gScratch);
                  if (n > 0) acc.add(gScratch, 0, n);
                } else {
                  // Cross-page group value — rtx fallback; getValueBytes allocates.
                  final byte[] gv = rtx.getValueBytes();
                  if (gv != null && gv.length > 0) acc.add(gv);
                }
              }
              break;
            }
          } while (rtx.moveToRightSibling());
        }
      }
    });

    ScanResult.GroupByResult merged = perThread[0];
    for (int i = 1; i < eff; i++) merged.merge(perThread[i]);

    final List<Item> items = new ArrayList<>(merged.size());
    final QNm groupFieldQNm = new QNm(groupField);
    final QNm countQNm = new QNm("count");
    merged.forEach((k, count) -> {
      final QNm[] fields = { groupFieldQNm, countQNm };
      final Sequence[] values = { new Str(new String(k, StandardCharsets.UTF_8)), new Int64(count) };
      items.add(new ArrayObject(fields, values));
    });
    return new DArray(items);
  }

  // Filter operator encoding — int compare in hot loop, decoded once.
  private static final int OP_GT = 1;
  private static final int OP_LT = 2;
  private static final int OP_GE = 3;
  private static final int OP_LE = 4;
  private static final int OP_EQ = 5;

  // Zone-map pre-check outcomes — lets the scan skip a tag range or shortcut
  // the count without iterating values.
  private static final int ZONE_NONE    = 0; // predicate can never match any value in [min,max]
  private static final int ZONE_SOME    = 1; // predicate might match; need to iterate
  private static final int ZONE_ALL     = 2; // predicate matches every value in [min,max]

  /**
   * Evaluate whether the predicate {@code v OP threshold} can be decided against the
   * [min,max] range of a zone map without iterating the values.
   * Branchless-friendly: six int comparisons, one small switch.
   */
  private static int zoneMapOutcome(final int op, final long threshold, final long min, final long max) {
    return switch (op) {
      case OP_GT -> max <= threshold ? ZONE_NONE : (min > threshold ? ZONE_ALL : ZONE_SOME);
      case OP_LT -> min >= threshold ? ZONE_NONE : (max < threshold ? ZONE_ALL : ZONE_SOME);
      case OP_GE -> max <  threshold ? ZONE_NONE : (min >= threshold ? ZONE_ALL : ZONE_SOME);
      case OP_LE -> min >  threshold ? ZONE_NONE : (max <= threshold ? ZONE_ALL : ZONE_SOME);
      case OP_EQ -> (threshold < min || threshold > max)
          ? ZONE_NONE
          : (min == threshold && max == threshold ? ZONE_ALL : ZONE_SOME);
      default    -> ZONE_SOME;
    };
  }

  private static int encodeOp(String op) {
    if (op == null) return 0;
    return switch (op) {
      case "gt" -> OP_GT;
      case "lt" -> OP_LT;
      case "ge" -> OP_GE;
      case "le" -> OP_LE;
      case "eq" -> OP_EQ;
      default -> 0;
    };
  }

  /**
   * Map our internal int-coded op to the Vector API's comparison constant.
   * Returns {@code null} for unsupported ops (caller falls back to scalar).
   */
  private static VectorOperators.Comparison vectorOp(final int op) {
    return switch (op) {
      case OP_GT -> VectorOperators.GT;
      case OP_LT -> VectorOperators.LT;
      case OP_GE -> VectorOperators.GE;
      case OP_LE -> VectorOperators.LE;
      case OP_EQ -> VectorOperators.EQ;
      default -> null;
    };
  }

  private long getMaxNodeKey() {
    long cached = cachedMaxNodeKey;
    if (cached >= 0) return cached;
    try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
      long v = rtx.getMaxNodeKey();
      cachedMaxNodeKey = v;
      return v;
    }
  }

  @FunctionalInterface
  private interface ChunkTask {
    void run(int threadIndex) throws Exception;
  }

  private void parallel(int n, ChunkTask task) {
    try {
      Future<?>[] futures = new Future[n];
      for (int i = 0; i < n; i++) {
        int idx = i;
        futures[i] = workerPool.submit(() -> {
          task.run(idx);
          return null;
        });
      }
      for (Future<?> f : futures) f.get();
    } catch (Exception e) {
      throw new RuntimeException("Parallel scan failed", e);
    }
  }
}
