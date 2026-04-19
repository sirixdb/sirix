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
import io.brackit.query.util.simd.VectorOps;
import io.brackit.query.util.simd.VectorizedPredicate;
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

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

import jdk.incubator.vector.VectorOperators;

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.Label;
import java.lang.classfile.MethodBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
   * Toggle the column-batched generic-predicate evaluator. When {@code true}
   * (default), {@link #parallelGenericPredicateCount} and
   * {@link #parallelGenericPredicateGroupByCount} collect each page's matching
   * records into per-field primitive columns and evaluate the compiled predicate
   * against those columns into a bitmap — amortising rtx navigation / dispatch
   * overhead across the page. When {@code false}, falls back to the legacy
   * record-at-a-time interpreter. Use {@code -Dsirix.vec.batchGenericEval=false}
   * for A/B regression bisection.
   */
  private static final boolean BATCH_GENERIC_EVAL_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.vec.batchGenericEval", "true"));

  /**
   * Toggle per-predicate JIT-style class generation via {@link java.lang.classfile}.
   * When {@code true} (default), each distinct {@link CompiledPredicate} tree is
   * compiled to a dedicated hidden class implementing {@link BatchPredicate} — the
   * opcode switch + recursive tree walk collapse to straight-line column kernels
   * with all constants (comparison threshold, cmp op, fieldIdx) inlined. When
   * {@code false}, {@link #evalCompiledBatch} interprets the flat op-array.
   * Use {@code -Dsirix.vec.compiledPredicate=false} for A/B bisection.
   */
  private static final boolean COMPILED_PREDICATE_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.vec.compiledPredicate", "true"));

  /**
   * Toggle direct-slot column feed in {@link #collectColumns}. When {@code true}
   * (default), multi-field predicates collect each referenced column via a
   * name-key slot scan + parent-OBJECT lookup table — bypassing
   * {@code rtx.moveToFirstChild + moveToRightSibling} per record. When
   * {@code false}, every multi-field page falls back to the legacy rtx cursor
   * walk ({@link #loadFieldsIntoBatch}). Use
   * {@code -Dsirix.vec.directSlotColumns=false} for A/B bisection.
   */
  private static final boolean DIRECT_SLOT_COLUMNS_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.vec.directSlotColumns", "true"));

  /** Monotonic counter for generated-class name uniqueness (aids in debugger symbol tables). */
  private static final AtomicInteger COMPILED_PREDICATE_SEQ = new AtomicInteger();

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
   * {@link #parallelGenericPredicateCount(PredicateNode)}. Key is the
   * canonicalized predicate signature (field + op + value); value is the
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

  /**
   * Per-(sourcePath, field) cache of the resolved {@code pathNodeKey} for the
   * fully-qualified query path. Hot queries (dashboards that re-issue the
   * same aggregate/filter) pay the path-summary walk once; subsequent lookups
   * are a {@link ConcurrentHashMap} probe. Value {@code -1L} records the
   * "no such path" decision so we don't re-attempt resolution.
   *
   * <p>Safe for the executor's (session, revision) lifetime because the path
   * summary is immutable for a read-only revision.
   */
  private final ConcurrentHashMap<String, Long> pathNodeKeyCache = new ConcurrentHashMap<>();

  /**
   * Cache of {@link #tryPathSummaryStats(String[], String, String)} results,
   * keyed by {@code pathNodeKey + "#" + func}. The executor is bound to a
   * single (session, revision) so each PathNode's statistics are stable for
   * its lifetime; we don't need to reopen the PathSummary for repeat aggregate
   * queries on the same path. Without this cache every {@code sum($doc[].age)}
   * call paid the cost of {@code beginNodeReadOnlyTrx + openPathSummary} —
   * roughly 0.5–1 ms per query on 10 M records, completely dominating the
   * µs-level stats lookup itself.
   */
  private final ConcurrentHashMap<String, Sequence> pathStatsCache = new ConcurrentHashMap<>();

  /**
   * Cache of count-distinct results (HLL estimate) per source path + field.
   * Same rationale as {@link #pathStatsCache}: the underlying HLL sketch is
   * immutable for the executor's (session, revision), so there's no reason
   * to reopen the PathSummary on repeat calls. Key is
   * {@code pathCacheKey(sourcePath, field)}.
   */
  private final ConcurrentHashMap<String, Sequence> countDistinctResultCache = new ConcurrentHashMap<>();

  /** Cache of aggregated scans keyed by predicate + field + func (for predicate aggregate). */
  private final ConcurrentHashMap<String, Sequence> predicateAggregateCache = new ConcurrentHashMap<>();

  /**
   * Per-predicate cache of generated {@link BatchPredicate} instances. Key is
   * {@code predicateCacheKey} plus a field-layout suffix so two predicates with
   * structurally identical trees but different field indices get distinct
   * classes. The generated class is a hidden class loaded under an anonymous
   * Lookup — it is GC'd when its last reference (this map entry) is cleared.
   */
  private final ConcurrentHashMap<String, BatchPredicate> compiledPredicateCache = new ConcurrentHashMap<>();

  /**
   * Keys recorded in this set represent predicates whose
   * {@link #compileToClass(CompiledPredicate)} attempt threw. We fall back to
   * the interpreter on every subsequent lookup rather than re-trying the
   * bytecode build — failures are pathological (classfile API bug, unsupported
   * shape) and should never hot-spin.
   */
  private final Set<String> skipCompileSet = ConcurrentHashMap.newKeySet();

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
  public Sequence executeGroupByCount(QueryContext ctx, String[] sourcePath, String groupField) throws QueryException {
    try {
      final String cacheKey = pathCacheKey(sourcePath, groupField);
      Sequence cached = groupByCountCache.get(cacheKey);
      if (cached == null) {
        final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, groupField);
        final Sequence fresh = parallelGroupByCount(groupField, targetPathNodeKey);
        cached = groupByCountCache.putIfAbsent(cacheKey, fresh);
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
  public Sequence executePredicateCount(QueryContext ctx, String[] sourcePath, PredicateNode predicate)
      throws QueryException {
    if (predicate == null) return null;
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) return new Int64(0L);
      if (predicate instanceof PredicateNode.AlwaysTrue) {
        // count(*) requires a known field to anchor the PathSummary lookup; without one
        // we fall through to the generic pipeline (caller handles this).
        return null;
      }
      final String cacheKey = pathCacheKey(sourcePath, null) + "@@" + predicateCacheKey(predicate);
      Long cached = filterCountCache.get(cacheKey);
      if (cached == null) {
        final long fresh = parallelGenericPredicateCount(sourcePath, predicate);
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
  public Sequence executePredicateGroupByCount(QueryContext ctx, String[] sourcePath, PredicateNode predicate,
      String groupField) throws QueryException {
    if (predicate == null || groupField == null) return null;
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) {
        return new DArray(new ArrayList<>());
      }
      final String cacheKey =
          "gb:" + pathCacheKey(sourcePath, groupField) + "#" + predicateCacheKey(predicate);
      Sequence cached = filteredGroupByCountCache.get(cacheKey);
      if (cached != null) return cached;
      final Sequence fresh = parallelGenericPredicateGroupByCount(sourcePath, predicate, groupField);
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
  private Sequence parallelGenericPredicateGroupByCount(final String[] sourcePath, final PredicateNode predicate,
      final String groupField) throws Exception {
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
    // Resolve the anchor field's fully-qualified pathNodeKey once. -1L ⇒
    // PathSummary unavailable or path not found; fall back to name-only match
    // (legacy behaviour — correct for flat schemas, may over-count nested
    // same-name fields, but preserves existing semantics).
    final long anchorPathNodeKey = resolveTargetPathNodeKey(sourcePath, cp.fieldNames[0]);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    @SuppressWarnings("unchecked")
    final Object2LongOpenHashMap<String>[] perThread = new Object2LongOpenHashMap[eff];

    final boolean useBatch = BATCH_GENERIC_EVAL_ENABLED;
    // Compile once, share across workers. Key disambiguates by field layout.
    final String compileKey = compiledClassKey(predicate, cp) + "#gb:" + groupField;
    final BatchPredicate compiled = useBatch ? resolveCompiledPredicate(compileKey, cp) : null;
    // Work-stealing via shared atomic cursor — see parallelGenericPredicateCount
    // for rationale. Stride = 8 amortises CAS over enough work.
    final AtomicLong cursor = new AtomicLong();
    final int STRIDE = 8;
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      // Primitive-long accumulator — avoids the per-group `long[] { 0L }`
      // box-on-insert that the previous HashMap<String, long[]> carried, and
      // uses addTo() so the hot loop does one hashmap op per match instead
      // of the get + (optional put) pair.
      final Object2LongOpenHashMap<String> local = new Object2LongOpenHashMap<>();
      local.defaultReturnValue(0L);
      if (useBatch) {
        final EvalBatch batch = new EvalBatch(cp.fieldNames.length, cp.ops.length, Constants.INP_REFERENCE_COUNT);
        final long[] rootOut = new long[batch.bitmapStride];
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= totalPages) break;
          final long e = Math.min(s + STRIDE, totalPages);
          for (long pk = s; pk < e; pk++) {
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            collectColumns(kv, base, matches, cp, anchorPathNodeKey, batch, rtx);
            if (batch.size == 0) continue;
            final long[] bitmap;
            if (compiled != null) {
              compiled.evaluate(batch, rootOut);
              bitmap = rootOut;
            } else {
              bitmap = evalCompiledBatch(cp, batch);
            }
            final byte[] gpk = batch.presentKind[groupFieldIdx];
            final String[] gvs = batch.strCols[groupFieldIdx];
            final int size = batch.size;
            final int stride = (size + 63) >>> 6;
            for (int w = 0; w < stride; w++) {
              long word = bitmap[w];
              while (word != 0L) {
                final int bit = Long.numberOfTrailingZeros(word);
                final int rowIdx = (w << 6) + bit;
                if (rowIdx >= size) break;
                if (gpk[rowIdx] == 3) {
                  final String gv = gvs[rowIdx];
                  if (gv != null) {
                    local.addTo(gv, 1L);
                  }
                }
                word &= word - 1L;
              }
            }
          }
        }
      } else {
        final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= totalPages) break;
          final long e = Math.min(s + STRIDE, totalPages);
          for (long pk = s; pk < e; pk++) {
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            for (final int slot : matches) {
            final long anchorObjectKey = base + slot;
            // Path-scope filter: reject anchor slots whose pathNodeKey is not
            // the one the query targets. Cheap (one varint decode off the page)
            // and branch-predictable because anchorPathNodeKey is loop-invariant.
            if (anchorPathNodeKey != -1L
                && kv.getObjectKeyPathNodeKeyFromSlot(slot, anchorObjectKey) != anchorPathNodeKey) {
              continue;
            }
            if (!rtx.moveTo(anchorObjectKey)) continue;
            if (!rtx.moveToParent()) continue;
            loadFields(rtx, cp, scratch);
            if (!evalCompiled(cp, 0, scratch)) continue;
            // Read the group field's value from the same scratch.
            if (scratch.fieldKind[groupFieldIdx] != 3) continue;  // require string group
            final String gv = scratch.strVals[groupFieldIdx];
            if (gv == null) continue;
            local.addTo(gv, 1L);
          }
          }
        }
      }
      perThread[idx] = local;
    });

    // Merge per-thread maps into a primitive-long accumulator (no boxing).
    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    for (final Object2LongOpenHashMap<String> m : perThread) {
      if (m == null) continue;
      final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
      while (it.hasNext()) {
        final Object2LongMap.Entry<String> e = it.next();
        merged.addTo(e.getKey(), e.getLongValue());
      }
    }

    // Build Brackit output: array of {"groupField": key, "count": n}.
    final ArrayList<Item> out = new ArrayList<>(merged.size());
    final QNm keyQnm = new QNm(groupField);
    final QNm cntQnm = new QNm("count");
    final ObjectIterator<Object2LongMap.Entry<String>> mergedIt =
        merged.object2LongEntrySet().fastIterator();
    while (mergedIt.hasNext()) {
      final Object2LongMap.Entry<String> e = mergedIt.next();
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { new Str(e.getKey()), new Int64(e.getLongValue()) };
      out.add(new ArrayObject(keys, vals));
    }
    return new DArray(out);
  }

  /**
   * Generic predicate-tree aggregate: walks every record with the compiled
   * predicate + an extra aggregate field; on match, accumulates sum/count/
   * min/max over the aggregate field. Replaces the filter-then-aggregate
   * Volcano pipeline with a single parallel scan.
   */
  @Override
  public Sequence executePredicateAggregate(QueryContext ctx, String[] sourcePath, PredicateNode predicate,
      String func, String field) throws QueryException {
    if (predicate == null || func == null) return null;
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) {
        return switch (func) {
          case "count", "sum" -> new Int64(0L);
          case "avg", "min", "max" -> new Int64(0L);
          default -> null;
        };
      }
      // "count" doesn't require a field — fall through to generic handling,
      // but a null `field` for any other func means we can't run the scan.
      if (!"count".equals(func) && field == null) return null;
      final String cacheKey = "pa:" + func + "#" + pathCacheKey(sourcePath, field) + "@@"
          + predicateCacheKey(predicate);
      Sequence cached = predicateAggregateCache.get(cacheKey);
      if (cached != null) return cached;
      final Sequence fresh = parallelGenericPredicateAggregate(sourcePath, predicate, func, field);
      if (fresh == null) return null;
      cached = predicateAggregateCache.putIfAbsent(cacheKey, fresh);
      return cached == null ? fresh : cached;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized predicate-aggregate failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Walk every leaf page in parallel; per-record evaluate the compiled
   * predicate; on match, read the aggregate {@code field}'s value from the
   * shared scratch and fold it into the per-thread accumulator. The aggregate
   * field is compiled into the predicate's field set via
   * {@link #compileWithExtraField} so a single {@code loadFields} sweep covers
   * every value we need for this record.
   */
  private Sequence parallelGenericPredicateAggregate(final String[] sourcePath, final PredicateNode predicate,
      final String func, final String aggField) throws Exception {
    final boolean isCount = "count".equals(func);
    final CompiledPredicate cp = isCount ? compile(predicate) : compileWithExtraField(predicate, aggField);
    if (cp.fieldNames.length == 0) return null;
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) {
      return switch (func) {
        case "count", "sum" -> new Int64(0L);
        case "avg", "min", "max" -> new Int64(0L);
        default -> null;
      };
    }
    final int aggFieldIdx = isCount ? -1 : indexOfStr(cp.fieldNames, aggField);
    if (!isCount && (aggFieldIdx < 0 || cp.fieldNameKeys[aggFieldIdx] == -1)) {
      // Aggregate field not in this document — zero rows would contribute.
      return switch (func) {
        case "sum" -> new Int64(0L);
        case "avg", "min", "max" -> new Int64(0L);
        default -> null;
      };
    }
    // Resolve the fully-qualified path for the anchor field. See parallelAggregate.
    final long anchorPathNodeKey = resolveTargetPathNodeKey(sourcePath, cp.fieldNames[0]);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    // [count, sum, min, max] per thread.
    final long[][] perThread = new long[eff][4];
    for (int i = 0; i < eff; i++) {
      perThread[i][2] = Long.MAX_VALUE;
      perThread[i][3] = Long.MIN_VALUE;
    }

    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
      final long[] acc = perThread[idx];
      for (long pk = s; pk < e; pk++) {
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
        for (final int slot : matches) {
          final long anchorObjectKey = base + slot;
          if (anchorPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, anchorObjectKey) != anchorPathNodeKey) {
            continue;
          }
          if (!rtx.moveTo(anchorObjectKey)) continue;
          if (!rtx.moveToParent()) continue;
          loadFields(rtx, cp, scratch);
          if (!evalCompiled(cp, 0, scratch)) continue;
          if (isCount) {
            acc[0]++;
            continue;
          }
          // Numeric aggregate — require the agg field to be a number on this row.
          if (scratch.fieldKind[aggFieldIdx] != 1) continue;
          final long v = scratch.longVals[aggFieldIdx];
          acc[0]++;
          acc[1] += v;
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
        }
      }
    });

    long count = 0L;
    long sum = 0L;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (final long[] a : perThread) {
      count += a[0];
      sum += a[1];
      if (a[0] > 0) {
        if (a[2] < min) min = a[2];
        if (a[3] > max) max = a[3];
      }
    }
    return switch (func) {
      case "count" -> new Int64(count);
      case "sum"   -> new Int64(sum);
      case "avg"   -> count == 0 ? new Int64(0L) : new Dbl((double) sum / (double) count);
      case "min"   -> count == 0 ? new Int64(0L) : new Int64(min);
      case "max"   -> count == 0 ? new Int64(0L) : new Int64(max);
      default      -> null;
    };
  }

  /**
   * Cache key combining the query's sourcePath with a local field name.
   * Null and empty sourcePaths produce a fixed prefix so the legacy
   * "no path context" code path still caches deterministically.
   */
  private static String pathCacheKey(final String[] sourcePath, final String field) {
    final StringBuilder sb = new StringBuilder(32);
    if (sourcePath != null) {
      for (final String seg : sourcePath) {
        sb.append(seg == null ? "" : seg).append('/');
      }
    }
    sb.append('|');
    if (field != null) sb.append(field);
    return sb.toString();
  }

  /**
   * Resolve the unique pathNodeKey matching the fully-qualified query path
   * formed by {@code sourcePath + field}. The PathSummary only creates
   * PathNodes for ELEMENT and OBJECT_KEY, so {@code "[]"} entries in the
   * source path (array descent) don't correspond to PathNode ancestors —
   * they're filtered out before matching. Returns {@code -1L} when:
   * <ul>
   *   <li>the resource has no path summary,</li>
   *   <li>no PathNode's ancestor chain matches the expected named prefix, or</li>
   *   <li>more than one PathNode matches (shouldn't happen with a consistent
   *       summary, but we fail closed).</li>
   * </ul>
   *
   * <p>Result is cached per (sourcePath, field) — hot queries pay the walk
   * once, subsequent resolutions are a {@link ConcurrentHashMap} probe.
   */
  private long resolveTargetPathNodeKey(final String[] sourcePath, final String field) {
    if (field == null) return -1L;
    // Path resolution needs only the path summary structure — statistics are
    // a separate opt-in. Conflating the two was the cause of path scoping
    // silently falling back to name-only whenever stats were disabled.
    if (!session.getResourceConfig().withPathSummary) return -1L;
    final String cacheKey = pathCacheKey(sourcePath, field);
    final Long cached = pathNodeKeyCache.get(cacheKey);
    if (cached != null) return cached.longValue();
    final long resolved = computeTargetPathNodeKey(sourcePath, field);
    pathNodeKeyCache.putIfAbsent(cacheKey, Long.valueOf(resolved));
    return resolved;
  }

  private long computeTargetPathNodeKey(final String[] sourcePath, final String field) {
    // Build the expected named-ancestor chain: sourcePath with "[]" entries
    // stripped (array-descent nodes aren't materialised in the PathSummary),
    // reversed so walking up getParent() from a candidate yields it in order.
    final int srcLen = sourcePath == null ? 0 : sourcePath.length;
    int namedCount = 0;
    for (int i = 0; i < srcLen; i++) {
      final String seg = sourcePath[i];
      if (seg != null && !"[]".equals(seg)) namedCount++;
    }
    final String[] expectedAncestors = new String[namedCount];
    int w = 0;
    for (int i = 0; i < srcLen; i++) {
      final String seg = sourcePath[i];
      if (seg != null && !"[]".equals(seg)) expectedAncestors[w++] = seg;
    }
    // Ancestor chain is walked from deepest (closest to the candidate) upward;
    // compare against `expectedAncestors` in reverse order (last source-path
    // segment = closest ancestor).
    try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final PathSummaryReader summary = rtx.getResourceSession().openPathSummary(revision);
      try {
        final List<PathNode> candidates = summary.findPathsByLocalName(field);
        if (candidates.isEmpty()) return -1L;
        long onlyMatch = -1L;
        for (final PathNode candidate : candidates) {
          PathNode cursor = candidate.getParent();
          boolean ok = true;
          for (int i = expectedAncestors.length - 1; i >= 0; i--) {
            // Skip anonymous ancestors (no name, or non-ELEMENT/OBJECT_KEY roots).
            while (cursor != null && cursor.getName() == null) {
              cursor = cursor.getParent();
            }
            if (cursor == null) { ok = false; break; }
            final String name = cursor.getName().getLocalName();
            if (!expectedAncestors[i].equals(name)) { ok = false; break; }
            cursor = cursor.getParent();
          }
          if (!ok) continue;
          // Post-condition: after consuming expectedAncestors, the remaining
          // ancestor chain must contain NO more named ancestors (otherwise the
          // candidate lives deeper than the query path — e.g. pet.dept when
          // the query is $u.dept). Without this check, an empty
          // expectedAncestors matches every candidate regardless of depth.
          while (cursor != null && cursor.getName() == null) {
            cursor = cursor.getParent();
          }
          if (cursor != null) continue;  // extra named ancestor — too deep
          if (onlyMatch != -1L) {
            // Ambiguous — two PathNodes share the same qualified prefix.
            // Can't happen with a consistent summary; fail closed.
            return -1L;
          }
          onlyMatch = candidate.getNodeKey();
        }
        return onlyMatch;
      } finally {
        summary.close();
      }
    }
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
  private long parallelGenericPredicateCount(final String[] sourcePath, final PredicateNode predicate)
      throws Exception {
    final CompiledPredicate cp = compile(predicate);
    if (cp.fieldNames.length == 0) return 0L;
    // Anchor on the first field — its nameKey drives slot iteration.
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) return 0L;
    // Resolve the fully-qualified pathNodeKey for the anchor. -1L ⇒ unresolvable;
    // fall back to name-only matching (legacy behaviour).
    final long anchorPathNodeKey = resolveTargetPathNodeKey(sourcePath, cp.fieldNames[0]);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long[] perThread = new long[eff];

    final boolean useBatch = BATCH_GENERIC_EVAL_ENABLED;
    // Resolve the generated BatchPredicate up-front (single compile for all
    // workers). Null = interpreter fallback. We include the field-layout hash
    // in the cache key so two predicates with identical structure but
    // different resolved fieldIdx order don't alias.
    final String compileKey = compiledClassKey(predicate, cp);
    final BatchPredicate compiled = useBatch ? resolveCompiledPredicate(compileKey, cp) : null;
    // Pre-extract anchor-field NumCmp constraints for zone-map pruning.
    // Per-page we probe tagMin/tagMax of the anchor nameKey in the page's
    // NumberRegion and skip the whole page on a provable miss. Cost: a
    // handful of int compares per page when the probe is active.
    final AnchorZoneProbe zoneProbe = extractAnchorZoneProbe(cp);
    // Work-stealing via shared atomic cursor — each worker grabs the next
    // stride of pages dynamically. Previously we pre-partitioned into
    // (totalPages / eff) per-worker ranges; that's catastrophic when one
    // range has all the pathNodeKey matches and others are empty, because
    // the lucky workers idle while one chugs. At 10M records with 20
    // workers and a sparse match distribution this alone doubles throughput.
    // Stride = 8 pages per steal amortises the atomic CAS over enough work.
    final AtomicLong cursor = new AtomicLong();
    final int STRIDE = 8;
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      long localCount = 0;
      if (useBatch) {
        final EvalBatch batch = new EvalBatch(cp.fieldNames.length, cp.ops.length, Constants.INP_REFERENCE_COUNT);
        final long[] rootOut = new long[batch.bitmapStride];
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= totalPages) break;
          final long e = Math.min(s + STRIDE, totalPages);
          for (long pk = s; pk < e; pk++) {
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            // Zone-map pre-check: if the anchor field's per-page tagMin/tagMax
            // proves no row on this page can satisfy the conjunctive NumCmp
            // constraints, skip the entire page without decoding a single
            // record.
            if (zoneProbe != null) {
              final NumberRegion.Header numHdr = kv.getNumberRegionHeader();
              if (numHdr != null && numHdr.tagMin != null) {
                final int lookupKey = (numHdr.tagKind == NumberRegion.TAG_KIND_PATH_NODE
                    && anchorPathNodeKey > 0 && anchorPathNodeKey <= Integer.MAX_VALUE)
                    ? (int) anchorPathNodeKey : anchorNameKey;
                final int tag = NumberRegion.lookupTag(numHdr, lookupKey);
                if (tag >= 0 && canPruneByZone(zoneProbe,
                    numHdr.tagMin[tag], numHdr.tagMax[tag])) {
                  continue;
                }
              }
            }
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            collectColumns(kv, base, matches, cp, anchorPathNodeKey, batch, rtx);
            if (batch.size == 0) continue;
            final long[] bitmap;
            if (compiled != null) {
              compiled.evaluate(batch, rootOut);
              bitmap = rootOut;
            } else {
              bitmap = evalCompiledBatch(cp, batch);
            }
            localCount += countBits(bitmap, batch.size);
          }
        }
      } else {
        final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= totalPages) break;
          final long e = Math.min(s + STRIDE, totalPages);
          for (long pk = s; pk < e; pk++) {
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            for (final int slot : matches) {
              final long anchorObjectKey = base + slot;
              if (anchorPathNodeKey != -1L
                  && kv.getObjectKeyPathNodeKeyFromSlot(slot, anchorObjectKey) != anchorPathNodeKey) {
                continue;
              }
              if (!rtx.moveTo(anchorObjectKey)) continue;
              if (!rtx.moveToParent()) continue;  // enclosing object
              loadFields(rtx, cp, scratch);
              if (evalCompiled(cp, 0, scratch)) localCount++;
            }
          }
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

  // ------------------------------------------------------------------
  // Column-batched generic-predicate evaluator (Task #30).
  //
  // The record-at-a-time path (loadFields + evalCompiled) pays one rtx
  // cursor walk per record and then re-dispatches the compiled predicate
  // interpreter on every single row. For Umbra/ClickHouse-style throughput
  // we want the inner loop to look like a column kernel: sweep all rows of
  // one field, then the next, then AND / OR their match bitmaps.
  //
  // Phase 1 (collectColumns): for each page, scan the matching anchor
  // slots; for each row, load every referenced field into
  // EvalBatch's primitive columns. The anchor field's value is read
  // directly off the page slot (bypasses rtx.moveTo) whenever it lives
  // on the same page as the OBJECT_KEY; siblings still use rtx because
  // they're scattered across the object's children. Missing fields leave
  // presentKind = 0.
  //
  // Phase 2 (evalCompiledBatch): evaluate the CompiledPredicate against
  // the columns. Each tree node produces a match bitmap sized
  // (batch.size + 63) >>> 6. NOT = bitwise invert, AND / OR = bitwise
  // combine. Leaves iterate all batch.size rows of the referenced column
  // and set the bit if the comparison passes. Bitmaps are per-worker
  // scratch, grown on demand — no allocation in the hot loop.
  // ------------------------------------------------------------------

  /**
   * Per-field primitive columns for a single page's worth of matching records.
   * Sized for the typical page slot count (~256) and grown on demand by
   * {@link #ensureCapacity}. All per-worker reusable — allocated once, reset
   * {@code size} between pages.
   */
  static final class EvalBatch {
    int size;
    int capacity;
    /** Anchor slot for row i on the current page — used for direct-slot reads. */
    int[] anchorSlots;
    /** Sirix nodeKey (base + slot) of the anchor OBJECT_KEY for row i. */
    long[] anchorObjectKeys;
    /** [fieldIdx] → long[capacity] — valid iff presentKind[fieldIdx][i] == 1. */
    final long[][] longCols;
    /** [fieldIdx] → boolean[capacity] — valid iff presentKind[fieldIdx][i] == 2. */
    final boolean[][] boolCols;
    /** [fieldIdx] → String[capacity] — valid iff presentKind[fieldIdx][i] == 3. */
    final String[][] strCols;
    /** [fieldIdx] → byte[capacity]. 0=missing, 1=num, 2=bool, 3=str. */
    final byte[][] presentKind;
    /** Per-compiled-node match bitmap; length (capacity + 63) >>> 6. */
    final long[][] nodeBitmaps;
    final int bitmapStride;
    /** Work bitmap for NOT etc. */
    final long[] workBitmap;
    /**
     * Scratch indices array consumed by
     * {@link VectorizedPredicate#filterIndices} — capacity sized; reused across
     * every OP_NUM_CMP node on every page. One allocation per worker lifetime.
     */
    final int[] indicesScratch;
    /**
     * Scratch long[] used by OP_NUM_CMP to overwrite missing rows with a
     * sentinel that cannot match the current comparison (e.g. Long.MIN_VALUE
     * for GT/GE/EQ, Long.MAX_VALUE for LT/LE). Keeps the SIMD compare
     * branchless — the JIT auto-vectorises `compare + filterIndices` into an
     * AVX2 masked lane-write. One allocation per worker lifetime, rewritten
     * per OP_NUM_CMP leaf evaluation.
     */
    final long[] maskedColScratch;
    /**
     * Parent-OBJECT nodeKey → row index. Populated in {@link #collectColumns}
     * pass 1 so pass 2 can join sibling fields in O(1) per slot without an
     * rtx cursor walk. Reused across pages — {@code clear()} per page.
     */
    final Long2IntOpenHashMap parentKeyToRow;
    /**
     * Scratch byte[] for direct-slot string reads in pass 2. Sized to a
     * typical JSON string payload; oversized values fall back to rtx.
     */
    final byte[] strScratch;
    /**
     * Bulk-decoded {@code pathNodeKey} / {@code parentKey} / {@code firstChildKey}
     * columns produced by {@link KeyValueLeafPage#bulkDecodeObjectKeyColumns}.
     * Indexed by anchor-slot position (not row) — pass 1 walks these in
     * lock-step with the input {@code anchorSlots} array. Reused across
     * pages; sized to {@code capacity}, which matches the max anchor match
     * count per page (INP_REFERENCE_COUNT).
     */
    final long[] slotPathNodeKeys;
    final long[] slotParentKeys;
    final long[] slotFirstChildKeys;
    /**
     * Canonical-String dedup cache for column-read string values. Analytical
     * group-by queries typically target low-cardinality columns (e.g. 8
     * departments over 10M records) but {@code readFieldValueDirect}
     * allocates a fresh {@link String} per row. The cache is keyed by a
     * 64-bit content hash of the decoded bytes; on hit we reuse the
     * canonical String reference, avoiding both the allocation and the
     * downstream {@code String.hashCode} recompute that the group-by
     * accumulator pays on insert.
     *
     * <p>Scope: per-worker (EvalBatch is per-worker). Unbounded to keep the
     * code simple — for the analytical workloads the executor targets the
     * distinct-value count is bounded by column cardinality, which is small.
     */
    final Long2ObjectOpenHashMap<String> canonicalStrings;

    EvalBatch(final int nFields, final int nNodes, final int initialCapacity) {
      final int cap = Math.max(64, initialCapacity);
      this.capacity = cap;
      this.anchorSlots = new int[cap];
      this.anchorObjectKeys = new long[cap];
      this.longCols = new long[nFields][cap];
      this.boolCols = new boolean[nFields][cap];
      this.strCols = new String[nFields][cap];
      this.presentKind = new byte[nFields][cap];
      this.bitmapStride = (cap + 63) >>> 6;
      this.nodeBitmaps = new long[nNodes][bitmapStride];
      this.workBitmap = new long[bitmapStride];
      this.indicesScratch = new int[cap];
      this.maskedColScratch = new long[cap];
      // Sized to the expected row count (1024 slots max per page). fastutil
      // rounds up to the next power-of-two internal capacity, so with cap=1024
      // the table has 2048 buckets — half the clear cost vs the previous
      // cap*2 sizing without risking a rehash (we never exceed `cap` entries).
      // defaultReturnValue(-1) so absent lookups signal "parent not in batch"
      // without a containsKey probe.
      this.parentKeyToRow = new Long2IntOpenHashMap(cap);
      this.parentKeyToRow.defaultReturnValue(-1);
      this.strScratch = new byte[4096];
      this.slotPathNodeKeys = new long[cap];
      this.slotParentKeys = new long[cap];
      this.slotFirstChildKeys = new long[cap];
      // Pre-size for typical analytical column cardinality; the map grows
      // naturally if the query targets a high-cardinality column.
      this.canonicalStrings = new Long2ObjectOpenHashMap<>(64);
    }

    /**
     * Assert that we have room for {@code n} rows. EvalBatch is sized for the
     * maximum page slot count at construction (caller uses INP_REFERENCE_COUNT);
     * anchor match sets are bounded by that, so this method only catches bugs.
     */
    void ensureCapacity(final int n) {
      if (n <= capacity) return;
      throw new IllegalStateException(
          "EvalBatch capacity exceeded (" + capacity + " < " + n + "); "
              + "initial capacity must be >= max page slot count");
    }
  }

  /**
   * Phase 1: collect per-row column values for the current page. Returns the
   * number of rows stored in {@code batch.size}.
   *
   * <p>Strategy (multi-field, direct-slot default):
   * <ol>
   *   <li>Pass 1 — anchor: iterate the candidate anchor OBJECT_KEY slots,
   *       filter by pathNodeKey, read the anchor value via direct-slot
   *       primitives. Record (parentOBJ nodeKey → row) in
   *       {@link EvalBatch#parentKeyToRow} so pass 2 can join.</li>
   *   <li>Pass 2 — one pass per sibling field: iterate that field's
   *       {@code getObjectKeySlotsForNameKey}, decode the OBJECT_KEY's
   *       parentKey directly, look up the row index; if in-batch, read the
   *       value node via direct-slot primitives (same-page fast path).</li>
   *   <li>Pass 3 — rtx fallback: for any row whose anchor value couldn't be
   *       read direct (cross-page / FSST string oversize / float payload),
   *       walk the rtx cursor for that row only.</li>
   * </ol>
   *
   * <p>Single-field predicates keep the existing anchor-only fast path.
   */
  private static void collectColumns(final KeyValueLeafPage kv, final long pageBase, final int[] anchorSlots,
      final CompiledPredicate cp, final long anchorPathNodeKey, final EvalBatch batch,
      final JsonNodeReadOnlyTrx rtx) {
    batch.size = 0;
    if (anchorSlots.length == 0) return;
    batch.ensureCapacity(anchorSlots.length);
    final int[] fieldKeys = cp.fieldNameKeys;
    final int nFields = fieldKeys.length;
    final int anchorFieldIdx = 0;
    final int anchorPageMask = (1 << Constants.INP_REFERENCE_COUNT_EXPONENT) - 1;
    // rowNeedsRtx[i]: the i-th row's anchor value couldn't be direct-read.
    // Reuses workBitmap as a scratch bitmap (we zero it up-front, caller owns
    // it per-page). Keeps allocation off the hot loop.
    final long[] rowNeedsRtx = batch.workBitmap;
    final int stride = batch.bitmapStride;
    for (int i = 0; i < stride; i++) rowNeedsRtx[i] = 0L;

    // ---------------------------------------------------------------------
    // Pass 1: anchor scan — fills anchorSlots/anchorObjectKeys/longCols[0]
    // and populates parentKeyToRow for pass 2.
    //
    // When the pathNodeKey scope filter or the parent-row map is active,
    // fetch the bulk-decoded column set for this field via the page's
    // ObjectKeyColumns cache (task #41). First call on the page for this
    // nameKey pays the varint decode; subsequent calls — including the
    // sibling-field pass below, iter 2+ of the same query, and any
    // follow-up query touching this field — get zero-decode array access.
    // ---------------------------------------------------------------------
    final Long2IntOpenHashMap parentToRow = batch.parentKeyToRow;
    parentToRow.clear();
    final boolean multi = nFields > 1;
    final boolean directSlot = DIRECT_SLOT_COLUMNS_ENABLED;
    final boolean needPath = anchorPathNodeKey != -1L;
    final boolean needParent = multi && directSlot;
    final boolean useBulk = needPath || needParent;
    final long[] slotPathNodeKeys = batch.slotPathNodeKeys;
    final long[] slotParentKeys = batch.slotParentKeys;
    final long[] slotFirstChildKeys = batch.slotFirstChildKeys;
    if (useBulk) {
      kv.bulkDecodeObjectKeyColumns(anchorSlots, anchorSlots.length, pageBase,
          slotPathNodeKeys, slotParentKeys, slotFirstChildKeys);
    }
    int row = 0;
    for (int si = 0, nSlots = anchorSlots.length; si < nSlots; si++) {
      final int slot = anchorSlots[si];
      final long anchorObjectKey = pageBase + slot;
      if (needPath && slotPathNodeKeys[si] != anchorPathNodeKey) {
        continue;
      }
      // Reset this row's presence bits for all fields. (Anchor will be set
      // below; sibling rows default to missing unless pass 2 fills them.)
      for (int f = 0; f < nFields; f++) batch.presentKind[f][row] = 0;

      // Anchor value — direct slot first. Pull the firstChildKey from the
      // bulk-decoded scratch when available, else decode per-slot.
      final long anchorValueKey = useBulk
          ? slotFirstChildKeys[si]
          : kv.getObjectKeyFirstChildKeyFromSlot(slot, anchorObjectKey);
      final boolean anchorDirect = readAnchorValueDirect(
          kv, pageBase, anchorValueKey, anchorPageMask, anchorFieldIdx, row, batch);
      if (!anchorDirect) {
        // Single-field: fall back to rtx right here (legacy behaviour kept).
        if (!multi) {
          if (!rtx.moveTo(anchorObjectKey)) continue;
          if (!rtx.moveToParent()) continue;
          if (!loadFieldsIntoBatch(rtx, cp, batch, row)) continue;
        } else {
          // Multi-field: mark the row for pass-3 rtx fallback.
          rowNeedsRtx[row >>> 6] |= 1L << (row & 63);
        }
      }

      batch.anchorSlots[row] = slot;
      batch.anchorObjectKeys[row] = anchorObjectKey;
      if (needParent) {
        // Record parent-OBJECT nodeKey → row so pass 2 can join by parentKey.
        final long parentKey = slotParentKeys[si];
        if (parentKey != -1L) parentToRow.put(parentKey, row);
      }
      row++;
    }
    batch.size = row;
    if (row == 0) return;

    // ---------------------------------------------------------------------
    // Pass 2: sibling fields via direct-slot join on parentKey.
    // Only when multi-field and the switch is on.
    // ---------------------------------------------------------------------
    if (multi && directSlot) {
      // Reuse the anchor-slot scratch arrays for sibling bulk decode —
      // their size is cap == INP_REFERENCE_COUNT which matches the hard
      // upper bound on sibling-match arrays too. Decoding only (parent,
      // firstChild) here, not pathNodeKey (unused for the sibling join).
      final long[] siblingParentKeys = batch.slotParentKeys;
      final long[] siblingFirstChildKeys = batch.slotFirstChildKeys;
      for (int fi = 1; fi < nFields; fi++) {
        final int nameKey = fieldKeys[fi];
        if (nameKey == -1) continue;
        final int[] siblingSlots = kv.getObjectKeySlotsForNameKey(nameKey);
        final int nSibs = siblingSlots.length;
        if (nSibs == 0) continue;
        kv.bulkDecodeObjectKeyParentAndChildKeys(siblingSlots, nSibs, pageBase,
            siblingParentKeys, siblingFirstChildKeys);
        for (int k = 0; k < nSibs; k++) {
          final long parentKey = siblingParentKeys[k];
          if (parentKey == -1L) continue;
          final int rowIdx = parentToRow.get(parentKey);
          if (rowIdx < 0) continue;  // not in batch
          final long valueKey = siblingFirstChildKeys[k];
          if (!readFieldValueDirect(kv, pageBase, valueKey, anchorPageMask, fi, rowIdx, batch)) {
            // Cross-page / FSST / unsupported — defer to rtx fallback for
            // this row. Mark the row so pass 3 picks it up.
            rowNeedsRtx[rowIdx >>> 6] |= 1L << (rowIdx & 63);
          }
        }
      }
    } else if (multi) {
      // Direct-slot disabled: every row needs rtx for its sibling fields.
      for (int i = 0; i < row; i++) rowNeedsRtx[i >>> 6] |= 1L << (i & 63);
    }

    // ---------------------------------------------------------------------
    // Pass 3: rtx fallback. Two reasons a row needs it:
    //   (a) anchor value couldn't be direct-read (cross-page, FSST oversize,
    //       float/double payload) — flagged in rowNeedsRtx during pass 1.
    //   (b) a required sibling OBJECT_KEY lives on a DIFFERENT page from
    //       its anchor (the enclosing object's children straddle pages) —
    //       pass 2 can't see it, so the row's sibling field stays
    //       presentKind == 0. We detect this here by scanning sibling
    //       fields and marking any row with an unresolved field for rtx.
    //
    // Note: the scan is conservative — if a field is genuinely absent in
    // the source JSON, rtx.loadFieldsIntoBatch will also leave it at
    // presentKind=0. Cost is bounded by batch.size (<=1024 per page).
    // ---------------------------------------------------------------------
    if (multi) {
      if (directSlot) {
        // Detect unresolved sibling fields and promote those rows to rtx.
        for (int fi = 1; fi < nFields; fi++) {
          final byte[] pk = batch.presentKind[fi];
          for (int i = 0; i < row; i++) {
            if (pk[i] == 0) rowNeedsRtx[i >>> 6] |= 1L << (i & 63);
          }
        }
      }
      for (int i = 0; i < row; i++) {
        if ((rowNeedsRtx[i >>> 6] & (1L << (i & 63))) == 0L) continue;
        final long anchorObjectKey = batch.anchorObjectKeys[i];
        if (!rtx.moveTo(anchorObjectKey)) continue;
        if (!rtx.moveToParent()) continue;
        // loadFieldsIntoBatch overwrites all fields for this row, including
        // any that pass 2 already populated — benign (rtx is the source of
        // truth) and avoids branching inside the inner loader.
        loadFieldsIntoBatch(rtx, cp, batch, i);
      }
    }
  }

  /**
   * Direct-slot value read for an arbitrary sibling field (not just the
   * anchor). Handles OBJECT_NUMBER_VALUE / OBJECT_BOOLEAN_VALUE / OBJECT_STRING_VALUE
   * when the value node lives on the same page as its OBJECT_KEY. Returns
   * {@code false} if the caller must fall back to rtx.
   */
  private static boolean readFieldValueDirect(final KeyValueLeafPage kv, final long pageBase,
      final long valueKey, final int pageMask, final int fieldIdx, final int row, final EvalBatch batch) {
    // Cross-page? Same shared base means both nodeKeys fall into the same
    // 1024-slot page → upper bits match.
    if ((valueKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT)
        != (pageBase >>> Constants.INP_REFERENCE_COUNT_EXPONENT)) {
      return false;
    }
    final int valueSlot = (int) (valueKey & pageMask);
    final int kindId = kv.getSlotNodeKindId(valueSlot);
    if (kindId == 42) {  // OBJECT_NUMBER_VALUE
      final long v = kv.getNumberValueLongFromSlot(valueSlot);
      if (v == Long.MIN_VALUE) return false;  // float/double/BigDecimal sentinel
      batch.longCols[fieldIdx][row] = v;
      batch.presentKind[fieldIdx][row] = 1;
      return true;
    }
    if (kindId == 41) {  // OBJECT_BOOLEAN_VALUE
      batch.boolCols[fieldIdx][row] = kv.getObjectBooleanValueFromSlot(valueSlot);
      batch.presentKind[fieldIdx][row] = 2;
      return true;
    }
    if (kindId == 40) {  // OBJECT_STRING_VALUE
      final int n = kv.readObjectStringValueBytesFromSlot(valueSlot, batch.strScratch);
      if (n < 0) return false;  // FSST oversize / unavailable
      batch.strCols[fieldIdx][row] = canonicalize(batch, batch.strScratch, n);
      batch.presentKind[fieldIdx][row] = 3;
      return true;
    }
    return false;
  }

  /**
   * Return a canonical {@link String} for the decoded bytes in
   * {@code scratch[0..n)}. Uses a 64-bit content hash keyed lookup on the
   * per-worker {@link EvalBatch#canonicalStrings} cache so low-cardinality
   * group-by columns (e.g. 8 department names over 10M rows) allocate only
   * one String per distinct value instead of one per row.
   *
   * <p>The hash is a simple xor-multiply FNV-like accumulator — fast to
   * inline and collision-resistant enough that a collision would require a
   * content mismatch, which we defend against with a byte-by-byte verify
   * before returning the cached String.
   */
  private static String canonicalize(final EvalBatch batch, final byte[] scratch, final int n) {
    // Hash the scratch bytes. FNV-1a-ish 64-bit mix — cheap and avoids a
    // multiply dependency chain by XORing after the shift.
    long h = 0xCBF29CE484222325L;
    for (int i = 0; i < n; i++) {
      h ^= scratch[i] & 0xFFL;
      h *= 0x100000001B3L;
    }
    final Long2ObjectOpenHashMap<String> cache = batch.canonicalStrings;
    final String cached = cache.get(h);
    if (cached != null && stringMatchesBytes(cached, scratch, n)) {
      return cached;
    }
    final String fresh = new String(scratch, 0, n, StandardCharsets.UTF_8);
    cache.put(h, fresh);
    return fresh;
  }

  /**
   * Allocation-free verify that {@code s}'s ASCII encoding matches
   * {@code bytes[0..n)}. Returns {@code false} if {@code s} contains any
   * non-ASCII character so the caller falls back to a fresh decoded String
   * — safe for the UTF-8 general case at the cost of cache misses on
   * multi-byte strings. Group-by keys in analytical workloads are almost
   * always ASCII (enumerations, product codes, department names), so the
   * ASCII path covers the common case without a {@code byte[]} allocation.
   */
  private static boolean stringMatchesBytes(final String s, final byte[] bytes, final int n) {
    if (s.length() != n) return false;
    for (int i = 0; i < n; i++) {
      final char c = s.charAt(i);
      if (c >= 0x80 || ((byte) c) != bytes[i]) return false;
    }
    return true;
  }

  /**
   * Direct-slot value read for the anchor field when both the OBJECT_KEY and
   * its value live on the same page. Returns {@code true} on success, or
   * {@code false} if caller must fall back to rtx (cross-page, FSST string
   * too large for scratch, or unsupported number type).
   */
  private static boolean readAnchorValueDirect(final KeyValueLeafPage kv, final long pageBase,
      final long valueKey, final int pageMask, final int fieldIdx, final int row, final EvalBatch batch) {
    // Cross-page? Shared base means (valueKey >>> 10) == (pageBase >>> 10).
    if ((valueKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT)
        != (pageBase >>> Constants.INP_REFERENCE_COUNT_EXPONENT)) {
      return false;
    }
    final int valueSlot = (int) (valueKey & pageMask);
    final int kindId = kv.getSlotNodeKindId(valueSlot);
    // 42 = OBJECT_NUMBER_VALUE, 40 = OBJECT_STRING_VALUE, 41 = OBJECT_BOOLEAN_VALUE
    if (kindId == 42) {
      final long v = kv.getNumberValueLongFromSlot(valueSlot);
      if (v == Long.MIN_VALUE) return false; // float/double/BigDecimal sentinel
      batch.longCols[fieldIdx][row] = v;
      batch.presentKind[fieldIdx][row] = 1;
      return true;
    }
    if (kindId == 41) {
      batch.boolCols[fieldIdx][row] = kv.getObjectBooleanValueFromSlot(valueSlot);
      batch.presentKind[fieldIdx][row] = 2;
      return true;
    }
    if (kindId == 40) {
      // String: go through the rtx path — readObjectStringValueBytesFromSlot
      // requires a caller-supplied scratch byte[] and FSST decode logic;
      // keeping parity with the legacy path is safer than duplicating decode
      // here. Cheap to opt out (single-field STR_EQ isn't the common hot case).
      return false;
    }
    return false;
  }

  /**
   * Variant of {@link #loadFields} that writes into {@link EvalBatch}'s
   * column arrays at the given {@code row} rather than into a single scratch.
   * Returns {@code false} iff the enclosing object has no children (caller
   * must skip this row).
   */
  private static boolean loadFieldsIntoBatch(final JsonNodeReadOnlyTrx rtx, final CompiledPredicate cp,
      final EvalBatch batch, final int row) {
    if (!rtx.moveToFirstChild()) return false;
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
            batch.longCols[fi][row] = n.longValue();
            batch.presentKind[fi][row] = 1;
          }
        }
        case OBJECT_BOOLEAN_VALUE, BOOLEAN_VALUE -> {
          batch.boolCols[fi][row] = rtx.getBooleanValue();
          batch.presentKind[fi][row] = 2;
        }
        case OBJECT_STRING_VALUE, STRING_VALUE -> {
          batch.strCols[fi][row] = rtx.getValue();
          batch.presentKind[fi][row] = 3;
        }
        default -> { /* unsupported — leave presentKind[fi][row] = 0 */ }
      }
      rtx.moveToParent();
    } while (rtx.moveToRightSibling());
    return true;
  }

  /**
   * Phase 2: evaluate the compiled predicate over the columns into a bitmap.
   * Returns the root node's bitmap (stored in
   * {@code batch.nodeBitmaps[0]}). Valid bits are [0, batch.size).
   */
  private static long[] evalCompiledBatch(final CompiledPredicate cp, final EvalBatch batch) {
    return evalCompiledBatchRec(cp, 0, batch);
  }

  private static long[] evalCompiledBatchRec(final CompiledPredicate cp, final int nodeIdx, final EvalBatch batch) {
    final byte op = cp.ops[nodeIdx];
    final long[] out = batch.nodeBitmaps[nodeIdx];
    final int size = batch.size;
    final int stride = (size + 63) >>> 6;
    // Zero the valid portion of the output bitmap.
    for (int i = 0; i < stride; i++) out[i] = 0L;
    switch (op) {
      case CompiledPredicate.OP_ALWAYS_T: {
        // Set all valid bits.
        final int full = size >>> 6;
        for (int i = 0; i < full; i++) out[i] = -1L;
        final int tail = size & 63;
        if (tail > 0) out[full] = (1L << tail) - 1L;
        return out;
      }
      case CompiledPredicate.OP_ALWAYS_F:
        return out;  // already zeroed
      case CompiledPredicate.OP_NOT: {
        final long[] child = evalCompiledBatchRec(cp, cp.children[cp.childStart[nodeIdx]], batch);
        final int full = size >>> 6;
        for (int i = 0; i < full; i++) out[i] = ~child[i];
        final int tail = size & 63;
        if (tail > 0) out[full] = (~child[full]) & ((1L << tail) - 1L);
        return out;
      }
      case CompiledPredicate.OP_AND: {
        final int start = cp.childStart[nodeIdx];
        final int n = cp.childCount[nodeIdx];
        if (n == 0) {
          // vacuously true
          final int full = size >>> 6;
          for (int i = 0; i < full; i++) out[i] = -1L;
          final int tail = size & 63;
          if (tail > 0) out[full] = (1L << tail) - 1L;
          return out;
        }
        final long[] first = evalCompiledBatchRec(cp, cp.children[start], batch);
        for (int i = 0; i < stride; i++) out[i] = first[i];
        for (int k = 1; k < n; k++) {
          final long[] child = evalCompiledBatchRec(cp, cp.children[start + k], batch);
          for (int i = 0; i < stride; i++) out[i] &= child[i];
        }
        return out;
      }
      case CompiledPredicate.OP_OR: {
        final int start = cp.childStart[nodeIdx];
        final int n = cp.childCount[nodeIdx];
        for (int k = 0; k < n; k++) {
          final long[] child = evalCompiledBatchRec(cp, cp.children[start + k], batch);
          for (int i = 0; i < stride; i++) out[i] |= child[i];
        }
        return out;
      }
      case CompiledPredicate.OP_NUM_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        final int cmp = cp.cmpOp[nodeIdx];
        final long lit = cp.longLit[nodeIdx];
        final byte[] pk = batch.presentKind[fi];
        final long[] vals = batch.longCols[fi];
        // SIMD path via Brackit's VectorizedPredicate.compareLong +
        // VectorOps.filter*Long kernels. The JIT (or Panama's Vector API
        // when -XX:+UseVectorAPI is active) lowers filterEqLong/filterLtLong
        // et al. to a dense AVX2 branchless compare → index-pack pipeline.
        //
        // Missing rows (presentKind != 1) must never match. We overwrite
        // them in a scratch column with a sentinel chosen per-op so the
        // SIMD compare trivially rejects them:
        //   GT, GE, EQ → Long.MIN_VALUE  (lowest possible, >lit is false;
        //                                 MIN_VALUE==lit would be a collision
        //                                 only for lit == MIN_VALUE — guarded)
        //   LT, LE     → Long.MAX_VALUE
        // If the literal happens to coincide with the sentinel we fall back
        // to the presence-masked scalar loop — correctness over perf.
        final long sentinel;
        final boolean sentinelCollides;
        switch (cmp) {
          case OP_GT, OP_GE, OP_EQ -> { sentinel = Long.MIN_VALUE; sentinelCollides = (lit == Long.MIN_VALUE); }
          case OP_LT, OP_LE        -> { sentinel = Long.MAX_VALUE; sentinelCollides = (lit == Long.MAX_VALUE); }
          default                  -> throw new IllegalStateException("bad cmpOp " + cmp);
        }
        if (sentinelCollides) {
          // Fallback scalar path — matches the legacy behaviour exactly.
          switch (cmp) {
            case OP_GT -> { for (int i = 0; i < size; i++) if (pk[i] == 1 && vals[i] > lit)  out[i >>> 6] |= 1L << (i & 63); }
            case OP_LT -> { for (int i = 0; i < size; i++) if (pk[i] == 1 && vals[i] < lit)  out[i >>> 6] |= 1L << (i & 63); }
            case OP_GE -> { for (int i = 0; i < size; i++) if (pk[i] == 1 && vals[i] >= lit) out[i >>> 6] |= 1L << (i & 63); }
            case OP_LE -> { for (int i = 0; i < size; i++) if (pk[i] == 1 && vals[i] <= lit) out[i >>> 6] |= 1L << (i & 63); }
            case OP_EQ -> { for (int i = 0; i < size; i++) if (pk[i] == 1 && vals[i] == lit) out[i >>> 6] |= 1L << (i & 63); }
            default    -> throw new IllegalStateException("bad cmpOp " + cmp);
          }
          return out;
        }
        // Fast path: copy col into maskedColScratch, overwrite misses with the
        // sentinel, then hand off to VectorizedPredicate. One sequential pass
        // over ~256 longs — JIT lowers the conditional to a cmov; the SIMD
        // compare that follows is loop-fusible with the mask, so the entire
        // OP_NUM_CMP reduces to two short linear sweeps.
        final long[] masked = batch.maskedColScratch;
        for (int i = 0; i < size; i++) {
          masked[i] = (pk[i] == 1) ? vals[i] : sentinel;
        }
        final int[] idxs = batch.indicesScratch;
        final VectorizedPredicate.ComparisonOp vOp = toVecCmpOp(cmp);
        final int n = VectorizedPredicate.compareLong(vOp, lit).filterIndices(masked, 0, size, idxs);
        for (int k = 0; k < n; k++) {
          final int idx = idxs[k];
          out[idx >>> 6] |= 1L << (idx & 63);
        }
        return out;
      }
      case CompiledPredicate.OP_STR_EQ: {
        final int fi = cp.fieldIdx[nodeIdx];
        final String lit = cp.strLiterals[cp.strIdx[nodeIdx]];
        final byte[] pk = batch.presentKind[fi];
        final String[] vals = batch.strCols[fi];
        // Fast path: String.equals has a JIT-intrinsified bytewise compare
        // using StringLatin1/UTF-16 internals — hoist length + identity checks
        // so the hot branch is the length mismatch case (common short-circuit).
        final int litLen = lit.length();
        for (int i = 0; i < size; i++) {
          if (pk[i] != 3) continue;
          final String v = vals[i];
          if (v == lit || (v != null && v.length() == litLen && lit.equals(v))) {
            out[i >>> 6] |= 1L << (i & 63);
          }
        }
        return out;
      }
      case CompiledPredicate.OP_BOOL_REF: {
        final int fi = cp.fieldIdx[nodeIdx];
        final byte[] pk = batch.presentKind[fi];
        final boolean[] vals = batch.boolCols[fi];
        // Branchless / auto-vectorisable: produce a byte-per-row mask then
        // pack into bits. JIT lifts this into SIMD compares on hot paths.
        for (int i = 0; i < size; i++) if (pk[i] == 2 && vals[i]) out[i >>> 6] |= 1L << (i & 63);
        return out;
      }
      default:
        throw new IllegalStateException("bad opcode " + op);
    }
  }

  /**
   * Popcount over the valid prefix of a match bitmap. Sums
   * {@link Long#bitCount} per word; the final word is masked to only the
   * valid tail bits.
   */
  private static int countBits(final long[] bitmap, final int size) {
    int c = 0;
    final int full = size >>> 6;
    for (int i = 0; i < full; i++) c += Long.bitCount(bitmap[i]);
    final int tail = size & 63;
    if (tail > 0) c += Long.bitCount(bitmap[full] & ((1L << tail) - 1L));
    return c;
  }

  @Override
  public Sequence executeCountDistinct(QueryContext ctx, String[] sourcePath, String field) throws QueryException {
    try {
      // Only honour the short-circuit when the resource maintains an HLL per path.
      if (!session.getResourceConfig().withPathStatistics) {
        return null;
      }
      final String cdKey = pathCacheKey(sourcePath, field);
      final Sequence cached = countDistinctResultCache.get(cdKey);
      if (cached != null) return cached;
      final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
      try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
        final PathSummaryReader summary = rtx.getResourceSession().openPathSummary(revision);
        try {
          final Sequence computed;
          if (targetPathNodeKey != -1L) {
            // Path-scoped fast path: read the single PathNode's HLL — no union needed.
            final PathNode target = summary.getPathNodeForPathNodeKey(targetPathNodeKey);
            if (target == null) return null;
            final HyperLogLogSketch hll = target.getHllSketch();
            computed = new Int64(hll == null ? 0L : hll.estimate());
          } else {
            // Fallback: union across every PathNode with this local name. Matches
            // the pre-refactor behaviour for documents without PathSummary or
            // whose sourcePath can't be disambiguated.
            final List<PathNode> paths = summary.findPathsByLocalName(field);
            if (paths.isEmpty()) {
              return null;
            }
            HyperLogLogSketch union = null;
            for (final PathNode p : paths) {
              final HyperLogLogSketch hll = p.getHllSketch();
              if (hll == null) {
                continue;
              }
              if (union == null) {
                union = new HyperLogLogSketch();
              }
              union.union(hll);
            }
            computed = new Int64(union == null ? 0L : union.estimate());
          }
          countDistinctResultCache.putIfAbsent(cdKey, computed);
          return computed;
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
  public Sequence executeAggregate(QueryContext ctx, String[] sourcePath, String func, String field)
      throws QueryException {
    try {
      // PathStatistics short-circuit: when the resource maintains per-path stats, an
      // unfiltered aggregate over a single field resolves directly from the PathSummary
      // — microseconds instead of a parallel scan of every data page.
      final Sequence fast = tryPathSummaryStats(sourcePath, field, func);
      if (fast != null) {
        return fast;
      }
      // The executor is bound to a single (session, revision) — any aggregate we
      // computed earlier for this field is still valid. ComputeIfAbsent keeps
      // the scan exactly-once even under concurrent callers.
      final String cacheKey = pathCacheKey(sourcePath, field);
      long[] stats = aggregateCache.get(cacheKey);
      if (stats == null) {
        final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
        final long[] fresh = parallelAggregate(field, targetPathNodeKey);
        stats = aggregateCache.putIfAbsent(cacheKey, fresh);
        if (stats == null) stats = fresh;
      }
      final long count = stats[0];
      final long sum = stats[1];
      final long min = stats[2];
      final long max = stats[3];
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
  private Sequence tryPathSummaryStats(final String[] sourcePath, final String field, final String func) {
    if (!session.getResourceConfig().withPathStatistics) {
      return null;
    }
    switch (func) {
      case "count", "sum", "avg", "min", "max" -> { /* supported */ }
      default -> { return null; }
    }
    if (field == null) {
      return null;
    }
    // Resolve the exact PathNode for sourcePath + field. Ambiguous / unresolved
    // paths return -1L — we fall back to the full scan rather than produce a
    // potentially wrong cross-path sum.
    final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
    if (targetPathNodeKey == -1L) {
      return null;
    }
    // Cached per (pathNodeKey, func) — executor is bound to one (session, revision)
    // so the stats snapshot is stable. Hot path: single ConcurrentHashMap probe.
    final String statsKey = targetPathNodeKey + "#" + func;
    final Sequence cached = pathStatsCache.get(statsKey);
    if (cached != null) {
      return cached;
    }
    final Sequence computed = computePathStatsSequence(targetPathNodeKey, func);
    if (computed != null) {
      pathStatsCache.putIfAbsent(statsKey, computed);
    }
    return computed;
  }

  /**
   * Actually read the PathNode's per-path statistics and package them as a
   * Brackit Sequence. Opens a read-only trx + PathSummaryReader for the call
   * duration. Called at most once per {@code (pathNodeKey, func)} during the
   * executor's lifetime — results are cached by the caller.
   */
  private Sequence computePathStatsSequence(final long targetPathNodeKey, final String func) {
    try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final PathSummaryReader summary = rtx.getResourceSession().openPathSummary(revision);
      try {
        final PathNode p = summary.getPathNodeForPathNodeKey(targetPathNodeKey);
        if (p == null) {
          return null;
        }
        if (("min".equals(func) || "avg".equals(func)) && p.isStatsMinDirty()) {
          return null;
        }
        if ("max".equals(func) && p.isStatsMaxDirty()) {
          return null;
        }
        final long count = p.getStatsValueCount();
        final long sum = p.getStatsSum();
        final boolean numeric = p.hasNumericStats();
        final long pMin = numeric ? p.getStatsMin() : Long.MAX_VALUE;
        final long pMax = numeric ? p.getStatsMax() : Long.MIN_VALUE;
        return switch (func) {
          case "count" -> new Int64(count);
          case "sum"   -> new Int64(sum);
          case "avg"   -> count == 0 ? new Int64(0) : new Dbl((double) sum / (double) count);
          case "min"   -> count == 0 || !numeric ? new Int64(0) : new Int64(pMin);
          case "max"   -> count == 0 || !numeric ? new Int64(0) : new Int64(pMax);
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
  private long[] parallelAggregate(final String field, final long targetPathNodeKey) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey < 0) return new long[] { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };

    final long maxNodeKey = getMaxNodeKey();
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
        //
        // Two dispatches:
        //   * No path-scope (targetPathNodeKey == -1)  — lookup by nameKey.
        //   * Path-scope + region is pathNodeKey-tagged — lookup by pathNodeKey
        //     (correctness-safe: a tag hit implies every value in range belongs
        //     to the requested pathNodeKey, so the SIMD kernel cannot over-count).
        //   * Path-scope + region is nameKey-tagged — skip the SIMD path, fall
        //     through to the slot-walk which filters per-slot by pathNodeKey.
        if (PAX_FAST_PATH_ENABLED) {
          final NumberRegion.Header hdr = kv.getNumberRegionHeader();
          if (hdr != null) {
            final int simdLookupTag;
            if (targetPathNodeKey == -1L) {
              simdLookupTag = fieldKey;
            } else if (hdr.tagKind == NumberRegion.TAG_KIND_PATH_NODE
                && targetPathNodeKey > 0L
                && targetPathNodeKey <= (long) Integer.MAX_VALUE) {
              simdLookupTag = (int) targetPathNodeKey;
            } else {
              simdLookupTag = Integer.MIN_VALUE; // sentinel — no tag will match
            }
            final int tag = simdLookupTag == Integer.MIN_VALUE
                ? -1
                : NumberRegion.lookupTag(hdr, simdLookupTag);
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
          // Path-scope filter: drop matches that share the field name but sit
          // at a different tree depth (e.g. $doc.items[].age vs $doc.age).
          if (targetPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, objectKeyNodeKey) != targetPathNodeKey) {
            continue;
          }
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
   * Parallel group-by-count: walks all leaf pages, finds OBJECT_KEY slots whose
   * nameKey matches the pre-resolved key, descends to value, accumulates counts
   * per unique value into a 1BRC-style open-addressing byte-key hash map
   * (per-thread, then merged). Returns a Brackit array of
   * {@code {"groupField": value, "count": N}} objects.
   */
  private Sequence parallelGroupByCount(final String groupField, final long targetPathNodeKey) throws Exception {
    final int fieldKey = resolveFieldKey(groupField);
    if (fieldKey < 0) return new DArray(List.of());

    final long maxNodeKey = getMaxNodeKey();
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
        //
        // Two dispatches:
        //   * No path-scope (targetPathNodeKey == -1): lookup by nameKey. Safe
        //     only when the document has no nested same-name fields — callers
        //     that don't need path scoping accept this.
        //   * Path-scope AND region is path-tagged: lookup by (int)pathNodeKey.
        //     Correctness-safe because the region's tag now encodes the full
        //     tree path, not just the local name.
        //   * Path-scope AND region is name-tagged: skip the SIMD path and fall
        //     through to the slot-walk, which filters per-slot by pathNodeKey.
        final StringRegion.Header sh = kv.getStringRegionHeader();
        final int fastLookupTag;
        if (sh == null) {
          fastLookupTag = Integer.MIN_VALUE;
        } else if (targetPathNodeKey == -1L) {
          fastLookupTag = fieldKey;
        } else if (sh.tagKind == StringRegion.TAG_KIND_PATH_NODE
            && targetPathNodeKey > 0L
            && targetPathNodeKey <= (long) Integer.MAX_VALUE) {
          fastLookupTag = (int) targetPathNodeKey;
        } else {
          fastLookupTag = Integer.MIN_VALUE;  // region name-tagged + path-scoped → fall through
        }
        if (sh == null || fastLookupTag == Integer.MIN_VALUE) {
          if (sh == null && DIAGNOSTICS_ENABLED) diag.stringRegionMissesNoHeader++;
        } else {
          final int tag = StringRegion.lookupTag(sh, fastLookupTag);
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
              // Bulk-count with one-entry word cache inside StringRegion —
              // amortises the 64-bit payload read across consecutive dict-ids
              // that share an 8-byte window (at bw=3 for 8-value dept fields
              // each word covers ~21 dict-ids, so one read serves 21 counts).
              StringRegion.countDictIds(payload, sh, start, n, localDictCounts);
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

          final long objectKeyNodeKey = base + slot;
          // Path-scope filter: reject same-name slots at a different tree
          // depth. No-op when targetPathNodeKey is -1 (unresolvable path).
          if (targetPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, objectKeyNodeKey) != targetPathNodeKey) {
            continue;
          }
          final long firstChildKey = kv.getObjectKeyFirstChildKeyFromSlot(slot, objectKeyNodeKey);
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
   * Anchor-field zone-map probe extracted once per query. Lists the
   * {@code fieldIdx == 0} OP_NUM_CMP leaves of a purely conjunctive
   * (AND-only) predicate tree. On each page's pass we probe the anchor
   * nameKey's per-tag zone map against these constraints; if ANY
   * constraint returns {@link #ZONE_NONE}, the whole page can be
   * skipped without touching a single record. Built lazily; returns
   * {@code null} for predicate shapes we don't handle (OR/NOT, no
   * anchor-field NumCmp constraint, single field-idx that's not 0).
   */
  private static final class AnchorZoneProbe {
    final int n;
    final int[] cmpOps;
    final long[] thresholds;

    AnchorZoneProbe(final int n, final int[] cmpOps, final long[] thresholds) {
      this.n = n;
      this.cmpOps = cmpOps;
      this.thresholds = thresholds;
    }
  }

  /**
   * Extract conjunctive OP_NUM_CMP constraints on the anchor field
   * (fieldIdx 0) from {@code cp}, or {@code null} if the tree contains
   * OR/NOT (which would need per-branch zone probing) or has no anchor
   * NumCmp at all.
   */
  private static AnchorZoneProbe extractAnchorZoneProbe(final CompiledPredicate cp) {
    final int n = cp.ops.length;
    if (n == 0) return null;
    // Purely conjunctive: no OR or NOT anywhere in the tree.
    for (int i = 0; i < n; i++) {
      final byte op = cp.ops[i];
      if (op == CompiledPredicate.OP_OR || op == CompiledPredicate.OP_NOT) return null;
    }
    int[] ops = new int[4];
    long[] ts = new long[4];
    int cnt = 0;
    for (int i = 0; i < n; i++) {
      if (cp.ops[i] == CompiledPredicate.OP_NUM_CMP && cp.fieldIdx[i] == 0) {
        if (cnt == ops.length) {
          ops = java.util.Arrays.copyOf(ops, ops.length * 2);
          ts = java.util.Arrays.copyOf(ts, ts.length * 2);
        }
        ops[cnt] = cp.cmpOp[i];
        ts[cnt] = cp.longLit[i];
        cnt++;
      }
    }
    if (cnt == 0) return null;
    return new AnchorZoneProbe(cnt, ops, ts);
  }

  /**
   * Returns {@code true} if {@code probe}'s conjunctive constraints on
   * the anchor field can be proven unsatisfiable on a page whose
   * anchor-tag zone map spans {@code [tagMin, tagMax]}. First
   * {@link #zoneMapOutcome} == {@link #ZONE_NONE} wins.
   */
  private static boolean canPruneByZone(final AnchorZoneProbe probe, final long tagMin, final long tagMax) {
    final int[] ops = probe.cmpOps;
    final long[] ts = probe.thresholds;
    for (int k = 0, n = probe.n; k < n; k++) {
      if (zoneMapOutcome(ops[k], ts[k], tagMin, tagMax) == ZONE_NONE) return true;
    }
    return false;
  }

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
   * Map our internal int-coded op to Brackit's {@link VectorizedPredicate.ComparisonOp}.
   * Called once per OP_NUM_CMP leaf per page (≈256 rows of work follow it), so
   * the switch cost is noise.
   */
  private static VectorizedPredicate.ComparisonOp toVecCmpOp(final int op) {
    return switch (op) {
      case OP_GT -> VectorizedPredicate.ComparisonOp.GT;
      case OP_LT -> VectorizedPredicate.ComparisonOp.LT;
      case OP_GE -> VectorizedPredicate.ComparisonOp.GE;
      case OP_LE -> VectorizedPredicate.ComparisonOp.LE;
      case OP_EQ -> VectorizedPredicate.ComparisonOp.EQ;
      default    -> throw new IllegalStateException("bad cmpOp " + op);
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

  // ==================================================================
  // Per-predicate class-generated evaluator (Task #29).
  //
  // At query compile time we emit a specialised class implementing
  // BatchPredicate. The class's evaluate() method is straight-line code:
  //
  //   • every opcode is expanded inline (no runtime dispatch)
  //   • threshold constants, field indices, and string-literal indices
  //     are baked into the bytecode as LDC / SIPUSH instructions
  //   • AND/OR/NOT are plain long[] bitwise loops over the per-node
  //     scratch bitmaps that EvalBatch already allocates
  //
  // The class is loaded as a hidden class (Lookup.defineHiddenClass) so
  // the JVM can unload it as soon as the last reference clears.
  //
  // If class generation fails for any reason we fall back to the
  // interpreter — correctness never regresses. See skipCompileSet.
  // ==================================================================

  /**
   * Bytecode-generated equivalent of {@link #evalCompiledBatch}. Implementations
   * are produced on demand by {@link #compileToClass(CompiledPredicate)} and
   * cached in {@link #compiledPredicateCache}. Implementations MUST:
   *
   * <ul>
   *   <li>Write the match bitmap for the root predicate node into {@code out}
   *       covering the valid range {@code [0, batch.size)}. Words beyond the
   *       valid prefix are left unmodified — callers mask {@code size & 63}
   *       when counting.</li>
   *   <li>Populate {@code batch.nodeBitmaps[nodeIdx]} for every non-root node
   *       as a side effect. Callers assume these bitmaps are scratch-only.</li>
   *   <li>Allocate NOTHING on the call path.</li>
   * </ul>
   */
  public interface BatchPredicate {
    void evaluate(EvalBatch batch, long[] out);
  }

  /**
   * Build a hidden class implementing {@link BatchPredicate} for {@code cp}.
   * Called at most once per distinct predicate signature — the result is
   * cached in {@link #compiledPredicateCache}. Uses the Java 25
   * {@code java.lang.classfile} API (no ASM dependency).
   *
   * <p>The emitted {@code evaluate} method:
   * <ol>
   *   <li>Hoists all EvalBatch array references into locals (one aaload per
   *       2D column array, not per node).</li>
   *   <li>For every tree node (in post-order on tree indices), emits an
   *       inline kernel that writes the node's bitmap into
   *       {@code batch.nodeBitmaps[nodeIdx]}.</li>
   *   <li>Ends with a bitwise copy of {@code batch.nodeBitmaps[0]} to the
   *       caller's {@code out} array.</li>
   * </ol>
   *
   * @throws Exception if the classfile emission or hidden-class load fails.
   */
  /**
   * Cache key for the generated-class map: predicate signature plus the
   * distinct ordered field list. Two structurally identical predicate trees
   * with different field sets generate different classes.
   */
  private static String compiledClassKey(final PredicateNode predicate, final CompiledPredicate cp) {
    final StringBuilder sb = new StringBuilder(64);
    appendKey(sb, predicate);
    sb.append("|f:");
    for (int i = 0; i < cp.fieldNames.length; i++) {
      if (i > 0) sb.append(',');
      sb.append(cp.fieldNames[i]);
    }
    return sb.toString();
  }

  /**
   * Resolve a compiled {@link BatchPredicate} for {@code cp} if class generation
   * is enabled and the predicate hasn't been blacklisted from a prior failure.
   * Returns {@code null} if the caller must fall back to {@link #evalCompiledBatch}.
   * Never throws — failures are caught, the key is recorded in
   * {@link #skipCompileSet}, and {@code null} is returned so the caller invokes
   * the interpreter.
   */
  private BatchPredicate resolveCompiledPredicate(final String cacheKey, final CompiledPredicate cp) {
    if (!COMPILED_PREDICATE_ENABLED) return null;
    if (skipCompileSet.contains(cacheKey)) return null;
    final BatchPredicate cached = compiledPredicateCache.get(cacheKey);
    if (cached != null) return cached;
    try {
      final BatchPredicate built = compileToClass(cp);
      final BatchPredicate existing = compiledPredicateCache.putIfAbsent(cacheKey, built);
      return existing != null ? existing : built;
    } catch (Throwable t) {
      // Blacklist the key — do not retry on the hot path. Surface a single
      // stderr log entry for diagnosis.
      if (skipCompileSet.add(cacheKey)) {
        System.err.println("[sirix-vec] compileToClass failed for " + cacheKey + " — falling back ("
            + t.getClass().getSimpleName() + ": " + t.getMessage() + ")");
      }
      return null;
    }
  }

  private BatchPredicate compileToClass(final CompiledPredicate cp) throws Exception {
    final int seq = COMPILED_PREDICATE_SEQ.incrementAndGet();
    final String simpleName = "SirixBatchPred$" + seq;
    final ClassDesc thisClass = ClassDesc.of("io.sirix.query.scan." + simpleName);

    final byte[] classBytes = ClassFile.of().build(thisClass, cb -> emitBatchPredicateClass(cb, thisClass, cp));

    // NESTMATE: join the nest of SirixVectorizedExecutor so the generated code
    // can access private nested types (BatchPredicate, EvalBatch) without
    // synthetic accessor methods.
    final Lookup lookup = MethodHandles.lookup()
        .defineHiddenClass(classBytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
    final Class<?> genClass = lookup.lookupClass();
    try {
      final Object instance = genClass.getDeclaredConstructor(String[].class)
          .newInstance((Object) cp.strLiterals);
      return (BatchPredicate) instance;
    } catch (InvocationTargetException ite) {
      final Throwable cause = ite.getCause();
      if (cause instanceof RuntimeException re) throw re;
      throw new RuntimeException("Generated predicate class constructor threw", cause);
    }
  }

  // ClassDesc constants used across the emitter — hoisted to avoid re-allocating on each compile.
  private static final ClassDesc CD_OBJECT = ClassDesc.ofDescriptor("Ljava/lang/Object;");
  private static final ClassDesc CD_STRING = ClassDesc.ofDescriptor("Ljava/lang/String;");
  private static final ClassDesc CD_VOID = ClassDesc.ofDescriptor("V");
  private static final ClassDesc CD_BATCH_PRED =
      ClassDesc.ofDescriptor("Lio/sirix/query/scan/SirixVectorizedExecutor$BatchPredicate;");
  private static final ClassDesc CD_EVAL_BATCH =
      ClassDesc.ofDescriptor("Lio/sirix/query/scan/SirixVectorizedExecutor$EvalBatch;");
  private static final ClassDesc CD_STRING_ARR = ClassDesc.ofDescriptor("[Ljava/lang/String;");
  private static final ClassDesc CD_STRING_2D = ClassDesc.ofDescriptor("[[Ljava/lang/String;");
  private static final ClassDesc CD_LONG_2D = ClassDesc.ofDescriptor("[[J");
  private static final ClassDesc CD_BYTE_2D = ClassDesc.ofDescriptor("[[B");
  private static final ClassDesc CD_BOOL_2D = ClassDesc.ofDescriptor("[[Z");
  private static final ClassDesc CD_LONG_ARR = ClassDesc.ofDescriptor("[J");
  private static final ClassDesc CD_BYTE_ARR = ClassDesc.ofDescriptor("[B");
  private static final ClassDesc CD_BOOL_ARR = ClassDesc.ofDescriptor("[Z");

  /**
   * Emit the generated class. Structure:
   * <pre>
   *   public final class SirixBatchPred$N implements BatchPredicate {
   *     private final String[] strLits;
   *     public SirixBatchPred$N(String[] lits) { this.strLits = lits; }
   *     public void evaluate(EvalBatch batch, long[] out) {
   *        // hoist all arrays; for every compiled node, inline-evaluate into
   *        // batch.nodeBitmaps[nodeIdx]; finally copy root bitmap into out.
   *     }
   *   }
   * </pre>
   */
  private static void emitBatchPredicateClass(final ClassBuilder cb, final ClassDesc thisClass,
      final CompiledPredicate cp) {
    cb.withSuperclass(CD_OBJECT);
    cb.withInterfaceSymbols(CD_BATCH_PRED);
    cb.withFlags(ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL | ClassFile.ACC_SUPER);

    cb.withField("strLits", CD_STRING_ARR,
        fb -> fb.withFlags(ClassFile.ACC_PRIVATE | ClassFile.ACC_FINAL));

    // Constructor: (String[] lits) { super(); this.strLits = lits; }
    cb.withMethod("<init>", MethodTypeDesc.of(CD_VOID, CD_STRING_ARR),
        ClassFile.ACC_PUBLIC,
        (MethodBuilder mb) -> mb.withCode(code -> {
          code.aload(0);
          code.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.of(CD_VOID));
          code.aload(0);
          code.aload(1);
          code.putfield(thisClass, "strLits", CD_STRING_ARR);
          code.return_();
        }));

    cb.withMethod("evaluate",
        MethodTypeDesc.of(CD_VOID, CD_EVAL_BATCH, CD_LONG_ARR),
        ClassFile.ACC_PUBLIC | ClassFile.ACC_FINAL,
        (MethodBuilder mb) -> mb.withCode(code -> emitEvaluateBody(code, thisClass, cp)));
  }

  /**
   * Local-slot layout for the generated {@code evaluate(EvalBatch, long[])}:
   *
   * <pre>
   *  slot 0   this
   *  slot 1   batch              (EvalBatch)
   *  slot 2   out                (long[])
   *  slot 3   size               (int)    — batch.size
   *  slot 4   stride             (int)    — (size + 63) >>> 6
   *  slot 5   nodeBitmaps        (long[][])
   *  slot 6   presentKind        (byte[][])
   *  slot 7   longCols           (long[][])
   *  slot 8   boolCols           (boolean[][])
   *  slot 9   strCols            (String[][])
   *  slot 10  strLits            (String[])
   *  slot 11+ per-node scratch ints / longs as needed (reused within each kernel)
   * </pre>
   *
   * Each node kernel reuses slots 11..13 (int i, long word) and 14+ as
   * needed. Because we emit straight-line kernels one after the other the
   * slot budget stays bounded: O(1) per tree.
   */
  private static final int SLOT_THIS = 0;
  private static final int SLOT_BATCH = 1;
  private static final int SLOT_OUT = 2;
  private static final int SLOT_SIZE = 3;
  private static final int SLOT_STRIDE = 4;
  private static final int SLOT_NODE_BITMAPS = 5;
  private static final int SLOT_PRESENT_KIND = 6;
  private static final int SLOT_LONG_COLS = 7;
  private static final int SLOT_BOOL_COLS = 8;
  private static final int SLOT_STR_COLS = 9;
  private static final int SLOT_STR_LITS = 10;
  // Scratch slots used within individual kernels — reused node-by-node.
  private static final int SLOT_BM = 11;      // long[] current-node bitmap (ref)
  private static final int SLOT_I = 12;       // int  loop counter
  private static final int SLOT_STR_V = 13;   // String scratch (for STR_EQ)
  private static final int SLOT_LIT = 14;     // String literal (for STR_EQ)
  private static final int SLOT_CHILD_BM = 16; // long[] child bitmap (for NOT/AND/OR)

  /**
   * Emit the evaluate() body. We recursively descend through the
   * {@link CompiledPredicate} tree (node 0 is root) and, for each node,
   * emit a self-contained kernel that populates
   * {@code batch.nodeBitmaps[nodeIdx]}. Finally we copy node 0's bitmap
   * into {@code out}.
   */
  private static void emitEvaluateBody(final CodeBuilder code, final ClassDesc thisClass,
      final CompiledPredicate cp) {
    // 1. Hoist all array references into locals.
    // size = batch.size
    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "size", ClassDesc.ofDescriptor("I"));
    code.istore(SLOT_SIZE);

    // stride = (size + 63) >>> 6
    code.iload(SLOT_SIZE);
    code.bipush(63);
    code.iadd();
    code.bipush(6);
    code.iushr();
    code.istore(SLOT_STRIDE);

    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "nodeBitmaps", CD_LONG_2D);
    code.astore(SLOT_NODE_BITMAPS);

    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "presentKind", CD_BYTE_2D);
    code.astore(SLOT_PRESENT_KIND);

    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "longCols", CD_LONG_2D);
    code.astore(SLOT_LONG_COLS);

    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "boolCols", CD_BOOL_2D);
    code.astore(SLOT_BOOL_COLS);

    code.aload(SLOT_BATCH);
    code.getfield(CD_EVAL_BATCH, "strCols", CD_STRING_2D);
    code.astore(SLOT_STR_COLS);

    code.aload(SLOT_THIS);
    code.getfield(thisClass, "strLits", CD_STRING_ARR);
    code.astore(SLOT_STR_LITS);

    // 2. Emit every tree node in index order. Because children are always at
    // higher indices in the flatten layout, emitting ascending node order
    // would evaluate children before parents — but CompiledPredicate does
    // NOT guarantee that: flatten reserves the parent slot first, then
    // recurses, so children end up at *higher* indices. We exploit that:
    // iterate from n-1 down to 0 to evaluate children first. (Equivalent
    // to post-order for the tree emitted by flatten().)
    final int n = cp.ops.length;
    for (int idx = n - 1; idx >= 0; idx--) {
      emitNodeKernel(code, cp, idx);
    }

    // 3. Copy nodeBitmaps[0] into out.
    // for (int i = 0; i < stride; i++) out[i] = nodeBitmaps[0][i];
    code.aload(SLOT_NODE_BITMAPS);
    code.iconst_0();
    code.aaload();
    code.astore(SLOT_BM); // root bitmap
    final Label copyCond = code.newLabel();
    final Label copyTop = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(copyCond);
    code.labelBinding(copyTop);
    code.aload(SLOT_OUT);
    code.iload(SLOT_I);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.laload();
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(copyCond);
    code.iload(SLOT_I);
    code.iload(SLOT_STRIDE);
    code.if_icmplt(copyTop);

    code.return_();
  }

  /**
   * Emit the straight-line kernel for a single compiled-predicate node. Writes
   * into {@code batch.nodeBitmaps[nodeIdx]}. Semantics match
   * {@link #evalCompiledBatchRec} case-for-case — constants are inlined.
   */
  private static void emitNodeKernel(final CodeBuilder code, final CompiledPredicate cp, final int nodeIdx) {
    final byte op = cp.ops[nodeIdx];
    // Load nodeBitmaps[nodeIdx] into SLOT_BM.
    code.aload(SLOT_NODE_BITMAPS);
    loadIntConst(code, nodeIdx);
    code.aaload();
    code.astore(SLOT_BM);

    // Zero the valid portion: for (int i = 0; i < stride; i++) bm[i] = 0L;
    emitZeroBitmap(code);

    switch (op) {
      case CompiledPredicate.OP_ALWAYS_T -> emitAlwaysTrue(code);
      case CompiledPredicate.OP_ALWAYS_F -> { /* already zero */ }
      case CompiledPredicate.OP_NOT ->
          emitNot(code, cp.children[cp.childStart[nodeIdx]]);
      case CompiledPredicate.OP_AND ->
          emitAnd(code, cp, cp.childStart[nodeIdx], cp.childCount[nodeIdx]);
      case CompiledPredicate.OP_OR ->
          emitOr(code, cp, cp.childStart[nodeIdx], cp.childCount[nodeIdx]);
      case CompiledPredicate.OP_NUM_CMP ->
          emitNumCmp(code, cp.fieldIdx[nodeIdx], cp.cmpOp[nodeIdx], cp.longLit[nodeIdx]);
      case CompiledPredicate.OP_STR_EQ ->
          emitStrEq(code, cp.fieldIdx[nodeIdx], cp.strIdx[nodeIdx]);
      case CompiledPredicate.OP_BOOL_REF ->
          emitBoolRef(code, cp.fieldIdx[nodeIdx]);
      default -> throw new IllegalStateException("unsupported opcode " + op);
    }
  }

  /** bm = nodeBitmaps[nodeIdx] already in SLOT_BM. Zero bm[0..stride). */
  private static void emitZeroBitmap(final CodeBuilder code) {
    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.lconst_0();
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_STRIDE);
    code.if_icmplt(top);
  }

  /**
   * Emit OP_ALWAYS_T: set all valid bits in bm. Full words to -1L, tail word
   * to {@code (1L << (size & 63)) - 1}.
   */
  private static void emitAlwaysTrue(final CodeBuilder code) {
    // full = size >>> 6
    // for (int i = 0; i < full; i++) bm[i] = -1L;
    // final int tail = size & 63;
    // if (tail > 0) bm[full] = (1L << tail) - 1;
    final int SLOT_FULL = SLOT_STR_V;   // reuse int slot
    code.iload(SLOT_SIZE);
    code.bipush(6);
    code.iushr();
    code.istore(SLOT_FULL);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.ldc(-1L);
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_FULL);
    code.if_icmplt(top);

    // tail = size & 63; store in SLOT_I (its loop use is done).
    code.iload(SLOT_SIZE);
    code.bipush(63);
    code.iand();
    code.istore(SLOT_I);
    final Label noTail = code.newLabel();
    code.iload(SLOT_I);
    code.ifeq(noTail);
    // bm[full] = (1L << tail) - 1L
    code.aload(SLOT_BM);
    code.iload(SLOT_FULL);
    code.lconst_1();
    code.iload(SLOT_I);
    code.lshl();
    code.lconst_1();
    code.lsub();
    code.lastore();
    code.labelBinding(noTail);
  }

  /**
   * bm = ~childBm, masked to the valid prefix. Emits:
   * <pre>
   *   final int full = size >>> 6;
   *   for (int i = 0; i < full; i++) bm[i] = ~childBm[i];
   *   final int tail = size & 63;
   *   if (tail > 0) bm[full] = (~childBm[full]) & ((1L << tail) - 1L);
   * </pre>
   */
  private static void emitNot(final CodeBuilder code, final int childIdx) {
    // Load childBm
    code.aload(SLOT_NODE_BITMAPS);
    loadIntConst(code, childIdx);
    code.aaload();
    code.astore(SLOT_CHILD_BM);

    final int SLOT_FULL = SLOT_STR_V;
    code.iload(SLOT_SIZE);
    code.bipush(6);
    code.iushr();
    code.istore(SLOT_FULL);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.aload(SLOT_CHILD_BM);
    code.iload(SLOT_I);
    code.laload();
    code.ldc(-1L);
    code.lxor();
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_FULL);
    code.if_icmplt(top);

    // tail
    final Label noTail = code.newLabel();
    code.iload(SLOT_SIZE);
    code.bipush(63);
    code.iand();
    code.ifeq(noTail);
    code.aload(SLOT_BM);
    code.iload(SLOT_FULL);
    code.aload(SLOT_CHILD_BM);
    code.iload(SLOT_FULL);
    code.laload();
    code.ldc(-1L);
    code.lxor();
    code.lconst_1();
    code.iload(SLOT_SIZE);
    code.bipush(63);
    code.iand();
    code.lshl();
    code.lconst_1();
    code.lsub();
    code.land();
    code.lastore();
    code.labelBinding(noTail);
  }

  /**
   * bm = firstChild; for k in 1..n-1: bm &amp;= childKBm;
   * Vacuously true for n==0.
   */
  private static void emitAnd(final CodeBuilder code, final CompiledPredicate cp, final int start, final int n) {
    if (n == 0) { emitAlwaysTrue(code); return; }
    // bm = first child's bitmap (copy stride words)
    final int firstChild = cp.children[start];
    emitBitmapCopy(code, firstChild);
    for (int k = 1; k < n; k++) {
      final int ci = cp.children[start + k];
      emitBitmapAnd(code, ci);
    }
  }

  /** bm = 0; for k in 0..n-1: bm |= childKBm; */
  private static void emitOr(final CodeBuilder code, final CompiledPredicate cp, final int start, final int n) {
    // already zero from emitZeroBitmap
    for (int k = 0; k < n; k++) {
      final int ci = cp.children[start + k];
      emitBitmapOr(code, ci);
    }
  }

  /** for i in 0..stride-1: bm[i] = nodeBitmaps[childIdx][i]; */
  private static void emitBitmapCopy(final CodeBuilder code, final int childIdx) {
    code.aload(SLOT_NODE_BITMAPS);
    loadIntConst(code, childIdx);
    code.aaload();
    code.astore(SLOT_CHILD_BM);
    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.aload(SLOT_CHILD_BM);
    code.iload(SLOT_I);
    code.laload();
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_STRIDE);
    code.if_icmplt(top);
  }

  /** for i in 0..stride-1: bm[i] &amp;= nodeBitmaps[childIdx][i]; */
  private static void emitBitmapAnd(final CodeBuilder code, final int childIdx) {
    code.aload(SLOT_NODE_BITMAPS);
    loadIntConst(code, childIdx);
    code.aaload();
    code.astore(SLOT_CHILD_BM);
    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    // existing bm[i]
    code.dup2();               // [bm, i, bm, i]
    code.laload();             // [bm, i, bm[i](long)]
    code.aload(SLOT_CHILD_BM);
    code.iload(SLOT_I);
    code.laload();             // [bm, i, bm[i], child[i]]
    code.land();               // [bm, i, result]
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_STRIDE);
    code.if_icmplt(top);
  }

  /** for i in 0..stride-1: bm[i] |= nodeBitmaps[childIdx][i]; */
  private static void emitBitmapOr(final CodeBuilder code, final int childIdx) {
    code.aload(SLOT_NODE_BITMAPS);
    loadIntConst(code, childIdx);
    code.aaload();
    code.astore(SLOT_CHILD_BM);
    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.dup2();
    code.laload();
    code.aload(SLOT_CHILD_BM);
    code.iload(SLOT_I);
    code.laload();
    code.lor();
    code.lastore();
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_STRIDE);
    code.if_icmplt(top);
  }

  /**
   * Numeric comparison kernel:
   * <pre>
   *   final byte[] pk = presentKind[fieldIdx];
   *   final long[] vals = longCols[fieldIdx];
   *   for (int i = 0; i < size; i++) {
   *     if (pk[i] == 1 &amp;&amp; vals[i] {cmp} lit) bm[i>>>6] |= 1L << (i &amp; 63);
   *   }
   * </pre>
   * Comparison is constant-folded in the emitted bytecode via the opcode
   * selection below (one lcmp + if-family per row).
   */
  private static void emitNumCmp(final CodeBuilder code, final int fieldIdx, final int cmpOp, final long lit) {
    // pkLocal = presentKind[fieldIdx] — overlay SLOT_CHILD_BM for a byte[] ref
    final int SLOT_PK = SLOT_CHILD_BM;  // byte[]
    final int SLOT_VALS = SLOT_CHILD_BM + 1;  // long[]
    code.aload(SLOT_PRESENT_KIND);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_PK);
    code.aload(SLOT_LONG_COLS);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_VALS);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    final Label skip = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    // if (pk[i] != 1) goto skip
    code.aload(SLOT_PK);
    code.iload(SLOT_I);
    code.baload();
    code.iconst_1();
    code.if_icmpne(skip);
    // vals[i] {cmp} lit?
    code.aload(SLOT_VALS);
    code.iload(SLOT_I);
    code.laload();
    code.ldc(lit);
    code.lcmp();  // stack: int (-1, 0, 1)
    // lcmp result vs 0 — use the right branch
    //   GT (1): lcmp > 0 iff val > lit      -> iflt skip (skip if lcmp<=0)
    //   LT (2): lcmp < 0 iff val < lit      -> ifgt skip (skip if lcmp>=0)... inverted
    //   GE (3): lcmp >= 0
    //   LE (4): lcmp <= 0
    //   EQ (5): lcmp == 0
    switch (cmpOp) {
      case OP_GT -> code.ifle(skip);
      case OP_LT -> code.ifge(skip);
      case OP_GE -> code.iflt(skip);
      case OP_LE -> code.ifgt(skip);
      case OP_EQ -> code.ifne(skip);
      default -> throw new IllegalStateException("bad cmpOp " + cmpOp);
    }
    // bm[i>>>6] |= 1L << (i & 63)
    emitSetBit(code);
    code.labelBinding(skip);
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_SIZE);
    code.if_icmplt(top);
  }

  /**
   * String-equality kernel:
   * <pre>
   *   final String lit = strLits[strIdx];
   *   final int litLen = lit.length();
   *   final byte[] pk = presentKind[fieldIdx];
   *   final String[] vals = strCols[fieldIdx];
   *   for (int i = 0; i < size; i++) {
   *     if (pk[i] != 3) continue;
   *     final String v = vals[i];
   *     if (v == lit || (v != null &amp;&amp; v.length() == litLen &amp;&amp; lit.equals(v)))
   *         bm[i>>>6] |= 1L << (i &amp; 63);
   *   }
   * </pre>
   */
  private static void emitStrEq(final CodeBuilder code, final int fieldIdx, final int strIdx) {
    // Load lit into SLOT_LIT; litLen into SLOT_STR_V (int)
    code.aload(SLOT_STR_LITS);
    loadIntConst(code, strIdx);
    code.aaload();
    code.astore(SLOT_LIT);

    code.aload(SLOT_LIT);
    code.invokevirtual(CD_STRING, "length", MethodTypeDesc.of(ClassDesc.ofDescriptor("I")));
    code.istore(SLOT_STR_V);

    // pk = presentKind[fi], vals = strCols[fi]
    final int SLOT_PK = SLOT_CHILD_BM;
    final int SLOT_VALS = SLOT_CHILD_BM + 1;
    code.aload(SLOT_PRESENT_KIND);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_PK);
    code.aload(SLOT_STR_COLS);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_VALS);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    final Label skip = code.newLabel();
    final Label setBit = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    // if (pk[i] != 3) skip
    code.aload(SLOT_PK);
    code.iload(SLOT_I);
    code.baload();
    code.iconst_3();
    code.if_icmpne(skip);
    // v = vals[i]
    final int SLOT_V = SLOT_CHILD_BM + 2;
    code.aload(SLOT_VALS);
    code.iload(SLOT_I);
    code.aaload();
    code.astore(SLOT_V);
    // if (v == lit) -> setBit
    code.aload(SLOT_V);
    code.aload(SLOT_LIT);
    code.if_acmpeq(setBit);
    // if (v == null) skip
    code.aload(SLOT_V);
    code.ifnull(skip);
    // if (v.length() != litLen) skip
    code.aload(SLOT_V);
    code.invokevirtual(CD_STRING, "length", MethodTypeDesc.of(ClassDesc.ofDescriptor("I")));
    code.iload(SLOT_STR_V);
    code.if_icmpne(skip);
    // if (!lit.equals(v)) skip
    code.aload(SLOT_LIT);
    code.aload(SLOT_V);
    code.invokevirtual(CD_STRING, "equals",
        MethodTypeDesc.of(ClassDesc.ofDescriptor("Z"), CD_OBJECT));
    code.ifeq(skip);
    code.labelBinding(setBit);
    emitSetBit(code);
    code.labelBinding(skip);
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_SIZE);
    code.if_icmplt(top);
  }

  /**
   * Boolean-ref kernel:
   * <pre>
   *   final byte[] pk = presentKind[fieldIdx];
   *   final boolean[] vals = boolCols[fieldIdx];
   *   for (int i = 0; i < size; i++) {
   *     if (pk[i] == 2 &amp;&amp; vals[i]) bm[i>>>6] |= 1L << (i &amp; 63);
   *   }
   * </pre>
   */
  private static void emitBoolRef(final CodeBuilder code, final int fieldIdx) {
    final int SLOT_PK = SLOT_CHILD_BM;
    final int SLOT_VALS = SLOT_CHILD_BM + 1;
    code.aload(SLOT_PRESENT_KIND);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_PK);
    code.aload(SLOT_BOOL_COLS);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_VALS);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    final Label skip = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    code.aload(SLOT_PK);
    code.iload(SLOT_I);
    code.baload();
    code.iconst_2();
    code.if_icmpne(skip);
    code.aload(SLOT_VALS);
    code.iload(SLOT_I);
    code.baload();  // boolean[] via baload
    code.ifeq(skip);
    emitSetBit(code);
    code.labelBinding(skip);
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_SIZE);
    code.if_icmplt(top);
  }

  /**
   * Emit {@code bm[i>>>6] |= 1L << (i &amp; 63);}. {@code i} must be in
   * {@link #SLOT_I}, {@code bm} in {@link #SLOT_BM}.
   */
  private static void emitSetBit(final CodeBuilder code) {
    code.aload(SLOT_BM);
    code.iload(SLOT_I);
    code.bipush(6);
    code.iushr();
    // stack: [bm, wordIdx]
    code.dup2();  // [bm, wordIdx, bm, wordIdx]
    code.laload();  // [bm, wordIdx, bm[wordIdx]]
    code.lconst_1();
    code.iload(SLOT_I);
    code.bipush(63);
    code.iand();
    code.lshl();    // [bm, wordIdx, bm[wordIdx], 1L<<bit]
    code.lor();     // [bm, wordIdx, result]
    code.lastore();
  }

  /** Push an int constant using the smallest applicable opcode. */
  private static void loadIntConst(final CodeBuilder code, final int v) {
    if (v >= -1 && v <= 5) {
      switch (v) {
        case -1 -> code.iconst_m1();
        case 0 -> code.iconst_0();
        case 1 -> code.iconst_1();
        case 2 -> code.iconst_2();
        case 3 -> code.iconst_3();
        case 4 -> code.iconst_4();
        case 5 -> code.iconst_5();
      }
    } else if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE) {
      code.bipush(v);
    } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE) {
      code.sipush(v);
    } else {
      code.ldc(v);
    }
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
      // Surface the root cause's message in the RuntimeException so call-site
      // logs ({@code QueryException.getMessage()}) show what actually failed
      // instead of the generic "Parallel scan failed" wrapper. The original
      // exception stays as cause so full stack traces are preserved.
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      while (cause.getCause() != null && cause.getCause() != cause) {
        cause = cause.getCause();
      }
      final String msg = cause.getClass().getSimpleName() + ": " + cause.getMessage();
      throw new RuntimeException("Parallel scan failed — " + msg, e);
    }
  }
}
