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
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.util.simd.VectorOps;
import io.brackit.query.util.simd.VectorizedPredicate;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.index.pageskip.PageSkipRegistry;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexScan;
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

import org.roaringbitmap.RoaringBitmap;

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

  /** Fused kind ids — each plays the OBJECT_KEY structural role (carries a name). */
  private static final int OBJECT_NAMED_BOOLEAN_KIND = 48;
  private static final int OBJECT_NAMED_NUMBER_KIND = 49;
  private static final int OBJECT_NAMED_STRING_KIND = 50;
  private static final int OBJECT_NAMED_NULL_KIND = 51;
  /** iter#32 P2 fused-structural — same OBJECT_KEY role with an inline object/array body. */
  private static final int OBJECT_NAMED_OBJECT_KIND = 52;
  private static final int OBJECT_NAMED_ARRAY_KIND = 53;

  /** Returns {@code true} if the kindId plays the OBJECT_KEY role (legacy or fused). */
  private static boolean playsObjectKeyRole(final int kindId) {
    return kindId == OBJECT_KEY_KIND
        || kindId == OBJECT_NAMED_BOOLEAN_KIND
        || kindId == OBJECT_NAMED_NUMBER_KIND
        || kindId == OBJECT_NAMED_STRING_KIND
        || kindId == OBJECT_NAMED_NULL_KIND
        || kindId == OBJECT_NAMED_OBJECT_KIND
        || kindId == OBJECT_NAMED_ARRAY_KIND;
  }

  private final JsonResourceSession session;
  private final int revision;
  private final int threads;

  /**
   * Cached resource-identifier string used as the
   * {@link ProjectionIndexRegistry} lookup key. Computed once at
   * construction to keep the per-query fast-path check branch-free and
   * allocation-free. {@code null} on any resolution failure — disables
   * the fast path for this executor.
   */
  private final String projectionRegistryKey;

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
  /** Diagnostic: force {@code parallelGroupByCount} off the StringRegion fast path. */
  private static final boolean STRING_REGION_FAST_OFF = Boolean.getBoolean("sirix.scan.stringRegion.off");

  /**
   * iter#07 range-fusion — when the same column has one {@code GT}/{@code GE}
   * predicate and one {@code LT}/{@code LE} predicate in the same conjunctive
   * projection-scan, fuse them into a single {@code BETWEEN_*} op that shares
   * one scalar-to-scratch load and one zone-map probe. Toggle via
   * {@code -Dsirix.projection.rangeFusion=false} to disable (defaults
   * <b>on</b>). See {@code profiling-output/iter07-range-fusion-analysis.md}
   * for the correctness derivation and measurement plan.
   */
  private static final boolean RANGE_FUSION_ENABLED =
      !"false".equalsIgnoreCase(System.getProperty("sirix.projection.rangeFusion", "true"));

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
    long groupByDirectSuccess;  // fused inline read produced > 0 bytes
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
   * {@link #parallelGroupByCount(String)}. The Sequence is a flat
   * {@code ItemSequence} of immutable {@code ArrayObject}s — safe to share
   * across callers. Same validity argument as the aggregate/filter caches:
   * the executor is bound to one (session, revision).
   */
  private final ConcurrentHashMap<String, Sequence> groupByCountCache = new ConcurrentHashMap<>();

  /**
   * Per-(group, filter) cache of filtered-group-by-count. Same validity
   * argument as the other caches — executor is bound to one (session,
   * revision). Key is {@code groupField|filterField|filterOp|filterValue}.
   */
  private final ConcurrentHashMap<String, Sequence> filteredGroupByCountCache = new ConcurrentHashMap<>();

  /**
   * Cache for the generalized (multi-key / renamed-output / typed-key)
   * group-by-count results. Same validity argument — bound to one
   * (session, revision). Key encodes fields, output names, count name and
   * the predicate signature.
   */
  private final ConcurrentHashMap<String, Sequence> multiGroupByCountCache = new ConcurrentHashMap<>();

  /**
   * Double-typed aggregate stats for columns carrying non-integral numbers —
   * parallel to {@code aggregateCache}; same (session, revision) validity.
   * Layout: {@code [count, sum, min, max]} as doubles.
   */
  private final ConcurrentHashMap<String, double[]> aggregateDblCache = new ConcurrentHashMap<>();

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
    this.projectionRegistryKey = computeProjectionRegistryKey(session);
    final ThreadFactory tf = r -> {
      Thread t = new Thread(r, "sirix-vec-exec");
      t.setDaemon(true);
      return t;
    };
    this.workerPool = Executors.newFixedThreadPool(threads, tf);
  }

  private static String computeProjectionRegistryKey(final JsonResourceSession session) {
    try {
      return session.getResourceConfig().getResource().toString();
    } catch (final Exception e) {
      return null;
    }
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
        // Projection fast-path: if a covering projection index is installed
        // AND it carries `groupField` as a STRING_DICT column, answer the
        // unpredicated groupBy-count entirely from the in-memory projection
        // bytes — no per-page page load, no slot-walk, no PageSummary lookup.
        // At cold 100 M this is the difference between ~27 s (slot-walk) and
        // ~1 ms (in-memory SIMD scan).
        final Sequence projected = tryProjectionIndexGroupByCountOnly(sourcePath, groupField);
        if (projected != null) {
          cached = groupByCountCache.putIfAbsent(cacheKey, projected);
          if (cached == null) cached = projected;
          return cached;
        }
        final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, groupField);
        Sequence fresh;
        if (isTypedPrimitiveKind(probeFirstAnchorSlotKind(groupField))) {
          // Number/boolean/null-keyed column: go straight to the typed kernel
          // (columnar NumberRegion/BooleanRegion page paths inside) — the string
          // kernel would full-scan, mismatch, and redo anyway.
          fresh = typedGroupByCount(sourcePath,
                                    null,
                                    new String[] { groupField },
                                    new String[] { groupField },
                                    "count");
        } else {
          // String fast path first (StringRegion / byte-key kernel) — then VERIFY:
          // it only understands STRING values, so any visited record that did not
          // contribute (numeric / boolean / null / complex value) shows up as a
          // visited-vs-counted mismatch and the scan is redone by the typed kernel.
          // Historically the mismatch was silent and such group-bys returned an
          // EMPTY (or partial) sequence — wrong results, not a degradation.
          //
          // SPARSE group field: records lacking the field are invisible to the
          // anchor scan. When the record total is known (top-level array) and
          // some records were never visited, redo with the typed kernel, which
          // synthesizes the missing-key group — the interpreter's behavior.
          // (This used to be a loud QueryException; correct beats loud.)
          final long[] scanStats = new long[2];
          fresh = parallelGroupByCount(groupField, targetPathNodeKey, scanStats);
          final boolean pathScopingSound = targetPathNodeKey != -1L || resolveFieldKey(groupField) == -1;
          final long recordCount = pathScopingSound ? topLevelRecordCountOrUnknown(sourcePath) : -1L;
          final boolean sparseGap = recordCount >= 0 && scanStats[0] != recordCount;
          if (sparseGap || scanStats[0] != scanStats[1]) {
            fresh = typedGroupByCount(sourcePath,
                                      null,
                                      new String[] { groupField },
                                      new String[] { groupField },
                                      "count");
          }
        }
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
   * Unpredicated group-by-count via the projection index — no filter, just
   * aggregate the {@code groupField} column across all rows. Returns
   * {@code null} when the projection isn't usable (no handle, group column
   * not STRING_DICT, etc.) so the caller falls back to the generic slot-walk.
   *
   * <p>The zero-predicate path re-uses {@link ProjectionIndexByteScan#conjunctiveCountByGroup}
   * with an empty {@code ColumnPredicate[]}: the evaluator fills an
   * all-true mask and the grouping loop immediately bumps every row. That
   * makes it a pure columnar read over 100 M dict-ids with no per-leaf
   * predicate tree to evaluate.
   */
  private Sequence tryProjectionIndexGroupByCountOnly(final String[] sourcePath, final String groupField) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) return null;
    final int groupColumn = handle.columnOf(groupField);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate: the group column must carry presence data in
    // every leaf and never hold unrepresentable values (null/object/array/
    // kind mismatch) — otherwise missing-vs-default and null-vs-missing are
    // indistinguishable in the columnar layout. Fail closed to the scan path.
    if (!handle.columnSparseClean(groupColumn)) return null;
    final ProjectionIndexScan.ColumnPredicate[] emptyPreds = new ProjectionIndexScan.ColumnPredicate[0];
    final long[] missing = new long[1];
    final Object2LongOpenHashMap<String> agg;
    try {
      agg = parallelConjunctiveCountByGroup(handle, emptyPreds, groupColumn, missing);
    } catch (final IllegalStateException ise) {
      return null;
    }
    if (agg == null) return null;
    final ArrayList<Item> outItems = new ArrayList<>(agg.size() + 1);
    final QNm keyQnm = new QNm(groupField);
    final QNm cntQnm = new QNm("count");
    final ObjectIterator<Object2LongMap.Entry<String>> it = agg.object2LongEntrySet().fastIterator();
    while (it.hasNext()) {
      final Object2LongMap.Entry<String> e = it.next();
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { new Str(e.getKey()), new Int64(e.getLongValue()) };
      outItems.add(new ArrayObject(keys, vals));
    }
    if (missing[0] > 0) {
      // Records lacking the group field — the interpreter groups them under
      // the empty key, which serializes as a null group value.
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { Null.INSTANCE, new Int64(missing[0]) };
      outItems.add(new ArrayObject(keys, vals));
    }
    return new ItemSequence(outItems.toArray(new Item[0]));
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
        return new ItemSequence();
      }
      final String cacheKey =
          "gb:" + pathCacheKey(sourcePath, groupField) + "#" + predicateCacheKey(predicate);
      Sequence cached = filteredGroupByCountCache.get(cacheKey);
      if (cached != null) return cached;
      // String kernel first, then VERIFY: rows that passed the predicate but did
      // not group (non-string / null / missing group value) force a typed redo —
      // previously they were dropped silently (wrong results).
      final long[] scanStats = new long[2];
      Sequence fresh = parallelGenericPredicateGroupByCount(sourcePath, predicate, groupField, scanStats);
      if (fresh == null || scanStats[0] != scanStats[1]) {
        fresh = typedGroupByCount(sourcePath,
                                  predicate,
                                  new String[] { groupField },
                                  new String[] { groupField },
                                  "count");
      }
      cached = filteredGroupByCountCache.putIfAbsent(cacheKey, fresh);
      return cached == null ? fresh : cached;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized predicate-group-by-count failed: %s",
                               e.getMessage());
    }
  }

  // ==================== Typed (multi-key) group-by ====================

  @Override
  public boolean supportsMultiKeyGroupBy() {
    return true;
  }

  /**
   * Generalized group-by-count entry point: one or more key fields, query-defined
   * output names, optional predicate, and — unlike the string-only kernels — group
   * key values keep their JSON types (numbers group as numbers, booleans as
   * booleans). See the {@code VectorizedExecutor} RESULT ENVELOPE CONTRACT.
   */
  @Override
  public Sequence executeGroupByCountMulti(QueryContext ctx, String[] sourcePath, String[] groupFields,
      String[] outNames, String countName, PredicateNode predicate) throws QueryException {
    if (groupFields == null || groupFields.length == 0 || outNames == null || outNames.length != groupFields.length
        || countName == null) {
      return null;
    }
    try {
      if (predicate instanceof PredicateNode.AlwaysFalse) {
        return new ItemSequence();
      }
      final String cacheKey = "gbm:" + pathCacheKey(sourcePath, String.join("\u0001", groupFields)) + "|"
          + String.join("\u0001", outNames) + "|" + countName + "#"
          + (predicate == null ? "-" : predicateCacheKey(predicate));
      Sequence cached = multiGroupByCountCache.get(cacheKey);
      if (cached != null) return cached;
      // Projection fast path: when a covering projection carries EVERY group field
      // as a STRING_DICT column (and the predicate, if any, is projection-evaluable),
      // the composite grouping is a dict-id sweep over in-memory columnar leaves —
      // the same memory-bandwidth-bound shape as a dedicated column store. Falls
      // back to the typed slot-walk kernel on any mismatch.
      Sequence fresh = null;
      final Object2LongOpenHashMap<String> projected =
          tryProjectionMultiGroupCounts(sourcePath, groupFields, predicate);
      if (projected != null) {
        fresh = buildTypedGroupRecords(projected, outNames, countName);
      }
      if (fresh == null) {
        fresh = typedGroupByCount(sourcePath, predicate, groupFields, outNames, countName);
      }
      cached = multiGroupByCountCache.putIfAbsent(cacheKey, fresh);
      return cached == null ? fresh : cached;
    } catch (final QueryException qe) {
      throw qe;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized multi-key group-by-count failed: %s",
                               e.getMessage());
    }
  }

  /** Typed group-key kinds — see {@link #loadFieldsTypedGroups}. */
  private static final byte TK_MISSING = 0;
  private static final byte TK_LONG = 1;
  private static final byte TK_BOOL = 2;
  private static final byte TK_STR = 3;
  private static final byte TK_NULL = 4;
  private static final byte TK_DBL = 5;
  private static final byte TK_DECSTR = 6;
  private static final byte TK_COMPLEX = 7;

  /**
   * Per-record typed view of the fields in a {@link CompiledPredicate} layout.
   * Parallel to {@link EvalScratch} but WITHOUT the legacy lossy conventions:
   * doubles stay doubles (EvalScratch truncates to long for the integer-only
   * predicate ops), JSON null is distinguished from a missing field, and
   * object/array values are tagged complex instead of dropped.
   */
  private static final class TypedGroupScratch {
    final byte[] kind;
    final long[] lng;
    final double[] dbl;
    final boolean[] bool;
    final String[] str;

    TypedGroupScratch(final int nFields) {
      kind = new byte[nFields];
      lng = new long[nFields];
      dbl = new double[nFields];
      bool = new boolean[nFields];
      str = new String[nFields];
    }
  }

  /**
   * Typed grouping kernel: anchor-slot walk + per-record predicate evaluation
   * (identical semantics to the string kernels), but the group key is a TYPED
   * composite of one or more field values, so numeric / boolean / null / missing
   * keys group and emit correctly instead of being silently dropped.
   *
   * <p>Anchor visibility caveat (shared with every anchor-based kernel here):
   * records lacking the anchor field are never visited. For the UNPREDICATED
   * top-level-array case this is detected via {@link #requireDenseGroupField}
   * and surfaces as a loud error rather than silently missing groups.
   */
  /**
   * Projection-backed numeric aggregate: a NUMERIC_LONG column sweep over the
   * covering projection's leaves. Returns {@code [count, sum, min, max]} or
   * {@code null} when the projection cannot serve it (no handle, field not a
   * NUMERIC_LONG column, predicate not a supported conjunction).
   */
  private long[] tryProjectionAggregate(final String[] sourcePath, final String field,
      final PredicateNode predicateOrNull) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) return null;
    final java.util.List<byte[]> leafPayloads = handle.leafPayloads();
    if (leafPayloads.isEmpty()) return null;
    final int col = handle.columnOf(field);
    if (col < 0 || leafPayloads.get(0)[24 + col] != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
      return null;
    }
    // Value-exact gate: the builder truncates non-integral numbers into the
    // NUMERIC_LONG column (Number#longValue). Serve aggregates only when the
    // column is PROVABLY integral; unknown provenance falls back.
    if (!handle.numericColumnIsIntegral(col)) {
      return null;
    }
    // Sparse-evidence gate: without per-row presence, MISSING rows would fold
    // their phantom default 0 into count/min/max (sum survives by luck).
    if (!handle.columnSparseClean(col)) {
      return null;
    }
    final ProjectionIndexScan.ColumnPredicate[] preds;
    if (predicateOrNull == null) {
      preds = new ProjectionIndexScan.ColumnPredicate[0];
    } else {
      final CompiledPredicate cp = compile(predicateOrNull);
      if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
      final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
      if (extracted == null) return null;
      preds = fuseRangePredicates(extracted);
    }
    final int leafCount = leafPayloads.size();
    try {
      if (leafCount < 64) {
        final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(leafPayloads, preds, col, acc);
        return acc;
      }
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final long[][] perThread = new long[eff][];
      final int chunkSize = (leafCount + eff - 1) / eff;
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(leafPayloads.subList(from, to), preds, col, acc);
        perThread[idx] = acc;
      });
      final long[] merged = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
      for (final long[] a : perThread) {
        if (a == null || a[0] == 0) continue;
        merged[0] += a[0];
        merged[1] += a[1];
        if (a[2] < merged[2]) merged[2] = a[2];
        if (a[3] > merged[3]) merged[3] = a[3];
      }
      return merged;
    } catch (final IllegalStateException ise) {
      return null;
    }
  }

  /**
   * Multi-key projection group-by: composite dict-id counting over the covering
   * projection's columnar leaves. Returns composite-encoded group counts (the
   * typed-kernel key encoding), or {@code null} when the projection cannot serve
   * this query (no handle, a group field missing or not STRING_DICT, predicate
   * not a supported conjunction) — callers then use the typed slot-walk kernel.
   */
  private Object2LongOpenHashMap<String> tryProjectionMultiGroupCounts(final String[] sourcePath,
      final String[] groupFields, final PredicateNode predicateOrNull) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) return null;
    final java.util.List<byte[]> leafPayloads = handle.leafPayloads();
    if (leafPayloads.isEmpty()) return null;
    final int[] cols = new int[groupFields.length];
    final byte[] firstLeaf = leafPayloads.get(0);
    for (int i = 0; i < groupFields.length; i++) {
      final int col = handle.columnOf(groupFields[i]);
      if (col < 0 || firstLeaf[24 + col] != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
        return null;
      }
      // Sparse-evidence gate: the composite kernel emits the 'm' missing
      // segment from presence bitmaps; without them (or with null/complex
      // values poisoning the column) the typed slot-walk kernel is the
      // correct fallback.
      if (!handle.columnSparseClean(col)) {
        return null;
      }
      cols[i] = col;
    }
    final ProjectionIndexScan.ColumnPredicate[] preds;
    if (predicateOrNull == null) {
      preds = new ProjectionIndexScan.ColumnPredicate[0];
    } else {
      final CompiledPredicate cp = compile(predicateOrNull);
      if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
      final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
      if (extracted == null) return null;
      preds = fuseRangePredicates(extracted);
    }
    final int leafCount = leafPayloads.size();
    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    try {
      if (leafCount < 64) {
        ProjectionIndexByteScan.conjunctiveCountByGroupMulti(leafPayloads, preds, cols, merged);
        return merged;
      }
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      @SuppressWarnings("unchecked")
      final Object2LongOpenHashMap<String>[] perThread = new Object2LongOpenHashMap[eff];
      final int chunkSize = (leafCount + eff - 1) / eff;
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final Object2LongOpenHashMap<String> local = new Object2LongOpenHashMap<>();
        local.defaultReturnValue(0L);
        ProjectionIndexByteScan.conjunctiveCountByGroupMulti(leafPayloads.subList(from, to), preds, cols, local);
        perThread[idx] = local;
      });
      for (final var m : perThread) {
        if (m == null) continue;
        final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
        while (it.hasNext()) {
          final Object2LongMap.Entry<String> e = it.next();
          merged.addTo(e.getKey(), e.getLongValue());
        }
      }
      return merged;
    } catch (final IllegalStateException ise) {
      // Column-kind drift across leaves or similar — typed kernel handles it.
      return null;
    }
  }

  /**
   * Slot kind of the FIRST anchor slot found for {@code field}, or {@code -1}
   * when none exists. One page probe in the common case — used to route
   * number/boolean-keyed group-bys straight to the typed kernel instead of
   * paying a doomed full string-kernel scan first. Mixed-type columns stay
   * safe regardless of the probe's answer: the string path verifies
   * visited-vs-contributed, and the typed path is type-complete.
   */
  private int probeFirstAnchorSlotKind(final String field) {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey == -1) return -1;
    try {
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final IndexLogKey key = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long totalPages = (getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
      for (long pk = 0; pk < totalPages; pk++) {
        final var res = reader.getRecordPage(key.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        if (matches.length > 0) {
          return kv.getSlotNodeKindId(matches[0]);
        }
      }
    } catch (final Exception ignored) {
      // Probe is best-effort routing only.
    }
    return -1;
  }

  private static boolean isTypedPrimitiveKind(final int kindId) {
    return kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
        || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID
        || kindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NULL_KIND_ID;
  }

  private Sequence typedGroupByCount(final String[] sourcePath, final PredicateNode predicateOrNull,
      final String[] groupFields, final String[] outNames, final String countName) throws Exception {
    final Object2LongOpenHashMap<String> merged =
        typedGroupKeyCounts(sourcePath, predicateOrNull, groupFields);
    return buildTypedGroupRecords(merged, outNames, countName);
  }

  /**
   * The typed grouping scan shared by {@link #typedGroupByCount} and the
   * count-distinct redo: returns composite-encoded group keys → counts.
   */
  private Object2LongOpenHashMap<String> typedGroupKeyCounts(final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] groupFields) throws Exception {
    final PredicateNode effective = predicateOrNull == null ? PredicateNode.AlwaysTrue.INSTANCE : predicateOrNull;
    final CompiledPredicate cp = compileWithExtraFields(effective, groupFields);
    final int[] groupIdxs = new int[groupFields.length];
    for (int i = 0; i < groupFields.length; i++) {
      groupIdxs[i] = indexOfStr(cp.fieldNames, groupFields[i]);
    }
    final int anchorNameKey = cp.fieldNameKeys[0];
    final boolean anchorIsGroupField = predicateOrNull == null;
    if (anchorNameKey == -1) {
      if (anchorIsGroupField) {
        // Group field absent from the name dictionary — NO record carries it.
        // The correct result is ONE missing-key group covering every record,
        // which we can synthesize whenever the record total is known (top-
        // level array source). Multi-key grouping cannot be reconstructed
        // (the secondary key values of unvisited records are unknown) — stay
        // loud there rather than return silently wrong groups.
        final long recordCount = topLevelRecordCountOrUnknown(sourcePath);
        if (recordCount > 0) {
          if (groupFields.length == 1) {
            final Object2LongOpenHashMap<String> allMissing = emptyGroupMap();
            allMissing.addTo("m", recordCount);
            return allMissing;
          }
          requireDenseGroupField(sourcePath, groupFields[0], true, 0L);
        }
      }
      // Predicate anchor unresolvable → no record satisfies the leaf → empty
      // (same convention as the predicate-count kernels).
      return emptyGroupMap();
    }
    final long anchorPathNodeKey = resolveTargetPathNodeKey(sourcePath, cp.fieldNames[0]);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    @SuppressWarnings("unchecked")
    final Object2LongOpenHashMap<String>[] perThread = new Object2LongOpenHashMap[eff];

    final PageScanSchedule schedule = planPageScan(anchorNameKey, anchorPathNodeKey, totalPages, eff);
    final AtomicLong cursor = new AtomicLong();
    final int STRIDE = 8;
    final long scheduleSize = schedule.scheduleSize();
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final RoaringBitmap recordBuf = schedule.recordBufferOrNull(idx);
      final Object2LongOpenHashMap<String> local = new Object2LongOpenHashMap<>();
      local.defaultReturnValue(0L);
      final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
      final TypedGroupScratch typed = new TypedGroupScratch(cp.fieldNames.length);
      final StringBuilder keyBuf = new StringBuilder(48);
      // Columnar fast paths only apply to the single-key unpredicated shape: a
      // predicate needs per-record evaluation, and multi-key needs record-aligned
      // tuples that per-field PAX regions don't provide.
      final boolean regionEligible = groupIdxs.length == 1 && predicateOrNull == null;
      final RegionGroupScratch regionScratch = regionEligible ? new RegionGroupScratch() : null;
      while (true) {
        final long s = cursor.getAndAdd(STRIDE);
        if (s >= scheduleSize) break;
        final long e = Math.min(s + STRIDE, scheduleSize);
        for (long j = s; j < e; j++) {
          final long pk = schedule.pageAt(j);
          final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
          if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
          final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
          final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
          if (matches.length == 0) continue;
          if (recordBuf != null) recordBuf.add((int) pk);
          if (regionEligible
              && tryRegionGroupByPage(kv, matches.length, anchorPathNodeKey, anchorNameKey, local, regionScratch)) {
            continue;
          }
          for (final int slot : matches) {
            final long anchorObjectKey = base + slot;
            if (anchorPathNodeKey != -1L
                && kv.getObjectKeyPathNodeKeyFromSlot(slot, anchorObjectKey) != anchorPathNodeKey) {
              continue;
            }
            if (!rtx.moveTo(anchorObjectKey)) continue;
            if (!rtx.moveToParent()) continue;
            loadFieldsTypedGroups(rtx, cp, scratch, typed);
            if (!evalCompiled(cp, 0, scratch)) continue;
            keyBuf.setLength(0);
            encodeTypedGroupKey(typed, groupIdxs, groupFields, keyBuf);
            local.addTo(keyBuf.toString(), 1L);
          }
        }
      }
      perThread[idx] = local;
    });
    schedule.publish(anchorNameKey);

    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    long counted = 0L;
    for (final Object2LongOpenHashMap<String> m : perThread) {
      if (m == null) continue;
      final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
      while (it.hasNext()) {
        final Object2LongMap.Entry<String> e = it.next();
        merged.addTo(e.getKey(), e.getLongValue());
        counted += e.getLongValue();
      }
    }
    if (anchorIsGroupField && anchorPathNodeKey != -1L) {
      // Anchor-visibility fixup: records LACKING the group field were never
      // visited (the scan iterates the field's slots). For a SINGLE group key
      // over a top-level array source the unvisited remainder is exactly the
      // missing-key group — synthesize it instead of erroring out (the
      // interpreter groups those records under the empty key). Multi-key
      // grouping cannot be reconstructed this way — stay LOUD on a gap.
      final long recordCount = topLevelRecordCountOrUnknown(sourcePath);
      if (recordCount >= 0 && counted < recordCount) {
        if (groupFields.length == 1) {
          merged.addTo("m", recordCount - counted);
        } else {
          requireDenseGroupField(sourcePath, groupFields[0], true, counted);
        }
      }
    }
    return merged;
  }

  private static Object2LongOpenHashMap<String> emptyGroupMap() {
    final Object2LongOpenHashMap<String> m = new Object2LongOpenHashMap<>();
    m.defaultReturnValue(0L);
    return m;
  }

  /**
   * Loud guard for the anchor-visibility gap: an anchor-based scan never sees a
   * record that lacks the anchor field, while the generic pipeline groups such
   * records under the empty key. Only enforceable when the record total is
   * cheaply known (top-level array source) and path scoping is sound (otherwise
   * nested same-name fields could legitimately inflate the visited count).
   */
  private void requireDenseGroupField(final String[] sourcePath, final String groupField,
      final boolean pathScopingSound, final long visited) throws QueryException {
    if (!pathScopingSound) {
      return;
    }
    final long recordCount = topLevelRecordCountOrUnknown(sourcePath);
    if (recordCount < 0 || visited == recordCount) {
      return;
    }
    throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                             "Vectorized group-by requires group field '%s' on every record: %s of %s records carry it"
                                 + " (nameKey=%s)",
                             groupField,
                             visited,
                             recordCount,
                             resolveFieldKey(groupField));
  }

  /**
   * Record count of the loop source when it is the TOP-LEVEL ARRAY
   * ({@code sourcePath == ["[]"]}); -1 when unknown (nested or unrepresentable
   * sources — callers must then skip completeness checks).
   */
  private long topLevelRecordCountOrUnknown(final String[] sourcePath) {
    if (sourcePath == null || sourcePath.length != 1 || !"[]".equals(sourcePath[0])) {
      return -1L;
    }
    try {
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      rtx.moveToDocumentRoot();
      if (!rtx.moveToFirstChild()) {
        return 0L;
      }
      if (rtx.getKind() != NodeKind.ARRAY) {
        return -1L;
      }
      return rtx.getChildCount();
    } catch (final Exception e) {
      return -1L;
    }
  }

  /**
   * Append {@code extraFields} to a compiled predicate's field layout (skipping
   * fields the predicate already references) so one {@code loadFields} sweep
   * covers predicate and group fields together.
   */
  private CompiledPredicate compileWithExtraFields(final PredicateNode root, final String[] extraFields) {
    CompiledPredicate cp = compile(root);
    for (final String extra : extraFields) {
      cp = appendFieldIfMissing(cp, extra);
    }
    return cp;
  }

  /**
   * Clone of {@link #loadFields} that ADDITIONALLY fills a typed view of every
   * field. The legacy {@link EvalScratch} fills keep their exact historical
   * semantics (numbers truncate to long, null and complex values read as
   * missing) because the integer-only predicate evaluator depends on them; the
   * typed view is what the group-key encoder consumes.
   */
  private static void loadFieldsTypedGroups(final JsonNodeReadOnlyTrx rtx, final CompiledPredicate cp,
      final EvalScratch scratch, final TypedGroupScratch typed) {
    for (int i = 0; i < scratch.fieldKind.length; i++) {
      scratch.fieldKind[i] = 0;
      typed.kind[i] = TK_MISSING;
      typed.str[i] = null;
    }
    if (!rtx.moveToFirstChild()) return;
    final int[] wantKeys = cp.fieldNameKeys;
    do {
      final int nk = rtx.getNameKey();
      int fi = -1;
      for (int i = 0; i < wantKeys.length; i++) if (wantKeys[i] == nk) { fi = i; break; }
      if (fi < 0) continue;
      final NodeKind anchorKind = rtx.getKind();
      switch (anchorKind) {
        case OBJECT_NAMED_NUMBER -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoScratch(scratch, cp, fi, n);
            fillTypedNumber(typed, fi, n);
          }
          continue;
        }
        case OBJECT_NAMED_BOOLEAN -> {
          final boolean b = rtx.getBooleanValue();
          scratch.boolVals[fi] = b;
          scratch.fieldKind[fi] = FK_BOOL;
          typed.bool[fi] = b;
          typed.kind[fi] = TK_BOOL;
          continue;
        }
        case OBJECT_NAMED_STRING -> {
          final String s = rtx.getValue();
          scratch.strVals[fi] = s;
          scratch.fieldKind[fi] = FK_STR;
          typed.str[fi] = s;
          typed.kind[fi] = TK_STR;
          continue;
        }
        case OBJECT_NAMED_NULL -> {
          // Legacy scratch: null reads as missing (predicate ops never match).
          typed.kind[fi] = TK_NULL;
          continue;
        }
        default -> { /* OBJECT_KEY — descend */ }
      }
      if (!rtx.moveToFirstChild()) { continue; }
      final NodeKind kind = rtx.getKind();
      switch (kind) {
        case NUMBER_VALUE -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoScratch(scratch, cp, fi, n);
            fillTypedNumber(typed, fi, n);
          }
        }
        case BOOLEAN_VALUE -> {
          final boolean b = rtx.getBooleanValue();
          scratch.boolVals[fi] = b;
          scratch.fieldKind[fi] = FK_BOOL;
          typed.bool[fi] = b;
          typed.kind[fi] = TK_BOOL;
        }
        case STRING_VALUE -> {
          final String s = rtx.getValue();
          scratch.strVals[fi] = s;
          scratch.fieldKind[fi] = FK_STR;
          typed.str[fi] = s;
          typed.kind[fi] = TK_STR;
        }
        case NULL_VALUE -> typed.kind[fi] = TK_NULL;
        default -> typed.kind[fi] = TK_COMPLEX;
      }
      rtx.moveToParent();
    } while (rtx.moveToRightSibling());
  }

  /** Typed-number fill: integers stay longs, doubles stay doubles, decimals keep their literal. */
  private static void fillTypedNumber(final TypedGroupScratch typed, final int fi, final Number n) {
    if (n instanceof Double || n instanceof Float) {
      typed.dbl[fi] = n.doubleValue();
      typed.kind[fi] = TK_DBL;
    } else if (n instanceof java.math.BigDecimal || n instanceof java.math.BigInteger) {
      typed.str[fi] = n.toString();
      typed.kind[fi] = TK_DECSTR;
    } else {
      typed.lng[fi] = n.longValue();
      typed.kind[fi] = TK_LONG;
    }
  }

  /**
   * Encode the typed values of the group fields into an unambiguous composite
   * key: one tag char per part; longs/doubles are NUL-terminated (their digits
   * never contain NUL), strings and decimal literals are length-prefixed.
   */
  private static void encodeTypedGroupKey(final TypedGroupScratch typed, final int[] groupIdxs,
      final String[] groupFields, final StringBuilder out) {
    for (int g = 0; g < groupIdxs.length; g++) {
      final int fi = groupIdxs[g];
      switch (typed.kind[fi]) {
        case TK_MISSING -> out.append('m');
        case TK_NULL -> out.append('z');
        case TK_BOOL -> out.append('b').append(typed.bool[fi] ? '1' : '0');
        case TK_LONG -> out.append('l').append(typed.lng[fi]).append('\u0000');
        case TK_DBL -> out.append('d').append(typed.dbl[fi]).append('\u0000');
        case TK_DECSTR -> {
          final String s = typed.str[fi];
          out.append('D').append(s.length()).append(':').append(s);
        }
        case TK_STR -> {
          final String s = typed.str[fi];
          out.append('s').append(s.length()).append(':').append(s);
        }
        default -> throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                                            "Vectorized group-by: group field '%s' has a non-atomic (object/array) value",
                                            groupFields[g]);
      }
    }
  }

  /** Per-worker scratch for the single-key unpredicated columnar group-by fast paths. */
  private static final class RegionGroupScratch {
    final io.sirix.page.pax.BooleanRegion.Header boolHeader = new io.sirix.page.pax.BooleanRegion.Header();
    final it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap pageCounts =
        new it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap(16);
    final StringBuilder keyBuf = new StringBuilder(24);

    RegionGroupScratch() {
      pageCounts.defaultReturnValue(0L);
    }
  }

  /**
   * Columnar (PAX-region) page kernels for single-key unpredicated typed group-by:
   * numbers via {@link NumberRegion} (zone-map single-value shortcut, else a
   * decoded column sweep into a per-page distinct map), booleans via
   * {@link io.sirix.page.pax.BooleanRegion#countTrue} (branch-free popcount).
   * A page commits ONLY when the region's per-tag count equals the page's anchor
   * slot count — the same completeness oracle as the StringRegion paths: any
   * straggler (cross-page record, double value the long-only number region
   * skipped, null) falls back to the per-record typed slot walk for that page.
   *
   * @return {@code true} when the page was fully accounted via a region
   */
  private static boolean tryRegionGroupByPage(final KeyValueLeafPage kv, final int anchorSlotCount,
      final long targetPathNodeKey, final int anchorNameKey, final Object2LongOpenHashMap<String> counts,
      final RegionGroupScratch scratch) {
    // ---- numbers (long-typed values only by construction) ----
    final NumberRegion.Header nh = kv.getNumberRegionHeader();
    if (nh != null) {
      final int lookup =
          regionLookupTag(nh.tagKind == NumberRegion.TAG_KIND_PATH_NODE, targetPathNodeKey, anchorNameKey);
      final int tag = lookup == Integer.MIN_VALUE ? -1 : NumberRegion.lookupTag(nh, lookup);
      if (tag >= 0 && nh.tagCount[tag] == anchorSlotCount) {
        final int start = nh.tagStart[tag];
        final int n = nh.tagCount[tag];
        final StringBuilder kb = scratch.keyBuf;
        // Zone-map shortcut: a constant column (tagMin == tagMax) is ONE group.
        if (nh.tagMin != null && nh.tagMax != null && nh.tagMin[tag] == nh.tagMax[tag]) {
          kb.setLength(0);
          kb.append('l').append(nh.tagMin[tag]).append('\u0000');
          counts.addTo(kb.toString(), n);
          return true;
        }
        final byte[] payload = kv.getNumberRegionPayload();
        if (payload == null) return false;
        final it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap pc = scratch.pageCounts;
        pc.clear();
        for (int i = 0; i < n; i++) {
          pc.addTo(NumberRegion.decodeValueAt(payload, nh, start + i), 1L);
        }
        for (final var e : pc.long2LongEntrySet()) {
          kb.setLength(0);
          kb.append('l').append(e.getLongKey()).append('\u0000');
          counts.addTo(kb.toString(), e.getLongValue());
        }
        return true;
      }
    }
    // ---- booleans (bit-packed; popcount) ----
    final byte[] boolPayload = kv.getBooleanRegionPayload();
    if (boolPayload != null) {
      final io.sirix.page.pax.BooleanRegion.Header bh = scratch.boolHeader.parseInto(boolPayload);
      final int lookup = regionLookupTag(bh.tagKind == io.sirix.page.pax.BooleanRegion.TAG_KIND_PATH_NODE,
                                         targetPathNodeKey,
                                         anchorNameKey);
      final int tag =
          lookup == Integer.MIN_VALUE ? -1 : io.sirix.page.pax.BooleanRegion.lookupTag(bh, lookup);
      if (tag >= 0 && bh.tagCount[tag] == anchorSlotCount) {
        final int start = bh.tagStart[tag];
        final int n = bh.tagCount[tag];
        final int trues = io.sirix.page.pax.BooleanRegion.countTrue(boolPayload, bh, start, n);
        if (trues > 0) counts.addTo("b1", trues);
        if (n - trues > 0) counts.addTo("b0", n - trues);
        return true;
      }
    }
    return false;
  }

  /**
   * The shared tag-dispatch rule of every region fast path: unscoped queries
   * look up by nameKey; path-scoped queries may only use a PATH-tagged region
   * (a nameKey-tagged region cannot prove per-slot path membership).
   */
  private static int regionLookupTag(final boolean regionPathTagged, final long targetPathNodeKey,
      final int nameKey) {
    if (targetPathNodeKey == -1L) return nameKey;
    if (regionPathTagged && targetPathNodeKey > 0L && targetPathNodeKey <= (long) Integer.MAX_VALUE) {
      return (int) targetPathNodeKey;
    }
    return Integer.MIN_VALUE;
  }

  /** Decode the composite keys and emit {@code {out1: k1, ..., countName: n}} records. */
  private Sequence buildTypedGroupRecords(final Object2LongOpenHashMap<String> merged, final String[] outNames,
      final String countName) {
    final ArrayList<Item> out = new ArrayList<>(merged.size());
    final ObjectIterator<Object2LongMap.Entry<String>> it = merged.object2LongEntrySet().fastIterator();
    while (it.hasNext()) {
      final Object2LongMap.Entry<String> e = it.next();
      final String enc = e.getKey();
      final QNm[] names = new QNm[outNames.length + 1];
      final Sequence[] vals = new Sequence[outNames.length + 1];
      int pos = 0;
      for (int g = 0; g < outNames.length; g++) {
        names[g] = new QNm(outNames[g]);
        final char tag = enc.charAt(pos++);
        switch (tag) {
          case 'm', 'z' -> vals[g] = Null.INSTANCE;
          case 'b' -> vals[g] = enc.charAt(pos++) == '1' ? Bool.TRUE : Bool.FALSE;
          case 'l' -> {
            final int end = enc.indexOf('\u0000', pos);
            vals[g] = new Int64(Long.parseLong(enc.substring(pos, end)));
            pos = end + 1;
          }
          case 'd' -> {
            final int end = enc.indexOf('\u0000', pos);
            vals[g] = new Dbl(Double.parseDouble(enc.substring(pos, end)));
            pos = end + 1;
          }
          case 's', 'D' -> {
            final int colon = enc.indexOf(':', pos);
            final int len = Integer.parseInt(enc.substring(pos, colon));
            final String v = enc.substring(colon + 1, colon + 1 + len);
            pos = colon + 1 + len;
            vals[g] = tag == 's' ? new Str(v) : new Dec(v);
          }
          default -> throw new IllegalStateException("bad group-key tag " + tag);
        }
      }
      names[outNames.length] = new QNm(countName);
      vals[outNames.length] = new Int64(e.getLongValue());
      out.add(new ArrayObject(names, vals));
    }
    return new ItemSequence(out.toArray(new Item[0]));
  }

  /**
   * Walk every leaf page in parallel; per-record evaluate the compiled
   * predicate; on match, bump the per-group counter. The group field is
   * compiled into the same {@link CompiledPredicate} via an extra field slot
   * so the per-record load pass covers both predicate fields and the group
   * field in one sweep.
   */
  private Sequence parallelGenericPredicateGroupByCount(final String[] sourcePath, final PredicateNode predicate,
      final String groupField, final long[] statsOut) throws Exception {
    // Augment the predicate's field set with the group field so `loadFields`
    // picks it up in the same sweep over the record's children.
    final CompiledPredicate cp = compileWithExtraField(predicate, groupField);
    // Projection fast-path: if a covering projection index is installed and
    // the predicate tree is a pure conjunction of supported leaves over
    // columns the index carries — plus the group column is STRING_DICT —
    // route the groupBy-count to the zero-copy SIMD byte-scan, which
    // avoids the OBJECT_KEY traversal + per-record loadFields entirely.
    // Registry lookup is a single ConcurrentHashMap.get; the dispatch
    // cost on a miss is negligible against a millisecond-scale fallback.
    {
      final Sequence projected = tryProjectionIndexGroupByFastPath(sourcePath, cp, groupField);
      if (projected != null) return projected;
    }
    // The anchor is still the predicate's first field — the group field is
    // not necessarily selective, so we don't drive from it.
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) return new ItemSequence();
    final int groupFieldIdx = indexOfStr(cp.fieldNames, groupField);
    if (groupFieldIdx < 0 || cp.fieldNameKeys[groupFieldIdx] == -1) {
      // Group field unresolvable in the dictionary — rows can still pass the
      // predicate and must group under the missing key. Signal the caller to
      // redo with the typed kernel, which represents missing keys.
      return null;
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
    // Per-thread predicate-pass tally. Compared against the grouped total by the
    // caller: a passed row that contributed no group (non-string / null / missing
    // group value) means this string-only kernel was not authoritative.
    final long[] passedPerThread = new long[eff];
    // Compile once, share across workers. Key disambiguates by field layout.
    final String compileKey = compiledClassKey(predicate, cp) + "#gb:" + groupField;
    final BatchPredicate compiled = useBatch ? resolveCompiledPredicate(compileKey, cp) : null;
    // Page-skip index: reuse bitmap from prior scans anchored on the same
    // nameKey, or build it as a side-effect of this scan.
    final PageScanSchedule schedule = planPageScan(anchorNameKey, anchorPathNodeKey, totalPages, eff);
    // Work-stealing via shared atomic cursor — see parallelGenericPredicateCount
    // for rationale. Stride = 8 amortises CAS over enough work.
    final AtomicLong cursor = new AtomicLong();
    final int STRIDE = 8;
    final long scheduleSize = schedule.scheduleSize();
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final RoaringBitmap recordBuf = schedule.recordBufferOrNull(idx);
      // Primitive-long accumulator — avoids the per-group `long[] { 0L }`
      // box-on-insert that the previous HashMap<String, long[]> carried, and
      // uses addTo() so the hot loop does one hashmap op per match instead
      // of the get + (optional put) pair.
      final Object2LongOpenHashMap<String> local = new Object2LongOpenHashMap<>();
      local.defaultReturnValue(0L);
      long passed = 0L;
      if (useBatch) {
        final EvalBatch batch = new EvalBatch(cp.fieldNames.length, cp.ops.length, Constants.INP_REFERENCE_COUNT);
        final long[] rootOut = new long[batch.bitmapStride];
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= scheduleSize) break;
          final long e = Math.min(s + STRIDE, scheduleSize);
          for (long j = s; j < e; j++) {
            final long pk = schedule.pageAt(j);
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            if (recordBuf != null) recordBuf.add((int) pk);
            collectColumns(kv, base, matches, cp, anchorPathNodeKey, batch, rtx);
            if (batch.size == 0) continue;
            final long[] bitmap;
            if (compiled != null && !batch.hasDecimalRows) {
              compiled.evaluate(batch, rootOut);
              bitmap = rootOut;
            } else {
              // Decimal rows need the exact decimal arms — only the
              // interpreter models them (the generated kernels are long/
              // double only).
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
                passed++;
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
          if (s >= scheduleSize) break;
          final long e = Math.min(s + STRIDE, scheduleSize);
          for (long j = s; j < e; j++) {
            final long pk = schedule.pageAt(j);
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            if (recordBuf != null) recordBuf.add((int) pk);
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
            passed++;
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
      passedPerThread[idx] = passed;
    });
    schedule.publish(anchorNameKey);

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

    // Build Brackit output: flat sequence of {"groupField": key, "count": n} records.
    final ArrayList<Item> out = new ArrayList<>(merged.size());
    final QNm keyQnm = new QNm(groupField);
    final QNm cntQnm = new QNm("count");
    long counted = 0L;
    final ObjectIterator<Object2LongMap.Entry<String>> mergedIt =
        merged.object2LongEntrySet().fastIterator();
    while (mergedIt.hasNext()) {
      final Object2LongMap.Entry<String> e = mergedIt.next();
      counted += e.getLongValue();
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { new Str(e.getKey()), new Int64(e.getLongValue()) };
      out.add(new ArrayObject(keys, vals));
    }
    if (statsOut != null && statsOut.length >= 2) {
      long totalPassed = 0L;
      for (final long p : passedPerThread) totalPassed += p;
      statsOut[0] = totalPassed;
      statsOut[1] = counted;
    }
    return new ItemSequence(out.toArray(new Item[0]));
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
          // avg/min/max over the empty sequence ARE the empty sequence — the
          // interpreter serializes nothing, not 0.
          case "avg", "min", "max" -> new ItemSequence();
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
      Sequence fresh = null;
      if (!"count".equals(func)) {
        final long[] projected = tryProjectionAggregate(sourcePath, field, predicate);
        if (projected != null) {
          final long count = projected[0];
          final long sum = projected[1];
          fresh = switch (func) {
            case "sum" -> new Int64(sum);
            case "avg" -> count == 0 ? new ItemSequence() : new Int64(sum).div(new Int64(count));
            case "min" -> count == 0 ? new ItemSequence() : new Int64(projected[2]);
            case "max" -> count == 0 ? new ItemSequence() : new Int64(projected[3]);
            default -> null;
          };
        }
      }
      if (fresh == null) {
        fresh = parallelGenericPredicateAggregate(sourcePath, predicate, func, field);
      }
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
    final CompiledPredicate cp = isCount && aggField == null
        ? compile(predicate)
        : compileWithExtraField(predicate, aggField);
    if (cp.fieldNames.length == 0) return null;
    // Projection-index fast-path for aggregate-count — ONLY when no aggregate
    // field is involved: count(... return $u.f) counts records where f is
    // PRESENT among the matches, which the plain conjunctive count cannot
    // express. Aggregates other than count need the aggregate column in the
    // projection too; the generic path below handles them.
    if (isCount && aggField == null) {
      final Long projected = tryProjectionIndexFastPath(sourcePath, cp);
      if (projected != null) {
        return new Int64(projected);
      }
    }
    final int anchorNameKey = cp.fieldNameKeys[0];
    if (anchorNameKey == -1) {
      return switch (func) {
        case "count", "sum" -> new Int64(0L);
        case "avg", "min", "max" -> new ItemSequence();
        default -> null;
      };
    }
    final int aggFieldIdx = aggField == null ? -1 : indexOfStr(cp.fieldNames, aggField);
    if (aggField != null && (aggFieldIdx < 0 || cp.fieldNameKeys[aggFieldIdx] == -1)) {
      // Aggregate field not in this document — zero rows would contribute.
      return switch (func) {
        case "count", "sum" -> new Int64(0L);
        case "avg", "min", "max" -> new ItemSequence();
        default -> null;
      };
    }
    // Resolve the fully-qualified path for the anchor field. See parallelAggregate.
    final long anchorPathNodeKey = resolveTargetPathNodeKey(sourcePath, cp.fieldNames[0]);

    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    // [count, sum, min, max] per thread — exact longs; doubles fold separately.
    final long[][] perThread = new long[eff][4];
    final double[][] perThreadDbl = new double[eff][4];
    for (int i = 0; i < eff; i++) {
      perThread[i][2] = Long.MAX_VALUE;
      perThread[i][3] = Long.MIN_VALUE;
      perThreadDbl[i][2] = Double.MAX_VALUE;
      perThreadDbl[i][3] = -Double.MAX_VALUE;
    }
    // Benign-race flag: any worker that sees a non-numeric aggregate value
    // (string/boolean/null/complex) sets it — used for the loud
    // pure-non-numeric guard below.
    final boolean[] sawNonNumeric = new boolean[1];
    // Page-skip index — reuse bitmap from prior scans anchored on the
    // same nameKey, or build it as a side-effect of this scan. The
    // aggregate path uses a per-worker contiguous range over the
    // schedule (simpler than work-stealing and sufficient for the
    // balanced case).
    final PageScanSchedule schedule = planPageScan(anchorNameKey, anchorPathNodeKey, totalPages, eff);
    final long scheduleSize = schedule.scheduleSize();
    final long ppt = (scheduleSize + eff - 1) / eff;

    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, scheduleSize);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
      final TypedGroupScratch typed = new TypedGroupScratch(cp.fieldNames.length);
      final long[] acc = perThread[idx];
      final double[] dacc = perThreadDbl[idx];
      final RoaringBitmap recordBuf = schedule.recordBufferOrNull(idx);
      for (long j = s; j < e; j++) {
        final long pk = schedule.pageAt(j);
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
        if (matches.length == 0) continue;
        if (recordBuf != null) recordBuf.add((int) pk);
        for (final int slot : matches) {
          final long anchorObjectKey = base + slot;
          if (anchorPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, anchorObjectKey) != anchorPathNodeKey) {
            continue;
          }
          if (!rtx.moveTo(anchorObjectKey)) continue;
          if (!rtx.moveToParent()) continue;
          loadFieldsTypedGroups(rtx, cp, scratch, typed);
          if (!evalCompiled(cp, 0, scratch)) continue;
          if (isCount) {
            // count(... return $u.f) counts non-empty derefs among the
            // matches: a record missing f contributes ZERO items. Any
            // present value (number, string, boolean, null, complex)
            // derefs to exactly one item.
            if (aggFieldIdx < 0 || typed.kind[aggFieldIdx] != TK_MISSING) {
              acc[0]++;
            }
            continue;
          }
          if (typed.kind[aggFieldIdx] == TK_STR || typed.kind[aggFieldIdx] == TK_BOOL
              || typed.kind[aggFieldIdx] == TK_NULL || typed.kind[aggFieldIdx] == TK_COMPLEX) {
            sawNonNumeric[0] = true;
          }
          // Numeric aggregate — fold by the agg value's REAL type. Longs use the
          // exact long accumulator; doubles/decimals fold into the double
          // accumulator (the legacy long-only fold TRUNCATED them — measured 14%
          // short on a half-double column).
          switch (typed.kind[aggFieldIdx]) {
            case TK_LONG -> {
              final long v = typed.lng[aggFieldIdx];
              acc[0]++;
              acc[1] += v;
              if (v < acc[2]) acc[2] = v;
              if (v > acc[3]) acc[3] = v;
            }
            case TK_DBL, TK_DECSTR -> {
              final double v = typed.kind[aggFieldIdx] == TK_DBL
                  ? typed.dbl[aggFieldIdx]
                  : Double.parseDouble(typed.str[aggFieldIdx]);
              dacc[0]++;
              dacc[1] += v;
              if (v < dacc[2]) dacc[2] = v;
              if (v > dacc[3]) dacc[3] = v;
            }
            default -> { /* non-numeric value on this row — skip (legacy semantics) */ }
          }
        }
      }
    });
    schedule.publish(anchorNameKey);

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
    double dCount = 0d;
    double dSum = 0d;
    double dMin = Double.MAX_VALUE;
    double dMax = -Double.MAX_VALUE;
    for (final double[] a : perThreadDbl) {
      dCount += a[0];
      dSum += a[1];
      if (a[0] > 0) {
        if (a[2] < dMin) dMin = a[2];
        if (a[3] > dMax) dMax = a[3];
      }
    }
    if (!isCount && count == 0 && dCount == 0 && sawNonNumeric[0]) {
      // Matching rows carried the aggregate field, but never as a number —
      // the interpreter applies string/error semantics here. Fail loudly.
      throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                               "Vectorized %s over field '%s': matching records hold no numeric values — "
                                   + "string/boolean/null aggregation is not supported by the vectorized executor",
                               func,
                               aggField);
    }
    if (dCount > 0 && !isCount) {
      // Mixed or pure-double column — combine both accumulators as doubles and
      // emit double results, like the generic pipeline's numeric promotion.
      final long totalCount = count + (long) dCount;
      final double totalSum = (double) sum + dSum;
      final double totalMin = Math.min(count > 0 ? (double) min : Double.MAX_VALUE, dMin);
      final double totalMax = Math.max(count > 0 ? (double) max : -Double.MAX_VALUE, dMax);
      return switch (func) {
        case "sum"   -> new Dbl(totalSum);
        case "avg"   -> new Dbl(totalSum / totalCount);
        case "min"   -> new Dbl(totalMin);
        case "max"   -> new Dbl(totalMax);
        default      -> null;
      };
    }
    return switch (func) {
      case "count" -> new Int64(count);
      case "sum"   -> new Int64(sum);
      // xs:decimal division via brackit — digit-exact with the generic pipeline.
      case "avg"   -> count == 0 ? new ItemSequence() : new Int64(sum).div(new Int64(count));
      case "min"   -> count == 0 ? new ItemSequence() : new Int64(min);
      case "max"   -> count == 0 ? new ItemSequence() : new Int64(max);
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
    return appendFieldIfMissing(compile(root), extraField);
  }

  /** Append one field to a compiled predicate's layout unless already referenced. */
  private CompiledPredicate appendFieldIfMissing(final CompiledPredicate base, final String extraField) {
    for (int i = 0; i < base.fieldNames.length; i++) {
      if (base.fieldNames[i].equals(extraField)) return base;
    }
    final int n = base.fieldNames.length + 1;
    final String[] newNames = new String[n];
    final int[] newKeys = new int[n];
    final boolean[] newUsed = new boolean[n];
    System.arraycopy(base.fieldNames, 0, newNames, 0, base.fieldNames.length);
    System.arraycopy(base.fieldNameKeys, 0, newKeys, 0, base.fieldNameKeys.length);
    System.arraycopy(base.fieldUsedInPredicate, 0, newUsed, 0, base.fieldUsedInPredicate.length);
    newNames[n - 1] = extraField;
    newKeys[n - 1] = resolveFieldKey(extraField);
    newUsed[n - 1] = false;  // group-key / aggregate fields are not predicate operands
    base.fieldNames = newNames;
    base.fieldNameKeys = newKeys;
    base.fieldUsedInPredicate = newUsed;
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
      case PredicateNode.FpCmp fc -> sb.append('D').append(fc.field()).append(':').append(fc.op()).append(':')
                                       .append(Double.doubleToRawLongBits(fc.value())).append(';');
      case PredicateNode.DecCmp dc -> sb.append('C').append(dc.field()).append(':').append(dc.op()).append(':')
                                        .append(dc.value().toPlainString()).append(';');
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
    /** Floating-point comparison — see {@link PredicateNode.FpCmp}'s semantics contract. */
    static final byte OP_FP_CMP = 8;
    /** Exact decimal comparison — see {@link PredicateNode.DecCmp}'s semantics contract. */
    static final byte OP_DEC_CMP = 9;

    /** Long-row arm of a DEC_CMP node: provably false for every long. */
    static final byte DEC_ARM_FALSE = 0;
    /** Long-row arm: provably true for every long. */
    static final byte DEC_ARM_TRUE = 1;
    /** Long-row arm: {@code L >= decLongLit}. */
    static final byte DEC_ARM_GE = 2;
    /** Long-row arm: {@code L <= decLongLit}. */
    static final byte DEC_ARM_LE = 3;
    /** Long-row arm: {@code L == decLongLit}. */
    static final byte DEC_ARM_EQ = 4;

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
    /** Per-node double literal (for FpCmp), 0.0 if N/A. */
    double[] dblLit;
    /** Per-node EXACT decimal literal (for DecCmp), null if N/A. */
    java.math.BigDecimal[] decLit;
    /**
     * Per-node precomputed long-row evaluation of a DecCmp: the exact decimal
     * comparison {@code L <op> c} collapses to a pure long-space predicate
     * ({@link #DEC_ARM_GE}/{@link #DEC_ARM_LE}/{@link #DEC_ARM_EQ} against
     * {@link #decLongLit}, or a constant) — computed once at compile time so
     * the per-row hot path never touches BigDecimal for integer rows.
     */
    byte[] decLongArm;
    long[] decLongLit;
    /** Per-node {@code decLit.doubleValue()} — the interpreter's promotion for DOUBLE rows. */
    double[] decDblImage;
    /** Per-node string literal index (for StrEq), -1 if N/A. */
    int[] strIdx;

    /** Distinct field names referenced by the predicate, in first-seen order. */
    String[] fieldNames;
    /** Resolved nameKeys in the same order as {@link #fieldNames}; -1 = not found in document. */
    int[] fieldNameKeys;
    /**
     * Per-field flag: the field is referenced by a PREDICATE leaf (as opposed
     * to being appended for group-key / aggregate loading). Loaders use it to
     * fail LOUDLY when a predicate-referenced field carries a number the
     * vectorized evaluator cannot compare exactly (BigDecimal / BigInteger /
     * float) — silently truncating it would change results.
     */
    boolean[] fieldUsedInPredicate;
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

    // ANCHOR SOUNDNESS: the scan iterates fieldNames[0]'s slots, so records
    // missing that field are never visited. That is only sound when the
    // predicate provably excludes them (comparison/EBV over a missing field is
    // false in XQuery). Reorder so a sound anchor sits at index 0; if none
    // exists (e.g. `a > 1 or b > 1` from a pre-guard brackit), FAIL LOUDLY —
    // an anchor-based scan would silently lose matches on sparse data, and the
    // dispatch has no generic pipeline left to fall back to at evaluate time.
    if (!fieldSet.isEmpty()) {
      final String soundAnchor = root.findSoundAnchorField();
      if (soundAnchor == null) {
        throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                                 "Vectorized scan cannot anchor predicate '%s': it may match records missing every "
                                     + "referenced field. The query detection should not have claimed it — "
                                     + "update brackit or rewrite the predicate.",
                                 predicateCacheKey(root));
      }
      if (!soundAnchor.equals(fieldSet.iterator().next())) {
        final LinkedHashSet<String> reordered = new LinkedHashSet<>();
        reordered.add(soundAnchor);
        reordered.addAll(fieldSet);
        fieldSet.clear();
        fieldSet.addAll(reordered);
      }
    }

    final CompiledPredicate cp = new CompiledPredicate();
    cp.fieldNames = fieldSet.toArray(new String[0]);
    cp.fieldNameKeys = new int[cp.fieldNames.length];
    for (int i = 0; i < cp.fieldNames.length; i++) {
      cp.fieldNameKeys[i] = resolveFieldKey(cp.fieldNames[i]);
    }
    cp.fieldUsedInPredicate = new boolean[cp.fieldNames.length];
    java.util.Arrays.fill(cp.fieldUsedInPredicate, true);  // every collected field comes from a leaf
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
    final ArrayList<Double> dblLit = new ArrayList<>();
    final ArrayList<java.math.BigDecimal> decLit = new ArrayList<>();
    final ArrayList<Integer> strIdx = new ArrayList<>();
    flatten(root, cp.fieldNames, cp.strLiterals, ops, children, childStart, childCount, fieldIdx, cmpOp, longLit,
            dblLit, decLit, strIdx);

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
    cp.dblLit = new double[dblLit.size()];
    for (int i = 0; i < dblLit.size(); i++) cp.dblLit[i] = dblLit.get(i);
    cp.decLit = new java.math.BigDecimal[decLit.size()];
    cp.decLongArm = new byte[decLit.size()];
    cp.decLongLit = new long[decLit.size()];
    cp.decDblImage = new double[decLit.size()];
    for (int i = 0; i < decLit.size(); i++) {
      final java.math.BigDecimal c = decLit.get(i);
      cp.decLit[i] = c;
      if (c != null) {
        cp.decDblImage[i] = c.doubleValue();
        precomputeDecLongArm(cp, i, c);
      }
    }
    cp.strIdx = new int[strIdx.size()];
    for (int i = 0; i < strIdx.size(); i++) cp.strIdx[i] = strIdx.get(i);
    return cp;
  }

  /**
   * Precompute the long-row arm of a DEC_CMP node: the EXACT decimal
   * comparison {@code L <op> c} over a long {@code L} reduces to a long-space
   * predicate via floor/ceil of {@code c} (clamped at the long range):
   * {@code L > c ⟺ L >= floor(c)+1}, {@code L >= c ⟺ L >= ceil(c)},
   * {@code L < c ⟺ L <= ceil(c)-1}, {@code L <= c ⟺ L <= floor(c)},
   * {@code L == c ⟺ L == c} only for integral in-range {@code c} (DecCmp is
   * normalized to NumCmp there at detection — defensive handling stays).
   */
  private static void precomputeDecLongArm(final CompiledPredicate cp, final int nodeIdx,
      final java.math.BigDecimal c) {
    final java.math.BigDecimal floor = c.setScale(0, java.math.RoundingMode.FLOOR);
    final java.math.BigDecimal ceil = c.setScale(0, java.math.RoundingMode.CEILING);
    switch (cp.cmpOp[nodeIdx]) {
      case OP_GT -> setDecArmGe(cp, nodeIdx, floor.add(java.math.BigDecimal.ONE));
      case OP_GE -> setDecArmGe(cp, nodeIdx, ceil);
      case OP_LT -> setDecArmLe(cp, nodeIdx, ceil.subtract(java.math.BigDecimal.ONE));
      case OP_LE -> setDecArmLe(cp, nodeIdx, floor);
      case OP_EQ -> {
        if (floor.compareTo(ceil) == 0 && inLongRange(floor)) {
          cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_EQ;
          cp.decLongLit[nodeIdx] = floor.longValueExact();
        } else {
          cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_FALSE;
        }
      }
      default -> cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_FALSE;
    }
  }

  private static final java.math.BigDecimal BD_LONG_MIN = java.math.BigDecimal.valueOf(Long.MIN_VALUE);
  private static final java.math.BigDecimal BD_LONG_MAX = java.math.BigDecimal.valueOf(Long.MAX_VALUE);

  private static boolean inLongRange(final java.math.BigDecimal integral) {
    return integral.compareTo(BD_LONG_MIN) >= 0 && integral.compareTo(BD_LONG_MAX) <= 0;
  }

  /** {@code L >= threshold} with clamping: below long range → always true; above → always false. */
  private static void setDecArmGe(final CompiledPredicate cp, final int nodeIdx,
      final java.math.BigDecimal threshold) {
    if (threshold.compareTo(BD_LONG_MIN) <= 0) {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_TRUE;
    } else if (threshold.compareTo(BD_LONG_MAX) > 0) {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_FALSE;
    } else {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_GE;
      cp.decLongLit[nodeIdx] = threshold.longValueExact();
    }
  }

  /** {@code L <= threshold} with clamping: above long range → always true; below → always false. */
  private static void setDecArmLe(final CompiledPredicate cp, final int nodeIdx,
      final java.math.BigDecimal threshold) {
    if (threshold.compareTo(BD_LONG_MAX) >= 0) {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_TRUE;
    } else if (threshold.compareTo(BD_LONG_MIN) < 0) {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_FALSE;
    } else {
      cp.decLongArm[nodeIdx] = CompiledPredicate.DEC_ARM_LE;
      cp.decLongLit[nodeIdx] = threshold.longValueExact();
    }
  }

  /** Evaluate a DEC_CMP node's precomputed long-row arm. */
  private static boolean evalDecLongArm(final CompiledPredicate cp, final int nodeIdx, final long v) {
    return switch (cp.decLongArm[nodeIdx]) {
      case CompiledPredicate.DEC_ARM_TRUE -> true;
      case CompiledPredicate.DEC_ARM_GE -> v >= cp.decLongLit[nodeIdx];
      case CompiledPredicate.DEC_ARM_LE -> v <= cp.decLongLit[nodeIdx];
      case CompiledPredicate.DEC_ARM_EQ -> v == cp.decLongLit[nodeIdx];
      default -> false;
    };
  }

  /** Exact decimal-row evaluation of a DEC_CMP node. */
  private static boolean evalDecOnDecimal(final CompiledPredicate cp, final int nodeIdx,
      final java.math.BigDecimal v) {
    final int c = v.compareTo(cp.decLit[nodeIdx]);
    return switch (cp.cmpOp[nodeIdx]) {
      case OP_GT -> c > 0;
      case OP_LT -> c < 0;
      case OP_GE -> c >= 0;
      case OP_LE -> c <= 0;
      case OP_EQ -> c == 0;
      default -> false;
    };
  }

  private static void collectLiterals(final PredicateNode n, final LinkedHashSet<String> fields,
      final LinkedHashSet<String> strs) {
    switch (n) {
      case PredicateNode.NumCmp nc -> fields.add(nc.field());
      case PredicateNode.FpCmp fc -> fields.add(fc.field());
      case PredicateNode.DecCmp dc -> fields.add(dc.field());
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
      final ArrayList<Long> longLit, final ArrayList<Double> dblLit,
      final ArrayList<java.math.BigDecimal> decLit, final ArrayList<Integer> strIdx) {
    final int myIdx = ops.size();
    // Reserve slot.
    ops.add((byte) 0);
    childStart.add(0);
    childCount.add(0);
    fieldIdx.add(-1);
    cmpOp.add(0);
    longLit.add(0L);
    dblLit.add(0.0d);
    decLit.add(null);
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
      case PredicateNode.FpCmp fc -> {
        ops.set(myIdx, CompiledPredicate.OP_FP_CMP);
        fieldIdx.set(myIdx, indexOf(fieldNames, fc.field()));
        cmpOp.set(myIdx, encodeOp(fc.op()));
        dblLit.set(myIdx, fc.value());
      }
      case PredicateNode.DecCmp dc -> {
        ops.set(myIdx, CompiledPredicate.OP_DEC_CMP);
        fieldIdx.set(myIdx, indexOf(fieldNames, dc.field()));
        cmpOp.set(myIdx, encodeOp(dc.op()));
        decLit.set(myIdx, dc.value());
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
                                     cmpOp, longLit, dblLit, decLit, strIdx);
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
                                          childCount, fieldIdx, cmpOp, longLit, dblLit, decLit, strIdx));
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
                                          childCount, fieldIdx, cmpOp, longLit, dblLit, decLit, strIdx));
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
    double[] dblVals;
    java.math.BigDecimal[] decVals;
    boolean[] boolVals;
    String[] strVals;
    byte[] fieldKind;  // 0 = missing, 1 = long num, 2 = bool, 3 = str, 4 = double num, 5 = decimal
    /** Work stack for iterative AND/OR short-circuit evaluation. */
    int[] stackNode;
    boolean[] stackRes;

    EvalScratch(final int nFields, final int nNodes) {
      longVals = new long[nFields];
      dblVals = new double[nFields];
      decVals = new java.math.BigDecimal[nFields];
      boolVals = new boolean[nFields];
      strVals = new String[nFields];
      fieldKind = new byte[nFields];
      stackNode = new int[Math.max(16, nNodes)];
      stackRes = new boolean[Math.max(16, nNodes)];
    }
  }

  /** Field-kind codes shared by {@link EvalScratch#fieldKind} and {@link EvalBatch#presentKind}. */
  private static final byte FK_MISSING = 0;
  private static final byte FK_LONG = 1;
  private static final byte FK_BOOL = 2;
  private static final byte FK_STR = 3;
  private static final byte FK_DOUBLE = 4;
  private static final byte FK_DECIMAL = 5;

  /**
   * Classify a document number for predicate evaluation: {@link #FK_LONG} for
   * exact integers, {@link #FK_DOUBLE} for doubles, {@link #FK_DECIMAL} for
   * BigDecimal/BigInteger (kept EXACT — the interpreter compares them in
   * decimal space; the historical {@code Number#longValue()} coercion
   * silently truncated them), and a LOUD failure for Float when the field is
   * referenced by a predicate leaf. For fields NOT used by the predicate
   * (appended group/aggregate fields) unsupported kinds stay out of the
   * scratch ({@link #FK_MISSING}) — the typed view carries them for
   * grouping/aggregation.
   */
  private static byte classifyNumberForPredicate(final Number n, final boolean usedInPredicate,
      final String fieldName) {
    if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
      return FK_LONG;
    }
    if (n instanceof Double) {
      return FK_DOUBLE;
    }
    if (n instanceof java.math.BigDecimal || n instanceof java.math.BigInteger) {
      // Exact decimal representation — jn:store ingests fractional JSON
      // numbers as BigDecimal, so this is a COMMON kind, not an exotic one.
      return FK_DECIMAL;
    }
    if (!usedInPredicate) {
      return FK_MISSING;
    }
    // Float is the remaining oddball: the interpreter compares xs:float
    // operands in FLOAT space (Float.compare), which double-space evaluation
    // cannot reproduce ((double) 0.1f != 0.1d). JSON ingestion has not
    // produced floats since the alpha13 narrowing removal — fail loudly.
    throw new IllegalStateException(
        "Vectorized predicate over field '" + fieldName + "' hit a " + n.getClass().getSimpleName()
            + " value (" + n + ") that cannot be compared exactly in the vectorized representation — "
            + "failing loudly instead of silently truncating. Rewrite the query or disable vectorization.");
  }

  /** Exact decimal of a document number classified {@link #FK_DECIMAL}. */
  private static java.math.BigDecimal decimalOfNumber(final Number n) {
    if (n instanceof java.math.BigDecimal bd) {
      return bd;
    }
    return new java.math.BigDecimal((java.math.BigInteger) n);
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
  /**
   * If a covering projection index is installed in
   * {@link ProjectionIndexRegistry} for this {@code (session, sourcePath)}
   * and the predicate is a conjunctive tree over supported leaf shapes
   * (NUM_CMP, STR_EQ, BOOL_REF), returns the count produced by
   * {@link ProjectionIndexByteScan#conjunctiveCount}. Returns {@code null}
   * otherwise, triggering the generic predicate path below.
   *
   * <p>Supported conjunctive shapes: a single NUM_CMP / STR_EQ / BOOL_REF
   * leaf, or OP_AND whose direct children are all such leaves. Nested
   * ANDs, ORs, NOTs, or comparisons against literals that aren't integer /
   * UTF-8 string are intentionally not matched — they fall back rather
   * than silently mis-evaluate.
   */
  private Long tryProjectionIndexFastPath(final String[] sourcePath, final CompiledPredicate cp) {
    // Registry key is resolved once at executor construction — no
    // per-query toString() or try/catch on the hot path.
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) {
      if (PROJ_DIAG) System.err.println("[proj] null resourceKey");
      return null;
    }
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) {
      if (PROJ_DIAG) System.err.println("[proj] no handle for key=" + resourceKey + " path=" + java.util.Arrays.toString(sourcePath));
      return null;
    }
    if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) {
      if (PROJ_DIAG) System.err.println("[proj] not covered: fields=" + java.util.Arrays.toString(cp.fieldNames));
      return null;
    }
    final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
    if (extracted == null) {
      if (PROJ_DIAG) System.err.println("[proj] unsupported shape");
      return null;
    }
    // iter#07 range fusion: collapse same-column (GT|GE, LT|LE) pairs
    // into one BETWEEN_* predicate so the evaluator only loads the
    // column once. Defaults on; disable with
    // -Dsirix.projection.rangeFusion=false.
    final ProjectionIndexScan.ColumnPredicate[] preds = fuseRangePredicates(extracted);
    if (preds.length == 0) return ProjectionIndexByteScan.countRows(handle.leafPayloads());
    return parallelConjunctiveCount(handle.leafPayloads(), preds);
  }

  /**
   * Parallel wrapper around {@link ProjectionIndexByteScan#conjunctiveCount}.
   * At 100M records the projection index holds ~100K leaves (~2 GB of
   * {@code byte[]}s); a single-threaded iteration spends most of its
   * time in per-leaf overhead (header decode + zone-map probe + mask
   * reset + SIMD evaluate) rather than payload bandwidth. Chunking the
   * leaf list across the executor's worker pool turns this into a
   * parallel reduction — essentially free given we already have the
   * threads, and roughly {@code threads}× throughput on scans where
   * per-leaf work dominates.
   *
   * <p>The chunks are materialised as {@link java.util.List} sub-views
   * (no copy). Each worker gets its own per-chunk scratch (long[],
   * mask buffers) — the underlying {@code countLeaf} is already
   * zero-alloc per leaf so the only allocation is the per-worker
   * scratch, amortised over {@code leafCount / threads} leaves.
   */
  private long parallelConjunctiveCount(final java.util.List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] preds) {
    final int leafCount = leafPayloads.size();
    if (leafCount == 0) return 0L;
    // For very small leaf counts (e.g. small datasets) the fan-out cost
    // dominates; fall back to single-threaded scan.
    if (leafCount < 64) {
      return ProjectionIndexByteScan.conjunctiveCount(leafPayloads, preds);
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    final long[] perThread = new long[eff];
    final int chunkSize = (leafCount + eff - 1) / eff;

    try {
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        // subList is a view — no copy. The underlying List is immutable
        // (installed once, read many), so concurrent subList iteration is
        // safe.
        perThread[idx] = ProjectionIndexByteScan.conjunctiveCount(
            leafPayloads.subList(from, to), preds);
      });
    } catch (final Exception e) {
      throw new RuntimeException("parallel projection conjunctiveCount failed", e);
    }

    long total = 0L;
    for (final long c : perThread) total += c;
    return total;
  }

  /**
   * iter#10 feature flag: route the group-by accumulator through a dense
   * {@code long[N]} indexed by canonical-dict position instead of an
   * {@code Object2LongOpenHashMap<String>} when the group column's
   * cardinality is bounded.
   *
   * <h2>Why it's <em>not</em> the default</h2>
   *
   * On the iter#09 post-commit JVM (C2 + {@code -XX:-UseJVMCICompiler} +
   * AppCDS + install-time JIT pre-warm of
   * {@code conjunctiveCountByGroup}), the hashmap path under C2 is
   * already near-optimal: pre-warm + stable type profile collapse the
   * theoretical {@code Object2LongOpenHashMap.addTo + String.equals +
   * String.hashCode} cost (iter#09 measured ~11 % of cold CPU) to a
   * ~7-8 % steady-state frame.
   *
   * <p>The dense path trades that 7-8 % for a per-leaf {@code dictId → canonId}
   * remap (8×8 byte-compares per bench leaf × 97,657 leaves), which
   * consumes ~7-8 % of CPU on its own. Net-even CPU-wise. And the
   * dense-path method requires its own JIT tier-up; even with
   * install-time pre-warm the per-call cost is marginally higher than
   * the fully-intrinsified hashmap path.
   *
   * <p>Measurement from 16-run 8×8 A/B (iter#10 validation):
   * <ul>
   *   <li>dense=true median 2.32 s; hashmap=false median 2.07 s
   *       (dense +12 % slower);</li>
   *   <li>{@code groupByDept} min: dense 0.46 ms vs hashmap 0.42 ms;</li>
   *   <li>CPU profile: dense path eliminates the ~180 samples of
   *       addTo/String.equals but absorbs the same ~180 samples in
   *       {@code conjunctiveCountByGroupDense} self-time.</li>
   * </ul>
   *
   * <p><b>Kept as opt-in</b> because future workloads may benefit:
   * workloads with much higher cardinality on canonical dict (where the
   * hashmap hit rate falls and {@code String.equals} collision chains
   * grow); runtimes where {@code String.equals} is not aggressively
   * intrinsified (e.g. GraalVM native-image with graal#13377 unresolved,
   * where all {@link java.lang.foreign.MemorySegment} paths are slower
   * than their byte[]-based dense equivalents); or future rewrites of
   * the dense path that eliminate the per-leaf remap via a cached
   * {@code leafRemaps: int[][]} on the Handle (the dict is stable per
   * leaf, so the remap need only be computed once).
   *
   * <p>Enable with {@code -Dsirix.projection.denseGroupBy=true}.
   */
  private static final boolean DENSE_GROUPBY_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.denseGroupBy", "false"));

  /**
   * Probe-leaf cap for canonical-dict cardinality estimation. Default 16
   * leaves is enough for the bench's 8-way uniform {dept, city}
   * distribution (by row 100 on the first 1024-row leaf each distinct
   * value has typically already appeared); larger installs tolerate a
   * bigger probe without re-scanning the full 97K-leaf index.
   *
   * <p>Tune via {@code -Dsirix.projection.denseGroupBy.probeLeaves=N}.
   */
  private static final int DENSE_GROUPBY_PROBE_LEAVES =
      Integer.parseInt(System.getProperty("sirix.projection.denseGroupBy.probeLeaves", "16"));

  /**
   * Hard cap on dense group-by cardinality. Each worker pays
   * {@code N * 8 bytes} per scan for the {@code long[N]} accumulator;
   * at the default 256 that's 2 KB per worker — negligible vs the
   * per-thread scratch buffers already in
   * {@link ProjectionIndexByteScan.ScanScratch}.
   *
   * <p>Values above this limit fall back to the existing hashmap path.
   * Tune via {@code -Dsirix.projection.denseGroupBy.cardLimit=N}.
   */
  private static final int DENSE_GROUPBY_CARD_LIMIT =
      Integer.parseInt(System.getProperty("sirix.projection.denseGroupBy.cardLimit", "256"));

  /**
   * Parallel wrapper around {@link ProjectionIndexByteScan#conjunctiveCountByGroup}.
   * Same rationale as {@link #parallelConjunctiveCount} — the per-leaf
   * work is dominated by header decode + predicate eval, and a
   * single-thread scan over 100K leaves leaves ~19 cores idle. Per-worker
   * partial group-count maps are merged at the end.
   *
   * <p>iter#10: when {@link #DENSE_GROUPBY_ENABLED} is {@code true} and
   * the handle's canonical dict for {@code groupColumn} is known (built
   * on first access via {@link ProjectionIndexRegistry.Handle#canonicalDict}),
   * route through the dense accumulator via
   * {@link ProjectionIndexByteScan#conjunctiveCountByGroupDense}; otherwise
   * fall through to the legacy hashmap path.
   */
  private Object2LongOpenHashMap<String> parallelConjunctiveCountByGroup(
      final ProjectionIndexRegistry.Handle handle,
      final ProjectionIndexScan.ColumnPredicate[] preds,
      final int groupColumn,
      final long[] missingOut) {
    final java.util.List<byte[]> leafPayloads = handle.leafPayloads();
    final byte[][] canonicalDict = DENSE_GROUPBY_ENABLED
        ? handle.canonicalDict(groupColumn, DENSE_GROUPBY_PROBE_LEAVES, DENSE_GROUPBY_CARD_LIMIT)
        : null;
    if (canonicalDict != null) {
      return parallelConjunctiveCountByGroupDense(leafPayloads, preds, groupColumn, canonicalDict, missingOut);
    }
    return parallelConjunctiveCountByGroupHashMap(leafPayloads, preds, groupColumn, missingOut);
  }

  /**
   * Legacy hashmap-based group-by accumulator. Kept as the fallback when
   * (a) dense group-by is disabled by flag, (b) canonical dict exceeds
   * the cardinality limit, or (c) group column is not STRING_DICT.
   *
   * <p>{@code missingOut[0]} accumulates matching rows whose group field is
   * MISSING (sparse data) — the caller emits them as the null-key group, the
   * way the interpreter groups records lacking the field.
   */
  private Object2LongOpenHashMap<String> parallelConjunctiveCountByGroupHashMap(
      final java.util.List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] preds,
      final int groupColumn,
      final long[] missingOut) {
    final int leafCount = leafPayloads.size();
    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    if (leafCount == 0) return merged;
    if (leafCount < 64) {
      ProjectionIndexByteScan.conjunctiveCountByGroup(leafPayloads, preds, groupColumn, merged, missingOut);
      return merged;
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    @SuppressWarnings("unchecked")
    final Object2LongOpenHashMap<String>[] perThread = new Object2LongOpenHashMap[eff];
    final long[][] perThreadMissing = new long[eff][];
    final int chunkSize = (leafCount + eff - 1) / eff;

    try {
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final Object2LongOpenHashMap<String> local = new Object2LongOpenHashMap<>();
        local.defaultReturnValue(0L);
        final long[] localMissing = missingOut != null ? new long[1] : null;
        ProjectionIndexByteScan.conjunctiveCountByGroup(
            leafPayloads.subList(from, to), preds, groupColumn, local, localMissing);
        perThread[idx] = local;
        perThreadMissing[idx] = localMissing;
      });
    } catch (final Exception e) {
      throw new RuntimeException("parallel projection conjunctiveCountByGroup failed", e);
    }

    for (final var m : perThread) {
      if (m == null) continue;
      final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
      while (it.hasNext()) {
        final Object2LongMap.Entry<String> e = it.next();
        merged.addTo(e.getKey(), e.getLongValue());
      }
    }
    if (missingOut != null) {
      for (final long[] lm : perThreadMissing) {
        if (lm != null) missingOut[0] += lm[0];
      }
    }
    return merged;
  }

  /**
   * Dense parallel group-by accumulator: per-worker {@code long[N]}
   * indexed by canonical-dict position. Replaces the
   * {@link Object2LongOpenHashMap#addTo} CPU cost (~5-11 % of cold CPU
   * on the 100M bench's group-by queries) with a pure array-indexed
   * increment on the per-matching-row hot path.
   *
   * <p>A per-worker {@link Object2LongOpenHashMap} fallback catches any
   * leaf whose dict contains a value NOT in {@code canonicalDict} (late-
   * arriving value beyond what the probe found). The dense + fallback
   * counts are merged together at the end.
   */
  private Object2LongOpenHashMap<String> parallelConjunctiveCountByGroupDense(
      final java.util.List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] preds,
      final int groupColumn,
      final byte[][] canonicalDict,
      final long[] missingOut) {
    final int leafCount = leafPayloads.size();
    final int canonLen = canonicalDict.length;
    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    if (leafCount == 0) return merged;

    if (leafCount < 64) {
      // Serial dense path — small enough that parallelism overhead dominates.
      final long[] counts = new long[canonLen];
      final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
      fallback.defaultReturnValue(0L);
      ProjectionIndexByteScan.conjunctiveCountByGroupDense(
          leafPayloads, preds, groupColumn, canonicalDict, counts, fallback, missingOut);
      mergeDense(merged, canonicalDict, counts, fallback);
      return merged;
    }

    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    final long[][] perThreadCounts = new long[eff][];
    @SuppressWarnings("unchecked")
    final Object2LongOpenHashMap<String>[] perThreadFallback = new Object2LongOpenHashMap[eff];
    final long[][] perThreadMissing = new long[eff][];
    final int chunkSize = (leafCount + eff - 1) / eff;

    try {
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final long[] counts = new long[canonLen];
        final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
        fallback.defaultReturnValue(0L);
        final long[] localMissing = missingOut != null ? new long[1] : null;
        ProjectionIndexByteScan.conjunctiveCountByGroupDense(
            leafPayloads.subList(from, to), preds, groupColumn, canonicalDict, counts, fallback, localMissing);
        perThreadCounts[idx] = counts;
        perThreadFallback[idx] = fallback;
        perThreadMissing[idx] = localMissing;
      });
    } catch (final Exception e) {
      throw new RuntimeException("parallel projection conjunctiveCountByGroupDense failed", e);
    }
    if (missingOut != null) {
      for (final long[] lm : perThreadMissing) {
        if (lm != null) missingOut[0] += lm[0];
      }
    }

    // Reduce per-thread dense counts into the merged hashmap. The merged
    // output needs to use the same canonical String keys as the legacy
    // path for API parity — decode each canonical dict entry once here.
    final long[] totals = new long[canonLen];
    for (final long[] tc : perThreadCounts) {
      if (tc == null) continue;
      for (int i = 0; i < canonLen; i++) totals[i] += tc[i];
    }
    for (int i = 0; i < canonLen; i++) {
      if (totals[i] != 0L) {
        merged.put(new String(canonicalDict[i], StandardCharsets.UTF_8), totals[i]);
      }
    }
    // Accumulate per-thread fallback maps (values not in canonical dict).
    for (final var m : perThreadFallback) {
      if (m == null) continue;
      final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
      while (it.hasNext()) {
        final Object2LongMap.Entry<String> e = it.next();
        merged.addTo(e.getKey(), e.getLongValue());
      }
    }
    return merged;
  }

  /**
   * Merge helper used by the serial ({@code leafCount < 64}) dense path:
   * accumulate {@code counts[i]} against the canonical dict keys and
   * union any fallback entries.
   */
  private static void mergeDense(final Object2LongOpenHashMap<String> merged,
      final byte[][] canonicalDict, final long[] counts,
      final Object2LongOpenHashMap<String> fallback) {
    for (int i = 0; i < canonicalDict.length; i++) {
      if (counts[i] != 0L) {
        merged.put(new String(canonicalDict[i], StandardCharsets.UTF_8), counts[i]);
      }
    }
    if (fallback != null) {
      final ObjectIterator<Object2LongMap.Entry<String>> it = fallback.object2LongEntrySet().fastIterator();
      while (it.hasNext()) {
        final Object2LongMap.Entry<String> e = it.next();
        merged.addTo(e.getKey(), e.getLongValue());
      }
    }
  }

  /**
   * Group-by-count analogue of {@link #tryProjectionIndexFastPath}. Returns
   * a Brackit {@link Sequence} of {@code {"<groupField>": value, "count": n}}
   * objects when the query can be answered entirely from a projection index;
   * {@code null} otherwise (triggers fallback to the generic scan).
   *
   * <p>Eligibility criteria:
   * <ul>
   *   <li>A projection index is registered for the (resource, sourcePath).
   *   <li>All predicate fields AND the group field are columns of that index.
   *   <li>The predicate tree is a conjunction of supported leaves
   *       (see {@link #extractConjunctivePredicates}).
   *   <li>The group column is STRING_DICT — numeric / boolean group-by is
   *       out of scope here and falls through.
   * </ul>
   */
  private Sequence tryProjectionIndexGroupByFastPath(
      final String[] sourcePath, final CompiledPredicate cp, final String groupField) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) return null;
    // cp.fieldNames includes the group field (compileWithExtraField).
    // covers() checks every name, so it implicitly requires the group
    // field to also be a column of the index.
    if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
    final int groupColumn = handle.columnOf(groupField);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate — see tryProjectionIndexGroupByCountOnly.
    if (!handle.columnSparseClean(groupColumn)) return null;
    final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
    if (extracted == null) return null;
    // iter#07 range fusion — same policy as tryProjectionIndexFastPath.
    final ProjectionIndexScan.ColumnPredicate[] preds = fuseRangePredicates(extracted);
    final long[] missing = new long[1];
    final Object2LongOpenHashMap<String> agg;
    try {
      agg = parallelConjunctiveCountByGroup(handle, preds, groupColumn, missing);
    } catch (final IllegalStateException ise) {
      // Group column kind mismatch (e.g. numeric group). Fall back to the
      // generic scan path rather than raise an executor-level error.
      return null;
    }
    if (agg == null) return null;
    final ArrayList<Item> outItems = new ArrayList<>(agg.size() + 1);
    final QNm keyQnm = new QNm(groupField);
    final QNm cntQnm = new QNm("count");
    final ObjectIterator<Object2LongMap.Entry<String>> it = agg.object2LongEntrySet().fastIterator();
    while (it.hasNext()) {
      final Object2LongMap.Entry<String> e = it.next();
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { new Str(e.getKey()), new Int64(e.getLongValue()) };
      outItems.add(new ArrayObject(keys, vals));
    }
    if (missing[0] > 0) {
      final QNm[] keys = { keyQnm, cntQnm };
      final Sequence[] vals = { Null.INSTANCE, new Int64(missing[0]) };
      outItems.add(new ArrayObject(keys, vals));
    }
    return new ItemSequence(outItems.toArray(new Item[0]));
  }

  private static final boolean PROJ_DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Page-skip scan plan: decides whether to scan all pages or only a
   * cached subset, and whether to populate the page-skip bitmap as a
   * side-effect of this scan.
   *
   * <p>Layout:
   * <ul>
   *   <li>{@code pages != null}: iterate only these {@code int} page keys
   *       (sorted ascending). No recording — the bitmap was already
   *       built by a previous scan anchored on the same nameKey.</li>
   *   <li>{@code pages == null && recordBuffers != null}: iterate all
   *       pages [0, totalPages); each worker records the pages where
   *       anchor nameKey matched into its own buffer. Buffers are
   *       merged and published at scan end.</li>
   *   <li>{@code pages == null && recordBuffers == null}: iterate all
   *       pages; don't record (no resource key, or nameKey == -1).</li>
   * </ul>
   */
  private final class PageScanSchedule {
    private final int[] pages;
    private final RoaringBitmap[] recordBuffers;
    private final long totalPages;

    PageScanSchedule(final int[] pages, final RoaringBitmap[] recordBuffers, final long totalPages) {
      this.pages = pages;
      this.recordBuffers = recordBuffers;
      this.totalPages = totalPages;
    }

    /** @return number of pages this scan should visit. */
    long scheduleSize() {
      return pages != null ? pages.length : totalPages;
    }

    /** Map a schedule index to the actual page key. */
    long pageAt(final long scheduleIdx) {
      return pages != null ? Integer.toUnsignedLong(pages[(int) scheduleIdx]) : scheduleIdx;
    }

    /** @return per-worker record buffer for the given worker index, or {@code null} if not recording. */
    RoaringBitmap recordBufferOrNull(final int workerIdx) {
      return recordBuffers == null ? null : recordBuffers[workerIdx];
    }

    /**
     * Merge the per-worker record buffers and publish the resulting
     * bitmap to the page-skip registry under {@code anchorNameKey}. A
     * no-op when the schedule wasn't recording. First publisher wins
     * (see {@link PageSkipRegistry.Handle#record}); concurrent publishes
     * of the same {@code (resource, nameKey)} are safe and harmless.
     */
    void publish(final int anchorNameKey) {
      if (recordBuffers == null || projectionRegistryKey == null || anchorNameKey < 0) return;
      final RoaringBitmap merged = new RoaringBitmap();
      for (final RoaringBitmap b : recordBuffers) merged.or(b);
      merged.runOptimize();
      PageSkipRegistry.handleFor(projectionRegistryKey).record(anchorNameKey, merged, totalPages);
    }
  }

  /**
   * Build a {@link PageScanSchedule} for the given anchor.
   *
   * <p>Three sources, in priority order:
   * <ol>
   *   <li><b>PathSummary bitmap</b> — if {@code anchorPathNodeKey} resolves
   *       to a PathNode whose persisted presence bitmap is non-null, the
   *       schedule iterates only those pages. This survives restarts and
   *       is populated at write time.</li>
   *   <li><b>In-memory registry</b> — if a previous scan in this session
   *       already built the per-{@code nameKey} bitmap, reuse it.</li>
   *   <li><b>Full scan + record</b> — iterate all pages and populate the
   *       registry as a side effect, so subsequent scans in the same
   *       session skip the pages where the anchor is absent.</li>
   * </ol>
   */
  private PageScanSchedule planPageScan(final int anchorNameKey, final long anchorPathNodeKey,
      final long totalPages, final int eff) {
    // Source 1: PathSummary-persisted bitmap.
    if (anchorPathNodeKey > 0L) {
      try {
        final JsonNodeReadOnlyTrx rtx = workerTrx();
        try (PathSummaryReader summary = rtx.getResourceSession().openPathSummary(revision)) {
          final io.sirix.index.path.summary.PathNode pn = summary.getPathNodeForPathNodeKey(anchorPathNodeKey);
          if (pn != null) {
            final int[] persisted = pn.getPageKeysArray();
            if (persisted != null) {
              return new PageScanSchedule(persisted, null, totalPages);
            }
          }
        }
      } catch (final Exception ignored) {
        // PathSummary open can fail on resources without summaries;
        // gracefully fall through to source 2.
      }
    }
    // Source 2 + 3: opportunistic in-memory registry.
    if (projectionRegistryKey == null || anchorNameKey < 0) {
      return new PageScanSchedule(null, null, totalPages);
    }
    final PageSkipRegistry.Handle handle = PageSkipRegistry.handleFor(projectionRegistryKey);
    final int[] cached = handle.pagesForIfValid(anchorNameKey, totalPages);
    if (cached != null) {
      return new PageScanSchedule(cached, null, totalPages);
    }
    final RoaringBitmap[] buffers = new RoaringBitmap[eff];
    for (int i = 0; i < eff; i++) buffers[i] = new RoaringBitmap();
    return new PageScanSchedule(null, buffers, totalPages);
  }

  /**
   * Walk the {@link CompiledPredicate} tree and build a
   * {@link ProjectionIndexScan.ColumnPredicate}{@code []} if the tree is a
   * pure conjunction of supported leaves. Brackit's binary-AND folding
   * produces nested trees like {@code AND(AND(a>30, a<50), active)} for a
   * 3-way conjunction — we flatten those in place via a small array
   * stack, no recursion, no boxing. {@code null} signals "unsupported
   * shape; fall back to the generic path".
   */
  private static ProjectionIndexScan.ColumnPredicate[] extractConjunctivePredicates(
      final CompiledPredicate cp, final ProjectionIndexRegistry.Handle handle) {
    // Collect leaf node indices via a flat conjunction walk. Stack + out
    // arrays are sized to the full node count — bounded by the predicate's
    // tree size, which is small (tens of nodes max). One allocation per
    // query, no per-leaf churn.
    final int nodes = cp.ops.length;
    final int[] stack = new int[nodes];
    final int[] leaves = new int[nodes];
    int stackTop = 0;
    int leafCount = 0;
    stack[stackTop++] = 0;
    while (stackTop > 0) {
      final int n = stack[--stackTop];
      final byte op = cp.ops[n];
      if (op == CompiledPredicate.OP_AND) {
        final int cs = cp.childStart[n];
        final int cc = cp.childCount[n];
        for (int i = 0; i < cc; i++) stack[stackTop++] = cp.children[cs + i];
      } else if (op == CompiledPredicate.OP_NUM_CMP
          || op == CompiledPredicate.OP_FP_CMP
          || op == CompiledPredicate.OP_STR_EQ
          || op == CompiledPredicate.OP_BOOL_REF) {
        leaves[leafCount++] = n;
      } else {
        // OR / NOT / ALWAYS_* / anything else — can't represent as a
        // conjunction. Fall back to the generic predicate path.
        return null;
      }
    }
    if (leafCount == 0) return null;

    final ProjectionIndexScan.ColumnPredicate[] out = new ProjectionIndexScan.ColumnPredicate[leafCount];
    for (int i = 0; i < leafCount; i++) {
      final int n = leaves[i];
      final byte op = cp.ops[n];
      final int fi = cp.fieldIdx[n];
      if (fi < 0 || fi >= cp.fieldNames.length) return null;
      final int column = handle.columnOf(cp.fieldNames[fi]);
      if (column < 0) return null; // field not in index — should have been filtered by covers()
      // Sparse-evidence gate: a predicate over a column without per-row
      // presence data (legacy v1 leaves) or with unrepresentable values
      // (null/object/array/kind mismatch) would evaluate against stored
      // DEFAULTS — e.g. `x < 40` matching every missing row via the phantom
      // 0. Fail closed; the scan-path kernels handle missing correctly.
      if (!handle.columnSparseClean(column)) return null;
      final ProjectionIndexScan.ColumnPredicate pred;
      switch (op) {
        case CompiledPredicate.OP_NUM_CMP -> {
          final ProjectionIndexScan.Op translated = translateCmpOp(cp.cmpOp[n]);
          if (translated == null) return null;
          // Comparisons evaluate against the column's TRUNCATED longs. When the
          // builder positively saw non-integral values in this column, decline —
          // e.g. `score > 2` would match 2.5 stored as 2 incorrectly. Unknown
          // provenance keeps the legacy behavior (no regression for re-encoded
          // handles); known-clean columns are exact.
          if (handle.numericColumnKnownNonIntegral(column)) return null;
          pred = ProjectionIndexScan.ColumnPredicate.numeric(column, translated, cp.longLit[n]);
        }
        case CompiledPredicate.OP_FP_CMP -> {
          // Double-threshold comparison on a NUMERIC_LONG column: only valid
          // when the column is PROVABLY integral (builder-tracked evidence —
          // unknown provenance fails closed, the column stores truncated
          // doubles otherwise). The threshold is rewritten into exact long
          // space (x > 9.99 ⟺ x >= 10) — see rewriteFpCmpForIntegralColumn.
          if (!handle.numericColumnIsIntegral(column)) return null;
          pred = rewriteFpCmpForIntegralColumn(column, cp.cmpOp[n], cp.dblLit[n]);
          if (pred == null) return null;
        }
        case CompiledPredicate.OP_DEC_CMP -> {
          // Exact-decimal threshold on a NUMERIC_LONG column: the compile
          // step already collapsed the decimal comparison into a pure
          // long-space arm — translate it. Same integrality gate as FP_CMP.
          if (!handle.numericColumnIsIntegral(column)) return null;
          pred = switch (cp.decLongArm[n]) {
            case CompiledPredicate.DEC_ARM_GE ->
                ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GE, cp.decLongLit[n]);
            case CompiledPredicate.DEC_ARM_LE ->
                ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.LE, cp.decLongLit[n]);
            case CompiledPredicate.DEC_ARM_EQ ->
                ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.EQ, cp.decLongLit[n]);
            // Constant false: no long is greater than Long.MAX_VALUE.
            case CompiledPredicate.DEC_ARM_FALSE ->
                ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
            // Constant true for PRESENT rows: every long >= Long.MIN_VALUE
            // (presence bitmaps still exclude missing rows).
            case CompiledPredicate.DEC_ARM_TRUE ->
                ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GE, Long.MIN_VALUE);
            default -> null;
          };
          if (pred == null) return null;
        }
        case CompiledPredicate.OP_STR_EQ -> {
          pred = ProjectionIndexScan.ColumnPredicate.stringEq(column, cp.strLiteralBytes[cp.strIdx[n]]);
        }
        case CompiledPredicate.OP_BOOL_REF -> {
          // BOOL_REF means "field value is true" (a standalone truthy
          // field reference compiles here). Represent as booleanEq(true).
          pred = ProjectionIndexScan.ColumnPredicate.booleanEq(column, true);
        }
        default -> {
          return null; // unsupported leaf — fall back
        }
      }
      out[i] = pred;
    }
    return out;
  }

  /**
   * Range-fusion pass: detect same-column conjunctive pairs of the shape
   * {@code (GT|GE, lowLit) AND (LT|LE, highLit)} and rewrite into a
   * single fused {@link ProjectionIndexScan.Op#BETWEEN_GT_LT}
   * (or sibling BETWEEN_*) predicate. Eliminates the redundant
   * scalar-to-scratch load in
   * {@link ProjectionIndexByteScan#evalNumericBytes} that the un-fused
   * path runs twice on the same column.
   *
   * <p>Conservative fusion policy: at most one pair per column is
   * fused in a single pass. Extra predicates (a third GT, or an EQ,
   * or a BOOL_REF on the same column) are left unchanged — correct by
   * construction because AND is associative and the un-fused
   * predicates still get AND'd into the final mask.
   *
   * <p>Zero allocation when no fusion is possible: the method returns
   * the input array unchanged. One fresh array is allocated when at
   * least one fusion fires — amortised across the whole query
   * execution, not per-leaf.
   *
   * <p>Toggle: {@code -Dsirix.projection.rangeFusion=false} disables
   * the pass (also used as the rollback switch for benchmarking).
   */
  static ProjectionIndexScan.ColumnPredicate[] fuseRangePredicates(
      final ProjectionIndexScan.ColumnPredicate[] preds) {
    if (!RANGE_FUSION_ENABLED || preds == null || preds.length < 2) {
      return preds;
    }
    // Two-pass scan: (1) find fusible pairs per column, (2) rewrite.
    // The array is tiny (tens of entries max), so O(n) per-column
    // lookup is fine without a hash structure — the branch predictor
    // handles the column-equality loop well on short predicates.
    // Per-predicate role: -1 = keep, -2 = drop (fused into another),
    // -3 = low-bound of a pair, -4 = high-bound of a pair.
    final int n = preds.length;
    final int[] role = new int[n];
    java.util.Arrays.fill(role, -1);
    // Pair indices: lowIdx[i] = index of the low-bound predicate that
    // pairs with high-bound at i; symmetric for highIdx. -1 = no pair.
    final int[] pairLow = new int[n];
    final int[] pairHigh = new int[n];
    java.util.Arrays.fill(pairLow, -1);
    java.util.Arrays.fill(pairHigh, -1);
    int fusedPairs = 0;
    for (int i = 0; i < n; i++) {
      if (role[i] != -1) continue;
      final ProjectionIndexScan.ColumnPredicate pi = preds[i];
      if (!isLowBound(pi.op) && !isHighBound(pi.op)) continue;
      for (int j = i + 1; j < n; j++) {
        if (role[j] != -1) continue;
        final ProjectionIndexScan.ColumnPredicate pj = preds[j];
        if (pj.column != pi.column) continue;
        // String literal + numeric compare can never clash — both must
        // be NumCmp to even be comparable. isLowBound/isHighBound
        // already excludes EQ / non-numeric ops.
        final boolean iLow = isLowBound(pi.op);
        final boolean jHigh = isHighBound(pj.op);
        final boolean iHigh = isHighBound(pi.op);
        final boolean jLow = isLowBound(pj.op);
        if (iLow && jHigh) {
          role[i] = -3; pairLow[j] = i;
          role[j] = -4; pairHigh[i] = j;
          fusedPairs++;
          break;
        } else if (iHigh && jLow) {
          role[j] = -3; pairLow[i] = j;
          role[i] = -4; pairHigh[j] = i;
          fusedPairs++;
          break;
        }
        // Same-direction same-column (e.g. two GT) or any other shape:
        // leave both as-is and move on.
      }
    }
    if (fusedPairs == 0) return preds;
    // Build the output array. Policy: emit each fused BETWEEN at the
    // FIRST occurrence of its pair in the input order; skip at the
    // second occurrence. Non-fused predicates pass through unchanged.
    // This preserves overall ordering: relative positions of non-fused
    // predicates are stable, and the fused predicate takes the slot of
    // whichever bound appeared earlier.
    final ProjectionIndexScan.ColumnPredicate[] out =
        new ProjectionIndexScan.ColumnPredicate[n - fusedPairs];
    int w = 0;
    for (int i = 0; i < n; i++) {
      if (role[i] == -1) {
        out[w++] = preds[i];
        continue;
      }
      // Fused bound — find the partner.
      final int partnerIdx = role[i] == -3 ? pairHigh[i] : pairLow[i];
      if (partnerIdx < i) {
        // Partner came earlier → fused predicate was already emitted
        // at partnerIdx. Drop this one.
        continue;
      }
      // We're at the earlier of the two bounds → emit the fused pred.
      // Sort out which index holds which role so (lowOp, lowLit) comes
      // from the -3 slot and (highOp, highLit) from the -4 slot
      // regardless of input order.
      final int lowIdx  = role[i] == -3 ? i : partnerIdx;
      final int highIdx = role[i] == -4 ? i : partnerIdx;
      final ProjectionIndexScan.ColumnPredicate lo = preds[lowIdx];
      final ProjectionIndexScan.ColumnPredicate hi = preds[highIdx];
      out[w++] = ProjectionIndexScan.ColumnPredicate.numericBetween(
          lo.column, lo.op, lo.longLit, hi.op, hi.longLit);
    }
    return out;
  }

  private static boolean isLowBound(final ProjectionIndexScan.Op op) {
    return op == ProjectionIndexScan.Op.GT || op == ProjectionIndexScan.Op.GE;
  }

  private static boolean isHighBound(final ProjectionIndexScan.Op op) {
    return op == ProjectionIndexScan.Op.LT || op == ProjectionIndexScan.Op.LE;
  }

  /**
   * Map the executor's internal cmpOp encoding ({@link #OP_GT}..{@link #OP_EQ})
   * to {@link ProjectionIndexScan.Op}. Returns {@code null} for unrecognised
   * codes (e.g. NE), which trigger fallback.
   */
  private static ProjectionIndexScan.Op translateCmpOp(final int cmp) {
    return switch (cmp) {
      case OP_GT -> ProjectionIndexScan.Op.GT;
      case OP_LT -> ProjectionIndexScan.Op.LT;
      case OP_GE -> ProjectionIndexScan.Op.GE;
      case OP_LE -> ProjectionIndexScan.Op.LE;
      case OP_EQ -> ProjectionIndexScan.Op.EQ;
      default -> null;
    };
  }

  /**
   * Rewrite a double-threshold comparison over a PROVABLY-INTEGRAL
   * NUMERIC_LONG column into an exact long-space {@link
   * ProjectionIndexScan.ColumnPredicate}.
   *
   * <p>A long {@code L} satisfies the FpCmp leaf iff
   * {@code Double.compare((double) L, d) <op> 0} (the interpreter's numeric
   * promotion — see {@link PredicateNode.FpCmp}). Because {@code (double) L}
   * is monotone non-decreasing in {@code L}, {@code Double.compare((double) L, d)}
   * partitions the long range into a {@code < 0} prefix, an optional
   * {@code == 0} middle, and a {@code > 0} suffix. The boundaries are found
   * by binary search over that TOTAL ORDER — exact for every edge the
   * hand-derived floor/ceil arithmetic gets wrong (negative thresholds,
   * ±0.0, exact integral doubles, thresholds beyond 2^53 where
   * {@code (double) L} rounds, and the Long.MIN/MAX extremes):
   * <ul>
   * <li>{@code GT d ⟺ L >= firstGreater(d)} (unsatisfiable → constant-false);</li>
   * <li>{@code GE d ⟺ L >= firstGreaterOrEqual(d)};</li>
   * <li>{@code LT d ⟺ L <= lastLess(d)};</li>
   * <li>{@code LE d ⟺ L <= lastLessOrEqual(d)};</li>
   * <li>{@code EQ d ⟺ L ∈ [firstGreaterOrEqual(d), lastLessOrEqual(d)]} —
   * empty for fractional {@code d}, a single value for small integral
   * {@code d}, and a RANGE above 2^53 where multiple longs share one double
   * image (matching the interpreter exactly).</li>
   * </ul>
   * Returns {@code null} only for unsupported cmp codes. Constant-false is
   * expressed as {@code GT Long.MAX_VALUE} (no long satisfies it; the
   * zone-map prunes every leaf), constant-true-for-present-rows as
   * {@code GE Long.MIN_VALUE} (presence bitmaps still exclude missing rows).
   */
  static ProjectionIndexScan.ColumnPredicate rewriteFpCmpForIntegralColumn(final int column, final int cmpOp,
      final double d) {
    if (Double.isNaN(d)) {
      // NaN comparisons are always false (detection never emits them — defensive).
      return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
    }
    switch (cmpOp) {
      case OP_GT: {
        final long k = firstLongWithCompareAtLeast(d, 1);
        if (k == Long.MIN_VALUE && Double.compare((double) Long.MIN_VALUE, d) <= 0) {
          // No long compares greater — constant false.
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
        }
        return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GE, k);
      }
      case OP_GE: {
        final long k = firstLongWithCompareAtLeast(d, 0);
        if (k == Long.MIN_VALUE && Double.compare((double) Long.MIN_VALUE, d) < 0) {
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
        }
        return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GE, k);
      }
      case OP_LT: {
        final long k = lastLongWithCompareAtMost(d, -1);
        if (k == Long.MAX_VALUE && Double.compare((double) Long.MAX_VALUE, d) >= 0) {
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
        }
        return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.LE, k);
      }
      case OP_LE: {
        final long k = lastLongWithCompareAtMost(d, 0);
        if (k == Long.MAX_VALUE && Double.compare((double) Long.MAX_VALUE, d) > 0) {
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
        }
        return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.LE, k);
      }
      case OP_EQ: {
        final long lo = firstLongWithCompareAtLeast(d, 0);
        final long hi = lastLongWithCompareAtMost(d, 0);
        final boolean loValid = Double.compare((double) lo, d) == 0;
        final boolean hiValid = Double.compare((double) hi, d) == 0;
        if (!loValid || !hiValid || lo > hi) {
          // Fractional / out-of-range threshold — equality is unsatisfiable.
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.GT, Long.MAX_VALUE);
        }
        if (lo == hi) {
          return ProjectionIndexScan.ColumnPredicate.numeric(column, ProjectionIndexScan.Op.EQ, lo);
        }
        return ProjectionIndexScan.ColumnPredicate.numericBetween(column,
            ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LE, hi);
      }
      default:
        return null;
    }
  }

  /**
   * Smallest long {@code L} with {@code Double.compare((double) L, d) >= bound}
   * (bound ∈ {0, 1}); returns {@code Long.MIN_VALUE} when even the smallest
   * long satisfies it AND when none does — callers disambiguate by probing.
   * Binary search over the monotone non-decreasing compare.
   */
  private static long firstLongWithCompareAtLeast(final double d, final int bound) {
    long lo = Long.MIN_VALUE;
    long hi = Long.MAX_VALUE;
    if (Double.compare((double) hi, d) < bound) {
      return Long.MIN_VALUE;  // unsatisfiable — caller probes MIN_VALUE and detects
    }
    if (Double.compare((double) lo, d) >= bound) {
      return lo;
    }
    // Invariant: compare(lo) < bound <= compare(hi), lo < hi. The midpoint
    // uses an UNSIGNED halved difference — (hi - lo) wraps for the full long
    // range but its unsigned interpretation is the true difference, so
    // `>>> 1` halves correctly. The loop condition must NOT be a signed
    // `hi - lo > 1` (it wraps negative for MIN..MAX and skips the loop).
    while (lo + 1 != hi) {
      final long mid = lo + ((hi - lo) >>> 1);
      if (Double.compare((double) mid, d) >= bound) {
        hi = mid;
      } else {
        lo = mid;
      }
    }
    return hi;
  }

  /**
   * Largest long {@code L} with {@code Double.compare((double) L, d) <= bound}
   * (bound ∈ {-1, 0}); returns {@code Long.MAX_VALUE} when every long
   * satisfies it AND when none does — callers disambiguate by probing.
   */
  private static long lastLongWithCompareAtMost(final double d, final int bound) {
    long lo = Long.MIN_VALUE;
    long hi = Long.MAX_VALUE;
    if (Double.compare((double) lo, d) > bound) {
      return Long.MAX_VALUE;  // unsatisfiable — caller probes MAX_VALUE and detects
    }
    if (Double.compare((double) hi, d) <= bound) {
      return hi;
    }
    // Invariant: compare(lo) <= bound < compare(hi), lo < hi — see the
    // unsigned-midpoint note in firstLongWithCompareAtLeast.
    while (lo + 1 != hi) {
      final long mid = lo + ((hi - lo) >>> 1);
      if (Double.compare((double) mid, d) <= bound) {
        lo = mid;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private long parallelGenericPredicateCount(final String[] sourcePath, final PredicateNode predicate)
      throws Exception {
    final CompiledPredicate cp = compile(predicate);
    if (cp.fieldNames.length == 0) return 0L;
    // Fast-path: if a covering projection index is installed for this
    // (resource, sourcePath), convert the predicate tree to ColumnPredicate[]
    // and route the count to the zero-copy SIMD byte-scan — no OBJECT_KEY
    // traversal, no collectColumns, no per-record varint decodes. Only
    // conjunctive (AND-only) trees of NUM_CMP / STR_EQ / BOOL_REF leaves
    // are eligible; anything else falls back to the generic path below.
    // Registry lookup is a single ConcurrentHashMap.get — adds ~50 ns to
    // queries that have no projection index installed (measured in the
    // iter-5 bench), swallowed by the millisecond-scale fallback path.
    {
      final Long projected = tryProjectionIndexFastPath(sourcePath, cp);
      if (projected != null) {
        return projected;
      }
    }
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
    // Page-skip index: if a previous scan built a bitmap of pages where
    // this anchor nameKey exists, iterate only those pages. Otherwise
    // iterate all pages AND populate the bitmap as a side effect (per-
    // thread accumulators merged at the end). The bitmap build is free:
    // the loop had to visit every page anyway to produce this scan's
    // result. After publication, subsequent scans for the same nameKey
    // skip the pages where it's absent.
    final PageScanSchedule schedule = planPageScan(anchorNameKey, anchorPathNodeKey, totalPages, eff);
    // Work-stealing via shared atomic cursor — each worker grabs the next
    // stride of pages dynamically. Previously we pre-partitioned into
    // (totalPages / eff) per-worker ranges; that's catastrophic when one
    // range has all the pathNodeKey matches and others are empty, because
    // the lucky workers idle while one chugs. At 10M records with 20
    // workers and a sparse match distribution this alone doubles throughput.
    // Stride = 8 pages per steal amortises the atomic CAS over enough work.
    final AtomicLong cursor = new AtomicLong();
    final int STRIDE = 8;
    final long scheduleSize = schedule.scheduleSize();
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final RoaringBitmap recordBuf = schedule.recordBufferOrNull(idx);
      long localCount = 0;
      if (useBatch) {
        final EvalBatch batch = new EvalBatch(cp.fieldNames.length, cp.ops.length, Constants.INP_REFERENCE_COUNT);
        final long[] rootOut = new long[batch.bitmapStride];
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= scheduleSize) break;
          final long e = Math.min(s + STRIDE, scheduleSize);
          for (long j = s; j < e; j++) {
            final long pk = schedule.pageAt(j);
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            // Zone-map pre-check: if the anchor field's per-page tagMin/tagMax
            // proves no row on this page can satisfy the conjunctive NumCmp
            // constraints, skip the entire page without decoding a single
            // record. COMPLETENESS ORACLE (same rule as every region fast
            // path): the NumberRegion only carries LONG-typed values, so its
            // bounds say nothing about DOUBLE/decimal-valued rows — pruning is
            // only sound when the tag covers EVERY anchor slot. Without the
            // tagCount check, `rating gt 3` on a page holding {3, 3.7} pruned
            // via tagMin=tagMax=3 and silently dropped the 3.7 match.
            if (zoneProbe != null) {
              final NumberRegion.Header numHdr = kv.getNumberRegionHeader();
              if (numHdr != null && numHdr.tagMin != null) {
                final int lookupKey = (numHdr.tagKind == NumberRegion.TAG_KIND_PATH_NODE
                    && anchorPathNodeKey > 0 && anchorPathNodeKey <= Integer.MAX_VALUE)
                    ? (int) anchorPathNodeKey : anchorNameKey;
                final int tag = NumberRegion.lookupTag(numHdr, lookupKey);
                if (tag >= 0 && numHdr.tagCount[tag] == matches.length
                    && canPruneByZone(zoneProbe, numHdr.tagMin[tag], numHdr.tagMax[tag])) {
                  continue;
                }
              }
            }
            if (recordBuf != null) recordBuf.add((int) pk);
            collectColumns(kv, base, matches, cp, anchorPathNodeKey, batch, rtx);
            if (batch.size == 0) continue;
            final long[] bitmap;
            if (compiled != null && !batch.hasDecimalRows) {
              compiled.evaluate(batch, rootOut);
              bitmap = rootOut;
            } else {
              // Decimal rows need the exact decimal arms — only the
              // interpreter models them (the generated kernels are long/
              // double only).
              bitmap = evalCompiledBatch(cp, batch);
            }
            localCount += countBits(bitmap, batch.size);
          }
        }
      } else {
        final EvalScratch scratch = new EvalScratch(cp.fieldNames.length, cp.ops.length);
        while (true) {
          final long s = cursor.getAndAdd(STRIDE);
          if (s >= scheduleSize) break;
          final long e = Math.min(s + STRIDE, scheduleSize);
          for (long j = s; j < e; j++) {
            final long pk = schedule.pageAt(j);
            final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
            if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
            final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
            final int[] matches = kv.getObjectKeySlotsForNameKey(anchorNameKey);
            if (matches.length == 0) continue;
            if (recordBuf != null) recordBuf.add((int) pk);
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
    schedule.publish(anchorNameKey);
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
      // Fused OBJECT_NAMED_* carries the value inline — no descent. Legacy OBJECT_KEY falls through.
      final NodeKind anchorKind = rtx.getKind();
      switch (anchorKind) {
        case OBJECT_NAMED_NUMBER -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoScratch(scratch, cp, fi, n);
          }
          continue;
        }
        case OBJECT_NAMED_BOOLEAN -> {
          scratch.boolVals[fi] = rtx.getBooleanValue();
          scratch.fieldKind[fi] = FK_BOOL;
          continue;
        }
        case OBJECT_NAMED_STRING -> {
          scratch.strVals[fi] = rtx.getValue();
          scratch.fieldKind[fi] = FK_STR;
          continue;
        }
        case OBJECT_NAMED_NULL -> {
          // Null never satisfies numeric/bool/string-eq ops — leave kind=0 (missing).
          continue;
        }
        default -> { /* OBJECT_KEY — descend */ }
      }
      if (!rtx.moveToFirstChild()) { continue; }
      final NodeKind kind = rtx.getKind();
      switch (kind) {
        case NUMBER_VALUE -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoScratch(scratch, cp, fi, n);
          }
        }
        case BOOLEAN_VALUE -> {
          scratch.boolVals[fi] = rtx.getBooleanValue();
          scratch.fieldKind[fi] = FK_BOOL;
        }
        case STRING_VALUE -> {
          scratch.strVals[fi] = rtx.getValue();
          scratch.fieldKind[fi] = FK_STR;
        }
        default -> { /* unsupported field type — leave kind=0 (missing) */ }
      }
      rtx.moveToParent();
    } while (rtx.moveToRightSibling());
  }

  /** Typed number load for the record-at-a-time scratch — see {@link #classifyNumberForPredicate}. */
  private static void loadNumberIntoScratch(final EvalScratch scratch, final CompiledPredicate cp, final int fi,
      final Number n) {
    final byte kind = classifyNumberForPredicate(n, cp.fieldUsedInPredicate[fi], cp.fieldNames[fi]);
    switch (kind) {
      case FK_LONG -> {
        scratch.longVals[fi] = n.longValue();
        scratch.fieldKind[fi] = FK_LONG;
      }
      case FK_DOUBLE -> {
        scratch.dblVals[fi] = n.doubleValue();
        scratch.fieldKind[fi] = FK_DOUBLE;
      }
      case FK_DECIMAL -> {
        scratch.decVals[fi] = decimalOfNumber(n);
        scratch.fieldKind[fi] = FK_DECIMAL;
      }
      default -> { /* unsupported + unused by the predicate — stays missing in the scratch */ }
    }
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
        // Long rows compare exactly in long space; double rows promote the
        // integer literal to double (the interpreter's Dbl#cmp:
        // Double.compare(v, lit.doubleValue())); decimal rows compare
        // EXACTLY in decimal space (Dec#cmp's DecNumeric branch).
        return switch (scratch.fieldKind[fi]) {
          case FK_LONG -> scalarEval(scratch.longVals[fi], cp.cmpOp[nodeIdx], cp.longLit[nodeIdx]);
          case FK_DOUBLE -> scalarEvalDbl(scratch.dblVals[fi], cp.cmpOp[nodeIdx], (double) cp.longLit[nodeIdx]);
          case FK_DECIMAL -> scalarEvalCmp(
              scratch.decVals[fi].compareTo(java.math.BigDecimal.valueOf(cp.longLit[nodeIdx])),
              cp.cmpOp[nodeIdx]);
          default -> false;
        };
      }
      case CompiledPredicate.OP_FP_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        // EVERY numeric row kind promotes to double against an xs:double
        // literal — exactly the interpreter's DblNumeric dispatch
        // (Double.compare(v.doubleValue(), lit)), including its precision
        // loss for longs above 2^53.
        return switch (scratch.fieldKind[fi]) {
          case FK_LONG -> scalarEvalDbl((double) scratch.longVals[fi], cp.cmpOp[nodeIdx], cp.dblLit[nodeIdx]);
          case FK_DOUBLE -> scalarEvalDbl(scratch.dblVals[fi], cp.cmpOp[nodeIdx], cp.dblLit[nodeIdx]);
          case FK_DECIMAL -> scalarEvalDbl(scratch.decVals[fi].doubleValue(), cp.cmpOp[nodeIdx],
              cp.dblLit[nodeIdx]);
          default -> false;
        };
      }
      case CompiledPredicate.OP_DEC_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        // Long rows use the precomputed exact long-space arm; decimal rows
        // compare exactly; double rows promote the DECIMAL literal to double
        // (the interpreter's Dbl#cmp over a DecNumeric operand).
        return switch (scratch.fieldKind[fi]) {
          case FK_LONG -> evalDecLongArm(cp, nodeIdx, scratch.longVals[fi]);
          case FK_DOUBLE -> scalarEvalDbl(scratch.dblVals[fi], cp.cmpOp[nodeIdx], cp.decDblImage[nodeIdx]);
          case FK_DECIMAL -> evalDecOnDecimal(cp, nodeIdx, scratch.decVals[fi]);
          default -> false;
        };
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
    /** [fieldIdx] → double[capacity] — valid iff presentKind[fieldIdx][i] == 4. */
    final double[][] dblCols;
    /** [fieldIdx] → BigDecimal[capacity] — valid iff presentKind[fieldIdx][i] == 5. */
    final java.math.BigDecimal[][] decCols;
    /**
     * Whether the CURRENT page loaded any {@code FK_DECIMAL} row. Reset per
     * page in {@link #collectColumns}; routes evaluation to the
     * decimal-aware interpreter (the generated classes and fused kernels
     * only model long/double rows).
     */
    boolean hasDecimalRows;
    /** [fieldIdx] → boolean[capacity] — valid iff presentKind[fieldIdx][i] == 2. */
    final boolean[][] boolCols;
    /** [fieldIdx] → String[capacity] — valid iff presentKind[fieldIdx][i] == 3. */
    final String[][] strCols;
    /** [fieldIdx] → byte[capacity]. 0=missing, 1=long num, 2=bool, 3=str, 4=double num. */
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
      this.dblCols = new double[nFields][cap];
      this.decCols = new java.math.BigDecimal[nFields][cap];
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
    batch.hasDecimalRows = false;
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

      // Anchor value — direct slot first. Fused OBJECT_NAMED_* slots carry the value inline.
      // Non-fused legacy slots no longer have a direct-read fast path and defer to rtx.
      final int anchorKindId = kv.getSlotNodeKindId(slot);
      final boolean anchorDirect = KeyValueLeafPage.isFusedObjectNamedKindId(anchorKindId)
          && readFusedAnchorValueDirect(kv, anchorKindId, slot, anchorFieldIdx, row, batch);
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
          // Non-fused sibling — direct-slot path subsumed by fused; defer to rtx.
          rowNeedsRtx[rowIdx >>> 6] |= 1L << (rowIdx & 63);
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
   * Direct-slot value read for a fused {@code OBJECT_NAMED_*} anchor. The value is
   * inline on the fused record itself — no child descent — so dispatch on the kind
   * and pull the primitive via the page's inline-slot getters.
   *
   * @return {@code true} on success, {@code false} when the payload encoding falls
   *         outside the direct-read fast path (e.g. unsupported number subtype),
   *         forcing the caller to the rtx fallback.
   */
  private static boolean readFusedAnchorValueDirect(final KeyValueLeafPage kv,
      final int kindId, final int slot, final int fieldIdx, final int row,
      final EvalBatch batch) {
    switch (kindId) {
      case KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID: {
        final long v = kv.getFusedObjectNamedNumberValueLongFromSlot(slot);
        if (v == Long.MIN_VALUE) return false; // float/double/BigDecimal → rtx fallback
        batch.longCols[fieldIdx][row] = v;
        batch.presentKind[fieldIdx][row] = 1;
        return true;
      }
      case KeyValueLeafPage.FUSED_OBJECT_NAMED_BOOLEAN_KIND_ID: {
        batch.boolCols[fieldIdx][row] = kv.getFusedObjectNamedBooleanValueFromSlot(slot);
        batch.presentKind[fieldIdx][row] = 2;
        return true;
      }
      case KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID: {
        // String: defer to rtx path — FSST decode + scratch sizing parity with
        // readAnchorValueDirect. Single-field STR_EQ is not the common hot case.
        return false;
      }
      case KeyValueLeafPage.FUSED_OBJECT_NAMED_NULL_KIND_ID: {
        // Null: leave presentKind[fieldIdx][row] = 0 (missing). Reported-null never
        // satisfies the numeric / boolean / string-equality ops the compiler emits.
        return true;
      }
      default:
        return false;
    }
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
      // Fused OBJECT_NAMED_* carries the value inline — no descent. Legacy OBJECT_KEY falls through.
      final NodeKind anchorKind = rtx.getKind();
      switch (anchorKind) {
        case OBJECT_NAMED_NUMBER -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoBatch(batch, cp, fi, row, n);
          }
          continue;
        }
        case OBJECT_NAMED_BOOLEAN -> {
          batch.boolCols[fi][row] = rtx.getBooleanValue();
          batch.presentKind[fi][row] = FK_BOOL;
          continue;
        }
        case OBJECT_NAMED_STRING -> {
          batch.strCols[fi][row] = rtx.getValue();
          batch.presentKind[fi][row] = FK_STR;
          continue;
        }
        case OBJECT_NAMED_NULL -> {
          // Null never satisfies numeric/bool/string-eq ops — leave presentKind=0 (missing).
          continue;
        }
        default -> { /* OBJECT_KEY — descend */ }
      }
      if (!rtx.moveToFirstChild()) { continue; }
      final NodeKind kind = rtx.getKind();
      switch (kind) {
        case NUMBER_VALUE -> {
          final Number n = rtx.getNumberValue();
          if (n != null) {
            loadNumberIntoBatch(batch, cp, fi, row, n);
          }
        }
        case BOOLEAN_VALUE -> {
          batch.boolCols[fi][row] = rtx.getBooleanValue();
          batch.presentKind[fi][row] = FK_BOOL;
        }
        case STRING_VALUE -> {
          batch.strCols[fi][row] = rtx.getValue();
          batch.presentKind[fi][row] = FK_STR;
        }
        default -> { /* unsupported — leave presentKind[fi][row] = 0 */ }
      }
      rtx.moveToParent();
    } while (rtx.moveToRightSibling());
    return true;
  }

  /** Typed number load for the column batch — see {@link #classifyNumberForPredicate}. */
  private static void loadNumberIntoBatch(final EvalBatch batch, final CompiledPredicate cp, final int fi,
      final int row, final Number n) {
    final byte kind = classifyNumberForPredicate(n, cp.fieldUsedInPredicate[fi], cp.fieldNames[fi]);
    switch (kind) {
      case FK_LONG -> {
        batch.longCols[fi][row] = n.longValue();
        batch.presentKind[fi][row] = FK_LONG;
      }
      case FK_DOUBLE -> {
        batch.dblCols[fi][row] = n.doubleValue();
        batch.presentKind[fi][row] = FK_DOUBLE;
      }
      case FK_DECIMAL -> {
        batch.decCols[fi][row] = decimalOfNumber(n);
        batch.presentKind[fi][row] = FK_DECIMAL;
        batch.hasDecimalRows = true;
      }
      default -> { /* unsupported + unused by the predicate — stays missing in the batch */ }
    }
  }

  /**
   * Phase 2: evaluate the compiled predicate over the columns into a bitmap.
   * Returns the root node's bitmap (stored in
   * {@code batch.nodeBitmaps[0]}). Valid bits are [0, batch.size).
   */
  private static long[] evalCompiledBatch(final CompiledPredicate cp, final EvalBatch batch) {
    // Shape-specialised fast path for the common predicate trees. When the
    // ASM-compiled BatchPredicate isn't available (native-image AOT where
    // runtime class definition is rejected), the recursive
    // evalCompiledBatchRec walks the ops/children arrays with a switch per
    // node and a child-bitmap allocation per intermediate — that's 30-100×
    // slower than a fused single-pass evaluator on the hot shapes. Try the
    // specialised path first; fall through to the generic interpreter on
    // non-matching shapes (which still works, just slower).
    // Pages carrying DECIMAL rows route straight to the recursive
    // interpreter — the shape-specialised fused kernels only model
    // long/double rows and would silently skip them.
    if (!batch.hasDecimalRows) {
      final long[] specialized = evalCompiledBatchSpecialized(cp, batch);
      if (specialized != null) return specialized;
    }
    return evalCompiledBatchRec(cp, 0, batch);
  }

  /**
   * Shape-specialised single-pass interpreters for the predicate trees that
   * show up in the benchmark suite. Returns the result bitmap, or
   * {@code null} if the compiled predicate doesn't match any specialisation.
   *
   * <p>Shapes handled:
   * <ul>
   *   <li>{@code OP_AND(NUM_CMP(fi=0), BOOL_REF(fi=1))} — filterCount / filterGroupBy</li>
   *   <li>{@code OP_AND(AND(NUM_CMP(fi=0), NUM_CMP(fi=0)), BOOL_REF(fi=2))} — compoundAndFilter</li>
   *   <li>{@code OP_NUM_CMP(fi=0)} — single-leaf age range — uses OP_NUM_CMP's existing SIMD path</li>
   *   <li>{@code OP_BOOL_REF(fi=0)} — single-leaf boolean filter</li>
   * </ul>
   */
  private static long[] evalCompiledBatchSpecialized(final CompiledPredicate cp, final EvalBatch batch) {
    final byte[] ops = cp.ops;
    final int[] children = cp.children;
    final int[] childStart = cp.childStart;
    final int[] childCount = cp.childCount;
    final int[] fieldIdx = cp.fieldIdx;
    final int[] cmpOp = cp.cmpOp;
    final long[] longLit = cp.longLit;
    final int size = batch.size;
    final long[] out = batch.nodeBitmaps[0];
    final int stride = (size + 63) >>> 6;
    for (int i = 0; i < stride; i++) out[i] = 0L;
    final byte root = ops[0];
    // Shape: single OP_BOOL_REF leaf (just `$u.active`).
    if (root == CompiledPredicate.OP_BOOL_REF) {
      final int fi = fieldIdx[0];
      final byte[] pk = batch.presentKind[fi];
      final boolean[] vals = batch.boolCols[fi];
      for (int i = 0; i < size; i++) {
        if (pk[i] == 2 && vals[i]) out[i >>> 6] |= 1L << (i & 63);
      }
      return out;
    }
    if (root != CompiledPredicate.OP_AND) return null;
    final int rootStart = childStart[0];
    final int rootCount = childCount[0];
    // Shape: OP_AND with exactly 2 leaves — NUM_CMP + BOOL_REF (filterCount shape).
    if (rootCount == 2) {
      final int cA = children[rootStart];
      final int cB = children[rootStart + 1];
      if (ops[cA] == CompiledPredicate.OP_NUM_CMP && ops[cB] == CompiledPredicate.OP_BOOL_REF) {
        return fusedNumCmpAndBool(batch, fieldIdx[cA], cmpOp[cA], longLit[cA],
            fieldIdx[cB], out, size);
      }
      if (ops[cA] == CompiledPredicate.OP_BOOL_REF && ops[cB] == CompiledPredicate.OP_NUM_CMP) {
        return fusedNumCmpAndBool(batch, fieldIdx[cB], cmpOp[cB], longLit[cB],
            fieldIdx[cA], out, size);
      }
      // Shape: two NUM_CMP on same or different numeric fields (range query).
      if (ops[cA] == CompiledPredicate.OP_NUM_CMP && ops[cB] == CompiledPredicate.OP_NUM_CMP) {
        return fusedTwoNumCmp(batch, fieldIdx[cA], cmpOp[cA], longLit[cA],
            fieldIdx[cB], cmpOp[cB], longLit[cB], out, size);
      }
    }
    // Shape: OP_AND(nested-AND-of-NUM_CMPs, BOOL_REF) — compoundAndFilter.
    if (rootCount == 2) {
      // One child is inner AND, the other is BOOL_REF.
      int andChild = -1, boolChild = -1;
      for (int k = 0; k < 2; k++) {
        final int c = children[rootStart + k];
        if (ops[c] == CompiledPredicate.OP_AND) andChild = c;
        else if (ops[c] == CompiledPredicate.OP_BOOL_REF) boolChild = c;
      }
      if (andChild >= 0 && boolChild >= 0
          && childCount[andChild] == 2) {
        final int iA = children[childStart[andChild]];
        final int iB = children[childStart[andChild] + 1];
        if (ops[iA] == CompiledPredicate.OP_NUM_CMP && ops[iB] == CompiledPredicate.OP_NUM_CMP) {
          return fusedTwoNumCmpAndBool(batch, fieldIdx[iA], cmpOp[iA], longLit[iA],
              fieldIdx[iB], cmpOp[iB], longLit[iB],
              fieldIdx[boolChild], out, size);
        }
      }
    }
    return null;
  }

  /**
   * Per-row numeric leg of the fused kernels: long rows compare in long
   * space, double rows promote the integer literal to double (the
   * interpreter's promotion) — missing/other kinds never match.
   */
  private static boolean fusedNumLeg(final byte kind, final long lv, final double dv,
      final int cmp, final long lit) {
    if (kind == FK_LONG) {
      return switch (cmp) {
        case OP_GT -> lv > lit;
        case OP_LT -> lv < lit;
        case OP_GE -> lv >= lit;
        case OP_LE -> lv <= lit;
        case OP_EQ -> lv == lit;
        default -> false;
      };
    }
    if (kind == FK_DOUBLE) {
      return scalarEvalDbl(dv, cmp, (double) lit);
    }
    return false;
  }

  /** {@code (num[fiNum] OP numLit) AND (bool[fiBool] == true)} — single pass. */
  private static long[] fusedNumCmpAndBool(final EvalBatch batch,
      final int fiNum, final int cmp, final long numLit,
      final int fiBool, final long[] out, final int size) {
    switch (cmp) {
      case OP_GT, OP_LT, OP_GE, OP_LE, OP_EQ -> { /* supported */ }
      default -> {
        return null; // unsupported cmpOp, fall back
      }
    }
    final byte[] pkNum = batch.presentKind[fiNum];
    final long[] vals = batch.longCols[fiNum];
    final double[] dvals = batch.dblCols[fiNum];
    final byte[] pkBool = batch.presentKind[fiBool];
    final boolean[] bvals = batch.boolCols[fiBool];
    for (int i = 0; i < size; i++) {
      if (pkBool[i] == 2 && bvals[i] && fusedNumLeg(pkNum[i], vals[i], dvals[i], cmp, numLit)) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
    return out;
  }

  /** {@code (num[fiA] OPA aLit) AND (num[fiB] OPB bLit)} — single pass. */
  private static long[] fusedTwoNumCmp(final EvalBatch batch,
      final int fiA, final int cmpA, final long aLit,
      final int fiB, final int cmpB, final long bLit,
      final long[] out, final int size) {
    final byte[] pkA = batch.presentKind[fiA];
    final long[] valsA = batch.longCols[fiA];
    final double[] dvalsA = batch.dblCols[fiA];
    final byte[] pkB = batch.presentKind[fiB];
    final long[] valsB = batch.longCols[fiB];
    final double[] dvalsB = batch.dblCols[fiB];
    for (int i = 0; i < size; i++) {
      if (!fusedNumLeg(pkA[i], valsA[i], dvalsA[i], cmpA, aLit)) continue;
      if (fusedNumLeg(pkB[i], valsB[i], dvalsB[i], cmpB, bLit)) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
    return out;
  }

  /**
   * {@code (num[fiA] OPA aLit) AND (num[fiB] OPB bLit) AND (bool[fiBool] == true)}
   * — compoundAndFilter shape.
   */
  private static long[] fusedTwoNumCmpAndBool(final EvalBatch batch,
      final int fiA, final int cmpA, final long aLit,
      final int fiB, final int cmpB, final long bLit,
      final int fiBool, final long[] out, final int size) {
    final byte[] pkA = batch.presentKind[fiA];
    final long[] valsA = batch.longCols[fiA];
    final double[] dvalsA = batch.dblCols[fiA];
    final byte[] pkB = batch.presentKind[fiB];
    final long[] valsB = batch.longCols[fiB];
    final double[] dvalsB = batch.dblCols[fiB];
    final byte[] pkBool = batch.presentKind[fiBool];
    final boolean[] bvals = batch.boolCols[fiBool];
    for (int i = 0; i < size; i++) {
      if (pkBool[i] != 2 || !bvals[i]) continue;
      if (!fusedNumLeg(pkA[i], valsA[i], dvalsA[i], cmpA, aLit)) continue;
      if (fusedNumLeg(pkB[i], valsB[i], dvalsB[i], cmpB, bLit)) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
    return out;
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
        // Missing/non-long rows (presentKind != 1) must never match the long
        // sweep. We overwrite them in a scratch column with a sentinel chosen
        // per-op so the SIMD compare trivially rejects them:
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
        } else {
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
        }
        // DOUBLE-typed rows (mixed int/double columns, e.g. rating 3 vs 3.7):
        // the interpreter promotes the INTEGER LITERAL to double and compares
        // in double space — sweep the double column for presentKind == 4 rows.
        // DECIMAL rows compare EXACTLY against the integer literal.
        final double dLit = (double) lit;
        final double[] dvals = batch.dblCols[fi];
        final java.math.BigDecimal[] decvals = batch.decCols[fi];
        final java.math.BigDecimal bdLit = batch.hasDecimalRows ? java.math.BigDecimal.valueOf(lit) : null;
        for (int i = 0; i < size; i++) {
          if (pk[i] == FK_DOUBLE && scalarEvalDbl(dvals[i], cmp, dLit)) {
            out[i >>> 6] |= 1L << (i & 63);
          } else if (pk[i] == FK_DECIMAL && scalarEvalCmp(decvals[i].compareTo(bdLit), cmp)) {
            out[i >>> 6] |= 1L << (i & 63);
          }
        }
        return out;
      }
      case CompiledPredicate.OP_FP_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        final int cmp = cp.cmpOp[nodeIdx];
        final double dLit = cp.dblLit[nodeIdx];
        final byte[] pk = batch.presentKind[fi];
        final long[] vals = batch.longCols[fi];
        final double[] dvals = batch.dblCols[fi];
        final java.math.BigDecimal[] decvals = batch.decCols[fi];
        // Every numeric row kind evaluates in double space — the FpCmp
        // semantics contract (the interpreter's DblNumeric promotion).
        for (int i = 0; i < size; i++) {
          final byte k = pk[i];
          final boolean match;
          if (k == FK_LONG) {
            match = scalarEvalDbl((double) vals[i], cmp, dLit);
          } else if (k == FK_DOUBLE) {
            match = scalarEvalDbl(dvals[i], cmp, dLit);
          } else if (k == FK_DECIMAL) {
            match = scalarEvalDbl(decvals[i].doubleValue(), cmp, dLit);
          } else {
            match = false;
          }
          if (match) out[i >>> 6] |= 1L << (i & 63);
        }
        return out;
      }
      case CompiledPredicate.OP_DEC_CMP: {
        final int fi = cp.fieldIdx[nodeIdx];
        final int cmp = cp.cmpOp[nodeIdx];
        final byte[] pk = batch.presentKind[fi];
        final long[] vals = batch.longCols[fi];
        final double[] dvals = batch.dblCols[fi];
        final java.math.BigDecimal[] decvals = batch.decCols[fi];
        final double decImage = cp.decDblImage[nodeIdx];
        for (int i = 0; i < size; i++) {
          final byte k = pk[i];
          final boolean match;
          if (k == FK_LONG) {
            match = evalDecLongArm(cp, nodeIdx, vals[i]);
          } else if (k == FK_DOUBLE) {
            match = scalarEvalDbl(dvals[i], cmp, decImage);
          } else if (k == FK_DECIMAL) {
            match = evalDecOnDecimal(cp, nodeIdx, decvals[i]);
          } else {
            match = false;
          }
          if (match) out[i >>> 6] |= 1L << (i & 63);
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
      final String cdKey = pathCacheKey(sourcePath, field);
      final Sequence cached = countDistinctResultCache.get(cdKey);
      if (cached != null) return cached;
      // Projection fast path: if a covering projection index is installed
      // and carries {@code field} as a STRING_DICT column, count the global
      // dict via the in-memory projection rather than walking record pages.
      // This elides the per-page StringRegion probe + 100 M-slot walk that
      // the generic {@link #parallelCountDistinct} does. Sub-ms vs ~10 s
      // at cold 100 M.
      final Sequence projected = tryProjectionIndexCountDistinct(sourcePath, field);
      if (projected != null) {
        countDistinctResultCache.putIfAbsent(cdKey, projected);
        return projected;
      }
      // Exact count-distinct (task #66). The prior HLL-based fast path
      // returned approximate answers above the ~2 K linear-counting
      // threshold (2.3% std error at precision P=11) — not acceptable
      // for a user-facing query result. The HLL sketch in PathSummary
      // is retained but only consumed by the cost-based optimizer's
      // cardinality estimator.
      //
      // We don't delegate to executeGroupByCount because its
      // ScanResult.GroupByResult hash map has a fixed 4 K capacity —
      // workloads with more distinct values deadlock on linear probe
      // when no slot is ever free. A dedicated kernel with a growable
      // HashSet<ByteBuffer> handles any cardinality.
      final int fieldKey = resolveFieldKey(field);
      // -1 = MISSING sentinel only — negative hashes are legitimate nameKeys.
      if (fieldKey == -1) {
        final Sequence empty = new Int64(0L);
        countDistinctResultCache.putIfAbsent(cdKey, empty);
        return empty;
      }
      final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
      // Byte-hash kernel first, then VERIFY: a visited record that contributed no
      // value (non-string / null / missing — fused primitives have no child to
      // descend into) means the string-only kernel was not authoritative; redo with
      // the typed group-key kernel and count its groups. The empty-key group maps
      // to `return $d` emitting ZERO items per spec, so it does not count.
      long distinct;
      if (isTypedPrimitiveKind(probeFirstAnchorSlotKind(field))) {
        // Number/boolean/null column — typed kernel directly (columnar page paths).
        final Object2LongOpenHashMap<String> groups =
            typedGroupKeyCounts(sourcePath, null, new String[] { field });
        distinct = groups.size() - (groups.containsKey("m") ? 1 : 0);
      } else {
        final long[] cdStats = new long[2];
        distinct = parallelCountDistinct(fieldKey, targetPathNodeKey, cdStats);
        if (cdStats[0] != cdStats[1]) {
          final Object2LongOpenHashMap<String> groups =
              typedGroupKeyCounts(sourcePath, null, new String[] { field });
          distinct = groups.size() - (groups.containsKey("m") ? 1 : 0);
        }
      }
      final Sequence computed = new Int64(distinct);
      countDistinctResultCache.putIfAbsent(cdKey, computed);
      return computed;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized count-distinct failed: %s",
                               e.getMessage());
    }
  }

  /**
   * Count-distinct via the projection index: reuses
   * {@link ProjectionIndexByteScan#conjunctiveCountByGroup} with an empty
   * predicate set, then returns the size of the resulting group map. The
   * aggregator already de-duplicates the group values with a long-hash
   * intern map, so the map's {@code size()} is the exact distinct count.
   *
   * <p>Returns {@code null} when the projection isn't usable for this
   * field — caller falls back to the generic page-walk kernel.
   */
  private Sequence tryProjectionIndexCountDistinct(final String[] sourcePath, final String field) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup(resourceKey, sourcePath);
    if (handle == null) return null;
    final int groupColumn = handle.columnOf(field);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate: without presence data, rows MISSING the field
    // would contribute the "" default as a phantom distinct value.
    if (!handle.columnSparseClean(groupColumn)) return null;
    final ProjectionIndexScan.ColumnPredicate[] emptyPreds = new ProjectionIndexScan.ColumnPredicate[0];
    // Missing rows produce ZERO items under `return $d` ($d is the empty
    // sequence) — they are counted out, not a distinct value.
    final long[] missing = new long[1];
    final Object2LongOpenHashMap<String> agg;
    try {
      agg = parallelConjunctiveCountByGroup(handle, emptyPreds, groupColumn, missing);
    } catch (final IllegalStateException ise) {
      // Group column kind mismatch (e.g. numeric column — count-distinct on
      // numerics is supported by the generic path but not by the string-dict
      // kernel here).
      return null;
    }
    if (agg == null) return null;
    return new Int64(agg.size());
  }

  /**
   * Dedicated count-distinct kernel. HFT-grade, zero-allocation on the
   * per-page hot path:
   *
   * <ul>
   *   <li>For pages with a {@link io.sirix.page.pax.StringRegion} that
   *       knows about the anchor tag, iterate the per-page dict
   *       directly (3–10 entries typical). The dict is already deduped
   *       by the region build, so we pay one long-hash per distinct
   *       value per page — not per record.</li>
   *   <li>For pages without a region (or where the region has no tag
   *       for our anchor), fall back to a slot-walk + value-read, still
   *       feeding long-hashes into the set.</li>
   * </ul>
   *
   * <p>The per-thread set is a fastutil {@link it.unimi.dsi.fastutil.longs.LongOpenHashSet}
   * of 64-bit {@code xxHash3}s. Collision probability at 10 M distinct
   * values is roughly {@code 10^-19} — two orders of magnitude below
   * hardware error rates. If perfect correctness is required at
   * billion-cardinality scale, swap to a 128-bit hash or a keyed-set
   * variant.
   */
  private long parallelCountDistinct(final int fieldKey, final long targetPathNodeKey,
      final long[] statsOut) throws Exception {
    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final it.unimi.dsi.fastutil.longs.LongOpenHashSet[] perThread =
        new it.unimi.dsi.fastutil.longs.LongOpenHashSet[eff];
    // [idx] = {visited, contributed} — compared by the caller; a gap means
    // non-string values were silently skipped and a typed redo is required.
    final long[] visitedPerThread = new long[eff];
    final long[] contributedPerThread = new long[eff];

    parallel(eff, idx -> {
      long visited = 0L;
      long contributed = 0L;
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final int SLOT_MASK = Constants.NDP_NODE_COUNT - 1;
      final byte[] scratch = new byte[256];
      final it.unimi.dsi.fastutil.longs.LongOpenHashSet local =
          new it.unimi.dsi.fastutil.longs.LongOpenHashSet(16);
      for (long pk = s; pk < e; pk++) {
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        // Fast path: iterate the StringRegion's per-tag dict directly.
        // 3–10 unique values per page — one long-hash per entry, no
        // per-record touch. Matches the same dispatch rules as the
        // groupByCount fast path (path-tagged region if path-scoped).
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
          fastLookupTag = Integer.MIN_VALUE;
        }
        boolean fastPathComplete = false;
        if (sh != null && fastLookupTag != Integer.MIN_VALUE) {
          final int tag = StringRegion.lookupTag(sh, fastLookupTag);
          if (tag >= 0) {
            // Correctness guard mirrors parallelGroupByCount: the region
            // under-counts when a record spans a page boundary (value on
            // P, OBJECT_KEY on P-1). Verify per-tag total against the
            // slot oracle; on mismatch, fall through to slot walk.
            final byte[] payload = kv.getStringRegionPayload();
            final int dictSize = sh.tagStringDictSize[tag];
            final int[] anchorSlots = kv.getObjectKeySlotsForNameKey(fieldKey);
            // sh.tagCount[tag] is the number of VALUES (not distinct)
            // contributing to this tag. If it matches the slot count,
            // every anchor OBJECT_KEY on this page was captured by the
            // region, so every distinct dept value is in the dict.
            if (sh.tagCount[tag] == anchorSlots.length) {
              for (int d = 0; d < dictSize; d++) {
                final int off = StringRegion.decodeStringOffset(payload, sh, tag, d);
                final int len = StringRegion.decodeStringLength(payload, sh, tag, d);
                local.add(fnv1a64(payload, off, len));
              }
              visited += anchorSlots.length;
              contributed += anchorSlots.length;
              fastPathComplete = true;
            }
          }
        }
        if (fastPathComplete) continue;

        // Slow path: walk OBJECT_KEY / fused OBJECT_NAMED_* slots; read each value's bytes.
        // Since legacy OBJECT_*_VALUE kinds are gone, OBJECT_KEY only flags object/array-valued
        // fields (no string leaf), so the only string-leaf fast read is the fused inline path.
        // OBJECT_KEY slots accordingly fall through to the rtx fallback.
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        if (matches.length == 0) continue;
        for (final int slot : matches) {
          final long objectKeyNodeKey = base + slot;
          if (targetPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, objectKeyNodeKey) != targetPathNodeKey) {
            continue;
          }
          visited++;
          final int slotKindId = kv.getSlotNodeKindId(slot);
          byte[] valueBytes = null;
          int valueLen = 0;
          int valueOff = 0;
          if (slotKindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
            final byte[] inline = kv.readFusedObjectNamedStringBytes(slot);
            if (inline != null && inline.length > 0) {
              valueBytes = inline;
              valueLen = inline.length;
            }
          }
          if (valueBytes == null) {
            // OBJECT_KEY (object/array-valued field — leaf is not on this slot) or
            // FSST oversize — defer to rtx for the value read.
            if (!rtx.moveTo(objectKeyNodeKey)) continue;
            if (!rtx.moveToFirstChild()) continue;
            final byte[] rawValue = rtx.getValueBytes();
            if (rawValue == null || rawValue.length == 0) continue;
            valueBytes = rawValue;
            valueLen = rawValue.length;
          }
          contributed++;
          local.add(fnv1a64(valueBytes, valueOff, valueLen));
        }
      }
      perThread[idx] = local;
      visitedPerThread[idx] = visited;
      contributedPerThread[idx] = contributed;
    });

    // Merge per-thread long-hash sets. Union is O(total distinct), no
    // key copies. addAll on LongOpenHashSet bypasses autoboxing.
    final it.unimi.dsi.fastutil.longs.LongOpenHashSet merged =
        new it.unimi.dsi.fastutil.longs.LongOpenHashSet(32);
    for (final var localSet : perThread) {
      if (localSet != null) merged.addAll(localSet);
    }
    if (statsOut != null && statsOut.length >= 2) {
      long v = 0L, c = 0L;
      for (int i = 0; i < eff; i++) {
        v += visitedPerThread[i];
        c += contributedPerThread[i];
      }
      statsOut[0] = v;
      statsOut[1] = c;
    }
    return merged.size();
  }

  /**
   * 64-bit FNV-1a hash over a byte slice. Stable across JVMs; good
   * distribution for short strings. Matches the hash used in
   * {@link io.sirix.page.pax.StringRegion.Encoder} so encoder/decoder
   * share the same key space when needed.
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
      double[] dblStats = aggregateDblCache.get(cacheKey);
      if (stats == null && dblStats == null) {
        // Projection fast path: NUMERIC_LONG column sweep over in-memory leaves
        // (the long-only column excludes non-integral values by construction, so
        // no typed redo can be needed when it answers).
        final long[] projected = tryProjectionAggregate(sourcePath, field, null);
        if (projected != null) {
          stats = aggregateCache.putIfAbsent(cacheKey, projected);
          if (stats == null) stats = projected;
        }
      }
      if (stats == null && dblStats == null) {
        final long targetPathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
        final boolean[] nonIntegral = new boolean[1];
        final long[] fresh = parallelAggregate(field, targetPathNodeKey, nonIntegral);
        // count(for $u return $u.field) counts NON-EMPTY derefs — i.e. every
        // record carrying the field, regardless of the value's type. The
        // numeric accumulator's count only covers numbers (a count over a
        // string field historically returned 0); the visited tally is the
        // type-agnostic answer.
        if ("count".equals(func)) {
          return new Int64(fresh[4]);
        }
        // Value aggregates over a field that EXISTS but never yielded a
        // numeric value (pure string/boolean/null column): the interpreter
        // applies string/error semantics the numeric kernels cannot
        // reproduce — fail LOUDLY instead of fabricating 0/empty.
        if (!nonIntegral[0] && fresh[0] == 0 && fresh[4] > 0) {
          throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                                   "Vectorized %s over field '%s': the field exists but holds no numeric values — "
                                       + "string/boolean/null aggregation is not supported by the vectorized executor",
                                   func,
                                   field);
        }
        if (nonIntegral[0]) {
          // The column carries non-integral numbers — long accumulation TRUNCATES
          // (sum over a half-double column measured 14% short). Redo with double
          // accumulation and emit doubles, like the generic pipeline does.
          final double[] dfresh = parallelAggregateDouble(field, targetPathNodeKey);
          dblStats = aggregateDblCache.putIfAbsent(cacheKey, dfresh);
          if (dblStats == null) dblStats = dfresh;
        } else {
          stats = aggregateCache.putIfAbsent(cacheKey, fresh);
          if (stats == null) stats = fresh;
        }
      }
      if (dblStats != null) {
        final long dCount = (long) dblStats[0];
        final double dSum = dblStats[1];
        final double dMin = dblStats[2];
        final double dMax = dblStats[3];
        return switch (func) {
          case "count" -> new Int64(dCount);
          case "sum" -> new Dbl(dSum);
          case "avg" -> dCount == 0 ? new ItemSequence() : new Dbl(dSum / dCount);
          case "min" -> dCount == 0 ? new ItemSequence() : new Dbl(dMin);
          case "max" -> dCount == 0 ? new ItemSequence() : new Dbl(dMax);
          default -> null;  // unknown func → fall back
        };
      }
      final long count = stats[0];
      final long sum = stats[1];
      final long min = stats[2];
      final long max = stats[3];
      return switch (func) {
        case "count" -> new Int64(count);
        case "sum" -> new Int64(sum);
        // Integer avg is xs:decimal in XQuery — use brackit's own division so the
        // result matches the generic pipeline digit-for-digit (a double here
        // diverged in the ~16th digit whenever count doesn't divide a power of 10).
        // avg/min/max over ZERO contributing rows are the EMPTY sequence, not 0.
        case "avg" -> count == 0 ? new ItemSequence() : new Int64(sum).div(new Int64(count));
        case "min" -> count == 0 ? new ItemSequence() : new Int64(min);
        case "max" -> count == 0 ? new ItemSequence() : new Int64(max);
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
        // Non-numeric stats cannot answer value aggregates — fall back to the
        // scan instead of fabricating 0 (count alone is type-agnostic).
        if (!numeric && !"count".equals(func)) {
          return null;
        }
        return switch (func) {
          case "count" -> new Int64(count);
          case "sum"   -> new Int64(sum);
          // avg/min/max over ZERO values are the EMPTY sequence, not 0.
          case "avg"   -> count == 0 ? new ItemSequence() : new Dbl((double) sum / (double) count);
          case "min"   -> count == 0 ? new ItemSequence() : new Int64(pMin);
          case "max"   -> count == 0 ? new ItemSequence() : new Int64(pMax);
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

      // Fast path: the nameKey is {@code name.hashCode()} — a pure
      // deterministic function of the field string (see
      // {@link io.sirix.utils.NamePageHash#generateHashForString}). When we
      // *know* the field exists in the document we don't need to walk pages
      // to discover its key.
      //
      // When to trust the fast path:
      //   - If a projection index is registered for this executor and the
      //     field is a column of that index, the field definitionally
      //     exists in the document.
      //   - If a PathSummary is available (standard configuration), probe
      //     it via {@code findPathsByLocalName}; a match means the field
      //     exists with a node bearing its hash as nameKey.
      //
      // Otherwise we fall back to the legacy page-walk, which correctly
      // returns -1 for missing fields. At cold 100 M the projection/path-
      // summary fast paths collapse what used to be a 40 s filterCount
      // cold-warmup (triggered by the walk itself) into a sub-ms probe.
      int foundKey = resolveFieldKeyFast(field);
      if (foundKey == Integer.MIN_VALUE) {
        foundKey = resolveFieldKeyByWalk(field);
      }
      fieldKeyCache.put(field, foundKey);
      return foundKey;
    }
  }

  /**
   * Fast path for {@link #resolveFieldKey}: returns {@code rtx.keyForName(field)}
   * when we have proof the field exists in the document (without a scan), or
   * {@link Integer#MIN_VALUE} to signal "couldn't prove — use the walk".
   */
  private int resolveFieldKeyFast(final String field) {
    // Projection-registry probe: if a projection covers this field, it
    // exists.
    final String resourceKey = projectionRegistryKey;
    if (resourceKey != null) {
      // Probe all registered handles for this resource — the sourcePath
      // doesn't matter here; we just need to know if any projection
      // carries the field.
      if (ProjectionIndexRegistry.anyHandleCoversField(resourceKey, field)) {
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          return rtx.keyForName(field);
        }
      }
    }
    // PathSummary probe: the summary is much smaller than the document and
    // enumerates every path. If the field appears there, it exists.
    if (session.getResourceConfig().withPathSummary) {
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
           var summary = rtx.getResourceSession().openPathSummary(revision)) {
        if (summary != null && summary.containsLocalName(field)) {
          return rtx.keyForName(field);
        }
      } catch (final Exception ignored) {
        // Fall through to the walk.
      }
    }
    return Integer.MIN_VALUE;
  }

  /** Legacy full-document walk used when the fast path can't prove existence. */
  private int resolveFieldKeyByWalk(final String field) {
    int[] found = { -1 };
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      var reader = rtx.getStorageEngineReader();
      long totalPages = (getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
      for (long pk = 0; pk < totalPages && found[0] < 0; pk++) {
        var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        kv.forEachPopulatedSlot(slot -> {
          // Both legacy OBJECT_KEY and fused OBJECT_NAMED_* carry a field name — scan both.
          if (!playsObjectKeyRole(kv.getSlotNodeKindId(slot))) return true;
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
    return found[0];
  }

  /**
   * Walk all leaf pages in parallel, find every OBJECT_KEY slot whose nameKey
   * matches the pre-resolved key for {@code field}, descend to its numeric
   * value via the page's slot relationships (skipping {@code moveTo} +
   * {@code Number} boxing), and accumulate.
   *
   * @return {@code [count, sum, min, max]}
   */
  /** {@code true} for numbers whose long truncation loses information. */
  private static boolean isNonIntegralNumber(final Number n) {
    if (n instanceof Double || n instanceof Float) {
      final double d = n.doubleValue();
      return d != Math.rint(d) || Math.abs(d) > (double) Long.MAX_VALUE;
    }
    if (n instanceof java.math.BigDecimal bd) {
      return bd.stripTrailingZeros().scale() > 0;
    }
    return false;
  }

  /**
   * Double-accumulating aggregate walk — the redo path when
   * {@link #parallelAggregate} flags non-integral values. Pure slot walk
   * (regions only carry longs, so they cannot serve this column completely).
   *
   * @return {@code [count, sum, min, max]} as doubles
   */
  private double[] parallelAggregateDouble(final String field, final long targetPathNodeKey) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey == -1) return new double[] { 0, 0, Double.MAX_VALUE, -Double.MAX_VALUE };
    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final double[][] perThread = new double[eff][];
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final double[] acc = { 0, 0, Double.MAX_VALUE, -Double.MAX_VALUE };
      for (long pk = s; pk < e; pk++) {
        final var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        final long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        for (final int slot : matches) {
          final long objectKeyNodeKey = base + slot;
          if (targetPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, objectKeyNodeKey) != targetPathNodeKey) {
            continue;
          }
          if (!rtx.moveTo(objectKeyNodeKey)) continue;
          final NodeKind kind = rtx.getKind();
          final Number n;
          if (kind == NodeKind.OBJECT_NAMED_NUMBER) {
            n = rtx.getNumberValue();
          } else if (rtx.moveToFirstChild() && rtx.getKind() == NodeKind.NUMBER_VALUE) {
            n = rtx.getNumberValue();
          } else {
            continue;
          }
          if (n == null) continue;
          final double v = n.doubleValue();
          acc[0]++;
          acc[1] += v;
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
        }
      }
      perThread[idx] = acc;
    });
    final double[] merged = { 0, 0, Double.MAX_VALUE, -Double.MAX_VALUE };
    for (final double[] a : perThread) {
      if (a == null) continue;
      merged[0] += a[0];
      merged[1] += a[1];
      if (a[2] < merged[2]) merged[2] = a[2];
      if (a[3] > merged[3]) merged[3] = a[3];
    }
    return merged;
  }

  private long[] parallelAggregate(final String field, final long targetPathNodeKey,
      final boolean[] nonIntegralOut) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    // -1 = MISSING sentinel only — negative hashes are legitimate nameKeys.
    if (fieldKey == -1) return new long[] { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0 };

    final long maxNodeKey = getMaxNodeKey();
    long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    int eff = (int) Math.min(threads, Math.max(1, totalPages));
    long ppt = (totalPages + eff - 1) / eff;

    // [count, sum, min, max, visited] — `visited` counts every path-filtered
    // anchor slot regardless of value type: it is the interpreter's
    // count(for $u return $u.field) (a present field derefs to exactly one
    // item — number, string, boolean, null, object or array alike).
    long[][] perThread = new long[eff][5];
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
            // Completeness oracle (mirrors the group-by region paths): the number
            // region only carries LONG-typed values — a double/decimal field value
            // never enters it, so committing a partial tag range would silently
            // drop those records (measured: sum over a half-double column came
            // back 14% short). Commit only when the tag covers every anchor slot.
            if (tag >= 0 && hdr.tagCount[tag] == kv.getObjectKeySlotsForNameKey(fieldKey).length) {
              final int start = hdr.tagStart[tag];
              final int tagN = hdr.tagCount[tag];
              final int end = start + tagN;
              final MemorySegment payloadSeg = kv.getNumberRegionPayloadSegment();
              if (NumberRegionSimd.aggregateRange(payloadSeg, hdr, start, end, simdAggOut)) {
                acc[0] += tagN;
                acc[1] += simdAggOut[0];
                if (simdAggOut[1] < acc[2]) acc[2] = simdAggOut[1];
                if (simdAggOut[2] > acc[3]) acc[3] = simdAggOut[2];
                acc[4] += tagN;
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
              acc[4] += tagN;
              continue;
            }
          }
        }

        // Slow path: walk OBJECT_KEY / fused OBJECT_NAMED_NUMBER slots matching the nameKey.
        // Used when the page has no number region (e.g. first-time commit before Phase-1
        // writer landed) or the target field isn't in the region's parent-nameKey dictionary.
        // Since legacy OBJECT_NUMBER_VALUE is gone, OBJECT_KEY slots only flag object/array-
        // valued fields and route through rtx; fused records carry the value inline.
        final int[] matches = kv.getObjectKeySlotsForNameKey(fieldKey);
        for (final int slot : matches) {
          final long objectKeyNodeKey = base + slot;
          // Path-scope filter: drop matches that share the field name but sit
          // at a different tree depth (e.g. $doc.items[].age vs $doc.age).
          if (targetPathNodeKey != -1L
              && kv.getObjectKeyPathNodeKeyFromSlot(slot, objectKeyNodeKey) != targetPathNodeKey) {
            continue;
          }
          acc[4]++;
          final int slotKindId = kv.getSlotNodeKindId(slot);
          long v;
          if (slotKindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID) {
            v = kv.getFusedObjectNamedNumberValueLongFromSlot(slot);
            if (v == Long.MIN_VALUE) {
              // Payload is float/double/BigDecimal — fall back to full path.
              if (!rtx.moveTo(objectKeyNodeKey)) continue;
              final Number n = rtx.getNumberValue();
              if (n == null) continue;
              if (isNonIntegralNumber(n)) nonIntegralOut[0] = true;
              v = n.longValue();
            }
          } else {
            // OBJECT_KEY (object/array-valued field) — value isn't a primitive on this slot.
            // The aggregate target is a number field; non-fused matches with this nameKey
            // would only contain numbers via descent through the rtx, but with fusion
            // mandatory the only descent that yields a primitive is via the fused path.
            // Defer to rtx for safety.
            if (!rtx.moveTo(objectKeyNodeKey)) continue;
            if (!rtx.moveToFirstChild()) continue;
            final Number n = rtx.getNumberValue();
            if (n == null) continue;
            if (isNonIntegralNumber(n)) nonIntegralOut[0] = true;
            v = n.longValue();
          }
          acc[0]++;
          acc[1] += v;
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
        }
      }
    });

    long count = 0, sum = 0, min = Long.MAX_VALUE, max = Long.MIN_VALUE, visited = 0;
    for (long[] a : perThread) {
      count += a[0];
      sum += a[1];
      visited += a[4];
      if (a[0] > 0) {
        if (a[2] < min) min = a[2];
        if (a[3] > max) max = a[3];
      }
    }
    return new long[] { count, sum, min, max, visited };
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
   * Double-space comparison via {@link Double#compare} — NOT raw operators:
   * the interpreter's {@code Dbl#cmp}/{@code Int64#cmp} use
   * {@code Double.compare}, whose total order distinguishes {@code -0.0 < 0.0}
   * (raw {@code <}/{@code >} treat them equal). Document doubles come from
   * JSON and are never NaN; literals are detection-gated finite.
   */
  private static boolean scalarEvalDbl(final double v, final int op, final double t) {
    return scalarEvalCmp(Double.compare(v, t), op);
  }

  /** Apply {@code op} to a three-way comparison result. */
  private static boolean scalarEvalCmp(final int c, final int op) {
    return switch (op) {
      case OP_GT -> c > 0;
      case OP_LT -> c < 0;
      case OP_GE -> c >= 0;
      case OP_LE -> c <= 0;
      case OP_EQ -> c == 0;
      default -> true;
    };
  }

  /**
   * Parallel group-by-count: walks all leaf pages, finds OBJECT_KEY slots whose
   * nameKey matches the pre-resolved key, descends to value, accumulates counts
   * per unique value into a 1BRC-style open-addressing byte-key hash map
   * (per-thread, then merged). Returns a flat Sequence of
   * {@code {"groupField": value, "count": N}} record objects (the FLWOR's
   * envelope — see the VectorizedExecutor RESULT ENVELOPE CONTRACT).
   */
  private Sequence parallelGroupByCount(final String groupField, final long targetPathNodeKey,
      final long[] statsOut) throws Exception {
    final int fieldKey = resolveFieldKey(groupField);
    // -1 is the MISSING sentinel; nameKeys are String hashes and may legitimately
    // be negative ('active'.hashCode() < 0) — `< 0` here silently emptied every
    // group-by over a negative-hash field.
    if (fieldKey == -1) return new ItemSequence();

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
    // Records this scan was RESPONSIBLE for (path-filtered anchor slots). The caller
    // compares against the grouped total — a visited record that contributed nothing
    // (non-string / null / complex value) means this string kernel must be redone by
    // the typed kernel instead of silently dropping the record.
    final long[] visitedPerThread = new long[eff];

    parallel(eff, i -> {
      ScanResult.GroupByResult acc = perThread[i];
      long visited = 0L;
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

        // Diagnostic kill switch for the StringRegion fast path. Setting
        // {@code -Dsirix.scan.stringRegion.off=true} bypasses the fast
        // path entirely — we use this to localize correctness discrepancies
        // (the unfiltered groupBy has undercounted records vs DuckDB; see
        // task #65). Keep it cheap by evaluating once per page, branch-
        // predictable; JIT folds it away when the system property is unset.
        final boolean stringRegionFastOff = STRING_REGION_FAST_OFF;
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
        final StringRegion.Header sh = stringRegionFastOff ? null : kv.getStringRegionHeader();
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
              // Correctness guard: the StringRegion fast path groups VALUES
              // by their parent OBJECT_KEY's nameKey. When a record spans a
              // page boundary (OBJECT_KEY on page P-1, VALUE on page P), the
              // value's parent lives cross-page and the region build tags it
              // with a sentinel (-1) instead of the actual nameKey. The
              // canonical-nameKey tag on P then under-counts by exactly the
              // number of cross-page values, and the canonical-nameKey tag
              // on P-1 under-counts by the number of OBJECT_KEYs whose VALUE
              // spills to P. Query at groupByDept(dept) on 10M records would
              // miss ~0.1% — pre-existing bug documented in task #65.
              //
              // Fix: compare the fast path's per-tag total against the slot
              // count returned by getObjectKeySlotsForNameKey (the slow-path
              // oracle). If they agree, the fast path captures every record
              // that a complete scan would — commit its counts. If they
              // disagree, fall through to the slow path, which walks each
              // OBJECT_KEY and handles cross-page children via rtx-fallback.
              //
              // Cost: one extra slot-lookup per page. The lookup is cached
              // per (page, nameKey), so the subsequent slow-path call reuses
              // the same int[] — no second SIMD scan, no re-allocation.
              final int[] anchorSlots = kv.getObjectKeySlotsForNameKey(fieldKey);
              // Reset only what we'll touch.
              for (int d = 0; d < dictSize; d++) localDictCounts[d] = 0;
              final int start = sh.tagStart[tag];
              final int n = sh.tagCount[tag];
              // Bulk-count with one-entry word cache inside StringRegion —
              // amortises the 64-bit payload read across consecutive dict-ids
              // that share an 8-byte window (at bw=3 for 8-value dept fields
              // each word covers ~21 dict-ids, so one read serves 21 counts).
              StringRegion.countDictIds(payload, sh, start, n, localDictCounts);
              long fastPathTotal = 0L;
              for (int d = 0; d < dictSize; d++) fastPathTotal += localDictCounts[d];
              if (fastPathTotal != (long) anchorSlots.length) {
                // Mismatch → fast path is incomplete for this page (likely
                // a page-boundary record). Fall through to slow path, which
                // walks OBJECT_KEY slots and handles cross-page children.
                if (DIAGNOSTICS_ENABLED) diag.stringRegionMissesDictBig++;
              } else {
                if (DIAGNOSTICS_ENABLED) diag.stringRegionHits++;
                visited += fastPathTotal;
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
        }

        // Slow path: walk OBJECT_KEY / fused OBJECT_NAMED_STRING slots, hash-insert
        // each string value. Since legacy OBJECT_STRING_VALUE is gone, OBJECT_KEY only
        // flags object/array-valued fields (no string leaf), so the fast inline path is
        // limited to fused records and OBJECT_KEY slots route through rtx.
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
          visited++;
          final int slotKindId = kv.getSlotNodeKindId(slot);
          if (slotKindId == KeyValueLeafPage.FUSED_OBJECT_NAMED_STRING_KIND_ID) {
            final byte[] inline = kv.readFusedObjectNamedStringBytes(slot);
            if (inline != null && inline.length > 0) {
              acc.add(inline, 0, inline.length);
              if (DIAGNOSTICS_ENABLED) {
                diag.groupByDirectSuccess++;
                diag.recordsMatched++;
              }
              continue;
            }
            if (DIAGNOSTICS_ENABLED) diag.groupByDirectNeg++;
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
      visitedPerThread[i] = visited;
    });

    // Merge per-thread maps into one
    ScanResult.GroupByResult merged = perThread[0];
    for (int i = 1; i < eff; i++) merged.merge(perThread[i]);

    if (DIAGNOSTICS_ENABLED && perThreadDiag != null) {
      final ScanDiagnostics mergedDiag = new ScanDiagnostics();
      for (final ScanDiagnostics d : perThreadDiag) if (d != null) mergedDiag.mergeFrom(d);
      mergedDiag.print("groupByCount(" + groupField + ")");
    }

    // Build Brackit result: flat sequence of {groupField: value, "count": N} records
    List<Item> items = new ArrayList<>(merged.size());
    final QNm groupFieldQNm = new QNm(groupField);
    final QNm countQNm = new QNm("count");
    final long[] countedHolder = new long[1];
    merged.forEach((key, count) -> {
      countedHolder[0] += count;
      QNm[] fields = { groupFieldQNm, countQNm };
      Sequence[] values = { new Str(new String(key, StandardCharsets.UTF_8)), new Int64(count) };
      items.add(new ArrayObject(fields, values));
    });
    if (statsOut != null && statsOut.length >= 2) {
      long totalVisited = 0L;
      for (final long v : visitedPerThread) totalVisited += v;
      statsOut[0] = totalVisited;
      statsOut[1] = countedHolder[0];
    }
    return new ItemSequence(items.toArray(new Item[0]));
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
  private static final ClassDesc CD_DOUBLE_2D = ClassDesc.ofDescriptor("[[D");
  private static final ClassDesc CD_BYTE_2D = ClassDesc.ofDescriptor("[[B");
  private static final ClassDesc CD_BOOL_2D = ClassDesc.ofDescriptor("[[Z");
  private static final ClassDesc CD_LONG_ARR = ClassDesc.ofDescriptor("[J");
  private static final ClassDesc CD_BYTE_ARR = ClassDesc.ofDescriptor("[B");
  private static final ClassDesc CD_BOOL_ARR = ClassDesc.ofDescriptor("[Z");
  private static final ClassDesc CD_BOXED_DOUBLE = ClassDesc.ofDescriptor("Ljava/lang/Double;");
  private static final MethodTypeDesc MTD_DOUBLE_COMPARE =
      MethodTypeDesc.of(ClassDesc.ofDescriptor("I"), ClassDesc.ofDescriptor("D"), ClassDesc.ofDescriptor("D"));

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
  private static final int SLOT_DBL_COLS = 19; // double[][] hoisted batch.dblCols
  private static final int SLOT_DVALS = 20;    // double[] current field's double column
  private static final int SLOT_KIND = 21;     // int — presentKind[fieldIdx][i] scratch

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
    code.getfield(CD_EVAL_BATCH, "dblCols", CD_DOUBLE_2D);
    code.astore(SLOT_DBL_COLS);

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
      case CompiledPredicate.OP_FP_CMP ->
          emitFpCmp(code, cp.fieldIdx[nodeIdx], cp.cmpOp[nodeIdx], cp.dblLit[nodeIdx]);
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
   *   final double[] dvals = dblCols[fieldIdx];
   *   for (int i = 0; i < size; i++) {
   *     final byte k = pk[i];
   *     if (k == 1) { if (vals[i] {cmp} lit) setBit; }
   *     else if (k == 4) { if (Double.compare(dvals[i], (double) lit) {cmp} 0) setBit; }
   *   }
   * </pre>
   * Long rows compare exactly; double rows promote the integer literal to
   * double via {@link Double#compare} — the interpreter's numeric promotion
   * (mixed int/double columns, e.g. rating 3 vs 3.7). Comparison is
   * constant-folded in the emitted bytecode via the opcode selection below.
   */
  private static void emitNumCmp(final CodeBuilder code, final int fieldIdx, final int cmpOp, final long lit) {
    emitNumericKernel(code, fieldIdx, cmpOp, lit, (double) lit, false);
  }

  /**
   * Floating-point comparison kernel — both row kinds evaluate in double
   * space via {@link Double#compare} (the {@link PredicateNode.FpCmp}
   * semantics contract): long rows are promoted with {@code l2d}, double
   * rows compare directly.
   */
  private static void emitFpCmp(final CodeBuilder code, final int fieldIdx, final int cmpOp, final double dLit) {
    emitNumericKernel(code, fieldIdx, cmpOp, 0L, dLit, true);
  }

  /**
   * Shared numeric kernel emitter. {@code fp == false}: long rows compare in
   * long space against {@code lit}, double rows against {@code (double) lit}.
   * {@code fp == true}: long rows are converted with {@code l2d} and both
   * kinds compare against {@code dLit} via {@link Double#compare}.
   */
  private static void emitNumericKernel(final CodeBuilder code, final int fieldIdx, final int cmpOp,
      final long lit, final double dLit, final boolean fp) {
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
    code.aload(SLOT_DBL_COLS);
    loadIntConst(code, fieldIdx);
    code.aaload();
    code.astore(SLOT_DVALS);

    final Label cond = code.newLabel();
    final Label top = code.newLabel();
    final Label skip = code.newLabel();
    final Label longArm = code.newLabel();
    final Label dblArm = code.newLabel();
    final Label setBit = code.newLabel();
    code.iconst_0();
    code.istore(SLOT_I);
    code.goto_(cond);
    code.labelBinding(top);
    // k = pk[i]
    code.aload(SLOT_PK);
    code.iload(SLOT_I);
    code.baload();
    code.istore(SLOT_KIND);
    code.iload(SLOT_KIND);
    code.iconst_1();
    code.if_icmpeq(longArm);
    code.iload(SLOT_KIND);
    code.iconst_4();
    code.if_icmpeq(dblArm);
    code.goto_(skip);

    // ---- long arm ----
    code.labelBinding(longArm);
    code.aload(SLOT_VALS);
    code.iload(SLOT_I);
    code.laload();
    if (fp) {
      // (double) vals[i] vs dLit via Double.compare — interpreter promotion.
      code.l2d();
      code.ldc(dLit);
      code.invokestatic(CD_BOXED_DOUBLE, "compare", MTD_DOUBLE_COMPARE);
    } else {
      code.ldc(lit);
      code.lcmp();
    }
    // int comparison result vs 0 — skip when the op does NOT hold:
    //   GT: skip if cmp <= 0; LT: skip if cmp >= 0; GE: skip if cmp < 0;
    //   LE: skip if cmp > 0; EQ: skip if cmp != 0.
    emitCmpBranch(code, cmpOp, skip);
    code.goto_(setBit);

    // ---- double arm ----
    code.labelBinding(dblArm);
    code.aload(SLOT_DVALS);
    code.iload(SLOT_I);
    code.daload();
    code.ldc(dLit);
    code.invokestatic(CD_BOXED_DOUBLE, "compare", MTD_DOUBLE_COMPARE);
    emitCmpBranch(code, cmpOp, skip);

    code.labelBinding(setBit);
    // bm[i>>>6] |= 1L << (i & 63)
    emitSetBit(code);
    code.labelBinding(skip);
    code.iinc(SLOT_I, 1);
    code.labelBinding(cond);
    code.iload(SLOT_I);
    code.iload(SLOT_SIZE);
    code.if_icmplt(top);
  }

  /** Branch to {@code skip} when the int comparison result on the stack does NOT satisfy {@code cmpOp}. */
  private static void emitCmpBranch(final CodeBuilder code, final int cmpOp, final Label skip) {
    switch (cmpOp) {
      case OP_GT -> code.ifle(skip);
      case OP_LT -> code.ifge(skip);
      case OP_GE -> code.iflt(skip);
      case OP_LE -> code.ifgt(skip);
      case OP_EQ -> code.ifne(skip);
      default -> throw new IllegalStateException("bad cmpOp " + cmpOp);
    }
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
