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
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.util.simd.VectorOps;
import io.brackit.query.util.simd.VectorizedPredicate;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.index.pageskip.PageSkipRegistry;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionDoubleEncoding;
import io.sirix.index.projection.ProjectionIndexLeafPage;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexScan;
import io.sirix.index.projection.ProjectionSegmentFoldScan;
import io.sirix.index.path.summary.PathNode;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberRegionSimd;
import io.sirix.page.pax.StringRegion;
import io.sirix.settings.Constants;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;

import jdk.incubator.vector.VectorOperators;

import org.jspecify.annotations.Nullable;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

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
   * Wtx-visible serving mode: when non-null, the executor is bound to an
   * OPEN write transaction and serves queries from its uncommitted state.
   * Only the projection fast paths run in this mode — they read through the
   * transaction log via {@code IndexController#openProjectionIndex} (which
   * applies pending incremental maintenance first, read-your-writes) — and
   * every miss returns {@code null} so the caller's generic pipeline (which
   * reads the same transaction) answers. Result caches and the generic
   * kernels are committed-revision scoped and are bypassed entirely.
   */
  private final @Nullable JsonNodeTrx wtx;

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
      !"false".equalsIgnoreCase(System.getProperty("sirix.vec.compiledPredicate", "true"))
          // Runtime class definition (MethodHandles.Lookup.defineHiddenClass) is
          // unavailable in a GraalVM native image — every compileToClass attempt
          // throws UnsupportedFeatureError and falls back to the interpreter. Default
          // the feature off in a native image so we skip the doomed classfile build +
          // throw/catch on the first call of each distinct predicate (and the noisy
          // multi-line stderr dump). Results are identical either way; the interpreter
          // path is the same one the native fallback would use. Override with an
          // explicit -Dsirix.vec.compiledPredicate=true if a future builder gains
          // runtime-codegen support.
          && !(System.getProperty("sirix.vec.compiledPredicate") == null && inNativeImage());

  /**
   * Whether we are running inside a GraalVM native image. The
   * {@code org.graalvm.nativeimage.imagecode} system property is set to
   * {@code "runtime"} inside the image and unset on HotSpot; checking presence
   * avoids a compile dependency on the GraalVM SDK's {@code ImageInfo}.
   */
  private static boolean inNativeImage() {
    return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
  }

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
  private final ConcurrentHashMap<String, MixedAgg> aggregateDblCache = new ConcurrentHashMap<>();

  /**
   * Served double-column aggregate results (§11-8), keyed {@code func + '#' + cacheKey}
   * (per-func — each function serves a different Sequence). Probed BEFORE
   * {@link #aggregateDblCache} so a served answer is stable across repeats and can never
   * be shadowed by a MixedAgg cached while the projection was still hydrating.
   */
  private final ConcurrentHashMap<String, Sequence> servedDoubleAggCache = new ConcurrentHashMap<>();

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
   * Full {@code [count, sum, min, max]} stats per (path, field, predicate) — keyed WITHOUT
   * the aggregate function, so {@code min}+{@code max}+{@code avg} over the same shape in
   * one query (or across queries) share ONE kernel scan (P5b stage 6). Only successful
   * servings are cached; a decline is never negative-cached (it can be transient).
   */
  private final ConcurrentHashMap<String, long[]> predicateStatsCache = new ConcurrentHashMap<>();

  /** Kernel scans performed by the predicated long-aggregate path — test observability. */
  private static final LongAdder PREDICATED_AGG_SCANS = new LongAdder();

  /** Total predicated long-aggregate kernel scans (stats-cache misses) so far. */
  public static long predicatedAggScanCount() {
    return PREDICATED_AGG_SCANS.sum();
  }

  /** Per-group aggregate servings (stage 7a) — test observability for served-vs-fallback. */
  private static final LongAdder GROUP_AGG_SERVED = new LongAdder();

  /** Total per-group aggregate queries SERVED from a projection so far. */
  public static long groupAggServedCount() {
    return GROUP_AGG_SERVED.sum();
  }

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
    this(session, revision, threads, null);
  }

  /**
   * Wtx-visible executor: serves projection-backed analytics from the given
   * OPEN write transaction's uncommitted state (see {@link #wtx}). The
   * transaction's contract is that each intermediate commit REPLACES its
   * storage engine with one bound to the successor revision (and rebinds its
   * index controller/listeners); this executor honors that by resolving the
   * writer and controller through the transaction facade PER CALL — it
   * follows the transaction across intermediate commits and reverts for as
   * long as the transaction stays open, no re-construction needed.
   */
  public SirixVectorizedExecutor(final JsonNodeTrx wtx, final int threads) {
    this(wtx.getResourceSession(), wtx.getRevisionNumber(), threads, wtx);
  }

  private SirixVectorizedExecutor(JsonResourceSession session, int revision, int threads,
      final @Nullable JsonNodeTrx wtx) {
    this.session = session;
    this.revision = revision;
    this.threads = threads;
    this.wtx = wtx;
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
    // Sorted-scan record trx (stage 7b): executor-owned, one per lifetime.
    try {
      final JsonNodeReadOnlyTrx trx = recordTrx;
      if (trx != null && !trx.isClosed()) {
        trx.close();
      }
    } catch (Exception ignored) {
    }
    // Sirix's session manages the per-thread shared trx pool via
    // getOrCreateSharedReadOnlyTrx; closeSharedReadOnlyTrxs releases all
    // entries for our revision. Bounded count = workerThreads + 1. Wtx mode
    // never opened shared read-only trxes (its revision is uncommitted).
    try {
      if (wtx == null) {
        session.closeSharedReadOnlyTrxs(revision);
      }
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

  /**
   * Confine serving to the resource this executor is bound to. Brackit's optimizer lifts each
   * vectorizable scan's source identity into a {@link SourceRef}; this executor is bound at
   * construction to a single {@code (session, revision)}, so answering a scan over a <em>different</em>
   * document from its own projection/columns would return the wrong resource's data. Fails <b>closed</b>:
   * only a document scan that provably matches the bound {@code (database, resource, revision)} — or the
   * request's own context item — may serve; a mismatched resource, a mismatched (or bare-latest against
   * a non-latest binding) revision, an unprovable {@link SourceRef.Kind#UNKNOWN} source, or any resolution
   * failure declines, and the translator builds the generic (always-correct) pipeline instead.
   *
   * <p>A decline only ever costs the fast path, never correctness.
   */
  @Override
  public boolean acceptsSource(final SourceRef source) {
    if (source == null) {
      return false;
    }
    return switch (source.kind()) {
      // The caller's own bound read transaction — the executor's own (session, revision).
      case CONTEXT_ITEM -> true;
      case DOCUMENT -> acceptsDocument(source);
      // Dynamic jn:doc, collection / multi-revision opener, unresolved variable, non-document source.
      case UNKNOWN -> false;
    };
  }

  /** Whether a concrete {@code jn:doc}/{@code jn:open} source matches this executor's bound resource. */
  private boolean acceptsDocument(final SourceRef source) {
    try {
      final Path resourcePath = session.getResourceConfig().getResource();
      final String resourceName = resourcePath.getFileName().toString();
      // Layout is <database>/data/<resource>; the database directory is two parents up.
      final String databaseName = resourcePath.getParent().getParent().getFileName().toString();
      if (!databaseName.equals(source.databaseName()) || !resourceName.equals(source.resourceName())) {
        return false;
      }
      if (source.opensLatestRevision()) {
        // A bare jn:doc opens the most-recent revision, so a pinned executor may serve it only when it
        // is itself bound to that most-recent revision (else it would answer with stale data).
        return revision == session.getMostRecentRevisionNumber();
      }
      return source.revision() == revision;
    } catch (final RuntimeException e) {
      // Never let the fast-path guard throw — an unresolvable binding simply declines (stays correct).
      return false;
    }
  }

  @Override
  public Sequence executeGroupByCount(QueryContext ctx, String[] sourcePath, String groupField) throws QueryException {
    try {
      if (wtx != null) {
        // Wtx mode: projection-only, no result caches (uncommitted state is
        // mutable); a miss returns null and the interpreter — which reads
        // the same transaction — answers.
        return tryProjectionIndexGroupByCountOnly(sourcePath, groupField);
      }
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
    final ProjectionIndexRegistry.Handle handle =
        lookupProjection(sourcePath, new String[] { groupField });
    if (handle == null) return null;
    final int groupColumn = handle.columnOf(groupField);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate: the group column must carry presence data in
    // every leaf and never hold unrepresentable values (null/object/array/
    // kind mismatch) — otherwise missing-vs-default and null-vs-missing are
    // indistinguishable in the columnar layout. Fail closed to the scan path.
    if (!handle.columnSparseClean(groupColumn, columnFetcher(), leafMaterializer(handle))) {
      return null;
    }
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
    // Wtx mode serves unpredicated projection queries only — predicate
    // compilation resolves name keys against committed-revision state.
    if (wtx != null) return null;
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
    // Wtx mode serves unpredicated projection queries only — see
    // executePredicateCount.
    if (wtx != null) return null;
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
      if (wtx != null) {
        // Wtx mode: unpredicated composite group-by via the projection only
        // (predicate compilation is committed-revision scoped); no result
        // caches. Null → the interpreter reads the same transaction.
        if (predicate != null) {
          return null;
        }
        final Object2LongOpenHashMap<String> wtxProjected =
            tryProjectionMultiGroupCounts(sourcePath, groupFields, null);
        return wtxProjected == null ? null : buildTypedGroupRecords(wtxProjected, outNames, countName);
      }
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
    // Existence probe first: resources without any projection (the common
    // case) must not pay predicate compilation for a lookup that cannot hit.
    if (!anyProjectionAvailable()) return null;
    // Compile the predicate BEFORE handle selection so the covering lookup
    // can pick among several installed projections by the full field set
    // (aggregate column + predicate columns).
    final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
    final String[] required = requiredFields(new String[] { field }, cp);
    final ProjectionIndexRegistry.Handle handle = lookupProjection(sourcePath, required);
    if (handle == null) return null;
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    // Session-bound sources built from THIS executor's own live session, threaded into the
    // shared handle's fill calls (no session-scoped state on the cached handle).
    final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
    final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
    final int col = handle.columnOf(field);
    if (col < 0) return null;
    // One kind-check path for both handle tiers; columnKindOf never materializes a
    // column-lazy handle (descriptor truth).
    if (handle.leafCount() == 0) return null;
    if (handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) return null;
    // Value-exact gate: the builder truncates non-integral numbers into the
    // NUMERIC_LONG column (Number#longValue). Serve aggregates only when the
    // column is PROVABLY integral; unknown provenance falls back. On a lazy
    // handle these gates read the column's OWN slices — no materialization.
    if (!handle.numericColumnIsIntegral(col, fetcher)) {
      return null;
    }
    // Sparse-evidence gate: without per-row presence, MISSING rows would fold
    // their phantom default 0 into count/min/max (sum survives by luck).
    if (!handle.columnSparseClean(col, fetcher, materializer)) {
      return null;
    }
    final ProjectionIndexScan.ColumnPredicate[] preds;
    if (cp == null) {
      preds = new ProjectionIndexScan.ColumnPredicate[0];
    } else {
      if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
      final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
      if (extracted == null) {
        // Not a pure conjunction — AND/OR trees serve through the fold kernels' tree
        // path (P5b stage 6); anything else (NOT, unsupported leaves) falls back.
        return tryTreeAggregate(cp, handle, store, col, fetcher);
      }
      preds = fuseRangePredicates(extracted);
    }
    if (store != null && predsSliceable(store, preds)) {
      try {
        return sliceAggregateParallel(store, preds, col, fetcher);
      } catch (final IllegalStateException ise) {
        // Corrupt/missing slices — fall through to the eager path, which re-surfaces
        // the condition through the established fail-soft flow.
      }
    }
    final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
    if (leafPayloads == null) {
      return null;
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
   * Resolved serving context for one double-column aggregate query
   * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6, §11-8): the covering handle, the
   * column, the compiled conjunctive predicates, and whether the pure-double-source gate
   * held. Resolved ONCE per query so no gate (and in particular no purity probe or leaf
   * scan) runs twice, and none runs at all for a query the gates reject.
   */
  private record DoubleAggServing(ProjectionIndexRegistry.Handle handle, int col,
      ProjectionIndexScan.ColumnPredicate[] preds, ProjectionColumnStore sliceStore,
      boolean pure, ProjectionColumnStore.SegmentFetcher fetcher,
      Supplier<List<byte[]>> materializer) {
    List<byte[]> leafPayloads() {
      return handle.leafPayloads(materializer);
    }
  }

  /** Pre-fill every needed column on the calling thread (one I/O batch each, outside the fan-out). */
  private static void prefillColumns(final ProjectionColumnStore store,
      final ProjectionIndexScan.ColumnPredicate[] preds, final int aggColOrNegative,
      final ProjectionColumnStore.SegmentFetcher fetcher) {
    for (final ProjectionIndexScan.ColumnPredicate p : preds) {
      store.column(p.column, fetcher);
    }
    if (aggColOrNegative >= 0) {
      store.column(aggColOrNegative, fetcher);
    }
  }

  /** Chunked parallel slice count — mirrors {@link #parallelConjunctiveCount}'s dispatch shape. */
  private long sliceCountParallel(final ProjectionColumnStore store,
      final ProjectionIndexScan.ColumnPredicate[] preds,
      final ProjectionColumnStore.SegmentFetcher fetcher) {
    final int leafCount = store.leafCount();
    // Fold-during-decode first (P5b stage 4): counts stream straight from the verified
    // segment bytes — no slice arrays. ALP/reserved width escapes route to the slice path.
    // eligible() pre-fills the involved columns on the calling thread through this reader's
    // own fetcher, so the parallel ranged calls below hit cached bytes.
    if (ProjectionSegmentFoldScan.eligible(store, preds, -1, fetcher)) {
      if (leafCount < 64) {
        return ProjectionSegmentFoldScan.conjunctiveCount(store, preds, fetcher);
      }
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final long[] perThread = new long[eff];
      final int chunkSize = (leafCount + eff - 1) / eff;
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        perThread[idx] = ProjectionSegmentFoldScan.conjunctiveCount(store, preds, from, to, fetcher);
      });
      long total = 0;
      for (final long t : perThread) {
        total += t;
      }
      return total;
    }
    prefillColumns(store, preds, -1, fetcher);
    if (leafCount < 64) {
      return ProjectionColumnScan.conjunctiveCount(store, preds, fetcher);
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    final long[] perThread = new long[eff];
    final int chunkSize = (leafCount + eff - 1) / eff;
    parallel(eff, idx -> {
      final int from = idx * chunkSize;
      final int to = Math.min(from + chunkSize, leafCount);
      if (from >= to) return;
      perThread[idx] = ProjectionColumnScan.conjunctiveCount(store, preds, from, to, fetcher);
    });
    long total = 0;
    for (final long t : perThread) {
      total += t;
    }
    return total;
  }

  /** Chunked parallel slice long-aggregate; exact integer merges. */
  private long[] sliceAggregateParallel(final ProjectionColumnStore store,
      final ProjectionIndexScan.ColumnPredicate[] preds, final int col,
      final ProjectionColumnStore.SegmentFetcher fetcher) {
    final int leafCount = store.leafCount();
    // Fold-during-decode first (P5b stage 4) — same merge shape, byte substrate.
    if (ProjectionSegmentFoldScan.eligible(store, preds, col, fetcher)) {
      if (leafCount < 64) {
        final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(store, preds, col, acc, fetcher);
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
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(store, preds, col, acc, from, to,
            fetcher);
        perThread[idx] = acc;
      });
      return mergeLongAgg(perThread);
    }
    prefillColumns(store, preds, col, fetcher);
    if (leafCount < 64) {
      final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
      ProjectionColumnScan.conjunctiveAggregateNumeric(store, preds, col, acc, fetcher);
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
      ProjectionColumnScan.conjunctiveAggregateNumeric(store, preds, col, acc, from, to, fetcher);
      perThread[idx] = acc;
    });
    return mergeLongAgg(perThread);
  }

  /** Exact integer merge of per-thread {@code [count, sum, min, max]} accumulators. */
  private static long[] mergeLongAgg(final long[][] perThread) {
    final long[] merged = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
    for (final long[] a : perThread) {
      if (a == null || a[0] == 0) continue;
      merged[0] += a[0];
      merged[1] += a[1];
      if (a[2] < merged[2]) merged[2] = a[2];
      if (a[3] > merged[3]) merged[3] = a[3];
    }
    return merged;
  }

  /** Chunked parallel slice double stats (count/min/max order-insensitive; sum diagnostic). */
  private double[] sliceDoubleStatsParallel(final ProjectionColumnStore store,
      final ProjectionIndexScan.ColumnPredicate[] preds, final int col,
      final ProjectionColumnStore.SegmentFetcher fetcher) {
    final int leafCount = store.leafCount();
    prefillColumns(store, preds, col, fetcher);
    if (leafCount < 64) {
      final double[] acc = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
      ProjectionColumnScan.conjunctiveAggregateNumericDouble(store, preds, col, acc, fetcher);
      return acc;
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    final double[][] perThread = new double[eff][];
    final int chunkSize = (leafCount + eff - 1) / eff;
    parallel(eff, idx -> {
      final int from = idx * chunkSize;
      final int to = Math.min(from + chunkSize, leafCount);
      if (from >= to) return;
      final double[] acc = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
      ProjectionColumnScan.conjunctiveAggregateNumericDouble(store, preds, col, acc, from, to,
          fetcher);
      perThread[idx] = acc;
    });
    final double[] merged = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
    for (final double[] a : perThread) {
      if (a == null || a[0] == 0) continue;
      merged[0] += a[0];
      merged[1] += a[1];
      if (Double.compare(a[2], merged[2]) < 0) merged[2] = a[2];
      if (Double.compare(a[3], merged[3]) > 0) merged[3] = a[3];
    }
    return merged;
  }

  /**
   * Materialized whole leaves, or {@code null} when a lazy handle's materializer fails
   * (dead-session window before the catalog's next rebind, truncated/corrupt store) —
   * callers decline serving and the generic pipeline answers.
   */
  private List<byte[]> leafPayloadsOrNull(final ProjectionIndexRegistry.Handle handle) {
    try {
      return handle.leafPayloads(leafMaterializer(handle));
    } catch (final IllegalStateException materializeFailed) {
      return null;
    }
  }

  /** All predicate columns servable from slices (numeric/boolean, no string literals). */
  private static boolean predsSliceable(final ProjectionColumnStore store,
      final ProjectionIndexScan.ColumnPredicate[] preds) {
    for (final ProjectionIndexScan.ColumnPredicate p : preds) {
      if (p.stringLitBytes != null || !store.columnSliceable(p.column)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gate chain for double-column aggregate serving. {@code needsPurity} is true for
   * sum/avg/min/max (value serving requires the §11-8 pure-double-source proof; the check
   * runs BEFORE any leaf work so impure columns decline at metadata cost) and false for
   * count (exact regardless of purity — the probe is skipped entirely, keeping wtx-mode
   * counts free of a per-query purity walk). Returns {@code null} to fall back.
   */
  private DoubleAggServing resolveDoubleAggServing(final String[] sourcePath, final String field,
      final PredicateNode predicateOrNull, final boolean needsPurity) {
    final String resourceKey = projectionRegistryKey;
    if (resourceKey == null) return null;
    if (!anyProjectionAvailable()) return null;
    final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
    final String[] required = requiredFields(new String[] { field }, cp);
    final ProjectionIndexRegistry.Handle handle = lookupProjection(sourcePath, required);
    if (handle == null) return null;
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    final int col = handle.columnOf(field);
    if (col < 0) return null;
    final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
    final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
    if (handle.leafCount() == 0) return null;
    if (handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE) return null;
    if (!handle.numericColumnIsIntegral(col, fetcher)) {
      return null; // not provably value-exact — fail closed
    }
    if (!handle.columnSparseClean(col, fetcher, materializer)) {
      return null;
    }
    final boolean pure = needsPurity && handle.doubleColumnPureSource(col, fetcher);
    if (needsPurity && !pure) {
      return null;
    }
    final ProjectionIndexScan.ColumnPredicate[] preds;
    if (cp == null) {
      preds = new ProjectionIndexScan.ColumnPredicate[0];
    } else {
      if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
      final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
      if (extracted == null) return null;
      preds = fuseRangePredicates(extracted);
    }
    final ProjectionColumnStore sliceStore =
        store != null && predsSliceable(store, preds) ? store : null;
    return new DoubleAggServing(handle, col, preds, sliceStore, pure, fetcher, materializer);
  }

  /**
   * Parallel kernel stats {@code [count, sum, min, max]} for a resolved double-column
   * serving. count is exact; min/max use {@code Double.compare} total order in the kernel
   * (identical winner to the interpreter's {@code MinMaxAggregator}) and are
   * merge-order-insensitive; the kernel {@code sum} is NOT served (association order is
   * not the interpreter's — served sums go through {@link #serveDoubleSumAvg}).
   */
  private double[] doubleKernelStats(final DoubleAggServing s) {
    if (s.sliceStore() != null) {
      try {
        return sliceDoubleStatsParallel(s.sliceStore(), s.preds(), s.col(), s.fetcher());
      } catch (final IllegalStateException ise) {
        // Corrupt/missing slices — fall through to the eager kernels (same fallback the
        // long path gets), not the row-at-a-time interpreter.
      }
    }
    final List<byte[]> leafPayloads = s.leafPayloads();
    final int leafCount = leafPayloads.size();
    if (leafCount < 64) {
      final double[] acc = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
      ProjectionIndexByteScan.conjunctiveAggregateNumericDouble(leafPayloads, s.preds(), s.col(), acc);
      return acc;
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    final double[][] perThread = new double[eff][];
    final int chunkSize = (leafCount + eff - 1) / eff;
    parallel(eff, idx -> {
      final int from = idx * chunkSize;
      final int to = Math.min(from + chunkSize, leafCount);
      if (from >= to) return;
      final double[] acc = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
      ProjectionIndexByteScan.conjunctiveAggregateNumericDouble(leafPayloads.subList(from, to), s.preds(), s.col(), acc);
      perThread[idx] = acc;
    });
    final double[] merged = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
    for (final double[] a : perThread) {
      if (a == null || a[0] == 0) continue;
      merged[0] += a[0];
      merged[1] += a[1];
      if (Double.compare(a[2], merged[2]) < 0) merged[2] = a[2];
      if (Double.compare(a[3], merged[3]) > 0) merged[3] = a[3];
    }
    return merged;
  }

  /**
   * Serve sum/avg over a pure double column with a SEED-FIRST sequential fold over the
   * predicate-matched cells in document order: {@code s1 = v1; si = si-1 + vi}. This is
   * bit-identical to the pairwise {@code Numeric.add} left fold the interpreter performs
   * for document-derived number items — including the lone-{@code -0.0} case a 0.0-seeded
   * accumulator would absorb ({@code 0.0 + -0.0 == +0.0}) and ill-conditioned sums where
   * association order changes digits.
   *
   * <p>Why not delegate to {@code Aggregate.SUM.aggregator()}: {@code fn:sum} does build a
   * {@code SumAvgAggregator}, but its batched double fast path (seed + 1024-slot buffer +
   * SIMD reduction — a DIFFERENT association order) only engages for {@code Dbl}-typed
   * items, which the document pipeline's number items are not — empirically the fallback
   * folds pairwise. {@code illConditionedSumMatchesInterpreterExactly} pins that premise:
   * if the pipeline's item typing ever changes, that test fails loudly and this fold must
   * follow suit.
   *
   * <p>Empty-match semantics mirror {@code fn:sum}/{@code fn:avg}: sum → 0, avg → empty.
   */
  private Sequence serveDoubleSumAvg(final DoubleAggServing s, final boolean avg) {
    long count = 0;
    double sum = 0.0;
    if (s.sliceStore() != null) {
      try {
        final ProjectionColumnScan.MatchingDoubleCursor cursor =
            new ProjectionColumnScan.MatchingDoubleCursor(s.sliceStore(), s.preds(), s.col(),
                s.fetcher());
        while (cursor.advance()) {
          final double v = cursor.value();
          sum = count == 0 ? v : sum + v;
          count++;
        }
        if (count == 0) {
          return avg ? new ItemSequence() : new Int64(0L);
        }
        return avg ? new Dbl(sum / count) : new Dbl(sum);
      } catch (final IllegalStateException ise) {
        // Corrupt/missing slices — restart the fold over the eager whole-leaf cursor.
        count = 0;
        sum = 0.0;
      }
    }
    final ProjectionIndexByteScan.MatchingDoubleCursor cursor =
        new ProjectionIndexByteScan.MatchingDoubleCursor(s.leafPayloads(), s.preds(), s.col());
    while (cursor.advance()) {
      final double v = cursor.value();
      sum = count == 0 ? v : sum + v;
      count++;
    }
    if (count == 0) {
      return avg ? new ItemSequence() : new Int64(0L);
    }
    return avg ? new Dbl(sum / count) : new Dbl(sum);
  }

  /**
   * Full double-column aggregate serving dispatch (§11-8): {@code count} from the parallel
   * kernel (exact, purity-free); {@code min}/{@code max} from the kernel's
   * {@code Double.compare} stats; {@code sum}/{@code avg} via {@link #serveDoubleSumAvg}.
   * Value aggregates require the pure-double-source gate, enforced inside
   * {@link #resolveDoubleAggServing} BEFORE any leaf is touched. Returns {@code null} to
   * fall back.
   */
  private Sequence tryServeDoubleAggregate(final String[] sourcePath, final String field,
      final PredicateNode predicateOrNull, final String func) {
    if (func == null) {
      return null;
    }
    switch (func) {
      case "count", "sum", "avg", "min", "max" -> { /* servable */ }
      default -> { return null; }
    }
    final boolean needsPurity = !"count".equals(func);
    final DoubleAggServing s = resolveDoubleAggServing(sourcePath, field, predicateOrNull, needsPurity);
    if (s == null) {
      return null;
    }
    try {
      final Sequence served;
      switch (func) {
        case "sum", "avg" -> served = serveDoubleSumAvg(s, "avg".equals(func));
        case "count" -> served = new Int64((long) doubleKernelStats(s)[0]);
        default -> {
          final double[] stats = doubleKernelStats(s);
          served = stats[0] == 0 ? new ItemSequence() : new Dbl(stats["min".equals(func) ? 2 : 3]);
        }
      }
      if (!"count".equals(func)) {
        DOUBLE_VALUE_SERVED.increment();
      }
      return served;
    } catch (final IllegalStateException ise) {
      // Malformed leaf discovered mid-scan — decline and fall back.
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
    // Existence probe first — see tryProjectionAggregate.
    if (!anyProjectionAvailable()) return null;
    final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
    final ProjectionIndexRegistry.Handle handle =
        lookupProjection(sourcePath, requiredFields(groupFields, cp));
    if (handle == null) return null;
    final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
    final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
    final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
    if (leafPayloads == null || leafPayloads.isEmpty()) return null;
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
      if (!handle.columnSparseClean(col, fetcher, materializer)) {
        return null;
      }
      cols[i] = col;
    }
    final ProjectionIndexScan.ColumnPredicate[] preds;
    if (cp == null) {
      preds = new ProjectionIndexScan.ColumnPredicate[0];
    } else {
      if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) return null;
      final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
      if (extracted == null) return null;
      preds = fuseRangePredicates(extracted);
    }
    final int leafCount = leafPayloads.size();
    final Object2LongOpenHashMap<String> merged = new Object2LongOpenHashMap<>();
    merged.defaultReturnValue(0L);
    try {
      // Dense composite path: canonical dicts for EVERY group column turn
      // the per-row work into one mixed-radix array increment (no composite
      // String, no hashing) — see conjunctiveCountByGroupMultiDense. Leaves
      // with out-of-canon values fall back per leaf into a hashmap that is
      // merged below.
      if (MULTI_DENSE_GROUPBY_ENABLED) {
        final byte[][][] canon = new byte[cols.length][][];
        boolean denseEligible = true;
        long cellCount = 1L;
        for (int i = 0; i < cols.length; i++) {
          canon[i] = handle.canonicalDict(cols[i], DENSE_GROUPBY_PROBE_LEAVES,
              DENSE_GROUPBY_CARD_LIMIT, materializer);
          if (canon[i] == null) {
            denseEligible = false;
            break;
          }
          cellCount *= canon[i].length + 1L;
          // Also bound by what an int-indexed long[] can hold — a user-raised
          // maxCells above 2^31 must not truncate at the (int) cast below.
          if (cellCount > MULTI_DENSE_MAX_CELLS || cellCount > Integer.MAX_VALUE - 8) {
            denseEligible = false;
            break;
          }
        }
        if (denseEligible) {
          return denseMultiGroupCounts(leafPayloads, preds, cols, canon, (int) cellCount, merged);
        }
      }
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
      // parallel() rethrows worker-side IllegalStateExceptions unwrapped, so
      // this single catch covers both the serial and parallel branches.
      return null;
    }
  }

  /**
   * Parallel driver + decode for the dense composite group-by kernel: one
   * mixed-radix {@code long[cellCount]} accumulator per worker, summed on
   * merge, then each non-zero cell is decoded ONCE into the executor's
   * composite key encoding ({@code 's<len>:<utf8>'} per column, {@code 'm'}
   * for missing — identical to the composite hashmap kernel). Per-leaf
   * fallbacks (out-of-canon dict values) land in per-worker hashmaps merged
   * the same way as the legacy path.
   */
  private Object2LongOpenHashMap<String> denseMultiGroupCounts(final List<byte[]> leafPayloads,
      final ProjectionIndexScan.ColumnPredicate[] preds, final int[] cols, final byte[][][] canon,
      final int cellCount, final Object2LongOpenHashMap<String> merged) {
    final int leafCount = leafPayloads.size();
    final long[] totals = new long[cellCount];
    if (leafCount < 64) {
      ProjectionIndexByteScan.conjunctiveCountByGroupMultiDense(leafPayloads, preds, cols, canon, totals, merged);
    } else {
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final long[][] perThreadCounts = new long[eff][];
      @SuppressWarnings("unchecked")
      final Object2LongOpenHashMap<String>[] perThreadFallback = new Object2LongOpenHashMap[eff];
      final int chunkSize = (leafCount + eff - 1) / eff;
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final long[] localCounts = new long[cellCount];
        final Object2LongOpenHashMap<String> localFallback = new Object2LongOpenHashMap<>();
        localFallback.defaultReturnValue(0L);
        ProjectionIndexByteScan.conjunctiveCountByGroupMultiDense(
            leafPayloads.subList(from, to), preds, cols, canon, localCounts, localFallback);
        perThreadCounts[idx] = localCounts;
        perThreadFallback[idx] = localFallback;
      });
      for (final long[] c : perThreadCounts) {
        if (c == null) continue;
        for (int i = 0; i < cellCount; i++) {
          totals[i] += c[i];
        }
      }
      for (final var m : perThreadFallback) {
        if (m == null) continue;
        final ObjectIterator<Object2LongMap.Entry<String>> it = m.object2LongEntrySet().fastIterator();
        while (it.hasNext()) {
          final Object2LongMap.Entry<String> e = it.next();
          merged.addTo(e.getKey(), e.getLongValue());
        }
      }
    }
    // Decode non-zero cells into composite keys — at most cellCount
    // (canonical-cardinality product) String builds for the whole query.
    final StringBuilder kb = new StringBuilder(32);
    final int[] ids = new int[cols.length];
    for (int cell = 0; cell < cellCount; cell++) {
      final long count = totals[cell];
      if (count == 0L) continue;
      int rem = cell;
      for (int g = cols.length - 1; g >= 0; g--) {
        final int radix = canon[g].length + 1;
        ids[g] = rem % radix;
        rem /= radix;
      }
      kb.setLength(0);
      for (int g = 0; g < cols.length; g++) {
        if (ids[g] == canon[g].length) {
          kb.append('m');
          continue;
        }
        final String v = new String(canon[g][ids[g]], StandardCharsets.UTF_8);
        kb.append('s').append(v.length()).append(':').append(v);
      }
      merged.addTo(kb.toString(), count);
    }
    return merged;
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
    // DENSE-ANCHOR SELECTION (unpredicated multi-key only). The scan iterates
    // the anchor field's slots, so records missing the anchor are never visited
    // — fatal for a SPARSE anchor whose absent records carry the OTHER group
    // keys. When a predicate is present its sound anchor (from compile()) already
    // governs visibility, so this only concerns the unpredicated path, where the
    // anchor defaults to groupFields[0]. If any group field is PROVABLY dense
    // (present on every record — path-summary reference count == record total),
    // anchor the slot walk on THAT field: every relevant record is then visited
    // and the remaining (possibly sparse) keys fall out as values, including the
    // 'm' missing bucket the encoder emits. If NONE is provably dense we keep the
    // historical order and the loud bail below (never guess density).
    final String[] anchorOrderedFields;
    if (predicateOrNull == null && groupFields.length > 1) {
      final int denseIdx = provablyDenseAnchor(sourcePath, groupFields);
      if (denseIdx > 0) {
        anchorOrderedFields = new String[groupFields.length];
        anchorOrderedFields[0] = groupFields[denseIdx];
        int w = 1;
        for (int i = 0; i < groupFields.length; i++) {
          if (i != denseIdx) anchorOrderedFields[w++] = groupFields[i];
        }
      } else {
        anchorOrderedFields = groupFields;
      }
    } else {
      anchorOrderedFields = groupFields;
    }
    // compileWithExtraFields appends fields in order, so fieldNames[0] (the scan
    // anchor) is anchorOrderedFields[0]. The group-key encoding below uses the
    // ORIGINAL groupFields order (via groupIdxs), so the emitted keys are
    // independent of which field anchors the scan.
    final CompiledPredicate cp = compileWithExtraFields(effective, anchorOrderedFields);
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
          // No provably-dense group field existed (else the anchor would not be
          // absent) — name the chosen anchor in the loud error.
          requireDenseGroupField(sourcePath, cp.fieldNames[0], true, 0L);
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
    final GroupKeyEvidence[] perThreadEvidence = new GroupKeyEvidence[eff];
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
      final GroupKeyEvidence evidence = new GroupKeyEvidence(groupIdxs.length);
      perThreadEvidence[idx] = evidence;
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
            // The region kernels emit 'l' keys (NumberRegion carries longs only).
            evidence.longEncoded[0] = true;
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
            encodeTypedGroupKey(typed, groupIdxs, groupFields, keyBuf, evidence);
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
    // Fail-closed numeric-key consistency (plateau / decimal-image mixtures —
    // interpreter grouping is order-dependent there).
    final GroupKeyEvidence mergedEvidence = new GroupKeyEvidence(groupIdxs.length);
    for (final GroupKeyEvidence ev : perThreadEvidence) {
      mergedEvidence.mergeFrom(ev);
    }
    mergedEvidence.requireConsistent(groupFields);
    if (anchorIsGroupField && anchorPathNodeKey != -1L) {
      // Anchor-visibility fixup: records LACKING the group field were never
      // visited (the scan iterates the field's slots). For a SINGLE group key
      // over a top-level array source the unvisited remainder is exactly the
      // missing-key group — synthesize it instead of erroring out (the
      // interpreter groups those records under the empty key). Multi-key
      // grouping cannot be reconstructed this way — stay LOUD on a gap (only
      // reachable when no group field was provably dense: a dense anchor visits
      // every record, so counted == recordCount and this fixup is a no-op).
      final long recordCount = topLevelRecordCountOrUnknown(sourcePath);
      if (recordCount >= 0 && counted < recordCount) {
        if (groupFields.length == 1) {
          merged.addTo("m", recordCount - counted);
        } else {
          requireDenseGroupField(sourcePath, cp.fieldNames[0], true, counted);
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
   * Number of records of a TOP-LEVEL ARRAY source that carry {@code field}, or
   * {@code -1} when this cannot be PROVED cheaply and soundly. Used to choose a
   * dense scan anchor among group-by fields (see {@link #provablyDenseAnchor}).
   *
   * <p>Density evidence comes from the PATH SUMMARY: an OBJECT_KEY path node's
   * reference count is incremented once per node that references it
   * ({@link io.sirix.index.path.summary.PathSummaryWriter#incrementReferenceCount}),
   * so for a top-level array of objects — where a key occurs at most once per
   * record — {@code getReferences()} is EXACTLY the count of records carrying
   * the field at that path. {@link #resolveTargetPathNodeKey} already fails
   * closed (returns {@code -1}) on an ambiguous or unresolvable path, so a
   * non-negative path node key means the count is unambiguous and scoped to the
   * query path (no nested same-name field can inflate it).
   *
   * <p>Returns {@code -1} (caller must NOT treat the field as dense) when the
   * source is not a top-level array, the path summary is unavailable, the path
   * is ambiguous/absent, or the reader cannot be read — never a guess.
   */
  private long recordsCarryingFieldOrUnknown(final String[] sourcePath, final String field) {
    if (sourcePath == null || sourcePath.length != 1 || !"[]".equals(sourcePath[0])) {
      return -1L;
    }
    if (!session.getResourceConfig().withPathSummary) {
      return -1L;
    }
    final long pathNodeKey = resolveTargetPathNodeKey(sourcePath, field);
    if (pathNodeKey < 0) {
      return -1L;
    }
    try (var summary = session.openPathSummary(revision)) {
      final PathNode pathNode = summary.getPathNodeForPathNodeKey(pathNodeKey);
      if (pathNode == null) {
        return -1L;
      }
      return pathNode.getReferences();
    } catch (final Exception e) {
      return -1L;
    }
  }

  /**
   * Index into {@code groupFields} of a field PROVABLY present on every record
   * of a top-level array source, or {@code -1} when re-anchoring is unsafe or
   * no field can be proved dense. Anchors the multi-key scan on a DENSE field so
   * the slot walk visits EVERY relevant record; the remaining (possibly sparse)
   * group keys then fall out as their per-record values — including the
   * {@code 'm'} missing bucket the typed encoder already emits for absent
   * fields. A field is dense iff the path summary proves its scoped occurrence
   * count equals the array's record total (see
   * {@link #recordsCarryingFieldOrUnknown}).
   *
   * <p><b>First-key collapse veto.</b> brackit's interpreter has an
   * order-dependent quirk: when the FIRST grouping variable evaluates to the
   * EMPTY SEQUENCE on EVERY tuple (the first group field is absent on every
   * record), the whole grouping degenerates to a single all-null tuple
   * (verified: {@code group by $ghost, $dept} yields one {@code {null,null}}
   * group, while {@code group by $dept, $ghost} yields proper per-dept groups).
   * A dense-anchored scan would visit every record and emit the per-group
   * tuples — diverging from that collapse. So if {@code groupFields[0]} is
   * entirely absent we REFUSE to re-anchor and let the existing loud bail fire
   * (fail-closed, exactly the historical behavior). A first key present on only
   * SOME records is fine — those rows group into the null/missing bucket
   * normally, which the typed kernel reproduces.
   *
   * <p>Returns the FIRST provably-dense field (group-field order) so the choice
   * is deterministic and stable across runs; never guesses density.
   */
  private int provablyDenseAnchor(final String[] sourcePath, final String[] groupFields) {
    final long recordCount = topLevelRecordCountOrUnknown(sourcePath);
    if (recordCount < 0) {
      return -1;
    }
    if (recordCount == 0) {
      // Empty source: zero output tuples either way; leave the order untouched.
      return -1;
    }
    // First-key collapse veto: an entirely-absent FIRST group field makes the
    // interpreter collapse the whole grouping — not reproducible by re-anchoring.
    final long firstCarrying = recordsCarryingFieldOrUnknown(sourcePath, groupFields[0]);
    if (firstCarrying == 0L || resolveFieldKey(groupFields[0]) == -1) {
      return -1;
    }
    for (int i = 0; i < groupFields.length; i++) {
      final long carrying = i == 0 ? firstCarrying : recordsCarryingFieldOrUnknown(sourcePath, groupFields[i]);
      if (carrying == recordCount) {
        return i;
      }
    }
    return -1;
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

  /** {@code 2^53} — below this magnitude an integral double maps to exactly ONE long. */
  private static final double TWO_POW_53 = 9007199254740992.0d;

  /**
   * Per-scan, per-group-field evidence for the numeric-key canonicalization's
   * fail-closed guards. The interpreter's grouping equality is
   * {@code hash(doubleValue bits)} + {@code atomicCmp} — NON-TRANSITIVE when
   * values of different numeric types share a double image without being
   * exactly equal (the >=2^53 long plateau; non-shortest-form decimals), and
   * the merged group's RENDERED key is the first tuple's lexical form. Both
   * are unreproducible by an unordered parallel scan, so such mixtures fail
   * LOUDLY instead of silently grouping differently. One instance per worker;
   * {@link #mergeFrom} folds them post-scan.
   */
  private static final class GroupKeyEvidence {
    final boolean[] longEncoded;
    final boolean[] largeIntegralDoubleImage;
    final boolean[] inexactDecIntegralImage;
    final it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet[] doubleImages;
    final it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet[] inexactDecImages;

    GroupKeyEvidence(final int nGroupFields) {
      longEncoded = new boolean[nGroupFields];
      largeIntegralDoubleImage = new boolean[nGroupFields];
      inexactDecIntegralImage = new boolean[nGroupFields];
      doubleImages = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet[nGroupFields];
      inexactDecImages = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet[nGroupFields];
    }

    void addDoubleImage(final int g, final double d) {
      if (doubleImages[g] == null) doubleImages[g] = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet(8);
      doubleImages[g].add(d);
    }

    void addInexactDecImage(final int g, final double d) {
      if (inexactDecImages[g] == null) inexactDecImages[g] = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet(8);
      inexactDecImages[g].add(d);
    }

    void mergeFrom(final GroupKeyEvidence o) {
      if (o == null) return;
      for (int g = 0; g < longEncoded.length; g++) {
        longEncoded[g] |= o.longEncoded[g];
        largeIntegralDoubleImage[g] |= o.largeIntegralDoubleImage[g];
        inexactDecIntegralImage[g] |= o.inexactDecIntegralImage[g];
        if (o.doubleImages[g] != null) {
          if (doubleImages[g] == null) doubleImages[g] = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet(8);
          doubleImages[g].addAll(o.doubleImages[g]);
        }
        if (o.inexactDecImages[g] != null) {
          if (inexactDecImages[g] == null) inexactDecImages[g] = new it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet(8);
          inexactDecImages[g].addAll(o.inexactDecImages[g]);
        }
      }
    }

    /**
     * Fail loudly on the mixtures whose interpreter grouping is order-dependent
     * (non-transitive equality and/or first-tuple key rendering):
     * <ol>
     * <li>long-encoded keys + an INTEGRAL double image at or above 2^53 — the
     * plateau: one double is atomicCmp-equal to several distinct longs;</li>
     * <li>long-encoded keys + a non-shortest-form decimal whose image is a
     * small integral — it merges with image-equal doubles (which canonicalize
     * into the long space) but not with the equal longs themselves;</li>
     * <li>a double image colliding with a non-shortest-form decimal's image —
     * they merge in the interpreter while their lexical renderings differ.</li>
     * </ol>
     */
    void requireConsistent(final String[] groupFields) {
      for (int g = 0; g < longEncoded.length; g++) {
        if (longEncoded[g] && largeIntegralDoubleImage[g]) {
          throw inconsistent(groupFields[g], "integer keys mixed with integral doubles at or above 2^53");
        }
        if (longEncoded[g] && inexactDecIntegralImage[g]) {
          throw inconsistent(groupFields[g],
              "integer keys mixed with non-shortest-form decimals carrying an integral double image");
        }
        if (doubleImages[g] != null && inexactDecImages[g] != null) {
          final var it = inexactDecImages[g].iterator();
          while (it.hasNext()) {
            if (doubleImages[g].contains(it.nextDouble())) {
              throw inconsistent(groupFields[g],
                  "double keys image-colliding with non-shortest-form decimal keys");
            }
          }
        }
      }
    }

    private static QueryException inconsistent(final String field, final String why) {
      return new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                                "Vectorized group-by over field '%s': %s — the generic pipeline's grouping is "
                                    + "order-dependent here and a parallel scan cannot reproduce it; rewrite the "
                                    + "query or disable vectorization",
                                field,
                                why);
    }
  }

  /**
   * Encode the typed values of the group fields into an unambiguous composite
   * key: one tag char per part; longs/doubles are NUL-terminated (their digits
   * never contain NUL), strings and decimal literals are length-prefixed.
   *
   * <p>NUMERIC CANONICALIZATION (interpreter grouping parity): XQuery group-by
   * merges keys under {@code eq}-style equality, so {@code 18}, {@code 18.0}
   * and {@code 18.00} are ONE group while the historical encoding split them
   * by type tag. Canonical mapping:
   * <ul>
   * <li>integral doubles with {@code |d| < 2^53} (the unique-long zone)
   * encode as the equal long ({@code 'l'});</li>
   * <li>decimals that are integral and long-representable encode as the
   * equal long; other decimals that EQUAL their shortest double form
   * ({@code Double.toString} round-trip — rendering-identical) encode in the
   * double image space ({@code 'd'}); the rest keep the exact decimal
   * encoding ({@code 'D'}) canonicalized via {@code stripTrailingZeros} (so
   * {@code 2.5} and {@code 2.50} merge);</li>
   * <li>a {@code -0.0} double key fails loudly: the interpreter merges it
   * with {@code 0} (its grouping canonicalizes negative zero) but renders
   * the FIRST tuple's lexical ({@code "-0"} vs {@code "0"}) — order-dependent
   * and unreproducible by a parallel scan.</li>
   * </ul>
   * {@code evidence} collects the fail-closed signals — see
   * {@link GroupKeyEvidence#requireConsistent}.
   */
  private static void encodeTypedGroupKey(final TypedGroupScratch typed, final int[] groupIdxs,
      final String[] groupFields, final StringBuilder out, final GroupKeyEvidence evidence) {
    for (int g = 0; g < groupIdxs.length; g++) {
      final int fi = groupIdxs[g];
      switch (typed.kind[fi]) {
        case TK_MISSING -> out.append('m');
        case TK_NULL -> out.append('z');
        case TK_BOOL -> out.append('b').append(typed.bool[fi] ? '1' : '0');
        case TK_LONG -> {
          out.append('l').append(typed.lng[fi]).append('\u0000');
          evidence.longEncoded[g] = true;
        }
        case TK_DBL -> encodeDoubleKey(typed.dbl[fi], g, groupFields, out, evidence);
        case TK_DECSTR -> encodeDecimalKey(typed.str[fi], g, out, evidence);
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

  /** Double-key encoding — canonicalizes small integral doubles into the long key space. */
  private static void encodeDoubleKey(final double d, final int g, final String[] groupFields,
      final StringBuilder out, final GroupKeyEvidence evidence) {
    if (d == 0.0d && Double.doubleToRawLongBits(d) != 0L) {
      // -0.0: the interpreter MERGES it with 0 but renders the group key as
      // the first tuple's lexical ("-0" vs "0") — order-dependent.
      throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                               "Vectorized group-by over field '%s': a -0.0 group key merges with 0 in the generic "
                                   + "pipeline but its rendered key depends on tuple order — rewrite the query or "
                                   + "disable vectorization",
                               groupFields[g]);
    }
    final boolean integral = d == Math.rint(d) && !Double.isInfinite(d);
    if (integral && Math.abs(d) < TWO_POW_53) {
      // Unique-long zone: exactly one long equals this double — same bucket.
      out.append('l').append((long) d).append('\u0000');
      evidence.longEncoded[g] = true;
      return;
    }
    out.append('d').append(d).append('\u0000');
    evidence.addDoubleImage(g, d);
    if (integral) {
      evidence.largeIntegralDoubleImage[g] = true;
    }
  }

  /**
   * Decimal-key encoding — canonicalizes long-representable integrals into the
   * long key space and shortest-double-form decimals into the double image
   * space; everything else keeps the exact decimal encoding (scale-canonical).
   */
  private static void encodeDecimalKey(final String lexical, final int g, final StringBuilder out,
      final GroupKeyEvidence evidence) {
    final java.math.BigDecimal bd = new java.math.BigDecimal(lexical);
    final java.math.BigDecimal strip = bd.stripTrailingZeros();
    if (strip.scale() <= 0 && inLongRange(strip)) {
      out.append('l').append(strip.longValueExact()).append('\u0000');
      evidence.longEncoded[g] = true;
      return;
    }
    final double d = bd.doubleValue();
    if (Double.isFinite(d) && new java.math.BigDecimal(Double.toString(d)).compareTo(bd) == 0) {
      // The decimal IS the double's shortest form: value-equal AND
      // rendering-identical to a double key — encode in image space. (It is
      // non-integral here: integral long-representable went to 'l' above, and
      // an integral image beyond long range cannot round-trip toString.)
      out.append('d').append(d).append('\u0000');
      evidence.addDoubleImage(g, d);
      return;
    }
    // Exact decimal key, canonical scale (2.5 == 2.50). toPlainString keeps
    // the encoding free of 'E' so huge decimals stay unambiguous.
    final String canonical = strip.toPlainString();
    out.append('D').append(canonical.length()).append(':').append(canonical);
    if (Double.isFinite(d)) {
      evidence.addInexactDecImage(g, d);
      if (d == Math.rint(d) && Math.abs(d) < TWO_POW_53) {
        evidence.inexactDecIntegralImage[g] = true;
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
    // Wtx mode serves unpredicated projection queries only — see
    // executePredicateCount.
    if (wtx != null) return null;
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
        // Stats-level cache first (keyed without func): min/max/avg/sum over the same
        // (path, field, predicate) fold from ONE kernel scan.
        final String statsKey = "ps:" + pathCacheKey(sourcePath, field) + "@@"
            + predicateCacheKey(predicate);
        long[] projected = predicateStatsCache.get(statsKey);
        if (projected == null) {
          projected = tryProjectionAggregate(sourcePath, field, predicate);
          if (projected != null) {
            PREDICATED_AGG_SCANS.increment();
            final long[] prior = predicateStatsCache.putIfAbsent(statsKey, projected);
            if (prior != null) {
              projected = prior;
            }
          }
        }
        if (projected != null) {
          fresh = longStatsToSequence(func, projected[0], projected[1], projected[2], projected[3]);
        }
        if (fresh == null) {
          // Double-column branch, purity-gated (§11-8): value aggregates serve only when
          // every leaf asserts pure Double sources — tryServeDoubleAggregate fails closed
          // (and scan-free) otherwise. Result rides the predicateAggregateCache below.
          fresh = tryServeDoubleAggregate(sourcePath, field, predicate, func);
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
    // Typed per-worker accumulators — exact longs, EXACT decimals, doubles.
    final MixedAgg[] perThread = new MixedAgg[eff];
    final long[] countPerThread = new long[eff];
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
      final MixedAgg acc = new MixedAgg();
      perThread[idx] = acc;
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
              countPerThread[idx]++;
            }
            continue;
          }
          if (typed.kind[aggFieldIdx] == TK_STR || typed.kind[aggFieldIdx] == TK_BOOL
              || typed.kind[aggFieldIdx] == TK_NULL || typed.kind[aggFieldIdx] == TK_COMPLEX) {
            sawNonNumeric[0] = true;
          }
          // Numeric aggregate — fold by the agg value's REAL type. Longs use the
          // exact long accumulator, DECIMALS the exact BigDecimal accumulator
          // (the legacy parseDouble fold lost precision vs the interpreter's
          // exact decimal arithmetic), doubles the double accumulator.
          switch (typed.kind[aggFieldIdx]) {
            case TK_LONG -> acc.addLong(typed.lng[aggFieldIdx]);
            case TK_DBL -> acc.addDouble(typed.dbl[aggFieldIdx]);
            case TK_DECSTR -> acc.addDecimal(new java.math.BigDecimal(typed.str[aggFieldIdx]));
            default -> { /* non-numeric value on this row — skip (legacy semantics) */ }
          }
        }
      }
    });
    schedule.publish(anchorNameKey);

    if (isCount) {
      long count = 0L;
      for (final long c : countPerThread) count += c;
      return new Int64(count);
    }
    final MixedAgg merged = new MixedAgg();
    for (final MixedAgg a : perThread) {
      merged.merge(a);
    }
    if (merged.longCount + merged.decCount + merged.dblCount == 0 && sawNonNumeric[0]) {
      // Matching rows carried the aggregate field, but never as a number —
      // the interpreter applies string/error semantics here. Fail loudly.
      throw new QueryException(ErrorCode.BIT_DYN_INT_ERROR,
                               "Vectorized %s over field '%s': matching records hold no numeric values — "
                                   + "string/boolean/null aggregation is not supported by the vectorized executor",
                               func,
                               aggField);
    }
    // MixedAgg#result mirrors the interpreter's numeric promotion: pure
    // long/decimal columns produce EXACT decimal results (incl. Dec#div for
    // avg — digit-for-digit parity); double-bearing columns produce doubles;
    // zero contributing rows yield sum=0 / empty for avg|min|max.
    return merged.result(func);
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
            // Skip anonymous ancestors (array layers, non-ELEMENT/OBJECT_KEY roots).
            while (cursor != null && resolvedLocalName(summary, cursor) == null) {
              cursor = cursor.getParent();
            }
            if (cursor == null) { ok = false; break; }
            final String name = resolvedLocalName(summary, cursor);
            if (!expectedAncestors[i].equals(name)) { ok = false; break; }
            cursor = cursor.getParent();
          }
          if (!ok) continue;
          // Post-condition: after consuming expectedAncestors, the remaining
          // ancestor chain must contain NO more named ancestors (otherwise the
          // candidate lives deeper than the query path — e.g. pet.dept when
          // the query is $u.dept). Without this check, an empty
          // expectedAncestors matches every candidate regardless of depth.
          while (cursor != null && resolvedLocalName(summary, cursor) == null) {
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

  /** Sentinel local name of anonymous {@code ARRAY} path-summary layers. */
  private static final String ARRAY_PATH_LOCAL_NAME = PathSummaryWriter.ARRAY_PATH_QNM.getLocalName();

  /**
   * Local name of a path-summary node, or {@code null} for anonymous layers.
   *
   * <p>{@link PathNode#getName()} is only populated for nodes created in
   * THIS process — a deserialized summary carries nameKeys, and reading the
   * raw field made every named ancestor look anonymous after re-open (the
   * chain matcher then failed and the aggregate fell back to an UNSCOPED
   * name sweep — over-counting same-named fields under other roots). Resolve
   * through the positioned reader instead, and treat the {@code __array__}
   * sentinel as anonymous so fresh and re-opened summaries walk identically.
   */
  private static String resolvedLocalName(final PathSummaryReader summary, final PathNode node) {
    final QNm inMemory = node.getName();
    if (inMemory != null) {
      final String local = inMemory.getLocalName();
      return local.isEmpty() || ARRAY_PATH_LOCAL_NAME.equals(local) ? null : local;
    }
    if (!summary.moveTo(node.getNodeKey())) {
      return null;
    }
    final QNm resolved = summary.getName();
    if (resolved == null) {
      return null;
    }
    final String local = resolved.getLocalName();
    return local.isEmpty() || ARRAY_PATH_LOCAL_NAME.equals(local) ? null : local;
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
    final ProjectionIndexRegistry.Handle handle = lookupProjection(sourcePath, cp.fieldNames);
    if (handle == null) {
      if (PROJ_DIAG) System.err.println("[proj] no covering handle for key=" + resourceKey
          + " path=" + Arrays.toString(sourcePath)
          + " fields=" + Arrays.toString(cp.fieldNames));
      return null;
    }
    final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
    final ProjectionIndexScan.ColumnPredicate[] extracted = extractConjunctivePredicates(cp, handle);
    if (extracted == null) {
      // Not a pure conjunction — AND/OR trees serve through the fold kernels (stage 6).
      final Long treeCount = tryTreeCount(cp, handle, fetcher);
      if (treeCount == null && PROJ_DIAG) System.err.println("[proj] unsupported shape");
      return treeCount;
    }
    // iter#07 range fusion: collapse same-column (GT|GE, LT|LE) pairs
    // into one BETWEEN_* predicate so the evaluator only loads the
    // column once. Defaults on; disable with
    // -Dsirix.projection.rangeFusion=false.
    final ProjectionIndexScan.ColumnPredicate[] preds = fuseRangePredicates(extracted);
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    if (preds.length == 0) {
      if (store != null) {
        // Descriptor truth only — zero segment loads for an unpredicated count.
        long rows = 0;
        final int leaves = store.leafCount();
        for (int leaf = 0; leaf < leaves; leaf++) {
          rows += store.rowCount(leaf);
        }
        return rows;
      }
      return ProjectionIndexByteScan.countRows(handle.leafPayloads(leafMaterializer(handle)));
    }
    if (store != null && predsSliceable(store, preds)) {
      try {
        return sliceCountParallel(store, preds, fetcher);
      } catch (final IllegalStateException ise) {
        // Corrupt/missing slices — eager path re-surfaces through fail-soft.
      }
    }
    final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
    return leafPayloads == null ? null : parallelConjunctiveCount(leafPayloads, preds);
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
   * Dense COMPOSITE group-by gate. Unlike the single-key case (where the
   * hashmap path's one intrinsified probe per row is competitive at tiny
   * cardinality — see {@link #DENSE_GROUPBY_ENABLED}), the composite kernel
   * replaces TWO hash probes plus lazy composite-String assembly per row
   * with a single mixed-radix array increment, which wins outright; hence
   * default ON. Disable via {@code -Dsirix.projection.denseMultiGroupBy=false}.
   */
  private static final boolean MULTI_DENSE_GROUPBY_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.denseMultiGroupBy", "true"));

  /**
   * Upper bound on the dense composite accumulator ({@code prod(canonLen+1)}
   * cells, one {@code long} each, per worker). 1M cells = 8 MB per worker —
   * beyond that the hashmap path is the better trade.
   */
  private static final long MULTI_DENSE_MAX_CELLS =
      Long.parseLong(System.getProperty("sirix.projection.denseMultiGroupBy.maxCells", String.valueOf(1L << 20)));

  /**
   * Cardinality bail-out for the dictionary-union count-distinct kernel
   * ({@link ProjectionIndexByteScan#distinctPresentStrings}); beyond this the
   * group-counting path (any cardinality) takes over.
   */
  private static final int COUNT_DISTINCT_DICT_CARD_LIMIT =
      Integer.parseInt(System.getProperty("sirix.projection.countDistinct.cardLimit", "1024"));

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
    final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
    final List<byte[]> leafPayloads = handle.leafPayloads(materializer);
    final byte[][] canonicalDict = DENSE_GROUPBY_ENABLED
        ? handle.canonicalDict(groupColumn, DENSE_GROUPBY_PROBE_LEAVES, DENSE_GROUPBY_CARD_LIMIT,
            materializer)
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
      final List<byte[]> leafPayloads,
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
    // cp.fieldNames includes the group field (compileWithExtraField), so the
    // covering lookup implicitly requires the group field to also be a
    // column of the selected projection.
    final ProjectionIndexRegistry.Handle handle = lookupProjection(sourcePath, cp.fieldNames);
    if (handle == null) return null;
    final int groupColumn = handle.columnOf(groupField);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate — see tryProjectionIndexGroupByCountOnly.
    if (!handle.columnSparseClean(groupColumn, columnFetcher(), leafMaterializer(handle))) {
      return null;
    }
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
      // nameKeys are String hashes and may legitimately be NEGATIVE
      // ('active'/'amount' hash below zero) — only the MISSING sentinel,
      // which is exactly -1, is unpublishable. The historical `< 0` check
      // silently excluded every negative-hash anchor from the page-skip
      // index, forcing full page sweeps on those fields forever.
      if (recordBuffers == null || projectionRegistryKey == null || anchorNameKey == -1) return;
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
    // Source 2 + 3: opportunistic in-memory registry. -1 is the MISSING
    // sentinel (unresolvable anchor — callers bail before scanning anyway);
    // every other value, NEGATIVE String hashes included, is a legitimate
    // registry key.
    if (projectionRegistryKey == null || anchorNameKey == -1) {
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
  private ProjectionIndexScan.ColumnPredicate[] extractConjunctivePredicates(
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
      out[i] = convertPredicateLeaf(cp, leaves[i], handle);
      if (out[i] == null) return null;
    }
    return out;
  }

  /**
   * Convert ONE compiled predicate leaf node to a {@link ProjectionIndexScan.ColumnPredicate},
   * applying every serving gate (coverage, sparse evidence, integrality/transform rules for
   * double columns). {@code null} = not servable — callers fall back. Shared by the flat
   * conjunctive extractor and the AND/OR tree extractor so the gates can never diverge.
   */
  private ProjectionIndexScan.ColumnPredicate convertPredicateLeaf(
      final CompiledPredicate cp, final int n, final ProjectionIndexRegistry.Handle handle) {
    {
      final byte op = cp.ops[n];
      final int fi = cp.fieldIdx[n];
      if (fi < 0 || fi >= cp.fieldNames.length) return null;
      final int column = handle.columnOf(cp.fieldNames[fi]);
      if (column < 0) return null; // field not in index — should have been filtered by covers()
      // Session-bound sources built from THIS executor's own live session, threaded into the
      // shared handle's gate/fill calls.
      final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
      final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
      // Sparse-evidence gate: a predicate over a column without per-row
      // presence data (legacy v1 leaves) or with unrepresentable values
      // (null/object/array/kind mismatch) would evaluate against stored
      // DEFAULTS — e.g. `x < 40` matching every missing row via the phantom
      // 0. Fail closed; the scan-path kernels handle missing correctly.
      if (!handle.columnSparseClean(column, fetcher, materializer)) return null;
      // NUMERIC_DOUBLE columns store the order-preserving transform: literals must be
      // transformed at plan time (comparing untransformed literals against transformed cells
      // silently returns wrong rows), and only provably value-exact columns may serve
      // (a lossy Big*→double cell differs from the source value the interpreted pipeline
      // compares exactly). Fail closed on anything unprovable.
      final byte columnKindByte = handle.columnKindOf(column);
      final boolean doubleColumn = columnKindByte == ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE;
      // Kind-gate EVERY arm: the mask evaluator dispatches on the column's ACTUAL kind,
      // so a literal of the wrong type — `where $r.numericField = "x"`, `where $r.longFlag`,
      // `where $r.stringField > 5` — must decline here. The interpreter type-errors (or
      // EBV-evaluates) those; a kind-blind predicate would silently compare unrelated
      // encodings (e.g. string-EQ over a long column running numeric EQ against 0).
      final boolean numericColumn =
          doubleColumn || columnKindByte == ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG;
      switch (op) {
        case CompiledPredicate.OP_NUM_CMP, CompiledPredicate.OP_FP_CMP,
            CompiledPredicate.OP_DEC_CMP -> {
          if (!numericColumn) {
            return null;
          }
        }
        case CompiledPredicate.OP_STR_EQ -> {
          if (columnKindByte != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT) {
            return null;
          }
        }
        case CompiledPredicate.OP_BOOL_REF -> {
          if (columnKindByte != ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN) {
            return null;
          }
        }
        default -> {
          return null; // unsupported leaf — fall back
        }
      }
      final ProjectionIndexScan.ColumnPredicate pred;
      switch (op) {
        case CompiledPredicate.OP_NUM_CMP -> {
          final ProjectionIndexScan.Op translated = translateCmpOp(cp.cmpOp[n]);
          if (translated == null) return null;
          if (doubleColumn) {
            if (!handle.numericColumnIsIntegral(column, fetcher)) return null; // value-exact gate
            final double asDouble = (double) cp.longLit[n];
            if ((long) asDouble != cp.longLit[n]) return null; // literal beyond 2^53 — inexact
            pred = ProjectionIndexScan.ColumnPredicate.numeric(column, translated,
                ProjectionDoubleEncoding.encode(asDouble));
          } else {
            // Comparisons evaluate against the column's TRUNCATED longs. When the
            // builder positively saw non-integral values in this column, decline —
            // e.g. `score > 2` would match 2.5 stored as 2 incorrectly. Unknown
            // provenance keeps the legacy behavior (no regression for re-encoded
            // handles); known-clean columns are exact.
            if (handle.numericColumnKnownNonIntegral(column, fetcher)) return null;
            pred = ProjectionIndexScan.ColumnPredicate.numeric(column, translated, cp.longLit[n]);
          }
        }
        case CompiledPredicate.OP_FP_CMP -> {
          if (doubleColumn) {
            // Native double comparison: transform the literal, keep the operator — no
            // threshold rewrite needed (the transform is order-isomorphic).
            if (!handle.numericColumnIsIntegral(column, fetcher)) return null; // value-exact gate
            if (Double.isNaN(cp.dblLit[n])) return null;
            final ProjectionIndexScan.Op translated = translateCmpOp(cp.cmpOp[n]);
            if (translated == null) return null;
            pred = ProjectionIndexScan.ColumnPredicate.numeric(column, translated,
                ProjectionDoubleEncoding.encode(cp.dblLit[n]));
          } else {
            // Double-threshold comparison on a NUMERIC_LONG column: only valid
            // when the column is PROVABLY integral (builder-tracked evidence —
            // unknown provenance fails closed, the column stores truncated
            // doubles otherwise). The threshold is rewritten into exact long
            // space (x > 9.99 ⟺ x >= 10) — see rewriteFpCmpForIntegralColumn.
            if (!handle.numericColumnIsIntegral(column, fetcher)) return null;
            pred = rewriteFpCmpForIntegralColumn(column, cp.cmpOp[n], cp.dblLit[n]);
            if (pred == null) return null;
          }
        }
        case CompiledPredicate.OP_DEC_CMP -> {
          if (doubleColumn) {
            // XQuery promotes xs:decimal to xs:double when compared against double values —
            // the interpreter's Dbl#cmp does exactly that — so encoding the PROMOTED literal
            // with the original operator is exact parity, even for decimals that are not
            // binary-representable. The long-space arm below would compare untransformed
            // integers against transformed cells (silent wrong rows).
            if (!handle.numericColumnIsIntegral(column, fetcher)) return null; // value-exact gate
            final ProjectionIndexScan.Op translatedDec = translateCmpOp(cp.cmpOp[n]);
            if (translatedDec == null || cp.decLit[n] == null) return null;
            final double promoted = cp.decLit[n].doubleValue();
            if (Double.isNaN(promoted)) return null;
            pred = ProjectionIndexScan.ColumnPredicate.numeric(column, translatedDec,
                ProjectionDoubleEncoding.encode(promoted));
          } else {
          // Exact-decimal threshold on a NUMERIC_LONG column: the compile
          // step already collapsed the decimal comparison into a pure
          // long-space arm — translate it. Same integrality gate as FP_CMP.
          if (!handle.numericColumnIsIntegral(column, fetcher)) return null;
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
      return pred;
    }
  }

  /**
   * Build a {@link ProjectionIndexScan.PredicateTree} from an AND/OR composition of
   * supported leaves (P5b stage 6). NOT and ALWAYS_* nodes fall back — {@code not()} over
   * a missing-field comparison flips missing ⇒ {@code true}, which the mask algebra's
   * missing ⇒ {@code false} leaves cannot express (see the tree type's contract). Pure
   * conjunctions are expected to have been taken by {@link #extractConjunctivePredicates}
   * first; this is the OR-bearing general case. {@code null} = fall back.
   */
  private ProjectionIndexScan.PredicateTree extractPredicateTree(
      final CompiledPredicate cp, final ProjectionIndexRegistry.Handle handle) {
    final ArrayList<ProjectionIndexScan.ColumnPredicate> leaves = new ArrayList<>();
    final byte[] program = new byte[2 * cp.ops.length];
    final int[] pc = { 0 };
    if (!emitTreeNode(cp, 0, handle, leaves, program, pc) || leaves.isEmpty()) {
      return null;
    }
    return ProjectionIndexScan.PredicateTree.of(
        leaves.toArray(new ProjectionIndexScan.ColumnPredicate[0]),
        Arrays.copyOf(program, pc[0]));
  }

  /** Recursive postfix emitter for {@link #extractPredicateTree}; {@code false} = unsupported shape. */
  private boolean emitTreeNode(final CompiledPredicate cp, final int n,
      final ProjectionIndexRegistry.Handle handle,
      final ArrayList<ProjectionIndexScan.ColumnPredicate> leaves, final byte[] program,
      final int[] pc) {
    final byte op = cp.ops[n];
    if (op == CompiledPredicate.OP_AND || op == CompiledPredicate.OP_OR) {
      final int cs = cp.childStart[n];
      final int cc = cp.childCount[n];
      if (cc == 0) {
        return false;
      }
      if (!emitTreeNode(cp, cp.children[cs], handle, leaves, program, pc)) {
        return false;
      }
      final byte combinator = op == CompiledPredicate.OP_AND
          ? ProjectionIndexScan.PredicateTree.OP_AND
          : ProjectionIndexScan.PredicateTree.OP_OR;
      for (int i = 1; i < cc; i++) {
        if (!emitTreeNode(cp, cp.children[cs + i], handle, leaves, program, pc)) {
          return false;
        }
        program[pc[0]++] = combinator;
      }
      return true;
    }
    if (op == CompiledPredicate.OP_NUM_CMP || op == CompiledPredicate.OP_FP_CMP
        || op == CompiledPredicate.OP_DEC_CMP || op == CompiledPredicate.OP_STR_EQ
        || op == CompiledPredicate.OP_BOOL_REF) {
      if (leaves.size() >= ProjectionIndexScan.PredicateTree.MAX_LEAVES) {
        return false;
      }
      final ProjectionIndexScan.ColumnPredicate leaf = convertPredicateLeaf(cp, n, handle);
      if (leaf == null) {
        return false;
      }
      program[pc[0]++] = (byte) leaves.size();
      leaves.add(leaf);
      return true;
    }
    return false; // NOT / ALWAYS_* / unknown — generic pipeline handles it
  }

  /** OR-tree count serving (stage 6): fold kernels only — the generic pipeline is the fallback. */
  private Long tryTreeCount(final CompiledPredicate cp,
      final ProjectionIndexRegistry.Handle handle,
      final ProjectionColumnStore.SegmentFetcher fetcher) {
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    if (store == null || store.leafCount() == 0) {
      return null;
    }
    final ProjectionIndexScan.PredicateTree tree = extractPredicateTree(cp, handle);
    if (tree == null) {
      return null;
    }
    try {
      if (!ProjectionSegmentFoldScan.eligibleTree(store, tree, -1, fetcher)) {
        return null;
      }
      final int leafCount = store.leafCount();
      if (leafCount < 64) {
        return ProjectionSegmentFoldScan.treeCount(store, tree, fetcher);
      }
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final long[] perThread = new long[eff];
      final int chunkSize = (leafCount + eff - 1) / eff;
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        perThread[idx] = ProjectionSegmentFoldScan.treeCount(store, tree, from, to, fetcher);
      });
      long total = 0;
      for (final long t : perThread) {
        total += t;
      }
      return total;
    } catch (final IllegalStateException ise) {
      // Corrupt/transient fills — the generic pipeline answers correctly.
      return null;
    }
  }

  /** OR-tree long-aggregate serving (stage 6) — same contract as {@link #tryTreeCount}. */
  private long[] tryTreeAggregate(final CompiledPredicate cp,
      final ProjectionIndexRegistry.Handle handle, final ProjectionColumnStore store,
      final int col, final ProjectionColumnStore.SegmentFetcher fetcher) {
    if (store == null || store.leafCount() == 0) {
      return null;
    }
    final ProjectionIndexScan.PredicateTree tree = extractPredicateTree(cp, handle);
    if (tree == null) {
      return null;
    }
    try {
      if (!ProjectionSegmentFoldScan.eligibleTree(store, tree, col, fetcher)) {
        return null;
      }
      final int leafCount = store.leafCount();
      if (leafCount < 64) {
        final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.treeAggregateNumeric(store, tree, col, acc, fetcher);
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
        ProjectionSegmentFoldScan.treeAggregateNumeric(store, tree, col, acc, from, to, fetcher);
        perThread[idx] = acc;
      });
      return mergeLongAgg(perThread);
    } catch (final IllegalStateException ise) {
      return null;
    }
  }

  /**
   * Union of the primary fields (aggregate / group columns) and the compiled
   * predicate's fields — the full column set a projection must cover to
   * serve the query. Order/duplicates don't matter: coverage checks are
   * membership tests.
   */
  private static String[] requiredFields(final String[] primary, final CompiledPredicate cpOrNull) {
    if (cpOrNull == null || cpOrNull.fieldNames == null || cpOrNull.fieldNames.length == 0) {
      return primary;
    }
    final String[] merged = new String[primary.length + cpOrNull.fieldNames.length];
    System.arraycopy(primary, 0, merged, 0, primary.length);
    System.arraycopy(cpOrNull.fieldNames, 0, merged, primary.length, cpOrNull.fieldNames.length);
    return merged;
  }

  /**
   * Projection lookup for this executor's revision: the revision-scoped
   * catalog + page layer first ({@link ProjectionIndexCatalog} — the same
   * discovery route the other index families use, correct across commits,
   * rollbacks and time travel by construction), then the in-memory registry
   * pool as the bench/test fallback for stores without catalogued
   * definitions.
   */
  private ProjectionIndexRegistry.Handle lookupProjection(final String[] sourcePath,
      final String[] requiredFields) {
    if (projectionRegistryKey == null) {
      return null;
    }
    if (wtx != null) {
      // Wtx-visible serving: controller-mediated, through the transaction's
      // own reader (uncached, read-your-writes). The bench/test pool holds
      // committed-state snapshots and must not answer for uncommitted state.
      // Runs under the transaction's lock: the flush inside drains listener
      // state, navigates the transaction, and writes through its log —
      // unlocked it would race a delay-scheduled auto-commit running the
      // same maintenance.
      final ProjectionIndexRegistry.Handle[] out = new ProjectionIndexRegistry.Handle[1];
      wtx.runLocked(() -> out[0] = wtxIndexController().openProjectionIndex(
          wtx.getStorageEngineWriter(), sourcePath, requiredFields));
      return out[0];
    }
    final ProjectionIndexRegistry.Handle catalogued = ProjectionIndexCatalog.lookupCovering(
        session, projectionRegistryKey, revision, sourcePath, requiredFields);
    if (catalogued != null) {
      return catalogued;
    }
    // The catalog is AUTHORITATIVE whenever the resource carries catalogued projection
    // definitions at this revision: a miss then means "not usable HERE" — stale tombstone,
    // no payloads, wrong shape — and the in-memory registry must not answer instead. Its
    // handles are only gated by validFromRevision, so a snapshot installed BEFORE an
    // invalidating commit would otherwise keep serving every later revision in-process
    // (stale reads the revision-scoped catalog exists to prevent). The registry fallback
    // stays for bench/test wiring only, where nothing is catalogued.
    if (ProjectionIndexCatalog.hasProjections(session, projectionRegistryKey, revision)) {
      return null;
    }
    return ProjectionIndexRegistry.lookupCovering(projectionRegistryKey, sourcePath, requiredFields,
        revision);
  }

  /**
   * A {@link ProjectionColumnStore.SegmentFetcher} bound to THIS executor's OWN live session
   * and revision — threaded into a SHARED column-lazy handle's per-column fill calls so every
   * fill reads through this reader's own transaction, never a since-closed sibling's. Cheap
   * (a closure); a caller builds one per query and reuses it across the query's fill calls.
   */
  private ProjectionColumnStore.SegmentFetcher columnFetcher() {
    return ProjectionIndexCatalog.segmentFetcher(session, revision);
  }

  /**
   * A whole-leaf materializer bound to THIS executor's OWN live session and revision, or
   * {@code null} for an eager handle whose leaves are already resident (the materializer is
   * never consulted then). Threaded into {@link ProjectionIndexRegistry.Handle#leafPayloads}
   * so a lazy handle's hydrate uses this reader's own transaction.
   */
  private Supplier<List<byte[]>> leafMaterializer(final ProjectionIndexRegistry.Handle handle) {
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    if (store == null) {
      return null;
    }
    return ProjectionIndexCatalog.leafMaterializer(session, revision, handle.defId(),
        store.leafCount());
  }

  /** The write transaction's index controller (wtx mode only). */
  private JsonIndexController wtxIndexController() {
    return session.getWtxIndexController(wtx.getRevisionNumber());
  }

  /**
   * Cheap existence probe — run BEFORE compiling predicates so resources
   * without any projection skip the compilation entirely (the common case).
   */
  private boolean anyProjectionAvailable() {
    if (projectionRegistryKey == null) {
      return false;
    }
    if (wtx != null) {
      return wtxIndexController().getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION) > 0;
    }
    return ProjectionIndexCatalog.hasProjections(session, projectionRegistryKey, revision)
        || ProjectionIndexRegistry.hasProjections(projectionRegistryKey);
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
      if (wtx != null) {
        // Wtx mode: projection-only, no result caches — see executeGroupByCount.
        return tryProjectionIndexCountDistinct(sourcePath, field);
      }
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
    final ProjectionIndexRegistry.Handle handle =
        lookupProjection(sourcePath, new String[] { field });
    if (handle == null) return null;
    final int groupColumn = handle.columnOf(field);
    if (groupColumn < 0) return null;
    // Sparse-evidence gate: without presence data, rows MISSING the field
    // would contribute the "" default as a phantom distinct value.
    if (!handle.columnSparseClean(groupColumn, columnFetcher(), leafMaterializer(handle))) {
      return null;
    }
    // Dictionary-union fast path: with the column sparse-clean, every
    // non-empty dict entry was interned by a real present row, so the
    // distinct set is the union of the per-leaf dictionaries — no per-row
    // scan at all (only a "" entry needs per-row disambiguation, and only
    // on leaves with missing rows). Valid because count-distinct has no
    // predicate: every row counts.
    final long dictDistinct = parallelDistinctPresentStrings(handle, groupColumn);
    if (dictDistinct >= 0) {
      return new Int64(dictDistinct);
    }
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
   * Parallel driver for {@link ProjectionIndexByteScan#distinctPresentStrings}:
   * per-worker dictionary unions over leaf chunks, merged with a final
   * byte-wise dedupe (both sides are canonical-cardinality small). Returns
   * the exact distinct-present count, or {@code -1} when any chunk declines
   * (kind mismatch / malformed leaf / cardinality beyond
   * {@link #COUNT_DISTINCT_DICT_CARD_LIMIT}) — callers fall back.
   */
  private long parallelDistinctPresentStrings(final ProjectionIndexRegistry.Handle handle,
      final int groupColumn) {
    final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
    if (leafPayloads == null) return -1L;
    if (leafPayloads.isEmpty()) return 0L;
    final int leafCount = leafPayloads.size();
    if (leafCount < 64) {
      final ArrayList<byte[]> set =
          ProjectionIndexByteScan.distinctPresentStrings(leafPayloads, groupColumn, COUNT_DISTINCT_DICT_CARD_LIMIT);
      return set == null ? -1L : set.size();
    }
    final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
    @SuppressWarnings("unchecked")
    final ArrayList<byte[]>[] perThread = new ArrayList[eff];
    final boolean[] declined = new boolean[1];
    final int chunkSize = (leafCount + eff - 1) / eff;
    parallel(eff, idx -> {
      final int from = idx * chunkSize;
      final int to = Math.min(from + chunkSize, leafCount);
      if (from >= to) return;
      final ArrayList<byte[]> local = ProjectionIndexByteScan.distinctPresentStrings(
          leafPayloads.subList(from, to), groupColumn, COUNT_DISTINCT_DICT_CARD_LIMIT);
      if (local == null) {
        declined[0] = true;
      } else {
        perThread[idx] = local;
      }
    });
    if (declined[0]) return -1L;
    final ArrayList<byte[]> merged = new ArrayList<>(16);
    for (final ArrayList<byte[]> local : perThread) {
      if (local == null) continue;
      for (final byte[] value : local) {
        boolean present = false;
        for (int c = 0; c < merged.size(); c++) {
          if (Arrays.equals(merged.get(c), value)) {
            present = true;
            break;
          }
        }
        if (!present) {
          if (merged.size() >= COUNT_DISTINCT_DICT_CARD_LIMIT) return -1L;
          merged.add(value);
        }
      }
    }
    return merged.size();
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
      if (wtx != null) {
        // Wtx mode: projection-only (path-summary stats and the generic
        // kernels are committed-revision scoped), no result caches.
        final long[] projected = tryProjectionAggregate(sourcePath, field, null);
        if (projected != null) {
          return longStatsToSequence(func, projected[0], projected[1], projected[2], projected[3]);
        }
        return tryServeDoubleAggregate(sourcePath, field, null, func);
      }
      // PathStatistics short-circuit: when the resource maintains per-path stats, an
      // unfiltered aggregate over a single field resolves directly from the PathSummary
      // — microseconds instead of a parallel scan of every data page.
      final Sequence fast = tryPathSummaryStats(sourcePath, field, func);
      if (fast != null) {
        return fast;
      }
      // Descriptor-tier bare count (P5b stage 1): count($doc[]) needs only the per-leaf
      // rowCounts, which live in the ~30-byte PIXD slot values — one metadata read + one
      // trie walk, ZERO segment-page hydrates AND no parallel document scan (the path this
      // shape otherwise takes: parallelAggregate's visited tally over every leaf page).
      // Exact-root candidate selection keeps the semantics identical: projection rows are
      // one-per-record under the same root, maintained every commit; stale/mismatched
      // stores decline via the probe and fall through.
      if ("count".equals(func) && field == null && projectionRegistryKey != null) {
        final long fromDescriptors = ProjectionIndexCatalog.countRowsFromDescriptors(
            session, projectionRegistryKey, revision, sourcePath);
        if (fromDescriptors >= 0) {
          return new Int64(fromDescriptors);
        }
      }
      // The executor is bound to a single (session, revision) — any aggregate we
      // computed earlier for this field is still valid. ComputeIfAbsent keeps
      // the scan exactly-once even under concurrent callers.
      final String cacheKey = pathCacheKey(sourcePath, field);
      final Sequence servedDbl = servedDoubleAggCache.get(func + '#' + cacheKey);
      if (servedDbl != null) {
        return servedDbl;
      }
      long[] stats = aggregateCache.get(cacheKey);
      MixedAgg dblStats = aggregateDblCache.get(cacheKey);
      if (stats == null && dblStats == null) {
        // Projection fast path: NUMERIC_LONG column sweep over in-memory leaves
        // (the long-only column excludes non-integral values by construction, so
        // no typed redo can be needed when it answers).
        final long[] projected = tryProjectionAggregate(sourcePath, field, null);
        if (projected != null) {
          stats = aggregateCache.putIfAbsent(cacheKey, projected);
          if (stats == null) stats = projected;
        } else {
          // Double-column serving (§11-8) runs BEFORE the MixedAgg cache below can be
          // populated for this key, and caches its own per-func result: repeats are O(1),
          // and a MixedAgg cached while the projection was still hydrating can never
          // shadow served digits (review finding — parallel-merged MixedAgg sums are not
          // fold-order-identical to the interpreter).
          final Sequence served = tryServeDoubleAggregate(sourcePath, field, null, func);
          if (served != null) {
            final Sequence prior = servedDoubleAggCache.putIfAbsent(func + '#' + cacheKey, served);
            return prior != null ? prior : served;
          }
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
          // (sum over a half-double column measured 14% short). Redo with a
          // typed re-walk: decimal rows accumulate EXACTLY (the interpreter
          // sums xs:decimal exactly and divides via Dec#div), double rows in
          // double space.
          final MixedAgg mfresh = parallelAggregateMixed(field, targetPathNodeKey);
          dblStats = aggregateDblCache.putIfAbsent(cacheKey, mfresh);
          if (dblStats == null) dblStats = mfresh;
        } else {
          stats = aggregateCache.putIfAbsent(cacheKey, fresh);
          if (stats == null) stats = fresh;
        }
      }
      if (dblStats != null) {
        return dblStats.result(func);
      }
      return longStatsToSequence(func, stats[0], stats[1], stats[2], stats[3]);
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized aggregate failed: %s",
                               e.getMessage());
    }
  }

  /**
   * The SINGLE conversion from a {@code [count, sum, min, max]} long-stats
   * accumulator to the aggregate result Sequence — used by the committed
   * aggregate tail, the wtx-mode aggregate branch, and the predicated
   * aggregate path, so the tuned semantics can never drift between them:
   * integer avg is xs:decimal via brackit's own division (digit-for-digit
   * with the generic pipeline), and avg/min/max over ZERO contributing rows
   * are the EMPTY sequence, not 0. Unknown {@code func} → {@code null}
   * (caller falls back).
   */
  /**
   * PER-GROUP aggregate serving (P5b stage 7a; gap item 1a widened it to MULTI-KEY):
   * {@code group by <string field(s)>} with any mix of {@code count($r)} and
   * {@code sum|min|max|avg($r.field)} record entries, folded per group by
   * {@link ProjectionIndexByteScan#conjunctiveAggregateByGroup} (one key) or
   * {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupMulti} (2..5 keys) over the
   * covering projection's leaves. Groups emit in DOCUMENT first-appearance order (the
   * interpreter's grouping order); matching rows missing a group field carry the
   * empty-sequence key for that component (single-key: the null-key group). Returns
   * {@code null} to fall back (callers compile the generic pipeline alongside).
   */
  public Sequence executeGroupByAggregate(final QueryContext ctx, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] groupFields, final String[] keyNames,
      final String[] funcs, final String[] aggFields, final String[] outNames) {
    try {
      if (projectionRegistryKey == null || !anyProjectionAvailable()) {
        return null;
      }
      final int keyCount = groupFields.length;
      if (keyCount < 1 || keyCount > ProjectionIndexByteScan.MAX_GROUP_COLUMNS
          || keyNames.length != keyCount) {
        return null;
      }
      final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
      final ArrayList<String> required = new ArrayList<>();
      for (final String g : groupFields) {
        if (!required.contains(g)) {
          required.add(g);
        }
      }
      for (final String f : aggFields) {
        if (f != null && !required.contains(f)) {
          required.add(f);
        }
      }
      final ProjectionIndexRegistry.Handle handle =
          lookupProjection(sourcePath, requiredFields(required.toArray(new String[0]), cp));
      if (handle == null) {
        return null;
      }
      final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
      final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
      final int[] groupCols = new int[keyCount];
      for (int g = 0; g < keyCount; g++) {
        final int col = handle.columnOf(groupFields[g]);
        if (col < 0
            || handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
            || !handle.columnSparseClean(col, fetcher, materializer)) {
          return null;
        }
        groupCols[g] = col;
      }
      final int groupCol = groupCols[0];
      // Distinct aggregate columns, gated exactly like the plain long-aggregate path.
      final ArrayList<String> distinctFields = new ArrayList<>();
      for (final String f : aggFields) {
        if (f != null && !distinctFields.contains(f)) {
          distinctFields.add(f);
        }
      }
      final int[] aggCols = new int[distinctFields.size()];
      for (int i = 0; i < aggCols.length; i++) {
        final int col = handle.columnOf(distinctFields.get(i));
        if (col < 0
            || handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
            || !handle.numericColumnIsIntegral(col, fetcher)
            || !handle.columnSparseClean(col, fetcher, materializer)) {
          return null;
        }
        aggCols[i] = col;
      }
      final ProjectionIndexScan.ColumnPredicate[] preds;
      if (cp == null) {
        preds = new ProjectionIndexScan.ColumnPredicate[0];
      } else {
        if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) {
          return null;
        }
        final ProjectionIndexScan.ColumnPredicate[] extracted =
            extractConjunctivePredicates(cp, handle);
        if (extracted == null) {
          return null; // OR trees on the group path: later stage
        }
        preds = fuseRangePredicates(extracted);
      }
      final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
      if (leafPayloads == null) {
        return null;
      }
      if (leafPayloads.isEmpty()) {
        return new ItemSequence();
      }
      final int leafCount = leafPayloads.size();
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final int chunkSize = (leafCount + eff - 1) / eff;
      if (keyCount > 1) {
        // MULTI-KEY path (gap 1a): composite GroupKey accumulators; missing components
        // ride inside the key (null part) rather than a separate null-key accumulator.
        @SuppressWarnings("unchecked")
        final Object2ObjectOpenHashMap<ProjectionIndexByteScan.GroupKey, long[]>[] perThreadMulti =
            new Object2ObjectOpenHashMap[eff];
        parallel(eff, idx -> {
          final int from = idx * chunkSize;
          final int to = Math.min(from + chunkSize, leafCount);
          if (from >= to) return;
          final Object2ObjectOpenHashMap<ProjectionIndexByteScan.GroupKey, long[]> local =
              new Object2ObjectOpenHashMap<>();
          ProjectionIndexByteScan.conjunctiveAggregateByGroupMulti(leafPayloads.subList(from, to),
              preds, groupCols, aggCols, local, from);
          perThreadMulti[idx] = local;
        });
        final Object2ObjectOpenHashMap<ProjectionIndexByteScan.GroupKey, long[]> mergedMulti =
            new Object2ObjectOpenHashMap<>();
        for (int t = 0; t < eff; t++) {
          if (perThreadMulti[t] == null) {
            continue;
          }
          for (final Object2ObjectMap.Entry<ProjectionIndexByteScan.GroupKey, long[]> e
              : perThreadMulti[t].object2ObjectEntrySet()) {
            final long[] existing = mergedMulti.get(e.getKey());
            if (existing == null) {
              mergedMulti.put(e.getKey(), e.getValue());
            } else {
              mergeGroupAgg(existing, e.getValue(), aggCols.length);
            }
          }
        }
        // Emit in document first-appearance order — the interpreter's grouping order.
        final ArrayList<Object2ObjectMap.Entry<ProjectionIndexByteScan.GroupKey, long[]>> orderedMulti =
            new ArrayList<>(mergedMulti.object2ObjectEntrySet());
        orderedMulti.sort(Comparator.comparingLong(e -> e.getValue()[1]));
        final ArrayList<Item> outMulti = new ArrayList<>(orderedMulti.size());
        for (final Object2ObjectMap.Entry<ProjectionIndexByteScan.GroupKey, long[]> e : orderedMulti) {
          outMulti.add(groupAggRecordMulti(e.getKey(), e.getValue(), keyNames, funcs, aggFields,
              outNames, distinctFields));
        }
        GROUP_AGG_SERVED.increment();
        return new ItemSequence(outMulti.toArray(new Item[0]));
      }
      @SuppressWarnings("unchecked")
      final Object2ObjectOpenHashMap<String, long[]>[] perThread = new Object2ObjectOpenHashMap[eff];
      final long[][] perThreadMissing = new long[eff][];
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final Object2ObjectOpenHashMap<String, long[]> local = new Object2ObjectOpenHashMap<>();
        final long[] missing = ProjectionIndexByteScan.newGroupAggAcc(aggCols.length, Long.MAX_VALUE);
        ProjectionIndexByteScan.conjunctiveAggregateByGroup(leafPayloads.subList(from, to), preds,
            groupCol, aggCols, local, missing, from);
        perThread[idx] = local;
        perThreadMissing[idx] = missing;
      });
      final Object2ObjectOpenHashMap<String, long[]> merged = new Object2ObjectOpenHashMap<>();
      final long[] missingMerged = ProjectionIndexByteScan.newGroupAggAcc(aggCols.length, Long.MAX_VALUE);
      for (int t = 0; t < eff; t++) {
        if (perThread[t] != null) {
          for (final Object2ObjectMap.Entry<String, long[]> e : perThread[t].object2ObjectEntrySet()) {
            final long[] existing = merged.get(e.getKey());
            if (existing == null) {
              merged.put(e.getKey(), e.getValue());
            } else {
              mergeGroupAgg(existing, e.getValue(), aggCols.length);
            }
          }
        }
        if (perThreadMissing[t] != null && perThreadMissing[t][0] > 0) {
          mergeGroupAgg(missingMerged, perThreadMissing[t], aggCols.length);
        }
      }
      // Emit in document first-appearance order — the interpreter's grouping order.
      final ArrayList<Object2ObjectMap.Entry<String, long[]>> ordered =
          new ArrayList<>(merged.object2ObjectEntrySet());
      ordered.sort(Comparator.comparingLong(e -> e.getValue()[1]));
      final ArrayList<Item> out = new ArrayList<>(ordered.size() + 1);
      int emittedMissing = 0;
      for (final Object2ObjectMap.Entry<String, long[]> e : ordered) {
        if (missingMerged[0] > 0 && emittedMissing == 0 && missingMerged[1] < e.getValue()[1]) {
          out.add(groupAggRecord(null, missingMerged, keyNames[0], funcs, aggFields, outNames,
              distinctFields));
          emittedMissing = 1;
        }
        out.add(groupAggRecord(e.getKey(), e.getValue(), keyNames[0], funcs, aggFields, outNames,
            distinctFields));
      }
      if (missingMerged[0] > 0 && emittedMissing == 0) {
        out.add(groupAggRecord(null, missingMerged, keyNames[0], funcs, aggFields, outNames,
            distinctFields));
      }
      GROUP_AGG_SERVED.increment();
      return new ItemSequence(out.toArray(new Item[0]));
    } catch (final ArithmeticException overflow) {
      // Expected decline, not a defect: an overflowing per-group sum routes to the
      // interpreter's decimal-promoting arithmetic via the generic pipeline.
      return null;
    } catch (final RuntimeException e) {
      // Fail soft — the compiled generic pipeline answers correctly. But an EXCEPTION
      // here (unlike a gate decline) means a defect or corruption, and a silent 100%
      // fallback would hide it — log it and count it so drift is observable.
      GROUP_AGG_FAILED.increment();
      if (PROJ_DIAG) {
        System.err.println("[proj] group-aggregate serving failed, using generic pipeline: " + e);
      }
      return null;
    }
  }

  /**
   * Sorted-scan serving (P5b stage 7b; gap 1b generalized to N order keys): matching
   * {@code (sortTuple, recordKey)} rows collected from the covering projection, stably
   * sorted by tuple with PER-KEY direction (document-order tiebreak — the collector
   * appends in document order and the sort is stable), record keys returned for LAZY
   * materialization by the caller. {@code null} = decline (callers evaluate the generic
   * pipeline compiled alongside). A matching row MISSING any order field declines
   * outright: the interpreter sorts empty order keys per the empty-least/greatest mode
   * (default empty least — it does NOT error), a placement the long-tuple kernel cannot
   * represent, so only the generic pipeline can emit those rows in the right position.
   *
   * <p>Gap 3: {@code limit >= 0} caps the result to the first {@code limit} rows of the
   * full stable sort — sole-consumer {@code fn:subsequence} truncation — selected via a
   * bounded heap ({@code n log K}) instead of the full {@code n log n} sort.
   *
   * @return record keys in emission order, or {@code null} to fall back
   */
  public long[] sortedScanRecordKeys(final String[] sourcePath, final PredicateNode predicateOrNull,
      final String[] orderFields, final boolean[] descending, final long limit) {
    try {
      if (projectionRegistryKey == null || !anyProjectionAvailable()) {
        return null;
      }
      final int keyCount = orderFields.length;
      if (keyCount < 1 || descending.length != keyCount) {
        return null;
      }
      final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
      final ProjectionIndexRegistry.Handle handle =
          lookupProjection(sourcePath, requiredFields(orderFields, cp));
      if (handle == null) {
        return null;
      }
      final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
      final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
      final int[] cols = new int[keyCount];
      for (int k = 0; k < keyCount; k++) {
        final int col = handle.columnOf(orderFields[k]);
        if (col < 0
            || handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
            || !handle.numericColumnIsIntegral(col, fetcher)
            || !handle.columnSparseClean(col, fetcher, materializer)) {
          return null;
        }
        cols[k] = col;
      }
      final ProjectionIndexScan.ColumnPredicate[] preds;
      if (cp == null) {
        preds = new ProjectionIndexScan.ColumnPredicate[0];
      } else {
        if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) {
          return null;
        }
        final ProjectionIndexScan.ColumnPredicate[] extracted =
            extractConjunctivePredicates(cp, handle);
        if (extracted == null) {
          return null;
        }
        preds = fuseRangePredicates(extracted);
      }
      final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
      if (leafPayloads == null) {
        return null;
      }
      final LongArrayList values = new LongArrayList();
      final LongArrayList keys = new LongArrayList();
      final LongArrayList missingKeys = new LongArrayList();
      ProjectionIndexByteScan.collectMatchingSortTuples(leafPayloads, preds, cols, values, keys,
          missingKeys);
      if (!missingKeys.isEmpty()) {
        // The interpreter sorts rows with EMPTY order keys per the empty-least/greatest
        // mode (default: empty least, first in ascending order) — it does NOT error.
        // The long-tuple kernel has no representation for an empty key, so serving
        // cannot place those rows; decline BEFORE paying the sort and let the generic
        // pipeline emit them in the spec-correct position.
        return null;
      }
      final int n = keys.size();
      final long[] tuple = values.elements();
      if (limit >= 0 && limit < n) {
        // TOP-K (gap 3): the sole consumer never pulls past row `limit` of the stable
        // sort. Bounded max-heap of the K best row indices (root = worst kept) under the
        // same strict total order (per-key direction + document-order tiebreak), so the
        // kept set IS the first K rows of the full stable sort. n log K, no full sort.
        final int k = (int) limit;
        if (k == 0) {
          SORTED_TOPK_APPLIED.increment();
          return new long[0];
        }
        final int[] heap = new int[k];
        for (int i = 0; i < k; i++) {
          heap[i] = i;
        }
        for (int i = (k >>> 1) - 1; i >= 0; i--) {
          siftDownWorst(heap, i, k, tuple, keyCount, descending);
        }
        for (int i = k; i < n; i++) {
          if (compareSortRows(tuple, keyCount, descending, i, heap[0]) < 0) {
            heap[0] = i;
            siftDownWorst(heap, 0, k, tuple, keyCount, descending);
          }
        }
        IntArrays.mergeSort(heap, (a, b) -> compareSortRows(tuple, keyCount, descending, a, b));
        final long[] topOut = new long[k];
        for (int i = 0; i < k; i++) {
          topOut[i] = keys.getLong(heap[i]);
        }
        SORTED_TOPK_APPLIED.increment();
        return topOut;
      }
      // Stable primitive index sort (document-order base, no boxing): tuple order with
      // per-key direction and index tiebreak reproduces the interpreter's stable
      // `order by k1, k2, ...`. Values sit row-major with stride keyCount.
      final int[] order = new int[n];
      for (int i = 0; i < n; i++) {
        order[i] = i;
      }
      final IntComparator byValue = (a, b) ->
          compareSortRows(tuple, keyCount, descending, order[a], order[b]);
      it.unimi.dsi.fastutil.Arrays.mergeSort(0, n, byValue, (a, b) -> {
        final int tmp = order[a];
        order[a] = order[b];
        order[b] = tmp;
      });
      final long[] out = new long[n];
      for (int i = 0; i < n; i++) out[i] = keys.getLong(order[i]);
      return out;
    } catch (final RuntimeException e) {
      SORTED_SCAN_FAILED.increment();
      if (PROJ_DIAG) {
        System.err.println("[proj] sorted-scan serving failed, using generic pipeline: " + e);
      }
      return null;
    }
  }

  /**
   * Row-index comparator for sorted-scan tuples: per-key direction over the row-major
   * {@code tuple} values (stride {@code keyCount}), DOCUMENT-ORDER index tiebreak — a
   * strict total order, so bounded selection and the full stable sort agree exactly.
   */
  private static int compareSortRows(final long[] tuple, final int keyCount,
      final boolean[] descending, final int a, final int b) {
    final int ra = a * keyCount;
    final int rb = b * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = Long.compare(tuple[ra + k], tuple[rb + k]);
      if (cmp != 0) {
        return descending[k] ? -cmp : cmp;
      }
    }
    return Integer.compare(a, b);
  }

  /** Max-heap sift-down (root = WORST kept row) under {@link #compareSortRows}. */
  private static void siftDownWorst(final int[] heap, final int start, final int size,
      final long[] tuple, final int keyCount, final boolean[] descending) {
    int i = start;
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size
          && compareSortRows(tuple, keyCount, descending, heap[right], heap[child]) > 0) {
        child = right;
      }
      if (compareSortRows(tuple, keyCount, descending, heap[child], heap[i]) <= 0) {
        return;
      }
      final int tmp = heap[i];
      heap[i] = heap[child];
      heap[child] = tmp;
      i = child;
    }
  }

  /** Sorted scans answered via BOUNDED top-K selection (gap 3) — test observability. */
  private static final LongAdder SORTED_TOPK_APPLIED = new LongAdder();

  /** Test/ops observability for {@link #SORTED_TOPK_APPLIED}. */
  public static long sortedTopKAppliedCount() {
    return SORTED_TOPK_APPLIED.sum();
  }

  /** Mark one sorted scan as actually EMITTED (called by the expr after materialization). */
  public static void markSortedScanServed() {
    SORTED_SCAN_SERVED.increment();
  }

  /** Sorted-scan serving attempts that FAILED with an exception (not gate declines). */
  private static final LongAdder SORTED_SCAN_FAILED = new LongAdder();

  /** Test/ops observability for {@link #SORTED_SCAN_FAILED}. */
  public static long sortedScanFailedCount() {
    return SORTED_SCAN_FAILED.sum();
  }

  /**
   * Fresh read transaction at the executor's bound revision — backs sorted-scan record
   * materialization (P5b stage 7b). Session-owned: closes with the session.
   */
  public JsonNodeReadOnlyTrx openRecordTrx() {
    return session.beginNodeReadOnlyTrx(revision);
  }

  /** Cached record-materialization trx (see {@link #recordTrx()}); executor-owned. */
  private volatile JsonNodeReadOnlyTrx recordTrx;

  /**
   * The executor's ONE cached record-materialization transaction — materialized items
   * read fields through it lazily during serialization, so it lives until the executor
   * closes (a per-query trx would leak one open transaction per served query).
   */
  public JsonNodeReadOnlyTrx recordTrx() {
    JsonNodeReadOnlyTrx trx = recordTrx;
    if (trx == null) {
      synchronized (this) {
        trx = recordTrx;
        if (trx == null) {
          trx = session.beginNodeReadOnlyTrx(revision);
          recordTrx = trx;
        }
      }
    }
    return trx;
  }

  /** Failure hook for the sorted-scan expr (counts + optional diagnostics). */
  public static void markSortedScanFailed(final RuntimeException e) {
    SORTED_SCAN_FAILED.increment();
    if (PROJ_DIAG) {
      System.err.println("[proj] sorted-scan materialization failed, using generic pipeline: " + e);
    }
  }

  /** Database name of the bound resource (layout {@code <database>/data/<resource>}). */
  public String boundDatabaseName() {
    return session.getResourceConfig().getResource().getParent().getParent().getFileName().toString();
  }

  /**
   * COVERED-ROW serving (P5b stage 7c): predicate-matching rows materialized as records
   * straight from projection segments — the document store is never touched. Field values
   * type exactly like the interpreter's derefs (Int64 / Dbl via the double transform /
   * Bool / Str); a missing field stores the EMPTY sequence in the record, exactly like the
   * interpreter's object constructor (serialized as JSON null, but empty under
   * composition). {@code null} = decline; callers evaluate the generic pipeline compiled
   * alongside.
   */
  public Sequence executeRowMaterialize(final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] fields, final String[] outNames,
      final int[] direct, final int[][] codes, final long[][] consts) {
    try {
      if (projectionRegistryKey == null || !anyProjectionAvailable()) {
        return null;
      }
      final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
      final ProjectionIndexRegistry.Handle handle =
          lookupProjection(sourcePath, requiredFields(fields, cp));
      if (handle == null) {
        return null;
      }
      final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
      final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
      final int n = fields.length;
      final int[] cols = new int[n];
      final byte[] kinds = new byte[n];
      for (int i = 0; i < n; i++) {
        final int col = handle.columnOf(fields[i]);
        if (col < 0 || !handle.columnSparseClean(col, fetcher, materializer)) {
          return null;
        }
        final byte kind = handle.columnKindOf(col);
        // Emission understands exactly these four kinds; anything else (a future encoding,
        // corrupt metadata) must decline rather than fall into the Int64 default arm.
        switch (kind) {
          case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT:
          case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN:
            break;
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG:
            if (!handle.numericColumnIsIntegral(col, fetcher)) {
              return null; // value-exact gate, same as aggregate serving
            }
            break;
          case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE:
            // Dbl(decode(bits)) reproduces the interpreter's deref ONLY when every stored
            // value came from a real Double/Float source (same gate as double aggregates).
            if (!handle.doubleColumnPureSource(col, fetcher)) {
              return null;
            }
            break;
          default:
            return null;
        }
        cols[i] = col;
        kinds[i] = kind;
      }
      // Computed entries (gap 2): every operand slot must be an EXACT long column — the
      // program computes in long space with Math.*Exact semantics.
      final int entryCount = outNames.length;
      if (direct.length != entryCount || codes.length != entryCount
          || consts.length != entryCount) {
        return null;
      }
      int maxCodeLen = 0;
      for (int e = 0; e < entryCount; e++) {
        if (direct[e] >= 0) {
          if (direct[e] >= n) {
            return null;
          }
          continue;
        }
        final int[] code = codes[e];
        if (!validProgram(code, consts[e], n)) {
          return null;
        }
        if (code.length > maxCodeLen) {
          maxCodeLen = code.length;
        }
        for (final int slot : code) {
          if (slot >= 0 && slot < ProjectionIndexByteScan.COMPUTED_CONST_BASE
              && kinds[slot] != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
            return null; // computed operands compute in exact long space only
          }
        }
      }
      final ProjectionIndexScan.ColumnPredicate[] preds;
      if (cp == null) {
        preds = new ProjectionIndexScan.ColumnPredicate[0];
      } else {
        if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) {
          return null;
        }
        final ProjectionIndexScan.ColumnPredicate[] extracted =
            extractConjunctivePredicates(cp, handle);
        if (extracted == null) {
          return null;
        }
        preds = fuseRangePredicates(extracted);
      }
      final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
      if (leafPayloads == null) {
        return null;
      }
      // Materialization is eager (every matching row becomes a heap-resident record before
      // the sequence returns) — cap by TOTAL store rows, the cheap upper bound on matches,
      // so a huge store routes to the streaming generic pipeline instead of an OOM.
      if (ProjectionIndexByteScan.countRows(leafPayloads) > ROW_MAT_MAX_ROWS) {
        return null;
      }
      final QNm[] names = new QNm[entryCount];
      for (int i = 0; i < entryCount; i++) {
        names[i] = new QNm(outNames[i]);
      }
      final long[] stack = new long[Math.max(1, maxCodeLen)];
      final ArrayList<Item> rows = new ArrayList<>();
      ProjectionIndexByteScan.materializeMatchingRows(leafPayloads, preds, cols, kinds,
          (longVals, stringVals, present) -> {
            final Sequence[] vals = new Sequence[entryCount];
            for (int e = 0; e < entryCount; e++) {
              final int d = direct[e];
              if (d < 0) {
                // Computed entry (gap 2): any missing operand makes the interpreter's
                // arithmetic empty — store the empty sequence; overflow throws and the
                // whole query declines to the promoting interpreter.
                vals[e] = evalRowProgram(codes[e], consts[e], longVals, present, stack);
                continue;
              }
              if (!present[d]) {
                // The interpreter's object constructor stores the EMPTY sequence (Java
                // null) for a missing deref — not Null.INSTANCE. Serialization is
                // identical, but composition (count((pipe).a)) observes the difference.
                vals[e] = null;
                continue;
              }
              vals[e] = switch (kinds[d]) {
                case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> new Str(stringVals[d]);
                case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN ->
                    longVals[d] != 0 ? Bool.TRUE : Bool.FALSE;
                case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE ->
                    new Dbl(ProjectionDoubleEncoding.decode(longVals[d]));
                default -> new Int64(longVals[d]);
              };
            }
            rows.add(new ArrayObject(names, vals));
          });
      ROW_MAT_SERVED.increment();
      return new ItemSequence(rows.toArray(new Item[0]));
    } catch (final ArithmeticException overflow) {
      // Expected decline: exact-math overflow in a computed entry routes the whole query
      // to the interpreter's decimal-promoting arithmetic.
      return null;
    } catch (final RuntimeException e) {
      ROW_MAT_FAILED.increment();
      if (PROJ_DIAG) {
        System.err.println("[proj] covered-row serving failed, using generic pipeline: " + e);
      }
      return null;
    }
  }

  /**
   * Covered-row serving materializes EAGERLY (a record per matching row, all heap-resident
   * before the sequence returns). Stores above this many TOTAL rows decline to the
   * streaming generic pipeline instead of risking an OOM on a broad predicate.
   */
  private static final long ROW_MAT_MAX_ROWS =
      Long.getLong("sirix.projection.rowMaterializeMaxRows", 1_000_000L);

  /** Computed-expression aggregate servings (gap 2) — test observability. */
  private static final LongAdder COMPUTED_AGG_SERVED = new LongAdder();

  /** Total computed-expression aggregates SERVED from a projection so far. */
  public static long computedAggServedCount() {
    return COMPUTED_AGG_SERVED.sum();
  }

  /** Computed-aggregate attempts that FAILED with an exception (not declines/overflow). */
  private static final LongAdder COMPUTED_AGG_FAILED = new LongAdder();

  /** Test/ops observability for {@link #COMPUTED_AGG_FAILED}. */
  public static long computedAggFailedCount() {
    return COMPUTED_AGG_FAILED.sum();
  }

  /**
   * COMPUTED-EXPRESSION aggregate serving (gap item 2):
   * {@code sum|avg|min|max|count(for $r in P [where p] return <+/-/* tree over $r.fields
   * and int literals>)} folded by
   * {@link ProjectionIndexByteScan#conjunctiveAggregateComputed} with EXACT arithmetic.
   * Overflow anywhere (program or running sum) declines — Brackit's interpreter promotes
   * overflowing integer math to exact decimal, so only the generic pipeline answers those
   * digit-exactly. Rows missing any operand field contribute nothing (empty arithmetic).
   * {@code null} = decline; callers evaluate the generic expression compiled alongside.
   */
  public Sequence executeComputedAggregate(final String[] sourcePath,
      final PredicateNode predicateOrNull, final String func, final String[] fields,
      final int[] code, final long[] consts) {
    try {
      if (projectionRegistryKey == null || !anyProjectionAvailable()) {
        return null;
      }
      if (!validProgram(code, consts, fields.length)) {
        return null; // untrusted-boundary re-validation, same as the row-materialize path
      }
      final CompiledPredicate cp = predicateOrNull == null ? null : compile(predicateOrNull);
      final ProjectionIndexRegistry.Handle handle =
          lookupProjection(sourcePath, requiredFields(fields, cp));
      if (handle == null) {
        return null;
      }
      final ProjectionColumnStore.SegmentFetcher fetcher = columnFetcher();
      final Supplier<List<byte[]>> materializer = leafMaterializer(handle);
      final int[] cols = new int[fields.length];
      for (int i = 0; i < fields.length; i++) {
        final int col = handle.columnOf(fields[i]);
        if (col < 0
            || handle.columnKindOf(col) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
            || !handle.numericColumnIsIntegral(col, fetcher)
            || !handle.columnSparseClean(col, fetcher, materializer)) {
          return null; // value-exact gate, same as plain aggregate serving
        }
        cols[i] = col;
      }
      final ProjectionIndexScan.ColumnPredicate[] preds;
      if (cp == null) {
        preds = new ProjectionIndexScan.ColumnPredicate[0];
      } else {
        if (!ProjectionIndexRegistry.covers(handle, cp.fieldNames)) {
          return null;
        }
        final ProjectionIndexScan.ColumnPredicate[] extracted =
            extractConjunctivePredicates(cp, handle);
        if (extracted == null) {
          return null;
        }
        preds = fuseRangePredicates(extracted);
      }
      final List<byte[]> leafPayloads = leafPayloadsOrNull(handle);
      if (leafPayloads == null) {
        return null;
      }
      final int leafCount = leafPayloads.size();
      final int eff = Math.min(threads, Math.max(1, (leafCount + 63) / 64));
      final int chunkSize = leafCount == 0 ? 1 : (leafCount + eff - 1) / eff;
      final long[][] perThread = new long[eff][];
      // The kernel throws ArithmeticException on overflow; parallel() propagates it.
      parallel(eff, idx -> {
        final int from = idx * chunkSize;
        final int to = Math.min(from + chunkSize, leafCount);
        if (from >= to) return;
        final long[] acc = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionIndexByteScan.conjunctiveAggregateComputed(leafPayloads.subList(from, to),
            preds, cols, code, consts, acc);
        perThread[idx] = acc;
      });
      long count = 0;
      long sum = 0;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (final long[] acc : perThread) {
        if (acc == null) {
          continue;
        }
        count += acc[0];
        sum = Math.addExact(sum, acc[1]);
        if (acc[2] < min) min = acc[2];
        if (acc[3] > max) max = acc[3];
      }
      final Sequence result = longStatsToSequence(func, count, sum, min, max);
      if (result == null) {
        return null; // unknown func — decline, never guess
      }
      COMPUTED_AGG_SERVED.increment();
      return result;
    } catch (final ArithmeticException overflow) {
      // Expected decline, not a defect: exact-math overflow routes to the interpreter's
      // decimal-promoting arithmetic via the generic expression.
      return null;
    } catch (final RuntimeException e) {
      COMPUTED_AGG_FAILED.increment();
      if (PROJ_DIAG) {
        System.err.println("[proj] computed-aggregate serving failed, using generic: " + e);
      }
      return null;
    }
  }

  /**
   * Re-validate a postfix program at the EXECUTOR boundary — AST properties are untrusted
   * inputs (cached plans, foreign producers): every push slot in range, every opcode
   * known, running depth never below 1, final depth exactly 1, non-empty. Anything else
   * declines — a depth-surplus or empty program would otherwise SERVE a silently wrong
   * value (the reused scratch stack's slot 0), not crash.
   */
  private static boolean validProgram(final int[] code, final long[] consts, final int nFields) {
    if (code == null || consts == null || code.length == 0) {
      return false;
    }
    int depth = 0;
    for (final int slot : code) {
      if (slot >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
        if (slot - ProjectionIndexByteScan.COMPUTED_CONST_BASE >= consts.length) {
          return false;
        }
        depth++;
      } else if (slot >= 0) {
        if (slot >= nFields) {
          return false;
        }
        depth++;
      } else {
        if (slot < ProjectionIndexByteScan.COMPUTED_OP_MUL) {
          return false; // unknown opcode
        }
        depth--;
      }
      if (depth < 1) {
        return false;
      }
    }
    return depth == 1;
  }

  /**
   * Per-row postfix evaluation for a COMPUTED record entry (gap 2): {@code null} when any
   * operand field is missing (the interpreter's arithmetic over the empty sequence is
   * empty); exact ops throw {@link ArithmeticException} on overflow, which the caller
   * treats as a whole-query decline. The {@code stack} is caller-owned scratch.
   */
  private static Int64 evalRowProgram(final int[] code, final long[] consts,
      final long[] longVals, final boolean[] present, final long[] stack) {
    for (final int op : code) {
      if (op >= 0 && op < ProjectionIndexByteScan.COMPUTED_CONST_BASE && !present[op]) {
        return null;
      }
    }
    int sp = 0;
    for (final int op : code) {
      if (op >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
        stack[sp++] = consts[op - ProjectionIndexByteScan.COMPUTED_CONST_BASE];
      } else if (op >= 0) {
        stack[sp++] = longVals[op];
      } else {
        final long b = stack[--sp];
        final long a = stack[--sp];
        stack[sp++] = switch (op) {
          case ProjectionIndexByteScan.COMPUTED_OP_ADD -> Math.addExact(a, b);
          case ProjectionIndexByteScan.COMPUTED_OP_SUB -> Math.subtractExact(a, b);
          case ProjectionIndexByteScan.COMPUTED_OP_MUL -> Math.multiplyExact(a, b);
          default -> throw new IllegalStateException("unknown computed opcode " + op);
        };
      }
    }
    return new Int64(stack[0]);
  }

  /** Covered-row servings (stage 7c) — test observability. */
  private static final LongAdder ROW_MAT_SERVED = new LongAdder();

  /** Total covered-row queries SERVED from a projection so far. */
  public static long rowMaterializeServedCount() {
    return ROW_MAT_SERVED.sum();
  }

  /** Covered-row serving attempts that FAILED with an exception (not gate declines). */
  private static final LongAdder ROW_MAT_FAILED = new LongAdder();

  /** Test/ops observability for {@link #ROW_MAT_FAILED}. */
  public static long rowMaterializeFailedCount() {
    return ROW_MAT_FAILED.sum();
  }

  /** Sorted-scan servings (stage 7b) — test observability. */
  private static final LongAdder SORTED_SCAN_SERVED = new LongAdder();

  /** Total sorted scans SERVED from a projection so far. */
  public static long sortedScanServedCount() {
    return SORTED_SCAN_SERVED.sum();
  }

  /** Group-aggregate servings that FAILED with an exception (not gate declines). */
  private static final LongAdder GROUP_AGG_FAILED = new LongAdder();

  /** Test/ops observability for {@link #GROUP_AGG_FAILED}. */
  public static long groupAggFailedCount() {
    return GROUP_AGG_FAILED.sum();
  }

  private static void mergeGroupAgg(final long[] into, final long[] from, final int aggCols) {
    into[0] += from[0];
    if (from[1] < into[1]) {
      into[1] = from[1];
    }
    for (int a = 0; a < aggCols; a++) {
      final int base = 2 + 4 * a;
      into[base] += from[base];
      // Exact merge or DECLINE — same interpreter-promotes-on-overflow rule the kernels
      // enforce per row (a wrapped cross-chunk sum is just as wrong as a wrapped fold).
      into[base + 1] = Math.addExact(into[base + 1], from[base + 1]);
      if (from[base + 2] < into[base + 2]) into[base + 2] = from[base + 2];
      if (from[base + 3] > into[base + 3]) into[base + 3] = from[base + 3];
    }
  }

  /** One per-group output record; {@code null} group key = the missing-field group. */
  private static ArrayObject groupAggRecord(final String groupKey, final long[] acc,
      final String keyName, final String[] funcs, final String[] aggFields,
      final String[] outNames, final ArrayList<String> distinctFields) {
    final QNm[] names = new QNm[1 + funcs.length];
    final Sequence[] vals = new Sequence[1 + funcs.length];
    names[0] = new QNm(keyName);
    // The interpreter's object constructor stores the EMPTY sequence (Java null) for the
    // missing-field group's key — serialized as JSON null, but empty under composition
    // (count((pipe).dept)) — so the served record must too, not Null.INSTANCE.
    vals[0] = groupKey == null ? null : new Str(groupKey);
    fillAggEntries(names, vals, 1, acc, funcs, aggFields, outNames, distinctFields);
    return new ArrayObject(names, vals);
  }

  /** MULTI-KEY per-group record (gap 1a); {@code null} key components = missing-field keys. */
  private static ArrayObject groupAggRecordMulti(final ProjectionIndexByteScan.GroupKey key,
      final long[] acc, final String[] keyNames, final String[] funcs, final String[] aggFields,
      final String[] outNames, final ArrayList<String> distinctFields) {
    final int k = keyNames.length;
    final QNm[] names = new QNm[k + funcs.length];
    final Sequence[] vals = new Sequence[k + funcs.length];
    for (int i = 0; i < k; i++) {
      names[i] = new QNm(keyNames[i]);
      final String part = key.part(i);
      // Same empty-sequence rule as the single-key record's null group.
      vals[i] = part == null ? null : new Str(part);
    }
    fillAggEntries(names, vals, k, acc, funcs, aggFields, outNames, distinctFields);
    return new ArrayObject(names, vals);
  }

  /** Shared aggregate-entry fill for single- and multi-key group records. */
  private static void fillAggEntries(final QNm[] names, final Sequence[] vals, final int offset,
      final long[] acc, final String[] funcs, final String[] aggFields, final String[] outNames,
      final ArrayList<String> distinctFields) {
    for (int i = 0; i < funcs.length; i++) {
      names[offset + i] = new QNm(outNames[i]);
      if ("count".equals(funcs[i])) {
        vals[offset + i] = new Int64(acc[0]);
        continue;
      }
      final int a = distinctFields.indexOf(aggFields[i]);
      final int base = 2 + 4 * a;
      final Sequence stat =
          longStatsToSequence(funcs[i], acc[base], acc[base + 1], acc[base + 2], acc[base + 3]);
      // longStatsToSequence returns an EMPTY ItemSequence exactly when count == 0
      // (avg/min/max over an all-missing group field) → store the empty sequence, exactly
      // what the interpreter's constructor keeps (JSON null on serialization).
      vals[offset + i] = stat instanceof ItemSequence ? null : stat;
    }
  }

  private static Sequence longStatsToSequence(final String func, final long count, final long sum,
      final long min, final long max) {
    return switch (func) {
      case "count" -> new Int64(count);
      case "sum" -> new Int64(sum);
      case "avg" -> count == 0 ? new ItemSequence() : new Int64(sum).div(new Int64(count));
      case "min" -> count == 0 ? new ItemSequence() : new Int64(min);
      case "max" -> count == 0 ? new ItemSequence() : new Int64(max);
      default -> null;
    };
  }

  /**
   * Value aggregates (sum/avg/min/max — never count) served from a double projection column
   * under the §11-8 purity gate, since process start. The catalog's {@code servedCount}
   * increments on HANDLE lookups and therefore cannot distinguish "served" from
   * "looked up, declined, fell back" — this counter is the test oracle for the distinction.
   */
  private static final LongAdder DOUBLE_VALUE_SERVED = new LongAdder();

  /** Test observability for {@link #DOUBLE_VALUE_SERVED}. */
  public static long doubleValueAggregatesServed() {
    return DOUBLE_VALUE_SERVED.sum();
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
      if (ProjectionIndexCatalog.anyDefCoversField(session, resourceKey, revision, field)
          || ProjectionIndexRegistry.anyHandleCoversField(resourceKey, field)) {
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
   * Typed accumulator for non-integral columns (the {@link #parallelAggregate}
   * redo path) and its result mapping. Per-kind accumulation mirrors the
   * interpreter's numeric promotion:
   * <ul>
   * <li>NO double rows (longs + decimals only): the interpreter folds
   * {@code Int + Dec} EXACTLY in decimal space and divides via
   * {@code Dec#div} — accumulate {@link java.math.BigDecimal} exactly and
   * delegate the avg division to brackit for digit-for-digit parity.
   * Exact accumulation is also ORDER-FREE, so the parallel fold cannot
   * drift from the interpreter's sequential fold.</li>
   * <li>any double row: the interpreter's running sum becomes xs:double and
   * stays there — fold everything in double space (the parallel
   * re-association can differ from the sequential fold in the last ulp
   * for adversarial values; exact for binary-fraction data — documented
   * limitation).</li>
   * </ul>
   */
  private static final class MixedAgg {
    long longCount;
    long longSum;
    long longMin = Long.MAX_VALUE;
    long longMax = Long.MIN_VALUE;
    long decCount;
    java.math.BigDecimal decSum = java.math.BigDecimal.ZERO;
    java.math.BigDecimal decMin;
    java.math.BigDecimal decMax;
    long dblCount;
    double dblSum;
    double dblMin = Double.MAX_VALUE;
    double dblMax = -Double.MAX_VALUE;

    void addLong(final long v) {
      longCount++;
      longSum += v;
      if (v < longMin) longMin = v;
      if (v > longMax) longMax = v;
    }

    void addDecimal(final java.math.BigDecimal v) {
      decCount++;
      decSum = decSum.add(v);
      if (decMin == null || v.compareTo(decMin) < 0) decMin = v;
      if (decMax == null || v.compareTo(decMax) > 0) decMax = v;
    }

    void addDouble(final double v) {
      dblCount++;
      dblSum += v;
      if (v < dblMin) dblMin = v;
      if (v > dblMax) dblMax = v;
    }

    void merge(final MixedAgg o) {
      if (o == null) return;
      longCount += o.longCount;
      longSum += o.longSum;
      if (o.longMin < longMin) longMin = o.longMin;
      if (o.longMax > longMax) longMax = o.longMax;
      decCount += o.decCount;
      decSum = decSum.add(o.decSum);
      if (o.decMin != null && (decMin == null || o.decMin.compareTo(decMin) < 0)) decMin = o.decMin;
      if (o.decMax != null && (decMax == null || o.decMax.compareTo(decMax) > 0)) decMax = o.decMax;
      dblCount += o.dblCount;
      dblSum += o.dblSum;
      if (o.dblMin < dblMin) dblMin = o.dblMin;
      if (o.dblMax > dblMax) dblMax = o.dblMax;
    }

    Sequence result(final String func) {
      final long count = longCount + decCount + dblCount;
      if ("count".equals(func)) {
        return new Int64(count);
      }
      if (count == 0) {
        // sum(()) = 0; avg/min/max(()) = the empty sequence.
        return "sum".equals(func) ? new Int64(0L) : new ItemSequence();
      }
      if (dblCount == 0) {
        // Decimal-exact path — Int + Dec folds are exact; division delegates
        // to brackit's Dec#div so avg matches the interpreter digit-for-digit.
        final java.math.BigDecimal sum = decSum.add(java.math.BigDecimal.valueOf(longSum));
        final java.math.BigDecimal min = minBd(decMin, longCount > 0 ? java.math.BigDecimal.valueOf(longMin) : null);
        final java.math.BigDecimal max = maxBd(decMax, longCount > 0 ? java.math.BigDecimal.valueOf(longMax) : null);
        return switch (func) {
          case "sum" -> new Dec(sum);
          case "avg" -> new Dec(sum).div(new Int64(count));
          case "min" -> new Dec(min);
          case "max" -> new Dec(max);
          default -> null;
        };
      }
      // Double-bearing column — interpreter promotion makes the whole
      // aggregate xs:double.
      final double sum = dblSum + (double) longSum + decSum.doubleValue();
      double min = dblMin;
      double max = dblMax;
      if (longCount > 0) {
        min = Math.min(min, (double) longMin);
        max = Math.max(max, (double) longMax);
      }
      if (decCount > 0) {
        min = Math.min(min, decMin.doubleValue());
        max = Math.max(max, decMax.doubleValue());
      }
      return switch (func) {
        case "sum" -> new Dbl(sum);
        case "avg" -> new Dbl(sum / count);
        case "min" -> new Dbl(min);
        case "max" -> new Dbl(max);
        default -> null;
      };
    }

    private static java.math.BigDecimal minBd(final java.math.BigDecimal a, final java.math.BigDecimal b) {
      if (a == null) return b;
      if (b == null) return a;
      return a.compareTo(b) <= 0 ? a : b;
    }

    private static java.math.BigDecimal maxBd(final java.math.BigDecimal a, final java.math.BigDecimal b) {
      if (a == null) return b;
      if (b == null) return a;
      return a.compareTo(b) >= 0 ? a : b;
    }
  }

  /**
   * Typed-accumulating aggregate walk — the redo path when
   * {@link #parallelAggregate} flags non-integral values. Pure slot walk
   * (regions only carry longs, so they cannot serve this column completely).
   */
  private MixedAgg parallelAggregateMixed(final String field, final long targetPathNodeKey) throws Exception {
    final int fieldKey = resolveFieldKey(field);
    if (fieldKey == -1) return new MixedAgg();
    final long maxNodeKey = getMaxNodeKey();
    final long totalPages = (maxNodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT) + 1;
    final int eff = (int) Math.min(threads, Math.max(1, totalPages));
    final long ppt = (totalPages + eff - 1) / eff;
    final MixedAgg[] perThread = new MixedAgg[eff];
    parallel(eff, idx -> {
      final IndexLogKey reusableKey = new IndexLogKey(IndexType.DOCUMENT, 0, 0, revision);
      final long s = (long) idx * ppt;
      final long e = Math.min(s + ppt, totalPages);
      final JsonNodeReadOnlyTrx rtx = workerTrx();
      final var reader = rtx.getStorageEngineReader();
      final MixedAgg acc = new MixedAgg();
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
          if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
            acc.addLong(n.longValue());
          } else if (n instanceof java.math.BigDecimal bd) {
            acc.addDecimal(bd);
          } else if (n instanceof java.math.BigInteger bi) {
            acc.addDecimal(new java.math.BigDecimal(bi));
          } else {
            acc.addDouble(n.doubleValue());
          }
        }
      }
      perThread[idx] = acc;
    });
    final MixedAgg merged = new MixedAgg();
    for (final MixedAgg a : perThread) {
      merged.merge(a);
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
      // Kernel-level IllegalStateExceptions are a FALLBACK signal (column-kind
      // drift, canonical-dict miss, ...): every tryProjection* caller catches
      // them to route to the typed kernels / interpreter. Rethrow them
      // unwrapped so the fallback contract holds at ANY leaf count — wrapping
      // them here made the same query succeed under 64 leaves and hard-fail
      // above (worker exceptions only occur on the parallel path).
      if (cause instanceof IllegalStateException ise) {
        throw ise;
      }
      // ArithmeticException is likewise a SIGNAL, not a failure: exact-math kernels
      // (computed aggregates) throw it on overflow and their callers decline to the
      // interpreter's decimal-promoting arithmetic. Rethrow unwrapped so the decline
      // contract holds on the parallel path exactly as it does single-threaded.
      if (cause instanceof ArithmeticException ae) {
        throw ae;
      }
      final String msg = cause.getClass().getSimpleName() + ": " + cause.getMessage();
      throw new RuntimeException("Parallel scan failed — " + msg, e);
    }
  }
}
