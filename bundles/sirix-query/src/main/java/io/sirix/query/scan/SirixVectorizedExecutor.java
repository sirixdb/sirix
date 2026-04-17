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
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.pax.NumberRegion;
import io.sirix.page.pax.NumberRegionSimd;
import io.sirix.settings.Constants;

import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
    }
  }
  /** Cached max node key — Sirix-internal, fixed for a given revision. */
  private volatile long cachedMaxNodeKey = -1;
  private final ExecutorService workerPool;
  /** Cached field-name → int nameKey resolution. Keyed once per executor lifetime. */
  private final ConcurrentHashMap<String, Integer> fieldKeyCache = new ConcurrentHashMap<>();

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
      return parallelGroupByCount(groupField);
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized group-by-count failed: %s",
                               e.getMessage());
    }
  }

  @Override
  public Sequence executeFilterCount2(QueryContext ctx,
      String field1, String op1, long value1,
      String field2, String op2, long value2) throws QueryException {
    try {
      // Same-field range fusion: two predicates on the same numeric field with
      // complementary comparators collapse to a single SIMD pass over the
      // NumberRegion with a range mask. For different fields, fall back to null
      // so Brackit runs the generic pipeline.
      if (field1.equals(field2)) {
        long count = parallelFilterCountRange(field1, op1, value1, op2, value2);
        return new Int64(count);
      }
      return null;
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized 2-predicate filter-count failed: %s",
                               e.getMessage());
    }
  }

  @Override
  public Sequence executeFilterCount(QueryContext ctx, String filterField, String filterOp, long filterValue)
      throws QueryException {
    try {
      long count = parallelFilterCount(filterField, filterOp, filterValue);
      return new Int64(count);
    } catch (Exception e) {
      throw new QueryException(e,
                               ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized filter-count failed: %s",
                               e.getMessage());
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
      long[] stats = parallelAggregate(field);
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
    int eff = (int) Math.min(threads, Math.max(1, totalPages));
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
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(reusableKey.setRecordPageKey(pk));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        if (DIAGNOSTICS_ENABLED) {
          diag.pagesVisited++;
          diag.pagesSlowPath++;
        }
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;
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
