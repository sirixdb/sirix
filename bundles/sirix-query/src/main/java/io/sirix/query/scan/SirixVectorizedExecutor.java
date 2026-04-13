/*
 * Copyright (c) 2024, SirixDB. All rights reserved.
 *
 * Vectorized executor that bridges Brackit's VectorizedExecutor interface
 * to SirixDB's parallel page-scan path.
 */
package io.sirix.query.scan;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.jdm.Sequence;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.List;
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
  /** Cached max node key — Sirix-internal, fixed for a given revision. */
  private volatile long cachedMaxNodeKey = -1;
  /** Per-worker-thread trx, opened lazily inside the worker thread on first use.
   *  Sirix transactions are not safely usable across thread boundaries, so each
   *  worker thread owns its own trx for the executor's lifetime. */
  private final ThreadLocal<JsonNodeReadOnlyTrx> threadLocalTrx;
  /** Track per-thread trxes for explicit close on shutdown. */
  private final List<JsonNodeReadOnlyTrx> openedTrxes;
  private final ExecutorService workerPool;
  /** Cached field-name → int nameKey resolution. Keyed once per executor lifetime. */
  private final java.util.concurrent.ConcurrentHashMap<String, Integer> fieldKeyCache = new java.util.concurrent.ConcurrentHashMap<>();

  public SirixVectorizedExecutor(JsonResourceSession session, int revision) {
    this(session, revision, Runtime.getRuntime().availableProcessors());
  }

  public SirixVectorizedExecutor(JsonResourceSession session, int revision, int threads) {
    this.session = session;
    this.revision = revision;
    this.threads = threads;
    this.openedTrxes = new ArrayList<>(threads);
    this.threadLocalTrx = ThreadLocal.withInitial(() -> {
      JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
      synchronized (openedTrxes) {
        openedTrxes.add(rtx);
      }
      return rtx;
    });
    final ThreadFactory tf = r -> {
      Thread t = new Thread(r, "sirix-vec-exec");
      t.setDaemon(true);
      return t;
    };
    this.workerPool = Executors.newFixedThreadPool(threads, tf);
  }

  /** Release per-thread transactions and shut down the worker pool. */
  public void close() {
    synchronized (openedTrxes) {
      for (JsonNodeReadOnlyTrx t : openedTrxes) {
        try {
          t.close();
        } catch (Exception ignored) {
        }
      }
      openedTrxes.clear();
    }
    workerPool.shutdown();
  }

  @Override
  public boolean canExecute(QueryContext ctx) {
    return session != null;
  }

  @Override
  public Sequence executeGroupByCount(QueryContext ctx, String groupField) throws QueryException {
    // Not yet implemented in this phase — fall back to Volcano.
    return null;
  }

  @Override
  public Sequence executeFilterCount(QueryContext ctx, String filterField, String filterOp, long filterValue)
      throws QueryException {
    // Not yet implemented in this phase — fall back to Volcano.
    return null;
  }

  @Override
  public Sequence executeAggregate(QueryContext ctx, String func, String field) throws QueryException {
    try {
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
                               io.brackit.query.ErrorCode.BIT_DYN_INT_ERROR,
                               "Sirix vectorized aggregate failed: %s",
                               e.getMessage());
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
      long s = (long) i * ppt;
      long e = Math.min(s + ppt, totalPages);
      JsonNodeReadOnlyTrx rtx = threadLocalTrx.get();
      var reader = rtx.getStorageEngineReader();
      for (long pk = s; pk < e; pk++) {
        var res = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pk, 0, revision));
        if (res == null || !(res.page() instanceof KeyValueLeafPage kv)) continue;
        long base = pk << Constants.INP_REFERENCE_COUNT_EXPONENT;

        kv.forEachPopulatedSlot(slot -> {
          if (kv.getSlotNodeKindId(slot) != OBJECT_KEY_KIND) return true;
          if (!rtx.moveTo(base + slot)) return true;
          // Pre-resolved nameKey int compare instead of String.equals:
          if (rtx.getNameKey() != fieldKey) return true;
          if (!rtx.moveToFirstChild()) return true;
          // Avoid Number boxing: use getDoubleValue when available, fall back to Number
          // (this still allocates one Long/Double per match, but only matched records).
          Number n = rtx.getNumberValue();
          if (n == null) return true;
          long v = n.longValue();
          acc[0]++;
          acc[1] += v;
          if (v < acc[2]) acc[2] = v;
          if (v > acc[3]) acc[3] = v;
          return true;
        });
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
