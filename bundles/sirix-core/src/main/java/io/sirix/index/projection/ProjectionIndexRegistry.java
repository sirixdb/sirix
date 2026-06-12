/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Process-wide lookup from (resource, sourcePath, fields) to a pre-built
 * list of serialised {@link ProjectionIndexLeafPage} byte[]s.
 *
 * <p>Interim bridge that lets the query-path executor
 * ({@link io.sirix.query.scan.SirixVectorizedExecutor}) detect a covering
 * projection index and route to {@link ProjectionIndexByteScan} without
 * first graduating projection indexes to the full
 * {@code IndexController} + {@code IndexListener} lifecycle. The
 * full lifecycle wiring is tracked as task #57 and is gated on the
 * {@code ChunkDirectory}/{@code BitmapChunkPage} sub-slot versioning
 * refactor — shipping either will fold into or replace this registry.
 *
 * <p>The key includes resource identifier + JSON source path + ordered
 * field list. Scans consult the registry by field-<em>set</em>: if the
 * installed index's fields are a superset of the query's predicate fields
 * and their column order in the index is known, predicate-to-column
 * mapping is trivial. Keys are deliberately simple strings so a bench or
 * test can install an index with one call and the executor can find it
 * on the hot path with one hash lookup.
 *
 * <h2>Thread-safety</h2>
 * Backed by a {@link ConcurrentHashMap}; {@link #install} publishes the
 * handle via a {@code put}-with-happens-before, reads are plain {@code get}s.
 * The installed handle's {@code leafPayloads} list must not be mutated
 * after install — callers should hand in an immutable snapshot.
 */
public final class ProjectionIndexRegistry {

  /**
   * Immutable handle published into the registry. Column order in
   * {@link #fieldNames} defines the column order that
   * {@link ProjectionIndexByteScan#conjunctiveCount} expects: the query
   * path converts each predicate leaf to a
   * {@link ProjectionIndexScan.ColumnPredicate} with
   * {@code column = Arrays.asList(fieldNames).indexOf(predFieldName)}.
   */
  public static final class Handle {
    private final String[] fieldNames;
    private final List<byte[]> leafPayloads;
    /**
     * iter#10 dense group-by: per-column canonical dictionary cache.
     * Sentinel value {@link #CANON_DICT_INELIGIBLE} flags
     * "probe determined {@code groupColumn} is not eligible for the
     * dense path" so we don't re-probe on subsequent lookups.
     * Array indexed by {@code groupColumn}; {@code null} slot = not yet
     * computed.
     */
    private volatile byte[][][] canonicalDicts;

    /**
     * Per-column integrality evidence for NUMERIC_LONG columns: {@code true}
     * means the builder SAW a non-integral (truncated) value in that column.
     * {@code null} = unknown provenance (e.g. re-encoded persisted leaves) —
     * value-exact consumers must treat unknown as not-provably-integral.
     */
    private final boolean[] numericNonIntegral;

    /**
     * Lazily-probed per-column sparse evidence — values are
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_CLEAN} /
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_DIRTY}. Computed once for
     * ALL columns by {@link ProjectionIndexByteScan#probeSparseEvidence} on
     * first use; the evidence lives INSIDE the leaf payloads (v2 presence
     * tail + per-column unrepresentable flags) so it survives persistence
     * and re-encoding, unlike the handle-carried integrality flags.
     */
    private volatile byte[] sparseStatus;

    public Handle(final String[] fieldNames, final List<byte[]> leafPayloads) {
      this(fieldNames, leafPayloads, null);
    }

    public Handle(final String[] fieldNames, final List<byte[]> leafPayloads,
        final boolean[] numericNonIntegral) {
      this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames").clone();
      this.leafPayloads = Objects.requireNonNull(leafPayloads, "leafPayloads");
      this.numericNonIntegral = numericNonIntegral == null ? null : numericNonIntegral.clone();
    }

    /**
     * {@code true} iff the column is PROVABLY integral: builder-tracked flags
     * exist and never saw a fractional value. Used to gate value-exact fast
     * paths (aggregates); unknown provenance returns {@code false}.
     */
    public boolean numericColumnIsIntegral(final int col) {
      return numericNonIntegral != null && col >= 0 && col < numericNonIntegral.length
          && !numericNonIntegral[col];
    }

    /** {@code true} iff the builder POSITIVELY saw non-integral values in the column. */
    public boolean numericColumnKnownNonIntegral(final int col) {
      return numericNonIntegral != null && col >= 0 && col < numericNonIntegral.length
          && numericNonIntegral[col];
    }

    /**
     * {@code true} iff column {@code col} can serve SPARSE-CORRECT answers:
     * every leaf carries the v2 presence tail and the column never saw a
     * present-but-unrepresentable value (JSON null, object/array, kind
     * mismatch). Anything else — including legacy v1 leaves, which have no
     * presence information at all — returns {@code false} and consumers must
     * fall back (typed scan kernels / generic pipeline). Probe runs once per
     * handle and is cached; rebuilding the projection
     * ({@code -Dsirix.projection.forceRebuild=true}) migrates v1 leaves.
     */
    public boolean columnSparseClean(final int col) {
      if (col < 0) return false;
      byte[] status = sparseStatus;
      if (status == null) {
        synchronized (this) {
          status = sparseStatus;
          if (status == null) {
            status = ProjectionIndexByteScan.probeSparseEvidence(leafPayloads);
            sparseStatus = status;
          }
        }
      }
      return col < status.length && status[col] == ProjectionIndexByteScan.SPARSE_STATUS_CLEAN;
    }

    public String[] fieldNames() {
      return fieldNames.clone();
    }

    public List<byte[]> leafPayloads() {
      return leafPayloads;
    }

    /** @return index of {@code name} in {@link #fieldNames}, or {@code -1}. */
    public int columnOf(final String name) {
      for (int i = 0; i < fieldNames.length; i++) {
        if (fieldNames[i].equals(name)) return i;
      }
      return -1;
    }

    /**
     * Return the canonical dictionary for the {@code groupColumn}'s
     * STRING_DICT values, or {@code null} if dense group-by is not
     * eligible (cardinality exceeds {@code cardLimit}, column is not
     * STRING_DICT, or leafPayloads is empty).
     *
     * <p>Result is cached per-column under a CAS so subsequent calls
     * are zero-cost. {@link #CANON_DICT_INELIGIBLE} caches "probe
     * established ineligible" so we don't re-probe.
     *
     * <p>HFT-grade: volatile read on fast path; one probe (bounded
     * to {@code probeLeaves} leaves) on first call per column.
     */
    public byte[][] canonicalDict(final int groupColumn,
        final int probeLeaves, final int cardLimit) {
      if (groupColumn < 0) return null;
      byte[][][] cache = canonicalDicts;
      if (cache != null && groupColumn < cache.length) {
        final byte[][] cached = cache[groupColumn];
        if (cached == CANON_DICT_INELIGIBLE) return null;
        if (cached != null) return cached;
      }
      // Compute outside the monitor — probe can be several ms; then
      // publish under the monitor to avoid lost wake-ups.
      final byte[][] probed =
          ProjectionIndexByteScan.probeCanonicalDict(leafPayloads, groupColumn, probeLeaves, cardLimit);
      synchronized (this) {
        cache = canonicalDicts;
        // Grow the array if needed (rare — first access usually pre-sizes).
        if (cache == null || cache.length <= groupColumn) {
          final byte[][][] grown = new byte[Math.max(groupColumn + 1, fieldNames.length)][][];
          if (cache != null) System.arraycopy(cache, 0, grown, 0, cache.length);
          cache = grown;
          canonicalDicts = cache;
        }
        final byte[][] existing = cache[groupColumn];
        if (existing != null && existing != CANON_DICT_INELIGIBLE) return existing;
        cache[groupColumn] = probed != null ? probed : CANON_DICT_INELIGIBLE;
        return probed;
      }
    }
  }

  /** Sentinel for "probe found ineligible for dense group-by" — see {@link Handle#canonicalDict}. */
  private static final byte[][] CANON_DICT_INELIGIBLE = new byte[0][];

  private static final ConcurrentMap<String, Handle> REGISTRY = new ConcurrentHashMap<>();

  /**
   * One-shot latch tracking which registry keys have already been JIT pre-warmed.
   * Pre-warm is idempotent, but re-firing it on every {@link #install} when a
   * caller re-registers the same resource wastes a few ms. The latch key is the
   * registry key (resourceKey + sourcePath).
   */
  private static final ConcurrentMap<String, Boolean> PREWARMED = new ConcurrentHashMap<>();

  /**
   * Default on — drain first-call JIT tier-up for {@link ProjectionIndexByteScan}
   * into the install step so the very first user-visible query on a freshly
   * installed projection index doesn't pay 100-1000 ms of C2 compilation cost.
   *
   * <p>On a cold 100M bench we measured 787-1417 ms of variance across runs
   * for the first {@code conjunctiveCountByGroup} invocation; pre-warming the
   * method shape with a few hundred tiny (2-leaf) calls at install time
   * collapses that spread and shifts the cost out of the user-facing
   * measured window.
   *
   * <p>Set {@code -Dsirix.projection.prewarmJit=false} to disable.
   */
  private static final boolean PREWARM_JIT_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.prewarmJit", "true"));

  /**
   * Outer-loop count for the pre-warm. Each iteration scans a 2-leaf subList
   * per fired method shape — enough to cross C2's tier-3 threshold and drive
   * the hot byte-code paths ({@link ProjectionIndexByteScan#conjunctiveCount},
   * {@link ProjectionIndexByteScan#conjunctiveCountByGroup},
   * plus their {@code evaluateLeafMask} / {@code evalColumn*} callees) into
   * a compiled state before the first user query fires.
   *
   * <p>Default {@code 200} was selected on the cold 100M brackit-scale-bench:
   * smaller (100) undershoots tier-up on the group-by-count path; larger
   * (500+) pays more up-front than the tier-up wins back. At 200 we measure
   * a median cold wall drop of −0.27 s vs pre-warm off, with the first-call
   * {@code compoundAndFilterCount} dropping from 1.05 ms → 0.53 ms.
   *
   * <p>Tune via {@code -Dsirix.projection.prewarmJit.iters=N}. Zero disables
   * even when {@link #PREWARM_JIT_ENABLED} is true.
   */
  private static final int PREWARM_ITERS =
      Integer.parseInt(System.getProperty("sirix.projection.prewarmJit.iters", "200"));

  /**
   * iter#10 — pre-warm the dense group-by method only when it's enabled
   * (opt-in). Default off aligns with {@link #PREWARM_DENSE_DEFAULT_OFF}
   * so we don't pay the install-time cost for a feature that's not being
   * used. Tune via {@code -Dsirix.projection.denseGroupBy=true}.
   */
  private static final boolean PREWARM_DENSE_GROUPBY_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.denseGroupBy", "false"));

  private ProjectionIndexRegistry() {
  }

  /**
   * Publish a projection index into the registry. Overwrites any prior
   * entry with the same key.
   *
   * @param resourceKey  stable identifier for the Sirix resource (e.g.
   *                     {@code database.getName() + "/" + resourceName}).
   * @param sourcePath   JSON source path segments (e.g. {@code ["doc", "records"]}
   *                     for {@code $doc/records[]}). {@code null} allowed and
   *                     treated as the empty path.
   * @param fieldNames   ordered column list — this order is authoritative
   *                     for the serialised leaf payloads' column layout.
   * @param leafPayloads one {@link ProjectionIndexLeafPage#serialize()}
   *                     byte[] per leaf, in leaf key order. Caller must
   *                     not mutate after publish.
   */
  public static void install(final String resourceKey, final String[] sourcePath,
      final String[] fieldNames, final List<byte[]> leafPayloads) {
    final String k = key(resourceKey, sourcePath);
    final Handle handle = new Handle(fieldNames, leafPayloads);
    REGISTRY.put(k, handle);
    prewarmIfFirst(k, handle);
  }

  /**
   * @return installed handle for {@code (resourceKey, sourcePath)}. Falls
   *         back to the wildcard entry (installed via {@link #installWildcard})
   *         if no exact match exists — makes bench wiring simpler when the
   *         sourcePath shape the Brackit optimizer produces is not known
   *         ahead of time.
   */
  public static Handle lookup(final String resourceKey, final String[] sourcePath) {
    final Handle exact = REGISTRY.get(key(resourceKey, sourcePath));
    if (exact != null) return exact;
    return REGISTRY.get(wildcardKey(resourceKey));
  }

  /**
   * Install a projection index that matches <em>any</em> source path for the
   * given resource. Useful in benches where the caller doesn't know the
   * exact {@code sourcePath} shape Brackit will pass to
   * {@code executePredicateCount}.
   */
  public static void installWildcard(final String resourceKey,
      final String[] fieldNames, final List<byte[]> leafPayloads) {
    installWildcard(resourceKey, fieldNames, leafPayloads, null);
  }

  /** Variant carrying builder-tracked NUMERIC_LONG integrality evidence. */
  public static void installWildcard(final String resourceKey,
      final String[] fieldNames, final List<byte[]> leafPayloads,
      final boolean[] numericNonIntegral) {
    final String k = wildcardKey(resourceKey);
    final Handle handle = new Handle(fieldNames, leafPayloads, numericNonIntegral);
    REGISTRY.put(k, handle);
    prewarmIfFirst(k, handle);
  }

  private static String wildcardKey(final String resourceKey) {
    return Objects.requireNonNull(resourceKey, "resourceKey") + "\0*";
  }

  /** Remove any installed index for {@code (resourceKey, sourcePath)}. */
  public static void uninstall(final String resourceKey, final String[] sourcePath) {
    REGISTRY.remove(key(resourceKey, sourcePath));
  }

  /** Drop every entry — for test isolation. */
  public static void clear() {
    REGISTRY.clear();
    PREWARMED.clear();
  }

  /**
   * Fire a one-shot JIT pre-warm for {@code handle} if this is the first
   * install under {@code registryKey}. Swallow all exceptions — a pre-warm
   * failure must never break real installs.
   *
   * <p>The pre-warm uses the actual installed payloads (not synthetic) so
   * the shapes C2 profiles during tier-up match the shapes the first real
   * query will invoke. A tiny 2-leaf subList keeps each iteration sub-ms
   * while still driving the back-branch / invocation counters past C2's
   * tier-3 threshold across the configured repeat count (see
   * {@link #PREWARM_ITERS}).
   *
   * <p>Idempotent per registry key — the {@link #PREWARMED} latch prevents
   * re-firing when a caller re-installs the same key.
   */
  private static void prewarmIfFirst(final String registryKey, final Handle handle) {
    if (!PREWARM_JIT_ENABLED) return;
    if (PREWARM_ITERS <= 0) return;
    if (handle.leafPayloads == null || handle.leafPayloads.isEmpty()) return;
    if (PREWARMED.putIfAbsent(registryKey, Boolean.TRUE) != null) return;
    try {
      prewarmJitForHandle(handle);
    } catch (final Throwable ignored) {
      // Pre-warm is best-effort; a failure must not interfere with installs.
    }
  }

  /**
   * JIT tier-up driver for {@link ProjectionIndexByteScan}'s hot methods.
   * Visible for tests. Safe to call multiple times — pre-warm is idempotent
   * by construction (no state leaks into the registry or its handles).
   *
   * <p>Shape coverage (matches the bench query set in
   * {@code ScaleBenchMain}):
   * <ul>
   *   <li>{@code conjunctiveCount(numeric GT)} — e.g. {@code $u.age > 40}</li>
   *   <li>{@code conjunctiveCount(numeric BETWEEN)} — e.g. fused
   *       {@code $u.age > 30 and $u.age < 50}</li>
   *   <li>{@code conjunctiveCount(boolean EQ)} — e.g. {@code $u.active}</li>
   *   <li>{@code conjunctiveCount(numeric + boolean)} — e.g.
   *       {@code $u.age > 40 and $u.active}</li>
   *   <li>{@code conjunctiveCountByGroup(empty preds)} — e.g.
   *       {@code group by $d}</li>
   *   <li>{@code conjunctiveCountByGroup(boolean EQ)} — e.g.
   *       {@code where $u.active ... group by $d}</li>
   * </ul>
   *
   * <p>Columns are selected on first-leaf inspection: the first
   * {@code NUMERIC_LONG} becomes the numeric predicate column, the first
   * {@code BOOLEAN} the boolean column, and the first {@code STRING_DICT}
   * the group column. When a shape's required column kind is absent, that
   * shape is skipped silently — still correct, just slightly less warm-up
   * coverage for unusual index schemas.
   */
  static void prewarmJitForHandle(final Handle handle) {
    final List<byte[]> payloads = handle.leafPayloads;
    if (payloads == null || payloads.isEmpty()) return;
    final byte[] firstLeaf = payloads.get(0);
    if (firstLeaf == null) return;
    // Column count encoded at offset 4 little-endian; kinds start at offset 24.
    final int columnCount =
        (firstLeaf[4] & 0xFF)
            | ((firstLeaf[5] & 0xFF) << 8)
            | ((firstLeaf[6] & 0xFF) << 16)
            | ((firstLeaf[7] & 0xFF) << 24);
    if (columnCount <= 0 || columnCount > 256) return;
    int numericCol = -1;
    int booleanCol = -1;
    int stringDictCol = -1;
    for (int c = 0; c < columnCount; c++) {
      final byte kind = firstLeaf[24 + c];
      if (kind == ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG && numericCol < 0) numericCol = c;
      else if (kind == ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN && booleanCol < 0) booleanCol = c;
      else if (kind == ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT && stringDictCol < 0) stringDictCol = c;
    }

    // Take at most 2 leaves as the pre-warm input. The goal is per-method
    // tier-up, not per-leaf: a small subList keeps per-call cost sub-ms while
    // driving the invocation counter past C2's tier-4 threshold.
    final int subSize = Math.min(2, payloads.size());
    final List<byte[]> sub = payloads.subList(0, subSize);

    // Numeric GT — e.g. $u.age > 40.
    if (numericCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] numGt = {
          ProjectionIndexScan.ColumnPredicate.numeric(numericCol, ProjectionIndexScan.Op.GT, 0L)
      };
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, numGt);
      }

      // Numeric BETWEEN — e.g. $u.age > 30 and $u.age < 50 (fused).
      final ProjectionIndexScan.ColumnPredicate[] numBetween = {
          ProjectionIndexScan.ColumnPredicate.numericBetween(
              numericCol, ProjectionIndexScan.Op.GT, 0L,
              ProjectionIndexScan.Op.LT, Long.MAX_VALUE)
      };
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, numBetween);
      }
    }

    // Boolean EQ — e.g. $u.active.
    if (booleanCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] boolEq = {
          ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)
      };
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, boolEq);
      }
    }

    // Numeric + boolean — e.g. $u.age > 40 and $u.active.
    if (numericCol >= 0 && booleanCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] mix = {
          ProjectionIndexScan.ColumnPredicate.numeric(numericCol, ProjectionIndexScan.Op.GT, 0L),
          ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)
      };
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, mix);
      }
    }

    // Group-by shapes. Need a STRING_DICT column to route through the
    // dict decode + intern paths in conjunctiveCountByGroup.
    if (stringDictCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] noPreds =
          new ProjectionIndexScan.ColumnPredicate[0];
      final Object2LongOpenHashMap<String> sink = new Object2LongOpenHashMap<>();
      sink.defaultReturnValue(0L);
      for (int i = 0; i < PREWARM_ITERS; i++) {
        sink.clear();
        ProjectionIndexByteScan.conjunctiveCountByGroup(sub, noPreds, stringDictCol, sink);
      }

      if (booleanCol >= 0) {
        final ProjectionIndexScan.ColumnPredicate[] boolEqGroup = {
            ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)
        };
        for (int i = 0; i < PREWARM_ITERS; i++) {
          sink.clear();
          ProjectionIndexByteScan.conjunctiveCountByGroup(sub, boolEqGroup, stringDictCol, sink);
        }
      }

      // iter#10: also pre-warm the dense group-by path. Same shape,
      // different accumulator. Skip if the dense path cannot be probed
      // (canonicalDict == null, meaning cardinality exceeded or other).
      // Call via handle.canonicalDict(...) so the per-column cache is
      // populated once; subsequent query-path lookups are zero-cost.
      //
      // Gated on PREWARM_DENSE_GROUPBY_ENABLED — off by default on the
      // iter#09 C2 baseline because dense doesn't beat the hashmap here
      // (see SirixVectorizedExecutor.DENSE_GROUPBY_ENABLED javadoc). Flip
      // on for workloads with larger STRING_DICT cardinality or non-C2
      // JIT where the hashmap path isn't as heavily intrinsified.
      final byte[][] canonical = PREWARM_DENSE_GROUPBY_ENABLED
          ? handle.canonicalDict(stringDictCol, 16, 256)
          : null;
      if (canonical != null) {
        final long[] denseCounts = new long[canonical.length];
        final Object2LongOpenHashMap<String> denseFallback = new Object2LongOpenHashMap<>();
        denseFallback.defaultReturnValue(0L);
        for (int i = 0; i < PREWARM_ITERS; i++) {
          java.util.Arrays.fill(denseCounts, 0L);
          denseFallback.clear();
          ProjectionIndexByteScan.conjunctiveCountByGroupDense(
              sub, noPreds, stringDictCol, canonical, denseCounts, denseFallback);
        }
        if (booleanCol >= 0) {
          final ProjectionIndexScan.ColumnPredicate[] boolEqGroup = {
              ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)
          };
          for (int i = 0; i < PREWARM_ITERS; i++) {
            java.util.Arrays.fill(denseCounts, 0L);
            denseFallback.clear();
            ProjectionIndexByteScan.conjunctiveCountByGroupDense(
                sub, boolEqGroup, stringDictCol, canonical, denseCounts, denseFallback);
          }
        }
      }
    }
  }

  private static String key(final String resourceKey, final String[] sourcePath) {
    return Objects.requireNonNull(resourceKey, "resourceKey")
        + "\0" + String.join("/", sourcePath == null ? new String[0] : sourcePath);
  }

  /**
   * Coverage check: every {@code predicateField} must appear in the
   * handle's {@code fieldNames}.
   */
  public static boolean covers(final Handle handle, final String[] predicateFields) {
    if (handle == null || predicateFields == null) return false;
    for (final String f : predicateFields) {
      if (handle.columnOf(f) < 0) return false;
    }
    return true;
  }

  /**
   * True if any projection registered under {@code resourceKey} (exact or
   * wildcard) carries {@code field} as a column. Used as a proof-of-existence
   * by the query executor's name-key resolver so it can skip an expensive
   * full-document walk when a covering projection is already installed.
   *
   * <p>Lookup is a prefix scan over the registry — O(N) in the number of
   * entries per resource, which is bounded to a small constant in practice.
   */
  public static boolean anyHandleCoversField(final String resourceKey, final String field) {
    if (resourceKey == null || field == null) return false;
    final String prefix = resourceKey + "\0";
    for (final var entry : REGISTRY.entrySet()) {
      if (!entry.getKey().startsWith(prefix)) continue;
      if (entry.getValue().columnOf(field) >= 0) return true;
    }
    return false;
  }

  // Package-private helper for diagnostic toString in tests.
  static int size() {
    return REGISTRY.size();
  }

  @Override
  public String toString() {
    return "ProjectionIndexRegistry{size=" + REGISTRY.size() + "}";
  }

  /**
   * Static helper used mainly in tests to describe the installed set.
   */
  public static String describe() {
    final StringBuilder sb = new StringBuilder("ProjectionIndexRegistry[\n");
    REGISTRY.forEach((k, h) ->
        sb.append("  ").append(k).append(" -> fields=").append(Arrays.toString(h.fieldNames))
          .append(", leaves=").append(h.leafPayloads.size()).append("\n"));
    return sb.append("]").toString();
  }
}
