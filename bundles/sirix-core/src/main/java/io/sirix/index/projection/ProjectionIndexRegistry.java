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

    public Handle(final String[] fieldNames, final List<byte[]> leafPayloads) {
      this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames").clone();
      this.leafPayloads = Objects.requireNonNull(leafPayloads, "leafPayloads");
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
  }

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
    final String k = wildcardKey(resourceKey);
    final Handle handle = new Handle(fieldNames, leafPayloads);
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
