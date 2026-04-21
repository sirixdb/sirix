/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

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
    REGISTRY.put(key(resourceKey, sourcePath), new Handle(fieldNames, leafPayloads));
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
    REGISTRY.put(wildcardKey(resourceKey), new Handle(fieldNames, leafPayloads));
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
