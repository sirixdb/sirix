/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Revision-scoped, catalog-driven access to projection indexes — the
 * projection analogue of how PATH/CAS/NAME scans reach their trees: the
 * resource's {@code Indexes} catalog (of the queried revision) says WHICH
 * projections exist, and the revision's {@code ProjectionIndexPage} sub-tree
 * says WHAT they contain. Because both are versioned by the page layer's
 * copy-on-write, every hard problem is solved by construction:
 * <ul>
 *   <li><b>Revision correctness / time travel</b> — a definition only
 *       exists in catalogs from its commit revision onward, and each
 *       revision's sub-tree is immutable, so an executor bound to revision
 *       R can only ever see projection data that was current at R.</li>
 *   <li><b>Transaction isolation</b> — uncommitted builds and tombstones
 *       ride the write transaction's log and are invisible to readers;
 *       rollback discards them with no compensation logic.</li>
 *   <li><b>Reopen</b> — a fresh process discovers persisted projections on
 *       first query, no re-bootstrap call needed (same as the other index
 *       families).</li>
 * </ul>
 *
 * <h2>Serving contract</h2>
 * A definition serves a query only when its record-set root EXACTLY equals
 * the query's canonical source path AND its trailing field names cover the
 * query's columns; among several matches the narrowest wins and unusable
 * candidates (stale, unreadable) are skipped in favor of the next match.
 * Descendant-pattern roots ({@code //...}) aggregate across every matching
 * subtree by design, which a path-specific query must not be served from —
 * they fail closed to the generic pipeline (pattern-aware matching is a
 * possible follow-up).
 *
 * <h2>Caching</h2>
 * Two tiers, both bounded:
 * <ul>
 *   <li>a cheap per-(resource, definition, revision) PROBE of the slot-0
 *       metadata (single-leaf read — no sub-tree hydrate) answering
 *       "usable at this revision, and which build revision?", plus a
 *       per-(resource, revision) snapshot of the projection definitions'
 *       precomputed root/name strings (avoids re-copying the def set and
 *       re-stringifying paths on every query);</li>
 *   <li>the DECODED leaves keyed by (resource, definition, BUILD revision)
 *       — unrelated commits advance the query revision but not the build
 *       revision, so an unchanged projection is decoded once and shared by
 *       every subsequent revision instead of once per revision. Weighed in
 *       KiB ({@code -Dsirix.projection.cacheBytes}, default 8 GiB).</li>
 * </ul>
 *
 * <h2>Failure policy</h2>
 * Expected non-usability (no payloads, older wire format, stale tombstone)
 * is cached silently. Corruption evidence (metadata that no longer matches
 * the catalogued definition, truncated leaf lists, decode failures) is
 * logged at WARN and cached as unusable — queries fall back to the
 * always-correct generic pipeline. Unexpected transient failures (a session
 * closing mid-read, I/O errors) are logged and NOT cached, so the next
 * query retries.
 *
 * <p>Resource lifecycle: {@link #invalidateUnder(String)} drops all cached
 * state for a database/resource path prefix — wired into database/resource
 * removal so a recreation at the same path can never see the old store's
 * decoded columns.
 *
 * <p>The static {@link ProjectionIndexRegistry} remains as bench/test
 * wiring for stores without catalogued definitions — production lookups go
 * through here first.
 */
public final class ProjectionIndexCatalog {

  private static final LogWrapper LOGGER =
      new LogWrapper(LoggerFactory.getLogger(ProjectionIndexCatalog.class));

  /** Precomputed per-definition strings — computed once per (resource, revision). */
  private record DefEntry(IndexDef def, String rootPath, String[] fieldNames) {
  }

  private record DefsKey(String resourceKey, int revision) {
  }

  private record ProbeKey(String resourceKey, int indexDefId, int revision) {
  }

  /** Slot-0 probe result: usable build revision, or {@link #UNUSABLE}. */
  private record Probe(int buildRevision) {
  }

  private static final Probe UNUSABLE = new Probe(-1);

  private record DataKey(String resourceKey, int indexDefId, int buildRevision) {
  }

  /** Negative decode entry: probed and not decodable at this build revision. */
  private static final ProjectionIndexRegistry.Handle NOT_USABLE =
      new ProjectionIndexRegistry.Handle(new String[0], List.of());

  private static final long CACHE_BYTES =
      Long.parseLong(System.getProperty("sirix.projection.cacheBytes",
          String.valueOf(8L << 30)));

  private static final Cache<DefsKey, DefEntry[]> DEFS = Caffeine.newBuilder()
      .maximumSize(8192)
      .build();

  private static final Cache<ProbeKey, Probe> PROBES = Caffeine.newBuilder()
      .maximumSize(1 << 16)
      .build();

  /** Decoded leaves, weighed in KiB so entries beyond 2 GiB stay accounted. */
  private static final Cache<DataKey, ProjectionIndexRegistry.Handle> DATA = Caffeine.newBuilder()
      .maximumWeight(Math.max(1L, CACHE_BYTES >> 10))
      .<DataKey, ProjectionIndexRegistry.Handle>weigher((key, handle) -> {
        long bytes = 64;
        for (final byte[] payload : handle.leafPayloads()) {
          bytes += payload == null ? 0 : payload.length;
        }
        return (int) Math.min(Integer.MAX_VALUE, 1 + (bytes >> 10));
      })
      .build();

  /** Successful catalog-served lookups — observable by tests. */
  private static final LongAdder SERVED = new LongAdder();

  private ProjectionIndexCatalog() {
  }

  /** Whether the catalog of {@code revision} holds any projection definition. */
  public static boolean hasProjections(final JsonResourceSession session, final String resourceKey,
      final int revision) {
    return defEntries(session, resourceKey, revision).length > 0;
  }

  /** Whether any catalogued projection of {@code revision} carries {@code field} as a column. */
  public static boolean anyDefCoversField(final JsonResourceSession session,
      final String resourceKey, final int revision, final String field) {
    for (final DefEntry entry : defEntries(session, resourceKey, revision)) {
      for (final String name : entry.fieldNames) {
        if (name.equals(field)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Select and load the projection serving {@code requiredFields} for the
   * record set identified by {@code sourcePath} at {@code revision}. See the
   * class javadoc's serving contract: exact root match, coverage, narrowest
   * first, skip-unusable.
   *
   * @param sourcePath the query's source path segments (e.g.
   *                   {@code ["b", "[]"]} for {@code $doc.b[]}); {@code null}
   *                   or empty fails closed
   * @return a usable handle, or {@code null}
   */
  public static ProjectionIndexRegistry.Handle lookupCovering(final JsonResourceSession session,
      final String resourceKey, final int revision, final String[] sourcePath,
      final String[] requiredFields) {
    final DefEntry[] entries = defEntries(session, resourceKey, revision);
    if (entries.length == 0) {
      return null;
    }
    final String canonicalSourcePath = canonicalSourcePath(sourcePath);
    if (canonicalSourcePath == null) {
      return null;
    }
    // Collect root-matching, covering candidates; tiny arrays — insertion
    // keeps them ordered narrowest-first.
    DefEntry[] candidates = null;
    int candidateCount = 0;
    for (final DefEntry entry : entries) {
      if (!entry.rootPath.equals(canonicalSourcePath) || !coversAll(entry.fieldNames, requiredFields)) {
        continue;
      }
      if (candidates == null) {
        candidates = new DefEntry[entries.length];
      }
      int at = candidateCount++;
      while (at > 0 && candidates[at - 1].fieldNames.length > entry.fieldNames.length) {
        candidates[at] = candidates[at - 1];
        at--;
      }
      candidates[at] = entry;
    }
    for (int i = 0; i < candidateCount; i++) {
      final ProjectionIndexRegistry.Handle handle =
          load(session, resourceKey, revision, candidates[i].def);
      if (handle != null) {
        SERVED.increment();
        return handle;
      }
    }
    return null;
  }

  /**
   * Load (decode-cached) the projection payloads of {@code def} as valid at
   * {@code revision}. Two-tier: a cheap slot-0 metadata probe decides
   * usability and yields the BUILD revision, which keys the decoded leaves —
   * so revisions that didn't rebuild the projection share one decoded copy.
   * Fail-soft: unusable stores yield {@code null} and the caller falls back
   * (or rebuilds, on the creation path).
   */
  public static ProjectionIndexRegistry.Handle load(final JsonResourceSession session,
      final int revision, final IndexDef def) {
    return load(session, session.getResourceConfig().getResource().toString(), revision, def);
  }

  /** {@link #load(JsonResourceSession, int, IndexDef)} with a precomputed resource key. */
  public static ProjectionIndexRegistry.Handle load(final JsonResourceSession session,
      final String resourceKey, final int revision, final IndexDef def) {
    try {
      final Probe probe = PROBES.get(new ProbeKey(resourceKey, def.getID(), revision),
          key -> probeMetadata(session, revision, def));
      if (probe == UNUSABLE || probe.buildRevision < 0) {
        return null;
      }
      final ProjectionIndexRegistry.Handle handle =
          DATA.get(new DataKey(resourceKey, def.getID(), probe.buildRevision),
              key -> decodeLeaves(session, revision, def));
      return handle == NOT_USABLE ? null : handle;
    } catch (final RuntimeException e) {
      // Transient failure (session closing mid-read, I/O error): logged,
      // NOT cached — the next query retries. The generic pipeline is
      // always correct, so fail soft.
      LOGGER.warn("Projection probe/load failed transiently for resource " + resourceKey
          + ", definition #" + def.getID() + " at revision " + revision + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Slot-0-only probe: reads the metadata payload (one leaf, no sub-tree
   * hydrate) and validates it against the catalogued definition. Corruption
   * is logged and cached as unusable; transient failures propagate to
   * {@link #load}'s no-cache handler.
   */
  private static Probe probeMetadata(final JsonResourceSession session, final int revision,
      final IndexDef def) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final byte[] slot0;
      final ProjectionIndexMetadata metadata;
      try {
        slot0 = ProjectionIndexHOTStorage.readOne(rtx.getStorageEngineReader(), def.getID(), 0L);
        metadata = ProjectionIndexMetadata.parse(slot0);
      } catch (final IllegalStateException corrupt) {
        LOGGER.warn("Projection definition #" + def.getID() + " has a corrupt metadata payload at "
            + "revision " + revision + " — falling back to the generic pipeline ("
            + corrupt.getMessage() + ")");
        return UNUSABLE;
      }
      if (metadata == null || metadata.isStale()) {
        // Expected: never persisted / older wire format / invalidated.
        return UNUSABLE;
      }
      if (!metadata.matches(def.getProjectionRootPath().toString(), defFieldPaths(def),
          defColumnKinds(def))) {
        LOGGER.warn("Projection definition #" + def.getID() + " does not match its persisted "
            + "metadata shape at revision " + revision + " (leftover sub-tree from a dropped "
            + "definition?) — falling back to the generic pipeline");
        return UNUSABLE;
      }
      return new Probe(metadata.buildRevision());
    }
  }

  /**
   * Full decode of the projection's persisted leaves. Only reached after a
   * successful metadata probe; corruption discovered here (truncated leaf
   * list, codec failures) is logged and cached as unusable for this build.
   */
  private static ProjectionIndexRegistry.Handle decodeLeaves(final JsonResourceSession session,
      final int revision, final IndexDef def) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<byte[]> persisted =
          ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), def.getID());
      if (persisted.isEmpty()) {
        return NOT_USABLE;
      }
      final ProjectionIndexMetadata metadata;
      try {
        metadata = ProjectionIndexMetadata.parse(persisted.get(0));
      } catch (final IllegalStateException corrupt) {
        LOGGER.warn("Projection definition #" + def.getID() + ": corrupt metadata during decode ("
            + corrupt.getMessage() + ")");
        return NOT_USABLE;
      }
      if (metadata == null || metadata.isStale()) {
        return NOT_USABLE;
      }
      final int leafCount = metadata.leafCount();
      if (persisted.size() < leafCount + 1) {
        LOGGER.warn("Projection definition #" + def.getID() + " declares " + leafCount
            + " leaves but only " + (persisted.size() - 1) + " are stored — the store is "
            + "truncated; falling back to the generic pipeline");
        return NOT_USABLE;
      }
      final List<byte[]> decoded = new ArrayList<>(leafCount);
      try {
        for (int i = 1; i <= leafCount; i++) {
          decoded.add(ProjectionIndexLeafCodec.decode(persisted.get(i)));
        }
      } catch (final IllegalStateException corrupt) {
        LOGGER.warn("Projection definition #" + def.getID() + ": corrupt leaf payload ("
            + corrupt.getMessage() + ")");
        return NOT_USABLE;
      }
      return new ProjectionIndexRegistry.Handle(metadata.rootPath(), metadata.buildRevision(),
          metadata.fieldNames(), decoded, null);
    }
  }

  private static DefEntry[] defEntries(final JsonResourceSession session, final String resourceKey,
      final int revision) {
    return DEFS.get(new DefsKey(resourceKey, revision), key -> {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      final List<DefEntry> entries = new ArrayList<>();
      for (final IndexDef def : controller.getIndexes().getIndexDefs()) {
        if (def.isProjectionIndex()) {
          entries.add(new DefEntry(def, def.getProjectionRootPath().toString(),
              ProjectionIndexChangeListener.trailingFieldNames(def)));
        }
      }
      return entries.toArray(new DefEntry[0]);
    });
  }

  private static String[] defFieldPaths(final IndexDef def) {
    final List<Path<QNm>> fieldPaths = def.getProjectionFields();
    final String[] paths = new String[fieldPaths.size()];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = fieldPaths.get(i).toString();
    }
    return paths;
  }

  private static byte[] defColumnKinds(final IndexDef def) {
    final byte[] kinds = new byte[def.getProjectionFieldTypes().size()];
    for (int i = 0; i < kinds.length; i++) {
      kinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(def.getProjectionFieldTypes().get(i));
    }
    return kinds;
  }

  /**
   * Find a catalogued PROJECTION definition with exactly this shape (root
   * path, ordered field paths, and — when {@code fieldTypesOrNull} is given —
   * ordered types). Comparison uses the parsed paths' canonical form. Shared
   * by the create/find/drop functions so shape identity has one definition.
   */
  public static IndexDef findMatchingDef(final Iterable<IndexDef> defs,
      final String rootPathCanonical, final String[] fieldPathCanonicals,
      final List<Type> fieldTypesOrNull) {
    for (final IndexDef def : defs) {
      if (!def.isProjectionIndex()) {
        continue;
      }
      if (!rootPathCanonical.equals(def.getProjectionRootPath().toString())) {
        continue;
      }
      final List<Path<QNm>> defFields = def.getProjectionFields();
      if (defFields.size() != fieldPathCanonicals.length) {
        continue;
      }
      if (fieldTypesOrNull != null && !def.getProjectionFieldTypes().equals(fieldTypesOrNull)) {
        continue;
      }
      boolean same = true;
      for (int i = 0; i < defFields.size(); i++) {
        if (!defFields.get(i).toString().equals(fieldPathCanonicals[i])) {
          same = false;
          break;
        }
      }
      if (same) {
        return def;
      }
    }
    return null;
  }

  /** Canonical path string of the query's source segments; {@code null} fails closed. */
  private static String canonicalSourcePath(final String[] sourcePath) {
    if (sourcePath == null || sourcePath.length == 0) {
      return null;
    }
    final StringBuilder sb = new StringBuilder(16);
    for (final String segment : sourcePath) {
      sb.append('/').append(segment);
    }
    return sb.toString();
  }

  private static boolean coversAll(final String[] names, final String[] requiredFields) {
    outer:
    for (final String required : requiredFields) {
      for (final String name : names) {
        if (name.equals(required)) {
          continue outer;
        }
      }
      return false;
    }
    return true;
  }

  /** Total catalog-served lookups since process start — for test assertions. */
  public static long servedCount() {
    return SERVED.sum();
  }

  /**
   * Drop all cached state whose resource path starts with {@code pathPrefix}
   * — wired into database/resource removal so a store recreated at the same
   * path can never be served the removed store's columns.
   */
  public static void invalidateUnder(final String pathPrefix) {
    DEFS.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
    PROBES.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
    DATA.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
  }

  /** Drop all cached decodes — for test isolation. */
  public static void clearCache() {
    DEFS.invalidateAll();
    PROBES.invalidateAll();
    DATA.invalidateAll();
  }
}
