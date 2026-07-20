/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.LongSet;
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
 * Descendant-pattern roots ({@code //...}) are resolved against the queried
 * revision's path summary when the pattern matches exactly ONE path class —
 * the definition then serves under that concrete path. A pattern matching
 * several subtrees aggregates across all of them by design, which a
 * path-specific query must not be served from — those (and unresolvable
 * patterns) fail closed to the generic pipeline.
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
 * <h2>Access</h2>
 * The uniform, controller-mediated entry point is
 * {@code IndexController#openProjectionIndex(reader, sourcePath, fields)} —
 * the projection sibling of {@code openPathIndex}/{@code openCASIndex}/
 * {@code openNameIndex} — which routes committed readers through the cached
 * tiers here and write-transaction readers through
 * {@link #lookupCoveringUncommitted} (read-your-writes, uncached). This
 * class is the selection + decode-cache engine behind that method; the
 * decode cache is the projection family's one structural extra over the
 * other index types, needed because the compact persisted form is not the
 * scan form (the others scan their pages as stored, so the buffer manager
 * suffices). The vectorized executor's committed fast path calls the cached
 * front-end here directly so a cache hit costs no transaction open.
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
    final DefEntry[] candidates = selectCandidates(entries, canonicalSourcePath, requiredFields);
    for (final DefEntry candidate : candidates) {
      final ProjectionIndexRegistry.Handle handle =
          load(session, resourceKey, revision, candidate.def);
      if (handle != null) {
        SERVED.increment();
        return handle;
      }
    }
    return null;
  }

  // ==================== wtx-visible (uncommitted) serving ====================
  // The caller (AbstractIndexController#openProjectionIndex) has already
  // flushed the transaction's pending incremental maintenance and passes the
  // transaction's own reader — the storage-engine writer, whose reads see
  // the transaction log.

  private static final IndexDef[] NO_DEFS = new IndexDef[0];

  /**
   * Root-matching, covering candidate DEFINITIONS for an uncommitted (wtx)
   * lookup, ordered narrowest-first. Selection only — the caller loads each
   * candidate via {@link #loadUncommitted} (so it can interpose its own
   * per-transaction handle cache between selection and decode). Descendant-
   * pattern roots stay fail-closed here: without a path summary the pattern
   * cannot be proven unambiguous against the transaction's current state.
   */
  public static IndexDef[] selectUncommittedCandidateDefs(final Indexes indexes,
      final String[] sourcePath, final String[] requiredFields) {
    final String canonicalSourcePath = canonicalSourcePath(sourcePath);
    if (canonicalSourcePath == null) {
      return NO_DEFS;
    }
    try {
      final DefEntry[] entries = defEntriesFrom(indexes);
      if (entries.length == 0) {
        return NO_DEFS;
      }
      final DefEntry[] candidates = selectCandidates(entries, canonicalSourcePath, requiredFields);
      if (candidates.length == 0) {
        return NO_DEFS;
      }
      final IndexDef[] defs = new IndexDef[candidates.length];
      for (int i = 0; i < defs.length; i++) {
        defs[i] = candidates[i].def;
      }
      return defs;
    } catch (final RuntimeException e) {
      LOGGER.warn("Uncommitted projection candidate selection failed for source path "
          + canonicalSourcePath + ": " + e.getMessage());
      return NO_DEFS;
    }
  }

  /**
   * Probe + decode ONE definition from the transaction's own reader (its
   * writer — sees the transaction log). NO shared cache tier: uncommitted
   * state is mutable within the transaction, so caching it under a revision
   * key would poison committed-revision serving; the CALLER caches per
   * transaction, keyed by the maintenance epoch of the definition's
   * listener.
   */
  public static ProjectionIndexRegistry.@Nullable Handle loadUncommitted(
      final StorageEngineReader reader, final IndexDef def) {
    try {
      final Probe probe = probeMetadata(reader, def, -1);
      if (probe == UNUSABLE || probe.buildRevision < 0) {
        return null;
      }
      final ProjectionIndexRegistry.Handle handle = decodeLeaves(reader, def, false);
      if (handle == NOT_USABLE) {
        return null;
      }
      SERVED.increment();
      return handle;
    } catch (final RuntimeException e) {
      LOGGER.warn("Uncommitted projection load failed for definition #" + def.getID()
          + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Root-matching, covering candidates ordered narrowest-first; tiny arrays —
   * insertion sort keeps them ordered.
   */
  private static DefEntry[] selectCandidates(final DefEntry[] entries,
      final String canonicalSourcePath, final String[] requiredFields) {
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
    if (candidates == null) {
      return new DefEntry[0];
    }
    if (candidateCount < candidates.length) {
      final DefEntry[] trimmed = new DefEntry[candidateCount];
      System.arraycopy(candidates, 0, trimmed, 0, candidateCount);
      return trimmed;
    }
    return candidates;
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
      return probeMetadata(rtx.getStorageEngineReader(), def, revision);
    }
  }

  /** Reader-based probe core — also serves uncommitted (writer) reads. */
  private static Probe probeMetadata(final StorageEngineReader reader, final IndexDef def,
      final int revisionForLog) {
    final byte[] slot0;
    final ProjectionIndexMetadata metadata;
    try {
      slot0 = ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L);
      metadata = ProjectionIndexMetadata.parse(slot0);
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + " has a corrupt metadata payload at "
          + "revision " + revisionForLog + " — falling back to the generic pipeline ("
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
          + "metadata shape at revision " + revisionForLog + " (leftover sub-tree from a dropped "
          + "definition?) — falling back to the generic pipeline");
      return UNUSABLE;
    }
    return new Probe(metadata.buildRevision());
  }

  /**
   * Full decode of the projection's persisted leaves. Only reached after a
   * successful metadata probe; corruption discovered here (truncated leaf
   * list, codec failures) is logged and cached as unusable for this build.
   */
  private static ProjectionIndexRegistry.Handle decodeLeaves(final JsonResourceSession session,
      final int revision, final IndexDef def) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      return decodeLeaves(rtx.getStorageEngineReader(), def, true);
    }
  }

  /** Reader-based decode core — also serves uncommitted (writer) reads. */
  private static ProjectionIndexRegistry.Handle decodeLeaves(final StorageEngineReader reader,
      final IndexDef def, final boolean parallelHydrate) {
    // A write transaction's reader consults the transaction intent log,
    // whose read path mutates shared state (reference rebinding); readAll's
    // parallel depth-2 hydrate is analyzed safe only for read-only
    // transactions ("TIL null for RO trx") — uncommitted reads take the
    // serial cursor.
    // Descriptor layout: metadata is the slot-0 blob; leaves assemble to the raw scan form
    // directly (no per-leaf decode step). The enumeration is a serial cursor walk, safe for
    // both read-only and uncommitted (writer) reads; segment-level corruption (hash/length
    // mismatches, mixed layouts) throws IllegalStateException and is negative-cached below.
    final ProjectionIndexMetadata metadata;
    final List<byte[]> persisted;
    try {
      metadata = ProjectionIndexMetadata.parse(
          ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L));
      if (metadata == null || metadata.isStale()) {
        return NOT_USABLE;
      }
      persisted = ProjectionIndexHOTStorage.readAllLeaves(reader, def.getID());
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + ": corrupt persisted state during "
          + "decode (" + corrupt.getMessage() + ")");
      return NOT_USABLE;
    }
    final int leafCount = metadata.leafCount();
    if (persisted.size() < leafCount) {
      LOGGER.warn("Projection definition #" + def.getID() + " declares " + leafCount
          + " leaves but only " + persisted.size() + " are stored — the store is "
          + "truncated; falling back to the generic pipeline");
      return NOT_USABLE;
    }
    final List<byte[]> decoded = new ArrayList<>(leafCount);
    try {
      for (int i = 0; i < leafCount; i++) {
        decoded.add(persisted.get(i));
      }
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + ": corrupt leaf payload ("
          + corrupt.getMessage() + ")");
      return NOT_USABLE;
    }
    return new ProjectionIndexRegistry.Handle(metadata.rootPath(), metadata.buildRevision(),
        metadata.fieldNames(), decoded, null);
  }

  private static DefEntry[] defEntries(final JsonResourceSession session, final String resourceKey,
      final int revision) {
    return DEFS.get(new DefsKey(resourceKey, revision), key -> {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      return resolveDescendantRoots(session, revision, defEntriesFrom(controller.getIndexes()));
    });
  }

  /**
   * Rewrite descendant-pattern roots ({@code //...}) to CONCRETE paths via
   * the revision's path summary so the exact-match serving contract applies:
   * a pattern matching exactly ONE path class serves under that path class's
   * concrete path. Ambiguous patterns (several matching subtrees — the
   * projection aggregates across all of them, which a path-specific query
   * must not be served from) and unresolvable patterns keep the pattern
   * string, which never equals a concrete query path — fail closed. Resolved
   * per (resource, revision) and cached with the entries, so the summary
   * walk happens once, not per query.
   */
  private static DefEntry[] resolveDescendantRoots(final JsonResourceSession session,
      final int revision, final DefEntry[] entries) {
    PathSummaryReader summary = null;
    try {
      for (int i = 0; i < entries.length; i++) {
        final DefEntry entry = entries[i];
        if (!entry.rootPath.contains("//")) {
          continue;
        }
        if (summary == null) {
          summary = session.openPathSummary(revision);
        }
        final LongSet pcrs = summary.getPCRsForPath(entry.def.getProjectionRootPath());
        if (pcrs.size() != 1 || !summary.moveTo(pcrs.iterator().nextLong())) {
          continue;
        }
        final Path<QNm> concrete = summary.getPath();
        if (concrete != null) {
          entries[i] = new DefEntry(entry.def, concrete.toString(), entry.fieldNames);
        }
      }
    } catch (final RuntimeException e) {
      // Unresolved patterns simply never match a query path — fail closed.
      LOGGER.warn("Projection descendant-root resolution failed at revision " + revision + ": "
          + e.getMessage());
    } finally {
      if (summary != null) {
        summary.close();
      }
    }
    return entries;
  }

  /** Fresh (uncached) projection def entries of an index catalog. */
  private static DefEntry[] defEntriesFrom(final Indexes indexes) {
    final List<DefEntry> entries = new ArrayList<>();
    for (final IndexDef def : indexes.getIndexDefs()) {
      if (def.isProjectionIndex()) {
        entries.add(new DefEntry(def, def.getProjectionRootPath().toString(),
            ProjectionIndexChangeListener.trailingFieldNames(def)));
      }
    }
    return entries.toArray(new DefEntry[0]);
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
