/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.StorageEngineReader;
import org.jspecify.annotations.Nullable;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.Indexes;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.concurrent.atomic.LongAdder;

/**
 * Revision-scoped, catalog-driven access to projection indexes — the projection analogue of how
 * PATH/CAS/NAME scans reach their trees: the resource's {@code Indexes} catalog (of the queried
 * revision) says WHICH projections exist, and the revision's {@code ProjectionIndexPage} sub-tree
 * says WHAT they contain. Because both are versioned by the page layer's copy-on-write, every hard
 * problem is solved by construction:
 * <ul>
 * <li><b>Revision correctness / time travel</b> — a definition only exists in catalogs from its
 * commit revision onward, and each revision's sub-tree is immutable, so an executor bound to
 * revision R can only ever see projection data that was current at R.</li>
 * <li><b>Transaction isolation</b> — uncommitted builds and tombstones ride the write transaction's
 * log and are invisible to readers; rollback discards them with no compensation logic.</li>
 * <li><b>Reopen</b> — a fresh process discovers persisted projections on first query, no
 * re-bootstrap call needed (same as the other index families).</li>
 * </ul>
 *
 * <h2>Serving contract</h2> A definition serves a query only when its record-set root EXACTLY
 * equals the query's canonical source path AND its trailing field names cover the query's columns;
 * among several matches the narrowest wins and unusable candidates (stale, unreadable) are skipped
 * in favor of the next match. Descendant-pattern roots ({@code //...}) are resolved against the
 * queried revision's path summary when the pattern matches exactly ONE path class — the definition
 * then serves under that concrete path. A pattern matching several subtrees aggregates across all
 * of them by design, which a path-specific query must not be served from — those (and unresolvable
 * patterns) fail closed to the generic pipeline.
 *
 * <h2>Caching</h2> Two tiers, both bounded:
 * <ul>
 * <li>a cheap per-(resource, definition, revision) PROBE of the slot-0 metadata (single-leaf read —
 * no sub-tree hydrate) answering "usable at this revision, and which build revision?", plus a
 * per-(resource, revision) snapshot of the projection definitions' precomputed root/name strings
 * (avoids re-copying the def set and re-stringifying paths on every query);</li>
 * <li>the DECODED leaves keyed by (resource, definition, BUILD revision) — unrelated commits
 * advance the query revision but not the build revision, so an unchanged projection is decoded once
 * and shared by every subsequent revision instead of once per revision. Weighed in KiB
 * ({@code -Dsirix.projection.cacheBytes}, default 8 GiB).</li>
 * </ul>
 *
 * <h2>Failure policy</h2> Expected non-usability (no payloads, unsupported format version, stale
 * tombstone) is cached silently. Corruption evidence (metadata that no longer matches the
 * catalogued definition, truncated leaf lists, decode failures) is logged at WARN and cached as
 * unusable — queries fall back to the always-correct generic pipeline. Unexpected transient
 * failures (a session closing mid-read, I/O errors) are logged and NOT cached, so the next query
 * retries.
 *
 * <p>
 * Resource lifecycle: {@link #invalidateUnder(String)} drops all cached state for a
 * database/resource path prefix — wired into database/resource removal so a recreation at the same
 * path can never see the old store's decoded columns.
 *
 * <h2>Access</h2> The uniform, controller-mediated entry point is
 * {@code IndexController#openProjectionIndex(reader, sourcePath, fields)} — the projection sibling
 * of {@code openPathIndex}/{@code openCASIndex}/ {@code openNameIndex} — which routes committed
 * readers through the cached tiers here and write-transaction readers through
 * {@link #lookupCoveringUncommitted} (read-your-writes, uncached). This class is the selection +
 * decode-cache engine behind that method; the decode cache is the projection family's one
 * structural extra over the other index types, needed because the canonical descriptor/segment
 * persistence form is not the assembled scan form (the others scan their pages as stored, so the
 * buffer manager suffices). The vectorized executor's committed fast path calls the cached
 * front-end here directly so a cache hit costs no transaction open.
 *
 */
public final class ProjectionIndexCatalog {

  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(ProjectionIndexCatalog.class));

  /**
   * Precomputed per-definition strings — computed once per (resource, revision). {@code fieldChains}
   * holds each column's declared path relative to the DECLARED root (index-aligned with
   * {@code fieldNames}, {@code null} per slot when not relativizable); coverage matches a query's
   * deref chain against it, so a nested column cannot cover a top-level field of the same trailing
   * name. {@code rootPath} may be rewritten by the descendant-pattern resolution, which is why the
   * chains are computed from the def's own declared root instead.
   */
  private record DefEntry(IndexDef def, String rootPath, String[] fieldNames, String[] fieldChains) {
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
      Long.parseLong(System.getProperty("sirix.projection.cacheBytes", String.valueOf(8L << 30)));

  private static final Cache<DefsKey, DefEntry[]> DEFS = Caffeine.newBuilder().maximumSize(8192).build();

  private static final Cache<ProbeKey, Probe> PROBES = Caffeine.newBuilder().maximumSize(1 << 16).build();

  /** Decoded leaves, weighed in KiB so entries beyond 2 GiB stay accounted. */
  private static final Cache<DataKey, ProjectionIndexRegistry.Handle> DATA =
      Caffeine.newBuilder()
              .maximumWeight(
                  Math.max(1L, CACHE_BYTES >> 10)).<DataKey, ProjectionIndexRegistry.Handle>weigher((key, handle) -> {
                    // Column-lazy handles (P5b stage 2) weigh at their worst-case RESIDENT size (raw
                    // materialized leaves + decoded slice arrays) — Caffeine weights are fixed at
                    // insert, so a lazily-growing handle must be accounted at what it can grow to.
                    long bytes = 64;
                    if (handle.columnStoreOrNull() != null) {
                      // A handle over the eager budget never materializes those leaves: it serves
                      // through bounded windows, so its RESIDENT bytes are the window cap plus what
                      // its store retains in filled columns — and the store's CUMULATIVE retained-
                      // fill budget caps that sum at eagerMaterializeBytes, which is what makes this
                      // flat charge an upper bound rather than a hope. Charging the whole-leaf
                      // projection instead put a 10 GB handle over this cache's own maximumWeight,
                      // so Caffeine evicted it on insert and every lookup re-decoded a fresh one.
                      // R1 (headroom-gated residency) can only LOWER what a store retains — its
                      // budget is min(eagerMaterializeBytes, the heap headroom share) and query-scope
                      // exits release back down to it — so this charge, fixed at insert as Caffeine
                      // requires, stays the upper bound it was. That is also the only sense in which
                      // a release can "inform" the weigher: a fixed weight cannot be lowered later,
                      // and lowering it would in any case only admit more handles than the heap has
                      // room for the moment the released columns are filled again.
                      bytes += windowedResidentWeightBytes(handle.projectedWeightBytes());
                    } else {
                      // Eager handle: leaves are pre-materialized, so no materializer is needed.
                      for (final byte[] payload : handle.rowGroupPayloads(null)) {
                        bytes += payload == null
                            ? 0
                            : payload.length;
                      }
                    }
                    return (int) Math.min(Integer.MAX_VALUE, 1 + (bytes >> 10));
                  })
              .build();

  /** Successful catalog-served lookups — observable by tests. */
  private static final LongAdder SERVED = new LongAdder();

  /**
   * Descriptor-tier stats (P5b stage 1): total row count summed from the tiny PIXD slot values —
   * computed WITHOUT loading any segment page, keyed like {@link #DATA} so revisions sharing a build
   * share one entry. {@code totalRows < 0} is the negative entry (probed, unusable at this build).
   */
  private record DescriptorStats(long totalRows) {
  }

  private static final DescriptorStats STATS_UNUSABLE = new DescriptorStats(-1);

  private static final Cache<DataKey, DescriptorStats> DESCRIPTOR_STATS =
      Caffeine.newBuilder().maximumSize(1 << 16).build();

  private ProjectionIndexCatalog() {}

  /**
   * Serve an UNPREDICATED record count from descriptors alone (P5b stage 1): resolve a root-matching
   * usable definition exactly like {@link #lookupCovering} with no required fields, then sum the
   * descriptors' row counts — one metadata read + one trie walk over ~30-byte slot values, ZERO
   * segment-page loads and no {@link #DATA} hydrate. The descriptor walk enforces the same contiguity
   * and truncated-store checks as a full hydrate, so it can never disagree with the count the hydrate
   * path would produce.
   *
   * @return the record count, or {@code -1} to fall back (no usable definition — includes
   *         wtx/uncommitted contexts, which must keep using their epoch-scoped handles)
   */
  public static long countRowsFromDescriptors(final ResourceSession<?, ?> session, final String resourceKey,
      final int revision, final String[] sourcePath) {
    final DefEntry[] entries = defEntries(session, resourceKey, revision);
    if (entries.length == 0) {
      return -1;
    }
    final String canonicalSourcePath = canonicalSourcePath(sourcePath);
    if (canonicalSourcePath == null) {
      return -1;
    }
    final DefEntry[] candidates = selectCandidates(entries, canonicalSourcePath, NO_FIELDS);
    for (final DefEntry candidate : candidates) {
      try {
        final Probe probe = PROBES.get(new ProbeKey(resourceKey, candidate.def.getID(), revision),
            key -> probeMetadata(session, revision, candidate.def));
        if (probe == UNUSABLE || probe.buildRevision < 0) {
          continue;
        }
        final DescriptorStats stats =
            DESCRIPTOR_STATS.get(new DataKey(resourceKey, candidate.def.getID(), probe.buildRevision),
                key -> readDescriptorStats(session, revision, candidate.def));
        if (stats == null || stats.totalRows() < 0) {
          continue;
        }
        SERVED.increment();
        return stats.totalRows();
      } catch (final RuntimeException e) {
        if (DIAG) {
          System.err.println("[cat] load: threw " + e);
        }
        // Transient (session closing, I/O): not cached; the next query retries.
        LOGGER.warn("Descriptor-tier count failed transiently for resource " + resourceKey + ", definition #"
            + candidate.def.getID() + " at revision " + revision + ": " + e.getMessage());
        return -1;
      }
    }
    return -1;
  }

  private static final String[] NO_FIELDS = new String[0];

  /** Descriptor walk behind {@link #countRowsFromDescriptors}; corruption → negative entry. */
  private static DescriptorStats readDescriptorStats(final ResourceSession<?, ?> session, final int revision,
      final IndexDef def) {
    try (NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final byte[] slot0 = ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L);
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(slot0);
      if (metadata == null || metadata.isStale()) {
        return STATS_UNUSABLE;
      }
      return new DescriptorStats(
          ProjectionIndexHOTStorage.sumRowsFromColumnSegmentSlots(reader, def.getID(), metadata.rowGroupCount()));
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + " failed the descriptor-tier walk"
          + " — falling back to hydrate/generic serving (" + corrupt.getMessage() + ")");
      return STATS_UNUSABLE;
    }
  }

  /** {@link #DATA} entry count — test observability for "served without hydrating". */
  public static long dataCacheSize() {
    DATA.cleanUp();
    return DATA.estimatedSize();
  }

  /** Whether the catalog of {@code revision} holds any projection definition. */
  public static boolean hasProjections(final ResourceSession<?, ?> session, final String resourceKey,
      final int revision) {
    return defEntries(session, resourceKey, revision).length > 0;
  }

  /** Whether any catalogued projection of {@code revision} carries {@code field} as a column. */
  public static boolean anyDefCoversField(final ResourceSession<?, ?> session, final String resourceKey,
      final int revision, final String field) {
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
   * Select and load the projection serving {@code requiredFields} for the record set identified by
   * {@code sourcePath} at {@code revision}. See the class javadoc's serving contract: exact root
   * match, coverage, narrowest first, skip-unusable.
   *
   * @param sourcePath the query's source path segments (e.g. {@code ["b", "[]"]} for
   *        {@code $doc.b[]}); {@code null} or empty fails closed
   * @return a usable handle, or {@code null}
   */
  public static ProjectionIndexRegistry.Handle lookupCovering(final ResourceSession<?, ?> session,
      final String resourceKey, final int revision, final String[] sourcePath, final String[] requiredFields) {
    final DefEntry[] entries = defEntries(session, resourceKey, revision);
    if (entries.length == 0) {
      return null;
    }
    final String canonicalSourcePath = canonicalSourcePath(sourcePath);
    if (canonicalSourcePath == null) {
      return null;
    }
    final DefEntry[] candidates = selectCandidates(entries, canonicalSourcePath, requiredFields);
    if (DIAG) {
      System.err.println("[cat] " + candidates.length + " candidate(s) after filtering");
    }
    for (final DefEntry candidate : candidates) {
      final ProjectionIndexRegistry.Handle handle = load(session, resourceKey, revision, candidate.def);
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
   * Root-matching, covering candidate DEFINITIONS for an uncommitted (wtx) lookup, ordered
   * narrowest-first. Selection only — the caller loads each candidate via {@link #loadUncommitted}
   * (so it can interpose its own per-transaction handle cache between selection and decode).
   * Descendant- pattern roots stay fail-closed here: without a path summary the pattern cannot be
   * proven unambiguous against the transaction's current state.
   */
  public static IndexDef[] selectUncommittedCandidateDefs(final Indexes indexes, final String[] sourcePath,
      final String[] requiredFields) {
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
      LOGGER.warn("Uncommitted projection candidate selection failed for source path " + canonicalSourcePath + ": "
          + e.getMessage());
      return NO_DEFS;
    }
  }

  /**
   * Probe + decode ONE definition from the transaction's own reader (its writer — sees the
   * transaction log). NO shared cache tier: uncommitted state is mutable within the transaction, so
   * caching it under a revision key would poison committed-revision serving; the CALLER caches per
   * transaction, keyed by the maintenance epoch of the definition's listener.
   */
  public static ProjectionIndexRegistry.@Nullable Handle loadUncommitted(final StorageEngineReader reader,
      final IndexDef def) {
    try {
      final Probe probe = probeMetadata(reader, def, -1);
      if (probe == UNUSABLE || probe.buildRevision < 0) {
        if (DIAG) {
          System.err.println("[cat] load: probe unusable/buildRevision<0 for def #" + def.getID());
        }
        return null;
      }
      final ProjectionIndexRegistry.Handle handle = decodeRowGroups(reader, def);
      if (handle == NOT_USABLE) {
        if (DIAG) {
          System.err.println("[cat] load: decode produced NOT_USABLE for def #" + def.getID());
        }
        return null;
      }
      SERVED.increment();
      return handle;
    } catch (final RuntimeException e) {
      LOGGER.warn("Uncommitted projection load failed for definition #" + def.getID() + ": " + e.getMessage());
      return null;
    }
  }

  /**
   * Root-matching, covering candidates ordered narrowest-first; tiny arrays — insertion sort keeps
   * them ordered.
   */
  /** Diagnostic switch shared with the executor's {@code sirix.projDiag}. */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

  private static DefEntry[] selectCandidates(final DefEntry[] entries, final String canonicalSourcePath,
      final String[] requiredFields) {
    DefEntry[] candidates = null;
    int candidateCount = 0;
    if (DIAG) {
      System.err.println("[cat] " + entries.length + " projection def(s); want root='" + canonicalSourcePath
          + "' fields=" + Arrays.toString(requiredFields));
    }
    for (final DefEntry entry : entries) {
      if (!entry.rootPath.equals(canonicalSourcePath)
          || !coversAll(entry.fieldNames, entry.fieldChains, requiredFields)) {
        if (DIAG) {
          System.err.println("[cat]   skip def root='" + entry.rootPath + "' fields="
              + Arrays.toString(entry.fieldNames) + (entry.rootPath.equals(canonicalSourcePath)
                  ? " (does not cover)"
                  : " (root mismatch)"));
        }
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
   * Load (decode-cached) the projection payloads of {@code def} as valid at {@code revision}.
   * Two-tier: a cheap slot-0 metadata probe decides usability and yields the BUILD revision, which
   * keys the decoded leaves — so revisions that did not change the projection share one decoded copy.
   * Fail-soft: unusable stores yield {@code null} and query callers fall back. Creation refuses to
   * overwrite the populated tree; replacement requires drop + commit + a fresh tree id.
   */
  public static ProjectionIndexRegistry.Handle load(final ResourceSession<?, ?> session, final int revision,
      final IndexDef def) {
    return load(session, session.getResourceConfig().getResource().toString(), revision, def);
  }

  /** {@link #load(ResourceSession, int, IndexDef)} with a precomputed resource key. */
  public static ProjectionIndexRegistry.Handle load(final ResourceSession<?, ?> session, final String resourceKey,
      final int revision, final IndexDef def) {
    try {
      final Probe probe =
          PROBES.get(new ProbeKey(resourceKey, def.getID(), revision), key -> probeMetadata(session, revision, def));
      if (probe == UNUSABLE || probe.buildRevision < 0) {
        return null;
      }
      final ProjectionIndexRegistry.Handle handle = DATA.get(new DataKey(resourceKey, def.getID(), probe.buildRevision),
          key -> decodeRowGroups(session, revision, def));
      if (handle == NOT_USABLE) {
        if (DIAG) {
          System.err.println("[cat] load: decodeRowGroups -> NOT_USABLE (def #" + def.getID() + ", buildRevision "
              + probe.buildRevision + ")");
        }
        return null;
      }
      // The cached handle is SHARED and stores nothing session-lifecycle-scoped: a column-
      // lazy handle's fills bind to the CALLER's own live session because the caller threads
      // its own fetcher/materializer (built via columnSegmentFetcher/rowGroupMaterializer) into every
      // fill call — so concurrent readers on the same build revision never interfere.
      return handle;
    } catch (final RuntimeException e) {
      // Transient failure (session closing mid-read, I/O error): logged,
      // NOT cached — the next query retries. The generic pipeline is
      // always correct, so fail soft.
      LOGGER.warn("Projection probe/load failed transiently for resource " + resourceKey + ", definition #"
          + def.getID() + " at revision " + revision + ": " + e.getMessage());
      if (DIAG) {
        System.err.println("[cat] load: transient failure — " + e);
      }
      return null;
    }
  }

  /**
   * Slot-0-only probe: reads the metadata payload (one leaf, no sub-tree hydrate) and validates it
   * against the catalogued definition. Corruption is logged and cached as unusable; transient
   * failures propagate to {@link #load}'s no-cache handler.
   */
  private static Probe probeMetadata(final ResourceSession<?, ?> session, final int revision, final IndexDef def) {
    try (NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      return probeMetadata(rtx.getStorageEngineReader(), def, revision);
    }
  }

  /** Reader-based probe core — also serves uncommitted (writer) reads. */
  private static Probe probeMetadata(final StorageEngineReader reader, final IndexDef def, final int revisionForLog) {
    final byte[] slot0;
    final ProjectionIndexMetadata metadata;
    try {
      slot0 = ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L);
      metadata = ProjectionIndexMetadata.parse(slot0);
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + " has a corrupt metadata payload at " + "revision "
          + revisionForLog + " — falling back to the generic pipeline (" + corrupt.getMessage() + ")");
      if (DIAG) {
        System.err.println("[cat] probe UNUSABLE: corrupt payload — " + corrupt);
      }
      return UNUSABLE;
    }
    if (metadata == null || metadata.isStale()) {
      // Expected: never persisted, unsupported format version, or explicitly invalidated.
      if (DIAG) {
        System.err.println("[cat] probe UNUSABLE: metadata " + (metadata == null
            ? "null (absent or unsupported format version)"
            : "stale") + ", slot0 bytes="
            + (slot0 == null
                ? -1
                : slot0.length));
      }
      return UNUSABLE;
    }
    if (!metadata.matches(def.getProjectionRootPath().toString(), defFieldPaths(def), defColumnKinds(def))) {
      LOGGER.warn("Projection definition #" + def.getID() + " does not match its persisted "
          + "metadata shape at revision " + revisionForLog + " (leftover sub-tree from a dropped "
          + "definition?) — falling back to the generic pipeline");
      if (DIAG) {
        System.err.println("[cat] probe UNUSABLE: shape mismatch");
      }
      return UNUSABLE;
    }
    return new Probe(metadata.buildRevision());
  }

  /**
   * Full decode of the projection's persisted leaves. Only reached after a successful metadata probe;
   * corruption discovered here (truncated leaf list, codec failures) is logged and cached as unusable
   * for this build.
   */
  private static ProjectionIndexRegistry.Handle decodeRowGroups(final ResourceSession<?, ?> session, final int revision,
      final IndexDef def) {
    try (NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexRegistry.Handle lazy =
          tryBuildColumnLazyHandle(session, revision, def, rtx.getStorageEngineReader());
      if (lazy != null) {
        return lazy;
      }
      return decodeRowGroups(rtx.getStorageEngineReader(), def);
    }
  }

  /**
   * P5b stage 2: build a COLUMN-LAZY handle from one descriptor walk — zero segment reads at load
   * time. Column kernels fetch only their columns' BODY segments (one fresh read transaction per
   * column fill); whole-leaf consumers materialize through the same assembling read the eager path
   * uses. Returns {@code null} to fall back to eager decoding (unresolved refs, corrupt walk — the
   * eager path re-surfaces the corruption through the established fail-soft flow), or
   * {@link #NOT_USABLE} for stale/truncated.
   */
  private static ProjectionIndexRegistry.@Nullable Handle tryBuildColumnLazyHandle(final ResourceSession<?, ?> session,
      final int revision, final IndexDef def, final StorageEngineReader reader) {
    final ProjectionIndexMetadata metadata;
    final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories;
    final int[] physicalOrder;
    final long tParse;
    final long t0 = DIAG
        ? System.nanoTime()
        : 0L;
    try {
      metadata = ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L));
      if (metadata == null || metadata.isStale()) {
        return NOT_USABLE;
      }
      physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, def.getID(), metadata.rowGroupCount());
      tParse = DIAG
          ? System.nanoTime()
          : 0L;
      // The segment-slot directory reader captures each segment's durable offset (and an inline
      // segment slot's bytes), so a column fill batches ONLY the queried column's offsets — reading
      // one column's segments across all row groups and skipping the rest. Whole-row-group query
      // shapes materialize from the same directory representation.
      // This call site — and only this one — opts into the parallel walk: it runs under a fresh
      // read-only transaction on a COMMITTED revision, so extra leases resolve the same immutable
      // pages. The writer-facing decode path keeps the serial cursor walk (its reader consults a
      // transaction intent log, whose read path mutates shared state).
      directories = ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, def.getID(),
          metadata.rowGroupCount(), physicalOrder, worker -> {
            try (NodeReadOnlyTrx laneRtx = session.beginNodeReadOnlyTrx(revision)) {
              worker.accept(laneRtx.getStorageEngineReader());
            }
          });
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + ": corrupt persisted state during " + "directory walk ("
          + corrupt.getMessage() + ")");
      if (DIAG) {
        System.err.println("[cat] directory walk CORRUPT — " + corrupt);
      }
      return NOT_USABLE;
    }
    if (directories == null) {
      return null; // unresolved segment refs — eager path handles it
    }
    final int rowGroupCount = metadata.rowGroupCount();
    if (directories.size() < rowGroupCount) {
      LOGGER.warn("Projection definition #" + def.getID() + " declares " + rowGroupCount + " leaves but only "
          + directories.size() + " are stored — the store is " + "truncated; falling back to the generic pipeline");
      if (DIAG) {
        System.err.println("[cat] decode: TRUNCATED — metadata declares " + rowGroupCount
            + " leaves, directory walk found " + directories.size());
      }
      return NOT_USABLE;
    }
    final List<ProjectionIndexHOTStorage.RowGroupDirectory> live = directories.size() == rowGroupCount
        ? directories
        : directories.subList(0, rowGroupCount);
    // Worst-case RESIDENT weight (Caffeine weights are fixed at insert): the raw leaves a
    // whole-leaf consumer materializes (Σ segment byteLens) PLUS the decoded column-slice
    // arrays (bit-packed segments decode to 8 bytes/value — up to ~8× their packed size).
    long projectedBytes = 0;
    for (final ProjectionIndexHOTStorage.RowGroupDirectory dir : live) {
      projectedBytes += residentWeightOf(dir.descriptor());
    }
    // The shared store carries only immutable descriptor state; every fill binds to the
    // CALLER's own live fetcher, threaded in per call — nothing session-scoped is stored.
    final long tWalk = DIAG
        ? System.nanoTime()
        : 0L;
    final ProjectionColumnStore store = new ProjectionColumnStore(live);
    // Fingerprint manifests and durable chunk locators are captured synchronously on this already
    // owned reader. Payload pages remain deferred to the caller-scoped fetcher, so hydration is a
    // small, resource-free range walk and there is no daemon transaction to leak on an early return.
    final ProjectionBloomChunks.ColumnEvidence[] bloomBlocks =
        readBloomBlocks(reader, def, metadata.columnKinds(), metadata.rowGroupCount(), physicalOrder);
    projectedBytes += ProjectionBloomChunks.retainedBytes(bloomBlocks);
    if (DIAG) {
      final long tBloom = System.nanoTime();
      System.err.printf(
          "[cat] lazy-handle timings: metaParse %.1f ms, directoryWalk %.1f ms, bloomBlocks %.1f ms (%d leaves,"
              + " projectedWeight %d MB)%n",
          (tParse - t0) / 1e6, (tWalk - tParse) / 1e6, (tBloom - tWalk) / 1e6, rowGroupCount, projectedBytes >> 20);
    }
    if (bloomBlocks != null) {
      store.attachBloomBlocks(bloomBlocks);
    }
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.Handle.columnLazy(metadata.rootPath(),
        metadata.buildRevision(), metadata.fieldNames(), store, def.getID(), projectedBytes);
    // Metadata identifies bounded summary chunks; hydrate them before the handle can serve counts.
    handle.setSetValueRowCounts(ProjectionSetSummaryChunks.readAll(reader, def.getID(), metadata.setValueRowCounts()));
    // …as do the per-column value dictionary anchors, without which a global string column can
    // only be scanned, never probed: resolving a predicate literal to an id needs the anchor.
    handle.setValueDictionaryHeaderKeys(metadata.valueDictionaryHeaderKeys());
    // …and so do the declared column paths, which is what keeps a NESTED column from answering a
    // top-level deref of the same trailing name (Handle#columnOf).
    handle.setFieldChains(metadata.fieldChains());
    return handle;
  }

  /**
   * Worst-case RESIDENT bytes for one row group — Caffeine fixes a weight at insert, so this counts
   * the raw segments a whole-leaf consumer materializes PLUS what they decode to (a bit-packed
   * segment becomes 8 bytes per value, up to ~8× its packed size).
   */
  private static long residentWeightOf(final byte[] descriptor) {
    long bytes = 0;
    final int columnSegmentCount = RowGroupDescriptor.columnSegmentCount(descriptor);
    for (int i = 0; i < columnSegmentCount; i++) {
      bytes += RowGroupDescriptor.entryByteLen(descriptor, i);
    }
    final int columnCount = RowGroupDescriptor.columnCount(descriptor);
    for (int c = 0; c < columnCount; c++) {
      // The SAME arithmetic the store's fill doors price against, so the weight that admits a
      // handle and the budget that declines its fills cannot disagree about what a column costs.
      bytes += ProjectionColumnStore.decodedColumnResidentBytes(descriptor, c, RowGroupDescriptor.kind(descriptor, c));
    }
    return bytes;
  }

  /**
   * The per-string-column fingerprint blocks, or {@code null} when this store carries none worth
   * attaching. A block covering fewer leaves than the store has is ignored rather than trusted: it
   * predates leaves that were added since, and a filter that has not seen a value cannot exclude it.
   */
  private static ProjectionBloomChunks.ColumnEvidence @Nullable [] readBloomBlocks(final StorageEngineReader reader,
      final IndexDef def, final byte[] columnKinds, final int rowGroupCount, final int[] physicalOrder) {
    return ProjectionBloomChunks.reorder(ProjectionBloomChunks.read(reader, def.getID(), columnKinds, rowGroupCount),
        physicalOrder);
  }

  /**
   * Segment fetcher bound to one session+revision: one fresh read transaction per column fill
   * (batched). Built by a CALLER from its OWN live session and threaded into the shared handle's fill
   * calls — the handle never stores it.
   */
  public static ProjectionColumnStore.ColumnSegmentFetcher columnSegmentFetcher(final ResourceSession<?, ?> session,
      final int revision) {
    return new ProjectionColumnStore.ColumnSegmentFetcher() {
      @Override
      public byte @Nullable [] @Nullable [] fetchAll(final long[] offsets) {
        try (NodeReadOnlyTrx fetchRtx = session.beginNodeReadOnlyTrx(revision)) {
          // Batched + backend-coalesced (P5b stage 4b): runs of near-adjacent segment offsets
          // become single ranged reads instead of a pread pair per segment.
          return ProjectionIndexHOTStorage.readSegmentBytesBatch(fetchRtx.getStorageEngineReader(), offsets);
        }
      }

      @Override
      public void fetchRange(final long[] offsets, final int from, final int to, final byte[][] out) {
        final int len = to - from;
        // A transaction PER RANGE: concurrent read transactions are supported (the parallel
        // materialize walk already relies on it), a single one is not thread-safe, and the offsets
        // ascend with the leaf index — so a contiguous range still coalesces into ranged reads.
        try (NodeReadOnlyTrx fetchRtx = session.beginNodeReadOnlyTrx(revision)) {
          final long[] requested = from == 0 && to == offsets.length
              ? offsets
              : Arrays.copyOfRange(offsets, from, to);
          final byte[][] part =
              ProjectionIndexHOTStorage.readSegmentBytesBatch(fetchRtx.getStorageEngineReader(), requested);
          if (part == null || part.length != len) {
            throw new IllegalStateException("Segment fetcher returned " + (part == null
                ? "null"
                : part.length + " results") + " for " + len + " offsets");
          }
          System.arraycopy(part, 0, out, from, len);
        }
      }

      @Override
      public boolean rangedFetchIsConcurrent() {
        return true;
      }
    };
  }

  /**
   * Byte budget above which a whole-leaf consumer is served by a WINDOWED payload view instead of an
   * eager whole-column materialization.
   *
   * <p>
   * Derived, not invented: an eager {@code List<byte[]>} of every leaf lives inside the {@link #DATA}
   * cache's byte budget ({@code sirix.projection.cacheBytes}) — an entry heavier than half that
   * budget is guaranteed cache-thrash — and it must also fit the heap beside the off-heap arena, so a
   * quarter of {@code Runtime.maxMemory()} bounds it from the other side. At 100M rows a fat-string
   * projection's leaves total ~8-10 GB; the first whole-leaf consumer (string LIKE, distinct over
   * strings) OOMed the materializer inside the catalog load. The projected weight compared against
   * this is the handle's worst-case RESIDENT bytes — the same conservative figure the cache weigher
   * uses — so the route flips to windowed strictly BEFORE the eager list could have fit.
   */
  private static final long EAGER_MATERIALIZE_BYTES_DEFAULT = Long.getLong("sirix.projection.eagerMaterializeBytes",
      Math.min(CACHE_BYTES / 2, Runtime.getRuntime().maxMemory() / 4));

  private static volatile long eagerMaterializeBytes = EAGER_MATERIALIZE_BYTES_DEFAULT;

  /** Logical row groups per materialized window — ~12 MB of ClickBench-shaped leaves. */
  private static final int WINDOW_LEAVES_DEFAULT = 128;

  private static volatile int windowLeaves = WINDOW_LEAVES_DEFAULT;

  /**
   * Test seam: shrink the window so a handful of leaves still spans several windows.
   *
   * @param value logical row groups per window
   * @return the previous value, for restoring in a finally block
   */
  public static int setWindowLeavesForTesting(final int value) {
    final int previous = windowLeaves;
    windowLeaves = Math.max(1, value);
    return previous;
  }

  /**
   * Test seam: shrink the eager budget so a small store still exercises the windowed route.
   *
   * @param value the budget in bytes
   * @return the previous budget, for restoring in a finally block
   */
  public static long setEagerMaterializeBytesForTesting(final long value) {
    final long previous = eagerMaterializeBytes;
    eagerMaterializeBytes = value;
    return previous;
  }

  /**
   * Whole-leaf materializer bound to one session+revision — built by a CALLER from its OWN live
   * session and threaded into {@link ProjectionIndexRegistry.Handle#rowGroupPayloads} on demand; the
   * handle never stores it. Always eager; callers that know the handle's projected weight use the
   * budget-aware overload.
   */
  public static Supplier<List<byte[]>> rowGroupMaterializer(final ResourceSession<?, ?> session, final int revision,
      final int defId, final int rowGroupCount) {
    return rowGroupMaterializer(session, revision, defId, rowGroupCount, 0L);
  }

  /**
   * As above, choosing the route by projected size: under the eager budget the whole column family
   * materializes once and serves every later query from bytes; above it the supplier returns a
   * {@link ProjectionWindowedRowGroupPayloads} view that materializes bounded leaf windows on demand
   * — the {@code List} contract every byte-scan kernel already programs against, without the
   * whole-column residency that OOMed fat string columns at 100M.
   *
   * @param projectedWeightBytes the handle's worst-case resident bytes
   *        ({@link ProjectionIndexRegistry.Handle#projectedWeightBytes()}), or {@code 0} to force the
   *        eager route
   */
  public static Supplier<List<byte[]>> rowGroupMaterializer(final ResourceSession<?, ?> session, final int revision,
      final int defId, final int rowGroupCount, final long projectedWeightBytes) {
    Objects.requireNonNull(session, "session");
    if (servesWindowedPayloads(projectedWeightBytes)) {
      // The windowed arm hands the handle BOTH the build and this caller's reader source, so a view
      // the handle already memoized is rebound to this live session instead of rebuilt.
      return new ProjectionWindowedRowGroupPayloads.BoundMaterializer() {
        @Override
        public List<byte[]> get() {
          // The BUILD yields the shared cache; what leaves this method is always a thin view over
          // it carrying THIS caller's source, so no shared object ever holds one.
          return windowedRowGroupPayloads(session, revision, defId, rowGroupCount, projectedWeightBytes).boundTo(
              readerSource());
        }

        @Override
        public ProjectionWindowedRowGroupPayloads.ReaderSource readerSource() {
          return () -> session.beginNodeReadOnlyTrx(revision);
        }
      };
    }
    return () -> {
      if (projectedWeightBytes > 0) {
        // A LAZY handle materialized eagerly because it fit — the banner's third counter.
        ChunkedBodyConfig.recordEagerColumnMaterialization();
      }
      final List<byte[]> persisted = materializeRowGroups(session, revision, defId, rowGroupCount);
      if (persisted.size() < rowGroupCount) {
        throw new IllegalStateException("Projection definition #" + defId + " truncated during " + "materialization: "
            + persisted.size() + " < " + rowGroupCount);
      }
      return persisted.size() == rowGroupCount
          ? persisted
          : new ArrayList<>(persisted.subList(0, rowGroupCount));
    };
  }

  /**
   * Whether a handle of this projected weight is served by the WINDOWED payload view rather than by
   * whole-column eager materialization.
   *
   * @param projectedWeightBytes the handle's worst-case resident bytes
   *        ({@link ProjectionIndexRegistry.Handle#projectedWeightBytes()})
   * @return {@code true} when the windowed route serves this weight
   */
  public static boolean servesWindowedPayloads(final long projectedWeightBytes) {
    return projectedWeightBytes > eagerMaterializeBytes;
  }

  /**
   * RESIDENT bytes a handle of this projected weight can hold — the figure the {@link #DATA} weigher
   * charges. Under the eager budget that is the projection itself; over it the leaves are never
   * materialized, so it is the budget that bounds the window cap and each column fill.
   *
   * @param projectedWeightBytes the handle's whole-leaf projection
   * @return the resident bytes to charge
   */
  static long windowedResidentWeightBytes(final long projectedWeightBytes) {
    final long resident = servesWindowedPayloads(projectedWeightBytes)
        // The window cap is a quarter of the budget and the store's cumulative retained fills are
        // capped at the budget; charge both so the figure bounds what the handle can actually hold.
        ? eagerMaterializeBytes + (eagerMaterializeBytes >> 2)
        // An under-budget handle materializes its leaves, so the projection IS its residency.
        : projectedWeightBytes;
    // Clamp ONCE, after the branch — EITHER branch can exceed what this cache admits, and the
    // windowed one is the likelier: eagerMaterializeBytes is settable, so 1.25x it can pass
    // maximumWeight while the eager branch is bounded by that same budget. A charge above the
    // ceiling self-evicts the entry on insert and makes every lookup re-decode a fresh handle,
    // which is the failure this figure exists to prevent.
    return Math.max(1L, Math.min(resident, Math.max(1L, CACHE_BYTES >> 1)));
  }

  /**
   * Build the windowed payload view: one physical-order read up front, one fetch per window after.
   */
  private static ProjectionWindowedRowGroupPayloads windowedRowGroupPayloads(final ResourceSession<?, ?> session,
      final int revision, final int defId, final int rowGroupCount, final long projectedWeightBytes) {
    ChunkedBodyConfig.recordWindowedColumnEngagement();
    final int windowSize = windowLeaves;
    final int[] physicalOrder;
    try (NodeReadOnlyTrx orderRtx = session.beginNodeReadOnlyTrx(revision)) {
      physicalOrder = ProjectionIndexFences.readPhysicalOrder(orderRtx.getStorageEngineReader(), defId, rowGroupCount);
    }
    // Resident cap from the SAME budget that declined the eager route: a quarter of it in windows,
    // sized by the projected per-leaf weight (conservative — includes decode expansion, so the cap
    // errs toward fewer resident windows), and never below the machine's worker count so a parallel
    // shard sweep does not thrash at its shard boundaries.
    final long projectedWindowBytes = Math.max(1L, projectedWeightBytes / Math.max(1, rowGroupCount)) * windowSize;
    final int residentCap = (int) Math.min(Integer.MAX_VALUE,
        Math.max(Runtime.getRuntime().availableProcessors(), (eagerMaterializeBytes / 4) / projectedWindowBytes));
    if (DIAG) {
      System.err.println("[cat] windowed payloads: defId=" + defId + " rowGroups=" + rowGroupCount + " projected="
          + (projectedWeightBytes >> 20) + "MB windowLeaves=" + windowSize + " residentCap=" + residentCap);
    }
    // The fetcher captures the physical order and the definition id — both inert — and NOTHING
    // session-scoped: its transaction comes from the source the consult site binds.
    return new ProjectionWindowedRowGroupPayloads(rowGroupCount, windowSize, residentCap,
        (source, from, toExclusive) -> {
          final byte[][] window = new byte[toExclusive - from][];
          // A transaction PER WINDOW: concurrent read transactions are supported, one is not
          // thread-safe, and parallel kernels touch disjoint windows concurrently.
          try (NodeReadOnlyTrx windowRtx = source.openReader()) {
            final StorageEngineReader reader = windowRtx.getStorageEngineReader();
            for (int logical = from; logical < toExclusive; logical++) {
              final byte[] payload =
                  ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, defId, physicalOrder[logical]);
              if (payload == null) {
                throw new IllegalStateException("Projection definition #" + defId + " truncated during windowed "
                    + "materialization: logical row group " + logical + " (physical slot " + physicalOrder[logical]
                    + ") has no payload");
              }
              window[logical - from] = payload;
            }
          }
          return window;
        });
  }

  /** Above this many row groups the slot walk partitions across per-thread readers. */
  private static final int PARALLEL_MATERIALIZE_MIN = 128;

  /**
   * One projection materialization. The slot WALK is the cold-start whale — the range cursor
   * force-decodes every trie page it passes, serially — so for large stores it partitions the
   * contiguous row-group id space across ForkJoin workers, each on its OWN read transaction
   * (concurrent read trxs are supported; decoded pages land in the shared buffer manager, so boundary
   * pages decoded twice cost duplicate work, never correctness). Batch resolution and assembly
   * (phases 3-5) then run exactly as the serial path does.
   */
  private static List<byte[]> materializeRowGroups(final ResourceSession<?, ?> session, final int revision,
      final int defId, final int rowGroupCount) {
    final int[] physicalOrder;
    try (NodeReadOnlyTrx orderRtx = session.beginNodeReadOnlyTrx(revision)) {
      physicalOrder = ProjectionIndexFences.readPhysicalOrder(orderRtx.getStorageEngineReader(), defId, rowGroupCount);
    }
    final int physicalUpperBound = ProjectionIndexHOTStorage.physicalSlotUpperBound(physicalOrder);
    final int workers = Math.min(Runtime.getRuntime().availableProcessors(), Math.max(1, physicalUpperBound / 64));
    final long t0 = DIAG
        ? System.nanoTime()
        : 0L;
    if (rowGroupCount < PARALLEL_MATERIALIZE_MIN || workers <= 1) {
      try (NodeReadOnlyTrx matRtx = session.beginNodeReadOnlyTrx(revision)) {
        return ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(matRtx.getStorageEngineReader(), defId,
            rowGroupCount, physicalOrder);
      }
    }
    @SuppressWarnings("unchecked")
    final Long2ObjectRBTreeMap<ProjectionIndexHOTStorage.RawBlobSlot>[] descParts = new Long2ObjectRBTreeMap[workers];
    @SuppressWarnings("unchecked")
    final ArrayList<ProjectionIndexHOTStorage.RawBlobSlot>[] segParts = new ArrayList[workers];
    final int chunk = (physicalUpperBound + workers - 1) / workers;
    ForkJoinPool.commonPool().invoke(new RecursiveAction() {
      @Override
      protected void compute() {
        final RecursiveAction[] subs = new RecursiveAction[workers];
        for (int w = 0; w < workers; w++) {
          final int idx = w;
          final long lo = 1L + (long) w * chunk;
          final long hi = Math.min(lo + chunk - 1, physicalUpperBound);
          subs[w] = new RecursiveAction() {
            @Override
            protected void compute() {
              final Long2ObjectRBTreeMap<ProjectionIndexHOTStorage.RawBlobSlot> desc = new Long2ObjectRBTreeMap<>();
              final ArrayList<ProjectionIndexHOTStorage.RawBlobSlot> segs = new ArrayList<>();
              try (NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
                ProjectionIndexHOTStorage.collectSlotsRange(rtx.getStorageEngineReader(), defId, rowGroupCount, lo, hi,
                    desc, segs);
              }
              descParts[idx] = desc;
              segParts[idx] = segs;
            }
          };
        }
        invokeAll(subs);
      }
    });
    final Long2ObjectRBTreeMap<ProjectionIndexHOTStorage.RawBlobSlot> descriptors = new Long2ObjectRBTreeMap<>();
    final ArrayList<ProjectionIndexHOTStorage.RawBlobSlot> segmentSlots = new ArrayList<>();
    for (int w = 0; w < workers; w++) {
      if (descParts[w] != null) {
        descriptors.putAll(descParts[w]); // disjoint id ranges — no overwrite possible
      }
      if (segParts[w] != null) {
        segmentSlots.addAll(segParts[w]);
      }
    }
    final long t1 = DIAG
        ? System.nanoTime()
        : 0L;
    final ProjectionIndexHOTStorage.RawBlobSlot[] descArr =
        ProjectionIndexHOTStorage.drainOrderedDescriptors(descriptors, rowGroupCount, defId, physicalOrder);
    final NodeReadOnlyTrx[] laneTrxs = new NodeReadOnlyTrx[workers];
    final StorageEngineReader[] laneReaders = new StorageEngineReader[workers];
    try (NodeReadOnlyTrx matRtx = session.beginNodeReadOnlyTrx(revision)) {
      try {
        for (int w = 0; w < workers; w++) {
          laneTrxs[w] = session.beginNodeReadOnlyTrx(revision);
          laneReaders[w] = laneTrxs[w].getStorageEngineReader();
        }
      } catch (final RuntimeException openFailed) {
        for (final NodeReadOnlyTrx t : laneTrxs) {
          if (t != null) {
            t.close();
          }
        }
        throw openFailed;
      }
      final List<byte[]> out;
      try {
        out = ProjectionIndexHOTStorage.assembleRowGroupsFromSlots(matRtx.getStorageEngineReader(), defId,
            rowGroupCount, descArr, segmentSlots, physicalOrder, laneReaders);
      } finally {
        for (final NodeReadOnlyTrx t : laneTrxs) {
          if (t != null) {
            t.close();
          }
        }
      }
      if (DIAG) {
        System.err.println("[cat] parallel materialize: walk=" + (t1 - t0) / 1_000_000 + "ms assemble="
            + (System.nanoTime() - t1) / 1_000_000 + "ms workers=" + workers + " rowGroups=" + rowGroupCount);
      }
      return out;
    }
  }

  /** Reader-based decode core — also serves uncommitted (writer) reads. */
  private static ProjectionIndexRegistry.Handle decodeRowGroups(final StorageEngineReader reader, final IndexDef def) {
    // Metadata is the slot-0 blob; leaves assemble to the raw scan form directly (no per-leaf
    // decode step). The enumeration is a serial cursor walk, safe for both read-only and
    // uncommitted (writer) reads alike — which is why this takes no read-only/writer mode: a
    // write transaction's reader consults the transaction intent log, whose read path mutates
    // shared state (reference rebinding), and no parallel hydrate runs over it. Segment-level
    // corruption (hash/length/shape mismatches) throws IllegalStateException and is
    // negative-cached below.
    final ProjectionIndexMetadata metadata;
    final List<byte[]> persisted;
    try {
      metadata = ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, def.getID(), 0L));
      if (metadata == null || metadata.isStale()) {
        return NOT_USABLE;
      }
      final int[] physicalOrder =
          ProjectionIndexFences.readPhysicalOrder(reader, def.getID(), metadata.rowGroupCount());
      persisted = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(reader, def.getID(),
          metadata.rowGroupCount(), physicalOrder);
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + ": corrupt persisted state during " + "decode ("
          + corrupt.getMessage() + ")");
      if (DIAG) {
        System.err.println("[cat] eager decode CORRUPT — " + corrupt);
      }
      return NOT_USABLE;
    }
    final int rowGroupCount = metadata.rowGroupCount();
    if (persisted.size() < rowGroupCount) {
      LOGGER.warn("Projection definition #" + def.getID() + " declares " + rowGroupCount + " leaves but only "
          + persisted.size() + " are stored — the store is " + "truncated; falling back to the generic pipeline");
      if (DIAG) {
        System.err.println("[cat] eager decode TRUNCATED — declares " + rowGroupCount + ", found " + persisted.size());
      }
      return NOT_USABLE;
    }
    final List<byte[]> decoded = new ArrayList<>(rowGroupCount);
    try {
      for (int i = 0; i < rowGroupCount; i++) {
        decoded.add(persisted.get(i));
      }
    } catch (final IllegalStateException corrupt) {
      LOGGER.warn("Projection definition #" + def.getID() + ": corrupt leaf payload (" + corrupt.getMessage() + ")");
      return NOT_USABLE;
    }
    final ProjectionIndexRegistry.Handle handle = new ProjectionIndexRegistry.Handle(metadata.rootPath(),
        metadata.buildRevision(), metadata.fieldNames(), decoded, null);
    handle.setSetValueRowCounts(ProjectionSetSummaryChunks.readAll(reader, def.getID(), metadata.setValueRowCounts()));
    handle.setFieldChains(metadata.fieldChains());
    // The eager path needs the value dictionary anchors as much as the column-lazy one does: without
    // them a global column's ids have nothing to resolve against, and every route that would consume
    // them declines — silently, and only on whichever hydrate path a given store happens to take.
    handle.setValueDictionaryHeaderKeys(metadata.valueDictionaryHeaderKeys());
    return handle;
  }

  private static DefEntry[] defEntries(final ResourceSession<?, ?> session, final String resourceKey,
      final int revision) {
    return DEFS.get(new DefsKey(resourceKey, revision), key -> {
      final IndexController<?, ?> controller = session.getRtxIndexController(revision);
      return resolveDescendantRoots(session, revision, defEntriesFrom(controller.getIndexes()));
    });
  }

  /**
   * Rewrite descendant-pattern roots ({@code //...}) to CONCRETE paths via the revision's path
   * summary so the exact-match serving contract applies: a pattern matching exactly ONE path class
   * serves under that path class's concrete path. Ambiguous patterns (several matching subtrees — the
   * projection aggregates across all of them, which a path-specific query must not be served from)
   * and unresolvable patterns keep the pattern string, which never equals a concrete query path —
   * fail closed. Resolved per (resource, revision) and cached with the entries, so the summary walk
   * happens once, not per query.
   */
  private static DefEntry[] resolveDescendantRoots(final ResourceSession<?, ?> session, final int revision,
      final DefEntry[] entries) {
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
          entries[i] = new DefEntry(entry.def, concrete.toString(), entry.fieldNames, entry.fieldChains);
        }
      }
    } catch (final RuntimeException e) {
      // Unresolved patterns simply never match a query path — fail closed.
      LOGGER.warn("Projection descendant-root resolution failed at revision " + revision + ": " + e.getMessage());
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
        final String declaredRoot = def.getProjectionRootPath().toString();
        entries.add(new DefEntry(def, declaredRoot, ProjectionIndexChangeListener.trailingFieldNames(def),
            ProjectionIndexMetadata.relativeFieldChains(declaredRoot, defFieldPaths(def))));
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
      kinds[i] = ProjectionIndexBuilder.mapTypeToColumnKind(def.getProjectionFieldTypes().get(i),
          def.getProjectionFields().get(i));
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

  /**
   * Whether every required field token resolves to a column of this definition, under exactly the
   * rule {@link ProjectionIndexRegistry.Handle#columnOf(String)} applies at scan time: a column with
   * a relativizable declared path matches its CHAIN ({@code "commit/collection"}), one without falls
   * back to its trailing name. Selecting a candidate the handle would then refuse to resolve would
   * only waste a load — and, worse, could shadow a definition that does match.
   */
  private static boolean coversAll(final String[] names, final String[] chains, final String[] requiredFields) {
    outer: for (final String required : requiredFields) {
      for (int i = 0; i < names.length; i++) {
        final String chain = chains == null || chains.length != names.length
            ? null
            : chains[i];
        if (chain == null
            ? names[i].equals(required)
            : chain.equals(required)) {
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
   * Drop all cached state whose resource path starts with {@code pathPrefix} — wired into
   * database/resource removal so a store recreated at the same path can never be served the removed
   * store's columns.
   */
  public static void invalidateUnder(final String pathPrefix) {
    DEFS.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
    PROBES.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
    DATA.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
    DESCRIPTOR_STATS.asMap().keySet().removeIf(key -> key.resourceKey.startsWith(pathPrefix));
  }

  /** Drop all cached decodes — for test isolation. */
  public static void clearCache() {
    DEFS.invalidateAll();
    PROBES.invalidateAll();
    DATA.invalidateAll();
    DESCRIPTOR_STATS.invalidateAll();
  }
}
