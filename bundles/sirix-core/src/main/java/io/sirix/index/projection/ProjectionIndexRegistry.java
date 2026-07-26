/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Process-wide lookup from (resource, sourcePath, fields) to a pre-built
 * list of serialised {@link ProjectionIndexRowGroupPage} byte[]s.
 *
 * <p>BENCH/TEST wiring: an in-memory pool that lets the query-path executor
 * ({@link io.sirix.query.scan.SirixVectorizedExecutor}) serve projections
 * for stores WITHOUT catalogued definitions (legacy bench layouts,
 * in-memory-only bench runs, tests). Production discovery goes through
 * {@link ProjectionIndexCatalog} — the revision-scoped catalog + page
 * layer, which inherits transactionality and invalidation (stale
 * tombstones written by {@link ProjectionIndexChangeListener}) from the
 * page layer's copy-on-write. Entries here are identified structurally by
 * (root path, ordered field list); NOTE that pool entries installed via the
 * legacy overloads (root {@code null}, valid-from {@code 0}) are NOT
 * invalidated by updates — callers own their lifecycle. Incremental leaf
 * maintenance remains future work (task #57).
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
 * The installed handle's {@code rowGroupPayloads} list must not be mutated
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
    /** Eagerly-hydrated raw leaves, or the lazily-materialized cache of a column-lazy handle. */
    private volatile List<byte[]> rowGroupPayloads;
    /**
     * Column-sliced view (P5b stage 2) — non-null marks a COLUMN-LAZY handle: constructed
     * from descriptors only; {@link #rowGroupPayloads} materializes whole raw leaves on first
     * whole-leaf consumer, while column-scoped kernels and gates read {@link #columnStore}
     * slices and never trigger that materialization.
     */
    private final ProjectionColumnStore columnStore;
    /**
     * Catalog index-definition id of a column-lazy handle (immutable identity, {@code -1} for
     * eager/bench handles). NOT session-scoped: the caller uses it to build a whole-leaf
     * materializer bound to ITS OWN live session and threads that into
     * {@link #rowGroupPayloads(Supplier)} — the handle stores nothing session-lifecycle-scoped.
     */
    private final int defId;
    /** Guards materialization only — never the gate caches, which use {@code this}. */
    private final Object materializeLock = new Object();
    /**
     * Worst-case RESIDENT weight of a column-lazy handle for cache accounting (Caffeine
     * weights are fixed at insert): raw materialized leaves (Σ descriptor byteLens) PLUS
     * the decoded column-slice arrays a fully-touched handle retains.
     */
    private final long projectedWeightBytes;
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
     * Sentinel for "integrality was probed and is UNKNOWN" — some leaf lacks
     * a valid presence tail, so value-exact consumers must fail closed.
     */
    private static final boolean[] INTEGRALITY_UNKNOWN = new boolean[0];

    /**
     * Per-column integrality evidence for NUMERIC_LONG columns: {@code true}
     * means a non-integral (truncated) value was SEEN in that column.
     * Populated eagerly from builder-tracked flags when the installer passes
     * them, otherwise lazily re-derived from the leaf payloads' presence tails via
     * {@link ProjectionIndexByteScan#probeNumericNonIntegral} — so the
     * evidence survives persistence and close/re-open just like the sparse
     * evidence. {@code null} = not yet resolved;
     * {@link #INTEGRALITY_UNKNOWN} = probed, provenance unavailable
     * (malformed leaves) — value-exact consumers must treat that as
     * not-provably-integral.
     */
    private volatile boolean[] integralityEvidence;

    /**
     * Lazily-probed per-column sparse evidence — values are
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_CLEAN} /
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_DIRTY}. Computed once for
     * ALL columns by {@link ProjectionIndexByteScan#probeSparseEvidence} on
     * first use; the evidence lives INSIDE the leaf payloads (presence
     * tail + per-column unrepresentable flags) so it survives persistence
     * and re-encoding — the integrality evidence follows the same pattern
     * via flag bit1 of the same tail (see {@link #integralityEvidence}).
     */
    private volatile byte[] sparseStatus;

    /**
     * Canonical root path of the record set the columns were built over, or
     * {@code null} for legacy/bench installs that predate root tracking
     * (matches any root). Part of the handle's identity: two projections
     * with identical trailing field names but different roots are DIFFERENT
     * indexes and must never overwrite or answer for each other.
     */
    private final String rootPath;

    /**
     * First revision this handle's columns are valid for. An executor bound
     * to an OLDER revision must not use the handle (time travel would read
     * future data). The gate says nothing about LATER revisions — the handle
     * is a point-in-time snapshot that update commits do not refresh or
     * uninstall, so for catalogued projections the revision-scoped
     * {@link ProjectionIndexCatalog} is authoritative and query paths must
     * not fall back to the registry (see the executor's lookup). Registry
     * serving is for bench/test wiring without catalogued definitions.
     * {@code 0} = legacy/bench install, valid for
     * any revision.
     */
    private final int validFromRevision;

    public Handle(final String[] fieldNames, final List<byte[]> rowGroupPayloads) {
      this(fieldNames, rowGroupPayloads, null);
    }

    public Handle(final String[] fieldNames, final List<byte[]> rowGroupPayloads,
        final boolean[] numericNonIntegral) {
      this(null, 0, fieldNames, rowGroupPayloads, numericNonIntegral);
    }

    public Handle(final String rootPath, final int validFromRevision, final String[] fieldNames,
        final List<byte[]> rowGroupPayloads, final boolean[] numericNonIntegral) {
      this.rootPath = rootPath;
      this.validFromRevision = validFromRevision;
      this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames").clone();
      this.rowGroupPayloads = Objects.requireNonNull(rowGroupPayloads, "rowGroupPayloads");
      this.integralityEvidence = numericNonIntegral == null ? null : numericNonIntegral.clone();
      this.columnStore = null;
      this.defId = -1;
      this.projectedWeightBytes = 0L;
    }

    private Handle(final String rootPath, final int validFromRevision, final String[] fieldNames,
        final ProjectionColumnStore columnStore, final int defId,
        final long projectedWeightBytes) {
      this.rootPath = rootPath;
      this.validFromRevision = validFromRevision;
      this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames").clone();
      this.rowGroupPayloads = null;
      this.integralityEvidence = null;
      this.columnStore = Objects.requireNonNull(columnStore, "columnStore");
      this.defId = defId;
      this.projectedWeightBytes = projectedWeightBytes;
    }

    /**
     * Column-lazy handle (P5b stage 2): built from one descriptor walk; segment bytes are
     * fetched per COLUMN by the kernels/gates through the CALLER's own live fetcher, and whole
     * raw leaves only materialize when a whole-leaf consumer (group-by, string predicates,
     * canonical dicts) first asks, through the CALLER's own materializer. {@code defId} is the
     * immutable catalog definition id the caller uses to build that session-bound materializer.
     */
    public static Handle columnLazy(final String rootPath, final int validFromRevision,
        final String[] fieldNames, final ProjectionColumnStore columnStore, final int defId,
        final long projectedWeightBytes) {
      return new Handle(rootPath, validFromRevision, fieldNames, columnStore, defId,
          projectedWeightBytes);
    }

    /** Catalog definition id of a column-lazy handle ({@code -1} for eager/bench handles). */
    public int defId() {
      return defId;
    }

    /** Leaf count without materializing (descriptor truth for lazy; list size for eager). */
    public int rowGroupCount() {
      return columnStore != null ? columnStore.rowGroupCount()
          : (rowGroupPayloads == null ? 0 : rowGroupPayloads.size());
    }

    /** Non-null on a column-lazy handle. */
    public ProjectionColumnStore columnStoreOrNull() {
      return columnStore;
    }

    /**
     * Column kind byte WITHOUT materializing a lazy handle (descriptor truth); eager
     * handles read leaf 0's kind byte as before (empty stores default to NUMERIC_LONG,
     * preserving the historical extraction semantics).
     */
    public byte columnKindOf(final int col) {
      if (columnStore != null) {
        return columnStore.columnKind(col);
      }
      final List<byte[]> leaves = materializedLeaves();
      return leaves.isEmpty() ? ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
          : leaves.get(0)[24 + col];
    }

    /** Whether whole raw leaves have been materialized (test observability for laziness). */
    public boolean rawRowGroupsMaterialized() {
      return rowGroupPayloads != null;
    }

    /** Pessimistic cache weight of a column-lazy handle (0 for eager handles). */
    public long projectedWeightBytes() {
      return projectedWeightBytes;
    }

    /** Canonical record-set root path, or {@code null} for legacy installs. */
    public String rootPath() {
      return rootPath;
    }

    /** First revision the columns are valid for; {@code 0} = any. */
    public int validFromRevision() {
      return validFromRevision;
    }

    /**
     * Resolve the per-column integrality evidence. Installer-provided flags
     * win; otherwise probe the leaf payloads once (double-checked, same
     * pattern as {@link #columnSparseClean}) — presence tails carry the
     * flags; malformed payloads resolve to {@link #INTEGRALITY_UNKNOWN}.
     */
    private boolean[] integralityEvidence() {
      boolean[] evidence = integralityEvidence;
      if (evidence != null) {
        return evidence;
      }
      // Eager-handle path only (lazy handles resolve numeric integrality from column slices
      // via sliceEvidence): the leaves are already materialized, so this never does I/O.
      final List<byte[]> leaves = materializedLeaves();
      final boolean[] probed = ProjectionIndexByteScan.probeNumericNonIntegral(leaves);
      final boolean[] resolved = probed == null ? INTEGRALITY_UNKNOWN : probed;
      synchronized (this) {
        evidence = integralityEvidence;
        if (evidence == null) {
          integralityEvidence = resolved;
          evidence = resolved;
        }
      }
      return evidence;
    }

    /**
     * {@code true} iff the column is PROVABLY integral: integrality evidence
     * exists (builder-tracked flags or persisted tail flags) and never saw
     * a fractional value. Used to gate value-exact fast paths (aggregates);
     * unknown provenance returns {@code false}.
     */
    public boolean numericColumnIsIntegral(final int col,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (columnStore != null && columnStore.columnSliceable(col)) {
        // Column-lazy fast path: flag truth from the column's own BODY slices — same
        // evidentiary weight as the whole-leaf probe (segment truth, hash-verified at
        // slice decode), touching ONLY this column's segments through the caller's fetcher.
        return !columnFlagAny(col, ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL, fetcher);
      }
      final boolean[] evidence = integralityEvidence();
      return evidence != INTEGRALITY_UNKNOWN && col >= 0 && col < evidence.length
          && !evidence[col];
    }

    // Per-column slice-evidence bits, derived ONCE from the column's slices (the gates run
    // several times per query; a 97k-leaf store must not pay a slice walk per gate call).
    // 0 = unresolved slot; races on the lazily-filled array are benign (recompute is
    // idempotent, byte writes never tear).
    private static final byte EV_RESOLVED = (byte) 0x80;
    private static final byte EV_CORRUPT = 0x40;
    private static final byte EV_UNREP_ANY = 0x01;
    private static final byte EV_NONINT_ANY = 0x02;
    private static final byte EV_PURE_ALL = 0x04;

    private volatile byte[] sliceEvidence;

    private byte sliceEvidence(final int col, final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      final byte[] ev = sliceEvidence;
      if (ev != null && ev[col] != 0) {
        return ev[col];
      }
      // The column fill (the only I/O) runs OUTSIDE the monitor — a racing duplicate
      // derivation writes identical bits.
      byte bits = (byte) (EV_RESOLVED | EV_PURE_ALL);
      try {
        for (final ProjectionColumnStore.ColumnSlice slice : columnStore.column(col, fetcher)) {
          final byte f = slice.flags();
          if ((f & ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE) != 0) {
            bits |= EV_UNREP_ANY;
          }
          if ((f & ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL) != 0) {
            bits |= EV_NONINT_ANY;
          }
          if ((f & ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE) == 0) {
            bits &= ~EV_PURE_ALL;
          }
        }
      } catch (final IllegalStateException fillFailed) {
        if (!columnStore.columnKnownCorrupt(col)) {
          // Transient fetch failure: decline THIS call but do not cache — the next
          // query's re-bound fetcher can still produce real evidence.
          return (byte) (EV_RESOLVED | EV_CORRUPT);
        }
        bits = (byte) (EV_RESOLVED | EV_CORRUPT);
      }
      synchronized (this) {
        if (sliceEvidence == null) {
          sliceEvidence = new byte[columnStore.columnCount()];
        }
        if (sliceEvidence[col] == 0) {
          sliceEvidence[col] = bits;
        }
        return sliceEvidence[col];
      }
    }

    /**
     * {@code true} iff any slice of sliceable column {@code col} carries {@code bit}.
     * Corrupt/unavailable evidence reports {@code true} — callers use this direction only
     * to PROVE cleanliness ({@code !columnFlagAny}), so unavailable evidence declines
     * serving; positive-sighting consumers must not route through here (see
     * {@link #numericColumnKnownNonIntegral}).
     */
    private boolean columnFlagAny(final int col, final byte bit,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      final byte ev = sliceEvidence(col, fetcher);
      if ((ev & EV_CORRUPT) != 0) {
        return true; // fail closed — gate callers treat "poisoned" as decline
      }
      if (bit == ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE) {
        return (ev & EV_UNREP_ANY) != 0;
      }
      return (ev & EV_NONINT_ANY) != 0;
    }

    /** {@code true} iff EVERY slice of sliceable column {@code col} carries {@code bit}; corrupt → false. */
    private boolean columnFlagAll(final int col, final byte bit,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      final byte ev = sliceEvidence(col, fetcher);
      // Only the purity bit is queried through the ALL direction today.
      return (ev & EV_CORRUPT) == 0 && (ev & EV_PURE_ALL) != 0;
    }

    /**
     * Lazily-probed pure-double-source evidence (§11-8), the {@code AND}-across-leaves
     * counterpart of {@link #integralityEvidence} — see
     * {@link ProjectionIndexByteScan#probeDoublePureSource}. {@code null} = unresolved;
     * {@link #PURITY_UNKNOWN} = probed, malformed payloads — treat as impure.
     */
    private volatile boolean[] pureDoubleEvidence;

    private static final boolean[] PURITY_UNKNOWN = new boolean[0];

    /**
     * {@code true} iff column {@code col} is a NUMERIC_DOUBLE column whose EVERY leaf asserts
     * pure {@code Double}/{@code Float} sources — the gate that lifts double aggregate
     * serving from count-only to sum/avg/min/max (the fallback provably computes in double
     * space and surfaces {@code Dbl}, so digit-and-type parity holds). Unknown provenance
     * returns {@code false}.
     */
    public boolean doubleColumnPureSource(final int col,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (columnStore != null && columnStore.columnSliceable(col)) {
        return columnStore.columnKind(col) == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE
            && columnFlagAll(col, ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE, fetcher);
      }
      boolean[] evidence = pureDoubleEvidence;
      if (evidence == null) {
        // Eager-handle path only: the leaves are already materialized, so no I/O here.
        final List<byte[]> leaves = materializedLeaves();
        final boolean[] probed = ProjectionIndexByteScan.probeDoublePureSource(leaves);
        final boolean[] resolved = probed == null ? PURITY_UNKNOWN : probed;
        synchronized (this) {
          evidence = pureDoubleEvidence;
          if (evidence == null) {
            pureDoubleEvidence = resolved;
            evidence = resolved;
          }
        }
      }
      return evidence != PURITY_UNKNOWN && col >= 0 && col < evidence.length && evidence[col];
    }

    /** {@code true} iff a non-integral value was POSITIVELY seen in the column. */
    public boolean numericColumnKnownNonIntegral(final int col,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (columnStore != null && columnStore.columnSliceable(col)) {
        // "Known" requires an actual sighting: corrupt/unavailable evidence is UNKNOWN,
        // never a fabricated positive. Exactness still holds — serving that would read
        // the corrupt column fails its own fill and declines through the fail-soft flow.
        final byte ev = sliceEvidence(col, fetcher);
        return (ev & EV_CORRUPT) == 0 && (ev & EV_NONINT_ANY) != 0;
      }
      final boolean[] evidence = integralityEvidence();
      return evidence != INTEGRALITY_UNKNOWN && col >= 0 && col < evidence.length
          && evidence[col];
    }

    /**
     * {@code true} iff column {@code col} can serve SPARSE-CORRECT answers:
     * every leaf carries a valid presence tail and the column never saw a
     * present-but-unrepresentable value (JSON null, object/array, kind
     * mismatch). Anything else returns {@code false} and consumers must
     * fall back (typed scan kernels / generic pipeline). Probe runs once per
     * handle and is cached.
     */
    public boolean columnSparseClean(final int col,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher,
        final Supplier<List<byte[]>> materializer) {
      if (col < 0) return false;
      if (columnStore != null && columnStore.columnSliceable(col)) {
        // BODY segments always carry presence; slice decode validated structure and
        // hash, so the eager probe's invalid-tail arm cannot occur here — sparse-clean
        // reduces to the unrepresentable check (same fail-closed direction).
        return !columnFlagAny(col, ProjectionIndexRowGroupPage.COLUMN_FLAG_UNREPRESENTABLE, fetcher);
      }
      byte[] status = sparseStatus;
      if (status == null) {
        // Whole-leaf path (eager handle, or a lazy handle's non-sliceable string column):
        // materialize + probe outside the monitor through the caller's own materializer;
        // transient materialize failure declines WITHOUT caching.
        final List<byte[]> leaves;
        try {
          leaves = rowGroupPayloads(materializer);
        } catch (final IllegalStateException materializeFailed) {
          return false;
        }
        final byte[] probed = ProjectionIndexByteScan.probeSparseEvidence(leaves);
        synchronized (this) {
          status = sparseStatus;
          if (status == null) {
            sparseStatus = probed;
            status = probed;
          }
        }
      }
      return col < status.length && status[col] == ProjectionIndexByteScan.SPARSE_STATUS_CLEAN;
    }

    public String[] fieldNames() {
      return fieldNames.clone();
    }

    /**
     * Whole raw leaves — materializing a column-lazy handle on first call through the
     * CALLER's own live {@code materializer} (built from the caller's session). Guarded by
     * the dedicated {@link #materializeLock} (NOT {@code this}) so a multi-second hydrate
     * never blocks the gate caches; waiters genuinely need the result, so blocking them is
     * the cheapest correct choice (a CAS race would duplicate the full hydrate I/O). Once
     * materialized the immutable list is cached and shared — the {@code materializer} is not
     * consulted again, so eager (pre-materialized) handles accept a {@code null} one.
     *
     * @throws IllegalStateException when a lazy handle's materializer fails (dead-session
     *         window, truncated/corrupt store) — callers decline to the generic pipeline
     */
    public List<byte[]> rowGroupPayloads(final Supplier<List<byte[]>> materializer) {
      List<byte[]> leaves = rowGroupPayloads;
      if (leaves != null) {
        return leaves;
      }
      synchronized (materializeLock) {
        leaves = rowGroupPayloads;
        if (leaves == null) {
          leaves = Objects.requireNonNull(
              Objects.requireNonNull(materializer, "materializer").get(),
              "materializer returned null");
          rowGroupPayloads = leaves;
        }
        return leaves;
      }
    }

    /**
     * Whole raw leaves of an ALREADY-materialized handle (eager handles, or a lazy handle a
     * prior consumer already hydrated). Does NO I/O and never needs a session-bound source.
     *
     * @throws IllegalStateException if the handle is a not-yet-materialized lazy handle
     */
    private List<byte[]> materializedLeaves() {
      final List<byte[]> leaves = rowGroupPayloads;
      if (leaves == null) {
        throw new IllegalStateException("whole-leaf access on a non-materialized column-lazy handle");
      }
      return leaves;
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
     * STRING_DICT, or rowGroupPayloads is empty).
     *
     * <p>Result is cached per-column under a CAS so subsequent calls
     * are zero-cost. {@link #CANON_DICT_INELIGIBLE} caches "probe
     * established ineligible" so we don't re-probe.
     *
     * <p>HFT-grade: volatile read on fast path; one probe (bounded
     * to {@code probeLeaves} leaves) on first call per column.
     */
    public byte[][] canonicalDict(final int groupColumn,
        final int probeLeaves, final int cardLimit, final Supplier<List<byte[]>> materializer) {
      if (groupColumn < 0) return null;
      byte[][][] cache = canonicalDicts;
      if (cache != null && groupColumn < cache.length) {
        final byte[][] cached = cache[groupColumn];
        if (cached == CANON_DICT_INELIGIBLE) return null;
        if (cached != null) return cached;
      }
      // Compute outside the monitor — probe can be several ms; then
      // publish under the monitor to avoid lost wake-ups.
      final byte[][] probed;
      try {
        probed = ProjectionIndexByteScan.probeCanonicalDict(rowGroupPayloads(materializer), groupColumn,
            probeLeaves, cardLimit);
      } catch (final IllegalStateException materializeFailed) {
        // Transient lazy-handle materialize failure: decline dense group-by for THIS call
        // without caching ineligibility — the next query retries with re-bound sources.
        return null;
      }
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

  /** Exact (resource, sourcePath) entries — test/bench wiring. */
  private static final ConcurrentMap<String, Handle> REGISTRY = new ConcurrentHashMap<>();

  /**
   * Wildcard projections, pooled per resource and matched STRUCTURALLY by
   * (rootPath, ordered field list) — no string-encoded composite keys, so
   * field names may contain any character and identity always includes the
   * record-set root. CopyOnWriteArrayList: pools are tiny (a handful of
   * projections per resource), reads vastly outnumber writes.
   */
  private static final ConcurrentMap<String, CopyOnWriteArrayList<Handle>> WILDCARDS =
      new ConcurrentHashMap<>();

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
   * plus their {@code evaluateRowGroupMask} / {@code evalColumn*} callees) into
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
   * @param rowGroupPayloads one {@link ProjectionIndexRowGroupPage#serialize()}
   *                     byte[] per leaf, in leaf key order. Caller must
   *                     not mutate after publish.
   */
  public static void install(final String resourceKey, final String[] sourcePath,
      final String[] fieldNames, final List<byte[]> rowGroupPayloads) {
    final String k = key(resourceKey, sourcePath);
    final Handle handle = new Handle(fieldNames, rowGroupPayloads);
    REGISTRY.put(k, handle);
    prewarmIfFirst(k, handle);
  }

  /**
   * @return installed handle for {@code (resourceKey, sourcePath)}. Falls
   *         back to the FIRST wildcard entry (installed via
   *         {@link #installWildcard}) if no exact match exists — makes bench
   *         wiring simpler when the sourcePath shape the Brackit optimizer
   *         produces is not known ahead of time. With several wildcard
   *         projections installed the fallback picks the oldest install —
   *         callers that know their required fields should use
   *         {@link #lookupCovering} instead.
   */
  public static Handle lookup(final String resourceKey, final String[] sourcePath) {
    final Handle exact = REGISTRY.get(key(resourceKey, sourcePath));
    if (exact != null) return exact;
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null) return null;
    // COW iteration is a single snapshot — isEmpty()+get(0) would race a
    // concurrent uninstall between the two calls.
    for (final Handle handle : pool) {
      return handle;
    }
    return null;
  }

  /**
   * Covering lookup without revision gating — for callers with no revision
   * context (bench/test wiring). Production query paths should pass the
   * executor's revision via the four-argument overload.
   */
  public static Handle lookupCovering(final String resourceKey, final String[] sourcePath,
      final String[] requiredFields) {
    return lookupCovering(resourceKey, sourcePath, requiredFields, Integer.MAX_VALUE);
  }

  /**
   * Covering lookup: the exact {@code (resourceKey, sourcePath)} entry when
   * it carries every required field, else the wildcard projection with the
   * FEWEST columns that covers all of {@code requiredFields} (fewest first:
   * narrower projections scan less per row, and the choice stays
   * deterministic when several overlapping projections are installed).
   *
   * <p>Two safety gates, both fail-closed to the generic scan pipeline:
   * <ul>
   *   <li><b>Revision</b>: a handle is only served to executors at
   *       {@code revision >= validFromRevision} — a time-travel executor
   *       bound to an older revision must not read columns built later.</li>
   *   <li><b>Root ambiguity</b>: when covering candidates were built over
   *       DIFFERENT record-set roots, the registry cannot tell which record
   *       set the query iterates (wildcard entries ignore sourcePath), so it
   *       returns {@code null} instead of guessing.</li>
   * </ul>
   *
   * @return a covering handle, or {@code null} if none is installed/safe
   */
  public static Handle lookupCovering(final String resourceKey, final String[] sourcePath,
      final String[] requiredFields, final int revision) {
    final Handle exact = REGISTRY.get(key(resourceKey, sourcePath));
    if (exact != null && covers(exact, requiredFields) && exact.validFromRevision <= revision) {
      return exact;
    }
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null) return null;
    Handle best = null;
    for (final Handle candidate : pool) {
      if (candidate.validFromRevision > revision) continue;
      if (!covers(candidate, requiredFields)) continue;
      if (best != null && !Objects.equals(best.rootPath, candidate.rootPath)
          && best.rootPath != null && candidate.rootPath != null) {
        // Distinct roots both cover the requested fields — ambiguous.
        return null;
      }
      if (best == null || candidate.fieldNames.length < best.fieldNames.length) {
        best = candidate;
      }
    }
    return best;
  }

  /**
   * @return the wildcard handle whose field list equals {@code fieldNames}
   *         exactly (same names, same order), regardless of root — or
   *         {@code null}. Prefer {@link #lookupExact} when the root is known.
   */
  public static Handle lookupExactFields(final String resourceKey, final String[] fieldNames) {
    return lookupExact(resourceKey, null, fieldNames);
  }

  /**
   * @return the wildcard handle with exactly this (rootPath, ordered field
   *         list) identity — {@code null} rootPath matches any root
   */
  public static Handle lookupExact(final String resourceKey, final String rootPath,
      final String[] fieldNames) {
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null) return null;
    for (final Handle handle : pool) {
      if (Arrays.equals(handle.fieldNames, fieldNames)
          && (rootPath == null || Objects.equals(handle.rootPath, rootPath))) {
        return handle;
      }
    }
    return null;
  }

  /** {@code true} when ANY projection (wildcard or exact) is installed for the resource. */
  public static boolean hasProjections(final String resourceKey) {
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool != null && !pool.isEmpty()) return true;
    if (REGISTRY.isEmpty()) return false;
    final String prefix = Objects.requireNonNull(resourceKey, "resourceKey") + "\0";
    for (final String k : REGISTRY.keySet()) {
      if (k.startsWith(prefix)) return true;
    }
    return false;
  }

  /**
   * Install a projection index that matches <em>any</em> source path for the
   * given resource. Useful in benches where the caller doesn't know the
   * exact {@code sourcePath} shape Brackit will pass to
   * {@code executePredicateCount}.
   */
  public static void installWildcard(final String resourceKey,
      final String[] fieldNames, final List<byte[]> rowGroupPayloads) {
    installWildcard(resourceKey, null, 0, fieldNames, rowGroupPayloads, null);
  }

  /** Variant carrying builder-tracked NUMERIC_LONG integrality evidence. */
  public static void installWildcard(final String resourceKey,
      final String[] fieldNames, final List<byte[]> rowGroupPayloads,
      final boolean[] numericNonIntegral) {
    installWildcard(resourceKey, null, 0, fieldNames, rowGroupPayloads, numericNonIntegral);
  }

  /**
   * Full identity variant. A resource holds SEVERAL projections side by side
   * — analogous to the other index types — matched STRUCTURALLY by
   * (rootPath, ordered field list): re-installing the same identity replaces
   * that entry, a different identity adds one. Query-time selection happens
   * in {@link #lookupCovering}.
   *
   * @param rootPath          canonical record-set root the columns were
   *                          built over; {@code null} = legacy/any
   * @param validFromRevision first revision the columns are valid for;
   *                          {@code 0} = any
   */
  public static void installWildcard(final String resourceKey, final String rootPath,
      final int validFromRevision, final String[] fieldNames, final List<byte[]> rowGroupPayloads,
      final boolean[] numericNonIntegral) {
    final Handle handle =
        new Handle(rootPath, validFromRevision, fieldNames, rowGroupPayloads, numericNonIntegral);
    final List<Handle> pool =
        WILDCARDS.computeIfAbsent(Objects.requireNonNull(resourceKey, "resourceKey"),
            k -> new CopyOnWriteArrayList<>());
    boolean replaced = false;
    for (int i = 0; i < pool.size(); i++) {
      final Handle existing = pool.get(i);
      if (Arrays.equals(existing.fieldNames, handle.fieldNames)
          && Objects.equals(existing.rootPath, handle.rootPath)) {
        pool.set(i, handle);
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      pool.add(handle);
    }
    prewarmIfFirst(prewarmKey(resourceKey, rootPath, fieldNames), handle);
  }

  /** Remove wildcard projections with exactly this field list (any root). */
  public static void uninstallWildcard(final String resourceKey, final String[] fieldNames) {
    uninstallWildcard(resourceKey, null, fieldNames);
  }

  /**
   * Remove the wildcard projection with exactly this (rootPath, field list)
   * identity; {@code null} rootPath removes matching-fields entries of any
   * root.
   */
  public static void uninstallWildcard(final String resourceKey, final String rootPath,
      final String[] fieldNames) {
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null) return;
    pool.removeIf(handle -> Arrays.equals(handle.fieldNames, fieldNames)
        && (rootPath == null || Objects.equals(handle.rootPath, rootPath)));
  }

  private static String prewarmKey(final String resourceKey, final String rootPath,
      final String[] fieldNames) {
    return resourceKey + "\0*\0" + rootPath + "\0" + String.join("\0", fieldNames);
  }

  /** Remove any installed index for {@code (resourceKey, sourcePath)}. */
  public static void uninstall(final String resourceKey, final String[] sourcePath) {
    REGISTRY.remove(key(resourceKey, sourcePath));
  }

  /** Drop every entry (incl. the catalog decode cache) — for test isolation. */
  public static void clear() {
    REGISTRY.clear();
    WILDCARDS.clear();
    PREWARMED.clear();
    ProjectionIndexCatalog.clearCache();
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
    if (handle.rowGroupPayloads == null || handle.rowGroupPayloads.isEmpty()) return;
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
    final List<byte[]> payloads = handle.rowGroupPayloads;
    if (payloads == null || payloads.isEmpty()) return;
    final byte[] firstRowGroup = payloads.get(0);
    if (firstRowGroup == null) return;
    // Column count encoded at offset 4 little-endian; kinds start at offset 24.
    final int columnCount =
        (firstRowGroup[4] & 0xFF)
            | ((firstRowGroup[5] & 0xFF) << 8)
            | ((firstRowGroup[6] & 0xFF) << 16)
            | ((firstRowGroup[7] & 0xFF) << 24);
    if (columnCount <= 0 || columnCount > 256) return;
    int numericCol = -1;
    int booleanCol = -1;
    int stringDictCol = -1;
    for (int c = 0; c < columnCount; c++) {
      final byte kind = firstRowGroup[24 + c];
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG && numericCol < 0) numericCol = c;
      else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN && booleanCol < 0) booleanCol = c;
      else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT && stringDictCol < 0) stringDictCol = c;
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
      // Pre-warm runs on eager installed handles whose leaves are already materialized,
      // so no materializer is needed for the canonical-dict probe.
      final byte[][] canonical = PREWARM_DENSE_GROUPBY_ENABLED
          ? handle.canonicalDict(stringDictCol, 16, 256, null)
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
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool != null) {
      for (final Handle handle : pool) {
        if (handle.columnOf(field) >= 0) return true;
      }
    }
    if (REGISTRY.isEmpty()) return false;
    final String prefix = resourceKey + "\0";
    for (final var entry : REGISTRY.entrySet()) {
      if (!entry.getKey().startsWith(prefix)) continue;
      if (entry.getValue().columnOf(field) >= 0) return true;
    }
    return false;
  }

  // Package-private helper for diagnostic toString in tests.
  static int size() {
    int wildcardCount = 0;
    for (final List<Handle> pool : WILDCARDS.values()) {
      wildcardCount += pool.size();
    }
    return REGISTRY.size() + wildcardCount;
  }

  @Override
  public String toString() {
    return "ProjectionIndexRegistry{size=" + size() + "}";
  }

  /**
   * Static helper used mainly in tests to describe the installed set.
   */
  public static String describe() {
    final StringBuilder sb = new StringBuilder("ProjectionIndexRegistry[\n");
    REGISTRY.forEach((k, h) ->
        sb.append("  ").append(k).append(" -> fields=").append(Arrays.toString(h.fieldNames))
          .append(", leaves=")
          .append(h.rowGroupPayloads == null ? "lazy" : String.valueOf(h.rowGroupPayloads.size()))
          .append("\n"));
    WILDCARDS.forEach((resource, pool) -> {
      for (final Handle h : pool) {
        sb.append("  ").append(resource).append(" * root=").append(h.rootPath)
          .append(" validFrom=").append(h.validFromRevision)
          .append(" -> fields=").append(Arrays.toString(h.fieldNames))
          .append(", leaves=").append(h.rowGroupPayloads.size()).append("\n");
      }
    });
    return sb.append("]").toString();
  }
}
