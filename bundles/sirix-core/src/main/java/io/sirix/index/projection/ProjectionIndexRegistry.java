/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import io.sirix.api.StorageEngineReader;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Process-wide lookup from (resource, sourcePath, fields) to a pre-built list of serialised
 * {@link ProjectionIndexRowGroupPage} byte[]s.
 *
 * <p>
 * BENCH/TEST wiring: an in-memory pool that lets the query-path executor
 * ({@link io.sirix.query.scan.SirixVectorizedExecutor}) serve projections for stores WITHOUT
 * catalogued definitions (legacy bench layouts, in-memory-only bench runs, tests). Production
 * discovery goes through {@link ProjectionIndexCatalog} — the revision-scoped catalog + page layer,
 * which inherits transactionality and invalidation (stale tombstones written by
 * {@link ProjectionIndexChangeListener}) from the page layer's copy-on-write. Entries here are
 * identified structurally by (root path, ordered field list); NOTE that pool entries installed via
 * the legacy overloads (root {@code null}, valid-from {@code 0}) are NOT invalidated by updates —
 * callers own their lifecycle. Incremental leaf maintenance remains future work (task #57).
 *
 * <p>
 * The key includes resource identifier + JSON source path + ordered field list. Scans consult the
 * registry by field-<em>set</em>: if the installed index's fields are a superset of the query's
 * predicate fields and their column order in the index is known, predicate-to-column mapping is
 * trivial. Keys are deliberately simple strings so a bench or test can install an index with one
 * call and the executor can find it on the hot path with one hash lookup.
 *
 * <h2>Thread-safety</h2> Backed by a {@link ConcurrentHashMap}; {@link #install} publishes the
 * handle via a {@code put}-with-happens-before, reads are plain {@code get}s. The installed
 * handle's {@code rowGroupPayloads} list must not be mutated after install — callers should hand in
 * an immutable snapshot.
 */
public final class ProjectionIndexRegistry {

  /**
   * Byte bound of the per-handle string-length table memo
   * ({@code sirix.projection.stringLength.memoBytes}, default 512 MB; {@code 0} disables retention).
   * An int per dictionary id: 72 MB for an 18M-id URL dictionary, so the default holds every global
   * column of the 100M ClickBench resource.
   */
  private static volatile long stringLengthMemoBytes =
      Math.max(0L, Long.getLong("sirix.projection.stringLength.memoBytes", 512L << 20));

  /**
   * Test seam: bound the string-length memo so a small table already exceeds it.
   *
   * @param value the bound in bytes
   * @return the previous bound, for restoring in a finally block
   */
  static long setStringLengthMemoBytesForTesting(final long value) {
    final long previous = stringLengthMemoBytes;
    stringLengthMemoBytes = Math.max(0L, value);
    return previous;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionIndexRegistry.class);

  /**
   * Executor-owned advisory work that can be abandoned before it starts.
   *
   * <p>
   * {@link java.util.concurrent.ExecutorService#shutdownNow()} returns commands that never started.
   * The executor owner calls {@link #cancelBeforeExecution()} for those commands so a handle's
   * one-shot latch is not stranded by cache eviction and a later live executor can issue the hint. A
   * command already running is fenced by executor termination instead.
   * </p>
   */
  public interface CancellableBackgroundTask extends Runnable {
    void cancelBeforeExecution();
  }

  /**
   * Immutable handle published into the registry. Column order in {@link #fieldNames} defines the
   * column order that {@link ProjectionIndexByteScan#conjunctiveCount} expects: the query path
   * converts each predicate leaf to a {@link ProjectionIndexScan.ColumnPredicate} with
   * {@code column = Arrays.asList(fieldNames).indexOf(predFieldName)}.
   */
  public static final class Handle {
    private final String[] fieldNames;
    /**
     * Per column, its declared path RELATIVE to the record root ({@code "commit/collection"} for
     * {@code /[]/commit/collection} under {@code /[]}), or {@code null} for handles installed without
     * declared paths (the bench/test registry route). Attached at construction by the catalog; see
     * {@link #columnOf(String)} for what it changes.
     */
    private volatile String[] fieldChains;
    /** Eagerly-hydrated raw leaves, or the lazily-materialized cache of a column-lazy handle. */
    private volatile List<byte[]> rowGroupPayloads;

    /**
     * The bounded WINDOW CACHE over the same leaves, for a handle whose whole-leaf materialization is
     * over the eager budget. Separate from {@link #rowGroupPayloads} because it is NOT resident: every
     * route predicate that asks "are the leaves in memory" must answer no for it. It holds no
     * session-derived state, which is what makes it safe on a process-wide handle.
     */
    private volatile ProjectionWindowedRowGroupPayloads windowedPayloads;
    /**
     * Column-sliced view (P5b stage 2) — non-null marks a COLUMN-LAZY handle: constructed from
     * descriptors only; {@link #rowGroupPayloads} materializes whole raw leaves on first whole-leaf
     * consumer, while column-scoped kernels and gates read {@link #columnStore} slices and never
     * trigger that materialization.
     */
    private final ProjectionColumnStore columnStore;
    /**
     * Catalog index-definition id of a column-lazy handle (immutable identity, {@code -1} for
     * eager/bench handles). NOT session-scoped: the caller uses it to build a whole-leaf materializer
     * bound to ITS OWN live session and threads that into {@link #rowGroupPayloads(Supplier)} — the
     * handle stores nothing session-lifecycle-scoped.
     */
    private final int defId;
    /** Guards materialization only — never the gate caches, which use {@code this}. */
    private final Object materializeLock = new Object();
    /**
     * Worst-case RESIDENT weight of a column-lazy handle for cache accounting (Caffeine weights are
     * fixed at insert): raw materialized leaves (Σ descriptor byteLens) PLUS the decoded column-slice
     * arrays a fully-touched handle retains.
     */
    private final long projectedWeightBytes;

    /**
     * Index-wide per-value ROW counts from the projection's metadata blob, or {@code null} when the
     * index carries none. Held on the handle because that blob is already read to build the handle — a
     * membership count then needs no further read at all.
     */
    private Map<Integer, Map<String, Long>> setValueRowCounts;

    /**
     * Rows in the index whose set at {@code column} contains {@code value}; {@code 0} for a value the
     * index does not hold, {@code null} when this column carries no summary.
     */
    public Long setValueRowCount(final int column, final String value) {
      final var counts = setValueRowCounts;
      if (counts == null) {
        return null;
      }
      final var forColumn = counts.get(column);
      if (forColumn == null) {
        return null;
      }
      final Long n = forColumn.get(value);
      return n == null
          ? Long.valueOf(0)
          : n;
    }

    /** Attach the metadata's summary; called once, at construction time, by the catalog. */
    public void setSetValueRowCounts(final Map<Integer, Map<String, Long>> counts) {
      this.setValueRowCounts = counts;
    }

    /**
     * Per {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} column, the node key of its
     * resource-wide value dictionary's header; {@code 0} for every other column. Read from the same
     * metadata blob the handle is built from, so carrying it costs nothing.
     */
    private long[] valueDictionaryHeaderKeys;

    /** Attach the per-column dictionary anchors; called once, at construction, by the catalog. */
    public void setValueDictionaryHeaderKeys(final long @Nullable [] keys) {
      this.valueDictionaryHeaderKeys = keys;
    }

    /**
     * The value dictionary anchor for {@code col}, or {@code 0} when the column has none — which is
     * every column of a store built before the kind existed, and every column that is not global.
     */
    public long valueDictionaryHeaderKey(final int col) {
      final long[] keys = valueDictionaryHeaderKeys;
      return keys == null || col < 0 || col >= keys.length
          ? 0L
          : keys[col];
    }

    /**
     * Every column's dictionary anchor, indexed by column, {@code 0} where the column is not global.
     *
     * <p>
     * A COPY, because the array is the handle's own and a caller enumerating anchors must not be able
     * to reach into it. For callers that need to act on the whole set — warming the dictionaries a
     * resource has, rather than resolving one column's.
     * </p>
     */
    public long[] valueDictionaryAnchors() {
      final long[] keys = valueDictionaryHeaderKeys;
      return keys == null
          ? new long[0]
          : keys.clone();
    }

    /**
     * Resolve a string literal to its id in {@code col}'s resource-wide dictionary.
     *
     * <p>
     * The whole reason a global column can serve a predicate at all: this runs ONCE per literal, and
     * every row afterwards is an integer compare. The three answers are deliberately distinct —
     * {@link GlobalValueDictionary#ID_ABSENT} is an exact "no row can match" and
     * {@link GlobalValueDictionary#ID_UNKNOWN} is "I cannot say", which must decline rather than be
     * read as absence.
     *
     * @param col the column
     * @param literalUtf8 the literal's UTF-8 bytes
     * @param reader a reader positioned at this handle's revision
     * @return the id, {@code ID_ABSENT}, or {@code ID_UNKNOWN}
     */
    public int globalDictionaryId(final int col, final byte[] literalUtf8, final StorageEngineReader reader) {
      final long headerKey = valueDictionaryHeaderKey(col);
      if (headerKey <= 0L || literalUtf8 == null || reader == null) {
        return GlobalValueDictionary.ID_UNKNOWN;
      }
      try {
        return GlobalValueDictionary.probe(headerKey, literalUtf8, reader);
      } catch (final RuntimeException unreadable) {
        // A dictionary this revision cannot read is "I cannot say", never "not there".
        return GlobalValueDictionary.ID_UNKNOWN;
      }
    }

    /**
     * iter#10 dense group-by: per-column canonical dictionary cache. Sentinel value
     * {@link #CANON_DICT_INELIGIBLE} flags "probe determined {@code groupColumn} is not eligible for
     * the dense path" so we don't re-probe on subsequent lookups. Array indexed by {@code groupColumn};
     * {@code null} slot = not yet computed.
     */
    private volatile byte[][][] canonicalDicts;

    /**
     * Sentinel for "integrality was probed and is UNKNOWN" — some leaf lacks a valid presence tail, so
     * value-exact consumers must fail closed.
     */
    private static final boolean[] INTEGRALITY_UNKNOWN = new boolean[0];

    /**
     * Per-column integrality evidence for NUMERIC_LONG columns: {@code true} means a non-integral
     * (truncated) value was SEEN in that column. Populated eagerly from builder-tracked flags when the
     * installer passes them, otherwise lazily re-derived from the leaf payloads' presence tails via
     * {@link ProjectionIndexByteScan#probeNumericNonIntegral} — so the evidence survives persistence
     * and close/re-open just like the sparse evidence. {@code null} = not yet resolved;
     * {@link #INTEGRALITY_UNKNOWN} = probed, provenance unavailable (malformed leaves) — value-exact
     * consumers must treat that as not-provably-integral.
     */
    private volatile boolean[] integralityEvidence;

    /**
     * Lazily-probed per-column sparse evidence — values are
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_CLEAN} /
     * {@link ProjectionIndexByteScan#SPARSE_STATUS_DIRTY}. Computed once for ALL columns by
     * {@link ProjectionIndexByteScan#probeSparseEvidence} on first use; the evidence lives INSIDE the
     * leaf payloads (presence tail + per-column unrepresentable flags) so it survives persistence and
     * re-encoding — the integrality evidence follows the same pattern via flag bit1 of the same tail
     * (see {@link #integralityEvidence}).
     */
    private volatile byte[] sparseStatus;

    /**
     * Canonical root path of the record set the columns were built over, or {@code null} for
     * legacy/bench installs that predate root tracking (matches any root). Part of the handle's
     * identity: two projections with identical trailing field names but different roots are DIFFERENT
     * indexes and must never overwrite or answer for each other.
     */
    private final String rootPath;

    /**
     * First revision this handle's columns are valid for. An executor bound to an OLDER revision must
     * not use the handle (time travel would read future data). The gate says nothing about LATER
     * revisions — the handle is a point-in-time snapshot that update commits do not refresh or
     * uninstall, so for catalogued projections the revision-scoped {@link ProjectionIndexCatalog} is
     * authoritative and query paths must not fall back to the registry (see the executor's lookup).
     * Registry serving is for bench/test wiring without catalogued definitions. {@code 0} =
     * legacy/bench install, valid for any revision.
     */
    private final int validFromRevision;

    public Handle(final String[] fieldNames, final List<byte[]> rowGroupPayloads) {
      this(fieldNames, rowGroupPayloads, null);
    }

    public Handle(final String[] fieldNames, final List<byte[]> rowGroupPayloads, final boolean[] numericNonIntegral) {
      this(null, 0, fieldNames, rowGroupPayloads, numericNonIntegral);
    }

    public Handle(final String rootPath, final int validFromRevision, final String[] fieldNames,
        final List<byte[]> rowGroupPayloads, final boolean[] numericNonIntegral) {
      this.rootPath = rootPath;
      this.validFromRevision = validFromRevision;
      this.fieldNames = Objects.requireNonNull(fieldNames, "fieldNames").clone();
      this.rowGroupPayloads = Objects.requireNonNull(rowGroupPayloads, "rowGroupPayloads");
      this.integralityEvidence = numericNonIntegral == null
          ? null
          : numericNonIntegral.clone();
      this.columnStore = null;
      this.defId = -1;
      this.projectedWeightBytes = 0L;
    }

    private Handle(final String rootPath, final int validFromRevision, final String[] fieldNames,
        final ProjectionColumnStore columnStore, final int defId, final long projectedWeightBytes) {
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
     * Column-lazy handle (P5b stage 2): built from one descriptor walk; segment bytes are fetched per
     * COLUMN by the kernels/gates through the CALLER's own live fetcher, and whole raw leaves only
     * materialize when a whole-leaf consumer (group-by, string predicates, canonical dicts) first asks,
     * through the CALLER's own materializer. {@code defId} is the immutable catalog definition id the
     * caller uses to build that session-bound materializer.
     */
    public static Handle columnLazy(final String rootPath, final int validFromRevision, final String[] fieldNames,
        final ProjectionColumnStore columnStore, final int defId, final long projectedWeightBytes) {
      return new Handle(rootPath, validFromRevision, fieldNames, columnStore, defId, projectedWeightBytes);
    }

    /** Catalog definition id of a column-lazy handle ({@code -1} for eager/bench handles). */
    public int defId() {
      return defId;
    }

    /** Leaf count without materializing (descriptor truth for lazy; list size for eager). */
    public int rowGroupCount() {
      return columnStore != null
          ? columnStore.rowGroupCount()
          : (rowGroupPayloads == null
              ? 0
              : rowGroupPayloads.size());
    }

    /** Non-null on a column-lazy handle. */
    public ProjectionColumnStore columnStoreOrNull() {
      return columnStore;
    }

    /**
     * Column kind byte WITHOUT materializing a lazy handle (descriptor truth); eager handles read leaf
     * 0's kind byte as before (empty stores default to NUMERIC_LONG, preserving the historical
     * extraction semantics).
     */
    public byte columnKindOf(final int col) {
      if (columnStore != null) {
        return columnStore.columnKind(col);
      }
      final List<byte[]> leaves = materializedLeaves();
      return leaves.isEmpty()
          ? ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
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
     * Resolve the per-column integrality evidence. Installer-provided flags win; otherwise probe the
     * leaf payloads once (double-checked, same pattern as {@link #columnSparseClean}) — presence tails
     * carry the flags; malformed payloads resolve to {@link #INTEGRALITY_UNKNOWN}.
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
      final boolean[] resolved = probed == null
          ? INTEGRALITY_UNKNOWN
          : probed;
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
     * {@code true} iff the column is PROVABLY integral: integrality evidence exists (builder-tracked
     * flags or persisted tail flags) and never saw a fractional value. Used to gate value-exact fast
     * paths (aggregates); unknown provenance returns {@code false}.
     */
    public boolean numericColumnIsIntegral(final int col, final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      // Numeric kinds only: sliceability now also covers string columns, whose flags never set
      // NON_INTEGRAL — reading them here would answer "provably integral" about a non-number.
      if (columnStore != null && columnStore.columnSliceable(col)
          && ProjectionIndexRowGroupPage.isNumericKind(columnStore.columnKind(col))) {
        // Column-lazy fast path: flag truth from the column's own BODY slices — same
        // evidentiary weight as the whole-leaf probe (segment truth, hash-verified at
        // slice decode), touching ONLY this column's segments through the caller's fetcher.
        return !columnFlagAny(col, ProjectionIndexRowGroupPage.COLUMN_FLAG_NON_INTEGRAL, fetcher);
      }
      final boolean[] evidence = integralityEvidence();
      return evidence != INTEGRALITY_UNKNOWN && col >= 0 && col < evidence.length && !evidence[col];
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
        // From the DESCRIPTORS, which this store already holds, not by materialising the column.
        // The flags byte a slice reports is the same byte the encoder wrote into the descriptor
        // entry, so the evidence is identical — but decoding every slice to read one byte per leaf
        // fetched the column's whole BODY and DICT chain: 110 MB on the movies corpus, paid by
        // EVERY predicate on a sliceable column, including ones answered from metadata alone.
        final boolean fromDescriptors = !"false".equals(System.getProperty("sirix.projection.descriptorEvidence"));
        final int leaves = fromDescriptors
            ? columnStore.rowGroupCount()
            : columnStore.column(col, fetcher).length;
        for (int leaf = 0; leaf < leaves; leaf++) {
          final byte f = fromDescriptors
              ? columnStore.columnFlags(leaf, col)
              : columnStore.column(col, fetcher)[leaf].flags();
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
     * Corrupt/unavailable evidence reports {@code true} — callers use this direction only to PROVE
     * cleanliness ({@code !columnFlagAny}), so unavailable evidence declines serving; positive-sighting
     * consumers must not route through here (see {@link #numericColumnKnownNonIntegral}).
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

    /**
     * {@code true} iff EVERY slice of sliceable column {@code col} carries {@code bit}; corrupt →
     * false.
     */
    private boolean columnFlagAll(final int col, final byte bit,
        final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      final byte ev = sliceEvidence(col, fetcher);
      // Only the purity bit is queried through the ALL direction today.
      return (ev & EV_CORRUPT) == 0 && (ev & EV_PURE_ALL) != 0;
    }

    /**
     * Lazily-probed pure-double-source evidence (§11-8), the {@code AND}-across-leaves counterpart of
     * {@link #integralityEvidence} — see {@link ProjectionIndexByteScan#probeDoublePureSource}.
     * {@code null} = unresolved; {@link #PURITY_UNKNOWN} = probed, malformed payloads — treat as
     * impure.
     */
    private volatile boolean[] pureDoubleEvidence;

    private static final boolean[] PURITY_UNKNOWN = new boolean[0];

    /**
     * {@code true} iff column {@code col} is a NUMERIC_DOUBLE column whose EVERY leaf asserts pure
     * {@code Double}/{@code Float} sources — the gate that lifts double aggregate serving from
     * count-only to sum/avg/min/max (the fallback provably computes in double space and surfaces
     * {@code Dbl}, so digit-and-type parity holds). Unknown provenance returns {@code false}.
     */
    public boolean doubleColumnPureSource(final int col, final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
      if (columnStore != null && columnStore.columnSliceable(col)) {
        return columnStore.columnKind(col) == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE
            && columnFlagAll(col, ProjectionIndexRowGroupPage.COLUMN_FLAG_PURE_DOUBLE_SOURCE, fetcher);
      }
      boolean[] evidence = pureDoubleEvidence;
      if (evidence == null) {
        // Eager-handle path only: the leaves are already materialized, so no I/O here.
        final List<byte[]> leaves = materializedLeaves();
        final boolean[] probed = ProjectionIndexByteScan.probeDoublePureSource(leaves);
        final boolean[] resolved = probed == null
            ? PURITY_UNKNOWN
            : probed;
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
      // Numeric kinds only — see numericColumnIsIntegral.
      if (columnStore != null && columnStore.columnSliceable(col)
          && ProjectionIndexRowGroupPage.isNumericKind(columnStore.columnKind(col))) {
        // "Known" requires an actual sighting: corrupt/unavailable evidence is UNKNOWN,
        // never a fabricated positive. Exactness still holds — serving that would read
        // the corrupt column fails its own fill and declines through the fail-soft flow.
        final byte ev = sliceEvidence(col, fetcher);
        return (ev & EV_CORRUPT) == 0 && (ev & EV_NONINT_ANY) != 0;
      }
      final boolean[] evidence = integralityEvidence();
      return evidence != INTEGRALITY_UNKNOWN && col >= 0 && col < evidence.length && evidence[col];
    }

    /**
     * {@code true} iff column {@code col} can serve SPARSE-CORRECT answers: every leaf carries a valid
     * presence tail and the column never saw a present-but-unrepresentable value (JSON null,
     * object/array, kind mismatch). Anything else returns {@code false} and consumers must fall back
     * (typed scan kernels / generic pipeline). Probe runs once per handle and is cached.
     */
    public boolean columnSparseClean(final int col, final ProjectionColumnStore.ColumnSegmentFetcher fetcher,
        final Supplier<List<byte[]>> materializer) {
      if (col < 0)
        return false;
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

    /**
     * Sentinel for "the numeric range was resolved and is UNKNOWN" — see {@link #numericGroupRange}.
     */
    private static final long[] RANGE_UNKNOWN = new long[0];

    /**
     * Per-column memo of {@link #numericGroupRange}; {@code null} slot = not yet resolved.
     */
    private volatile long[][] numericRanges;

    /**
     * Index-wide zone-map union of NUMERIC_LONG column {@code col}: {@code {min, max, totalRows}}, or
     * {@code null} when the range is UNKNOWN and the caller must not size an index-by-subtraction
     * accumulator from it.
     *
     * <p>
     * Metadata only on BOTH arms — a column-lazy handle reads the leaf DESCRIPTORS it already holds
     * ({@link ProjectionColumnStore#columnZoneRange}, zero I/O and no segment fetch, hence no
     * {@code ColumnSegmentFetcher} parameter), an eager handle walks leaf HEADERS
     * ({@link ProjectionIndexByteScan#numericZoneUnion}). Neither touches a row.
     *
     * <p>
     * Resolved values are memoized under the same double-checked publish the other gate caches use. A
     * TRANSIENT materialize failure declines WITHOUT caching, so the next query retries with re-bound
     * sources.
     */
    public long[] numericGroupRange(final int col, final Supplier<List<byte[]>> materializer) {
      if (col < 0) {
        return null;
      }
      long[][] cache = numericRanges;
      if (cache != null && col < cache.length && cache[col] != null) {
        return cache[col] == RANGE_UNKNOWN
            ? null
            : cache[col];
      }
      final long[] probe = new long[3];
      final boolean known;
      if (columnStore != null) {
        known = descriptorZoneUnion(col, probe);
      } else {
        final List<byte[]> leaves;
        try {
          leaves = rowGroupPayloads(materializer);
        } catch (final IllegalStateException materializeFailed) {
          return null;
        }
        known = ProjectionIndexByteScan.numericZoneUnion(leaves, col, probe);
      }
      final long[] resolved = known
          ? probe
          : RANGE_UNKNOWN;
      synchronized (this) {
        cache = numericRanges;
        if (cache == null || cache.length <= col) {
          final long[][] grown = new long[Math.max(col + 1, fieldNames.length)][];
          if (cache != null) {
            System.arraycopy(cache, 0, grown, 0, cache.length);
          }
          cache = grown;
          numericRanges = cache;
        }
        if (cache[col] == null) {
          cache[col] = resolved;
        }
        return cache[col] == RANGE_UNKNOWN
            ? null
            : cache[col];
      }
    }

    /**
     * Column-lazy arm of {@link #numericGroupRange}: fold the per-leaf descriptor zone pairs. Mirrors
     * {@link ProjectionIndexByteScan#numericZoneUnion}'s rules exactly — {@code min > max} leaves
     * contribute rows but no range; a leaf whose entry is ABSENT makes the whole union unknown.
     */
    private boolean descriptorZoneUnion(final int col, final long[] out3) {
      if (col >= columnStore.columnCount()
          || columnStore.columnKind(col) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        return false;
      }
      final int leaves = columnStore.rowGroupCount();
      final long[] pair = new long[2];
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      long rows = 0;
      boolean anyRange = false;
      for (int leaf = 0; leaf < leaves; leaf++) {
        if (!columnStore.columnZoneRange(leaf, col, pair)) {
          return false;
        }
        rows += columnStore.rowCount(leaf);
        if (pair[0] > pair[1]) {
          continue;
        }
        if (pair[0] < min) {
          min = pair[0];
        }
        if (pair[1] > max) {
          max = pair[1];
        }
        anyRange = true;
      }
      if (!anyRange) {
        return false;
      }
      out3[0] = min;
      out3[1] = max;
      out3[2] = rows;
      return true;
    }

    public String[] fieldNames() {
      return fieldNames.clone();
    }

    /**
     * Whole raw leaves — materializing a column-lazy handle on first call through the CALLER's own live
     * {@code materializer} (built from the caller's session). Guarded by the dedicated
     * {@link #materializeLock} (NOT {@code this}) so a multi-second hydrate never blocks the gate
     * caches; waiters genuinely need the result, so blocking them is the cheapest correct choice (a CAS
     * race would duplicate the full hydrate I/O). Once materialized the immutable list is cached and
     * shared — the {@code materializer} is not consulted again, so eager (pre-materialized) handles
     * accept a {@code null} one.
     *
     * <p>
     * The two routes memoize into DIFFERENT fields, because only one of them is resident. Eager leaves
     * are inert {@code byte[]}s and land in {@link #rowGroupPayloads}. A
     * {@link ProjectionWindowedRowGroupPayloads} view holds only a bounded window set, so it lands in
     * {@link #windowedPayloads} and {@link #payloadsMaterialized()} keeps meaning "resident". Both are
     * memoized HERE, on the handle, whose (resource, def, build revision) cache key is exactly the
     * identity of the bytes they hold — no shorter-lived owner can memoize a view without multiplying
     * the residency its window cap bounds.
     * </p>
     *
     * <p>
     * What the handle memoizes on the windowed route is the WINDOW CACHE, never a list bound to a
     * session: the cache holds no session-derived field, and each consult wraps it in a thin per-caller
     * view carrying THAT caller's reader source. A handle that has gone windowed keeps serving through
     * that cache for as long as callers hand it windowed materializers; it is promoted to resident
     * leaves only when a caller's materializer actually produces them, never by the handle deciding on
     * its own to pay for a whole-leaf materialization.
     * </p>
     *
     * @throws IllegalStateException when a lazy handle's materializer fails (dead-session window,
     *         truncated/corrupt store) — callers decline to the generic pipeline
     */
    public List<byte[]> rowGroupPayloads(final Supplier<List<byte[]>> materializer) {
      final List<byte[]> resident = rowGroupPayloads;
      if (resident != null) {
        return resident;
      }
      final ProjectionWindowedRowGroupPayloads windowed = windowedPayloads;
      if (windowed != null) {
        return boundView(windowed, materializer);
      }
      synchronized (materializeLock) {
        final List<byte[]> raced = rowGroupPayloads;
        if (raced != null) {
          return raced;
        }
        final ProjectionWindowedRowGroupPayloads racedWindow = windowedPayloads;
        if (racedWindow != null) {
          return boundView(racedWindow, materializer);
        }
        final List<byte[]> built = Objects.requireNonNull(Objects.requireNonNull(materializer, "materializer").get(),
            "materializer returned null");
        final ProjectionWindowedRowGroupPayloads cache = ProjectionWindowedRowGroupPayloads.cacheOf(built);
        if (cache != null) {
          windowedPayloads = cache;
        } else {
          rowGroupPayloads = built;
        }
        return built;
      }
    }

    /**
     * Wrap the memoized cache for THIS caller. The caller's source comes from its materializer, and a
     * windowed materializer carries one, so the common path allocates one thin view and reads no bytes.
     * A materializer that carries none is consulted: if it still built a windowed view, its cache is
     * discarded in favour of the memoized one and only its source is kept; if it built RESIDENT leaves
     * instead — a raised budget, or an eager handle — the handle is PROMOTED to them, because the
     * materialization is already paid for and resident leaves beat windows.
     */
    private List<byte[]> boundView(final ProjectionWindowedRowGroupPayloads cache,
        final Supplier<List<byte[]>> materializer) {
      if (materializer instanceof ProjectionWindowedRowGroupPayloads.BoundMaterializer bound) {
        return cache.boundTo(bound.readerSource());
      }
      final List<byte[]> built = Objects.requireNonNull(materializer, "materializer").get();
      final ProjectionWindowedRowGroupPayloads.ReaderSource source = ProjectionWindowedRowGroupPayloads.sourceOf(built);
      if (source != null) {
        return cache.boundTo(source);
      }
      rowGroupPayloads = built;
      return built;
    }

    /**
     * Whether whole-leaf payloads are ALREADY in memory (eager handle, or a prior consumer hydrated the
     * lazy handle). Regime probe for slice-vs-payload route choices: once the leaves are cached, a
     * contiguous byte-kernel scan beats scattered slice reads — the sliced routes exist to avoid the
     * materialization, not to replace the warm scan. Racy by design (a stale {@code null} just serves
     * one more query from slices).
     *
     * <p>
     * A windowed view never sets this: its leaves are fetched per window and evicted under a cap, so
     * reporting it as materialized would steer every later query off a viable sliced fill and into a
     * byte-kernel scan that re-reads windows from disk.
     * </p>
     */
    public boolean payloadsMaterialized() {
      return rowGroupPayloads != null;
    }

    /**
     * Sliced group-route ARRIVALS so far — the PROMOTION signal: a handle whose group arms keep
     * reaching the sliced route is in a hot loop, where the contiguous byte-kernel scan over
     * materialized leaves wins (~2x on 1M-row string groupings). Racy increments are benign (a
     * promotion one arrival late).
     */
    private final AtomicInteger slicedRouteArrivals = new AtomicInteger();

    /**
     * Count one ARRIVAL at the sliced group route; returns the count BEFORE the increment.
     *
     * <p>
     * This is the promotion POLICY signal and it deliberately counts arrivals, not successful sliced
     * serves. The policy asks "is this handle in a hot group-query loop", which route traffic answers;
     * an arm that then declined for an unsliceable predicate does not make the handle colder, and
     * gating the tick on a successful slice would move the promotion trigger point.
     * </p>
     *
     * <p>
     * The instrument that must not lie about REAL serves is a separate, per-kernel one:
     * {@code SirixVectorizedExecutor.groupAggSlicedServedCount()}, incremented inside the sliced
     * kernels themselves. That is what the regression tests assert on and what the benchmark runners
     * report — keeping policy and instrument apart is what lets either change without silently moving
     * the other.
     * </p>
     */
    public int slicedRouteTick() {
      return slicedRouteArrivals.getAndIncrement();
    }

    /**
     * Observed group cardinalities by GROUP-SHAPE FINGERPRINT — the pass-seeding memo. A group scan
     * that ABORTS (groups past the per-pass budget) has paid most of a full scan before it learns the
     * count; recording the abort-time estimate here lets the NEXT execution of the same shape start at
     * the right pass count and never pay the aborted scan (q18 at 100M: 18.1M groups vs a 12.58M budget
     * = one full wasted scan per TRY, hot included). Purely a performance seed: a stale or colliding
     * entry only mis-picks the pass count, which the abort-and-restart machinery corrects — it can
     * never change an answer. The handle's lifetime bounds staleness (a new revision is a new handle),
     * and entries only grow (max-keep), so a transient under-estimate cannot pin a lower count.
     */
    private final Long2LongOpenHashMap observedGroupCounts = new Long2LongOpenHashMap();

    /** The memoed group count for this shape, or 0 when never observed. */
    public long observedGroupsFor(final long fingerprint) {
      synchronized (observedGroupCounts) {
        return observedGroupCounts.get(fingerprint);
      }
    }

    /** Record an observed (or abort-estimated) group count; keeps the maximum ever seen. */
    public void noteObservedGroups(final long fingerprint, final long groups) {
      synchronized (observedGroupCounts) {
        if (groups > observedGroupCounts.get(fingerprint)) {
          observedGroupCounts.put(fingerprint, groups);
        }
      }
    }

    /**
     * The completed hash-range pass set of each group shape, by shape fingerprint: the exact group
     * count the merged partitions summed to (or, when a pass count seeded from the memo aborted anyway,
     * the count the completing pass count implies at its budget) and the pass count that completed.
     * Kept apart from {@link #observedGroupCounts} because the two disagree in kind: the abort-time
     * figure extrapolates the groups seen so far over the unscanned leaves, and a distinct-arrival rate
     * that decays (the heavy hitters are met early) makes it OVERSHOOT — q16 at 100M memoed past twice
     * the budget from an abort and then ran four seeded passes on every hot try where two held, so its
     * hot tries were slower than the cold one that had aborted. The pass count travels with the count
     * because a pass is judged by the groups it FLUSHED, not by the groups it held: a pass set that
     * completed held more per pass than the budget says a pass holds, and the count alone re-seeds
     * twice the passes that held. A completed scan is overwritten, never max-kept: the newest completed
     * scan is the truth of this shape, and a colliding shape's memo only mis-seeds a pass count the
     * abort corrects.
     */
    private final Long2ObjectOpenHashMap<CompletedGroupScan> completedGroupScans = new Long2ObjectOpenHashMap<>();

    /** The exact group count and pass count of a completed hash-range pass set. */
    public record CompletedGroupScan(long groups, int passes) {
      public CompletedGroupScan {
        if (groups <= 0L) {
          throw new IllegalArgumentException("groups must be positive: " + groups);
        }
        if (passes <= 0) {
          throw new IllegalArgumentException("passes must be positive: " + passes);
        }
      }
    }

    /** The last completed pass set of this shape, or {@code null} when none has completed. */
    public @Nullable CompletedGroupScan completedGroupScanFor(final long fingerprint) {
      synchronized (completedGroupScans) {
        return completedGroupScans.get(fingerprint);
      }
    }

    /**
     * Record a completed pass set — its group count and the pass count that completed; the newest
     * overwrites.
     */
    public void noteCompletedGroupScan(final long fingerprint, final long groups, final int passes) {
      if (groups <= 0L || passes <= 0) {
        return;
      }
      final CompletedGroupScan scan = new CompletedGroupScan(groups, passes);
      synchronized (completedGroupScans) {
        completedGroupScans.put(fingerprint, scan);
      }
    }

    /**
     * Dictionary-string columns whose EVERY per-leaf dictionary entry was proven pairwise distinct
     * under a {@link ProjectionStringIdentityRegistry.Fingerprint}, by column ordinal. A verdict is a
     * property of the column's data in this handle's revision, so it holds for every later scan over
     * the column whatever its predicates: a subset of pairwise-distinct strings is pairwise distinct.
     * Keyed by the fingerprint INSTANCE the proof ran under — a test that installs an adversarial
     * fingerprint must not inherit a verdict the production functions earned. Only a PROVEN verdict is
     * ever stored: a collision or a budget refusal declines that query and leaves the memo untouched,
     * so the next query proves again.
     */
    private final Int2ObjectOpenHashMap<ProjectionStringIdentityRegistry.Fingerprint> provenStringIdentities =
        new Int2ObjectOpenHashMap<>();

    /**
     * Whether {@code column}'s strings were proven pairwise distinct under {@code fingerprint}.
     *
     * @param column the column ordinal
     * @param fingerprint the fingerprint the asking registry proves under
     * @return {@code true} when a scan over the column needs no identity proof
     */
    public boolean stringIdentityProven(final int column,
        final ProjectionStringIdentityRegistry.Fingerprint fingerprint) {
      Objects.requireNonNull(fingerprint, "fingerprint must not be null");
      synchronized (provenStringIdentities) {
        return provenStringIdentities.get(column) == fingerprint;
      }
    }

    /**
     * Record that a FULL-coverage scan proved every dictionary entry of {@code column} pairwise
     * distinct under {@code fingerprint}. Callers must have proven every entry of every leaf — a lazy
     * or predicated scan proves only the entries its surviving rows name and must not note.
     *
     * @param column the column ordinal
     * @param fingerprint the fingerprint the proof ran under
     */
    public void noteStringIdentityProven(final int column,
        final ProjectionStringIdentityRegistry.Fingerprint fingerprint) {
      Objects.requireNonNull(fingerprint, "fingerprint must not be null");
      if (column < 0) {
        throw new IllegalArgumentException("column must be >= 0: " + column);
      }
      synchronized (provenStringIdentities) {
        provenStringIdentities.put(column, fingerprint);
      }
    }

    /**
     * Per-id string-length tables of GLOBAL dictionary columns, by {@code (dictionary header key,
     * length mode)}: what {@code AVG(length(col))} over a global column indexes per row. A table is one
     * walk of the whole dictionary — every block decoded once, ~18M ids for URL at 100M — and it is a
     * pure function of the dictionary this handle's build revision reads, so the first query to need it
     * derives it and every later one indexes it. Retention is bounded in BYTES by
     * {@code sirix.projection.stringLength.memoBytes}; past the bound a table is still returned to its
     * query but not kept (the "0 disables" of the property is the kill switch).
     */
    private final Long2ObjectOpenHashMap<int[]> stringLengthTables = new Long2ObjectOpenHashMap<>();

    /** Bytes {@link #stringLengthTables} retains; guarded by the map's monitor. */
    private long stringLengthTableBytes;

    /** The memoised length table of a dictionary in a mode, or {@code null} when none is retained. */
    public int @Nullable [] stringLengthTable(final long dictionaryHeaderKey, final byte lengthMode) {
      final long key = stringLengthTableKey(dictionaryHeaderKey, lengthMode);
      synchronized (stringLengthTables) {
        return stringLengthTables.get(key);
      }
    }

    /**
     * Retain a derived length table for later queries when the memo's byte bound allows it.
     *
     * @return whether the table is now retained (also {@code true} when an equal-sized table already
     *         was — the first derivation wins and the caller keeps using its own)
     */
    public boolean noteStringLengthTable(final long dictionaryHeaderKey, final byte lengthMode, final int[] table) {
      Objects.requireNonNull(table, "table");
      final long bytes = 16L + 4L * table.length;
      final long key = stringLengthTableKey(dictionaryHeaderKey, lengthMode);
      synchronized (stringLengthTables) {
        if (stringLengthTables.containsKey(key)) {
          return true;
        }
        if (bytes > stringLengthMemoBytes - stringLengthTableBytes) {
          return false;
        }
        stringLengthTables.put(key, table);
        stringLengthTableBytes += bytes;
        return true;
      }
    }

    /** Bytes of string-length tables this handle retains. */
    public long stringLengthTableBytes() {
      synchronized (stringLengthTables) {
        return stringLengthTableBytes;
      }
    }

    private static long stringLengthTableKey(final long dictionaryHeaderKey, final byte lengthMode) {
      if (dictionaryHeaderKey <= 0L || dictionaryHeaderKey > Long.MAX_VALUE >>> 2) {
        throw new IllegalArgumentException("dictionary header key out of range: " + dictionaryHeaderKey);
      }
      if (lengthMode < 0 || lengthMode > 3) {
        throw new IllegalArgumentException("not a string length mode: " + lengthMode);
      }
      return (dictionaryHeaderKey << 2) | lengthMode;
    }

    /** One-shot latch for the background whole-projection segment readahead. */
    private final AtomicBoolean segmentPrefetchKicked = new AtomicBoolean();

    /**
     * Background advisory readahead of every projection segment — a fresh process's first queries
     * otherwise demand-fault the segments column by column. One executor-owned sweep through
     * {@link ProjectionColumnStore#prefetchAllSegments}; failures stay silent (pure hint).
     */
    public void kickSegmentPrefetch(final Executor executor, final Supplier<AutoCloseable> trxFactory,
        final Function<AutoCloseable, StorageEngineReader> readerOf) {
      final ProjectionColumnStore store = columnStore;
      if (store == null || !segmentPrefetchKicked.compareAndSet(false, true)) {
        return;
      }
      final CancellableBackgroundTask task = new CancellableBackgroundTask() {
        @Override
        public void run() {
          try (AutoCloseable trx = trxFactory.get()) {
            store.prefetchAllSegments(readerOf.apply(trx));
          } catch (final Exception ignored) {
            // Advisory only.
          }
        }

        @Override
        public void cancelBeforeExecution() {
          segmentPrefetchKicked.set(false);
        }
      };
      try {
        executor.execute(task);
      } catch (final RejectedExecutionException rejected) {
        // The owning executor raced close. Let a later live executor issue the one-shot hint.
        task.cancelBeforeExecution();
      }
    }

    /** One-shot latch so background promotion is kicked exactly once per handle. */
    private final AtomicBoolean promotionKicked = new AtomicBoolean();

    /**
     * Largest whole-leaf payload {@link #promoteInBackground} will materialize, in bytes. The payload
     * is a {@code List<byte[]>} on the Java heap, so the default is a quarter of the heap, itself
     * capped so a huge heap does not authorise a huge speculative materialization.
     */
    private static long promoteMaxBytes() {
      final String configured = System.getProperty("sirix.projection.promoteMaxBytes");
      if (configured != null && !configured.isEmpty()) {
        try {
          return Long.parseLong(configured.trim());
        } catch (final NumberFormatException ignored) {
          // fall through to the derived default
        }
      }
      return Math.min(DEFAULT_PROMOTE_MAX_BYTES, Runtime.getRuntime().maxMemory() / 4);
    }

    /** Ceiling on the derived promotion budget — see {@link #promoteMaxBytes()}. */
    private static final long DEFAULT_PROMOTE_MAX_BYTES = 4L << 30;

    /**
     * HOT promotion without the stall: materialize the whole-leaf payloads on an executor-owned task
     * while callers keep serving from slices; {@link #payloadsMaterialized()} flips when the work lands
     * and the byte kernels take over on the NEXT query. Failures are swallowed — a failed promotion
     * just means staying on the (correct) sliced path; the next synchronous consumer re-surfaces the
     * error attributably through {@link #rowGroupPayloads(Supplier)}.
     */
    public void promoteInBackground(final Executor executor, final Supplier<List<byte[]>> materializer) {
      if (materializer == null || !promotionKicked.compareAndSet(false, true)) {
        return;
      }
      // Promotion is an OPTIMISATION — it trades heap for a faster kernel on a handle that keeps
      // serving sliced. At large row-group counts the whole-leaf payload is tens of GB, so an
      // ungated promotion turns a working sliced route into an OOM. Declining is free: the sliced
      // kernels are the correct route and already answer every query. The decision is latched, so a
      // handle too big to promote is never re-probed.
      if (projectedWeightBytes > promoteMaxBytes()) {
        LOGGER.debug("Projection promotion declined: {} bytes exceeds the {} byte promotion budget",
            projectedWeightBytes, promoteMaxBytes());
        return;
      }
      final CancellableBackgroundTask task = new CancellableBackgroundTask() {
        @Override
        public void run() {
          try {
            rowGroupPayloads(materializer);
          } catch (final RuntimeException ignored) {
            // Stay sliced; the sync path reports real corruption attributably.
          }
        }

        @Override
        public void cancelBeforeExecution() {
          promotionKicked.set(false);
        }
      };
      try {
        executor.execute(task);
      } catch (final RejectedExecutionException rejected) {
        // The owning executor raced close. A later live executor may retry the promotion.
        task.cancelBeforeExecution();
      }
    }

    /**
     * Whole raw leaves of an ALREADY-materialized handle (eager handles, or a lazy handle a prior
     * consumer already hydrated). Does NO I/O and never needs a session-bound source.
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

    /**
     * Attach the declared per-column paths relative to the record root — called once, at construction
     * time, by the catalog (the same discipline as {@link #setSetValueRowCounts(Map)}). A {@code null}
     * argument leaves the handle name-matched.
     */
    public void setFieldChains(final String[] chains) {
      this.fieldChains = chains != null && chains.length == fieldNames.length
          ? chains.clone()
          : null;
    }

    /**
     * Resolve the column a query field token names, or {@code -1}.
     *
     * <p>
     * The token is a deref CHAIN relative to the record root: {@code "dept"} for {@code $r.dept},
     * {@code "commit/collection"} for {@code $r.commit.collection}. When the handle carries declared
     * paths ({@link #setFieldChains}) the match is against those, so a nested column never answers a
     * top-level deref of the same trailing name and a nested deref never lands on a same-named
     * top-level column — both would be silent wrong answers, and both now miss (the caller declines to
     * the generic pipeline). A top-level column's chain IS its trailing name, so single-step derefs
     * resolve exactly as they always did. Columns without a relativizable declared path, and handles
     * installed without paths at all (bench/test registry installs), fall back to trailing name
     * matching.
     */
    public int columnOf(final String name) {
      final String[] chains = fieldChains;
      for (int i = 0; i < fieldNames.length; i++) {
        final String chain = chains == null
            ? null
            : chains[i];
        if (chain == null
            ? fieldNames[i].equals(name)
            : chain.equals(name))
          return i;
      }
      return -1;
    }

    /**
     * Return the canonical dictionary for the {@code groupColumn}'s STRING_DICT values, or {@code null}
     * if dense group-by is not eligible (cardinality exceeds {@code cardLimit}, column is not
     * STRING_DICT, or rowGroupPayloads is empty).
     *
     * <p>
     * Result is cached per-column under a CAS so subsequent calls are zero-cost.
     * {@link #CANON_DICT_INELIGIBLE} caches "probe established ineligible" so we don't re-probe.
     *
     * <p>
     * HFT-grade: volatile read on fast path; one probe (bounded to {@code probeLeaves} leaves) on first
     * call per column.
     */
    public byte[][] canonicalDict(final int groupColumn, final int probeLeaves, final int cardLimit,
        final Supplier<List<byte[]>> materializer) {
      if (groupColumn < 0)
        return null;
      byte[][][] cache = canonicalDicts;
      if (cache != null && groupColumn < cache.length) {
        final byte[][] cached = cache[groupColumn];
        if (cached == CANON_DICT_INELIGIBLE)
          return null;
        if (cached != null)
          return cached;
      }
      // Compute outside the monitor — probe can be several ms; then
      // publish under the monitor to avoid lost wake-ups.
      final byte[][] probed;
      try {
        probed = ProjectionIndexByteScan.probeCanonicalDict(rowGroupPayloads(materializer), groupColumn, probeLeaves,
            cardLimit);
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
          if (cache != null)
            System.arraycopy(cache, 0, grown, 0, cache.length);
          cache = grown;
          canonicalDicts = cache;
        }
        final byte[][] existing = cache[groupColumn];
        if (existing != null && existing != CANON_DICT_INELIGIBLE)
          return existing;
        cache[groupColumn] = probed != null
            ? probed
            : CANON_DICT_INELIGIBLE;
        return probed;
      }
    }
  }

  /** Sentinel for "probe found ineligible for dense group-by" — see {@link Handle#canonicalDict}. */
  private static final byte[][] CANON_DICT_INELIGIBLE = new byte[0][];

  /** Exact (resource, sourcePath) entries — test/bench wiring. */
  private static final ConcurrentMap<String, Handle> REGISTRY = new ConcurrentHashMap<>();

  /**
   * Wildcard projections, pooled per resource and matched STRUCTURALLY by (rootPath, ordered field
   * list) — no string-encoded composite keys, so field names may contain any character and identity
   * always includes the record-set root. CopyOnWriteArrayList: pools are tiny (a handful of
   * projections per resource), reads vastly outnumber writes.
   */
  private static final ConcurrentMap<String, CopyOnWriteArrayList<Handle>> WILDCARDS = new ConcurrentHashMap<>();

  /**
   * One-shot latch tracking which registry keys have already been JIT pre-warmed. Pre-warm is
   * idempotent, but re-firing it on every {@link #install} when a caller re-registers the same
   * resource wastes a few ms. The latch key is the registry key (resourceKey + sourcePath).
   */
  private static final ConcurrentMap<String, Boolean> PREWARMED = new ConcurrentHashMap<>();

  /**
   * Default on — drain first-call JIT tier-up for {@link ProjectionIndexByteScan} into the install
   * step so the very first user-visible query on a freshly installed projection index doesn't pay
   * 100-1000 ms of C2 compilation cost.
   *
   * <p>
   * On a cold 100M bench we measured 787-1417 ms of variance across runs for the first
   * {@code conjunctiveCountByGroup} invocation; pre-warming the method shape with a few hundred tiny
   * (2-leaf) calls at install time collapses that spread and shifts the cost out of the user-facing
   * measured window.
   *
   * <p>
   * Set {@code -Dsirix.projection.prewarmJit=false} to disable.
   */
  private static final boolean PREWARM_JIT_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.prewarmJit", "true"));

  /**
   * Outer-loop count for the pre-warm. Each iteration scans a 2-leaf subList per fired method shape —
   * enough to cross C2's tier-3 threshold and drive the hot byte-code paths
   * ({@link ProjectionIndexByteScan#conjunctiveCount},
   * {@link ProjectionIndexByteScan#conjunctiveCountByGroup}, plus their {@code evaluateRowGroupMask}
   * / {@code evalColumn*} callees) into a compiled state before the first user query fires.
   *
   * <p>
   * Default {@code 200} was selected on the cold 100M brackit-scale-bench: smaller (100) undershoots
   * tier-up on the group-by-count path; larger (500+) pays more up-front than the tier-up wins back.
   * At 200 we measure a median cold wall drop of −0.27 s vs pre-warm off, with the first-call
   * {@code compoundAndFilterCount} dropping from 1.05 ms → 0.53 ms.
   *
   * <p>
   * Tune via {@code -Dsirix.projection.prewarmJit.iters=N}. Zero disables even when
   * {@link #PREWARM_JIT_ENABLED} is true.
   */
  private static final int PREWARM_ITERS =
      Integer.parseInt(System.getProperty("sirix.projection.prewarmJit.iters", "200"));

  /**
   * iter#10 — pre-warm the dense group-by method only when it's enabled (opt-in). Default off aligns
   * with {@link #PREWARM_DENSE_DEFAULT_OFF} so we don't pay the install-time cost for a feature
   * that's not being used. Tune via {@code -Dsirix.projection.denseGroupBy=true}.
   */
  private static final boolean PREWARM_DENSE_GROUPBY_ENABLED =
      Boolean.parseBoolean(System.getProperty("sirix.projection.denseGroupBy", "false"));

  /**
   * Accumulator-cell ceiling for the NUMERIC_LONG dense pre-warm. Unlike the string dense arm this is
   * ON by default (the numeric dense arm is the driver's default choice), so the bound only exists to
   * keep the install-time allocation trivial: 64 K cells = 512 KB, one array, freed immediately.
   */
  private static final int PREWARM_DENSE_CELLS = 1 << 16;

  private ProjectionIndexRegistry() {}

  /**
   * Publish a projection index into the registry. Overwrites any prior entry with the same key.
   *
   * @param resourceKey stable identifier for the Sirix resource (e.g.
   *        {@code database.getName() + "/" + resourceName}).
   * @param sourcePath JSON source path segments (e.g. {@code ["doc", "records"]} for
   *        {@code $doc/records[]}). {@code null} allowed and treated as the empty path.
   * @param fieldNames ordered column list — this order is authoritative for the serialised leaf
   *        payloads' column layout.
   * @param rowGroupPayloads one {@link ProjectionIndexRowGroupPage#serialize()} byte[] per leaf, in
   *        leaf key order. Caller must not mutate after publish.
   */
  public static void install(final String resourceKey, final String[] sourcePath, final String[] fieldNames,
      final List<byte[]> rowGroupPayloads) {
    final String k = key(resourceKey, sourcePath);
    final Handle handle = new Handle(fieldNames, rowGroupPayloads);
    REGISTRY.put(k, handle);
    prewarmIfFirst(k, handle);
  }

  /**
   * @return installed handle for {@code (resourceKey, sourcePath)}. Falls back to the FIRST wildcard
   *         entry (installed via {@link #installWildcard}) if no exact match exists — makes bench
   *         wiring simpler when the sourcePath shape the Brackit optimizer produces is not known
   *         ahead of time. With several wildcard projections installed the fallback picks the oldest
   *         install — callers that know their required fields should use {@link #lookupCovering}
   *         instead.
   */
  public static Handle lookup(final String resourceKey, final String[] sourcePath) {
    final Handle exact = REGISTRY.get(key(resourceKey, sourcePath));
    if (exact != null)
      return exact;
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null)
      return null;
    // COW iteration is a single snapshot — isEmpty()+get(0) would race a
    // concurrent uninstall between the two calls.
    for (final Handle handle : pool) {
      return handle;
    }
    return null;
  }

  /**
   * Covering lookup without revision gating — for callers with no revision context (bench/test
   * wiring). Production query paths should pass the executor's revision via the four-argument
   * overload.
   */
  public static Handle lookupCovering(final String resourceKey, final String[] sourcePath,
      final String[] requiredFields) {
    return lookupCovering(resourceKey, sourcePath, requiredFields, Integer.MAX_VALUE);
  }

  /**
   * Covering lookup: the exact {@code (resourceKey, sourcePath)} entry when it carries every required
   * field, else the wildcard projection with the FEWEST columns that covers all of
   * {@code requiredFields} (fewest first: narrower projections scan less per row, and the choice
   * stays deterministic when several overlapping projections are installed).
   *
   * <p>
   * Two safety gates, both fail-closed to the generic scan pipeline:
   * <ul>
   * <li><b>Revision</b>: a handle is only served to executors at
   * {@code revision >= validFromRevision} — a time-travel executor bound to an older revision must
   * not read columns built later.</li>
   * <li><b>Root ambiguity</b>: when covering candidates were built over DIFFERENT record-set roots,
   * the registry cannot tell which record set the query iterates (wildcard entries ignore
   * sourcePath), so it returns {@code null} instead of guessing.</li>
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
    if (pool == null)
      return null;
    Handle best = null;
    for (final Handle candidate : pool) {
      if (candidate.validFromRevision > revision)
        continue;
      if (!covers(candidate, requiredFields))
        continue;
      if (best != null && !Objects.equals(best.rootPath, candidate.rootPath) && best.rootPath != null
          && candidate.rootPath != null) {
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
   * @return the wildcard handle whose field list equals {@code fieldNames} exactly (same names, same
   *         order), regardless of root — or {@code null}. Prefer {@link #lookupExact} when the root
   *         is known.
   */
  public static Handle lookupExactFields(final String resourceKey, final String[] fieldNames) {
    return lookupExact(resourceKey, null, fieldNames);
  }

  /**
   * @return the wildcard handle with exactly this (rootPath, ordered field list) identity —
   *         {@code null} rootPath matches any root
   */
  public static Handle lookupExact(final String resourceKey, final String rootPath, final String[] fieldNames) {
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null)
      return null;
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
    if (pool != null && !pool.isEmpty())
      return true;
    if (REGISTRY.isEmpty())
      return false;
    final String prefix = Objects.requireNonNull(resourceKey, "resourceKey") + "\0";
    for (final String k : REGISTRY.keySet()) {
      if (k.startsWith(prefix))
        return true;
    }
    return false;
  }

  /**
   * Install a projection index that matches <em>any</em> source path for the given resource. Useful
   * in benches where the caller doesn't know the exact {@code sourcePath} shape Brackit will pass to
   * {@code executePredicateCount}.
   */
  public static void installWildcard(final String resourceKey, final String[] fieldNames,
      final List<byte[]> rowGroupPayloads) {
    installWildcard(resourceKey, null, 0, fieldNames, rowGroupPayloads, null);
  }

  /** Variant carrying builder-tracked NUMERIC_LONG integrality evidence. */
  public static void installWildcard(final String resourceKey, final String[] fieldNames,
      final List<byte[]> rowGroupPayloads, final boolean[] numericNonIntegral) {
    installWildcard(resourceKey, null, 0, fieldNames, rowGroupPayloads, numericNonIntegral);
  }

  /**
   * Full identity variant. A resource holds SEVERAL projections side by side — analogous to the other
   * index types — matched STRUCTURALLY by (rootPath, ordered field list): re-installing the same
   * identity replaces that entry, a different identity adds one. Query-time selection happens in
   * {@link #lookupCovering}.
   *
   * @param rootPath canonical record-set root the columns were built over; {@code null} = legacy/any
   * @param validFromRevision first revision the columns are valid for; {@code 0} = any
   */
  public static void installWildcard(final String resourceKey, final String rootPath, final int validFromRevision,
      final String[] fieldNames, final List<byte[]> rowGroupPayloads, final boolean[] numericNonIntegral) {
    final Handle handle = new Handle(rootPath, validFromRevision, fieldNames, rowGroupPayloads, numericNonIntegral);
    final List<Handle> pool = WILDCARDS.computeIfAbsent(Objects.requireNonNull(resourceKey, "resourceKey"),
        k -> new CopyOnWriteArrayList<>());
    boolean replaced = false;
    for (int i = 0; i < pool.size(); i++) {
      final Handle existing = pool.get(i);
      if (Arrays.equals(existing.fieldNames, handle.fieldNames) && Objects.equals(existing.rootPath, handle.rootPath)) {
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
   * Remove the wildcard projection with exactly this (rootPath, field list) identity; {@code null}
   * rootPath removes matching-fields entries of any root.
   */
  public static void uninstallWildcard(final String resourceKey, final String rootPath, final String[] fieldNames) {
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool == null)
      return;
    pool.removeIf(handle -> Arrays.equals(handle.fieldNames, fieldNames)
        && (rootPath == null || Objects.equals(handle.rootPath, rootPath)));
  }

  private static String prewarmKey(final String resourceKey, final String rootPath, final String[] fieldNames) {
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
   * Fire a one-shot JIT pre-warm for {@code handle} if this is the first install under
   * {@code registryKey}. Swallow all exceptions — a pre-warm failure must never break real installs.
   *
   * <p>
   * The pre-warm uses the actual installed payloads (not synthetic) so the shapes C2 profiles during
   * tier-up match the shapes the first real query will invoke. A tiny 2-leaf subList keeps each
   * iteration sub-ms while still driving the back-branch / invocation counters past C2's tier-3
   * threshold across the configured repeat count (see {@link #PREWARM_ITERS}).
   *
   * <p>
   * Idempotent per registry key — the {@link #PREWARMED} latch prevents re-firing when a caller
   * re-installs the same key.
   */
  private static void prewarmIfFirst(final String registryKey, final Handle handle) {
    if (!PREWARM_JIT_ENABLED)
      return;
    if (PREWARM_ITERS <= 0)
      return;
    if (handle.rowGroupPayloads == null || handle.rowGroupPayloads.isEmpty())
      return;
    if (PREWARMED.putIfAbsent(registryKey, Boolean.TRUE) != null)
      return;
    try {
      prewarmJitForHandle(handle);
    } catch (final Throwable ignored) {
      // Pre-warm is best-effort; a failure must not interfere with installs.
    }
  }

  /**
   * JIT tier-up driver for {@link ProjectionIndexByteScan}'s hot methods. Visible for tests. Safe to
   * call multiple times — pre-warm is idempotent by construction (no state leaks into the registry or
   * its handles).
   *
   * <p>
   * Shape coverage (matches the bench query set in {@code ScaleBenchMain}):
   * <ul>
   * <li>{@code conjunctiveCount(numeric GT)} — e.g. {@code $u.age > 40}</li>
   * <li>{@code conjunctiveCount(numeric BETWEEN)} — e.g. fused
   * {@code $u.age > 30 and $u.age < 50}</li>
   * <li>{@code conjunctiveCount(boolean EQ)} — e.g. {@code $u.active}</li>
   * <li>{@code conjunctiveCount(numeric + boolean)} — e.g. {@code $u.age > 40 and $u.active}</li>
   * <li>{@code conjunctiveCountByGroup(empty preds)} — e.g. {@code group by $d}</li>
   * <li>{@code conjunctiveCountByGroup(boolean EQ)} — e.g.
   * {@code where $u.active ... group by $d}</li>
   * </ul>
   *
   * <p>
   * Columns are selected on first-leaf inspection: the first {@code NUMERIC_LONG} becomes the numeric
   * predicate column, the first {@code BOOLEAN} the boolean column, and the first {@code STRING_DICT}
   * the group column. When a shape's required column kind is absent, that shape is skipped silently —
   * still correct, just slightly less warm-up coverage for unusual index schemas.
   */
  static void prewarmJitForHandle(final Handle handle) {
    final List<byte[]> payloads = handle.rowGroupPayloads;
    if (payloads == null || payloads.isEmpty())
      return;
    final byte[] firstRowGroup = payloads.get(0);
    if (firstRowGroup == null)
      return;
    // Column count encoded at offset 4 little-endian; kinds start at offset 24.
    final int columnCount = (firstRowGroup[4] & 0xFF) | ((firstRowGroup[5] & 0xFF) << 8)
        | ((firstRowGroup[6] & 0xFF) << 16) | ((firstRowGroup[7] & 0xFF) << 24);
    if (columnCount <= 0 || columnCount > 256)
      return;
    int numericCol = -1;
    int booleanCol = -1;
    int stringDictCol = -1;
    for (int c = 0; c < columnCount; c++) {
      final byte kind = firstRowGroup[24 + c];
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG && numericCol < 0)
        numericCol = c;
      else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN && booleanCol < 0)
        booleanCol = c;
      else if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT && stringDictCol < 0)
        stringDictCol = c;
    }

    // Take at most 2 leaves as the pre-warm input. The goal is per-method
    // tier-up, not per-leaf: a small subList keeps per-call cost sub-ms while
    // driving the invocation counter past C2's tier-4 threshold.
    final int subSize = Math.min(2, payloads.size());
    final List<byte[]> sub = payloads.subList(0, subSize);

    // Numeric GT — e.g. $u.age > 40.
    if (numericCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] numGt =
          {ProjectionIndexScan.ColumnPredicate.numeric(numericCol, ProjectionIndexScan.Op.GT, 0L)};
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, numGt);
      }

      // Numeric BETWEEN — e.g. $u.age > 30 and $u.age < 50 (fused).
      final ProjectionIndexScan.ColumnPredicate[] numBetween =
          {ProjectionIndexScan.ColumnPredicate.numericBetween(numericCol, ProjectionIndexScan.Op.GT, 0L,
              ProjectionIndexScan.Op.LT, Long.MAX_VALUE)};
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, numBetween);
      }
    }

    // Boolean EQ — e.g. $u.active.
    if (booleanCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] boolEq =
          {ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)};
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, boolEq);
      }
    }

    // Numeric + boolean — e.g. $u.age > 40 and $u.active.
    if (numericCol >= 0 && booleanCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] mix =
          {ProjectionIndexScan.ColumnPredicate.numeric(numericCol, ProjectionIndexScan.Op.GT, 0L),
              ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)};
      for (int i = 0; i < PREWARM_ITERS; i++) {
        ProjectionIndexByteScan.conjunctiveCount(sub, mix);
      }
    }

    // NUMERIC_LONG group-by shapes. Separate kernels from the dict ones below (no
    // canonical dict, no intern, primitive-keyed accumulators), so they need their own
    // tier-up drive — without it the first real numeric group-by runs interpreted and
    // every cold measurement of it is wrong.
    //
    // Contained in its OWN try/catch: both numeric arms throw IllegalStateException by design on a
    // leaf they cannot serve (no presence tail, an unaddressable range), where the dict kernels
    // below simply tolerate. The single catch around the whole driver plus the already-latched
    // PREWARMED flag would turn one such leaf into a permanent skip of every shape declared after
    // this block — the dict group-by tier-up killed as collateral by a column it never reads.
    if (numericCol >= 0) {
      try {
        final ProjectionIndexScan.ColumnPredicate[] noPreds = new ProjectionIndexScan.ColumnPredicate[0];
        final Long2LongOpenHashMap numericSink = new Long2LongOpenHashMap(64);
        numericSink.defaultReturnValue(0L);
        final long[] missing = new long[1];
        for (int i = 0; i < PREWARM_ITERS; i++) {
          numericSink.clear();
          missing[0] = 0;
          ProjectionIndexByteScan.conjunctiveCountByGroupNumeric(sub, noPreds, numericCol, numericSink, missing);
        }
        // Dense arm over the SAME leaves: only reachable when the sub-list's zone map yields a
        // range small enough to address, which is exactly the regime the driver picks it in.
        final long[] range = new long[3];
        if (ProjectionIndexByteScan.numericZoneUnion(sub, numericCol, range)) {
          final long span = range[1] - range[0];
          if (span >= 0 && span < PREWARM_DENSE_CELLS) {
            final long[] denseCounts = new long[(int) span + 1];
            for (int i = 0; i < PREWARM_ITERS; i++) {
              Arrays.fill(denseCounts, 0L);
              missing[0] = 0;
              ProjectionIndexByteScan.conjunctiveCountByGroupNumericDense(sub, noPreds, numericCol, range[0],
                  denseCounts, missing);
            }
          }
        }
      } catch (final RuntimeException numericNotServable) {
        // Pre-warm only — a leaf these kernels decline costs a tier-up, never an answer.
      }
    }

    // Group-by shapes. Need a STRING_DICT column to route through the
    // dict decode + intern paths in conjunctiveCountByGroup.
    if (stringDictCol >= 0) {
      final ProjectionIndexScan.ColumnPredicate[] noPreds = new ProjectionIndexScan.ColumnPredicate[0];
      final Object2LongOpenHashMap<String> sink = new Object2LongOpenHashMap<>();
      sink.defaultReturnValue(0L);
      for (int i = 0; i < PREWARM_ITERS; i++) {
        sink.clear();
        ProjectionIndexByteScan.conjunctiveCountByGroup(sub, noPreds, stringDictCol, sink);
      }

      if (booleanCol >= 0) {
        final ProjectionIndexScan.ColumnPredicate[] boolEqGroup =
            {ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)};
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
          ProjectionIndexByteScan.conjunctiveCountByGroupDense(sub, noPreds, stringDictCol, canonical, denseCounts,
              denseFallback);
        }
        if (booleanCol >= 0) {
          final ProjectionIndexScan.ColumnPredicate[] boolEqGroup =
              {ProjectionIndexScan.ColumnPredicate.booleanEq(booleanCol, true)};
          for (int i = 0; i < PREWARM_ITERS; i++) {
            java.util.Arrays.fill(denseCounts, 0L);
            denseFallback.clear();
            ProjectionIndexByteScan.conjunctiveCountByGroupDense(sub, boolEqGroup, stringDictCol, canonical,
                denseCounts, denseFallback);
          }
        }
      }
    }
  }

  private static String key(final String resourceKey, final String[] sourcePath) {
    return Objects.requireNonNull(resourceKey, "resourceKey") + "\0" + String.join("/", sourcePath == null
        ? new String[0]
        : sourcePath);
  }

  /**
   * Coverage check: every {@code predicateField} must appear in the handle's {@code fieldNames}.
   */
  public static boolean covers(final Handle handle, final String[] predicateFields) {
    if (handle == null || predicateFields == null)
      return false;
    for (final String f : predicateFields) {
      if (handle.columnOf(f) < 0)
        return false;
    }
    return true;
  }

  /**
   * True if any projection registered under {@code resourceKey} (exact or wildcard) carries
   * {@code field} as a column. Used as a proof-of-existence by the query executor's name-key resolver
   * so it can skip an expensive full-document walk when a covering projection is already installed.
   *
   * <p>
   * Lookup is a prefix scan over the registry — O(N) in the number of entries per resource, which is
   * bounded to a small constant in practice.
   */
  public static boolean anyHandleCoversField(final String resourceKey, final String field) {
    if (resourceKey == null || field == null)
      return false;
    final List<Handle> pool = WILDCARDS.get(resourceKey);
    if (pool != null) {
      for (final Handle handle : pool) {
        if (handle.columnOf(field) >= 0)
          return true;
      }
    }
    if (REGISTRY.isEmpty())
      return false;
    final String prefix = resourceKey + "\0";
    for (final var entry : REGISTRY.entrySet()) {
      if (!entry.getKey().startsWith(prefix))
        continue;
      if (entry.getValue().columnOf(field) >= 0)
        return true;
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
    REGISTRY.forEach((k, h) -> sb.append("  ")
                                 .append(k)
                                 .append(" -> fields=")
                                 .append(Arrays.toString(h.fieldNames))
                                 .append(", leaves=")
                                 .append(h.rowGroupPayloads == null
                                     ? "lazy"
                                     : String.valueOf(h.rowGroupPayloads.size()))
                                 .append("\n"));
    WILDCARDS.forEach((resource, pool) -> {
      for (final Handle h : pool) {
        sb.append("  ")
          .append(resource)
          .append(" * root=")
          .append(h.rootPath)
          .append(" validFrom=")
          .append(h.validFromRevision)
          .append(" -> fields=")
          .append(Arrays.toString(h.fieldNames))
          .append(", leaves=")
          .append(h.rowGroupPayloads.size())
          .append("\n");
      }
    });
    return sb.append("]").toString();
  }
}
