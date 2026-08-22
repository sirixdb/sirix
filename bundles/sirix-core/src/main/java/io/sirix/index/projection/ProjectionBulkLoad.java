/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.path.summary.PathSummaryReader;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * One-pass (load-time) construction of a projection index: the index is declared on an EMPTY resource
 * and then maintained by the shred itself, instead of being derived afterwards by a second full walk
 * of the finished resource.
 *
 * <h2>Why this is not just the incremental maintenance path</h2>
 *
 * {@link ProjectionIndexChangeListener}'s per-leaf patcher maintains an EXISTING snapshot: it
 * re-extracts touched leaves, appends to the tail leaf, and updates derived structures in bounded
 * persistent units. Running it over a bulk load would still repeat touched-unit work at every
 * auto-commit window. This class instead keeps the REAL build machinery
 * ({@link ProjectionIndexBuilder} in its record-fed mode) alive for the whole load, so the leaves,
 * the global-dictionary decision, the fingerprint blocks, the fences and the metadata are produced by
 * exactly the code the post-pass build uses.
 *
 * <h2>Surviving the auto-commit</h2>
 *
 * A bulk load commits every {@code -Dsirix.autoCommit.nodes} nodes, and every one of those commits
 * destroys and rebinds the index listeners and installs a NEW storage-engine writer and path summary
 * ({@code AbstractNodeTrxImpl#reInstantiate}). Build state therefore cannot live in the listener. It
 * lives here, keyed by resource and definition, and the listener hands its CURRENT writer and summary
 * in on each use — nothing epoch-scoped is ever cached across a commit.
 *
 * <h2>What is written when</h2>
 *
 * Full leaves stream into the definition's HOT sub-tree as they fill and ride the auto-commit that
 * follows. Retained derived state is explicit and bounded: two fence longs per emitted leaf, one
 * 256-leaf Bloom-reference window per local-string column, and only those set-summary values that
 * still fit their one optional summary chunk. Complete Bloom windows stream to storage eagerly;
 * fence chunks, the Bloom manifests, set summaries and live metadata publish at {@link #finish}.
 * Slot 0 holds the {@link ProjectionIndexMetadata#staleTombstone() stale tombstone} for the whole
 * load, so a load that dies half-way leaves a projection every reader SKIPS in favour of the generic
 * pipeline. Writing a truthful-looking {@code rowGroupCount=0} metadata instead would make every
 * query answer from an empty index — zero rows, silently, which is the one outcome worse than being
 * slow.
 */
public final class ProjectionBulkLoad {

  /** Publication seam kept synchronous so a storage failure poisons the owning load immediately. */
  @FunctionalInterface
  interface RowGroupPublisher {
    void publish(ProjectionIndexHOTStorage storage, long rowGroupId,
        ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded);
  }

  private static final RowGroupPublisher DEFAULT_ROW_GROUP_PUBLISHER =
      ProjectionIndexHOTStorage::putRowGroupAsColumnSegmentSlots;

  /**
   * Active bulk loads by {@code resourceKey#defId}. Process-global by the same reasoning as
   * {@link ProjectionIndexRegistry}'s registry: the state has to outlive the objects that reach it
   * (listeners are rebuilt per commit epoch, index controllers per revision), and the resource path is
   * the only durable identity all of them agree on. Each value additionally carries the owning
   * write-transaction object identity, so a different transaction using the same path cannot attach
   * to the load.
   */
  private static final ConcurrentMap<String, ProjectionBulkLoad> ACTIVE = new ConcurrentHashMap<>();

  private final IndexDef indexDef;
  private final String key;
  /** Identity of the one write transaction whose successful intermediate epochs may feed this load. */
  private final Object ownerToken;

  /** Owner-confined FSST scratch reused by every row group in this bulk-load stream. */
  private final ProjectionIndexColumnSegmentCodec.EncodeWorkspace encodeWorkspace =
      new ProjectionIndexColumnSegmentCodec.EncodeWorkspace();

  /** The record-fed builder; owns the current leaf, the dictionary sample and the dictionaries. */
  private final ProjectionIndexBuilder builder;

  /** Per-leaf record-key fences, in leaf order — the metadata's zone maps. */
  private final LongArrayList firstKeys = new LongArrayList();
  private final LongArrayList lastKeys = new LongArrayList();

  /** Bounded 256-row-group fingerprint accumulator; full chunks are persisted eagerly. */
  private final ProjectionBloomChunks.Writer bloomChunks = new ProjectionBloomChunks.Writer();

  /** Bounded index-wide per-value row counts for {@code COLUMN_KIND_STRING_SET} columns. */
  private final ProjectionSetSummaryChunks.BuildAccumulator setSummaries =
      new ProjectionSetSummaryChunks.BuildAccumulator();

  private final boolean hasSetColumn;

  /**
   * Whether the declared root path selects ARRAY ELEMENTS. Then a record root is by construction an
   * array element, and a fused {@code OBJECT_NAMED_*} node — which is an object FIELD and nothing else
   * — can never be one. That is what lets the listener skip the ancestor read for every field
   * notification instead of paying one per node.
   */
  private final boolean arrayElementRoot;

  /**
   * The record currently being shredded, or {@code -1} before the first one. Held back from every
   * batch: an auto-commit can fire in the MIDDLE of a record's subtree, and extracting it there would
   * store a half-built row.
   */
  private long currentRecordKey = -1L;

  /**
   * Records proven complete (a later record has started) and not yet extracted. Bounded by the
   * async-flush storage epoch — roughly 300 records for ClickBench and 1.1k for Bluesky at the
   * 32,768-node steady bound — and drained at every commit or storage-only rotation.
   */
  private final LongArrayList completedRecordKeys = new LongArrayList();

  /** Highest record key ever closed — the append-only contract's witness. */
  private long lastClosedRecordKey = Long.MIN_VALUE;

  /**
   * Node keys of the ARRAY instances whose elements are this projection's records. Owned here rather
   * than by the listener because listeners are rebuilt at every auto-commit, and because these are
   * what make the end-of-load row-count check possible: an array node knows how many children it has,
   * so the load can prove it emitted a row for every one of them instead of hoping its per-node
   * attribution missed none.
   */
  private final LongOpenHashSet arrayRootInstances = new LongOpenHashSet(4);

  /** Storage of the CURRENT epoch; set on entry to every method that writes. */
  private @Nullable ProjectionIndexHOTStorage storage;

  private boolean finished;

  /**
   * {@code -Dsirix.projection.bulkDiag=true} prints how every change notification was classified when
   * the load finishes. A load-time build attributes records from notifications alone, so when it comes
   * out short the only question that matters is WHICH notifications it discarded — and without these
   * counters that question needs a debugger.
   */
  private static final boolean DIAG = Boolean.getBoolean("sirix.projection.bulkDiag");

  private long diagNotifications;
  private long diagSkippedNamedKind;
  private long diagSkippedPathClass;
  private long diagNotUnderRecordSet;
  private long diagUnresolved;
  private long diagObserved;
  private long diagClosed;
  private long diagDrains;
  private long diagExtractFailures;

  /** Whether the per-notification classification counters are being kept. */
  public static boolean diagnosticsEnabled() {
    return DIAG;
  }

  /** Count one classification outcome; {@code outcome} is one of the {@code DIAG_*} constants. */
  public void countClassification(final int outcome) {
    diagNotifications++;
    switch (outcome) {
      case DIAG_SKIPPED_NAMED_KIND -> diagSkippedNamedKind++;
      case DIAG_SKIPPED_PATH_CLASS -> diagSkippedPathClass++;
      case DIAG_NOT_UNDER_RECORD_SET -> diagNotUnderRecordSet++;
      case DIAG_UNRESOLVED -> diagUnresolved++;
      default -> diagObserved++;
    }
  }

  public static final int DIAG_SKIPPED_NAMED_KIND = 0;
  public static final int DIAG_SKIPPED_PATH_CLASS = 1;
  public static final int DIAG_NOT_UNDER_RECORD_SET = 2;
  public static final int DIAG_UNRESOLVED = 3;
  public static final int DIAG_OBSERVED = 4;

  private String diagnostics() {
    if (!DIAG) {
      return " (re-run with -Dsirix.projection.bulkDiag=true for the per-notification classification)";
    }
    return " [notifications=" + diagNotifications + " skippedNamedKind=" + diagSkippedNamedKind + " skippedPathClass="
        + diagSkippedPathClass + " notUnderRecordSet=" + diagNotUnderRecordSet + " unresolved=" + diagUnresolved
        + " observed=" + diagObserved + " closed=" + diagClosed + " drains=" + diagDrains
        + " extractFailures=" + diagExtractFailures + " arrayRoots=" + arrayRootInstances.size() + ']';
  }

  private ProjectionBulkLoad(final IndexDef indexDef, final String key, final Object ownerToken,
      final PathSummaryReader pathSummary, final RowGroupPublisher rowGroupPublisher) {
    this.indexDef = indexDef;
    this.key = key;
    this.ownerToken = Objects.requireNonNull(ownerToken, "ownerToken must not be null");
    final RowGroupPublisher checkedPublisher =
        Objects.requireNonNull(rowGroupPublisher, "rowGroupPublisher must not be null");
    this.hasSetColumn = ProjectionIndexBuilder.hasStringSetColumn(indexDef);
    this.arrayElementRoot = ProjectionIndexBuilder.isArrayLayerPath(indexDef.getProjectionRootPath());
    this.builder = ProjectionIndexBuilder.streamingBorrowed(indexDef, pathSummary, leaf -> {
      if (leaf.getRowCount() == 0) {
        throw new IllegalStateException("Projection leaf " + firstKeys.size() + " is empty");
      }
      final int physicalSlot = firstKeys.size() + 1;
      firstKeys.add(leaf.firstRecordKey());
      lastKeys.add(leaf.lastRecordKey());
      if (hasSetColumn) {
        setSummaries.append(leaf);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encodeReferencedOnly(leaf, encodeWorkspace);
      final ProjectionIndexHOTStorage currentStorage = currentStorage();
      checkedPublisher.publish(currentStorage, physicalSlot, encoded);
      ProjectionIndexBuilder.persistOrderExceptionLocators(leaf, physicalSlot,
          ProjectionRecordLocator.open(currentStorage));
      bloomChunks.append(encoded, physicalSlot, currentStorage);
    });
  }

  /** Registry key for a definition on a resource. */
  private static String keyOf(final String resourceKey, final int indexDefId) {
    return resourceKey + '#' + indexDefId;
  }

  /**
   * Arm a bulk load for {@code indexDef} on an empty resource and tombstone slot 0 so nothing reads the
   * half-built index. Fails loudly if one is already armed for this definition — two concurrent bulk
   * loads over one projection would interleave their leaf slots.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter, -1L,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /**
   * As above, telling the build how many records to expect so the global-dictionary election can
   * decline a column whose dictionary would not fit. {@code -1} means unknown.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter,
      final long expectedRows) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter, expectedRows,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /**
   * Arm a load owned by {@code ownerTrx}. Listener epochs may resolve the process-global handoff only
   * when they carry this exact transaction identity; another transaction on the same resource and
   * definition can neither attach to nor advance it.
   */
  public static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final NodeReadOnlyTrx ownerTrx, final PathSummaryReader pathSummary,
      final StorageEngineWriter storageEngineWriter, final long expectedRows) {
    return begin(indexDef, resourceKey, ownerTrx, pathSummary, storageEngineWriter, expectedRows,
        DEFAULT_ROW_GROUP_PUBLISHER);
  }

  /** Internal publication-injected form used by focused storage-failure coverage. */
  static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final PathSummaryReader pathSummary, final StorageEngineWriter storageEngineWriter,
      final long expectedRows, final RowGroupPublisher rowGroupPublisher) {
    return begin(indexDef, resourceKey, storageEngineWriter, pathSummary, storageEngineWriter,
        expectedRows, rowGroupPublisher);
  }

  /** Owner-bound publication-injected form used by focused lifecycle and storage-failure coverage. */
  static ProjectionBulkLoad begin(final IndexDef indexDef, final String resourceKey,
      final Object ownerToken, final PathSummaryReader pathSummary,
      final StorageEngineWriter storageEngineWriter, final long expectedRows,
      final RowGroupPublisher rowGroupPublisher) {
    if (!indexDef.isProjectionIndex()) {
      throw new IllegalArgumentException(
          "ProjectionBulkLoad requires an IndexType.PROJECTION IndexDef; got " + indexDef.getType());
    }
    final String key = keyOf(resourceKey, indexDef.getID());
    final ProjectionBulkLoad load = new ProjectionBulkLoad(indexDef, key, ownerToken, pathSummary,
        rowGroupPublisher);
    load.builder.setExpectedRows(expectedRows);
    final ProjectionBulkLoad previous = ACTIVE.putIfAbsent(key, load);
    if (previous != null) {
      throw new IllegalStateException(
          "A projection bulk load is already active for " + key + " — finish or abort it before starting another");
    }
    try {
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      // begin() is an explicit full build boundary. Clear every prior positive storage slot and
      // sparse negative record locator before publishing the fail-closed tombstone for the load.
      storage.resetTree();
      storage.putBlob(0, ProjectionIndexMetadata.staleTombstone().serialize());
      return load;
    } catch (final Throwable failure) {
      // The registry entry is already visible. A failure to establish the tombstone must retire it
      // before escaping, or a retry sees a phantom "already loading" build with no fail-closed slot.
      load.poisonAfterFailure(failure);
      throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
    }
  }

  /** The bulk load armed for this definition, or {@code null} when the resource is not being loaded. */
  public static @Nullable ProjectionBulkLoad active(final String resourceKey, final int indexDefId) {
    return ACTIVE.get(keyOf(resourceKey, indexDefId));
  }

  /** Resolve an armed load only for its exact owning write transaction. */
  public static @Nullable ProjectionBulkLoad active(final String resourceKey, final int indexDefId,
      final NodeReadOnlyTrx ownerTrx) {
    final ProjectionBulkLoad load = ACTIVE.get(keyOf(resourceKey, indexDefId));
    return load != null && load.ownerToken == ownerTrx ? load : null;
  }

  /** Whether any bulk load is armed at all — one map read on the listener's construction path. */
  public static boolean anyActive() {
    return !ACTIVE.isEmpty();
  }

  /** Drop the load without finishing it; slot 0 keeps the tombstone, so readers stay on the generic path. */
  public synchronized void abort() {
    if (finished) {
      return;
    }
    // Poison before any cleanup that can itself fail. A concurrent/re-entrant caller can never see
    // a resumable build after publication failed, and repeated listener cleanup is a no-op.
    finished = true;
    ACTIVE.remove(key, this);
    try {
      bloomChunks.release();
    } finally {
      try {
        setSummaries.release();
      } finally {
        try {
          builder.releaseTransientState();
        } finally {
          storage = null;
        }
      }
    }
  }

  /** Whether the declared root path selects array elements — see {@link #arrayElementRoot}. */
  public boolean isArrayElementRoot() {
    return arrayElementRoot;
  }

  /**
   * Whether the build is closed — {@link #finish} ran, or {@link #abort} did. From this point the
   * projection is an ordinary maintained index and the listener hands itself back to the dirty-set
   * maintenance path; see {@code ProjectionIndexChangeListener#activeBulkLoad}.
   */
  public boolean isFinished() {
    return finished;
  }

  /** The record whose subtree is currently being shredded, or {@code -1}. */
  public long currentRecordKey() {
    return currentRecordKey;
  }

  /** Whether {@code nodeKey} is a known record-set array instance. */
  public boolean isArrayRootInstance(final long nodeKey) {
    return arrayRootInstances.contains(nodeKey);
  }

  /** Remember an array instance whose elements are records. */
  public void noteArrayRootInstance(final long nodeKey) {
    arrayRootInstances.add(nodeKey);
  }

  /**
   * Note that {@code recordKey} is the record the shredder is now writing into. A change of record
   * CLOSES the previous one — node keys are monotone and a shred visits records in document order, so
   * a record can be extracted the moment a later one begins.
   *
   * <p>
   * Fails loudly on a non-monotone record key. This is an explicit append-only contract, not a
   * heuristic: an out-of-order record means the transaction is not a bulk load (an update reached back
   * into an already-extracted record), and continuing would silently drop that record's new state.
   */
  public void observeRecord(final long recordKey) {
    if (recordKey == currentRecordKey) {
      return;
    }
    if (finished) {
      // The build is closed and its metadata written; a record arriving now would be accepted into a
      // batch nothing will ever drain, and the index would be short that row with no sign of it. The
      // ordinary maintenance path takes over from the next transaction epoch, which is where a
      // post-load insert belongs.
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key
          + " saw record " + recordKey + " after its load-time build was finished. Records written after the "
          + "load's final commit belong to ordinary incremental maintenance, which the listener hands over to "
          + "automatically — reaching this means a listener kept feeding a closed build, and the record would "
          + "have been accepted into a batch nothing will ever drain.");
    }
    if (recordKey <= lastClosedRecordKey) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key
          + " is being built by the LOAD-TIME path, which is append-only: it extracts each record once, when a "
          + "later record begins, so it cannot revisit one. Record " + recordKey + " was touched after record "
          + lastClosedRecordKey + " had already been extracted, which means this transaction is not a "
          + "document-order bulk load (an update reached back into an already-indexed record). Two ways "
          + "forward: (1) load the data with no declared projection and build the index afterwards with "
          + "jn:create-projection-index, which walks the finished resource and handles any write order; or "
          + "(2) keep the load append-only and make these updates AFTER its final commit, where ordinary "
          + "incremental maintenance handles them.");
    }
    if (currentRecordKey >= 0) {
      completedRecordKeys.add(currentRecordKey);
      lastClosedRecordKey = currentRecordKey;
      diagClosed++;
    }
    currentRecordKey = recordKey;
  }

  /**
   * Extract every record closed since the last drain into the builder. Runs at pre-commit, where the
   * cursor may be moved: doing it inside a change notification would move the cursor out from under the
   * shredder mid-insert.
   */
  public void drain(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx) {
    if (finished || completedRecordKeys.isEmpty()) {
      return;
    }
    long savedNodeKey = -1L;
    boolean restoreCursor = false;
    Throwable primaryFailure = null;
    try {
      // Acquisition can allocate/prepare the HOT root and therefore fail. Keep it under the same
      // poison boundary as extraction and publication so no ACTIVE load survives a writer fault.
      this.storage = ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      savedNodeKey = rtx.getNodeKey();
      restoreCursor = true;
      // The summary is still growing: a field whose first occurrence is in this batch has only just
      // acquired a path class, and an extractor that predates it would record the field ABSENT.
      builder.refreshFieldPaths(pathSummary);
      diagDrains++;
      for (int i = 0; i < completedRecordKeys.size(); i++) {
        if (!appendRecord(rtx, completedRecordKeys.getLong(i))) {
          diagExtractFailures++;
        }
      }
    } catch (final Throwable failure) {
      primaryFailure = failure;
      poisonAfterFailure(failure);
      throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(failure);
    } finally {
      Throwable cleanupFailure = null;
      try {
        completedRecordKeys.clear();
        if (restoreCursor && !rtx.moveTo(savedNodeKey)) {
          ((NodeCursor) rtx).moveToDocumentRoot();
        }
      } catch (final Throwable failure) {
        cleanupFailure = failure;
      } finally {
        this.storage = null;
      }
      if (cleanupFailure != null) {
        if (primaryFailure != null) {
          if (cleanupFailure != primaryFailure) {
            primaryFailure.addSuppressed(cleanupFailure);
          }
        } else {
          poisonAfterFailure(cleanupFailure);
          throw ProjectionBulkLoad.<RuntimeException>rethrowUnchecked(cleanupFailure);
        }
      }
    }
  }

  /** Retire a partially published load while preserving the failure that caused the retirement. */
  private void poisonAfterFailure(final Throwable primaryFailure) {
    try {
      abort();
    } catch (final Throwable cleanupFailure) {
      if (cleanupFailure != primaryFailure) {
        primaryFailure.addSuppressed(cleanupFailure);
      }
    }
  }

  /** Rethrow any callback/extraction failure without widening the public drain signature. */
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> T rethrowUnchecked(final Throwable failure) throws T {
    throw (T) failure;
  }

  /**
   * Close the load: extract the last open record, drain the builder, persist the resource-wide
   * dictionaries, and write the metadata, fingerprint blocks and fences through the SAME
   * {@code finishPersist} the post-pass build uses — which is what makes the two indexes equivalent.
   *
   * <p>
   * The record-set shape is validated here rather than at declaration, because at declaration the
   * resource is empty and the root path has no path class yet: a root that turns out to select NESTED
   * record sets would have let an inner record's fields overwrite an outer row's columns, so it fails
   * loudly now, with the tombstone still in place.
   */
  public void finish(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummary,
      final NodeReadOnlyTrx rtx, final int buildRevision) {
    if (finished) {
      return;
    }
    if (currentRecordKey >= 0) {
      completedRecordKeys.add(currentRecordKey);
      lastClosedRecordKey = currentRecordKey;
      currentRecordKey = -1L;
    }
    // EVERYTHING inside the try, the shape and row-count checks included: a build that fails one of
    // them is dead either way, and its registry entry has to be retired regardless or the definition
    // stays "already loading" for the life of the JVM and a retry cannot even arm. Failing before
    // finishPersist leaves slot 0's tombstone in place, which is exactly the state a failed load
    // should leave behind.
    try {
      assertRecordSetShape(pathSummary);
      drain(storageEngineWriter, pathSummary, rtx);
      assertEveryRecordEmitted(rtx);
      this.storage = ProjectionIndexHOTStorage.forBulkBuild(storageEngineWriter, indexDef.getID());
      builder.finishStreaming();
      final byte[] columnKinds = builder.columnKinds();
      bloomChunks.finishChunks(storage, firstKeys.size(), columnKinds);
      final long[] valueDictionaryHeaderKeys =
          ProjectionIndexBuilder.flushValueDictionaries(builder.globalDictionaries(), storageEngineWriter);
      // priorRowGroupCount 0: a bulk load owns a sub-tree it created itself, so there is nothing above
      // the new leaf count to tombstone.
      ProjectionIndexBuilder.finishPersist(indexDef, storage, firstKeys, lastKeys, 0, buildRevision,
          columnKinds, setSummaries, valueDictionaryHeaderKeys, bloomChunks);
    } finally {
      try {
        bloomChunks.release();
      } finally {
        try {
          setSummaries.release();
        } finally {
          try {
            builder.releaseTransientState();
          } finally {
            this.storage = null;
            finished = true;
            ACTIVE.remove(key, this);
          }
        }
      }
    }
  }

  /** Rows appended so far — test and diagnostic observability. */
  public long rowsEmitted() {
    return builder.rowsEmitted();
  }

  /**
   * Prove the load emitted a row for EVERY record, by counting the record-set arrays' children.
   *
   * <p>
   * This is the invariant that makes per-notification record attribution safe to rely on. Attribution
   * decides which node belongs to which record from change notifications alone; if it ever missed a
   * record root, the index would simply be short a row — no exception, no wrong-looking output, just a
   * query that quietly answers over fewer records than the resource holds. An array node already
   * knows its child count, so the check costs one node read per record set and turns that class of bug
   * from silent into loud.
   *
   * <p>
   * Only the array-rooted shape is checkable this way, which is also the only shape whose attribution
   * takes the kind-filtered fast path; a non-array record root is resolved by the full ancestor walk
   * for every notification and has no comparable counter to check against.
   */
  private void assertEveryRecordEmitted(final NodeReadOnlyTrx rtx) {
    if (arrayRootInstances.isEmpty()) {
      return;
    }
    final long savedNodeKey = rtx.getNodeKey();
    long expected = 0;
    try {
      for (final LongIterator it = arrayRootInstances.iterator(); it.hasNext();) {
        final long arrayKey = it.nextLong();
        if (!rtx.moveTo(arrayKey)) {
          throw new IllegalStateException("Projection bulk load for " + key + " cannot re-read record-set array "
              + arrayKey + " at the end of the load");
        }
        expected += rtx.getChildCount();
      }
    } finally {
      if (!rtx.moveTo(savedNodeKey)) {
        ((NodeCursor) rtx).moveToDocumentRoot();
      }
    }
    if (expected != builder.rowsEmitted()) {
      throw new IllegalStateException("Projection index " + indexDef.getID() + " on " + key + " emitted "
          + builder.rowsEmitted() + " rows for " + expected + " records: the load-time build's per-notification "
          + "record attribution missed records, and the index would have answered over fewer records than the "
          + "resource holds. Nothing was published — slot 0 still holds the tombstone, so the resource's data is "
          + "intact and its queries fall back to the generic pipeline. Build the index with "
          + "jn:create-projection-index over the loaded resource, which walks it rather than attributing "
          + "notifications." + diagnostics());
    }
  }

  /**
   * Reject ancestor/descendant overlap only among matches of the declared ROOT path, mirroring the
   * walking builder's up-front guard. Projection COLUMN paths are not compared and may descend to
   * arbitrary and differing depths below a root. This check is reachable only after the resource has
   * data, so the load-time builder runs it at the end of the load.
   */
  private void assertRecordSetShape(final PathSummaryReader pathSummary) {
    final Path<QNm> rootPath = indexDef.getProjectionRootPath();
    final LongSet rootPcrs = pathSummary.getPCRsForPaths(Set.of(rootPath));
    if (rootPcrs.isEmpty()) {
      return; // the record set never materialised — the truthful empty projection
    }
    final LongOpenHashSet roots = new LongOpenHashSet(rootPcrs.size());
    for (final LongIterator it = rootPcrs.iterator(); it.hasNext();) {
      roots.add(it.nextLong());
    }
    final long saved = pathSummary.getNodeKey();
    try {
      for (final LongIterator it = roots.iterator(); it.hasNext();) {
        final long root = it.nextLong();
        if (!pathSummary.moveTo(root)) {
          continue;
        }
        while (pathSummary.moveToParent()) {
          final long pk = pathSummary.getNodeKey();
          if (pk <= 0) {
            break;
          }
          if (roots.contains(pk)) {
            throw new IllegalStateException("Projection ROOT path '" + rootPath
                + "' resolves to overlapping nested root matches (matched pathNodeKey " + root
                + " lies below matched root pathNodeKey " + pk + "). Only matches of the declared ROOT path are "
                + "compared here; projection COLUMN paths may descend to arbitrary and differing depths below each "
                + "root. Self-nested root matches are not supported; declare a more specific root path");
          }
        }
      }
    } finally {
      pathSummary.moveTo(saved);
    }
  }

  private ProjectionIndexHOTStorage currentStorage() {
    final ProjectionIndexHOTStorage current = storage;
    if (current == null) {
      throw new IllegalStateException(
          "Projection bulk load for " + key + " emitted a leaf outside a drain — no storage is bound");
    }
    return current;
  }

  private boolean appendRecord(final NodeReadOnlyTrx rtx, final long recordKey) {
    if (rtx instanceof final JsonNodeReadOnlyTrx jsonRtx) {
      return builder.appendRecord(jsonRtx, recordKey);
    }
    if (rtx instanceof final XmlNodeReadOnlyTrx xmlRtx) {
      return builder.appendRecord(xmlRtx, recordKey);
    }
    throw new IllegalArgumentException("projection bulk load requires a JSON or XML node transaction");
  }

  /** The declared field types, for callers that mirror the definition's column shape. */
  List<Type> fieldTypes() {
    return indexDef.getProjectionFieldTypes();
  }

  /** Column kinds the build settled on — used by tests to compare the two build routes. */
  public byte[] columnKinds() {
    return builder.columnKinds();
  }

  /** Leaf count so far — used by tests to compare the two build routes. */
  public int rowGroupCount() {
    return firstKeys.size();
  }

  /** Defensive copy of the per-leaf first record keys — test observability. */
  public long[] leafFirstKeys() {
    return firstKeys.toLongArray();
  }

  /** Defensive copy of the per-leaf last record keys — test observability. */
  public long[] leafLastKeys() {
    return lastKeys.toLongArray();
  }

  /** Drop every armed load — test isolation only. */
  public static void clearActive() {
    for (final ProjectionBulkLoad load : ACTIVE.values()) {
      load.abort();
    }
    ACTIVE.clear();
  }

  /** Names of the definitions currently loading — diagnostics. */
  public static List<String> activeKeys() {
    return new ArrayList<>(ACTIVE.keySet());
  }
}
