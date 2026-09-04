package io.sirix.access.trx.node;

import io.sirix.access.DatabaseType;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.ChangeListener;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.PathNodeKeyChangeListener;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.SearchMode;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.brackit.query.util.serialize.SubtreePrinter;
import org.jspecify.annotations.Nullable;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.cas.CASIndex;
import io.sirix.index.name.NameFilter;
import io.sirix.index.name.NameIndex;
import io.sirix.index.path.PCRCollector;
import io.sirix.index.path.PathFilter;
import io.sirix.index.path.PathIndex;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.vector.VectorIndex;
import io.sirix.index.vector.VectorSearchResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public abstract class AbstractIndexController<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements IndexController<R, W> {

  private static final ChangeListener[] NO_CHANGE_LISTENERS = new ChangeListener[0];

  private static final PathNodeKeyChangeListener[] NO_PRIMITIVE_LISTENERS = new PathNodeKeyChangeListener[0];

  /**
   * The index types.
   */
  protected final Indexes indexes;

  /**
   * Set of {@link ChangeListener}.
   */
  private final Set<ChangeListener> listeners;

  /**
   * Set of primitive listeners for allocation-conscious hot-path notifications.
   */
  private final Set<PathNodeKeyChangeListener> primitiveListeners;

  /**
   * Dense snapshots used by the per-node notification path. Index listeners change only at
   * transaction/index lifecycle boundaries; iterating the backing {@link HashSet} for every JSON node
   * would allocate one iterator per notification.
   */
  private ChangeListener[] listenerSnapshot = NO_CHANGE_LISTENERS;

  private PathNodeKeyChangeListener[] primitiveListenerSnapshot = NO_PRIMITIVE_LISTENERS;

  /**
   * Used to provide path indexes.
   */
  protected final PathIndex<?, ?> pathIndex;

  /**
   * Used to provide CAS indexes.
   */
  protected final CASIndex<?, ?, R> casIndex;

  /**
   * Used to provide name indexes.
   */
  protected final NameIndex<?, ?> nameIndex;

  /**
   * Used to provide vector indexes (may be null for XML controllers).
   */
  protected final @Nullable VectorIndex vectorIndex;

  /**
   * Cached capabilities for hot-path checks.
   */
  private boolean hasPathIndex;
  private boolean hasCASIndex;
  private boolean hasNameIndex;
  private boolean hasVectorIndex;
  private boolean hasValidTimeIndex;
  private boolean hasProjectionIndex;

  /**
   * Constructor.
   *
   * @param indexes the index definitions
   * @param listeners the set of listeners
   * @param pathIndex the path index manager
   * @param casIndex the CAS index manager
   * @param nameIndex the name index manager
   * @param vectorIndex the vector index manager (may be null for XML controllers)
   */
  public AbstractIndexController(final Indexes indexes, final Set<ChangeListener> listeners,
      final PathIndex<?, ?> pathIndex, final CASIndex<?, ?, R> casIndex, final NameIndex<?, ?> nameIndex,
      final @Nullable VectorIndex vectorIndex) {
    this.indexes = indexes;
    this.listeners = listeners;
    this.primitiveListeners = new HashSet<>(listeners.size());
    for (final ChangeListener listener : listeners) {
      if (listener instanceof final PathNodeKeyChangeListener primitiveListener) {
        primitiveListeners.add(primitiveListener);
      }
    }
    refreshListenerSnapshots();
    this.pathIndex = pathIndex;
    this.casIndex = casIndex;
    this.nameIndex = nameIndex;
    this.vectorIndex = vectorIndex;
    refreshIndexCapabilities();
  }

  final void refreshIndexCapabilities() {
    hasPathIndex = false;
    hasCASIndex = false;
    hasNameIndex = false;
    hasVectorIndex = false;
    hasValidTimeIndex = false;
    hasProjectionIndex = false;
    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      updateIndexCapability(indexDef.getType());
    }
  }

  @Override
  public boolean containsIndex(final IndexType type) {
    for (final IndexDef indexDef : indexes.getIndexDefs()) {
      if (indexDef.getType() == type)
        return true;
    }
    return false;
  }

  @Override
  public boolean hasPathIndex() {
    return hasPathIndex;
  }

  @Override
  public boolean hasCASIndex() {
    return hasCASIndex;
  }

  @Override
  public boolean hasNameIndex() {
    return hasNameIndex;
  }

  @Override
  public boolean hasVectorIndex() {
    return hasVectorIndex;
  }

  @Override
  public boolean hasValidTimeIndex() {
    return hasValidTimeIndex;
  }

  @Override
  public boolean hasProjectionIndex() {
    return hasProjectionIndex;
  }

  @Override
  public VectorSearchResult searchVectorIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final float[] query, final int k) {
    if (vectorIndex == null) {
      throw new IllegalStateException("This document does not support vector indexes.");
    }
    return vectorIndex.searchKnn(storageEngineReader, indexDef, query, k);
  }

  @Override
  public VectorSearchResult searchVectorIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final float[] query, final int k, final int efSearch) {
    if (vectorIndex == null) {
      throw new IllegalStateException("This document does not support vector indexes.");
    }
    return vectorIndex.searchKnn(storageEngineReader, indexDef, query, k, efSearch);
  }

  @Override
  public void deleteVectorEntry(final StorageEngineWriter storageEngineWriter, final IndexDef indexDef,
      final long hnswNodeKey) {
    if (vectorIndex == null) {
      throw new IllegalStateException("This document does not support vector indexes.");
    }
    vectorIndex.deleteVector(storageEngineWriter, indexDef, hnswNodeKey);
  }

  @Override
  public Indexes getIndexes() {
    return indexes;
  }

  @Override
  public void serialize(final OutputStream out) {
    try {
      final SubtreePrinter serializer = new SubtreePrinter(new PrintStream(requireNonNull(out)));
      serializer.print(indexes.materialize());
      serializer.end();
    } catch (final DocumentException e) {
      throw new SirixRuntimeException(e);
    }
  }

  @Override
  public void notifyChange(final ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].listen(type, node, pathNodeKey);
    }
  }

  @Override
  public void notifyChange(final ChangeType type, final long nodeKey, final NodeKind nodeKind, final long pathNodeKey,
      final @Nullable QNm name, final @Nullable Str value) {
    final PathNodeKeyChangeListener[] activeListeners = primitiveListenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].listen(type, nodeKey, nodeKind, pathNodeKey, name, value);
    }
  }

  @Override
  public void notifyChange(final ChangeType type, final long nodeKey, final NodeKind nodeKind, final long parentKey,
      final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    final PathNodeKeyChangeListener[] activeListeners = primitiveListenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].listen(type, nodeKey, nodeKind, parentKey, pathNodeKey, name, value);
    }
  }

  @Override
  public IndexController<R, W> createIndexListeners(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    requireNonNull(indexDefs);
    requireNonNull(nodeWriteTrx);
    validateListenerRegistrations(indexDefs, nodeWriteTrx);
    // Save for upcoming modifications.
    for (final IndexDef indexDef : indexDefs) {
      indexes.add(indexDef);
      updateIndexCapability(indexDef.getType());
      switch (indexDef.getType()) {
        case PATH -> addListener(
            createPathIndexListener(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
        case CAS -> addListener(
            createCASIndexListener(nodeWriteTrx.getStorageEngineWriter(), nodeWriteTrx.getPathSummary(), indexDef));
        case NAME -> addListener(createNameIndexListener(nodeWriteTrx.getStorageEngineWriter(), indexDef));
        // VECTOR is an explicit-API index: its HNSW entries cannot be derived from ordinary document
        // notifications, and registering a no-op listener would advertise maintenance that never
        // happens. Existing catalogued VECTOR definitions remain searchable and explicitly mutable;
        // validateListenerRegistrations prevents this listener API from cataloguing a new one.
        case VECTOR -> {
        }
        case VALIDTIME -> addListener(requireNonNull(createValidTimeIndexListener(nodeWriteTrx, indexDef),
            "A supported VALIDTIME definition must have an incremental change listener"));
        case PROJECTION -> {
          // Rebinding can occur after rollback/revert or after drop reconstructs the complete listener
          // set. Replace the same definition's old transaction-scoped listener so no closed writer
          // or path-summary handle survives into the new epoch.
          removeProjectionListenerFor(indexDef.getID());
          addListener(requireNonNull(createProjectionIndexListener(nodeWriteTrx, indexDef),
              "Projection-index controllers must provide an incremental change listener"));
        }
        default -> {
        }
      }
    }

    refreshListenerSnapshots();

    return this;
  }

  /**
   * Validate definitions before the standard document-index creation path mutates either the
   * catalogue or an index tree.
   *
   * <p>
   * Vector embeddings are caller-supplied data, not a value Sirix can reconstruct from a generic
   * document-node change. They therefore have a separate explicit lifecycle through
   * {@link VectorIndex}; accepting a VECTOR definition here used to catalogue an empty HNSW index and
   * bind a listener whose methods were deliberate no-ops.
   * </p>
   *
   * @param indexDefs definitions requested through {@link #createIndexes(Set, NodeTrx)}
   * @param nodeWriteTrx transaction whose physical index slots are validated
   * @throws UnsupportedOperationException if a definition has no correct document-listener lifecycle
   */
  protected final void validateNewIndexDefinitions(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    requireNonNull(indexDefs);
    requireNonNull(nodeWriteTrx);

    // Validate the whole request before consulting or mutating any physical container. In
    // particular, a mixed request containing VECTOR must fail atomically even if another
    // definition happens to precede it in the set's iteration order.
    for (final IndexDef indexDef : indexDefs) {
      requireNonNull(indexDef);
      if (indexDef.isVectorIndex()) {
        throw unsupportedVectorLifecycle();
      }
    }
    validateSupportedIndexLifecycles(indexDefs, nodeWriteTrx);
    if (indexDefs.isEmpty()) {
      return;
    }

    boolean needsPhysicalProbe = false;
    for (final IndexDef indexDef : indexDefs) {
      final IndexDef existingDefinition = indexes.getIndexDef(indexDef.getID(), indexDef.getType());
      if (existingDefinition == null) {
        needsPhysicalProbe = true;
      } else {
        requireSameDefinition(existingDefinition, indexDef);
      }
    }
    if (!needsPhysicalProbe) {
      return;
    }

    final StorageEngineWriter storageEngineWriter = requireNonNull(nodeWriteTrx.getStorageEngineWriter());
    final var revisionRoot = storageEngineWriter.getActualRevisionRootPage();
    for (final IndexDef indexDef : indexDefs) {
      final IndexType indexType = indexDef.getType();
      final IndexDef existingDefinition = indexes.getIndexDef(indexDef.getID(), indexType);
      if (existingDefinition != null) {
        continue;
      }

      final boolean physicalIdWasInitialized = switch (indexType) {
        case PATH -> storageEngineWriter.getPathPage(revisionRoot).isIndexInitialized(indexDef.getID());
        case CAS -> storageEngineWriter.getCASPage(revisionRoot).isIndexInitialized(indexDef.getID());
        case NAME ->
          storageEngineWriter.getNamePage(revisionRoot)
                             .isSecondaryNameIndexInitialized(databaseType(storageEngineWriter), indexDef.getID());
        case PROJECTION ->
          storageEngineWriter.getProjectionIndexPage(revisionRoot).isIndexInitialized(indexDef.getID());
        case VALIDTIME -> storageEngineWriter.getValidTimeIndexPage(revisionRoot).isIndexInitialized(indexDef.getID());
        default -> false;
      };
      if (physicalIdWasInitialized) {
        throw new IllegalStateException("Cannot create " + indexType + " index " + indexDef.getID()
            + ": that physical id was already initialized by a dropped or historical index; allocate a fresh id");
      }
    }
  }

  private static DatabaseType databaseType(final StorageEngineReader storageEngineReader) {
    final ResourceSession<?, ?> resourceSession = storageEngineReader.getResourceSession();
    if (resourceSession instanceof JsonResourceSession) {
      return DatabaseType.JSON;
    }
    if (resourceSession instanceof XmlResourceSession) {
      return DatabaseType.XML;
    }
    throw new IllegalStateException("Cannot determine database type from resource session " + resourceSession);
  }

  /** Reject unsupported or definition-changing listener registrations before catalogue mutation. */
  private void validateListenerRegistrations(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    for (final IndexDef indexDef : indexDefs) {
      requireNonNull(indexDef);
      final IndexDef existingDefinition = indexes.getIndexDef(indexDef.getID(), indexDef.getType());
      if (existingDefinition != null) {
        requireSameDefinition(existingDefinition, indexDef);
      } else if (indexDef.isVectorIndex()) {
        throw unsupportedVectorLifecycle();
      }
    }
    validateSupportedIndexLifecycles(indexDefs, nodeWriteTrx);
  }

  private static void requireSameDefinition(final IndexDef existingDefinition, final IndexDef requestedDefinition) {
    if (!existingDefinition.hasSameDefinition(requestedDefinition)) {
      throw new IllegalStateException("Cannot reuse " + requestedDefinition.getType() + " index "
          + requestedDefinition.getID() + " with a different definition; allocate a fresh physical id");
    }
  }

  private static UnsupportedOperationException unsupportedVectorLifecycle() {
    return new UnsupportedOperationException("VECTOR indexes cannot be created through the standard document "
        + "index lifecycle because document updates do not supply or delete embeddings. Use VectorIndex.createIndex, "
        + "insertVector, and deleteVector explicitly.");
  }

  /**
   * Validate resource-specific index lifecycles before a definition can reach the catalogue.
   *
   * <p>
   * VALIDTIME is JSON-only and requires an explicit resource configuration. JSON overrides this hook
   * to validate that configuration; every other controller rejects the definition.
   * </p>
   */
  protected void validateSupportedIndexLifecycles(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isValidTimeIndex()) {
        throw new UnsupportedOperationException(
            "VALIDTIME indexes are supported only for JSON resources with ValidTimeConfig");
      }
    }
  }

  /**
   * Create the projection index's incremental change listener.
   *
   * <p>
   * Every concrete resource controller must implement this lifecycle explicitly. There is no
   * nullable/no-maintenance fallback: cataloguing a projection without a listener would leave the
   * persisted index silently stale after the next document mutation.
   * </p>
   *
   * @param nodeWriteTrx the write transaction
   * @param indexDef the projection-index definition
   * @return the non-null incremental listener
   */
  protected abstract ChangeListener createProjectionIndexListener(W nodeWriteTrx, IndexDef indexDef);

  /**
   * Per-transaction cache entry for a wtx-visible decoded projection handle: valid only for the SAME
   * listener instance (listeners are rebound per transaction epoch) at the SAME maintenance epoch
   * (any new dirty change, invalidation, or apply pass bumps it).
   */
  private record UncommittedHandle(ProjectionIndexChangeListener listener, long epoch,
      ProjectionIndexRegistry.Handle handle) {
  }

  /** Decoded wtx handles per definition id; cleared with the listeners. */
  private final Int2ObjectOpenHashMap<UncommittedHandle> uncommittedHandles = new Int2ObjectOpenHashMap<>();

  private ProjectionIndexRegistry.@Nullable Handle uncommittedHandleFor(final StorageEngineReader reader,
      final IndexDef def) {
    final ProjectionIndexChangeListener listener = projectionListenerFor(def.getID());
    if (listener != null) {
      final UncommittedHandle cached = uncommittedHandles.get(def.getID());
      if (cached != null && cached.listener() == listener && cached.epoch() == listener.maintenanceEpoch()) {
        return cached.handle();
      }
    }
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.loadUncommitted(reader, def);
    if (handle != null && listener != null) {
      uncommittedHandles.put(def.getID(), new UncommittedHandle(listener, listener.maintenanceEpoch(), handle));
    }
    return handle;
  }

  private @Nullable ProjectionIndexChangeListener projectionListenerFor(final int indexDefId) {
    final PathNodeKeyChangeListener[] activeListeners = primitiveListenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      final PathNodeKeyChangeListener listener = activeListeners[i];
      if (listener instanceof final ProjectionIndexChangeListener projectionListener
          && projectionListener.indexDefId() == indexDefId) {
        return projectionListener;
      }
    }
    return null;
  }

  /** Remove any bound projection change listener for this definition id (from both listener sets). */
  private void removeProjectionListenerFor(final int indexDefId) {
    listeners.removeIf(
        listener -> listener instanceof final ProjectionIndexChangeListener p && p.indexDefId() == indexDefId);
    primitiveListeners.removeIf(
        listener -> listener instanceof final ProjectionIndexChangeListener p && p.indexDefId() == indexDefId);
  }

  @Override
  public IndexController<R, W> dropIndexes(final Set<IndexDef> indexDefs, final W nodeWriteTrx) {
    requireNonNull(nodeWriteTrx);
    if (indexDefs.isEmpty()) {
      return this;
    }

    // A load-time projection owns process-wide state outside the transaction's page log. Dropping its
    // definition removes the only listener that can finish or abort that state, so retire the exact
    // listener owner before touching the catalogue. Do not abort the remaining projection listeners:
    // dropIndexes rebinds those below, and a successful intermediate commit may deliberately keep
    // their streaming builders alive across listener epochs.
    for (final IndexDef indexDef : indexDefs) {
      if (indexDef.isProjectionIndex()) {
        final ProjectionIndexChangeListener projectionListener = projectionListenerFor(indexDef.getID());
        if (projectionListener != null) {
          projectionListener.transactionAborted();
        }
      }
    }

    // 1. Remove from the catalogue (marks it dirty so the reduced catalogue is persisted on commit).
    // Match on the FULL IndexDef (id + type) — index ids are only unique within a type, so a
    // remove-by-id would also drop a same-id index of another type (e.g. a CAS index with id 0).
    for (final IndexDef indexDef : indexDefs) {
      indexes.removeIndex(indexDef);
    }

    // 2. Re-derive listeners + capability flags from the REMAINING definitions. The dropped index's
    // listener is discarded (so it is not maintained for the rest of this transaction), and the
    // has*Index() fast-path flags are recomputed from scratch.
    clearChangeListeners();
    hasPathIndex = false;
    hasCASIndex = false;
    hasNameIndex = false;
    hasVectorIndex = false;
    hasValidTimeIndex = false;
    hasProjectionIndex = false;

    // createIndexListeners re-adds each remaining def (idempotent on the Set), re-sets the capability
    // flags, and rebinds the listeners for this write transaction.
    createIndexListeners(indexes.getIndexDefs(), nodeWriteTrx);

    return this;
  }

  /**
   * Create a valid-time interval index listener. Unsupported controllers reject the definition in
   * {@link #validateSupportedIndexLifecycles(Set, NodeTrx)} before this method can be reached; JSON
   * overrides both lifecycle methods.
   *
   * @param nodeWriteTrx the write transaction
   * @param indexDef the (VALIDTIME) index definition
   * @return a non-null change listener for a validated VALIDTIME definition
   */
  protected @Nullable ChangeListener createValidTimeIndexListener(final W nodeWriteTrx, final IndexDef indexDef) {
    return null;
  }

  @Override
  public void clearChangeListeners() {
    listeners.clear();
    primitiveListeners.clear();
    listenerSnapshot = NO_CHANGE_LISTENERS;
    primitiveListenerSnapshot = NO_PRIMITIVE_LISTENERS;
    // A rollback/revert may have reloaded this cached controller's catalogue before listener
    // rebinding. Derive the fast-path flags from that authoritative catalogue so an aborted index
    // definition cannot keep document notifications enabled for a listener that no longer exists.
    refreshIndexCapabilities();
    // Wtx handle cache entries are bound to listener instances — clearing
    // the listeners invalidates them (and releases the decoded payloads).
    uncommittedHandles.clear();
  }

  @Override
  public void applyPendingIndexMaintenance(final boolean finalCommit) {
    // Uniform listener lifecycle: every listener gets the commit-time hook;
    // eagerly-maintained index types (PATH/CAS/NAME/valid-time) keep the
    // default no-op, batching types (projection) apply their pending work.
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].beforeCommit(finalCommit);
    }
  }

  @Override
  public void notifyBeforePageFlush() {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].beforePageFlush();
    }
  }

  @Override
  public void notifyTransactionAbort() {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].transactionAborted();
    }
    // Wtx-serving handles are tied to the listener's maintenance epoch and may retain decoded
    // payloads from the lineage that is about to disappear.
    uncommittedHandles.clear();
  }

  @Override
  public void notifyStructuralChange() {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].structuralChange();
    }
  }

  @Override
  public void notifyBeforeStructuralChange(final long movedNodeKey) {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].beforeStructuralChange(movedNodeKey);
    }
  }

  @Override
  public void notifyAfterStructuralChange(final long movedNodeKey) {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].afterStructuralChange(movedNodeKey);
    }
  }

  @Override
  public void notifyStructuralChangeAborted(final long movedNodeKey) {
    final ChangeListener[] activeListeners = listenerSnapshot;
    for (int i = 0; i < activeListeners.length; i++) {
      activeListeners[i].structuralChangeAborted(movedNodeKey);
    }
  }

  private void updateIndexCapability(final IndexType type) {
    switch (type) {
      case PATH -> hasPathIndex = true;
      case CAS -> hasCASIndex = true;
      case NAME -> hasNameIndex = true;
      case VECTOR -> hasVectorIndex = true;
      case VALIDTIME -> hasValidTimeIndex = true;
      // Projection maintenance is listener-driven like PATH/CAS/NAME —
      // without this flag the hasAnyPrimitiveIndex() gate on the write hot
      // paths silently drops every notification when a projection is the
      // only index, and its listener never sees the changes.
      case PROJECTION -> hasProjectionIndex = true;
      default -> {
      }
    }
  }

  private void addListener(final ChangeListener listener) {
    listeners.add(listener);
    if (listener instanceof final PathNodeKeyChangeListener primitiveListener) {
      primitiveListeners.add(primitiveListener);
    } else {
      throw new IllegalStateException(
          "Listener does not support primitive change events: " + listener.getClass().getName());
    }
  }

  private void refreshListenerSnapshots() {
    listenerSnapshot = listeners.isEmpty()
        ? NO_CHANGE_LISTENERS
        : listeners.toArray(ChangeListener[]::new);
    primitiveListenerSnapshot = primitiveListeners.isEmpty()
        ? NO_PRIMITIVE_LISTENERS
        : primitiveListeners.toArray(PathNodeKeyChangeListener[]::new);
  }

  private ChangeListener createPathIndexListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return pathIndex.createListener(storageEngineWriter, pathSummaryReader, indexDef);
  }

  private ChangeListener createCASIndexListener(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return casIndex.createListener(storageEngineWriter, pathSummaryReader, indexDef);
  }

  private ChangeListener createNameIndexListener(final StorageEngineWriter storageEngineWriter,
      final IndexDef indexDef) {
    return nameIndex.createListener(storageEngineWriter, indexDef);
  }

  @Override
  public NameFilter createNameFilter(final Set<String> names) {
    final Set<QNm> includes = new HashSet<>(names.size());
    for (final String name : names) {
      // TODO: Prefix/NspURI
      includes.add(new QNm(name));
    }
    return new NameFilter(includes, Collections.emptySet());
  }

  @Override
  public CASFilter createCASFilter(final Set<String> stringPaths, final Atomic key, final SearchMode mode,
      final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(stringPaths.size());
    if (!stringPaths.isEmpty()) {
      for (final String path : stringPaths) {
        paths.add(parsePath(path));
      }
    }
    return new CASFilter(paths, key, mode, pcrCollector);
  }

  protected abstract Path<QNm> parsePath(String path);

  @Override
  public CASFilterRange createCASFilterRange(final Set<String> thePaths, final Atomic min, final Atomic max,
      final boolean incMin, final boolean incMax, final PCRCollector pcrCollector) throws PathException {
    final Set<Path<QNm>> paths = new HashSet<>(thePaths.size());
    if (!thePaths.isEmpty()) {
      for (final String path : thePaths) {
        paths.add(parsePath(path));
      }
    }
    return new CASFilterRange(paths, min, max, incMin, incMax, pcrCollector);
  }

  @Override
  public Iterator<NodeReferences> openPathIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final PathFilter filter) {
    if (pathIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return pathIndex.openIndex(storageEngineReader, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openNameIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final NameFilter filter) {
    if (nameIndex == null) {
      throw new IllegalStateException("This document does not support name indexes.");
    }

    return nameIndex.openIndex(storageEngineReader, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final CASFilter filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support CAS indexes.");
    }

    return casIndex.openIndex(storageEngineReader, indexDef, filter);
  }

  @Override
  public Iterator<NodeReferences> openCASIndex(final StorageEngineReader storageEngineReader, final IndexDef indexDef,
      final CASFilterRange filter) {
    if (casIndex == null) {
      throw new IllegalStateException("This document does not support path indexes.");
    }

    return casIndex.openIndex(storageEngineReader, indexDef, filter);
  }

  @Override
  public ProjectionIndexRegistry.@Nullable Handle openProjectionIndex(final StorageEngineReader storageEngineReader,
      final String[] sourcePath, final String[] requiredFields) {
    // Gate on the CATALOGUE, not the cached capability flag: read-only
    // controllers are populated via Indexes.init() after construction, which
    // never updates the flags — the flag is a write-transaction concept
    // (listener binding), and gating on it made this method's committed
    // branch unconditionally return null on rtx controllers.
    if (indexes.getNrOfIndexDefsWithType(IndexType.PROJECTION) == 0) {
      return null;
    }
    if (storageEngineReader instanceof StorageEngineWriter) {
      // Wtx-visible serving (read-your-writes): bring the leaves up to date
      // with this transaction's changes — the same work its commit would do,
      // an O(1) no-op when nothing is dirty — then read through the
      // transaction log. No SHARED caching (uncommitted state is mutable;
      // caching it under a revision key would poison committed-revision
      // serving), but decoded handles are memoized PER TRANSACTION against
      // the definition listener's maintenance epoch, so repeated analytics
      // over an unchanged state decode once.
      applyPendingIndexMaintenance();
      final IndexDef[] candidates =
          ProjectionIndexCatalog.selectUncommittedCandidateDefs(indexes, sourcePath, requiredFields);
      for (final IndexDef candidate : candidates) {
        final ProjectionIndexRegistry.Handle handle = uncommittedHandleFor(storageEngineReader, candidate);
        if (handle != null) {
          return handle;
        }
      }
      return null;
    }
    // Committed reader — the cached catalog front-end (probe + decoded-leaf
    // tiers keyed by resource and revision).
    final ResourceSession<?, ?> session = storageEngineReader.getResourceSession();
    return ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
        storageEngineReader.getRevisionNumber(), sourcePath, requiredFields);
  }
}
