package io.sirix.index.path.summary;

import io.sirix.access.Utils;
import io.sirix.access.trx.node.NodeFactory;
import io.sirix.api.Axis;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.LevelOrderAxis;
import io.sirix.axis.PostOrderAxis;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.PathKindFilter;
import io.sirix.axis.filter.PathNameFilter;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import javax.xml.namespace.QName;
import java.util.ArrayDeque;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Path summary writer organizing the path classes of a resource.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class PathSummaryWriter<R extends NodeCursor & NodeReadOnlyTrx>
    extends AbstractForwardingPathSummaryReader {

  /**
   * Operation type to determine behavior of path summary updates during {@code setQName(QName)} and
   * the move-operations.
   */
  public enum OPType {
    /**
     * Move from and to is on the same level (before and after the move, the node has the same parent).
     */
    MOVED_ON_SAME_LEVEL,

    /**
     * Move from and to is not on the same level (before and after the move, the node has a different
     * parent).
     */
    MOVED,

    /**
     * A new {@link QName} is set.
     */
    SETNAME,
  }

  /**
   * Determines if a path subtree must be deleted or not.
   */
  private enum RemoveSubtreePath {
    /**
     * Yes, it must be deleted.
     */
    YES,

    /**
     * No, it must not be deleted.
     */
    NO
  }

  /**
   * Sirix {@link StorageEngineWriter}.
   */
  private final StorageEngineWriter storageEngineWriter;

  /**
   * Sirix {@link PathSummaryReader}.
   */
  private final PathSummaryReader pathSummaryReader;

  /**
   * Sirix {@link NodeFactory} to create new nodes.
   */
  private final NodeFactory nodeFactory;

  /**
   * The read-only trx.
   */
  private final R nodeRtx;

  /**
   * Determines if number of children is stored or not.
   */
  private final boolean storeChildCount;

  /**
   * Determines if per-path value statistics (count, nullCount, sum, min, max, HLL) are
   * maintained on PathSummary nodes. Gated once at construction to make the
   * {@code recordValue}/{@code removeValue} hot path branch-free (JIT dead-code-eliminates
   * the body when the flag is false).
   */
  private final boolean withPathStatistics;

  /**
   * Deferred per-path statistics accumulator. The naive path calls
   * {@link StorageEngineWriter#prepareRecordForModification} — a COW op — once
   * per value insert, on top of the stats-update itself. On a 100M-record
   * shred that's hundreds of millions of COWs just to maintain per-path
   * aggregates. Here we keep the pending deltas in a primitive-keyed map
   * and flush them to the underlying PathNodes in one pass at commit (or
   * when the map exceeds a sanity threshold). Distinct-path cardinality in
   * real JSON schemas is O(10-100), so the map is tiny in practice.
   *
   * <p>Semantics: values deferred here are NOT yet visible to readers of
   * the PathSummary. Queries that need live stats must call
   * {@link #flushPendingStats()} before opening a reader. The standard
   * commit path does so via the pre-commit hook in
   * {@link io.sirix.access.trx.node.AbstractNodeTrxImpl}.
   */
  private final Long2ObjectOpenHashMap<DeferredStats> pendingStats;

  /** Hard cap on deferred entries — forces a flush when exceeded to bound memory. */
  private static final int MAX_PENDING_PATH_ENTRIES = 4096;

  /**
   * Per-path insert-side delta carried by {@link #pendingStats}. Remove
   * paths flush synchronously so we don't carry remove deltas here. Fields
   * track only the state needed to merge into a PathNode's stats in one
   * pass: count / sum / min / max / null-count for numeric; bytes min/max
   * for strings; HLL batched into a local sketch that's unioned on flush.
   * Kind is recorded so mixed-type updates (rare but defensible) keep the
   * right lane.
   *
   * <p>Objects are recycled: on flush we don't allocate a fresh
   * {@code DeferredStats} per path; we clear in place via {@link #reset()}
   * so the map is zero-alloc after the warm-up phase.
   */
  private static final class DeferredStats {
    /** Kind marker: 0 = none, 1 = long, 2 = bytes. */
    byte kind;
    long count;
    long nullCount;
    long sum;
    long min;
    long max;
    byte[] minBytes;
    byte[] maxBytes;
    HyperLogLogSketch hll;
    /**
     * Leaf-page keys witnessed during this batch. Small set — typically
     * O(distinct pages touched since the last flush for this path) which
     * is bounded by {@code MAX_PENDING_PATH_ENTRIES} / distinct paths.
     * Lazily allocated so paths that never feed the page-skip index pay
     * no extra state.
     */
    IntOpenHashSet seenPages;

    DeferredStats() {
      reset();
    }

    void reset() {
      kind = 0;
      count = 0L;
      nullCount = 0L;
      sum = 0L;
      min = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      minBytes = null;
      maxBytes = null;
      hll = null;
      if (seenPages != null) seenPages.clear();
    }

    void recordPage(final int pageKey) {
      if (pageKey < 0) return;
      if (seenPages == null) seenPages = new IntOpenHashSet(4);
      seenPages.add(pageKey);
    }

    void addLong(final long v) {
      kind = 1;
      count++;
      sum += v;
      if (v < min) min = v;
      if (v > max) max = v;
      if (hll == null) hll = new HyperLogLogSketch();
      hll.add(v);
    }

    void addBytes(final byte[] v) {
      kind = 2;
      count++;
      if (minBytes == null || Arrays.compareUnsigned(v, minBytes) < 0) minBytes = v.clone();
      if (maxBytes == null || Arrays.compareUnsigned(v, maxBytes) > 0) maxBytes = v.clone();
      if (hll == null) hll = new HyperLogLogSketch();
      hll.add(v);
    }

    void addNull() {
      nullCount++;
    }
  }

  /** Pool of recycled {@link DeferredStats}; avoids GC pressure across flushes. */
  private final ArrayDeque<DeferredStats> deferredStatsPool = new ArrayDeque<>();

  private DeferredStats acquireDeferredStats() {
    final DeferredStats d = deferredStatsPool.pollFirst();
    return d != null ? d : new DeferredStats();
  }

  private void releaseDeferredStats(final DeferredStats d) {
    d.reset();
    deferredStatsPool.addLast(d);
  }

  /**
   * Constructor.
   *
   * @param storageEngineWriter Sirix {@link StorageEngineWriter}
   * @param resMgr The resource session
   * @param nodeFactory The node factory to create path nodes
   * @param rtx the read-only trx
   */
  public PathSummaryWriter(final StorageEngineWriter storageEngineWriter, final ResourceSession<R, ? extends NodeTrx> resMgr,
      final NodeFactory nodeFactory, final R rtx) {
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    pathSummaryReader = PathSummaryReader.getInstance(storageEngineWriter, resMgr);
    nodeRtx = requireNonNull(rtx);
    this.nodeFactory = requireNonNull(nodeFactory);
    storeChildCount = resMgr.getResourceConfig().storeChildCount();
    withPathStatistics = resMgr.getResourceConfig().withPathStatistics;
    // Tiny initial capacity — real JSON schemas see O(10-100) distinct paths.
    pendingStats = withPathStatistics ? new Long2ObjectOpenHashMap<>(32) : null;
  }

  /**
   * Get the path summary reader.
   *
   * @return {@link PathSummaryReader} instance
   */
  public PathSummaryReader getPathSummary() {
    return pathSummaryReader;
  }

  /**
   * Look up or create an {@code __array__/ARRAY} child path node under the path summary entry
   * identified by {@code parentPathNodeKey}, then return that ARRAY child's path node key.
   *
   * <p>iter#32 P2 structural fusion: a single {@code OBJECT_NAMED_ARRAY} record collapses the
   * legacy {@code OBJECT_KEY + ARRAY} pair. Path-summary semantics still need the {@code []}
   * (anonymous-array) layer so that user paths like {@code /features/[]/type} resolve. This
   * helper anchors the ARRAY layer underneath the OBJECT_KEY layer the field name created.</p>
   *
   * <p>Idempotent: if an {@code __array__/ARRAY} child already exists, its reference count is
   * incremented and the existing key is returned.</p>
   *
   * @param parentPathNodeKey path node key of the OBJECT_KEY parent (must be a valid path node)
   * @return path node key of the {@code __array__/ARRAY} child (existing or freshly inserted)
   */
  public long getArrayChildPathNodeKey(final long parentPathNodeKey) {
    if (parentPathNodeKey < 0) {
      throw new IllegalArgumentException("parentPathNodeKey must be a valid path node key");
    }
    final QNm arrayName = ARRAY_PATH_QNM;
    final long existing = pathSummaryReader.findChild(parentPathNodeKey, arrayName, NodeKind.ARRAY);
    if (existing >= 0) {
      final PathNode pathNode =
          storageEngineWriter.prepareRecordForModification(existing, IndexType.PATH_SUMMARY, 0);
      pathNode.incrementReferenceCount();
      persistPathSummaryRecord(pathNode);
      pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      return existing;
    }
    pathSummaryReader.moveTo(parentPathNodeKey);
    final int level = pathSummaryReader.getLevel();
    insertPathAsFirstChild(arrayName, NodeKind.ARRAY, level + 1);
    return pathSummaryReader.getNodeKey();
  }

  /** Canonical name of the synthetic {@code __array__/ARRAY} path-summary layer. */
  public static final QNm ARRAY_PATH_QNM = new QNm("__array__");

  /**
   * Look up the parent path node key (an OBJECT_KEY entry) of an {@code __array__/ARRAY} path
   * entry — used to balance ref counts when a fused {@code OBJECT_NAMED_ARRAY} record is removed.
   *
   * @param arrayPathNodeKey path node key of an {@code __array__/ARRAY} entry
   * @return parent OBJECT_KEY path node key, or {@code -1L} if not found / not an array entry
   */
  public long lookupArrayPathParentKey(final long arrayPathNodeKey) {
    if (arrayPathNodeKey < 0) {
      return -1L;
    }
    if (!pathSummaryReader.moveTo(arrayPathNodeKey)) {
      return -1L;
    }
    final PathNode arrayPathNode = pathSummaryReader.getPathNode();
    if (arrayPathNode == null || arrayPathNode.getPathKind() != NodeKind.ARRAY) {
      return -1L;
    }
    final long parent = arrayPathNode.getParentKey();
    if (parent < 0 || parent == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
      return -1L;
    }
    return parent;
  }

  /**
   * Decrement the reference count on the path-summary entry identified by {@code pathNodeKey}.
   * If the resulting count is zero, the path subtree is removed (matches the {@link
   * #remove(ImmutableNameNode)} contract). Used to balance the parent OBJECT_KEY ref the fused
   * {@code OBJECT_NAMED_ARRAY} insertion bumped via {@link #getArrayChildPathNodeKey(long)}.
   *
   * @param pathNodeKey path node key whose reference count should be decremented
   */
  public void decrementObjectKeyRefByKey(final long pathNodeKey) {
    if (pathNodeKey < 0) {
      return;
    }
    if (!pathSummaryReader.moveTo(pathNodeKey)) {
      return;
    }
    final PathNode parentPathNode = pathSummaryReader.getPathNode();
    if (parentPathNode == null) {
      return;
    }
    if (parentPathNode.getReferences() <= 1) {
      removePathSummaryNode(RemoveSubtreePath.YES);
    } else {
      decrementAndPersist(pathNodeKey);
    }
  }

  /**
   * Rename an existing OBJECT_KEY path-summary entry in place (does not move it among siblings).
   * Used when {@code setObjectKeyName} renames a fused {@link NodeKind#OBJECT_NAMED_ARRAY} field
   * — the OBJECT_KEY layer that carries the field name lives one level above the fused record's
   * {@code __array__/ARRAY} pathNodeKey, so the rename targets that parent entry.
   *
   * @param objectKeyPathNodeKey path node key of the OBJECT_KEY entry to rename
   * @param newName              new name for the entry
   * @param newLocalNameKey      pre-allocated NamePage local-name key for the new name
   */
  public long renameObjectKeyPathEntry(final long objectKeyPathNodeKey, final QNm newName,
      final int newLocalNameKey) {
    if (objectKeyPathNodeKey < 0) {
      return -1;
    }
    if (!pathSummaryReader.moveTo(objectKeyPathNodeKey)) {
      return -1;
    }
    final PathNode existing = pathSummaryReader.getPathNode();
    if (existing == null) {
      return -1;
    }
    final QNm oldName = pathSummaryReader.getName();
    final long parentKey = existing.getParentKey();
    final NodeKind pathKind = existing.getPathKind();
    final int level = existing.getLevel();

    if (existing.getReferences() <= 1) {
      // EXCLUSIVE path class: rename in place, and refresh the (parent, name, kind) child-lookup
      // cache — without the refresh, findChild for the OLD name kept resolving to this renamed
      // entry, so a later insert of a field with the old name incremented the WRONG path class.
      final PathNode pathNode = storageEngineWriter.prepareRecordForModification(objectKeyPathNodeKey,
          IndexType.PATH_SUMMARY, 0);
      pathNode.setPrefixKey(-1);
      pathNode.setLocalNameKey(newLocalNameKey);
      pathNode.setURIKey(-1);
      pathNode.setName(newName);
      persistPathSummaryRecord(pathNode);
      pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      pathSummaryReader.putQNameMapping(pathNode, newName);
      pathSummaryReader.removeChildLookup(parentKey, oldName, pathKind);
      pathSummaryReader.putChildLookup(parentKey, newName, pathKind, objectKeyPathNodeKey);
      // The __array__/ARRAY child layer is unchanged for an exclusive rename.
      return pathSummaryReader.findChild(objectKeyPathNodeKey, ARRAY_PATH_QNM, NodeKind.ARRAY);
    }

    // SHARED path class (references > 1): renaming IN PLACE would silently rename every OTHER
    // instance's path class too. SPLIT instead: the renamed instance leaves both layers
    // (decrement), then joins (or creates) the entry for the new name under the same parent.
    // NOTE: descendant path classes of the renamed instance's array elements remain under their
    // original (structurally identical) classes — migrating them is the full rebuild machinery.
    final long oldArrayChild = pathSummaryReader.findChild(objectKeyPathNodeKey, ARRAY_PATH_QNM, NodeKind.ARRAY);
    if (oldArrayChild >= 0) {
      decrementObjectKeyRefByKey(oldArrayChild);
    }
    decrementObjectKeyRefByKey(objectKeyPathNodeKey);

    pathSummaryReader.moveTo(parentKey);
    long newObjectKeyEntry = pathSummaryReader.findChild(parentKey, newName, pathKind);
    if (newObjectKeyEntry >= 0) {
      final PathNode newEntry = storageEngineWriter.prepareRecordForModification(newObjectKeyEntry,
          IndexType.PATH_SUMMARY, 0);
      newEntry.incrementReferenceCount();
      persistPathSummaryRecord(newEntry);
      pathSummaryReader.putMapping(newEntry.getNodeKey(), newEntry);
    } else {
      insertPathAsFirstChild(newName, pathKind, level);
      newObjectKeyEntry = pathSummaryReader.getNodeKey();
    }
    return getArrayChildPathNodeKey(newObjectKeyEntry);
  }

  /**
   * Insert a new path node or increment the counter of an existing node and return the path node key.
   *
   * @param name the name of the path node to search for
   * @param pathKind the kind of the path node to search for
   * @return a path node key of the found node, or the path node key of a new inserted node
   * @throws SirixException if anything went wrong
   */
  public long getPathNodeKey(final QNm name, final NodeKind pathKind) {
    final NodeKind kind = nodeRtx.getKind();
    int level = 0;
    if (kind == NodeKind.XML_DOCUMENT || kind == NodeKind.JSON_DOCUMENT) {
      pathSummaryReader.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    } else {
      movePathSummary();
      level = pathSummaryReader.getLevel();
    }

    final long parentNodeKey = pathSummaryReader.getNodeKey();

    // Use O(1) cache lookup instead of O(n) ChildAxis iteration
    // The child name for lookup - handle namespace prefix case
    final QNm lookupName = pathKind == NodeKind.NAMESPACE
        ? new QNm(name.getPrefix())
        : name;

    final long childNodeKey = pathSummaryReader.findChild(parentNodeKey, lookupName, pathKind);

    long retVal;
    if (childNodeKey >= 0) {
      // Found existing child - increment reference count
      retVal = childNodeKey;
      final PathNode pathNode = storageEngineWriter.prepareRecordForModification(retVal, IndexType.PATH_SUMMARY, 0);
      pathNode.incrementReferenceCount();
      persistPathSummaryRecord(pathNode);
      pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
    } else {
      // Child not found - insert new path node
      assert parentNodeKey == pathSummaryReader.getNodeKey();
      insertPathAsFirstChild(name, pathKind, level + 1);
      retVal = pathSummaryReader.getNodeKey();
    }
    return retVal;
  }

  /**
   * Move path summary cursor to the path node which is references by the current node.
   */
  private void movePathSummary() {
    NodeKind currentKind = nodeRtx.getKind();
    // Bare OBJECT/ARRAY containers don't carry a pathNodeKey of their own — the OBJECT_KEY
    // (or fused OBJECT_NAMED_OBJECT/ARRAY) anchor does. Hop up so the path-summary cursor
    // can reuse the parent's pathNodeKey. DO carry their
    // own pathNodeKey, so do not hop past them.
    if (currentKind == NodeKind.OBJECT) {
      nodeRtx.moveToParent();
      currentKind = nodeRtx.getKind();
    }

    final long pathNodeKey = nodeRtx.getPathNodeKey();
    if (pathNodeKey >= 0) {
      pathSummaryReader.moveTo(pathNodeKey);
      return;
    }

    final var node = nodeRtx.getNode();
    throw new IllegalStateException("movePathSummary: unexpected node kind=" + currentKind
        + " nodeClass=" + (node != null ? node.getClass().getName() : "null")
        + " nodeKey=" + nodeRtx.getNodeKey()
        + " pathNodeKey=" + pathNodeKey
        + " instanceOfImmutableNameNode=" + (node instanceof ImmutableNameNode));
  }

  /**
   * Insert a path node as first child.
   *
   * @param name {@link QNm} of the path node (not stored) twice
   * @param pathKind kind of node to index
   * @param level level in the path summary
   * @return this path writer instance
   * @throws SirixException if an I/O error occurs
   */
  public PathSummaryWriter<R> insertPathAsFirstChild(final QNm name, final NodeKind pathKind, final int level) {
    final long parentKey = pathSummaryReader.getNodeKey();
    final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long rightSibKey = pathSummaryReader.getFirstChildKey();
    final PathNode node = nodeFactory.createPathNode(parentKey, leftSibKey, rightSibKey, name, pathKind, level);

    pathSummaryReader.removeFromCache(name);
    pathSummaryReader.putMapping(node.getNodeKey(), node);
    pathSummaryReader.moveTo(node.getNodeKey());
    adaptForInsert(node);
    pathSummaryReader.moveTo(node.getNodeKey());
    pathSummaryReader.putQNameMapping(node, name);

    // Add to O(1) child lookup cache (primitives only)
    pathSummaryReader.putChildLookup(parentKey, name, pathKind, node.getNodeKey());

    return this;
  }

  /**
   * Adapting everything for insert operations.
   *
   * @param newNode pointer of the new node to be inserted
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final Node newNode) {
    assert newNode != null;

    if (newNode instanceof StructNode strucNode) {
      final StructNode parent = storageEngineWriter.prepareRecordForModification(newNode.getParentKey(), IndexType.PATH_SUMMARY, 0);
      if (storeChildCount) {
        parent.incrementChildCount();
      }
      parent.setFirstChildKey(newNode.getNodeKey());
      persistPathSummaryRecord(parent);
      pathSummaryReader.putMapping(parent.getNodeKey(), parent);

      if (strucNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            storageEngineWriter.prepareRecordForModification(strucNode.getRightSiblingKey(), IndexType.PATH_SUMMARY, 0);
        rightSiblingNode.setLeftSiblingKey(newNode.getNodeKey());
        persistPathSummaryRecord(rightSiblingNode);
        pathSummaryReader.putMapping(rightSiblingNode.getNodeKey(), rightSiblingNode);
      }
      if (strucNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            storageEngineWriter.prepareRecordForModification(strucNode.getLeftSiblingKey(), IndexType.PATH_SUMMARY, 0);
        leftSiblingNode.setRightSiblingKey(newNode.getNodeKey());
        persistPathSummaryRecord(leftSiblingNode);
        pathSummaryReader.putMapping(leftSiblingNode.getNodeKey(), leftSiblingNode);
      }
    }
  }

  /**
   * Adapt path summary either for moves or {@code setQName(QName)}.
   *
   * @param node the node for which the path node needs to be adapted
   * @param name the new {@link QName} in case of a new one is set, the old {@link QName} otherwise
   * @param uriKey uriKey of the new node
   * @throws SirixException if a Sirix operation fails
   * @throws NullPointerException if {@code pNode} or {@code pQName} is null
   */
  public void adaptPathForChangedNode(final ImmutableNameNode node, final QNm name, final int uriKey,
      final int prefixKey, final int localNameKey, final OPType type) {
    // Possibly either reset a path node or decrement its reference counter
    // and search for the new path node or insert it.
    movePathSummary();

    final long oldPathNodeKey = pathSummaryReader.getNodeKey();

    // Only one path node is referenced (after a setQName(QName) the
    // reference-counter would be 0).
    // Fused OBJECT_NAMED_* records are represented as OBJECT_KEY entries in the path summary, so
    // the filter needs the logical path kind, not the physical record kind.
    final NodeKind nodeKind = node.getKind();
    final NodeKind pathFilterKind = nodeKind.isFusedAnyNamed() ? NodeKind.OBJECT_NAMED_OBJECT : nodeKind;
    if (type == OPType.SETNAME && pathSummaryReader.getReferences() == 1) {
      moveSummaryGetLevel(node);
      // Search for new path entry.
      final Axis axis = new FilterAxis<>(new ChildAxis(pathSummaryReader),
          new PathNameFilter(pathSummaryReader, Utils.buildName(name)),
          new PathKindFilter(pathSummaryReader, pathFilterKind));
      if (axis.hasNext()) {
        axis.nextLong();

        long nodeKey = decrementReferenceCountOrRemove(node);

        pathSummaryReader.moveTo(nodeKey);

        // Found node.
        processFoundPathNode(oldPathNodeKey, pathSummaryReader.getNodeKey(), node.getNodeKey(), name, uriKey, prefixKey,
            localNameKey);
      } else {
        /* The path summary just needs to be updated for the new renamed node. */
        pathSummaryReader.moveTo(oldPathNodeKey);
        final PathNode pathNode =
            storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
        pathNode.setPrefixKey(prefixKey);
        pathNode.setLocalNameKey(localNameKey);
        pathNode.setURIKey(uriKey);
        pathNode.setName(name);
        persistPathSummaryRecord(pathNode);
        pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
        pathSummaryReader.putQNameMapping(pathNode, name);
      }
    } else {
      int level = moveSummaryGetLevel(node);
      // TODO: Johannes: Optimize? (either use this or use the name-mapping,
      // depending on the number of child nodes or nodes with a certain name).

      // Search for new path entry.
      final Axis axis = new FilterAxis<>(new ChildAxis(pathSummaryReader),
          new PathNameFilter(pathSummaryReader, Utils.buildName(name)),
          new PathKindFilter(pathSummaryReader, pathFilterKind));
      if (axis.hasNext()) {
        axis.nextLong();

        long nodeKey = decrementReferenceCountOrRemove(node);

        pathSummaryReader.moveTo(nodeKey);

        // Found node.
        processFoundPathNode(oldPathNodeKey, pathSummaryReader.getNodeKey(), node.getNodeKey(), name, uriKey, prefixKey,
            localNameKey);
      } else {
        long nodeKey = decrementReferenceCountOrRemove(node);

        pathSummaryReader.moveTo(nodeKey);

        // Not found => create new path nodes for the whole subtree.
        boolean firstRun = true;
        for (final Axis descendants = new DescendantAxis(nodeRtx, IncludeSelf.YES); descendants.hasNext();) {
          descendants.nextLong();
          final NodeKind rtxKind = nodeRtx.getKind();
          if (rtxKind == NodeKind.ELEMENT || rtxKind.playsObjectKeyRole()) {
            // Fused OBJECT_NAMED_* play the OBJECT_KEY role in the path-summary.
            final NodeKind pathKind = rtxKind == NodeKind.ELEMENT ? NodeKind.ELEMENT : NodeKind.OBJECT_NAMED_OBJECT;
            // Path Summary : New mapping.
            if (firstRun) {
              insertPathAsFirstChild(name, pathKind, ++level);
              nodeKey = pathSummaryReader.getNodeKey();
            } else {
              insertPathAsFirstChild(nodeRtx.getName(), pathKind, ++level);
            }
            resetPathNodeKey(nodeRtx.getNodeKey());

            if (nodeRtx instanceof XmlNodeReadOnlyTrx rtx) {
              // Namespaces.
              for (int i = 0, nsps = rtx.getNamespaceCount(); i < nsps; i++) {
                rtx.moveToNamespace(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), NodeKind.NAMESPACE, level + 1);
                resetPathNodeKey(rtx.getNodeKey());
                rtx.moveToParent();
                pathSummaryReader.moveToParent();
              }

              // Attributes.
              for (int i = 0, atts = rtx.getAttributeCount(); i < atts; i++) {
                rtx.moveToAttribute(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), NodeKind.ATTRIBUTE, level + 1);
                resetPathNodeKey(rtx.getNodeKey());
                rtx.moveToParent();
                pathSummaryReader.moveToParent();
              }
            }

            if (firstRun) {
              firstRun = false;
            } else {
              pathSummaryReader.moveToParent();
              level--;
            }
          }
        }

        pathSummaryReader.moveTo(nodeKey);
      }
    }
  }

  // Decrement reference count or remove path summary node.
  private long decrementReferenceCountOrRemove(final ImmutableNameNode node) {
    long nodeKey = pathSummaryReader.getNodeKey();
    nodeRtx.moveTo(node.getNodeKey());

    for (final Axis descendants = new PostOrderAxis(nodeRtx, IncludeSelf.YES); descendants.hasNext();) {
      descendants.nextLong();

      if (nodeRtx.getKind() == NodeKind.ELEMENT) {
        final XmlNodeReadOnlyTrx rtx = (XmlNodeReadOnlyTrx) nodeRtx;
        final ImmutableElement element = (ImmutableElement) rtx.getNode();

        // Namespaces.
        for (int i = 0, nsps = element.getNamespaceCount(); i < nsps; i++) {
          rtx.moveToNamespace(i);
          deleteOrDecrement();
          rtx.moveToParent();
        }

        // Attributes.
        for (int i = 0, atts = element.getAttributeCount(); i < atts; i++) {
          rtx.moveToAttribute(i);
          deleteOrDecrement();
          rtx.moveToParent();
        }
      }

      deleteOrDecrement();
    }

    return nodeKey;
  }

  /**
   * Process a found path node.
   *
   * @param oldPathNodeKey key of old path node
   * @param newPathNodeKey key of new path node
   * @param oldNodeKey key of old node
   * @param uriKey key of URI
   * @param prefixKey key of prefix
   * @param localNameKey key of local name
   * @throws SirixException if Sirix fails to do so
   */
  private void processFoundPathNode(final long oldPathNodeKey, final long newPathNodeKey,
      final long oldNodeKey, final QNm name, final int uriKey, final int prefixKey,
      final int localNameKey) {
    nodeRtx.moveTo(oldNodeKey);

    // Set new reference count of the root.
    final PathNode currNode =
        storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
    currNode.setLocalNameKey(localNameKey);
    currNode.setPrefixKey(prefixKey);
    currNode.setURIKey(uriKey);
    currNode.setName(name);
    persistPathSummaryRecord(currNode);
    pathSummaryReader.putMapping(currNode.getNodeKey(), currNode);
    pathSummaryReader.putQNameMapping(currNode, name);

    final long pathNodeKey = currNode.getNodeKey();

    processElementNonStructuralNodes(pathNodeKey, 0);

    // For all old path nodes: Merge paths and adapt reference counts.
    final boolean movedNodeCursorToFirstChild = nodeRtx.moveToFirstChild();
    final boolean movedPathSummaryToFirstChild = pathSummaryReader.moveToFirstChild();

    if (movedNodeCursorToFirstChild && movedPathSummaryToFirstChild) {
      final long pathRootNodeKey = pathSummaryReader.getNodeKey();

      for (final LevelOrderAxis levelOrderAxis =
          new LevelOrderAxis.Builder(nodeRtx).includeSelf().build(); levelOrderAxis.hasNext();) {
        levelOrderAxis.nextLong();

        if (nodeRtx.getNode() instanceof ImmutableNameNode) {
          adaptPathSummary(levelOrderAxis.getCurrentLevel(), pathRootNodeKey);

          processElementNonStructuralNodes(pathRootNodeKey, levelOrderAxis.getCurrentLevel());
        }
      }
    } else if (movedNodeCursorToFirstChild) {
      for (final LevelOrderAxis levelOrderAxis =
          new LevelOrderAxis.Builder(nodeRtx).includeSelf().build(); levelOrderAxis.hasNext();) {
        levelOrderAxis.nextLong();

        if (nodeRtx.getNode() instanceof ImmutableNameNode) {
          adaptForNewPathNode();

          processElementNonStructuralNodes(pathSummaryReader.getNodeKey(), levelOrderAxis.getCurrentLevel());
        }
      }
    } else if (movedPathSummaryToFirstChild) {
      // Only move back.
      pathSummaryReader.moveToParent();
    }

    pathSummaryReader.moveTo(pathNodeKey);

    // The moved/renamed node itself must now point at the FOUND path node — the descendant
    // walk above only adapts child NameNodes, and the create-new branch resets the root via
    // resetPathNodeKey. Without this the root keeps its OLD pathNodeKey, so path-scoped
    // consumers (path-filtered scans, path indexes) keep attributing it to the old path.
    // Runs AFTER the descendant walk: the walk's parent-path positioning
    // (moveToPathNodeOfParentNode) reads the root's pathNodeKey and must observe the
    // PRE-move value to merge existing children correctly.
    resetPathNodeKey(oldNodeKey);
  }

  private void processElementNonStructuralNodes(final long pathRootNodeKey, final int level) {
    if (nodeRtx.getNode().getKind() == NodeKind.ELEMENT) {
      final XmlNodeReadOnlyTrx rtx = (XmlNodeReadOnlyTrx) nodeRtx;
      final ImmutableElement element = (ImmutableElement) rtx.getNode();

      for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
        rtx.moveToNamespace(i);
        adaptPathSummary(level, pathRootNodeKey);
        rtx.moveToParent();
      }
      for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
        rtx.moveToAttribute(i);
        adaptPathSummary(level, pathRootNodeKey);
        rtx.moveToParent();
      }
    }
  }

  private void adaptPathSummary(int level, long newPathNodeKey) {
    // Fused OBJECT_NAMED_* records live in the path summary under the OBJECT_KEY pathKind, so
    // normalise the physical record kind to the logical path kind for filtering.
    final NodeKind rtxKind = nodeRtx.getKind();
    final NodeKind pathFilterKind = rtxKind.isFusedAnyNamed() ? NodeKind.OBJECT_NAMED_OBJECT : rtxKind;
    // Search for new path entry.
    final Axis axis =
        new FilterAxis<>(new LevelOrderAxis.Builder(pathSummaryReader).filterLevel(level).includeSelf().build(),
            new PathNameFilter(pathSummaryReader, Utils.buildName(nodeRtx.getName())),
            new PathKindFilter(pathSummaryReader, pathFilterKind));
    if (axis.hasNext()) {
      axis.nextLong();

      adaptForFoundPathNode();
    } else {
      adaptForNewPathNode();
    }

    pathSummaryReader.moveTo(newPathNodeKey);
  }

  private void adaptForNewPathNode() {
    // Move to parent path node.
    moveToPathNodeOfParentNode();

    // Insert new node. Fused OBJECT_NAMED_* records share the OBJECT_KEY pathKind slot.
    final NodeKind rtxKind = nodeRtx.getKind();
    final NodeKind pathKind = rtxKind.isFusedAnyNamed() ? NodeKind.OBJECT_NAMED_OBJECT : rtxKind;
    insertPathAsFirstChild(nodeRtx.getName(), pathKind, pathSummaryReader.getLevel() + 1);

    // Set reference count to one.
    setReferenceCountToOne();

    // Set new path node key.
    setNewPathNodeKey();
  }

  private void moveToPathNodeOfParentNode() {
    final long nodeKey = nodeRtx.getNodeKey();

    final long pathNodeKey;

    if (nodeRtx instanceof XmlNodeReadOnlyTrx xmlNodeReadOnlyTrx) {
      xmlNodeReadOnlyTrx.moveToParent();
      pathNodeKey = xmlNodeReadOnlyTrx.getPathNodeKey();
    } else if (nodeRtx instanceof JsonNodeReadOnlyTrx jsonNodeReadOnlyTrx) {
      jsonNodeReadOnlyTrx.moveToParent();
      pathNodeKey = jsonNodeReadOnlyTrx.getPathNodeKey();
    } else {
      throw new IllegalStateException("Node transaction kind not known.");
    }

    pathSummaryReader.moveTo(pathNodeKey);
    nodeRtx.moveTo(nodeKey);
  }

  private void adaptForFoundPathNode() {
    // Increase reference count.
    increaseReferenceCount();

    // Set new path node.
    resetPathNodeKey(nodeRtx.getNodeKey());
  }

  private void setNewPathNodeKey() {
    final NameNode node = storageEngineWriter.prepareRecordForModification(nodeRtx.getNodeKey(), IndexType.DOCUMENT, -1);
    node.setPathNodeKey(pathSummaryReader.getNodeKey());
    persistDocumentRecord(node);
  }

  private void setReferenceCountToOne() {
    final PathNode currNode =
        storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(1);
    persistPathSummaryRecord(currNode);
    pathSummaryReader.putMapping(currNode.getNodeKey(), currNode);
  }

  private void increaseReferenceCount() {
    // Set new reference count.
    final PathNode currNode =
        storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
    persistPathSummaryRecord(currNode);
    pathSummaryReader.putMapping(currNode.getNodeKey(), currNode);
  }

  /**
   * Move path summary to the associated {@code parent} {@link PathNode} and get the level of the
   * node.
   *
   * @param node the node to lookup for it's {@link PathNode}
   * @return level of the path node
   */
  private int moveSummaryGetLevel(final ImmutableNode node) {
    assert node != null;
    // Get parent path node and level.
    nodeRtx.moveToParent();
    if (nodeRtx.getKind() == NodeKind.OBJECT)
      nodeRtx.moveToParent();

    int level = 0;
    if (nodeRtx.getKind() == NodeKind.XML_DOCUMENT || nodeRtx.getKind() == NodeKind.JSON_DOCUMENT) {
      pathSummaryReader.moveToDocumentRoot();
    } else {
      movePathSummary();
      level = pathSummaryReader.getLevel();
    }
    nodeRtx.moveTo(node.getNodeKey());
    return level;
  }

  /**
   * Reset a path node key on a document record.
   *
   * <p>Applies to every record that implements {@link NameNode}: XML {@code ELEMENT/ATTRIBUTE/
   * NAMESPACE}, JSON legacy {@code OBJECT_KEY}, and the iter#30 fused {@code OBJECT_NAMED_*}
   * kinds. Each of those types stores its own {@code pathNodeKey}; the narrow {@code NameNode}
   * interface exposes the setter uniformly. A previous implementation cast the else branch to
   * {@code ObjectKeyNode}, which blew up with a {@code ClassCastException} as soon as
   * {@code sirix.json.fuseNamedPrimitives=true} moved a primitive-valued field.
   *
   * @param nodeKey the nodeKey of the node to adapt
   * @param nodeKind reserved for diagnostics; all current callers pass a NameNode-bearing kind
   * @throws SirixException if anything fails
   */
  @SuppressWarnings("unused")
  private void resetPathNodeKey(final long nodeKey) {
    final NameNode currNode = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    currNode.setPathNodeKey(pathSummaryReader.getNodeKey());
    persistDocumentRecord(currNode);
  }

  /**
   * Remove a path summary node with the specified PCR.
   *
   * @throws SirixException if Sirix fails to remove the path node
   */
  private void removePathSummaryNode(final RemoveSubtreePath remove) {
    pathSummaryReader.clearCache();

    // Remove all descendant nodes.
    if (remove == RemoveSubtreePath.YES) {
      for (final Axis axis = new PostOrderAxis(pathSummaryReader); axis.hasNext();) {
        axis.nextLong();
        final PathNode pathNode = pathSummaryReader.getPathNode();
        if (pathNode != null) {
          // Remove from child lookup cache using parent key
          pathSummaryReader.removeChildLookup(pathNode.getParentKey(), pathSummaryReader.getName(),
              pathNode.getPathKind());
        }
        // Purge deferred stats: a pending entry whose PathNode record is gone otherwise blew up
        // at commit-time flushPendingStats (prepareRecordForModification on a removed record).
        if (pendingStats != null) {
          pendingStats.remove(pathSummaryReader.getNodeKey());
        }
        pathSummaryReader.removeMapping(pathSummaryReader.getNodeKey());
        pathSummaryReader.removeQNameMapping(pathNode, pathSummaryReader.getName());
        storageEngineWriter.removeRecord(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
      }
    }

    // Purge the node's own deferred stats too (it is removed below).
    if (pendingStats != null) {
      pendingStats.remove(pathSummaryReader.getNodeKey());
    }

    // Adapt left sibling node if there is one.
    if (pathSummaryReader.hasLeftSibling()) {
      final StructNode leftSibling =
          storageEngineWriter.prepareRecordForModification(pathSummaryReader.getLeftSiblingKey(), IndexType.PATH_SUMMARY, 0);
      leftSibling.setRightSiblingKey(pathSummaryReader.getRightSiblingKey());
      persistPathSummaryRecord(leftSibling);
      pathSummaryReader.putMapping(leftSibling.getNodeKey(), leftSibling);
    }

    // Adapt right sibling node if there is one.
    if (pathSummaryReader.hasRightSibling()) {
      final StructNode rightSibling =
          storageEngineWriter.prepareRecordForModification(pathSummaryReader.getRightSiblingKey(), IndexType.PATH_SUMMARY, 0);
      rightSibling.setLeftSiblingKey(pathSummaryReader.getLeftSiblingKey());
      persistPathSummaryRecord(rightSibling);
      pathSummaryReader.putMapping(rightSibling.getNodeKey(), rightSibling);
    }

    // Adapt parent. If node has no left sibling it is a first child.
    StructNode parent =
        storageEngineWriter.prepareRecordForModification(pathSummaryReader.getParentKey(), IndexType.PATH_SUMMARY, 0);
    if (!pathSummaryReader.hasLeftSibling()) {
      parent.setFirstChildKey(pathSummaryReader.getRightSiblingKey());
    }
    if (storeChildCount) {
      parent.decrementChildCount();
    }
    persistPathSummaryRecord(parent);
    pathSummaryReader.putMapping(parent.getNodeKey(), parent);

    // Remove current node from child lookup cache
    final PathNode currentPathNode = pathSummaryReader.getPathNode();
    if (currentPathNode != null) {
      pathSummaryReader.removeChildLookup(currentPathNode.getParentKey(), pathSummaryReader.getName(),
          currentPathNode.getPathKind());
    }

    // Remove node.
    pathSummaryReader.removeMapping(pathSummaryReader.getNodeKey());
    pathSummaryReader.removeQNameMapping(currentPathNode, pathSummaryReader.getName());
    storageEngineWriter.removeRecord(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);

    // pathSummaryReader.moveToDocumentRoot();
    //
    // System.out.println("removed: =====================");
    //
    // for (final var descendantAxis = new DescendantAxis(pathSummaryReader); descendantAxis.hasNext();
    // ) {
    // descendantAxis.nextLong();
    // System.out.println("path: " + pathSummaryReader.getPath());
    // System.out.println("nodeKey: " + pathSummaryReader.getNodeKey());
    // System.out.println("rightSiblingKey: " + pathSummaryReader.getRightSiblingKey());
    // System.out.println("references: " + pathSummaryReader.getReferences());
    // }
  }

  private void deleteOrDecrement() {
    if (nodeRtx.getNode() instanceof ImmutableNameNode) {
      movePathSummary();
      if (pathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.NO);
      } else if (pathSummaryReader.getReferences() > 1) {
        decrementAndPersist(pathSummaryReader.getNodeKey());
      }
    }
  }

  /**
   * Decrements the reference-counter of the node or removes the path node if the reference-counter
   * would be zero otherwise.
   *
   * @param node node which is going to be removed from the storage
   * @throws SirixException if anything went wrong
   */
  public void remove(final ImmutableNameNode node) {
    if (pathSummaryReader.moveTo(node.getPathNodeKey())) {
      if (pathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.YES);
      } else if (pathSummaryReader.getReferences() > 1) {
        decrementAndPersist(pathSummaryReader.getNodeKey());
      }
    }
  }

  private void decrementAndPersist(final long nodeKey) {
    final PathNode pathNode =
        storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.PATH_SUMMARY, 0);
    pathNode.decrementReferenceCount();
    persistPathSummaryRecord(pathNode);
    pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
  }

  private void persistDocumentRecord(final DataRecord record) {
    // No-op: records are mutated in-place via prepareRecordForModification()
    // and serialized at commit time via processEntries(). No slot sync needed.
  }

  private void persistPathSummaryRecord(final DataRecord record) {
    // No-op: records are mutated in-place via prepareRecordForModification()
    // and serialized at commit time via processEntries(). No slot sync needed.
  }

  // =====================================================================
  // PathStatistics maintenance — see ResourceConfiguration.withPathStatistics
  // and PathNode's record/remove mutators. Each method short-circuits when
  // stats are disabled so callers don't need a guard.
  // =====================================================================

  /**
   * Record a numeric value observation for the path identified by {@code pathNodeKey}.
   * No-op if path statistics are disabled or the key is negative (no path node).
   *
   * <p>Update is deferred into {@link #pendingStats}; the actual PathNode
   * COW happens at flush time (commit or size-threshold). This turns N
   * value inserts per path into 1 COW per path per commit, a ~4-5 order
   * magnitude reduction for analytical shreds.
   */
  public void recordValue(final long pathNodeKey, final long numericValue) {
    recordValue(pathNodeKey, numericValue, -1L);
  }

  /** Record a byte-sequence value observation (string / utf-8). No-op if stats disabled. */
  public void recordValue(final long pathNodeKey, final byte[] bytesValue) {
    recordValue(pathNodeKey, bytesValue, -1L);
  }

  /** Record a boolean value observation. No-op if stats disabled. */
  public void recordBooleanValue(final long pathNodeKey, final boolean value) {
    recordBooleanValue(pathNodeKey, value, -1L);
  }

  /** Record a null value observation. No-op if stats disabled. */
  public void recordNullValue(final long pathNodeKey) {
    recordNullValue(pathNodeKey, -1L);
  }

  /**
   * {@code pageSourceNodeKey}-aware variant. The caller passes the node
   * key of the just-inserted value node (or its enclosing OBJECT_KEY);
   * the leaf pageKey is derived as
   * {@code nodeKey >>> INP_REFERENCE_COUNT_EXPONENT} and folded into the
   * PathNode's presence bitmap at flush time. Pass {@code -1L} to skip
   * page-skip tracking — callers that don't yet have the nodeKey (or
   * resources where the bitmap is useless) can use the un-keyed overload.
   */
  public void recordValue(final long pathNodeKey, final long numericValue, final long pageSourceNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    final DeferredStats d = acquireOrCreate(pathNodeKey);
    d.addLong(numericValue);
    recordPageFor(d, pageSourceNodeKey);
    maybeFlushIfOverflow();
  }

  public void recordValue(final long pathNodeKey, final byte[] bytesValue, final long pageSourceNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    final DeferredStats d = acquireOrCreate(pathNodeKey);
    d.addBytes(bytesValue);
    recordPageFor(d, pageSourceNodeKey);
    maybeFlushIfOverflow();
  }

  public void recordBooleanValue(final long pathNodeKey, final boolean value, final long pageSourceNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    final DeferredStats d = acquireOrCreate(pathNodeKey);
    d.addLong(value ? 1L : 0L);
    recordPageFor(d, pageSourceNodeKey);
    maybeFlushIfOverflow();
  }

  public void recordNullValue(final long pathNodeKey, final long pageSourceNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    final DeferredStats d = acquireOrCreate(pathNodeKey);
    d.addNull();
    recordPageFor(d, pageSourceNodeKey);
    maybeFlushIfOverflow();
  }

  private DeferredStats acquireOrCreate(final long pathNodeKey) {
    DeferredStats d = pendingStats.get(pathNodeKey);
    if (d == null) {
      d = acquireDeferredStats();
      pendingStats.put(pathNodeKey, d);
    }
    return d;
  }

  private static void recordPageFor(final DeferredStats d, final long nodeKey) {
    if (nodeKey < 0L) return;
    final long pk = nodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
    if (pk > Integer.MAX_VALUE) return; // bitmap is int-keyed; above 2^31 pages, skip tracking
    d.recordPage((int) pk);
  }

  /** Bound the deferred buffer; distinct-path cardinality should be small in practice. */
  private void maybeFlushIfOverflow() {
    if (pendingStats.size() >= MAX_PENDING_PATH_ENTRIES) {
      flushPendingStats();
    }
  }

  /**
   * Apply every deferred stats delta to its PathNode via a single
   * {@link StorageEngineWriter#prepareRecordForModification} per path, then
   * clear the map. Called by {@link io.sirix.access.trx.node.AbstractNodeTrxImpl}
   * as the first pre-commit step AND by readers that need live stats.
   *
   * <p>Safe to call when {@code withPathStatistics == false} (no-op) or when
   * the pending map is empty. Idempotent; back-to-back calls are cheap.
   */
  public void flushPendingStats() {
    if (!withPathStatistics || pendingStats == null || pendingStats.isEmpty()) {
      return;
    }
    final var it = pendingStats.long2ObjectEntrySet().fastIterator();
    while (it.hasNext()) {
      final var entry = it.next();
      final long pathNodeKey = entry.getLongKey();
      final DeferredStats d = entry.getValue();
      if (d.count == 0 && d.nullCount == 0) {
        releaseDeferredStats(d);
        continue;
      }
      final PathNode pathNode = storageEngineWriter.prepareRecordForModification(
          pathNodeKey, IndexType.PATH_SUMMARY, 0);
      applyDeferredStats(pathNode, d);
      persistPathSummaryRecord(pathNode);
      pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      releaseDeferredStats(d);
    }
    pendingStats.clear();
  }

  /**
   * Merge one {@link DeferredStats} into a PathNode in a single update.
   * Splice the precomputed aggregates directly; HLL is union-merged with
   * the node's existing sketch. Remove paths don't flow through here —
   * they flush-then-synchronously-apply.
   */
  private static void applyDeferredStats(final PathNode pn, final DeferredStats d) {
    if (d.kind == 1 && d.count > 0) {
      pn.mergeLongStats(d.count, d.sum, d.min, d.max);
    } else if (d.kind == 2 && d.count > 0) {
      pn.mergeBytesStats(d.count, d.minBytes, d.maxBytes);
    }
    if (d.nullCount > 0) {
      pn.incrementNullCount(d.nullCount);
    }
    if (d.hll != null) {
      pn.unionHll(d.hll);
    }
    if (d.seenPages != null && !d.seenPages.isEmpty()) {
      pn.mergePageKeys(d.seenPages);
    }
  }

  /**
   * Decrement stats on delete. Flushes any pending inserts first so the
   * dirty-bound check sees the correct pre-delete state, then applies the
   * decrement synchronously. Removes are much rarer than inserts in
   * analytical shreds; the per-remove flush is cheap (flushes are O(distinct
   * paths) and typically O(10)).
   */
  public void removeValue(final long pathNodeKey, final long numericValue) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    flushPendingStats();
    final PathNode pathNode = storageEngineWriter.prepareRecordForModification(
        pathNodeKey, IndexType.PATH_SUMMARY, 0);
    pathNode.removeLongValue(numericValue);
    persistPathSummaryRecord(pathNode);
    pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
  }

  public void removeValue(final long pathNodeKey, final byte[] bytesValue) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    flushPendingStats();
    final PathNode pathNode = storageEngineWriter.prepareRecordForModification(
        pathNodeKey, IndexType.PATH_SUMMARY, 0);
    pathNode.removeBytesValue(bytesValue);
    persistPathSummaryRecord(pathNode);
    pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
  }

  public void removeBooleanValue(final long pathNodeKey, final boolean value) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    flushPendingStats();
    final PathNode pathNode = storageEngineWriter.prepareRecordForModification(
        pathNodeKey, IndexType.PATH_SUMMARY, 0);
    pathNode.removeBooleanValue(value);
    persistPathSummaryRecord(pathNode);
    pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
  }

  public void removeNullValue(final long pathNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    flushPendingStats();
    final PathNode pathNode = storageEngineWriter.prepareRecordForModification(
        pathNodeKey, IndexType.PATH_SUMMARY, 0);
    pathNode.removeNullValue();
    persistPathSummaryRecord(pathNode);
    pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
  }

  /** For callers that want to avoid redundant work when stats are off. */
  public boolean isPathStatisticsEnabled() {
    return withPathStatistics;
  }

  @Override
  protected PathSummaryReader delegate() {
    return pathSummaryReader;
  }
}
