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
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.settings.Fixed;
import io.brackit.query.atomic.QNm;
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
    if (type == OPType.SETNAME && pathSummaryReader.getReferences() == 1) {
      moveSummaryGetLevel(node);
      // Search for new path entry.
      final Axis axis = new FilterAxis<>(new ChildAxis(pathSummaryReader),
          new PathNameFilter(pathSummaryReader, Utils.buildName(name)),
          new PathKindFilter(pathSummaryReader, node.getKind()));
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
          new PathKindFilter(pathSummaryReader, node.getKind()));
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
          if (nodeRtx.getKind() == NodeKind.ELEMENT || nodeRtx.getKind() == NodeKind.OBJECT_KEY) {
            // Path Summary : New mapping.
            if (firstRun) {
              insertPathAsFirstChild(name, nodeRtx.getKind(), ++level);
              nodeKey = pathSummaryReader.getNodeKey();
            } else {
              insertPathAsFirstChild(nodeRtx.getName(), nodeRtx.getKind(), ++level);
            }
            resetPathNodeKey(nodeRtx.getNodeKey(), nodeRtx.getKind());

            if (nodeRtx instanceof XmlNodeReadOnlyTrx rtx) {
              // Namespaces.
              for (int i = 0, nsps = rtx.getNamespaceCount(); i < nsps; i++) {
                rtx.moveToNamespace(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), NodeKind.NAMESPACE, level + 1);
                resetPathNodeKey(rtx.getNodeKey(), NodeKind.NAMESPACE);
                rtx.moveToParent();
                pathSummaryReader.moveToParent();
              }

              // Attributes.
              for (int i = 0, atts = rtx.getAttributeCount(); i < atts; i++) {
                rtx.moveToAttribute(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), NodeKind.ATTRIBUTE, level + 1);
                resetPathNodeKey(rtx.getNodeKey(), NodeKind.ATTRIBUTE);
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
    // Search for new path entry.
    final Axis axis =
        new FilterAxis<>(new LevelOrderAxis.Builder(pathSummaryReader).filterLevel(level).includeSelf().build(),
            new PathNameFilter(pathSummaryReader, Utils.buildName(nodeRtx.getName())),
            new PathKindFilter(pathSummaryReader, nodeRtx.getKind()));
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

    // Insert new node.
    insertPathAsFirstChild(nodeRtx.getName(), nodeRtx.getKind(), pathSummaryReader.getLevel() + 1);

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
    resetPathNodeKey(nodeRtx.getNodeKey(), nodeRtx.getKind());
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
   * Reset a path node key.
   *
   * @param nodeKey the nodeKey of the node to adapt
   * @param nodeKind the kind of the node to adapt
   * @throws SirixException if anything fails
   */
  private void resetPathNodeKey(final long nodeKey, final NodeKind nodeKind) {
    if (nodeKind == NodeKind.ATTRIBUTE || nodeKind == NodeKind.ELEMENT || nodeKind == NodeKind.NAMESPACE) {
      final NameNode currNode = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
      currNode.setPathNodeKey(pathSummaryReader.getNodeKey());
      persistDocumentRecord(currNode);
    } else {
      final ObjectKeyNode currNode = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
      currNode.setPathNodeKey(pathSummaryReader.getNodeKey());
      persistDocumentRecord(currNode);
    }
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
        pathSummaryReader.removeMapping(pathSummaryReader.getNodeKey());
        pathSummaryReader.removeQNameMapping(pathNode, pathSummaryReader.getName());
        storageEngineWriter.removeRecord(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
      }
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
      } else {
        final PathNode pathNode =
            storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
        pathNode.decrementReferenceCount();
        persistPathSummaryRecord(pathNode);
        pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
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
      } else {
        assert pathSummaryReader.getReferences() > 1;
        final PathNode pathNode =
            storageEngineWriter.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
        pathNode.decrementReferenceCount();
        persistPathSummaryRecord(pathNode);
        pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      }
    }
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
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    DeferredStats d = pendingStats.get(pathNodeKey);
    if (d == null) {
      d = acquireDeferredStats();
      pendingStats.put(pathNodeKey, d);
    }
    d.addLong(numericValue);
    maybeFlushIfOverflow();
  }

  /** Record a byte-sequence value observation (string / utf-8). No-op if stats disabled. */
  public void recordValue(final long pathNodeKey, final byte[] bytesValue) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    DeferredStats d = pendingStats.get(pathNodeKey);
    if (d == null) {
      d = acquireDeferredStats();
      pendingStats.put(pathNodeKey, d);
    }
    d.addBytes(bytesValue);
    maybeFlushIfOverflow();
  }

  /** Record a boolean value observation. No-op if stats disabled. */
  public void recordBooleanValue(final long pathNodeKey, final boolean value) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    DeferredStats d = pendingStats.get(pathNodeKey);
    if (d == null) {
      d = acquireDeferredStats();
      pendingStats.put(pathNodeKey, d);
    }
    d.addLong(value ? 1L : 0L);
    maybeFlushIfOverflow();
  }

  /** Record a null value observation. No-op if stats disabled. */
  public void recordNullValue(final long pathNodeKey) {
    if (!withPathStatistics || pathNodeKey < 0) {
      return;
    }
    DeferredStats d = pendingStats.get(pathNodeKey);
    if (d == null) {
      d = acquireDeferredStats();
      pendingStats.put(pathNodeKey, d);
    }
    d.addNull();
    maybeFlushIfOverflow();
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
