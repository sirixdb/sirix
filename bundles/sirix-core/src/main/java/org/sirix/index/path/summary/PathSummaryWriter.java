package org.sirix.index.path.summary;

import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.Utils;
import org.sirix.access.trx.node.NodeFactory;
import org.sirix.api.*;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.*;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.PathKindFilter;
import org.sirix.axis.filter.PathNameFilter;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.immutable.json.ImmutableArrayNode;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.settings.Fixed;

import javax.xml.namespace.QName;

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
   * Sirix {@link PageTrx}.
   */
  private final PageTrx pageTrx;

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
   * Constructor.
   *
   * @param pageTrx     Sirix {@link PageTrx}
   * @param resMgr      The resource manager
   * @param nodeFactory The node factory to create path nodes
   * @param rtx         the read-only trx
   */
  public PathSummaryWriter(final PageTrx pageTrx, final ResourceSession<R, ? extends NodeTrx> resMgr,
      final NodeFactory nodeFactory, final R rtx) {
    this.pageTrx = requireNonNull(pageTrx);
    pathSummaryReader = PathSummaryReader.getInstance(pageTrx, resMgr);
    nodeRtx = requireNonNull(rtx);
    this.nodeFactory = requireNonNull(nodeFactory);
    storeChildCount = resMgr.getResourceConfig().storeChildCount();
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
   * @param name     the name of the path node to search for
   * @param pathKind the kind of the path node to search for
   * @return a path node key of the found node, or the path node key of a new inserted node
   * @throws SirixException if anything went wrong
   */
  public long getPathNodeKey(final QNm name, final NodeKind pathKind) {
    final NodeKind kind = nodeRtx.getNode().getKind();
    int level = 0;
    if (kind == NodeKind.XML_DOCUMENT || kind == NodeKind.JSON_DOCUMENT) {
      pathSummaryReader.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    } else {
      movePathSummary();
      level = pathSummaryReader.getLevel();
    }

    final long nodeKey = pathSummaryReader.getNodeKey();
    final Axis axis = new FilterAxis<>(new ChildAxis(pathSummaryReader),
                                       new PathNameFilter(pathSummaryReader,
                                                          pathKind == NodeKind.NAMESPACE
                                                              ? name.getPrefix()
                                                              : Utils.buildName(name)),
                                       new PathKindFilter(pathSummaryReader, pathKind));
    long retVal;
    if (axis.hasNext()) {
      retVal = axis.nextLong();
      final PathNode pathNode = pageTrx.prepareRecordForModification(retVal, IndexType.PATH_SUMMARY, 0);
      pathNode.incrementReferenceCount();
      pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
    } else {
      assert nodeKey == pathSummaryReader.getNodeKey();
      insertPathAsFirstChild(name, pathKind, level + 1);
      retVal = pathSummaryReader.getNodeKey();
    }
    return retVal;
  }

  /**
   * Move path summary cursor to the path node which is references by the current node.
   */
  private void movePathSummary() {
    if (nodeRtx.getKind() == NodeKind.OBJECT) {
      nodeRtx.moveToParent();
    }

    if (nodeRtx.getKind() == NodeKind.OBJECT_KEY) {
      pathSummaryReader.moveTo(((ImmutableObjectKeyNode) nodeRtx.getNode()).getPathNodeKey());
    } else if (nodeRtx.getKind() == NodeKind.ARRAY) {
      pathSummaryReader.moveTo(((ImmutableArrayNode) nodeRtx.getNode()).getPathNodeKey());
    } else if (nodeRtx.getNode() instanceof ImmutableNameNode) {
      pathSummaryReader.moveTo(((ImmutableNameNode) nodeRtx.getNode()).getPathNodeKey());
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * Insert a path node as first child.
   *
   * @param name     {@link QNm} of the path node (not stored) twice
   * @param pathKind kind of node to index
   * @param level    level in the path summary
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
      final StructNode parent = pageTrx.prepareRecordForModification(newNode.getParentKey(), IndexType.PATH_SUMMARY, 0);
      if (storeChildCount) {
        parent.incrementChildCount();
      }
      parent.setFirstChildKey(newNode.getNodeKey());
      pathSummaryReader.putMapping(parent.getNodeKey(), parent);

      if (strucNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            pageTrx.prepareRecordForModification(strucNode.getRightSiblingKey(), IndexType.PATH_SUMMARY, 0);
        rightSiblingNode.setLeftSiblingKey(newNode.getNodeKey());
        pathSummaryReader.putMapping(rightSiblingNode.getNodeKey(), rightSiblingNode);
      }
      if (strucNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            pageTrx.prepareRecordForModification(strucNode.getLeftSiblingKey(), IndexType.PATH_SUMMARY, 0);
        leftSiblingNode.setRightSiblingKey(newNode.getNodeKey());
        pathSummaryReader.putMapping(leftSiblingNode.getNodeKey(), leftSiblingNode);
      }
    }
  }

  /**
   * Adapt path summary either for moves or {@code setQName(QName)}.
   *
   * @param node   the node for which the path node needs to be adapted
   * @param name   the new {@link QName} in case of a new one is set, the old {@link QName} otherwise
   * @param uriKey uriKey of the new node
   * @throws SirixException       if a Sirix operation fails
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
        processFoundPathNode(oldPathNodeKey,
                             pathSummaryReader.getNodeKey(),
                             node.getNodeKey(),
                             name,
                             uriKey,
                             prefixKey,
                             localNameKey);
      } else {
        if (pathSummaryReader.getKind() == NodeKind.XML_DOCUMENT
            || pathSummaryReader.getKind() == NodeKind.JSON_DOCUMENT) {
          insertPathAsFirstChild(name, node.getKind(), 1);
        } else {
          /* The path summary just needs to be updated for the new renamed node. */
          pathSummaryReader.moveTo(oldPathNodeKey);
          final PathNode pathNode =
              pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
          pathNode.setPrefixKey(prefixKey);
          pathNode.setLocalNameKey(localNameKey);
          pathNode.setURIKey(uriKey);
          pathNode.setName(name);
          pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
          pathSummaryReader.putQNameMapping(pathNode, name);
        }
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
        processFoundPathNode(oldPathNodeKey,
                             pathSummaryReader.getNodeKey(),
                             node.getNodeKey(),
                             name,
                             uriKey,
                             prefixKey,
                             localNameKey);
      } else {
        long nodeKey = decrementReferenceCountOrRemove(node);

        pathSummaryReader.moveTo(nodeKey);

        // Not found => create new path nodes for the whole subtree.
        boolean firstRun = true;
        for (final Axis descendants = new DescendantAxis(nodeRtx, IncludeSelf.YES); descendants.hasNext(); ) {
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

        // /*
        // * Remove path nodes with zero node references.
        // *
        // * (TODO: Johannes: might not be necessary, as it's likely that future
        // * updates will reinsert the path).
        // */
        // for (final long key : nodesToDelete) {
        // if (mPathSummaryReader.moveTo(key).hasMoved()) {
        // removePathSummaryNode(Remove.NO);
        // }
        // }

        pathSummaryReader.moveTo(nodeKey);
      }
    }
  }

  // Decrement reference count or remove path summary node.
  private long decrementReferenceCountOrRemove(final ImmutableNameNode node) {
    long nodeKey = pathSummaryReader.getNodeKey();
    nodeRtx.moveTo(node.getNodeKey());

    for (final Axis descendants = new PostOrderAxis(nodeRtx, IncludeSelf.YES); descendants.hasNext(); ) {
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
   * @param oldNodeKey     key of old node
   * @param uriKey         key of URI
   * @param prefixKey      key of prefix
   * @param localNameKey   key of local name
   * @throws SirixException if Sirix fails to do so
   */
  private void processFoundPathNode(final @NonNegative long oldPathNodeKey, final @NonNegative long newPathNodeKey,
      final @NonNegative long oldNodeKey, final QNm name, final int uriKey, final int prefixKey,
      final int localNameKey) {
    nodeRtx.moveTo(oldNodeKey);

    // Set new reference count of the root.
    final PathNode currNode =
        pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
    currNode.setLocalNameKey(localNameKey);
    currNode.setPrefixKey(prefixKey);
    currNode.setURIKey(uriKey);
    currNode.setName(name);
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
           new LevelOrderAxis.Builder(nodeRtx).includeSelf().build(); levelOrderAxis.hasNext(); ) {
        levelOrderAxis.nextLong();

        if (nodeRtx.getNode() instanceof ImmutableNameNode) {
          adaptPathSummary(levelOrderAxis.getCurrentLevel(), pathRootNodeKey);

          processElementNonStructuralNodes(pathRootNodeKey, levelOrderAxis.getCurrentLevel());
        }
      }
    } else if (movedNodeCursorToFirstChild) {
      for (final LevelOrderAxis levelOrderAxis =
           new LevelOrderAxis.Builder(nodeRtx).includeSelf().build(); levelOrderAxis.hasNext(); ) {
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
    final NameNode node = pageTrx.prepareRecordForModification(nodeRtx.getNodeKey(), IndexType.DOCUMENT, -1);
    node.setPathNodeKey(pathSummaryReader.getNodeKey());
  }

  private void setReferenceCountToOne() {
    final PathNode currNode =
        pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(1);
    pathSummaryReader.putMapping(currNode.getNodeKey(), currNode);
  }

  private void increaseReferenceCount() {
    // Set new reference count.
    final PathNode currNode =
        pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
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
   * @param nodeKey  the nodeKey of the node to adapt
   * @param nodeKind the kind of the node to adapt
   * @throws SirixException if anything fails
   */
  private void resetPathNodeKey(final @NonNegative long nodeKey, final NodeKind nodeKind) {
    if (nodeKind == NodeKind.ATTRIBUTE || nodeKind == NodeKind.ELEMENT || nodeKind == NodeKind.NAMESPACE) {
      final NameNode currNode = pageTrx.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
      currNode.setPathNodeKey(pathSummaryReader.getNodeKey());
    } else {
      final ObjectKeyNode currNode = pageTrx.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
      currNode.setPathNodeKey(pathSummaryReader.getNodeKey());
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
      for (final Axis axis = new PostOrderAxis(pathSummaryReader); axis.hasNext(); ) {
        axis.nextLong();
        pathSummaryReader.removeMapping(pathSummaryReader.getNodeKey());
        pathSummaryReader.removeQNameMapping(pathSummaryReader.getPathNode(), pathSummaryReader.getName());
        pageTrx.removeRecord(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
      }
    }

    // Adapt left sibling node if there is one.
    if (pathSummaryReader.hasLeftSibling()) {
      final StructNode leftSibling =
          pageTrx.prepareRecordForModification(pathSummaryReader.getLeftSiblingKey(), IndexType.PATH_SUMMARY, 0);
      leftSibling.setRightSiblingKey(pathSummaryReader.getRightSiblingKey());
      pathSummaryReader.putMapping(leftSibling.getNodeKey(), leftSibling);
    }

    // Adapt right sibling node if there is one.
    if (pathSummaryReader.hasRightSibling()) {
      final StructNode rightSibling =
          pageTrx.prepareRecordForModification(pathSummaryReader.getRightSiblingKey(), IndexType.PATH_SUMMARY, 0);
      rightSibling.setLeftSiblingKey(pathSummaryReader.getLeftSiblingKey());
      pathSummaryReader.putMapping(rightSibling.getNodeKey(), rightSibling);
    }

    // Adapt parent. If node has no left sibling it is a first child.
    StructNode parent =
        pageTrx.prepareRecordForModification(pathSummaryReader.getParentKey(), IndexType.PATH_SUMMARY, 0);
    if (!pathSummaryReader.hasLeftSibling()) {
      parent.setFirstChildKey(pathSummaryReader.getRightSiblingKey());
    }
    if (storeChildCount) {
      parent.decrementChildCount();
    }
    pathSummaryReader.putMapping(parent.getNodeKey(), parent);

    // Remove node.
    pathSummaryReader.removeMapping(pathSummaryReader.getNodeKey());
    pathSummaryReader.removeQNameMapping(pathSummaryReader.getPathNode(), pathSummaryReader.getName());
    pageTrx.removeRecord(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);

    //    pathSummaryReader.moveToDocumentRoot();
    //
    //    System.out.println("removed: =====================");
    //
    //    for (final var descendantAxis = new DescendantAxis(pathSummaryReader); descendantAxis.hasNext(); ) {
    //      descendantAxis.nextLong();
    //      System.out.println("path: " + pathSummaryReader.getPath());
    //      System.out.println("nodeKey: " + pathSummaryReader.getNodeKey());
    //      System.out.println("rightSiblingKey: " + pathSummaryReader.getRightSiblingKey());
    //      System.out.println("references: " + pathSummaryReader.getReferences());
    //    }
  }

  private void deleteOrDecrement() {
    if (nodeRtx.getNode() instanceof ImmutableNameNode) {
      movePathSummary();
      if (pathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.NO);
      } else {
        final PathNode pathNode =
            pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
        pathNode.decrementReferenceCount();
        pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      }
    }
  }

  /**
   * Decrements the reference-counter of the node or removes the path node if the reference-counter
   * would be zero otherwise.
   *
   * @param node     node which is going to be removed from the storage
   * @throws SirixException if anything went wrong
   */
  public void remove(final ImmutableNameNode node) {
    if (pathSummaryReader.moveTo(node.getPathNodeKey())) {
      if (pathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.YES);
      } else {
        assert pathSummaryReader.getReferences() > 1;
        final PathNode pathNode =
            pageTrx.prepareRecordForModification(pathSummaryReader.getNodeKey(), IndexType.PATH_SUMMARY, 0);
        pathNode.decrementReferenceCount();
        pathSummaryReader.putMapping(pathNode.getNodeKey(), pathNode);
      }
    }
  }

  @Override
  protected PathSummaryReader delegate() {
    return pathSummaryReader;
  }
}
