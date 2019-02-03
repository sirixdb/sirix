package org.sirix.index.path.summary;

import static com.google.common.base.Preconditions.checkNotNull;
import javax.annotation.Nonnegative;
import javax.xml.namespace.QName;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Utils;
import org.sirix.access.trx.node.NodeFactory;
import org.sirix.access.trx.node.xdm.InsertPos;
import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.PathKindFilter;
import org.sirix.axis.filter.PathNameFilter;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.immutable.xdm.ImmutableElement;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.page.NamePage;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

/**
 * Path summary writer organizing the path classes of a resource.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathSummaryWriter<R extends NodeCursor & NodeReadOnlyTrx> extends AbstractForwardingPathSummaryReader {

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

    /** A new {@link QName} is set. */
    SETNAME,
  }

  /** Determines if a path subtree must be deleted or not. */
  private enum RemoveSubtreePath {
    /** Yes, it must be deleted. */
    YES,

    /** No, it must not be deleted. */
    NO
  }

  /** Sirix {@link PageTrx}. */
  private final PageTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /** Sirix {@link PathSummaryReader}. */
  private final PathSummaryReader mPathSummaryReader;

  /** Sirix {@link NodeFactory} to create new nodes. */
  private final NodeFactory mNodeFactory;

  /** The read-only trx. */
  private final R mNodeRtx;

  /**
   * Constructor.
   *
   * @param pageWriteTrx Sirix {@link PageTrx}
   * @param resMgr The resource manager
   * @param nodeFactory The node factory to create path nodes
   * @param rtx the read-only trx
   */
  public PathSummaryWriter(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
      final ResourceManager<R, ? extends NodeTrx> resMgr, final NodeFactory nodeFactory, final R rtx) {
    mPageWriteTrx = checkNotNull(pageWriteTrx);
    mPathSummaryReader = PathSummaryReader.getInstance(pageWriteTrx, resMgr);
    mNodeRtx = checkNotNull(rtx);
    mNodeFactory = checkNotNull(nodeFactory);
  }

  /**
   * Get the path summary reader.
   *
   * @return {@link PathSummaryReader} instance
   */
  public PathSummaryReader getPathSummary() {
    return mPathSummaryReader;
  }

  /**
   * Insert a new path node or increment the counter of an existing node and return the path node key.
   *
   * @param name the name of the path node to search for
   * @param pathKind the kind of the path node to search for
   * @return a path node key of the found node, or the path node key of a new inserted node
   * @throws SirixException if anything went wrong
   */
  public long getPathNodeKey(final QNm name, final Kind pathKind) {
    final Kind kind = mNodeRtx.getNode().getKind();
    int level = 0;
    if (kind == Kind.XDM_DOCUMENT || kind == Kind.JSON_DOCUMENT) {
      mPathSummaryReader.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
    } else {
      movePathSummary();
      level = mPathSummaryReader.getLevel();
    }

    final long nodeKey = mPathSummaryReader.getNodeKey();
    final Axis axis = new FilterAxis<>(new ChildAxis(mPathSummaryReader),
        new PathNameFilter(mPathSummaryReader, pathKind == Kind.NAMESPACE
            ? name.getPrefix()
            : Utils.buildName(name)),
        new PathKindFilter(mPathSummaryReader, pathKind));
    long retVal = nodeKey;
    if (axis.hasNext()) {
      axis.next();
      retVal = mPathSummaryReader.getNodeKey();
      final PathNode pathNode =
          (PathNode) mPageWriteTrx.prepareEntryForModification(retVal, PageKind.PATHSUMMARYPAGE, 0);
      pathNode.incrementReferenceCount();
    } else {
      assert nodeKey == mPathSummaryReader.getNodeKey();
      insertPathAsFirstChild(name, pathKind, level + 1);
      retVal = mPathSummaryReader.getNodeKey();
    }
    return retVal;
  }

  /**
   * Move path summary cursor to the path node which is references by the current node.
   */
  private void movePathSummary() {
    if (mNodeRtx.getNode() instanceof ImmutableNameNode) {
      mPathSummaryReader.moveTo(((ImmutableNameNode) mNodeRtx.getNode()).getPathNodeKey());
    } else if (mNodeRtx.getKind() == Kind.JSON_OBJECT_KEY) {
      mPathSummaryReader.moveTo(((ObjectKeyNode) mNodeRtx.getNode()).getPathNodeKey());
    } else if (mNodeRtx.getKind() == Kind.JSON_ARRAY) {
      mPathSummaryReader.moveTo(((ArrayNode) mNodeRtx.getNode()).getPathNodeKey());
    } else {
      throw new IllegalStateException();
    }
  }

  /**
   * Insert a path node as first child.
   *
   * @param name {@link QNm} of the path node (not stored) twice
   * @param pathKind kind of node to index
   * @param level level in the path summary
   * @return this {@link WriteTransaction} instance
   * @throws SirixException if an I/O error occurs
   */
  public PathSummaryWriter<R> insertPathAsFirstChild(final QNm name, final Kind pathKind, final int level) {
    final long parentKey = mPathSummaryReader.getNodeKey();
    final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long rightSibKey = mPathSummaryReader.getFirstChildKey();
    final PathNode node = mNodeFactory.createPathNode(parentKey, leftSibKey, rightSibKey, name, pathKind, level);

    mPathSummaryReader.putMapping(node.getNodeKey(), node);
    mPathSummaryReader.moveTo(node.getNodeKey());
    adaptForInsert(node, InsertPos.ASFIRSTCHILD, PageKind.PATHSUMMARYPAGE);
    mPathSummaryReader.moveTo(node.getNodeKey());
    mPathSummaryReader.putQNameMapping(node, name);

    return this;
  }

  /**
   * Adapting everything for insert operations.
   *
   * @param newNode pointer of the new node to be inserted
   * @param insertPos determines the position where to insert
   * @param pageKind kind of subtree root page
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final Node newNode, final InsertPos insertPos, final PageKind pageKind) {
    assert newNode != null;
    assert insertPos != null;
    assert pageKind != null;

    if (newNode instanceof StructNode) {
      final StructNode strucNode = (StructNode) newNode;
      final StructNode parent =
          (StructNode) mPageWriteTrx.prepareEntryForModification(newNode.getParentKey(), pageKind, 0);
      parent.incrementChildCount();
      if (insertPos == InsertPos.ASFIRSTCHILD) {
        parent.setFirstChildKey(newNode.getNodeKey());
      }

      if (strucNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            (StructNode) mPageWriteTrx.prepareEntryForModification(strucNode.getRightSiblingKey(), pageKind, 0);
        rightSiblingNode.setLeftSiblingKey(newNode.getNodeKey());
      }
      if (strucNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            (StructNode) mPageWriteTrx.prepareEntryForModification(strucNode.getLeftSiblingKey(), pageKind, 0);
        leftSiblingNode.setRightSiblingKey(newNode.getNodeKey());
      }
    }
  }

  /**
   * Adapt path summary either for moves or {@code setQName(QName)}.
   *
   * @param node the node for which the path node needs to be adapted
   * @param name the new {@link QName} in case of a new one is set, the old {@link QName} otherwise
   * @param nameKey nameKey of the new node
   * @param uriKey uriKey of the new node
   * @throws SirixException if a Sirix operation fails
   * @throws NullPointerException if {@code pNode} or {@code pQName} is null
   */
  public void adaptPathForChangedNode(final ImmutableNameNode node, final QNm name, final int uriKey,
      final int prefixKey, final int localNameKey, final OPType type) {
    // Possibly either reset a path node or decrement its reference counter
    // and search for the new path node or insert it.
    movePathSummary();

    final long oldPathNodeKey = mPathSummaryReader.getNodeKey();

    // Only one path node is referenced (after a setQName(QName) the
    // reference-counter would be 0).
    if (type == OPType.SETNAME && mPathSummaryReader.getReferences() == 1) {
      moveSummaryGetLevel(node);
      // Search for new path entry.
      final Axis axis = new FilterAxis<>(new ChildAxis(mPathSummaryReader),
          new PathNameFilter(mPathSummaryReader, Utils.buildName(name)),
          new PathKindFilter(mPathSummaryReader, node.getKind()));
      if (axis.hasNext()) {
        axis.next();

        long nodeKey = decrementReferenceCountOrRemove(node);

        mPathSummaryReader.moveTo(nodeKey);

        // Found node.
        processFoundPathNode(oldPathNodeKey, mPathSummaryReader.getNodeKey(), node.getNodeKey(), uriKey, prefixKey,
            localNameKey, RemoveSubtreePath.YES, type);
      } else {
        if (mPathSummaryReader.getKind() != Kind.XDM_DOCUMENT) {
          /* The path summary just needs to be updated for the new renamed node. */
          mPathSummaryReader.moveTo(oldPathNodeKey);
          final PathNode pathNode =
              (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
                  PageKind.PATHSUMMARYPAGE, 0);
          pathNode.setPrefixKey(prefixKey);
          pathNode.setLocalNameKey(localNameKey);
          pathNode.setURIKey(uriKey);
        }
      }
    } else {
      int level = moveSummaryGetLevel(node);
      // TODO: Johannes: Optimize? (either use this or use the name-mapping,
      // depending on the number of child nodes or nodes with a certain name).

      // Search for new path entry.
      final Axis axis = new FilterAxis<>(new ChildAxis(mPathSummaryReader),
          new PathNameFilter(mPathSummaryReader, Utils.buildName(name)),
          new PathKindFilter(mPathSummaryReader, node.getKind()));
      if (axis.hasNext()) {
        axis.next();

        long nodeKey = decrementReferenceCountOrRemove(node);

        mPathSummaryReader.moveTo(nodeKey);

        // Found node.
        processFoundPathNode(oldPathNodeKey, mPathSummaryReader.getNodeKey(), node.getNodeKey(), uriKey, prefixKey,
            localNameKey, RemoveSubtreePath.NO, type);
      } else {
        long nodeKey = decrementReferenceCountOrRemove(node);

        mPathSummaryReader.moveTo(nodeKey);

        // Not found => create new path nodes for the whole subtree.
        boolean firstRun = true;
        for (final Axis descendants = new DescendantAxis(mNodeRtx, IncludeSelf.YES); descendants.hasNext();) {
          descendants.next();
          if (mNodeRtx.getKind() == Kind.ELEMENT || mNodeRtx.getKind() == Kind.JSON_OBJECT_KEY) {
            // Path Summary : New mapping.
            if (firstRun) {
              insertPathAsFirstChild(name, mNodeRtx.getKind(), ++level);
              nodeKey = mPathSummaryReader.getNodeKey();
            } else {
              insertPathAsFirstChild(mNodeRtx.getName(), mNodeRtx.getKind(), ++level);
            }
            resetPathNodeKey(mNodeRtx.getNodeKey());

            if (mNodeRtx instanceof XdmNodeReadOnlyTrx) {
              final XdmNodeReadOnlyTrx rtx = (XdmNodeReadOnlyTrx) mNodeRtx;

              // Namespaces.
              for (int i = 0, nsps = rtx.getNamespaceCount(); i < nsps; i++) {
                rtx.moveToNamespace(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), Kind.NAMESPACE, level + 1);
                resetPathNodeKey(rtx.getNodeKey());
                rtx.moveToParent();
                mPathSummaryReader.moveToParent();
              }

              // Attributes.
              for (int i = 0, atts = rtx.getAttributeCount(); i < atts; i++) {
                rtx.moveToAttribute(i);
                // Path Summary : New mapping.
                insertPathAsFirstChild(rtx.getName(), Kind.ATTRIBUTE, level + 1);
                resetPathNodeKey(rtx.getNodeKey());
                rtx.moveToParent();
                mPathSummaryReader.moveToParent();
              }
            }

            if (firstRun) {
              firstRun = false;
            } else {
              mPathSummaryReader.moveToParent();
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

        mPathSummaryReader.moveTo(nodeKey);
      }
    }
  }

  // Decrement reference count or remove path summary node.
  private long decrementReferenceCountOrRemove(final ImmutableNameNode node) {
    long nodeKey = mPathSummaryReader.getNodeKey();
    mNodeRtx.moveTo(node.getNodeKey());

    for (final Axis descendants = new PostOrderAxis(mNodeRtx, IncludeSelf.YES); descendants.hasNext();) {
      descendants.next();

      if (mNodeRtx.getKind() == Kind.ELEMENT) {
        final XdmNodeReadOnlyTrx rtx = (XdmNodeReadOnlyTrx) mNodeRtx;
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
   * @param remove determines if a {@link PathNode} must be removed or not
   * @param type type of operation
   * @throws SirixException if Sirix fails to do so
   */
  private void processFoundPathNode(final @Nonnegative long oldPathNodeKey, final @Nonnegative long newPathNodeKey,
      final @Nonnegative long oldNodeKey, final int uriKey, final int prefixKey, final int localNameKey,
      final RemoveSubtreePath remove, final OPType type) {
    mNodeRtx.moveTo(oldNodeKey);

    // Set new reference count of the root.
    final PathNode currNode = (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
        PageKind.PATHSUMMARYPAGE, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
    currNode.setLocalNameKey(localNameKey);
    currNode.setPrefixKey(prefixKey);
    currNode.setURIKey(uriKey);

    final long pathNodeKey = currNode.getNodeKey();

    processElementNonStructuralNodes(pathNodeKey, 0);

    // For all old path nodes: Merge paths and adapt reference counts.
    final boolean movedNodeCursorToFirstChild = mNodeRtx.moveToFirstChild().hasMoved();
    final boolean movedPathSummaryToFirstChild = mPathSummaryReader.moveToFirstChild().hasMoved();

    if (movedNodeCursorToFirstChild && movedPathSummaryToFirstChild) {
      final long pathRootNodeKey = mPathSummaryReader.getNodeKey();

      for (final LevelOrderAxis levelOrderAxis =
          new LevelOrderAxis.Builder(mNodeRtx).includeSelf().build(); levelOrderAxis.hasNext();) {
        levelOrderAxis.next();

        if (mNodeRtx.getNode() instanceof ImmutableNameNode) {
          adaptPathSummary(levelOrderAxis.getCurrentLevel(), pathRootNodeKey);

          processElementNonStructuralNodes(pathRootNodeKey, levelOrderAxis.getCurrentLevel());
        }
      }
    } else if (movedNodeCursorToFirstChild) {
      for (final LevelOrderAxis levelOrderAxis =
          new LevelOrderAxis.Builder(mNodeRtx).includeSelf().build(); levelOrderAxis.hasNext();) {
        levelOrderAxis.next();

        if (mNodeRtx.getNode() instanceof ImmutableNameNode) {
          adaptForNewPathNode();

          processElementNonStructuralNodes(mPathSummaryReader.getNodeKey(), levelOrderAxis.getCurrentLevel());
        }
      }
    } else if (movedPathSummaryToFirstChild) {
      // Only move back.
      mPathSummaryReader.moveToParent();
    }

    mPathSummaryReader.moveTo(pathNodeKey);
  }

  private void processElementNonStructuralNodes(final long pathRootNodeKey, final int level) {
    if (mNodeRtx.getNode().getKind() == Kind.ELEMENT) {
      final XdmNodeReadOnlyTrx rtx = (XdmNodeReadOnlyTrx) mNodeRtx;
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
        new FilterAxis<>(new LevelOrderAxis.Builder(mPathSummaryReader).filterLevel(level).includeSelf().build(),
            new PathNameFilter(mPathSummaryReader, Utils.buildName(mNodeRtx.getName())),
            new PathKindFilter(mPathSummaryReader, mNodeRtx.getKind()));
    if (axis.hasNext()) {
      axis.next();

      adaptForFoundPathNode();
    } else {
      adaptForNewPathNode();
    }

    mPathSummaryReader.moveTo(newPathNodeKey);
  }

  private void adaptForNewPathNode() {
    // Move to parent path node.
    moveToPathNodeOfParentNode();

    // Insert new node.
    insertPathAsFirstChild(mNodeRtx.getName(), mNodeRtx.getKind(), mPathSummaryReader.getLevel() + 1);

    // Set reference count to one.
    setReferenceCountToOne();

    // Set new path node key.
    setNewPathNodeKey();
  }

  private void moveToPathNodeOfParentNode() {
    final long nodeKey = mNodeRtx.getNodeKey();

    final long pathNodeKey;

    if (mNodeRtx instanceof XdmNodeReadOnlyTrx)
      pathNodeKey = ((XdmNodeReadOnlyTrx) mNodeRtx.moveToParent().getCursor()).getPathNodeKey();
    else if (mNodeRtx instanceof JsonNodeReadOnlyTrx)
      pathNodeKey = ((JsonNodeReadOnlyTrx) mNodeRtx.moveToParent().getCursor()).getPathNodeKey();
    else
      throw new IllegalStateException("Node transaction kind not known.");

    mPathSummaryReader.moveTo(pathNodeKey);
    mNodeRtx.moveTo(nodeKey);
  }

  private void adaptForFoundPathNode() {
    // Increase reference count.
    increaseReferenceCount();

    // Set new path node.
    resetPathNodeKey(mNodeRtx.getNodeKey());
  }

  private void setNewPathNodeKey() {
    final NameNode node =
        (NameNode) mPageWriteTrx.prepareEntryForModification(mNodeRtx.getNodeKey(), PageKind.RECORDPAGE, -1);
    node.setPathNodeKey(mPathSummaryReader.getNodeKey());
  }

  private void setReferenceCountToOne() {
    final PathNode currNode = (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
        PageKind.PATHSUMMARYPAGE, 0);
    currNode.setReferenceCount(1);
  }

  private void increaseReferenceCount() {
    // Set new reference count.
    final PathNode currNode = (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
        PageKind.PATHSUMMARYPAGE, 0);
    currNode.setReferenceCount(currNode.getReferences() + 1);
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
    mNodeRtx.moveToParent();
    int level = 0;
    if (mNodeRtx.getKind() == Kind.XDM_DOCUMENT) {
      mPathSummaryReader.moveToDocumentRoot();
    } else {
      movePathSummary();
      level = mPathSummaryReader.getLevel();
    }
    mNodeRtx.moveTo(node.getNodeKey());
    return level;
  }

  /**
   * Reset a path node key.
   *
   * @param nodeKey the nodeKey of the node to adapt
   * @throws SirixException if anything fails
   */
  private void resetPathNodeKey(final @Nonnegative long nodeKey) {
    final NameNode currNode = (NameNode) mPageWriteTrx.prepareEntryForModification(nodeKey, PageKind.RECORDPAGE, -1);
    currNode.setPathNodeKey(mPathSummaryReader.getNodeKey());
  }

  /**
   * Remove a path summary node with the specified PCR.
   *
   * @throws SirixException if Sirix fails to remove the path node
   */
  private void removePathSummaryNode(final RemoveSubtreePath remove) {
    // Remove all descendant nodes.
    if (remove == RemoveSubtreePath.YES) {
      for (final Axis axis = new DescendantAxis(mPathSummaryReader); axis.hasNext();) {
        axis.next();
        mPathSummaryReader.removeMapping(mPathSummaryReader.getNodeKey());
        mPathSummaryReader.removeQNameMapping(mPathSummaryReader.getPathNode(), mPathSummaryReader.getName());
        mPageWriteTrx.removeEntry(mPathSummaryReader.getNodeKey(), PageKind.PATHSUMMARYPAGE, 0);
      }
    }

    // Adapt left sibling node if there is one.
    if (mPathSummaryReader.hasLeftSibling()) {
      final StructNode leftSibling =
          (StructNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getLeftSiblingKey(),
              PageKind.PATHSUMMARYPAGE, 0);
      leftSibling.setRightSiblingKey(mPathSummaryReader.getRightSiblingKey());
    }

    // Adapt right sibling node if there is one.
    if (mPathSummaryReader.hasRightSibling()) {
      final StructNode rightSibling =
          (StructNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getRightSiblingKey(),
              PageKind.PATHSUMMARYPAGE, 0);
      rightSibling.setLeftSiblingKey(mPathSummaryReader.getLeftSiblingKey());
    }

    // Adapt parent. If node has no left sibling it is a first child.
    StructNode parent = (StructNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getParentKey(),
        PageKind.PATHSUMMARYPAGE, 0);
    if (!mPathSummaryReader.hasLeftSibling()) {
      parent.setFirstChildKey(mPathSummaryReader.getRightSiblingKey());
    }
    parent.decrementChildCount();

    // Remove node.
    mPathSummaryReader.removeMapping(mPathSummaryReader.getNodeKey());
    mPathSummaryReader.removeQNameMapping(mPathSummaryReader.getPathNode(), mPathSummaryReader.getName());
    mPageWriteTrx.removeEntry(mPathSummaryReader.getNodeKey(), PageKind.PATHSUMMARYPAGE, 0);
  }

  private void deleteOrDecrement() {
    if (mNodeRtx.getNode() instanceof ImmutableNameNode) {
      movePathSummary();
      if (mPathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.NO);
      } else {
        final PathNode pathNode = (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
            PageKind.PATHSUMMARYPAGE, 0);
        pathNode.decrementReferenceCount();
      }
    }
  }

  /**
   * Decrements the reference-counter of the node or removes the path node if the reference-counter
   * would be zero otherwise.
   *
   * @param node node which is going to removed from the storage
   * @param nodeKind the node kind
   * @param page the name page
   * @throws SirixException if anything went wrong
   */
  public void remove(final NameNode node, final Kind nodeKind, final NamePage page) {
    if (mPathSummaryReader.moveTo(node.getPathNodeKey()).hasMoved()) {
      if (mPathSummaryReader.getReferences() == 1) {
        removePathSummaryNode(RemoveSubtreePath.YES);
      } else {
        assert page.getCount(node.getLocalNameKey(), nodeKind) != 0;
        if (mPathSummaryReader.getReferences() > 1) {
          final PathNode pathNode =
              (PathNode) mPageWriteTrx.prepareEntryForModification(mPathSummaryReader.getNodeKey(),
                  PageKind.PATHSUMMARYPAGE, 0);
          pathNode.decrementReferenceCount();
        }
      }
    }
  }

  @Override
  protected PathSummaryReader delegate() {
    return mPathSummaryReader;
  }
}
