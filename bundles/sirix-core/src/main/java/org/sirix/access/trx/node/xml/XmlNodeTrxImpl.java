/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.access.trx.node.xml;

import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.trx.node.*;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.api.Axis;
import org.sirix.api.Movement;
import org.sirix.api.PageTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathSummaryWriter;
import org.sirix.index.path.summary.PathSummaryWriter.OPType;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.immutable.xml.ImmutableAttributeNode;
import org.sirix.node.immutable.xml.ImmutableNamespace;
import org.sirix.node.interfaces.*;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.*;
import org.sirix.page.NamePage;
import org.sirix.service.InsertPosition;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.XMLToken;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * Single-threaded instance of only write-transaction per resource, thus it is not thread-safe.
 * </p>
 *
 * <p>
 * If auto-commit is enabled, that is a scheduled commit(), all access to public methods is
 * synchronized, such that a commit() and another method doesn't interfere, which could produce
 * severe inconsistencies.
 * </p>
 *
 * <p>
 * All methods throw {@link NullPointerException}s in case of null values for reference parameters.
 * </p>
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
@SuppressWarnings("StatementWithEmptyBody")
final class XmlNodeTrxImpl extends
    AbstractNodeTrxImpl<XmlNodeReadOnlyTrx, XmlNodeTrx, XmlNodeFactory, ImmutableXmlNode, InternalXmlNodeReadOnlyTrx>
    implements InternalXmlNodeTrx, ForwardingXmlNodeReadOnlyTrx {

  /**
   * The deweyID manager.
   */
  private final XmlDeweyIDManager deweyIDManager;

  /**
   * Determines if text values should be compressed or not.
   */
  private final boolean useTextCompression;

  /**
   * Flag to decide whether to store child count.
   */
  private final boolean storeChildCount;

  /**
   * Constructor.
   *
   * @param resourceManager   the resource manager this transaction is bound to
   * @param nodeReadOnlyTrx   {@link PageTrx} to interact with the page layer
   * @param pathSummaryWriter the path summary writer
   * @param maxNodeCount      maximum number of node modifications before auto commit
   * @param nodeHashing       hashes node contents
   * @param nodeFactory       the node factory used to create nodes
   * @throws SirixIOException    if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  XmlNodeTrxImpl(final InternalResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager,
      final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx, final PathSummaryWriter<XmlNodeReadOnlyTrx> pathSummaryWriter,
      final @NonNegative int maxNodeCount, @Nullable final Lock transactionLock, final Duration afterCommitDelay,
      final @NonNull XmlNodeHashing nodeHashing, final XmlNodeFactory nodeFactory,
      final @NonNull AfterCommitState afterCommitState, final RecordToRevisionsIndex nodeToRevisionsIndex) {

    super(Executors.defaultThreadFactory(),
          resourceManager.getResourceConfig().hashType,
          nodeReadOnlyTrx,
          nodeReadOnlyTrx,
          resourceManager,
          afterCommitState,
          nodeHashing,
          pathSummaryWriter,
          nodeFactory,
          nodeToRevisionsIndex,
          transactionLock,
          afterCommitDelay,
          maxNodeCount);

    indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    storeChildCount = this.resourceManager.getResourceConfig().storeChildCount();

    useTextCompression = resourceManager.getResourceConfig().useTextCompression;
    deweyIDManager = new XmlDeweyIDManager(this);

  }

  @Override
  public XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    return nodeReadOnlyTrx;
  }

  @Override
  public XmlNodeTrx moveSubtreeToFirstChild(final @NonNegative long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      checkArgument(fromKey >= 0 && fromKey <= getMaxNodeKey(), "Argument must be a valid node key!");

      checkArgument(fromKey != getCurrentNode().getNodeKey(), "Can't move itself to right sibling of itself!");

      final DataRecord node = pageTrx.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist!");
      }

      final var nodeToMove = node;
      if (nodeToMove instanceof StructNode && getCurrentNode().getKind() == NodeKind.ELEMENT) {
        // Safe to cast (because StructNode is a subtype of Node).
        checkAncestors((Node) nodeToMove);
        checkAccessAndCommit();

        final ElementNode nodeAnchor = (ElementNode) getCurrentNode();

        // Check that it's not already the first child.
        if (nodeAnchor.getFirstChildKey() != nodeToMove.getNodeKey()) {
          final StructNode toMove = (StructNode) nodeToMove;

          // Adapt index-structures (before move).
          adaptSubtreeForMove(toMove, ChangeType.DELETE);

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers and merge sibling text nodes.
          adaptForMove(toMove, nodeAnchor, InsertPos.ASFIRSTCHILD);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            pathSummaryWriter.adaptPathForChangedNode(moved,
                                                      getName(),
                                                      moved.getURIKey(),
                                                      moved.getPrefixKey(),
                                                      moved.getLocalNameKey(),
                                                      OPType.MOVED);
          }

          // Adapt index-structures (after move).
          adaptSubtreeForMove(toMove, ChangeType.INSERT);

          // Compute and assign new DeweyIDs.
          if (storeDeweyIDs()) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException(
            "Move is not allowed if moved node is not an ElementNode and the node isn't inserted at an element node!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Adapt subtree regarding the index-structures.
   *
   * @param node node which is moved (either before the move, or after the move)
   * @param type the type of change (either deleted from the old position or inserted into the new
   *             position)
   * @throws SirixIOException if an I/O exception occurs
   */
  private void adaptSubtreeForMove(final Node node, final ChangeType type) throws SirixIOException {
    assert type != null;
    final long beforeNodeKey = getNode().getNodeKey();
    moveTo(node.getNodeKey());
    final Axis axis = new DescendantAxis(this, IncludeSelf.YES);
    while (axis.hasNext()) {
      axis.nextLong();
      for (int i = 0, attCount = getAttributeCount(); i < attCount; i++) {
        moveToAttribute(i);
        final ImmutableAttributeNode att = (ImmutableAttributeNode) getNode();
        indexController.notifyChange(type, att, att.getPathNodeKey());
        moveToParent();
      }
      for (int i = 0, nspCount = getNamespaceCount(); i < nspCount; i++) {
        moveToAttribute(i);
        final ImmutableNamespace nsp = (ImmutableNamespace) getNode();
        indexController.notifyChange(type, nsp, nsp.getPathNodeKey());
        moveToParent();
      }
      long pathNodeKey = -1;
      if (getNode() instanceof ValueNode && getNode().getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        final long nodeKey = getNode().getNodeKey();
        moveToParent();
        pathNodeKey = getNameNode().getPathNodeKey();
        moveTo(nodeKey);
      } else if (getNode() instanceof NameNode) {
        pathNodeKey = getNameNode().getPathNodeKey();
      }
      indexController.notifyChange(type, getNode(), pathNodeKey);
    }
    moveTo(beforeNodeKey);
  }

  @Override
  public XmlNodeTrx moveSubtreeToLeftSibling(final @NonNegative long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()) {
        moveToLeftSibling();
        return moveSubtreeToRightSibling(fromKey);
      } else {
        moveToParent();
        return moveSubtreeToFirstChild(fromKey);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx moveSubtreeToRightSibling(final @NonNegative long fromKey) {
    if (lock != null) {
      lock.lock();
    }

    try {
      if (fromKey < 0 || fromKey > getMaxNodeKey()) {
        throw new IllegalArgumentException("Argument must be a valid node key!");
      }
      if (fromKey == getCurrentNode().getNodeKey()) {
        throw new IllegalArgumentException("Can't move itself to first child of itself!");
      }

      // Save: Every node in the "usual" node page is of type Node.
      final var node = pageTrx.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist: " + fromKey);
      }

      final DataRecord nodeToMove = node;
      if (nodeToMove instanceof StructNode toMove && getCurrentNode() instanceof StructNode nodeAnchor) {
        checkAncestors(toMove);
        checkAccessAndCommit();

        if (nodeAnchor.getRightSiblingKey() != nodeToMove.getNodeKey()) {
          final long parentKey = nodeAnchor.getParentKey();

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers and merge sibling text nodes.
          adaptForMove(toMove, nodeAnchor, InsertPos.ASRIGHTSIBLING);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            final OPType type = moved.getParentKey() == parentKey ? OPType.MOVED_ON_SAME_LEVEL : OPType.MOVED;

            if (type != OPType.MOVED_ON_SAME_LEVEL) {
              pathSummaryWriter.adaptPathForChangedNode(moved,
                                                        getName(),
                                                        moved.getURIKey(),
                                                        moved.getPrefixKey(),
                                                        moved.getLocalNameKey(),
                                                        type);
            }
          }

          // Recompute DeweyIDs if they are used.
          if (storeDeweyIDs()) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException(
            "Move is not allowed if moved node is not an ElementNode or TextNode and the node isn't inserted at an ElementNode or TextNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Adapt hashes for move operation ("remove" phase).
   *
   * @param nodeToMove node which implements {@link StructNode} and is moved
   * @throws SirixIOException if any I/O operation fails
   */
  private void adaptHashesForMove(final StructNode nodeToMove) throws SirixIOException {
    assert nodeToMove != null;
    nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) nodeToMove);
    nodeHashing.adaptHashesWithRemove();
  }

  /**
   * Adapting everything for move operations.
   *
   * @param fromNode  root {@link StructNode} of the subtree to be moved
   * @param toNode    the {@link StructNode} which is the anchor of the new subtree
   * @param insertPos determines if it has to be inserted as a first child or a right sibling
   * @throws SirixException if removing a node fails after merging text nodes
   */
  private void adaptForMove(final StructNode fromNode, final StructNode toNode, final InsertPos insertPos) {
    assert fromNode != null;
    assert toNode != null;
    assert insertPos != null;

    // Modify nodes where the subtree has been moved from.
    // ==============================================================================
    final StructNode parent = pageTrx.prepareRecordForModification(fromNode.getParentKey(), IndexType.DOCUMENT, -1);
    switch (insertPos) {
      case ASRIGHTSIBLING:
        if (fromNode.getParentKey() != toNode.getParentKey() && storeChildCount) {
          parent.decrementChildCount();
        }
        break;
      case ASFIRSTCHILD:
        if (fromNode.getParentKey() != toNode.getNodeKey() && storeChildCount) {
          parent.decrementChildCount();
        }
        break;
      case ASNONSTRUCTURAL:
        // Do not decrement child count.
        break;
      case ASLEFTSIBLING:
      default:
    }

    // Adapt first child key of former parent.
    if (parent.getFirstChildKey() == fromNode.getNodeKey()) {
      parent.setFirstChildKey(fromNode.getRightSiblingKey());
    }

    // Adapt left sibling key of former right sibling.
    if (fromNode.hasRightSibling()) {
      final StructNode rightSibling =
          pageTrx.prepareRecordForModification(fromNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
      rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
    }

    // Adapt right sibling key of former left sibling.
    if (fromNode.hasLeftSibling()) {
      final StructNode leftSibling =
          pageTrx.prepareRecordForModification(fromNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
    }

    // Merge text nodes.
    if (fromNode.hasLeftSibling() && fromNode.hasRightSibling()) {
      moveTo(fromNode.getLeftSiblingKey());
      if (getCurrentNode() != null && getCurrentNode().getKind() == NodeKind.TEXT) {
        final StringBuilder builder = new StringBuilder(getValue());
        moveTo(fromNode.getRightSiblingKey());
        if (getCurrentNode() != null && getCurrentNode().getKind() == NodeKind.TEXT) {
          builder.append(getValue());
          if (fromNode.getRightSiblingKey() == toNode.getNodeKey()) {
            moveTo(fromNode.getLeftSiblingKey());
            if (nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()) {
              final StructNode leftSibling =
                  pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getStructuralNode().getLeftSiblingKey(),
                                                       IndexType.DOCUMENT,
                                                       -1);
              leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
            }
            final long leftSiblingKey =
                nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()
                    ? nodeReadOnlyTrx.getStructuralNode()
                                     .getLeftSiblingKey()
                    : getCurrentNode().getNodeKey();
            moveTo(fromNode.getRightSiblingKey());
            final StructNode rightSibling =
                pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
            rightSibling.setLeftSiblingKey(leftSiblingKey);
            moveTo(fromNode.getLeftSiblingKey());
            remove();
            moveTo(fromNode.getRightSiblingKey());
          } else {
            if (nodeReadOnlyTrx.getStructuralNode().hasRightSibling()) {
              final StructNode rightSibling =
                  pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getStructuralNode().getRightSiblingKey(),
                                                       IndexType.DOCUMENT,
                                                       -1);
              rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
            }
            final long rightSiblingKey =
                nodeReadOnlyTrx.getStructuralNode().hasRightSibling()
                    ? nodeReadOnlyTrx.getStructuralNode()
                                     .getRightSiblingKey()
                    : getCurrentNode().getNodeKey();
            moveTo(fromNode.getLeftSiblingKey());
            final StructNode leftSibling =
                pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
            leftSibling.setRightSiblingKey(rightSiblingKey);
            moveTo(fromNode.getRightSiblingKey());
            remove();
            moveTo(fromNode.getLeftSiblingKey());
          }
          setValue(builder.toString());
        }
      }
    }

    // Modify nodes where the subtree has been moved to.
    // ==============================================================================
    insertPos.processMove(fromNode, toNode, this);
  }

  @Override
  public XmlNodeTrx insertElementAsFirstChild(final QNm name) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = nodeReadOnlyTrx.getCurrentNode().getKind();
      if (kind == NodeKind.ELEMENT || kind == NodeKind.XML_DOCUMENT) {
        checkAccessAndCommit();

        final long parentKey = nodeReadOnlyTrx.getCurrentNode().getNodeKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = ((StructNode) nodeReadOnlyTrx.getCurrentNode()).getFirstChildKey();

        final long pathNodeKey = buildPathSummary ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT) : 0;
        final SirixDeweyID id = deweyIDManager.newFirstChildID();
        final ElementNode node =
            nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASFIRSTCHILD);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertElementAsLeftSibling(final QNm name) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && getCurrentNode().getKind() != NodeKind.XML_DOCUMENT) {
        checkAccessAndCommit();

        final long key = getCurrentNode().getNodeKey();
        moveToParent();
        final long pathNodeKey = buildPathSummary ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT) : 0;
        moveTo(key);

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
        final long rightSibKey = getCurrentNode().getNodeKey();

        final SirixDeweyID id = deweyIDManager.newLeftSiblingID();
        final ElementNode node =
            nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASLEFTSIBLING);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertElementAsRightSibling(final QNm name) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && !isDocumentRoot()) {
        checkAccessAndCommit();

        final long key = getCurrentNode().getNodeKey();
        moveToParent();
        final long pathNodeKey = buildPathSummary ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT) : 0;
        moveTo(key);

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = getCurrentNode().getNodeKey();
        final long rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();

        final SirixDeweyID id = deweyIDManager.newRightSiblingID();
        final ElementNode node =
            nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASRIGHTSIBLING);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertSubtreeAsFirstChild(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, Commit.Implicit);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsRightSibling(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, Commit.Implicit);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsLeftSibling(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, Commit.Implicit);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsFirstChild(final XMLEventReader reader, final Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD, commit);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsRightSibling(final XMLEventReader reader, final Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING, commit);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsLeftSibling(final XMLEventReader reader, final Commit commit) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING, commit);
  }

  private XmlNodeTrx insertSubtree(final XMLEventReader reader, final InsertPosition insertionPosition,
      final Commit commit) {
    checkNotNull(reader);
    assert insertionPosition != null;

    try {
      if (insertionPosition != InsertPosition.AS_FIRST_CHILD && !reader.peek().isStartElement() && reader.hasNext()) {
        reader.next();
      }
    } catch (XMLStreamException e) {
      throw new IllegalArgumentException(e);
    }

    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode) {
        checkAccessAndCommit();
        nodeHashing.setBulkInsert(true);
        long nodeKey = getCurrentNode().getNodeKey();
        final XmlShredder shredder = new XmlShredder.Builder(this, reader, insertionPosition).build();
        shredder.call();
        moveTo(nodeKey);

        switch (insertionPosition) {
          case AS_FIRST_CHILD -> {
            moveToFirstChild();
            nonElementHashes();
          }
          case AS_RIGHT_SIBLING -> moveToRightSibling();
          case AS_LEFT_SIBLING -> moveToLeftSibling();
          default -> {
          }
          // May not happen.
        }

        adaptHashesInPostorderTraversal();

        nodeHashing.setBulkInsert(false);

        if (commit == XmlNodeTrx.Commit.Implicit) {
          commit();
        }
      }
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void nonElementHashes() {
    while (getCurrentNode().getKind() != NodeKind.ELEMENT) {
      BigInteger hashToAdd = getCurrentNode().computeHash();
      Node node =
          pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(hashToAdd);

      moveToRightSibling();
    }
  }

  @Override
  public XmlNodeTrx insertPIAsLeftSibling(final String target, @NonNull final String content) {
    return pi(target, content, InsertPosition.AS_LEFT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertPIAsRightSibling(final String target, @NonNull final String content) {
    return pi(target, content, InsertPosition.AS_RIGHT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertPIAsFirstChild(final String target, @NonNull final String content) {
    return pi(target, content, InsertPosition.AS_FIRST_CHILD);
  }

  /**
   * Processing instruction.
   *
   * @param target  target PI
   * @param content content of PI
   * @param insert  insertion location
   * @throws SirixException if any unexpected error occurs
   */
  private XmlNodeTrx pi(final String target, final String content, final InsertPosition insert) {
    final byte[] targetBytes = getBytes(target);
    if (!XMLToken.isNCName(checkNotNull(targetBytes))) {
      throw new IllegalArgumentException("The target is not valid!");
    }
    if (content.contains("?>-")) {
      throw new SirixUsageException("The content must not contain '?>-'");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode) {
        checkAccessAndCommit();

        // Insert new processing instruction node.
        final byte[] processingContent = getBytes(content);
        long parentKey;
        long leftSibKey;
        long rightSibKey;
        InsertPos pos = InsertPos.ASFIRSTCHILD;
        SirixDeweyID id;
        switch (insert) {
          case AS_FIRST_CHILD -> {
            parentKey = getCurrentNode().getNodeKey();
            leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
            rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
            id = deweyIDManager.newFirstChildID();
          }
          case AS_RIGHT_SIBLING -> {
            parentKey = getCurrentNode().getParentKey();
            leftSibKey = getCurrentNode().getNodeKey();
            rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
            pos = InsertPos.ASRIGHTSIBLING;
            id = deweyIDManager.newRightSiblingID();
          }
          case AS_LEFT_SIBLING -> {
            parentKey = getCurrentNode().getParentKey();
            leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
            rightSibKey = getCurrentNode().getNodeKey();
            pos = InsertPos.ASLEFTSIBLING;
            id = deweyIDManager.newLeftSiblingID();
          }
          default -> throw new IllegalStateException("Insert location not known!");
        }

        final QNm targetName = new QNm(target);
        final long pathNodeKey =
            buildPathSummary ? pathSummaryWriter.getPathNodeKey(targetName, NodeKind.PROCESSING_INSTRUCTION) : 0;
        final PINode node = nodeFactory.createPINode(parentKey,
                                                     leftSibKey,
                                                     rightSibKey,
                                                     targetName,
                                                     processingContent,
                                                     useTextCompression,
                                                     pathNodeKey,
                                                     id);

        // Adapt local nodes and hashes.
        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, pos);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException("Current node must be a structural node!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertCommentAsLeftSibling(final String value) {
    return comment(value, InsertPosition.AS_LEFT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertCommentAsRightSibling(final String value) {
    return comment(value, InsertPosition.AS_RIGHT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertCommentAsFirstChild(final String value) {
    return comment(value, InsertPosition.AS_FIRST_CHILD);
  }

  /**
   * Comment node.
   *
   * @param value  value of comment
   * @param insert insertion location
   * @throws SirixException if any unexpected error occurs
   */
  private XmlNodeTrx comment(final String value, final InsertPosition insert) {
    // Produces a NPE if value is null (what we want).
    if (value.contains("--")) {
      throw new SirixUsageException("Character sequence \"--\" is not allowed in comment content!");
    }
    if (value.endsWith("-")) {
      throw new SirixUsageException("Comment content must not end with \"-\"!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && (getCurrentNode().getKind() != NodeKind.XML_DOCUMENT || (
          getCurrentNode().getKind() == NodeKind.XML_DOCUMENT && insert == InsertPosition.AS_FIRST_CHILD))) {
        checkAccessAndCommit();

        // Insert new comment node.
        final byte[] commentValue = getBytes(value);
        long parentKey;
        long leftSibKey;
        long rightSibKey;
        final InsertPos pos;
        final SirixDeweyID id;

        switch (insert) {
          case AS_FIRST_CHILD -> {
            parentKey = getCurrentNode().getNodeKey();
            leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
            rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();
            pos = InsertPos.ASFIRSTCHILD;
            id = deweyIDManager.newFirstChildID();
          }
          case AS_RIGHT_SIBLING -> {
            parentKey = getCurrentNode().getParentKey();
            leftSibKey = getCurrentNode().getNodeKey();
            rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();
            pos = InsertPos.ASRIGHTSIBLING;
            id = deweyIDManager.newRightSiblingID();
          }
          case AS_LEFT_SIBLING -> {
            parentKey = getCurrentNode().getParentKey();
            leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
            rightSibKey = getCurrentNode().getNodeKey();
            pos = InsertPos.ASLEFTSIBLING;
            id = deweyIDManager.newLeftSiblingID();
          }
          default -> throw new IllegalStateException("Insert location not known!");
        }

        final CommentNode node =
            nodeFactory.createCommentNode(parentKey, leftSibKey, rightSibKey, commentValue, useTextCompression, id);

        // Adapt local nodes and hashes.
        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, pos);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        return this;
      } else {
        throw new SirixUsageException("Current node must be a structural node!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsFirstChild(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && !value.isEmpty()) {
        checkAccessAndCommit();

        final long pathNodeKey = ((NameNode) getCurrentNode()).getPathNodeKey();
        final long parentKey = getCurrentNode().getNodeKey();
        final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        final long rightSibKey = ((StructNode) getCurrentNode()).getFirstChildKey();

        // Update value in case of adjacent text nodes.
        if (hasNode(rightSibKey)) {
          moveTo(rightSibKey);
          if (getCurrentNode().getKind() == NodeKind.TEXT) {
            setValue(value + getValue());
            nodeHashing.adaptHashedWithUpdate(getCurrentNode().getHash());
            return this;
          }
          moveTo(parentKey);
        }

        // Insert new text node if no adjacent text nodes are found.
        final byte[] textValue = getBytes(value);
        final SirixDeweyID id = deweyIDManager.newFirstChildID();
        final TextNode node =
            nodeFactory.createTextNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);

        // Adapt local nodes and hashes.
        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASFIRSTCHILD);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode or TextNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsLeftSibling(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && getCurrentNode().getKind() != NodeKind.XML_DOCUMENT
          && !value.isEmpty()) {
        checkAccessAndCommit();

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = ((StructNode) getCurrentNode()).getLeftSiblingKey();
        final long rightSibKey = getCurrentNode().getNodeKey();

        // Update value in case of adjacent text nodes.
        final StringBuilder builder = new StringBuilder();
        if (getCurrentNode().getKind() == NodeKind.TEXT) {
          builder.append(value);
        }
        builder.append(getValue());

        if (!value.equals(builder.toString())) {
          setValue(builder.toString());
          return this;
        }
        if (hasNode(leftSibKey)) {
          moveTo(leftSibKey);
          final StringBuilder valueBuilder = new StringBuilder();
          if (getCurrentNode().getKind() == NodeKind.TEXT) {
            valueBuilder.append(getValue()).append(builder);
          }
          if (!value.equals(valueBuilder.toString())) {
            setValue(valueBuilder.toString());
            return this;
          }
        }

        // Insert new text node if no adjacent text nodes are found.
        moveTo(rightSibKey);
        final byte[] textValue = getBytes(builder.toString());
        final SirixDeweyID id = deweyIDManager.newLeftSiblingID();
        final TextNode node =
            nodeFactory.createTextNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);

        // Adapt local nodes and hashes.
        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASLEFTSIBLING);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        // Get the path node key.
        moveToParent();
        final long pathNodeKey = isElement() ? getNameNode().getPathNodeKey() : -1;
        nodeReadOnlyTrx.setCurrentNode(node);

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an Element- or Text-node!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsRightSibling(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof StructNode && getCurrentNode().getKind() != NodeKind.XML_DOCUMENT
          && !value.isEmpty()) {
        checkAccessAndCommit();

        final long parentKey = getCurrentNode().getParentKey();
        final long leftSibKey = getCurrentNode().getNodeKey();
        final long rightSibKey = ((StructNode) getCurrentNode()).getRightSiblingKey();

        // Update value in case of adjacent text nodes.
        final StringBuilder builder = new StringBuilder();
        if (getCurrentNode().getKind() == NodeKind.TEXT) {
          builder.append(getValue());
        }
        builder.append(value);
        if (!value.equals(builder.toString())) {
          setValue(builder.toString());
          return this;
        }
        if (hasNode(rightSibKey)) {
          moveTo(rightSibKey);
          if (getCurrentNode().getKind() == NodeKind.TEXT) {
            builder.append(getValue());
          }
          if (!value.equals(builder.toString())) {
            setValue(builder.toString());
            return this;
          }
        }

        // Insert new text node if no adjacent text nodes are found.
        moveTo(leftSibKey);
        final byte[] textValue = getBytes(builder.toString());
        final SirixDeweyID id = deweyIDManager.newRightSiblingID();

        final TextNode node =
            nodeFactory.createTextNode(parentKey, leftSibKey, rightSibKey, textValue, useTextCompression, id);

        // Adapt local nodes and hashes.
        nodeReadOnlyTrx.setCurrentNode(node);
        adaptForInsert(node, InsertPos.ASRIGHTSIBLING);
        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        // Get the path node key.
        moveToParent();
        final long pathNodeKey = isElement() ? getNameNode().getPathNodeKey() : -1;
        nodeReadOnlyTrx.setCurrentNode(node);

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Get a byte-array from a value.
   *
   * @param value the value
   * @return byte-array representation of {@code pValue}
   */
  private static byte[] getBytes(final String value) {
    return value.getBytes(Constants.DEFAULT_ENCODING);
  }

  @Override
  public XmlNodeTrx insertAttribute(final QNm name, @NonNull final String value) {
    return insertAttribute(name, value, Movement.NONE);
  }

  @Override
  public XmlNodeTrx insertAttribute(final QNm name, @NonNull final String value, @NonNull final Movement move) {
    checkNotNull(value);
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode().getKind() == NodeKind.ELEMENT) {
        checkAccessAndCommit();

        /*
         * Update value in case of the same attribute name is found but the attribute to insert has a
         * different value (otherwise an exception is thrown because of a duplicate attribute which would
         * otherwise be inserted!).
         */
        final ElementNode element = (ElementNode) getCurrentNode();
        final Optional<Long> attKey = element.getAttributeKeyByName(name);
        if (attKey.isPresent()) {
          moveTo(attKey.get());
          final QNm qName = getName();
          if (name.equals(qName)) {
            if (getValue().equals(value)) {
              return this;
              // throw new SirixUsageException("Duplicate attribute!");
            } else {
              setValue(value);
            }
          }
          moveToParent();
        }

        // Get the path node key.
        final long pathNodeKey = resourceManager.getResourceConfig().withPathSummary ? pathSummaryWriter.getPathNodeKey(
            name,
            NodeKind.ATTRIBUTE) : 0;
        final byte[] attValue = getBytes(value);

        final SirixDeweyID id = deweyIDManager.newAttributeID();
        final long elementKey = getCurrentNode().getNodeKey();
        final AttributeNode node = nodeFactory.createAttributeNode(elementKey, name, attValue, pathNodeKey, id);

        final Node parentNode = pageTrx.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        ((ElementNode) parentNode).insertAttribute(node.getNodeKey(), node.getPrefixKey() + node.getLocalNameKey());

        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        if (move == Movement.TOPARENT) {
          moveToParent();
        }
        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertNamespace(@NonNull final QNm name) {
    return insertNamespace(name, Movement.NONE);
  }

  @Override
  public XmlNodeTrx insertNamespace(@NonNull final QNm name, @NonNull final Movement move) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode().getKind() == NodeKind.ELEMENT) {
        checkAccessAndCommit();

        for (int i = 0, namespCount = ((ElementNode) getCurrentNode()).getNamespaceCount(); i < namespCount; i++) {
          moveToNamespace(i);
          final QNm qName = getName();
          if (name.getPrefix().equals(qName.getPrefix())) {
            throw new SirixUsageException("Duplicate namespace!");
          }
          moveToParent();
        }

        final long pathNodeKey = buildPathSummary ? pathSummaryWriter.getPathNodeKey(name, NodeKind.NAMESPACE) : 0;
        final long elementKey = getCurrentNode().getNodeKey();

        final SirixDeweyID id = deweyIDManager.newNamespaceID();
        final NamespaceNode node = nodeFactory.createNamespaceNode(elementKey, name, pathNodeKey, id);

        final Node parentNode = pageTrx.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        ((ElementNode) parentNode).insertNamespace(node.getNodeKey());

        nodeReadOnlyTrx.setCurrentNode(node);
        nodeHashing.adaptHashesWithAdd();
        if (move == Movement.TOPARENT) {
          moveToParent();
        }
        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Check ancestors of current node.
   *
   * @throws IllegalStateException if one of the ancestors is the node/subtree rooted at the node to
   *                               move
   */
  private void checkAncestors(final Node node) {
    assert node != null;
    final ImmutableNode item = getCurrentNode();
    while (getCurrentNode().hasParent()) {
      moveToParent();
      if (getCurrentNode().getNodeKey() == node.getNodeKey()) {
        throw new IllegalStateException("Moving one of the ancestor nodes is not permitted!");
      }
    }
    moveTo(item.getNodeKey());
  }

  @Override
  public XmlNodeTrx remove() {
    checkAccessAndCommit();
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode().getKind() == NodeKind.XML_DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      } else if (getCurrentNode() instanceof StructNode) {
        final StructNode node = (StructNode) nodeReadOnlyTrx.getCurrentNode();

//        for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); ) {
//          descendantAxis.nextLong();
//          System.out.println("path: " + getPathSummary().getPath());
//          System.out.println("nodeKey: " + getPathSummary().getNodeKey());
//          System.out.println("references: " + getPathSummary().getReferences());
//        }

        // Remove subtree.
        for (final Axis axis = new PostOrderAxis(this); axis.hasNext(); ) {
          axis.nextLong();

          final var currentNode = axis.getCursor().getNode();

          // Remove name.
          removeName();

          // Remove namespaces and attributes.
          removeNonStructural();

          // Remove text value.
          removeValue();

          // Then remove node.
          pageTrx.removeRecord(currentNode.getNodeKey(), IndexType.DOCUMENT, -1);
        }

//        getPathSummary().moveToDocumentRoot();
//
//        System.out.println("=====================");
//
//        for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); ) {
//          descendantAxis.nextLong();
//          System.out.println("path: " + getPathSummary().getPath());
//          System.out.println("nodeKey: " + getPathSummary().getNodeKey());
//          System.out.println("references: " + getPathSummary().getReferences());
//        }

//        removeNonStructural();
        removeName();
        removeValue();

//        getPathSummary().moveToDocumentRoot();
//
//        System.out.println("=====================");
//
//        for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); ) {
//          descendantAxis.nextLong();
//          System.out.println("path: " + getPathSummary().getPath());
//          System.out.println("nodeKey: " + getPathSummary().getNodeKey());
//          System.out.println("references: " + getPathSummary().getReferences());
//        }

        // Adapt hashes and neighbour nodes as well as the name from the
        // NamePage mapping if it's not a text node.
        final ImmutableXmlNode xmlNode = (ImmutableXmlNode) node;
        nodeReadOnlyTrx.setCurrentNode(xmlNode);
        nodeHashing.adaptHashesWithRemove();
        adaptForRemove(node);
        nodeReadOnlyTrx.setCurrentNode(xmlNode);

        // Set current node (don't remove the moveTo(long) inside the if-clause which is needed
        // because of text merges.
        if (nodeReadOnlyTrx.hasRightSibling() && moveTo(node.getRightSiblingKey())) {
          // Do nothing.
        } else if (node.hasLeftSibling()) {
          moveTo(node.getLeftSiblingKey());
        } else {
          moveTo(node.getParentKey());
        }
      } else if (getCurrentNode().getKind() == NodeKind.ATTRIBUTE) {
        final AttributeNode node = (AttributeNode) nodeReadOnlyTrx.getCurrentNode();

        indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = pageTrx.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        parent.removeAttribute(node.getNodeKey());
        nodeHashing.adaptHashesWithRemove();
        pageTrx.removeRecord(node.getNodeKey(), IndexType.DOCUMENT, -1);
        removeName();
        moveToParent();
      } else if (getCurrentNode().getKind() == NodeKind.NAMESPACE) {
        final NamespaceNode node = (NamespaceNode) nodeReadOnlyTrx.getCurrentNode();

        indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = pageTrx.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        parent.removeNamespace(node.getNodeKey());
        nodeHashing.adaptHashesWithRemove();
        pageTrx.removeRecord(node.getNodeKey(), IndexType.DOCUMENT, -1);
        removeName();
        moveToParent();
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void removeValue() throws SirixIOException {
    if (getCurrentNode() instanceof ValueNode) {
      final long nodeKey = getNodeKey();
      final long pathNodeKey = moveToParent() ? getPathNodeKey() : -1;
      moveTo(nodeKey);
      indexController.notifyChange(ChangeType.DELETE, getCurrentNode(), pathNodeKey);
    }
  }

  /**
   * Remove non-structural nodes of an {@link ElementNode}, that is namespaces and attributes.
   *
   * @throws SirixException if anything goes wrong
   */
  private void removeNonStructural() {
    if (nodeReadOnlyTrx.getKind() == NodeKind.ELEMENT) {
      for (int i = 0, attCount = nodeReadOnlyTrx.getAttributeCount(); i < attCount; i++) {
        moveToAttribute(i);
        removeName();
        removeValue();
        pageTrx.removeRecord(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        moveToParent();
      }
      final int nspCount = nodeReadOnlyTrx.getNamespaceCount();
      for (int i = 0; i < nspCount; i++) {
        moveToNamespace(i);
        removeName();
        pageTrx.removeRecord(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        moveToParent();
      }
    }
  }

  /**
   * Remove a name from the {@link NamePage} reference and the path summary if needed.
   *
   * @throws SirixException if Sirix fails
   */
  private void removeName() {
    if (getCurrentNode() instanceof ImmutableNameNode node) {
      indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
      final NodeKind nodeKind = node.getKind();
      final NamePage page = ((NamePage) pageTrx.getActualRevisionRootPage().getNamePageReference().getPage());
      page.removeName(node.getPrefixKey(), nodeKind, pageTrx);
      page.removeName(node.getLocalNameKey(), nodeKind, pageTrx);
      page.removeName(node.getURIKey(), NodeKind.NAMESPACE, pageTrx);

      assert nodeKind != NodeKind.XML_DOCUMENT;
      if (buildPathSummary) {
        pathSummaryWriter.remove(node, nodeKind, page);
      }
    }
  }

  @Override
  public XmlNodeTrx setName(final QNm name) {
    checkNotNull(name);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof NameNode) {
        if (!getName().equals(name)) {
          checkAccessAndCommit();

          NameNode node = (NameNode) nodeReadOnlyTrx.getCurrentNode();
          final BigInteger oldHash = node.computeHash();

          // Remove old keys from mapping.
          final NodeKind nodeKind = node.getKind();
          final int oldPrefixKey = node.getPrefixKey();
          final int oldLocalNameKey = node.getLocalNameKey();
          final int oldUriKey = node.getURIKey();
          final NamePage page = ((NamePage) pageTrx.getActualRevisionRootPage().getNamePageReference().getPage());
          page.removeName(oldPrefixKey, nodeKind, pageTrx);
          page.removeName(oldLocalNameKey, nodeKind, pageTrx);
          page.removeName(oldUriKey, NodeKind.NAMESPACE, pageTrx);

          // Create new keys for mapping.
          final int prefixKey =
              name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                              node.getKind()) : -1;
          final int localNameKey =
              name.getLocalName() != null && !name.getLocalName().isEmpty()
                  ? pageTrx.createNameKey(name.getLocalName(),
                                          node.getKind())
                  : -1;
          final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
              ? pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
              : -1;

          // Set new keys for current node.
          node = pageTrx.prepareRecordForModification(node.getNodeKey(), IndexType.DOCUMENT, -1);
          node.setLocalNameKey(localNameKey);
          node.setURIKey(uriKey);
          node.setPrefixKey(prefixKey);

          // Adapt path summary.
          if (buildPathSummary) {
            pathSummaryWriter.adaptPathForChangedNode(node, name, uriKey, prefixKey, localNameKey, OPType.SETNAME);
          }

          // Set path node key.
          node.setPathNodeKey(buildPathSummary ? pathSummaryWriter.getNodeKey() : 0);

          nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) node);
          nodeHashing.adaptHashedWithUpdate(oldHash);
        }

        return this;
      } else {
        throw new SirixUsageException("setName is not allowed if current node is not an INameNode implementation!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx setValue(final String value) {
    checkNotNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getCurrentNode() instanceof ValueNode) {
        checkAccessAndCommit();

        // If an empty value is specified the node needs to be removed (see XDM).
        if (value.isEmpty()) {
          remove();
          return this;
        }

        final long nodeKey = getNodeKey();
        moveToParent();
        final long pathNodeKey = getPathNodeKey();
        moveTo(nodeKey);

        // Remove old value from indexes.
        indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

        final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();
        final byte[] byteVal = getBytes(value);

        final ValueNode node =
            pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        node.setValue(byteVal);
        node.setPreviousRevision(node.getLastModifiedRevisionNumber());
        node.setLastModifiedRevision(nodeReadOnlyTrx.getRevisionNumber());

        nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) node);
        nodeHashing.adaptHashedWithUpdate(oldHash);

        // Index new value.
        indexController.notifyChange(ChangeType.INSERT, getNode(), pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "setValue(String) is not allowed if current node is not an IValNode implementation!");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Modifying hashes in a postorder-traversal.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  @Override
  protected void postOrderTraversalHashes() {
    new PostOrderAxis(this, IncludeSelf.YES).forEach((unused) -> {
      final StructNode node = nodeReadOnlyTrx.getStructuralNode();
      if (node.getKind() == NodeKind.ELEMENT) {
        final ElementNode element = (ElementNode) node;
        for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
          moveToNamespace(i);
          nodeHashing.addHashAndDescendantCount();
          moveToParent();
        }
        for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
          moveToAttribute(i);
          nodeHashing.addHashAndDescendantCount();
          moveToParent();
        }
      }
      nodeHashing.addHashAndDescendantCount();
    });
  }

  @Override
  protected void serializeUpdateDiffs(int revisionNumber) {

  }

  // ////////////////////////////////////////////////////////////
  // insert operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for insert operations.
   *
   * @param newNode   pointer of the new node to be inserted
   * @param insertPos determines the position where to insert
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final Node newNode, final InsertPos insertPos) throws SirixIOException {
    assert newNode != null;
    assert insertPos != null;

    if (newNode instanceof StructNode structNode) {
      final StructNode parent = pageTrx.prepareRecordForModification(newNode.getParentKey(), IndexType.DOCUMENT, -1);

      if (storeChildCount) {
        parent.incrementChildCount();
      }

      if (!((StructNode) newNode).hasLeftSibling()) {
        parent.setFirstChildKey(newNode.getNodeKey());
      }

      if (structNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            pageTrx.prepareRecordForModification(structNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
        rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
      }

      if (structNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            pageTrx.prepareRecordForModification(structNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
        leftSiblingNode.setRightSiblingKey(structNode.getNodeKey());
      }
    }
  }

  // ////////////////////////////////////////////////////////////
  // end of insert operation
  // ////////////////////////////////////////////////////////////

  // ////////////////////////////////////////////////////////////
  // remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Adapting everything for remove operations.
   *
   * @param oldNode pointer of the old node to be replaced
   * @throws SirixException if anything weird happens
   */
  private void adaptForRemove(final StructNode oldNode) {
    assert oldNode != null;

    // Concatenate neighbor text nodes if they exist (the right sibling is
    // deleted afterwards).
    boolean concatenated = false;
    if (oldNode.hasLeftSibling() && oldNode.hasRightSibling() && moveTo(oldNode.getRightSiblingKey())
        && getCurrentNode().getKind() == NodeKind.TEXT && moveTo(oldNode.getLeftSiblingKey())
        && getCurrentNode().getKind() == NodeKind.TEXT) {
      final StringBuilder builder = new StringBuilder(getValue());
      moveTo(oldNode.getRightSiblingKey());
      builder.append(getValue());
      moveTo(oldNode.getLeftSiblingKey());
      setValue(builder.toString());
      concatenated = true;
    }

    // Adapt left sibling node if there is one.
    if (oldNode.hasLeftSibling()) {
      final StructNode leftSibling =
          pageTrx.prepareRecordForModification(oldNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      if (concatenated) {
        moveTo(oldNode.getRightSiblingKey());
        leftSibling.setRightSiblingKey(((StructNode) getCurrentNode()).getRightSiblingKey());
      } else {
        leftSibling.setRightSiblingKey(oldNode.getRightSiblingKey());
      }
    }

    // Adapt right sibling node if there is one.
    if (oldNode.hasRightSibling()) {
      StructNode rightSibling;
      if (concatenated) {
        moveTo(oldNode.getRightSiblingKey());
        moveTo(nodeReadOnlyTrx.getStructuralNode().getRightSiblingKey());
        rightSibling =
            pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      } else {
        rightSibling = pageTrx.prepareRecordForModification(oldNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      }
    }

    // Adapt parent, if node has now left sibling it is a first child.
    StructNode parent = pageTrx.prepareRecordForModification(oldNode.getParentKey(), IndexType.DOCUMENT, -1);
    if (!oldNode.hasLeftSibling()) {
      parent.setFirstChildKey(oldNode.getRightSiblingKey());
    }
    if (storeChildCount) {
      parent.decrementChildCount();
    }
    if (concatenated) {
      parent.decrementDescendantCount();
      if (storeChildCount) {
        parent.decrementChildCount();
      }
    }
    if (concatenated) {
      // Adjust descendant count.
      moveTo(parent.getNodeKey());
      while (parent.hasParent()) {
        moveToParent();
        final StructNode ancestor =
            pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        ancestor.decrementDescendantCount();
        parent = ancestor;
      }
    }

    // Remove right sibling text node if text nodes have been
    // concatenated/merged.
    if (concatenated) {
      moveTo(oldNode.getRightSiblingKey());
      pageTrx.removeRecord(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    }

    // Remove non-structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    pageTrx.removeRecord(oldNode.getNodeKey(), IndexType.DOCUMENT, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  @Override
  public XmlNodeTrx copySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_FIRST_CHILD);
      moveTo(nodeKey);
      moveToFirstChild();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx copySubtreeAsLeftSibling(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_LEFT_SIBLING);
      moveTo(nodeKey);
      moveToFirstChild();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx copySubtreeAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_RIGHT_SIBLING);
      moveTo(nodeKey);
      moveToRightSibling();
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Helper method for copy-operations.
   *
   * @param trx    the source {@link XmlNodeReadOnlyTrx}
   * @param insert the insertion strategy
   * @throws SirixException if anything fails in sirix
   */
  private void copy(final XmlNodeReadOnlyTrx trx, final InsertPosition insert) {
    assert trx != null;
    assert insert != null;
    final XmlNodeReadOnlyTrx rtx = trx.getResourceSession().beginNodeReadOnlyTrx(trx.getRevisionNumber());
    assert rtx.getRevisionNumber() == trx.getRevisionNumber();
    rtx.moveTo(trx.getNodeKey());
    assert rtx.getNodeKey() == trx.getNodeKey();
    if (rtx.getKind() == NodeKind.XML_DOCUMENT) {
      rtx.moveToFirstChild();
    }
    if (!(rtx.isStructuralNode())) {
      throw new IllegalStateException(
          "Node to insert must be a structural node (Text, PI, Comment, Document root or Element)!");
    }

    final NodeKind kind = rtx.getKind();
    switch (kind) {
      case TEXT:
        final String textValue = rtx.getValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertTextAsFirstChild(textValue);
          case AS_LEFT_SIBLING -> insertTextAsLeftSibling(textValue);
          case AS_RIGHT_SIBLING -> insertTextAsRightSibling(textValue);
          default -> throw new IllegalStateException();
        }
        break;
      case PROCESSING_INSTRUCTION:
        switch (insert) {
          case AS_FIRST_CHILD -> insertPIAsFirstChild(rtx.getName().getLocalName(), rtx.getValue());
          case AS_LEFT_SIBLING -> insertPIAsLeftSibling(rtx.getName().getLocalName(), rtx.getValue());
          case AS_RIGHT_SIBLING -> insertPIAsRightSibling(rtx.getName().getLocalName(), rtx.getValue());
          default -> throw new IllegalStateException();
        }
        break;
      case COMMENT:
        final String commentValue = rtx.getValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertCommentAsFirstChild(commentValue);
          case AS_LEFT_SIBLING -> insertCommentAsLeftSibling(commentValue);
          case AS_RIGHT_SIBLING -> insertCommentAsRightSibling(commentValue);
          default -> throw new IllegalStateException();
        }
        break;
      // $CASES-OMITTED$
      default:
        new XmlShredder.Builder(this, new StAXSerializer(rtx), insert).build().call();
    }
    rtx.close();
  }

  @Override
  public XmlNodeTrx replaceNode(final XMLEventReader reader) {
    checkNotNull(reader);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();

      if (getCurrentNode() instanceof StructNode) {
        final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

        final InsertPosition pos;
        final long anchorNodeKey;
        if (currentNode.hasLeftSibling()) {
          anchorNodeKey = getLeftSiblingKey();
          pos = InsertPosition.AS_RIGHT_SIBLING;
        } else {
          anchorNodeKey = getParentKey();
          pos = InsertPosition.AS_FIRST_CHILD;
        }

        insertAndThenRemove(reader, pos, anchorNodeKey);
        return this;
      } else {
        throw new IllegalArgumentException("Not supported for attributes / namespaces.");
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void insertAndThenRemove(final XMLEventReader reader, InsertPosition pos, long anchorNodeKey) {
    long formerNodeKey = getNodeKey();
    insert(reader, pos, anchorNodeKey);
    moveTo(formerNodeKey);
    remove();
  }

  private void insert(final XMLEventReader reader, InsertPosition pos, long anchorNodeKey) {
    moveTo(anchorNodeKey);
    final XmlShredder shredder = new XmlShredder.Builder(this, reader, pos).build();
    shredder.call();
  }

  @Override
  protected XmlNodeTrx self() {
    return this;
  }

  @Override
  public XmlNodeTrx setBulkInsertion(boolean bulkInsertion) {
    nodeHashing.setBulkInsert(bulkInsertion);
    return this;
  }

  @Override
  public XmlNodeTrx replaceNode(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      /*
       * #text1 <emptyElement/> #text2 | <emptyElement/> #text <emptyElement/>
       *
       * Replace 2nd node each time => with text node
       */
      // $CASES-OMITTED$
      switch (rtx.getKind()) {
        case ELEMENT, TEXT, COMMENT, PROCESSING_INSTRUCTION -> {
          checkCurrentNode();
          if (isText()) {
            removeAndThenInsert(rtx);
          } else {
            insertAndThenRemove(rtx);
          }
        }
        case ATTRIBUTE -> {
          if (getCurrentNode().getKind() != NodeKind.ATTRIBUTE) {
            throw new IllegalStateException("Current node must be an attribute node!");
          }
          remove();
          insertAttribute(rtx.getName(), rtx.getValue());
        }
        case NAMESPACE -> {
          if (getCurrentNode().getKind() != NodeKind.NAMESPACE) {
            throw new IllegalStateException("Current node must be a namespace node!");
          }
          remove();
          insertNamespace(rtx.getName());
        }
        default -> throw new UnsupportedOperationException("Node type not supported!");
      }
      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  /**
   * Check current node type (must be a structural node).
   */
  private void checkCurrentNode() {
    if (!(getCurrentNode() instanceof StructNode)) {
      throw new IllegalStateException("Current node must be a structural node!");
    }
  }

  private ImmutableNode removeAndThenInsert(final XmlNodeReadOnlyTrx rtx) {
    assert rtx != null;
    final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
    long key;
    if (currentNode.hasLeftSibling()) {
      final long nodeKey = currentNode.getLeftSiblingKey();
      remove();
      moveTo(nodeKey);
      key = copySubtreeAsRightSibling(rtx).getNodeKey();
    } else {
      final long nodeKey = currentNode.getParentKey();
      remove();
      moveTo(nodeKey);
      key = copySubtreeAsFirstChild(rtx).getNodeKey();
    }
    moveTo(key);

    return nodeReadOnlyTrx.getCurrentNode();
  }

  private void insertAndThenRemove(final XmlNodeReadOnlyTrx rtx) {
    assert rtx != null;
    final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
    long key;
    if (currentNode.hasLeftSibling()) {
      moveToLeftSibling();
      key = copySubtreeAsRightSibling(rtx).getNodeKey();
    } else {
      moveToParent();
      key = copySubtreeAsFirstChild(rtx).getNodeKey();
      moveTo(key);
    }

    removeOldNode(currentNode, key);
  }

  private void removeOldNode(final StructNode node, final @NonNegative long key) {
    assert node != null;
    assert key >= 0;
    moveTo(node.getNodeKey());
    remove();
    moveTo(key);
  }

  @Override
  protected AbstractNodeHashing<ImmutableXmlNode, XmlNodeReadOnlyTrx> reInstantiateNodeHashing(HashType hashType, PageTrx pageTrx) {
    return new XmlNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);
  }

  @Override
  protected XmlNodeFactory reInstantiateNodeFactory(PageTrx pageTrx) {
    return new XmlNodeFactoryImpl(resourceManager.getResourceConfig().nodeHashFunction, pageTrx);
  }
}
