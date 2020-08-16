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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.User;
import org.sirix.access.trx.node.AfterCommitState;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.HashType;
import org.sirix.access.trx.node.InternalResourceManager;
import org.sirix.access.trx.node.InternalResourceManager.Abort;
import org.sirix.access.trx.node.xml.XmlIndexController.ChangeType;
import org.sirix.api.*;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.exception.SirixUsageException;
import org.sirix.index.path.summary.PathSummaryReader;
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
import org.sirix.page.PageKind;
import org.sirix.page.UberPage;
import org.sirix.service.xml.serialize.StAXSerializer;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.XMLToken;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
final class XmlNodeTrxImpl extends AbstractForwardingXmlNodeReadOnlyTrx implements XmlNodeTrx, InternalXmlNodeTrx {

  /**
   * Maximum number of node modifications before auto commit.
   */
  private final int maxNodeCount;

  /**
   * The deweyID manager.
   */
  private final XmlDeweyIDManager deweyIDManager;

  private final AfterCommitState afterCommitState;

  /**
   * Modification counter.
   */
  long modificationCount;

  /**
   * Hash kind of Structure.
   */
  private final HashType hashType;

  /**
   * Scheduled executor service.
   */
  private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);

  /**
   * {@link InternalXmlNodeReadOnlyTrx} reference.
   */
  final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * {@link PathSummaryWriter} instance.
   */
  private PathSummaryWriter<XmlNodeReadOnlyTrx> pathSummaryWriter;

  /**
   * Determines if a path summary should be built and kept up-to-date or not.
   */
  private final boolean buildPathSummary;

  /**
   * {@link XmlNodeFactory} to be able to create nodes.
   */
  private XmlNodeFactory nodeFactory;

  /**
   * An optional lock for all methods, if an automatic commit is issued.
   */
  private final Lock lock;

  /**
   * Determines if dewey IDs should be stored or not.
   */
  private final boolean storeDeweyIDs;

  /**
   * Determines if text values should be compressed or not.
   */
  private final boolean useTextCompression;

  /**
   * The {@link XmlIndexController} used within the resource manager this {@link XmlNodeTrx} is bound to.
   */
  private XmlIndexController indexController;

  /**
   * The resource manager.
   */
  private final InternalResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager;

  /**
   * The page write trx.
   */
  private PageTrx pageTrx;

  /**
   * Collection holding pre-commit hooks.
   */
  private final List<PreCommitHook> mPreCommitHooks = new ArrayList<>();

  /**
   * Collection holding post-commit hooks.
   */
  private final List<PostCommitHook> mPostCommitHooks = new ArrayList<>();

  /**
   * Hashes node contents.
   */
  private XmlNodeHashing nodeHashing;

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
   * @param timeUnit          unit of the number of the next param {@code pMaxTime}
   * @param maxTime           maximum number of seconds before auto commit
   * @param nodeHashing       hashes node contents
   * @param nodeFactory       the node factory used to create nodes
   * @throws SirixIOException    if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  XmlNodeTrxImpl(final InternalResourceManager<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceManager,
      final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx, final PathSummaryWriter<XmlNodeReadOnlyTrx> pathSummaryWriter,
      final @Nonnegative int maxNodeCount, final TimeUnit timeUnit, final @Nonnegative int maxTime,
      final @Nonnull XmlNodeHashing nodeHashing, final XmlNodeFactory nodeFactory,
      final @Nonnull AfterCommitState afterCommitState) {
    // Do not accept negative values.
    Preconditions.checkArgument(maxNodeCount >= 0 && maxTime >= 0,
                                "Negative arguments for maxNodeCount and maxTime are not accepted.");

    this.nodeHashing = Preconditions.checkNotNull(nodeHashing);
    this.resourceManager = Preconditions.checkNotNull(resourceManager);
    this.nodeReadOnlyTrx = Preconditions.checkNotNull(nodeReadOnlyTrx);
    this.buildPathSummary = resourceManager.getResourceConfig().withPathSummary;
    this.pathSummaryWriter = Preconditions.checkNotNull(pathSummaryWriter);

    indexController = resourceManager.getWtxIndexController(this.nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    pageTrx = (PageTrx) this.nodeReadOnlyTrx.getPageTrx();
    storeChildCount = this.resourceManager.getResourceConfig().getStoreChildCount();

    this.nodeFactory = Preconditions.checkNotNull(nodeFactory);

    // Only auto commit by node modifications if it is more then 0.
    this.maxNodeCount = maxNodeCount;
    this.modificationCount = 0L;

    if (maxTime > 0) {
      threadPool.scheduleAtFixedRate(this::commit, maxTime, maxTime, timeUnit);
    }

    // Synchronize commit and other public methods if needed.
    lock = maxTime > 0 ? new ReentrantLock() : null;

    hashType = resourceManager.getResourceConfig().hashType;
    storeDeweyIDs = resourceManager.getResourceConfig().areDeweyIDsStored;
    useTextCompression = resourceManager.getResourceConfig().useTextCompression;

    deweyIDManager = new XmlDeweyIDManager(this);
    this.afterCommitState = afterCommitState;

    // // Redo last transaction if the system crashed.
    // if (!pPageWriteTrx.isCreated()) {
    // try {
    // commit();
    // } catch (final SirixException e) {
    // throw new IllegalStateException(e);
    // }
    // }
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return getNode().getDeweyID();
  }

  @Override
  public boolean storeDeweyIDs() {
    return storeDeweyIDs;
  }

  @Override
  public Optional<User> getUser() {
    return resourceManager.getUser();
  }

  @Override
  public Optional<User> getUserOfRevisionToRepresent() {
    return nodeReadOnlyTrx.getUser();
  }

  @Override
  public XmlNodeTrx moveSubtreeToFirstChild(final @Nonnegative long fromKey) {
    acquireLock();
    try {
      Preconditions.checkArgument(fromKey >= 0 && fromKey <= getMaxNodeKey(), "Argument must be a valid node key!");

      Preconditions.checkArgument(fromKey != getCurrentNode().getNodeKey(),
                                  "Can't move itself to right sibling of itself!");

      final var node = pageTrx.getRecord(fromKey, PageKind.RECORDPAGE, -1);
      if (node.isEmpty()) {
        throw new IllegalStateException("Node to move must exist!");
      }

      final var nodeToMove = node.get();
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
          if (storeDeweyIDs) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException(
            "Move is not allowed if moved node is not an ElementNode and the node isn't inserted at an element node!");
      }
    } finally {
      unLock();
    }
  }

  /**
   * Get the current node.
   *
   * @return {@link Node} implementation
   */
  private ImmutableXmlNode getCurrentNode() {
    return nodeReadOnlyTrx.getCurrentNode();
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
      axis.next();
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
        pathNodeKey = moveToParent().trx().getNameNode().getPathNodeKey();
        moveTo(nodeKey);
      } else if (getNode() instanceof NameNode) {
        pathNodeKey = getNameNode().getPathNodeKey();
      }
      indexController.notifyChange(type, getNode(), pathNodeKey);
    }
    moveTo(beforeNodeKey);
  }

  @Override
  public XmlNodeTrx moveSubtreeToLeftSibling(final @Nonnegative long fromKey) {
    acquireLock();
    try {
      if (nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()) {
        moveToLeftSibling();
        return moveSubtreeToRightSibling(fromKey);
      } else {
        moveToParent();
        return moveSubtreeToFirstChild(fromKey);
      }
    } finally {
      unLock();
    }
  }

  @Override
  public XmlNodeTrx moveSubtreeToRightSibling(final @Nonnegative long fromKey) {
    acquireLock();
    try {
      if (fromKey < 0 || fromKey > getMaxNodeKey()) {
        throw new IllegalArgumentException("Argument must be a valid node key!");
      }
      if (fromKey == getCurrentNode().getNodeKey()) {
        throw new IllegalArgumentException("Can't move itself to first child of itself!");
      }

      // Save: Every node in the "usual" node page is of type Node.
      final var node = pageTrx.getRecord(fromKey, PageKind.RECORDPAGE, -1);
      if (node.isEmpty()) {
        throw new IllegalStateException("Node to move must exist!");
      }

      final DataRecord nodeToMove = node.get();
      if (nodeToMove instanceof StructNode toMove && getCurrentNode() instanceof StructNode nodeAnchor) {
        checkAncestors(toMove);
        checkAccessAndCommit();

        if (nodeAnchor.getRightSiblingKey() != nodeToMove.getNodeKey()) {
          final long parentKey = toMove.getParentKey();

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
          if (storeDeweyIDs) {
            deweyIDManager.computeNewDeweyIDs();
          }
        }
        return this;
      } else {
        throw new SirixUsageException(
            "Move is not allowed if moved node is not an ElementNode or TextNode and the node isn't inserted at an ElementNode or TextNode!");
      }
    } finally {
      unLock();
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
    final StructNode parent = pageTrx.prepareRecordForModification(fromNode.getParentKey(), PageKind.RECORDPAGE, -1);
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
          pageTrx.prepareRecordForModification(fromNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
      rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
    }

    // Adapt right sibling key of former left sibling.
    if (fromNode.hasLeftSibling()) {
      final StructNode leftSibling =
          pageTrx.prepareRecordForModification(fromNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
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
              final StructNode leftSibling = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getStructuralNode()
                                                                                                 .getLeftSiblingKey(),
                                                                                  PageKind.RECORDPAGE,
                                                                                  -1);
              leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
            }
            final long leftSiblingKey =
                nodeReadOnlyTrx.getStructuralNode().hasLeftSibling()
                    ? nodeReadOnlyTrx.getStructuralNode()
                                     .getLeftSiblingKey()
                    : getCurrentNode().getNodeKey();
            moveTo(fromNode.getRightSiblingKey());
            final StructNode rightSibling = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(),
                                                                                 PageKind.RECORDPAGE,
                                                                                 -1);
            rightSibling.setLeftSiblingKey(leftSiblingKey);
            moveTo(fromNode.getLeftSiblingKey());
            remove();
            moveTo(fromNode.getRightSiblingKey());
          } else {
            if (nodeReadOnlyTrx.getStructuralNode().hasRightSibling()) {
              final StructNode rightSibling = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getStructuralNode()
                                                                                                  .getRightSiblingKey(),
                                                                                   PageKind.RECORDPAGE,
                                                                                   -1);
              rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
            }
            final long rightSiblingKey =
                nodeReadOnlyTrx.getStructuralNode().hasRightSibling()
                    ? nodeReadOnlyTrx.getStructuralNode()
                                     .getRightSiblingKey()
                    : getCurrentNode().getNodeKey();
            moveTo(fromNode.getLeftSiblingKey());
            final StructNode leftSibling = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(),
                                                                                PageKind.RECORDPAGE,
                                                                                -1);
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
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertElementAsLeftSibling(final QNm name) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertElementAsRightSibling(final QNm name) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertSubtreeAsFirstChild(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_FIRST_CHILD);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsRightSibling(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_RIGHT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertSubtreeAsLeftSibling(final XMLEventReader reader) {
    return insertSubtree(reader, InsertPosition.AS_LEFT_SIBLING);
  }

  private XmlNodeTrx insertSubtree(final XMLEventReader reader, final InsertPosition insertionPosition) {
    checkNotNull(reader);
    assert insertionPosition != null;

    try {
      if (insertionPosition != InsertPosition.AS_FIRST_CHILD && !reader.peek().isStartElement() && reader.hasNext()) {
        reader.next();
      }
    } catch (XMLStreamException e) {
      throw new IllegalArgumentException(e);
    }
    acquireLock();
    try {
      if (getCurrentNode() instanceof StructNode) {
        checkAccessAndCommit();
        nodeHashing.setBulkInsert(true);
        long nodeKey = getCurrentNode().getNodeKey();
        final XmlShredder shredder = new XmlShredder.Builder(this, reader, insertionPosition).build();
        shredder.call();
        moveTo(nodeKey);

        switch (insertionPosition) {
          case AS_FIRST_CHILD:
            moveToFirstChild();
            nonElementHashes();
            break;
          case AS_RIGHT_SIBLING:
            moveToRightSibling();
            break;
          case AS_LEFT_SIBLING:
            moveToLeftSibling();
            break;
          default:
            // May not happen.
        }

        adaptHashesInPostorderTraversal();

        commit();
        nodeHashing.setBulkInsert(false);
      }
    } finally {
      unLock();
    }
    return this;
  }

  private void nonElementHashes() {
    while (getCurrentNode().getKind() != NodeKind.ELEMENT) {
      BigInteger hashToAdd = getCurrentNode().computeHash();
      Node node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                       PageKind.RECORDPAGE,
                                                       -1);
      node.setHash(hashToAdd);

      moveToRightSibling();
    }
  }

  @Override
  public XmlNodeTrx insertPIAsLeftSibling(final String target, @Nonnull final String content) {
    return pi(target, content, InsertPosition.AS_LEFT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertPIAsRightSibling(final String target, @Nonnull final String content) {
    return pi(target, content, InsertPosition.AS_RIGHT_SIBLING);
  }

  @Override
  public XmlNodeTrx insertPIAsFirstChild(final String target, @Nonnull final String content) {
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
    acquireLock();
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
      unLock();
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
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertTextAsFirstChild(final String value) {
    checkNotNull(value);
    acquireLock();
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertTextAsLeftSibling(final String value) {
    checkNotNull(value);
    acquireLock();
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
        final long pathNodeKey = moveToParent().trx().isElement() ? getNameNode().getPathNodeKey() : -1;
        nodeReadOnlyTrx.setCurrentNode(node);

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException("Insert is not allowed if current node is not an Element- or Text-node!");
      }
    } finally {
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertTextAsRightSibling(final String value) {
    checkNotNull(value);
    acquireLock();
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
        final long pathNodeKey = moveToParent().trx().isElement() ? getNameNode().getPathNodeKey() : -1;
        nodeReadOnlyTrx.setCurrentNode(node);

        // Index text value.
        indexController.notifyChange(ChangeType.INSERT, node, pathNodeKey);

        return this;
      } else {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
      }
    } finally {
      unLock();
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
  public XmlNodeTrx insertAttribute(final QNm name, @Nonnull final String value) {
    return insertAttribute(name, value, Movement.NONE);
  }

  @Override
  public XmlNodeTrx insertAttribute(final QNm name, @Nonnull final String value, @Nonnull final Movement move) {
    checkNotNull(value);
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    acquireLock();
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

        final Node parentNode = pageTrx.prepareRecordForModification(node.getParentKey(), PageKind.RECORDPAGE, -1);
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx insertNamespace(@Nonnull final QNm name) {
    return insertNamespace(name, Movement.NONE);
  }

  @Override
  public XmlNodeTrx insertNamespace(@Nonnull final QNm name, @Nonnull final Movement move) {
    if (!XMLToken.isValidQName(checkNotNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    acquireLock();
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

        final Node parentNode = pageTrx.prepareRecordForModification(node.getParentKey(), PageKind.RECORDPAGE, -1);
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
      unLock();
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
    acquireLock();
    try {
      if (getCurrentNode().getKind() == NodeKind.XML_DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      } else if (getCurrentNode() instanceof StructNode) {
        final StructNode node = (StructNode) nodeReadOnlyTrx.getCurrentNode();

        // Remove subtree.
        for (final Axis axis = new PostOrderAxis(this); axis.hasNext(); ) {
          axis.next();

          final var currentNode = axis.getCursor().getNode();

          // Remove name.
          removeName();

          // Remove namespaces and attributes.
          removeNonStructural();

          // Remove text value.
          removeValue();

          // Then remove node.
          pageTrx.removeRecord(currentNode.getNodeKey(), PageKind.RECORDPAGE, -1);
        }

        // Remove the name and value of subtree-root if necessary.
        removeName();
        removeValue();

        // Adapt hashes and neighbour nodes as well as the name from the
        // NamePage mapping if it's not a text node.
        final ImmutableXmlNode xmlNode = (ImmutableXmlNode) node;
        nodeReadOnlyTrx.setCurrentNode(xmlNode);
        nodeHashing.adaptHashesWithRemove();
        adaptForRemove(node);
        nodeReadOnlyTrx.setCurrentNode(xmlNode);

        // Set current node (don't remove the moveTo(long) inside the if-clause which is needed
        // because of text merges.
        if (nodeReadOnlyTrx.hasRightSibling() && moveTo(node.getRightSiblingKey()).hasMoved()) {
          // Do nothing.
        } else if (node.hasLeftSibling()) {
          moveTo(node.getLeftSiblingKey());
        } else {
          moveTo(node.getParentKey());
        }
      } else if (getCurrentNode().getKind() == NodeKind.ATTRIBUTE) {
        final AttributeNode node = (AttributeNode) nodeReadOnlyTrx.getCurrentNode();

        indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = pageTrx.prepareRecordForModification(node.getParentKey(), PageKind.RECORDPAGE, -1);
        parent.removeAttribute(node.getNodeKey());
        nodeHashing.adaptHashesWithRemove();
        pageTrx.removeRecord(node.getNodeKey(), PageKind.RECORDPAGE, -1);
        removeName();
        moveToParent();
      } else if (getCurrentNode().getKind() == NodeKind.NAMESPACE) {
        final NamespaceNode node = (NamespaceNode) nodeReadOnlyTrx.getCurrentNode();

        indexController.notifyChange(ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = pageTrx.prepareRecordForModification(node.getParentKey(), PageKind.RECORDPAGE, -1);
        parent.removeNamespace(node.getNodeKey());
        nodeHashing.adaptHashesWithRemove();
        pageTrx.removeRecord(node.getNodeKey(), PageKind.RECORDPAGE, -1);
        removeName();
        moveToParent();
      }

      return this;
    } finally {
      unLock();
    }
  }

  private void removeValue() throws SirixIOException {
    if (getCurrentNode() instanceof ValueNode) {
      final long nodeKey = getNodeKey();
      final long pathNodeKey = moveToParent().hasMoved() ? getPathNodeKey() : -1;
      moveTo(nodeKey);
      indexController.notifyChange(ChangeType.DELETE, getCurrentNode(), pathNodeKey);
    }
  }

  /**
   * Remove non structural nodes of an {@link ElementNode}, that is namespaces and attributes.
   *
   * @throws SirixException if anything goes wrong
   */
  private void removeNonStructural() {
    if (nodeReadOnlyTrx.getKind() == NodeKind.ELEMENT) {
      for (int i = 0, attCount = nodeReadOnlyTrx.getAttributeCount(); i < attCount; i++) {
        moveToAttribute(i);
        removeName();
        removeValue();
        pageTrx.removeRecord(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        moveToParent();
      }
      final int nspCount = nodeReadOnlyTrx.getNamespaceCount();
      for (int i = 0; i < nspCount; i++) {
        moveToNamespace(i);
        removeName();
        pageTrx.removeRecord(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
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
    acquireLock();
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
          node = pageTrx.prepareRecordForModification(node.getNodeKey(), PageKind.RECORDPAGE, -1);
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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx setValue(final String value) {
    checkNotNull(value);
    acquireLock();
    try {
      if (getCurrentNode() instanceof ValueNode) {
        checkAccessAndCommit();

        // If an empty value is specified the node needs to be removed (see XDM).
        if (value.isEmpty()) {
          remove();
          return this;
        }

        final long nodeKey = getNodeKey();
        final long pathNodeKey = moveToParent().trx().getPathNodeKey();
        moveTo(nodeKey);

        // Remove old value from indexes.
        indexController.notifyChange(ChangeType.DELETE, getNode(), pathNodeKey);

        final BigInteger oldHash = nodeReadOnlyTrx.getCurrentNode().computeHash();
        final byte[] byteVal = getBytes(value);

        final ValueNode node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                    PageKind.RECORDPAGE,
                                                                    -1);
        node.setValue(byteVal);

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
      unLock();
    }
  }

  @Override
  public XmlNodeTrx revertTo(final @Nonnegative int revision) {
    acquireLock();
    try {
      nodeReadOnlyTrx.assertNotClosed();
      resourceManager.assertAccess(revision);

      // Close current page transaction.
      final long trxID = getId();
      final int revNumber = getRevisionNumber();

      // Reset internal transaction state to new uber page.
      resourceManager.closeNodePageWriteTransaction(getId());
      pageTrx = resourceManager.createPageTransaction(trxID, revision, revNumber - 1, Abort.NO, true);
      nodeReadOnlyTrx.setPageReadTransaction(null);
      nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
      resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

      nodeHashing = new XmlNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);

      // Reset node factory.
      nodeFactory = null;
      nodeFactory = new XmlNodeFactoryImpl(resourceManager.getResourceConfig().nodeHashFunction, pageTrx);

      // New index instances.
      reInstantiateIndexes();

      // Reset modification counter.
      modificationCount = 0L;

      // Move to document root.
      moveToDocumentRoot();

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public void close() {
    acquireLock();
    try {
      if (!isClosed()) {
        // Make sure to commit all dirty data.
        if (modificationCount > 0) {
          throw new SirixUsageException("Must commit/rollback transaction first!");
        }

        // Release all state immediately.
        final long trxId = getId();
        nodeReadOnlyTrx.close();
        resourceManager.closeWriteTransaction(trxId);
        removeCommitFile();

        pathSummaryWriter = null;
        nodeFactory = null;

        // Shutdown pool.
        threadPool.shutdown();
        try {
          threadPool.awaitTermination(2, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          throw new SirixThreadedException(e);
        }
      }
    } finally {
      unLock();
    }
  }

  @Override
  public XmlNodeTrx rollback() {
    acquireLock();
    try {
      nodeReadOnlyTrx.assertNotClosed();

      // Reset modification counter.
      modificationCount = 0L;

      // Close current page transaction.
      final long trxID = getId();
      final int revision = getRevisionNumber();
      final int revNumber = pageTrx.getUberPage().isBootstrap() ? 0 : revision - 1;

      final UberPage uberPage = pageTrx.rollback();

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      resourceManager.closeNodePageWriteTransaction(getId());
      nodeReadOnlyTrx.setPageReadTransaction(null);
      removeCommitFile();

      pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.YES, true);
      nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
      resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

      nodeFactory = null;
      nodeFactory = new XmlNodeFactoryImpl(resourceManager.getResourceConfig().nodeHashFunction, pageTrx);

      reInstantiateIndexes();

      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public void adaptHashesInPostorderTraversal() {
    if (hashType != HashType.NONE) {
      final long nodeKey = getCurrentNode().getNodeKey();
      postOrderTraversalHashes();
      final ImmutableNode startNode = getCurrentNode();
      moveToParent();
      while (getCurrentNode().hasParent()) {
        moveToParent();
        nodeHashing.addParentHash(startNode);
      }
      moveTo(nodeKey);
    }
  }

  private void removeCommitFile() {
    try {
      final Path commitFile = resourceManager.getCommitFile();
      if (java.nio.file.Files.exists(commitFile))
        java.nio.file.Files.delete(resourceManager.getCommitFile());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public XmlNodeTrx commit() {
    return commit(null);
  }

  private void reInstantiateIndexes() {
    // Get a new path summary instance.
    if (buildPathSummary) {
      pathSummaryWriter = null;
      pathSummaryWriter =
          new PathSummaryWriter<>(pageTrx, nodeReadOnlyTrx.getResourceManager(), nodeFactory, nodeReadOnlyTrx);
    }

    // Recreate index listeners.
    indexController = resourceManager.getWtxIndexController(nodeReadOnlyTrx.getPageTrx().getRevisionNumber());
    indexController.createIndexListeners(indexController.getIndexes().getIndexDefs(), this);
  }

  /**
   * Modifying hashes in a postorder-traversal.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void postOrderTraversalHashes() {
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

  /**
   * Checking write access and intermediate commit.
   *
   * @throws SirixException if anything weird happens
   */
  private void checkAccessAndCommit() {
    nodeReadOnlyTrx.assertNotClosed();
    modificationCount++;
    intermediateCommitIfRequired();
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

    if (newNode instanceof StructNode) {
      final StructNode structNode = (StructNode) newNode;
      final StructNode parent = pageTrx.prepareRecordForModification(newNode.getParentKey(), PageKind.RECORDPAGE, -1);

      if (storeChildCount) {
        parent.incrementChildCount();
      }

      if (!((StructNode) newNode).hasLeftSibling()) {
        parent.setFirstChildKey(newNode.getNodeKey());
      }

      if (structNode.hasRightSibling()) {
        final StructNode rightSiblingNode =
            pageTrx.prepareRecordForModification(structNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
        rightSiblingNode.setLeftSiblingKey(structNode.getNodeKey());
      }

      if (structNode.hasLeftSibling()) {
        final StructNode leftSiblingNode =
            pageTrx.prepareRecordForModification(structNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
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
    if (oldNode.hasLeftSibling() && oldNode.hasRightSibling() && moveTo(oldNode.getRightSiblingKey()).hasMoved()
        && getCurrentNode().getKind() == NodeKind.TEXT && moveTo(oldNode.getLeftSiblingKey()).hasMoved()
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
          pageTrx.prepareRecordForModification(oldNode.getLeftSiblingKey(), PageKind.RECORDPAGE, -1);
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
        rightSibling = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                            PageKind.RECORDPAGE,
                                                            -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      } else {
        rightSibling = pageTrx.prepareRecordForModification(oldNode.getRightSiblingKey(), PageKind.RECORDPAGE, -1);
        rightSibling.setLeftSiblingKey(oldNode.getLeftSiblingKey());
      }
    }

    // Adapt parent, if node has now left sibling it is a first child.
    StructNode parent = pageTrx.prepareRecordForModification(oldNode.getParentKey(), PageKind.RECORDPAGE, -1);
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
        final StructNode ancestor = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getCurrentNode().getNodeKey(),
                                                                         PageKind.RECORDPAGE,
                                                                         -1);
        ancestor.decrementDescendantCount();
        parent = ancestor;
      }
    }

    // Remove right sibling text node if text nodes have been
    // concatenated/merged.
    if (concatenated) {
      moveTo(oldNode.getRightSiblingKey());
      pageTrx.removeRecord(nodeReadOnlyTrx.getNodeKey(), PageKind.RECORDPAGE, -1);
    }

    // Remove non structural nodes of old node.
    if (oldNode.getKind() == NodeKind.ELEMENT) {
      moveTo(oldNode.getNodeKey());
      removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNode.getNodeKey());
    pageTrx.removeRecord(oldNode.getNodeKey(), PageKind.RECORDPAGE, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  /**
   * Making an intermediate commit based on set attributes.
   *
   * @throws SirixException if commit fails
   */
  private void intermediateCommitIfRequired() {
    nodeReadOnlyTrx.assertNotClosed();
    if ((maxNodeCount > 0) && (modificationCount > maxNodeCount)) {
      commit();
    }
  }

  /**
   * Set new descendant count of ancestor after an add-operation.
   *
   * @param startNode       the node which has been removed
   * @param nodeToModify    node to modify
   * @param descendantCount the descendantCount to add
   */
  private static void setAddDescendants(final ImmutableNode startNode, final Node nodeToModify,
      final @Nonnegative long descendantCount) {
    assert startNode != null;
    assert descendantCount >= 0;
    assert nodeToModify != null;
    if (startNode instanceof StructNode) {
      final StructNode node = (StructNode) nodeToModify;
      final long oldDescendantCount = node.getDescendantCount();
      node.setDescendantCount(oldDescendantCount + descendantCount);
    }
  }

  @Override
  public XmlNodeTrx copySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    acquireLock();
    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_FIRST_CHILD);
      moveTo(nodeKey);
      moveToFirstChild();
    } finally {
      unLock();
    }
    return this;
  }

  @Override
  public XmlNodeTrx copySubtreeAsLeftSibling(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    acquireLock();
    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_LEFT_SIBLING);
      moveTo(nodeKey);
      moveToFirstChild();
    } finally {
      unLock();
    }
    return this;
  }

  @Override
  public XmlNodeTrx copySubtreeAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    acquireLock();
    try {
      checkAccessAndCommit();
      final long nodeKey = getCurrentNode().getNodeKey();
      copy(rtx, InsertPosition.AS_RIGHT_SIBLING);
      moveTo(nodeKey);
      moveToRightSibling();
    } finally {
      unLock();
    }
    return this;
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
    final XmlNodeReadOnlyTrx rtx = trx.getResourceManager().beginNodeReadOnlyTrx(trx.getRevisionNumber());
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
    acquireLock();
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
      } else {
        throw new IllegalArgumentException("Not supported for attributes / namespaces.");
      }
    } finally {
      unLock();
    }

    return this;
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
  public XmlNodeTrx setBulkInsertion(boolean bulkInsertion) {
    nodeHashing.setBulkInsert(bulkInsertion);
    return this;
  }

  @Override
  public XmlNodeTrx replaceNode(final XmlNodeReadOnlyTrx rtx) {
    checkNotNull(rtx);
    acquireLock();
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
    } finally {
      unLock();
    }
    return this;
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
    long key = currentNode.getNodeKey();
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
    long key = currentNode.getNodeKey();
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

  private void removeOldNode(final StructNode node, final @Nonnegative long key) {
    assert node != null;
    assert key >= 0;
    moveTo(node.getNodeKey());
    remove();
    moveTo(key);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("readTrx", nodeReadOnlyTrx.toString())
                      .add("hashKind", hashType)
                      .toString();
  }

  @Override
  protected XmlNodeReadOnlyTrx delegate() {
    return nodeReadOnlyTrx;
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof XmlNodeTrxImpl wtx) {
      return Objects.equal(nodeReadOnlyTrx, wtx.nodeReadOnlyTrx);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeReadOnlyTrx);
  }

  @Override
  public PathSummaryReader getPathSummary() {
    acquireLock();
    try {
      return pathSummaryWriter.getPathSummary();
    } finally {
      unLock();
    }
  }

  /**
   * Acquire a lock if necessary.
   */
  void acquireLock() {
    if (lock != null) {
      lock.lock();
    }
  }

  /**
   * Release a lock if necessary.
   */
  void unLock() {
    if (lock != null) {
      lock.unlock();
    }
  }

  @Override
  public XmlNodeTrx addPreCommitHook(final PreCommitHook hook) {
    acquireLock();
    try {
      mPreCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public XmlNodeTrx addPostCommitHook(final PostCommitHook hook) {
    acquireLock();
    try {
      mPostCommitHooks.add(checkNotNull(hook));
      return this;
    } finally {
      unLock();
    }
  }

  @Override
  public XmlNodeTrx truncateTo(final int revision) {
    nodeReadOnlyTrx.assertNotClosed();

    // TODO

    return this;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    nodeReadOnlyTrx.assertNotClosed();

    return nodeReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public XmlNodeTrx commit(final String commitMessage) {
    nodeReadOnlyTrx.assertNotClosed();

    // Optionally lock while commiting and assigning new instances.
    acquireLock();
    try {
      // Execute pre-commit hooks.
      for (final PreCommitHook hook : mPreCommitHooks) {
        hook.preCommit(this);
      }

      // Reset modification counter.
      modificationCount = 0L;

      final UberPage uberPage = commitMessage == null ? pageTrx.commit() : pageTrx.commit(commitMessage);

      // Remember succesfully committed uber page in resource manager.
      resourceManager.setLastCommittedUberPage(uberPage);

      // Reinstantiate everything.
      reInstantiate(getId(), getRevisionNumber());
    } finally {
      unLock();
    }

    // Execute post-commit hooks.
    for (final PostCommitHook hook : mPostCommitHooks) {
      hook.postCommit(this);
    }

    return this;
  }

  /**
   * Create new instances.
   *
   * @param trxID     transaction ID
   * @param revNumber revision number
   */
  void reInstantiate(final @Nonnegative long trxID, final @Nonnegative int revNumber) {
    // Reset page transaction to new uber page.
    resourceManager.closeNodePageWriteTransaction(getId());
    pageTrx = resourceManager.createPageTransaction(trxID, revNumber, revNumber, Abort.NO, true);
    nodeReadOnlyTrx.setPageReadTransaction(null);
    nodeReadOnlyTrx.setPageReadTransaction(pageTrx);
    resourceManager.setNodePageWriteTransaction(getId(), pageTrx);

    nodeFactory = null;
    nodeFactory = new XmlNodeFactoryImpl(resourceManager.getResourceConfig().nodeHashFunction, pageTrx);
    nodeHashing = new XmlNodeHashing(hashType, nodeReadOnlyTrx, pageTrx);

    reInstantiateIndexes();
  }

  @Override
  public PageTrx getPageWtx() {
    nodeReadOnlyTrx.assertNotClosed();
    return pageTrx;
  }
}
