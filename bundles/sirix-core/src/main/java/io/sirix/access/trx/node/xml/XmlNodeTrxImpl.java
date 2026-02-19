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

package io.sirix.access.trx.node.xml;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.access.trx.node.*;
import io.sirix.api.Axis;
import io.sirix.api.Movement;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.PostOrderAxis;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryWriter;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.*;
import io.sirix.node.interfaces.immutable.ImmutableNameNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.node.xml.*;
import io.sirix.page.NamePage;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.serialize.StAXSerializer;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.XMLToken;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
@SuppressWarnings({"StatementWithEmptyBody", "resource"})
final class XmlNodeTrxImpl extends
    AbstractNodeTrxImpl<XmlNodeReadOnlyTrx, XmlNodeTrx, XmlNodeFactory, ImmutableXmlNode, InternalXmlNodeReadOnlyTrx>
    implements InternalXmlNodeTrx, ForwardingXmlNodeReadOnlyTrx {

  private final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

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
   * @param resourceSession the resource manager this transaction is bound to
   * @param nodeReadOnlyTrx {@link StorageEngineWriter} to interact with the page layer
   * @param pathSummaryWriter the path summary writer
   * @param maxNodeCount maximum number of node modifications before auto commit
   * @param nodeHashing hashes node contents
   * @param nodeFactory the node factory used to create nodes
   * @throws SirixIOException if the reading of the props is failing
   * @throws SirixUsageException if {@code pMaxNodeCount < 0} or {@code pMaxTime < 0}
   */
  XmlNodeTrxImpl(final InternalResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> resourceSession,
      final InternalXmlNodeReadOnlyTrx nodeReadOnlyTrx, final PathSummaryWriter<XmlNodeReadOnlyTrx> pathSummaryWriter,
      final @NonNegative int maxNodeCount, @Nullable final Lock transactionLock, final Duration afterCommitDelay,
      final @NonNull XmlNodeHashing nodeHashing, final XmlNodeFactory nodeFactory,
      final @NonNull AfterCommitState afterCommitState, final RecordToRevisionsIndex nodeToRevisionsIndex) {
    super(Executors.defaultThreadFactory(), resourceSession.getResourceConfig().hashType, nodeReadOnlyTrx,
        nodeReadOnlyTrx, resourceSession, afterCommitState, nodeHashing, pathSummaryWriter, nodeFactory,
        nodeToRevisionsIndex, transactionLock, afterCommitDelay, maxNodeCount);
    indexController = resourceSession.getWtxIndexController(nodeReadOnlyTrx.getStorageEngineReader().getRevisionNumber());
    storeChildCount = this.resourceSession.getResourceConfig().storeChildCount();

    useTextCompression = resourceSession.getResourceConfig().useTextCompression;
    deweyIDManager = new XmlDeweyIDManager(this);

    // Register index listeners for any existing indexes.
    // This is critical for subsequent write transactions to update indexes on node modifications.
    final var existingIndexDefs = indexController.getIndexes().getIndexDefs();
    if (!existingIndexDefs.isEmpty()) {
      indexController.createIndexListeners(existingIndexDefs, this);
    }
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

      checkArgument(fromKey != getNodeKey(), "Can't move itself to first child of itself!");

      final DataRecord node = storageEngineWriter.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist!");
      }

      final var nodeToMove = node;
      if (nodeToMove instanceof StructNode && getKind() == NodeKind.ELEMENT) {
        // Safe to cast (because StructNode is a subtype of Node).
        checkAncestors((Node) nodeToMove);
        checkAccessAndCommit();

        final ElementNode nodeAnchor = (ElementNode) nodeReadOnlyTrx.getStructuralNode();

        // Check that it's not already the first child.
        if (nodeAnchor.getFirstChildKey() != nodeToMove.getNodeKey()) {
          final StructNode toMove = (StructNode) nodeToMove;

          // Adapt index-structures (before move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.DELETE);

          // Adapt hashes.
          adaptHashesForMove(toMove);

          // Adapt pointers and merge sibling text nodes.
          adaptForMove(toMove, nodeAnchor, InsertPos.ASFIRSTCHILD);
          nodeReadOnlyTrx.moveTo(toMove.getNodeKey());
          nodeHashing.adaptHashesWithAdd();

          // Adapt path summary.
          if (buildPathSummary && toMove instanceof NameNode moved) {
            pathSummaryWriter.adaptPathForChangedNode(moved, getName(), moved.getURIKey(), moved.getPrefixKey(),
                moved.getLocalNameKey(), PathSummaryWriter.OPType.MOVED);
          }

          // Adapt index-structures (after move).
          adaptSubtreeForMove(toMove, IndexController.ChangeType.INSERT);

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
   *        position)
   * @throws SirixIOException if an I/O exception occurs
   */
  private void adaptSubtreeForMove(final Node node, final IndexController.ChangeType type) throws SirixIOException {
    assert type != null;
    final long beforeNodeKey = getNodeKey();
    moveTo(node.getNodeKey());
    final Axis axis = new DescendantAxis(this, IncludeSelf.YES);
    while (axis.hasNext()) {
      axis.nextLong();
      for (int i = 0, attCount = getAttributeCount(); i < attCount; i++) {
        moveToAttribute(i);
        final AttributeNode att = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
        notifyPrimitiveIndexChange(type, att, att.getPathNodeKey());
        moveToParent();
      }
      for (int i = 0, nspCount = getNamespaceCount(); i < nspCount; i++) {
        moveToNamespace(i);
        final NamespaceNode nsp = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
        notifyPrimitiveIndexChange(type, nsp, nsp.getPathNodeKey());
        moveToParent();
      }
      final ImmutableNode currentNode = nodeReadOnlyTrx.getStructuralNode();
      long pathNodeKey = -1;
      if (currentNode instanceof ValueNode
          && currentNode.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
        final long nodeKey = currentNode.getNodeKey();
        moveToParent();
        pathNodeKey = getPathNodeKey();
        moveTo(nodeKey);
      } else if (currentNode instanceof NameNode nameNode) {
        pathNodeKey = nameNode.getPathNodeKey();
      }
      notifyPrimitiveIndexChange(type, currentNode, pathNodeKey);
    }
    moveTo(beforeNodeKey);
  }

  private void notifyPrimitiveIndexChange(final IndexController.ChangeType type, final ImmutableNode node,
      final long pathNodeKey) {
    if (!indexController.hasPathIndex() && !indexController.hasNameIndex() && !indexController.hasCASIndex()) {
      return;
    }

    final NodeKind kind = node.getKind();
    final long nodeKey = node.getNodeKey();

    final QNm name;
    if (indexController.hasNameIndex() && node instanceof NameNode nameNode) {
      name = switch (kind) {
        case ELEMENT, ATTRIBUTE, NAMESPACE, PROCESSING_INSTRUCTION -> nameNode.getName();
        default -> null;
      };
    } else {
      name = null;
    }

    final Str value;
    if (indexController.hasCASIndex() && node instanceof ValueNode valueNode) {
      value = switch (kind) {
        case ATTRIBUTE, COMMENT, PROCESSING_INSTRUCTION, TEXT -> new Str(valueNode.getValue());
        default -> null;
      };
    } else {
      value = null;
    }

    indexController.notifyChange(type, nodeKey, kind, pathNodeKey, name, value);
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
      if (fromKey == getNodeKey()) {
        throw new IllegalArgumentException("Can't move itself to first child of itself!");
      }

      // Save: Every node in the "usual" node page is of type Node.
      final var node = storageEngineWriter.getRecord(fromKey, IndexType.DOCUMENT, -1);
      if (node == null) {
        throw new IllegalStateException("Node to move must exist: " + fromKey);
      }

      final DataRecord nodeToMove = node;
      final NodeKind anchorKind = getKind();
      if (nodeToMove instanceof StructNode toMove && anchorKind != NodeKind.ATTRIBUTE
          && anchorKind != NodeKind.NAMESPACE) {
        final StructNode nodeAnchor = nodeReadOnlyTrx.getStructuralNode();
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
            final PathSummaryWriter.OPType type = moved.getParentKey() == parentKey
                ? PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL
                : PathSummaryWriter.OPType.MOVED;

            if (type != PathSummaryWriter.OPType.MOVED_ON_SAME_LEVEL) {
              pathSummaryWriter.adaptPathForChangedNode(moved, getName(), moved.getURIKey(), moved.getPrefixKey(),
                  moved.getLocalNameKey(), type);
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
   * @param fromNode root {@link StructNode} of the subtree to be moved
   * @param toNode the {@link StructNode} which is the anchor of the new subtree
   * @param insertPos determines if it has to be inserted as a first child or a right sibling
   * @throws SirixException if removing a node fails after merging text nodes
   */
  private void adaptForMove(final StructNode fromNode, final StructNode toNode, final InsertPos insertPos) {
    assert fromNode != null;
    assert toNode != null;
    assert insertPos != null;

    // Modify nodes where the subtree has been moved from.
    // ==============================================================================
    final StructNode parent = storageEngineWriter.prepareRecordForModification(fromNode.getParentKey(), IndexType.DOCUMENT, -1);
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
    persistUpdatedRecord(parent);

    // Adapt left sibling key of former right sibling.
    if (fromNode.hasRightSibling()) {
      final StructNode rightSibling =
          storageEngineWriter.prepareRecordForModification(fromNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
      rightSibling.setLeftSiblingKey(fromNode.getLeftSiblingKey());
      persistUpdatedRecord(rightSibling);
    }

    // Adapt right sibling key of former left sibling.
    if (fromNode.hasLeftSibling()) {
      final StructNode leftSibling =
          storageEngineWriter.prepareRecordForModification(fromNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
      leftSibling.setRightSiblingKey(fromNode.getRightSiblingKey());
      persistUpdatedRecord(leftSibling);
    }

    // Merge text nodes.
    if (fromNode.hasLeftSibling() && fromNode.hasRightSibling()) {
      final long leftSiblingNodeKey = fromNode.getLeftSiblingKey();
      final long rightSiblingNodeKey = fromNode.getRightSiblingKey();
      moveTo(leftSiblingNodeKey);
      if (getKind() == NodeKind.TEXT) {
        final StringBuilder builder = new StringBuilder(getValue());
        moveTo(rightSiblingNodeKey);
        if (getKind() == NodeKind.TEXT) {
          builder.append(getValue());
          if (rightSiblingNodeKey == toNode.getNodeKey()) {
            moveTo(leftSiblingNodeKey);
            final StructNode currentLeftNode = nodeReadOnlyTrx.getStructuralNode();
            final boolean hasLeftSibling = currentLeftNode.hasLeftSibling();
            if (hasLeftSibling) {
              final StructNode leftSibling =
                  storageEngineWriter.prepareRecordForModification(currentLeftNode.getLeftSiblingKey(), IndexType.DOCUMENT, -1);
              leftSibling.setRightSiblingKey(rightSiblingNodeKey);
              persistUpdatedRecord(leftSibling);
            }
            final long newLeftSiblingKey = hasLeftSibling
                ? currentLeftNode.getLeftSiblingKey()
                : currentLeftNode.getNodeKey();
            moveTo(rightSiblingNodeKey);
            final StructNode rightSibling = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
            rightSibling.setLeftSiblingKey(newLeftSiblingKey);
            persistUpdatedRecord(rightSibling);
            moveTo(leftSiblingNodeKey);
            remove();
            moveTo(rightSiblingNodeKey);
          } else {
            moveTo(rightSiblingNodeKey);
            final StructNode currentRightNode = nodeReadOnlyTrx.getStructuralNode();
            final boolean hasRightSibling = currentRightNode.hasRightSibling();
            if (hasRightSibling) {
              final StructNode rightSibling =
                  storageEngineWriter.prepareRecordForModification(currentRightNode.getRightSiblingKey(), IndexType.DOCUMENT, -1);
              rightSibling.setLeftSiblingKey(leftSiblingNodeKey);
              persistUpdatedRecord(rightSibling);
            }
            final long newRightSiblingKey = hasRightSibling
                ? currentRightNode.getRightSiblingKey()
                : currentRightNode.getNodeKey();
            moveTo(leftSiblingNodeKey);
            final StructNode leftSibling = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
            leftSibling.setRightSiblingKey(newRightSiblingKey);
            persistUpdatedRecord(leftSibling);
            moveTo(rightSiblingNodeKey);
            remove();
            moveTo(leftSiblingNodeKey);
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
    if (!XMLToken.isValidQName(requireNonNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();
      if (kind != NodeKind.ELEMENT && kind != NodeKind.XML_DOCUMENT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }

      checkAccessAndCommit();

      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = currentNode.getFirstChildKey();

      final long pathNodeKey = buildPathSummary
          ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT)
          : 0;
      final SirixDeweyID id = deweyIDManager.newFirstChildID();
      final ElementNode node = nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

      nodeReadOnlyTrx.setCurrentNode(node);
      adaptForInsert(node, InsertPos.ASFIRSTCHILD);
      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertElementAsLeftSibling(final QNm name) {
    if (!XMLToken.isValidQName(requireNonNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.XML_DOCUMENT || currentKind == NodeKind.ATTRIBUTE
          || currentKind == NodeKind.NAMESPACE) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
      }
      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long key = currentNode.getNodeKey();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();
      moveToParent();
      final long pathNodeKey = buildPathSummary
          ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT)
          : 0;
      moveTo(key);

      final SirixDeweyID id = deweyIDManager.newLeftSiblingID();
      final ElementNode node = nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

      nodeReadOnlyTrx.setCurrentNode(node);
      adaptForInsert(node, InsertPos.ASLEFTSIBLING);
      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertElementAsRightSibling(final QNm name) {
    if (!XMLToken.isValidQName(requireNonNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.XML_DOCUMENT || currentKind == NodeKind.ATTRIBUTE
          || currentKind == NodeKind.NAMESPACE) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an StructuralNode (either Text or Element)!");
      }
      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long key = currentNode.getNodeKey();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();
      moveToParent();
      final long pathNodeKey = buildPathSummary
          ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ELEMENT)
          : 0;
      moveTo(key);

      final SirixDeweyID id = deweyIDManager.newRightSiblingID();
      final ElementNode node = nodeFactory.createElementNode(parentKey, leftSibKey, rightSibKey, name, pathNodeKey, id);

      nodeReadOnlyTrx.setCurrentNode(node);
      adaptForInsert(node, InsertPos.ASRIGHTSIBLING);
      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
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
    requireNonNull(reader);
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
      final NodeKind currentKind = getKind();
      if (currentKind != NodeKind.ATTRIBUTE && currentKind != NodeKind.NAMESPACE) {
        checkAccessAndCommit();
        nodeHashing.setBulkInsert(true);
        final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
        final long nodeKey = currentNode.getNodeKey();
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
    while (getKind() != NodeKind.ELEMENT) {
      final ImmutableNode currentNode = nodeReadOnlyTrx.getStructuralNode();
      final long hashToAdd = currentNode.computeHash(bytes);
      final Node node = storageEngineWriter.prepareRecordForModification(currentNode.getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(hashToAdd);
      persistUpdatedRecord(node);

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
   * @param target target PI
   * @param content content of PI
   * @param insert insertion location
   * @throws SirixException if any unexpected error occurs
   */
  private record PositionKeys(long parentKey, long leftSibKey, long rightSibKey, InsertPos pos, SirixDeweyID id) {
  }

  private PositionKeys calculatePositionKeys(final StructNode currentNode, final InsertPosition insert) {
    return switch (insert) {
      case AS_FIRST_CHILD -> new PositionKeys(currentNode.getNodeKey(), Fixed.NULL_NODE_KEY.getStandardProperty(),
          currentNode.getFirstChildKey(), InsertPos.ASFIRSTCHILD, deweyIDManager.newFirstChildID());
      case AS_RIGHT_SIBLING -> new PositionKeys(currentNode.getParentKey(), currentNode.getNodeKey(),
          currentNode.getRightSiblingKey(), InsertPos.ASRIGHTSIBLING, deweyIDManager.newRightSiblingID());
      case AS_LEFT_SIBLING -> new PositionKeys(currentNode.getParentKey(), currentNode.getLeftSiblingKey(),
          currentNode.getNodeKey(), InsertPos.ASLEFTSIBLING, deweyIDManager.newLeftSiblingID());
      default -> throw new IllegalStateException("Insert location not known!");
    };
  }

  private XmlNodeTrx pi(final String target, final String content, final InsertPosition insert) {
    final byte[] targetBytes = getBytes(target);
    if (!XMLToken.isNCName(requireNonNull(targetBytes))) {
      throw new IllegalArgumentException("The target is not valid!");
    }
    if (content.contains("?>-")) {
      throw new SirixUsageException("The content must not contain '?>-'");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE) {
        throw new SirixUsageException("Current node must be a structural node!");
      }

      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
      final PositionKeys pk = calculatePositionKeys(currentNode, insert);

      final byte[] processingContent = getBytes(content);
      final QNm targetName = new QNm(target);
      final long pathNodeKey = buildPathSummary
          ? pathSummaryWriter.getPathNodeKey(targetName, NodeKind.PROCESSING_INSTRUCTION)
          : 0;
      final PINode node = nodeFactory.createPINode(pk.parentKey(), pk.leftSibKey(), pk.rightSibKey(), targetName,
          processingContent, useTextCompression, pathNodeKey, pk.id());

      // Adapt local nodes and hashes.
      nodeReadOnlyTrx.setCurrentNode(node);
      adaptForInsert(node, pk.pos());
      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      return this;
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
   * @param value value of comment
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
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE
          || (currentKind == NodeKind.XML_DOCUMENT && insert != InsertPosition.AS_FIRST_CHILD)) {
        throw new SirixUsageException("Current node must be a structural node!");
      }

      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();
      final PositionKeys pk = calculatePositionKeys(currentNode, insert);

      final byte[] commentValue = getBytes(value);
      final CommentNode node = nodeFactory.createCommentNode(pk.parentKey(), pk.leftSibKey(), pk.rightSibKey(),
          commentValue, useTextCompression, pk.id());

      // Adapt local nodes and hashes.
      nodeReadOnlyTrx.setCurrentNode(node);
      adaptForInsert(node, pk.pos());
      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsFirstChild(final String value) {
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (value.isEmpty() || currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE) {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode or TextNode!");
      }

      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final long pathNodeKey = ((NameNode) currentNode).getPathNodeKey();
      final long parentKey = currentNode.getNodeKey();
      final long leftSibKey = Fixed.NULL_NODE_KEY.getStandardProperty();
      final long rightSibKey = currentNode.getFirstChildKey();

      // Update value in case of adjacent text nodes.
      if (hasNode(rightSibKey)) {
        moveTo(rightSibKey);
        if (getKind() == NodeKind.TEXT) {
          final String mergedText = value + getValue();
          setValue(mergedText);
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
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsLeftSibling(final String value) {
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.XML_DOCUMENT || currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE
          || value.isEmpty()) {
        throw new SirixUsageException("Insert is not allowed if current node is not an Element- or Text-node!");
      }

      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final NodeKind currentNodeKind = currentNode.getKind();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getLeftSiblingKey();
      final long rightSibKey = currentNode.getNodeKey();

      // Update value in case of adjacent text nodes.
      final StringBuilder builder = new StringBuilder(value.length() + 16);
      if (currentNodeKind == NodeKind.TEXT) {
        builder.append(value);
      }
      builder.append(getValue());

      if (!value.contentEquals(builder)) {
        setValue(builder.toString());
        return this;
      }
      if (hasNode(leftSibKey)) {
        moveTo(leftSibKey);
        if (getKind() == NodeKind.TEXT) {
          final StringBuilder valueBuilder = new StringBuilder(builder.length() + 16);
          valueBuilder.append(getValue()).append(builder);
          if (!value.contentEquals(valueBuilder)) {
            setValue(valueBuilder.toString());
            return this;
          }
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
      final long pathNodeKey = isElement()
          ? getNameNode().getPathNodeKey()
          : -1;
      nodeReadOnlyTrx.setCurrentNode(node);

      // Index text value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  @Override
  public XmlNodeTrx insertTextAsRightSibling(final String value) {
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.XML_DOCUMENT || currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE
          || value.isEmpty()) {
        throw new SirixUsageException(
            "Insert is not allowed if current node is not an Element- or Text-node or value is empty!");
      }

      checkAccessAndCommit();
      final StructNode currentNode = nodeReadOnlyTrx.getStructuralNode();

      final NodeKind currentNodeKind = currentNode.getKind();
      final long parentKey = currentNode.getParentKey();
      final long leftSibKey = currentNode.getNodeKey();
      final long rightSibKey = currentNode.getRightSiblingKey();

      // Update value in case of adjacent text nodes.
      final StringBuilder currentValueBuilder = new StringBuilder(value.length() + 16);
      if (currentNodeKind == NodeKind.TEXT) {
        currentValueBuilder.append(getValue());
      }
      currentValueBuilder.append(value);
      String currentValue = currentValueBuilder.toString();
      if (!value.equals(currentValue)) {
        setValue(currentValue);
        return this;
      }
      if (hasNode(rightSibKey)) {
        moveTo(rightSibKey);
        if (getKind() == NodeKind.TEXT) {
          final StringBuilder valueBuilder = new StringBuilder(currentValue.length() + 16);
          valueBuilder.append(currentValue).append(getValue());
          currentValue = valueBuilder.toString();
          if (!value.equals(currentValue)) {
            setValue(currentValue);
            return this;
          }
        }
      }

      // Insert new text node if no adjacent text nodes are found.
      moveTo(leftSibKey);
      final byte[] textValue = getBytes(currentValue);
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
      final long pathNodeKey = isElement()
          ? getNameNode().getPathNodeKey()
          : -1;
      nodeReadOnlyTrx.setCurrentNode(node);

      // Index text value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      return this;
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
    requireNonNull(value);
    if (!XMLToken.isValidQName(requireNonNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getKind() != NodeKind.ELEMENT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }

      checkAccessAndCommit();

      /*
       * Update value in case of the same attribute name is found but the attribute to insert has a
       * different value (otherwise an exception is thrown because of a duplicate attribute which would
       * otherwise be inserted!).
       */
      final ElementNode element = (ElementNode) nodeReadOnlyTrx.getStructuralNode();
      final long elementKey = element.getNodeKey();
      long attKey = -1;
      for (int i = 0; i < element.getAttributeCount(); i++) {
        final long attributeKey = element.getAttributeKey(i);
        if (moveTo(attributeKey) && getName().equals(name)) {
          attKey = attributeKey;
          break;
        }
      }
      moveTo(elementKey);
      if (attKey != -1) {
        moveTo(attKey);
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
      final long pathNodeKey = resourceSession.getResourceConfig().withPathSummary
          ? pathSummaryWriter.getPathNodeKey(name, NodeKind.ATTRIBUTE)
          : 0;
      final byte[] attValue = getBytes(value);

      final SirixDeweyID id = deweyIDManager.newAttributeID();
      final AttributeNode node = nodeFactory.createAttributeNode(elementKey, name, attValue, pathNodeKey, id);

      final Node parentNode = storageEngineWriter.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
      ((ElementNode) parentNode).insertAttribute(node.getNodeKey());
      persistUpdatedRecord(parentNode);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();

      // Index text value.
      notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, node, pathNodeKey);

      if (move == Movement.TOPARENT) {
        moveToParent();
      }
      return this;
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
    if (!XMLToken.isValidQName(requireNonNull(name))) {
      throw new IllegalArgumentException("The QName is not valid!");
    }
    if (lock != null) {
      lock.lock();
    }

    try {
      if (getKind() != NodeKind.ELEMENT) {
        throw new SirixUsageException("Insert is not allowed if current node is not an ElementNode!");
      }

      checkAccessAndCommit();

      final ElementNode element = (ElementNode) nodeReadOnlyTrx.getStructuralNode();
      final long elementKey = element.getNodeKey();
      for (int i = 0, namespCount = element.getNamespaceCount(); i < namespCount; i++) {
        moveTo(element.getNamespaceKey(i));
        final QNm qName = getName();
        if (name.getPrefix().equals(qName.getPrefix())) {
          throw new SirixUsageException("Duplicate namespace!");
        }
      }
      moveTo(elementKey);

      final long pathNodeKey = buildPathSummary
          ? pathSummaryWriter.getPathNodeKey(name, NodeKind.NAMESPACE)
          : 0;

      final SirixDeweyID id = deweyIDManager.newNamespaceID();
      final NamespaceNode node = nodeFactory.createNamespaceNode(elementKey, name, pathNodeKey, id);

      final Node parentNode = storageEngineWriter.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
      ((ElementNode) parentNode).insertNamespace(node.getNodeKey());
      persistUpdatedRecord(parentNode);

      nodeReadOnlyTrx.setCurrentNode(node);
      nodeHashing.adaptHashesWithAdd();
      if (move == Movement.TOPARENT) {
        moveToParent();
      }
      return this;
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
   *         move
   */
  private void checkAncestors(final Node node) {
    assert node != null;
    final long startNodeKey = getNodeKey();
    while (hasParent()) {
      moveToParent();
      if (getNodeKey() == node.getNodeKey()) {
        throw new IllegalStateException("Moving one of the ancestor nodes is not permitted!");
      }
    }
    moveTo(startNodeKey);
  }

  @Override
  public XmlNodeTrx remove() {
    checkAccessAndCommit();
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind kind = getKind();
      if (kind == NodeKind.XML_DOCUMENT) {
        throw new SirixUsageException("Document root can not be removed.");
      } else if (kind == NodeKind.ATTRIBUTE) {
        final AttributeNode node = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);

        notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = storageEngineWriter.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        parent.removeAttribute(node.getNodeKey());
        persistUpdatedRecord(parent);
        nodeHashing.adaptHashesWithRemove();
        storageEngineWriter.removeRecord(node.getNodeKey(), IndexType.DOCUMENT, -1);
        removeName();
        moveToParent();
      } else if (kind == NodeKind.NAMESPACE) {
        final NamespaceNode node = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);

        notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, node.getPathNodeKey());
        final ElementNode parent = storageEngineWriter.prepareRecordForModification(node.getParentKey(), IndexType.DOCUMENT, -1);
        parent.removeNamespace(node.getNodeKey());
        persistUpdatedRecord(parent);
        nodeHashing.adaptHashesWithRemove();
        storageEngineWriter.removeRecord(node.getNodeKey(), IndexType.DOCUMENT, -1);
        removeName();
        moveToParent();
      } else {
        final StructNode node = nodeReadOnlyTrx.getStructuralNode();

        // for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); )
        // {
        // descendantAxis.nextLong();
        // System.out.println("path: " + getPathSummary().getPath());
        // System.out.println("nodeKey: " + getPathSummary().getNodeKey());
        // System.out.println("references: " + getPathSummary().getReferences());
        // }

        // Remove subtree.
        for (final Axis axis = new PostOrderAxis(this); axis.hasNext();) {
          final long currentNodeKey = axis.nextLong();

          // Remove name.
          removeName();

          // Remove namespaces and attributes.
          removeNonStructural();

          // Remove text value.
          removeValue();

          // Then remove node.
          storageEngineWriter.removeRecord(currentNodeKey, IndexType.DOCUMENT, -1);
        }

        // getPathSummary().moveToDocumentRoot();
        //
        // System.out.println("=====================");
        //
        // for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); )
        // {
        // descendantAxis.nextLong();
        // System.out.println("path: " + getPathSummary().getPath());
        // System.out.println("nodeKey: " + getPathSummary().getNodeKey());
        // System.out.println("references: " + getPathSummary().getReferences());
        // }

        // removeNonStructural();
        removeName();
        removeValue();

        // getPathSummary().moveToDocumentRoot();
        //
        // System.out.println("=====================");
        //
        // for (final var descendantAxis = new DescendantAxis(getPathSummary()); descendantAxis.hasNext(); )
        // {
        // descendantAxis.nextLong();
        // System.out.println("path: " + getPathSummary().getPath());
        // System.out.println("nodeKey: " + getPathSummary().getNodeKey());
        // System.out.println("references: " + getPathSummary().getReferences());
        // }

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
      }

      return this;
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
  }

  private void removeValue() throws SirixIOException {
    final NodeKind kind = getKind();
    final ValueNode valueNode;
    if (kind == NodeKind.TEXT || kind == NodeKind.COMMENT || kind == NodeKind.PROCESSING_INSTRUCTION) {
      valueNode = (ValueNode) nodeReadOnlyTrx.getStructuralNode();
    } else if (kind == NodeKind.ATTRIBUTE) {
      valueNode = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
    } else {
      return;
    }

    final long nodeKey = getNodeKey();
    final long pathNodeKey = moveToParent()
        ? getPathNodeKey()
        : -1;
    moveTo(nodeKey);
    notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) valueNode, pathNodeKey);
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
        final long attributeNodeKey = getNodeKey();
        removeName();
        removeValue();
        storageEngineWriter.removeRecord(attributeNodeKey, IndexType.DOCUMENT, -1);
        moveToParent();
      }
      final int nspCount = nodeReadOnlyTrx.getNamespaceCount();
      for (int i = 0; i < nspCount; i++) {
        moveToNamespace(i);
        final long namespaceNodeKey = getNodeKey();
        removeName();
        storageEngineWriter.removeRecord(namespaceNodeKey, IndexType.DOCUMENT, -1);
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
    final NodeKind kind = getKind();
    final ImmutableNameNode node;
    if (kind == NodeKind.ELEMENT || kind == NodeKind.PROCESSING_INSTRUCTION) {
      node = (NameNode) nodeReadOnlyTrx.getStructuralNode();
    } else if (kind == NodeKind.ATTRIBUTE || kind == NodeKind.NAMESPACE) {
      node = (ImmutableNameNode) nodeReadOnlyTrx.getCurrentNode();
    } else {
      return;
    }

    notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, node, node.getPathNodeKey());
    final NodeKind nodeKind = node.getKind();
    final NamePage page = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
    page.removeName(node.getPrefixKey(), nodeKind, storageEngineWriter);
    page.removeName(node.getLocalNameKey(), nodeKind, storageEngineWriter);
    page.removeName(node.getURIKey(), NodeKind.NAMESPACE, storageEngineWriter);

    assert nodeKind != NodeKind.XML_DOCUMENT;
    if (buildPathSummary) {
      pathSummaryWriter.remove(node);
    }
  }

  @Override
  public XmlNodeTrx setName(final QNm name) {
    requireNonNull(name);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.ELEMENT || currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE
          || currentKind == NodeKind.PROCESSING_INSTRUCTION) {
        if (!getName().equals(name)) {
          checkAccessAndCommit();

          final NameNode node;
          if (currentKind == NodeKind.ELEMENT || currentKind == NodeKind.PROCESSING_INSTRUCTION) {
            node = (NameNode) nodeReadOnlyTrx.getStructuralNode();
          } else {
            node = (NameNode) nodeReadOnlyTrx.getCurrentNode();
          }
          final long oldHash = node.computeHash(bytes);

          // Remove old keys from mapping.
          final NodeKind nodeKind = node.getKind();
          final int oldPrefixKey = node.getPrefixKey();
          final int oldLocalNameKey = node.getLocalNameKey();
          final int oldUriKey = node.getURIKey();
          final NamePage page = ((NamePage) storageEngineWriter.getActualRevisionRootPage().getNamePageReference().getPage());
          page.removeName(oldPrefixKey, nodeKind, storageEngineWriter);
          page.removeName(oldLocalNameKey, nodeKind, storageEngineWriter);
          page.removeName(oldUriKey, NodeKind.NAMESPACE, storageEngineWriter);

          // Create new keys for mapping.
          final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
              ? storageEngineWriter.createNameKey(name.getPrefix(), node.getKind())
              : -1;
          final int localNameKey = name.getLocalName() != null && !name.getLocalName().isEmpty()
              ? storageEngineWriter.createNameKey(name.getLocalName(), node.getKind())
              : -1;
          final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
              ? storageEngineWriter.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
              : -1;

          // Set new keys for current node.
          node.setLocalNameKey(localNameKey);
          node.setURIKey(uriKey);
          node.setPrefixKey(prefixKey);

          // Adapt path summary.
          if (buildPathSummary) {
            pathSummaryWriter.adaptPathForChangedNode(node, name, uriKey, prefixKey, localNameKey,
                PathSummaryWriter.OPType.SETNAME);
          }

          // Set path node key.
          node.setPathNodeKey(buildPathSummary
              ? pathSummaryWriter.getNodeKey()
              : 0);

          nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) node);
          persistUpdatedRecord(node);
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
    requireNonNull(value);
    if (lock != null) {
      lock.lock();
    }

    try {
      final NodeKind currentKind = getKind();
      if (currentKind == NodeKind.TEXT || currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.COMMENT
          || currentKind == NodeKind.PROCESSING_INSTRUCTION) {
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

        final ValueNode node;
        if (currentKind == NodeKind.TEXT || currentKind == NodeKind.COMMENT
            || currentKind == NodeKind.PROCESSING_INSTRUCTION) {
          node = (ValueNode) nodeReadOnlyTrx.getStructuralNode();
        } else {
          node = (ValueNode) nodeReadOnlyTrx.getCurrentNode();
        }
        // Remove old value from indexes before mutating the node.
        notifyPrimitiveIndexChange(IndexController.ChangeType.DELETE, (ImmutableNode) node, pathNodeKey);
        final long oldHash = node.computeHash(bytes);
        final byte[] byteVal = getBytes(value);
        node.setRawValue(byteVal);
        node.setPreviousRevision(node.getLastModifiedRevisionNumber());
        node.setLastModifiedRevision(nodeReadOnlyTrx.getRevisionNumber());

        nodeReadOnlyTrx.setCurrentNode((ImmutableXmlNode) node);
        persistUpdatedRecord(node);
        nodeHashing.adaptHashedWithUpdate(oldHash);

        // Index new value.
        notifyPrimitiveIndexChange(IndexController.ChangeType.INSERT, (ImmutableNode) node, pathNodeKey);

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
   * @param newNode pointer of the new node to be inserted
   * @param insertPos determines the position where to insert
   * @throws SirixIOException if anything weird happens
   */
  private void adaptForInsert(final Node newNode, final InsertPos insertPos) throws SirixIOException {
    assert newNode != null;
    assert insertPos != null;

    if (newNode instanceof StructNode structNode) {
      // Capture all needed keys before any prepareRecordForModification calls.
      // With write-path singletons, prepareRecordForModification for a node of the same kind
      // would overwrite the singleton, invalidating prior references.
      final long structNodeKey = structNode.getNodeKey();
      final long parentKey = newNode.getParentKey();
      final long leftSibKey = structNode.getLeftSiblingKey();
      final long rightSibKey = structNode.getRightSiblingKey();
      final boolean hasLeft = structNode.hasLeftSibling();
      final boolean hasRight = structNode.hasRightSibling();

      // Phase 1: Update parent — complete all modifications and persist BEFORE siblings.
      final StructNode parent = storageEngineWriter.prepareRecordForModification(parentKey, IndexType.DOCUMENT, -1);
      if (storeChildCount) {
        parent.incrementChildCount();
      }
      if (!hasLeft) {
        parent.setFirstChildKey(structNodeKey);
      }
      persistUpdatedRecord(parent);

      // Phase 2: Update right sibling (safe — parent already persisted)
      if (hasRight) {
        final StructNode rightSiblingNode = storageEngineWriter.prepareRecordForModification(rightSibKey, IndexType.DOCUMENT, -1);
        rightSiblingNode.setLeftSiblingKey(structNodeKey);
        persistUpdatedRecord(rightSiblingNode);
      }

      // Phase 3: Update left sibling
      if (hasLeft) {
        final StructNode leftSiblingNode = storageEngineWriter.prepareRecordForModification(leftSibKey, IndexType.DOCUMENT, -1);
        leftSiblingNode.setRightSiblingKey(structNodeKey);
        persistUpdatedRecord(leftSiblingNode);
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
    // Capture all needed values from oldNode before any prepareRecordForModification calls.
    // With write-path singletons, subsequent calls for the same kind would overwrite the singleton.
    final long leftSibKey = oldNode.getLeftSiblingKey();
    final long rightSibKey = oldNode.getRightSiblingKey();
    final long parentKey = oldNode.getParentKey();
    final boolean hasLeft = oldNode.hasLeftSibling();
    final boolean hasRight = oldNode.hasRightSibling();
    final long oldNodeKey = oldNode.getNodeKey();
    final NodeKind oldNodeKind = oldNode.getKind();

    // Concatenate neighbor text nodes if they exist (the right sibling is
    // deleted afterwards).
    boolean concatenated = false;
    if (hasLeft && hasRight && moveTo(rightSibKey) && getKind() == NodeKind.TEXT && moveTo(leftSibKey)
        && getKind() == NodeKind.TEXT) {
      final StringBuilder builder = new StringBuilder(getValue());
      moveTo(rightSibKey);
      builder.append(getValue());
      moveTo(leftSibKey);
      setValue(builder.toString());
      concatenated = true;
    }

    // Phase 1: Adapt left sibling node if there is one.
    if (hasLeft) {
      final StructNode leftSibling = storageEngineWriter.prepareRecordForModification(leftSibKey, IndexType.DOCUMENT, -1);
      if (concatenated) {
        moveTo(rightSibKey);
        leftSibling.setRightSiblingKey(nodeReadOnlyTrx.getStructuralNode().getRightSiblingKey());
      } else {
        leftSibling.setRightSiblingKey(rightSibKey);
      }
      persistUpdatedRecord(leftSibling);
    }

    // Phase 2: Adapt right sibling node if there is one.
    if (hasRight) {
      StructNode rightSibling;
      if (concatenated) {
        moveTo(rightSibKey);
        moveTo(nodeReadOnlyTrx.getStructuralNode().getRightSiblingKey());
        rightSibling = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
        rightSibling.setLeftSiblingKey(leftSibKey);
      } else {
        rightSibling = storageEngineWriter.prepareRecordForModification(rightSibKey, IndexType.DOCUMENT, -1);
        rightSibling.setLeftSiblingKey(leftSibKey);
      }
      persistUpdatedRecord(rightSibling);
    }

    // Phase 3: Adapt parent
    final StructNode parent = storageEngineWriter.prepareRecordForModification(parentKey, IndexType.DOCUMENT, -1);
    if (!hasLeft) {
      parent.setFirstChildKey(rightSibKey);
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
    final long parentNodeKey = parent.getNodeKey();
    final boolean parentHasParent = parent.hasParent();
    persistUpdatedRecord(parent);
    if (concatenated) {
      // Adjust descendant count — each ancestor gets its own singleton lifecycle.
      moveTo(parentNodeKey);
      boolean hasAncestorParent = parentHasParent;
      while (hasAncestorParent) {
        moveToParent();
        final StructNode ancestor = storageEngineWriter.prepareRecordForModification(getNodeKey(), IndexType.DOCUMENT, -1);
        ancestor.decrementDescendantCount();
        hasAncestorParent = ancestor.hasParent();
        persistUpdatedRecord(ancestor);
      }
    }

    // Remove right sibling text node if text nodes have been
    // concatenated/merged.
    if (concatenated) {
      moveTo(rightSibKey);
      storageEngineWriter.removeRecord(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    }

    // Remove non-structural nodes of old node.
    if (oldNodeKind == NodeKind.ELEMENT) {
      moveTo(oldNodeKey);
      removeNonStructural();
    }

    // Remove old node.
    moveTo(oldNodeKey);
    storageEngineWriter.removeRecord(oldNodeKey, IndexType.DOCUMENT, -1);
  }

  // ////////////////////////////////////////////////////////////
  // end of remove operation
  // ////////////////////////////////////////////////////////////

  @Override
  public XmlNodeTrx copySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
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
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
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
    requireNonNull(rtx);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();
      final long nodeKey = getNodeKey();
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
   * @param trx the source {@link XmlNodeReadOnlyTrx}
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
      case TEXT -> {
        final String textValue = rtx.getValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertTextAsFirstChild(textValue);
          case AS_LEFT_SIBLING -> insertTextAsLeftSibling(textValue);
          case AS_RIGHT_SIBLING -> insertTextAsRightSibling(textValue);
          default -> throw new IllegalStateException();
        }
      }
      case PROCESSING_INSTRUCTION -> {
        switch (insert) {
          case AS_FIRST_CHILD -> insertPIAsFirstChild(rtx.getName().getLocalName(), rtx.getValue());
          case AS_LEFT_SIBLING -> insertPIAsLeftSibling(rtx.getName().getLocalName(), rtx.getValue());
          case AS_RIGHT_SIBLING -> insertPIAsRightSibling(rtx.getName().getLocalName(), rtx.getValue());
          default -> throw new IllegalStateException();
        }
      }
      case COMMENT -> {
        final String commentValue = rtx.getValue();
        switch (insert) {
          case AS_FIRST_CHILD -> insertCommentAsFirstChild(commentValue);
          case AS_LEFT_SIBLING -> insertCommentAsLeftSibling(commentValue);
          case AS_RIGHT_SIBLING -> insertCommentAsRightSibling(commentValue);
          default -> throw new IllegalStateException();
        }
      }
      // $CASES-OMITTED$
      default -> new XmlShredder.Builder(this, new StAXSerializer(rtx), insert).build().call();
    }
    rtx.close();
  }

  @Override
  public XmlNodeTrx replaceNode(final XMLEventReader reader) {
    requireNonNull(reader);
    if (lock != null) {
      lock.lock();
    }

    try {
      checkAccessAndCommit();

      final NodeKind currentKind = getKind();
      if (currentKind != NodeKind.ATTRIBUTE && currentKind != NodeKind.NAMESPACE) {
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

  @SuppressWarnings("resource")
  @Override
  public XmlNodeTrx replaceNode(final XmlNodeReadOnlyTrx rtx) {
    requireNonNull(rtx);
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
          if (getKind() != NodeKind.ATTRIBUTE) {
            throw new IllegalStateException("Current node must be an attribute node!");
          }
          remove();
          insertAttribute(rtx.getName(), rtx.getValue());
        }
        case NAMESPACE -> {
          if (getKind() != NodeKind.NAMESPACE) {
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
    final NodeKind currentKind = getKind();
    if (currentKind == NodeKind.ATTRIBUTE || currentKind == NodeKind.NAMESPACE) {
      throw new IllegalStateException("Current node must be a structural node!");
    }
  }

  private void removeAndThenInsert(final XmlNodeReadOnlyTrx rtx) {
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

  private void removeOldNode(final @NonNull StructNode node, final @NonNegative long key) {
    moveTo(node.getNodeKey());
    remove();
    moveTo(key);
  }

  @Override
  protected AbstractNodeHashing<ImmutableXmlNode, XmlNodeReadOnlyTrx> reInstantiateNodeHashing(
      StorageEngineWriter storageEngineWriter) {
    return new XmlNodeHashing(resourceSession.getResourceConfig(), nodeReadOnlyTrx, storageEngineWriter);
  }

  @Override
  protected XmlNodeFactory reInstantiateNodeFactory(StorageEngineWriter storageEngineWriter) {
    return new XmlNodeFactoryImpl(resourceSession.getResourceConfig().nodeHashFunction, storageEngineWriter);
  }
}
