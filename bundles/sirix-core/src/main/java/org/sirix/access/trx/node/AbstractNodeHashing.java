package org.sirix.access.trx.node;

import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.IndexType;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.xml.ElementNode;

import java.nio.ByteBuffer;

public abstract class AbstractNodeHashing<N extends ImmutableNode, T extends NodeCursor & NodeReadOnlyTrx> {

  /**
   * Prime for computing the hash.
   */
  private static final long PRIME = 77081L;

  /**
   * The hash type.
   */
  private final HashType hashType;

  /**
   * The node read-only trx.
   */
  protected final T nodeReadOnlyTrx;

  /**
   * The page write trx.
   */
  private final PageTrx pageTrx;

  /**
   * {@code true} if bulk inserting is enabled, {@code false} otherwise
   */
  private boolean bulkInsert;

  private boolean autoCommit;

  private final Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer(50);

  /**
   * Constructor.
   *
   * @param resourceConfig  the resource configuration
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param pageTrx         the page trx
   */
  public AbstractNodeHashing(final ResourceConfiguration resourceConfig, final T nodeReadOnlyTrx,
      final PageTrx pageTrx) {
    this.hashType = resourceConfig.hashType;
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    this.pageTrx = pageTrx;
  }

  public void setBulkInsert(final boolean value) {
    this.bulkInsert = value;
  }

  public void setAutoCommit(final boolean value) {
    this.autoCommit = value;
  }

  /**
   * Adapting the structure with a hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  public void adaptHashesWithAdd() {
    if (!bulkInsert || autoCommit) {
      switch (hashType) {
        case ROLLING -> rollingAdd();
        case POSTORDER -> postorderAdd();
        case NONE, default -> {
        }
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with remove.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  public void adaptHashesWithRemove() {
    if (!bulkInsert || autoCommit) {
      switch (hashType) {
        case ROLLING -> rollingRemove();
        case POSTORDER -> postorderRemove();
        case NONE, default -> {
        }
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if an I/O error occurs
   */
  public void adaptHashedWithUpdate(final long oldHash) {
    if (!bulkInsert || autoCommit) {
      switch (hashType) {
        case ROLLING -> rollingUpdate(oldHash);
        case POSTORDER -> postorderAdd();
        case NONE, default -> {
        }
      }
    }
  }

  /**
   * Removal operation for postorder hash computation.
   *
   * @throws SirixIOException if anything weird happens
   */
  private void postorderRemove() {
    nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey());
    postorderAdd();
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if anything weird happened
   */
  private void postorderAdd() {
    // start with hash to add
    final var startNode = getCurrentNode();
    // long for adapting the hash of the parent
    long hashCodeForParent = 0;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(getCurrentNode().computeHash(bytes));
      nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      hashCodeForParent = getCurrentNode().computeHash(bytes) + hashCodeForParent * PRIME;
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == NodeKind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getAttributeKey(i));
          hashCodeForParent = getCurrentNode().computeHash(bytes) + hashCodeForParent * PRIME;
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getNamespaceKey(i));
          hashCodeForParent = getCurrentNode().computeHash(bytes) + hashCodeForParent * PRIME;
        }
        nodeReadOnlyTrx.moveTo(cursorToRoot.getNodeKey());
      }

      // Caring about the children of a node
      if (nodeReadOnlyTrx.moveTo(getStructuralNode().getFirstChildKey())) {
        do {
          hashCodeForParent = getCurrentNode().getHash() + hashCodeForParent * PRIME;
        } while (nodeReadOnlyTrx.moveTo(getStructuralNode().getRightSiblingKey()));
        nodeReadOnlyTrx.moveTo(getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
      hashCodeForParent = 0;
    } while (nodeReadOnlyTrx.moveTo(cursorToRoot.getParentKey()));

    setCurrentNode(startNode);
  }

  protected abstract StructNode getStructuralNode();

  protected abstract N getCurrentNode();

  protected abstract void setCurrentNode(N node);

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final long oldHash) {
    final var newNode = getCurrentNode();
    final long newHash = newNode.computeHash(bytes);
    long resultNew;

    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == newNode.getNodeKey()) {
        resultNew = newHash;
      } else {
        resultNew = node.getHash() - (oldHash * PRIME);
        resultNew = resultNew + newHash * PRIME;
      }
      node.setHash(resultNew);
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()));

    setCurrentNode(newNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final var startNode = getCurrentNode();
    long hashToRemove = startNode.getHash() == 0L ? startNode.computeHash(bytes) : startNode.getHash();
    long hashToAdd = 0;
    long newHash;
    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // the hash for the start node is always 0
        newHash = 0L;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // the parent node is just removed
        newHash = node.getHash() - hashToRemove * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      } else {
        // the ancestors are all touched regarding the modification
        newHash = node.getHash() - hashToRemove * PRIME;
        newHash = newHash + hashToAdd * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      }
      node.setHash(newHash);
      hashToAdd = newHash;
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()));

    setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startNode the node which has been removed
   */
  private void setRemoveDescendants(final ImmutableNode startNode) {
    assert startNode != null;
    if (startNode instanceof StructNode startNodeAsStructNode) {
      final StructNode node = getStructuralNode();
      node.setDescendantCount(node.getDescendantCount() - startNodeAsStructNode.getDescendantCount() - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() {
    // start with hash to add
    final var startNode = getCurrentNode();
    final long oldDescendantCount = getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;
    bytes.clear();
    long hashToAdd = startNode.getHash() == 0L ? startNode.computeHash(bytes) : startNode.getHash();
    long newHash;
    long possibleOldHash = 0L;

    if (isValueNode(startNode)) {
      nodeReadOnlyTrx.moveTo(startNode.getParentKey());
    }

    // go the path to the root
    Node node;
    do {
      node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // first, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // at the parent level, just add the node
        var newMultipliedHash = hashToAdd * PRIME;
        possibleOldHash = node.getHash();
        newHash = possibleOldHash + newMultipliedHash;
        hashToAdd = newHash;
        setAddDescendants(startNode, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element and add the new one
        var oldMultipliedHash = possibleOldHash * PRIME;
        var newMultipliedHash = hashToAdd * PRIME;
        newHash = node.getHash() - oldMultipliedHash + newMultipliedHash;
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startNode, node, descendantCount);
      }
      node.setHash(newHash);
    } while (nodeReadOnlyTrx.moveTo(node.getParentKey()));
    setCurrentNode(startNode);
  }

  private boolean isValueNode(N startNode) {
    return startNode.getKind() == NodeKind.STRING_VALUE || startNode.getKind() == NodeKind.OBJECT_STRING_VALUE
        || startNode.getKind() == NodeKind.BOOLEAN_VALUE || startNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE
        || startNode.getKind() == NodeKind.NUMBER_VALUE || startNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE
        || startNode.getKind() == NodeKind.NULL_VALUE || startNode.getKind() == NodeKind.OBJECT_NULL_VALUE
        || startNode.getKind() == NodeKind.ATTRIBUTE || startNode.getKind() == NodeKind.TEXT
        || startNode.getKind() == NodeKind.COMMENT || startNode.getKind() == NodeKind.PROCESSING_INSTRUCTION;
  }

  /**
   * Add a hash.
   *
   * @param startNode start node
   */
  public void addParentHash(final ImmutableNode startNode) {
    switch (hashType) {
      case ROLLING:
        long hashToAdd = startNode.computeHash(bytes);
        final Node parentNode =
            pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        var hash = parentNode.getHash();
        parentNode.setHash(hash + hashToAdd * PRIME);
        if (startNode instanceof StructNode startAsStructNode) {
          final StructNode parentNodeAsStructNode = (StructNode) parentNode;
          parentNodeAsStructNode.setDescendantCount(
              parentNodeAsStructNode.getDescendantCount() + startAsStructNode.getDescendantCount() + 1);
        }
        break;
      case POSTORDER:
        break;
      case NONE:
      default:
    }
  }

  /**
   * Add a hash and the descendant count.
   */
  public void addHashAndDescendantCount() {
    switch (hashType) {
      case ROLLING -> {
        // Setup.
        final var startNode = getCurrentNode();
        final long oldDescendantCount = getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;

        // Set start node.
        long hashToAdd = startNode.getHash() == 0L
            ? startNode.computeHash(bytes)
            : startNode.getHash() + startNode.computeHash(bytes);
        Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        node.setHash(hashToAdd);

        // Set parent node.
        if (startNode.hasParent()) {
          nodeReadOnlyTrx.moveTo(startNode.getParentKey());
          node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
          final var currentNodeHash = node.getHash();
          long hash = currentNodeHash == 0L ? node.computeHash(bytes) : currentNodeHash;
          node.setHash(hash + hashToAdd * PRIME);

          setAddDescendants(startNode, node, descendantCount);
        }
        setCurrentNode(startNode);
      }
      case POSTORDER -> postorderAdd();
      case NONE, default -> {
      }
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
      final @NonNegative long descendantCount) {
    assert startNode != null;
    assert nodeToModify != null;
    if (startNode instanceof StructNode) {
      final StructNode node = (StructNode) nodeToModify;
      final long oldDescendantCount = node.getDescendantCount();
      node.setDescendantCount(oldDescendantCount + descendantCount);
    }
  }

  public boolean isBulkInsert() {
    return bulkInsert;
  }
}
