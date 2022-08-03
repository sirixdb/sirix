package org.sirix.access.trx.node;

import org.checkerframework.checker.index.qual.NonNegative;
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

import java.math.BigInteger;

public abstract class AbstractNodeHashing<N extends ImmutableNode, T extends NodeCursor & NodeReadOnlyTrx> {

  /**
   * Prime for computing the hash.
   */
  private static final BigInteger PRIME = BigInteger.valueOf(77081);

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

  /**
   * Constructor.
   *
   * @param hashType        the hash type used
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param pageTrx         the page trx
   */
  public AbstractNodeHashing(final HashType hashType, final T nodeReadOnlyTrx, final PageTrx pageTrx) {
    this.hashType = hashType;
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    this.pageTrx = pageTrx;
  }

  public AbstractNodeHashing<N, T> setBulkInsert(final boolean value) {
    this.bulkInsert = value;
    return this;
  }

  public AbstractNodeHashing<N, T> setAutoCommit(final boolean value) {
    this.autoCommit = value;
    return this;
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
  public void adaptHashedWithUpdate(final BigInteger oldHash) {
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
    BigInteger hashCodeForParent = BigInteger.ZERO;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(getCurrentNode().computeHash());
      nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      hashCodeForParent = getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == NodeKind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getAttributeKey(i));
          hashCodeForParent = getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getNamespaceKey(i));
          hashCodeForParent = getCurrentNode().computeHash().add(hashCodeForParent.multiply(PRIME));
        }
        nodeReadOnlyTrx.moveTo(cursorToRoot.getNodeKey());
      }

      // Caring about the children of a node
      if (nodeReadOnlyTrx.moveTo(getStructuralNode().getFirstChildKey())) {
        do {
          hashCodeForParent = getCurrentNode().getHash().add(hashCodeForParent.multiply(PRIME));
        } while (nodeReadOnlyTrx.moveTo(getStructuralNode().getRightSiblingKey()));
        nodeReadOnlyTrx.moveTo(getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
      hashCodeForParent = BigInteger.ZERO;
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
  private void rollingUpdate(final BigInteger oldHash) {
    final var newNode = getCurrentNode();
    final BigInteger hash = newNode.computeHash();
    BigInteger resultNew;

    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == newNode.getNodeKey()) {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(oldHash).add(hash));
      } else {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash()
                                                          .subtract(oldHash.multiply(PRIME))
                                                          .add(hash.multiply(PRIME)));
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
    BigInteger hashToRemove = startNode.getHash() == null || BigInteger.ZERO.equals(startNode.getHash())
        ? startNode.computeHash()
        : startNode.getHash();
    BigInteger hashToAdd = BigInteger.ZERO;
    BigInteger newHash;
    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // the begin node is always null
        newHash = BigInteger.ZERO;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // the parent node is just removed
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(hashToRemove.multiply(PRIME)));
        hashToRemove = Node.to128BitsAtMaximumBigInteger(node.getHash());
        setRemoveDescendants(startNode);
      } else {
        // the ancestors are all touched regarding the modification
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash()
                                                        .subtract(hashToRemove.multiply(PRIME))
                                                        .add(hashToAdd.multiply(PRIME)));
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
    BigInteger hashToAdd = startNode.getHash() == null || BigInteger.ZERO.equals(startNode.getHash())
        ? startNode.computeHash()
        : startNode.getHash();
    BigInteger newHash;
    BigInteger possibleOldHash = BigInteger.ZERO;

    if (isValueNode(startNode)) {
      nodeReadOnlyTrx.moveTo(startNode.getParentKey());
    }

    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // first, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // at the parent level, just add the node
        var newMultipliedHash = hashToAdd.multiply(PRIME);
        possibleOldHash = node.getHash();
        newHash = Node.to128BitsAtMaximumBigInteger(possibleOldHash.add(newMultipliedHash));
        newMultipliedHash = null;
        hashToAdd = newHash;
        setAddDescendants(startNode, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element and add the new one
        var oldMultipliedHash = possibleOldHash.multiply(PRIME);
        var newMultipliedHash = hashToAdd.multiply(PRIME);
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash()
                                                        .subtract(oldMultipliedHash)
                                                        .add(newMultipliedHash));
        oldMultipliedHash = null;
        newMultipliedHash = null;
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startNode, node, descendantCount);
      }
      node.setHash(newHash);
      newHash = null;
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()));
    setCurrentNode(startNode);
  }

  private boolean isValueNode(N startNode) {
    return startNode.getKind() == NodeKind.STRING_VALUE || startNode.getKind() == NodeKind.OBJECT_STRING_VALUE
        || startNode.getKind() == NodeKind.BOOLEAN_VALUE || startNode.getKind() == NodeKind.OBJECT_BOOLEAN_VALUE
        || startNode.getKind() == NodeKind.NUMBER_VALUE || startNode.getKind() == NodeKind.OBJECT_NUMBER_VALUE
        || startNode.getKind() == NodeKind.NULL_VALUE || startNode.getKind() == NodeKind.OBJECT_NULL_VALUE;
  }

  /**
   * Add a hash.
   *
   * @param startNode start node
   */
  public void addParentHash(final ImmutableNode startNode) {
    switch (hashType) {
      case ROLLING:
        BigInteger hashToAdd = startNode.computeHash();
        final Node parentNode =
            pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        var hash = parentNode.getHash();
        parentNode.setHash(hash.add(hashToAdd.multiply(PRIME)));
        hash = null;
        hashToAdd = null;
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
        BigInteger hashToAdd = startNode.getHash() == null || BigInteger.ZERO.equals(startNode.getHash())
            ? startNode.computeHash()
            : startNode.getHash().add(startNode.computeHash());
        Node node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
        node.setHash(hashToAdd);

        // Set parent node.
        if (startNode.hasParent()) {
          nodeReadOnlyTrx.moveTo(startNode.getParentKey());
          node = pageTrx.prepareRecordForModification(getCurrentNode().getNodeKey(), IndexType.DOCUMENT, -1);
          final var currentNodeHash = node.getHash();
          BigInteger hash =
              currentNodeHash == null || BigInteger.ZERO.equals(currentNodeHash) ? node.computeHash() : currentNodeHash;
          node.setHash(hash.add(hashToAdd.multiply(PRIME)));
          hash = null;

          setAddDescendants(startNode, node, descendantCount);
        }
        setCurrentNode(startNode);
        hashToAdd = null;
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
    assert descendantCount >= 0;
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
