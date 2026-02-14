package io.sirix.access.trx.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.StorageEngineWriter;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.xml.ElementNode;
import org.checkerframework.checker.index.qual.NonNegative;

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
  private final StorageEngineWriter pageTrx;

  /**
   * {@code true} if bulk inserting is enabled, {@code false} otherwise
   */
  private boolean bulkInsert;

  private boolean autoCommit;

  private final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

  /**
   * Constructor.
   *
   * @param resourceConfig  the resource configuration
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param pageTrx         the page trx
   */
  protected AbstractNodeHashing(final ResourceConfiguration resourceConfig, final T nodeReadOnlyTrx,
      final StorageEngineWriter pageTrx) {
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
        case NONE -> {
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
        case NONE -> {
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
        case NONE -> {
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
    nodeReadOnlyTrx.moveTo(nodeReadOnlyTrx.getParentKey());
    postorderAdd();
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if anything weird happened
   */
  private void postorderAdd() {
    // start with hash to add
    final Node startNode = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    // long for adapting the hash of the parent
    long hashCodeForParent;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(node.computeHash(bytes));
      nodeReadOnlyTrx.moveTo(nodeReadOnlyTrx.getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      hashCodeForParent = cursorToRoot.computeHash(bytes);
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == NodeKind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getAttributeKey(i));
          final Node attributeNode =
              pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
          hashCodeForParent = attributeNode.computeHash(bytes) + hashCodeForParent * PRIME;
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getNamespaceKey(i));
          final Node namespaceNode =
              pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
          hashCodeForParent = namespaceNode.computeHash(bytes) + hashCodeForParent * PRIME;
        }
        nodeReadOnlyTrx.moveTo(cursorToRoot.getNodeKey());
      }

      // Caring about the children of a node
      if (nodeReadOnlyTrx.moveTo(getStructuralNode().getFirstChildKey())) {
        do {
          hashCodeForParent = nodeReadOnlyTrx.getHash() + hashCodeForParent * PRIME;
        } while (nodeReadOnlyTrx.moveTo(getStructuralNode().getRightSiblingKey()));
        nodeReadOnlyTrx.moveTo(getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
    } while (nodeReadOnlyTrx.moveTo(cursorToRoot.getParentKey()));

    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  protected abstract StructNode getStructuralNode();

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final long oldHash) {
    final Node newNode = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long newNodeKey = newNode.getNodeKey();
    final long newHash = newNode.computeHash(bytes);
    long resultNew;

    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == newNodeKey) {
        resultNew = newHash;
      } else {
        resultNew = node.getHash() - (oldHash * PRIME);
        resultNew = resultNew + newHash * PRIME;
      }
      node.setHash(resultNew);
    } while (nodeReadOnlyTrx.moveTo(nodeReadOnlyTrx.getParentKey()));

    nodeReadOnlyTrx.moveTo(newNodeKey);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final Node startNode = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    long hashToRemove = startNode.getHash() == 0L ? startNode.computeHash(bytes) : startNode.getHash();
    long hashToAdd = 0;
    long newHash;
    // go the path to the root
    do {
      final Node node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNodeKey) {
        // the hash for the start node is always 0
        newHash = 0L;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // the parent node is just removed
        newHash = node.getHash() - hashToRemove * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode, node);
      } else {
        // the ancestors are all touched regarding the modification
        newHash = node.getHash() - hashToRemove * PRIME;
        newHash = newHash + hashToAdd * PRIME;
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode, node);
      }
      node.setHash(newHash);
      hashToAdd = newHash;
    } while (nodeReadOnlyTrx.moveTo(nodeReadOnlyTrx.getParentKey()));

    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startNode the node which has been removed
   * @param ancestorNode the ancestor node to update (from prepareRecordForModification)
   */
  private void setRemoveDescendants(final ImmutableNode startNode, final Node ancestorNode) {
    assert startNode != null;
    if (startNode instanceof StructNode startNodeAsStructNode && ancestorNode instanceof StructNode structAncestor) {
      structAncestor.setDescendantCount(structAncestor.getDescendantCount() - startNodeAsStructNode.getDescendantCount() - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() {
    // start with hash to add
    final Node startNode = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    final long oldDescendantCount = getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;
    bytes.clear();
    long hashToAdd;
    long newHash;
    long possibleOldHash = 0L;

    if (isValueNode(startNode.getKind())) {
      hashToAdd = startNode.computeHash(bytes);
      // Set hash on value node so it can be serialized with metadata
      final Node valueNode = pageTrx.prepareRecordForModification(startNode.getNodeKey(), IndexType.DOCUMENT, -1);
      valueNode.setHash(hashToAdd);
      nodeReadOnlyTrx.moveTo(startNode.getParentKey());
    } else {
      if (startNode.getHash() == 0L) {
        hashToAdd = startNode.computeHash(bytes);
        startNode.setHash(hashToAdd);
      } else {
        hashToAdd = startNode.getHash();
      }
    }

    // go the path to the root
    Node node;
    do {
      node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
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
    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  private static boolean isValueNode(final NodeKind kind) {
    return kind == NodeKind.STRING_VALUE || kind == NodeKind.OBJECT_STRING_VALUE
        || kind == NodeKind.BOOLEAN_VALUE || kind == NodeKind.OBJECT_BOOLEAN_VALUE
        || kind == NodeKind.NUMBER_VALUE || kind == NodeKind.OBJECT_NUMBER_VALUE
        || kind == NodeKind.NULL_VALUE || kind == NodeKind.OBJECT_NULL_VALUE
        || kind == NodeKind.ATTRIBUTE || kind == NodeKind.TEXT || kind == NodeKind.COMMENT
        || kind == NodeKind.PROCESSING_INSTRUCTION;
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
            pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
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
   * Called during postorder traversal to compute hashes from leaves to root.
   */
  public void addHashAndDescendantCount() {
    switch (hashType) {
      case ROLLING -> {
        // Setup.
        final Node startNode = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        final long startNodeKey = startNode.getNodeKey();
        final long oldDescendantCount = getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;

        // Set start node's hash.
        // If hash is already set (from child processing), use it as-is.
        // The parent's computeHash() was already added when the first child was processed.
        // If hash is 0 (leaf node), compute it now.
        long hashToAdd = startNode.getHash() == 0L
            ? startNode.computeHash(bytes)
            : startNode.getHash();  // Already includes own data hash from child processing
        Node node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setHash(hashToAdd);

        // Set parent node's hash.
        if (startNode.hasParent()) {
          nodeReadOnlyTrx.moveTo(startNode.getParentKey());
          node = pageTrx.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
          final var currentNodeHash = node.getHash();
          // If parent's hash is 0, initialize with its own data hash.
          // Otherwise, use existing hash (which already includes parent's data hash).
          long hash = currentNodeHash == 0L ? node.computeHash(bytes) : currentNodeHash;
          node.setHash(hash + hashToAdd * PRIME);

          setAddDescendants(startNode, node, descendantCount);
        }
        nodeReadOnlyTrx.moveTo(startNodeKey);
      }
      case POSTORDER -> postorderAdd();
      case NONE -> {
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
