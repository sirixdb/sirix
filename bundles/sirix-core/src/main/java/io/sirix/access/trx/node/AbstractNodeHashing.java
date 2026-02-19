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
  private final StorageEngineWriter storageEngineWriter;

  /**
   * {@code true} if bulk inserting is enabled, {@code false} otherwise
   */
  private boolean bulkInsert;

  private boolean autoCommit;

  private final BytesOut<?> bytes = Bytes.elasticOffHeapByteBuffer();

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param storageEngineWriter the page trx
   */
  protected AbstractNodeHashing(final ResourceConfiguration resourceConfig, final T nodeReadOnlyTrx,
      final StorageEngineWriter storageEngineWriter) {
    this.hashType = resourceConfig.hashType;
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    this.storageEngineWriter = storageEngineWriter;
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
    final Node startNode = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    // long for adapting the hash of the parent
    long hashCodeForParent;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      node.setHash(node.computeHash(bytes));
      persistNode(node);
      nodeReadOnlyTrx.moveTo(nodeReadOnlyTrx.getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
      hashCodeForParent = cursorToRoot.computeHash(bytes);
      // Caring about attributes and namespaces if node is an element.
      if (cursorToRoot.getKind() == NodeKind.ELEMENT) {
        final ElementNode currentElement = (ElementNode) cursorToRoot;
        // setting the attributes and namespaces
        final int attCount = ((ElementNode) cursorToRoot).getAttributeCount();
        for (int i = 0; i < attCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getAttributeKey(i));
          final Node attributeNode =
              storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
          hashCodeForParent = attributeNode.computeHash(bytes) + hashCodeForParent * PRIME;
        }
        final int nspCount = ((ElementNode) cursorToRoot).getNamespaceCount();
        for (int i = 0; i < nspCount; i++) {
          nodeReadOnlyTrx.moveTo(currentElement.getNamespaceKey(i));
          final Node namespaceNode =
              storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
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
      persistNode(cursorToRoot);
    } while (nodeReadOnlyTrx.moveTo(cursorToRoot.getParentKey()));

    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  protected abstract StructNode getStructuralNode();

  private void persistNode(final Node node) {
    storageEngineWriter.updateRecordSlot(node, IndexType.DOCUMENT, -1);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final long oldHash) {
    long currentKey = nodeReadOnlyTrx.getNodeKey();
    final Node newNode = storageEngineWriter.prepareRecordForModification(currentKey, IndexType.DOCUMENT, -1);
    final long newNodeKey = newNode.getNodeKey();
    final long newHash = newNode.computeHash(bytes);
    long resultNew;

    // go the path to the root — track position via local key to avoid moveTo allocations
    do {
      final Node node = storageEngineWriter.prepareRecordForModification(currentKey, IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == newNodeKey) {
        resultNew = newHash;
      } else {
        resultNew = node.getHash() - (oldHash * PRIME);
        resultNew = resultNew + newHash * PRIME;
      }
      node.setHash(resultNew);
      persistNode(node);
      currentKey = node.getParentKey();
    } while (currentKey >= 0);

    nodeReadOnlyTrx.moveTo(newNodeKey);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final Node startNode = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    // Capture all needed values from startNode before any subsequent prepareRecordForModification
    // calls, which may return the same write-path singleton and overwrite startNode's fields.
    final long startParentKey = startNode.getParentKey();
    final boolean startNodeIsStruct = startNode instanceof StructNode;
    final long startDescendantCount = startNodeIsStruct
        ? ((StructNode) startNode).getDescendantCount()
        : 0;
    long hashToRemove = startNode.getHash() == 0L
        ? startNode.computeHash(bytes)
        : startNode.getHash();
    long hashToAdd = 0;
    long newHash;
    // go the path to the root — track position via local key to avoid moveTo allocations
    long currentKey = nodeReadOnlyTrx.getNodeKey();
    do {
      final Node node = storageEngineWriter.prepareRecordForModification(currentKey, IndexType.DOCUMENT, -1);
      if (node.getNodeKey() == startNodeKey) {
        // the hash for the start node is always 0
        newHash = 0L;
      } else if (node.getNodeKey() == startParentKey) {
        // the parent node is just removed
        newHash = node.getHash() - hashToRemove * PRIME;
        hashToRemove = node.getHash();
        if (startNodeIsStruct) {
          setRemoveDescendants(startDescendantCount, node);
        }
      } else {
        // the ancestors are all touched regarding the modification
        newHash = node.getHash() - hashToRemove * PRIME;
        newHash = newHash + hashToAdd * PRIME;
        hashToRemove = node.getHash();
        if (startNodeIsStruct) {
          setRemoveDescendants(startDescendantCount, node);
        }
      }
      node.setHash(newHash);
      persistNode(node);
      hashToAdd = newHash;
      currentKey = node.getParentKey();
    } while (currentKey >= 0);

    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startDescendantCount the descendant count of the removed node (pre-captured as local)
   * @param ancestorNode the ancestor node to update (from prepareRecordForModification)
   */
  private static void setRemoveDescendants(final long startDescendantCount, final Node ancestorNode) {
    if (ancestorNode instanceof StructNode structAncestor) {
      structAncestor.setDescendantCount(structAncestor.getDescendantCount() - startDescendantCount - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() {
    // start with hash to add
    final Node startNode = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
    final long startNodeKey = startNode.getNodeKey();
    // Capture all needed values from startNode before any subsequent prepareRecordForModification
    // calls, which may return the same write-path singleton and overwrite startNode's fields.
    final long startParentKey = startNode.getParentKey();
    final NodeKind startNodeKind = startNode.getKind();
    final boolean startIsStruct = startNode instanceof StructNode;
    final long oldDescendantCount = getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0
        ? 1
        : oldDescendantCount + 1;
    bytes.clear();
    long hashToAdd;
    long newHash;
    long possibleOldHash = 0L;

    long currentKey;
    if (isValueNode(startNodeKind)) {
      hashToAdd = startNode.computeHash(bytes);
      // Set hash on value node so it can be serialized with metadata
      final Node valueNode = storageEngineWriter.prepareRecordForModification(startNodeKey, IndexType.DOCUMENT, -1);
      valueNode.setHash(hashToAdd);
      persistNode(valueNode);
      currentKey = startParentKey;
    } else {
      if (startNode.getHash() == 0L) {
        hashToAdd = startNode.computeHash(bytes);
        startNode.setHash(hashToAdd);
        persistNode(startNode);
      } else {
        hashToAdd = startNode.getHash();
      }
      currentKey = startNodeKey;
    }

    // go the path to the root — track position via local key to avoid moveTo allocations
    Node node;
    long nodeParentKey;
    do {
      node = storageEngineWriter.prepareRecordForModification(currentKey, IndexType.DOCUMENT, -1);
      nodeParentKey = node.getParentKey();
      if (node.getNodeKey() == startNodeKey) {
        // first, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startParentKey) {
        // at the parent level, just add the node
        final long newMultipliedHash = hashToAdd * PRIME;
        possibleOldHash = node.getHash();
        newHash = possibleOldHash + newMultipliedHash;
        hashToAdd = newHash;
        setAddDescendants(startIsStruct, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element and add the new one
        final long oldMultipliedHash = possibleOldHash * PRIME;
        final long newMultipliedHash = hashToAdd * PRIME;
        newHash = node.getHash() - oldMultipliedHash + newMultipliedHash;
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startIsStruct, node, descendantCount);
      }
      node.setHash(newHash);
      persistNode(node);
      currentKey = nodeParentKey;
    } while (currentKey >= 0);
    nodeReadOnlyTrx.moveTo(startNodeKey);
  }

  private static boolean isValueNode(final NodeKind kind) {
    return kind == NodeKind.STRING_VALUE || kind == NodeKind.OBJECT_STRING_VALUE || kind == NodeKind.BOOLEAN_VALUE
        || kind == NodeKind.OBJECT_BOOLEAN_VALUE || kind == NodeKind.NUMBER_VALUE
        || kind == NodeKind.OBJECT_NUMBER_VALUE || kind == NodeKind.NULL_VALUE || kind == NodeKind.OBJECT_NULL_VALUE
        || kind == NodeKind.ATTRIBUTE || kind == NodeKind.TEXT || kind == NodeKind.COMMENT
        || kind == NodeKind.PROCESSING_INSTRUCTION;
  }

  /**
   * Add a hash and descendant count from a pre-captured start node to the current parent. Values are
   * pre-captured to avoid singleton aliasing — the start node's singleton may be overwritten by
   * prepareRecordForModification for the parent.
   *
   * @param hashToAdd pre-computed hash of the start node
   * @param startIsStruct whether the start node is a StructNode
   * @param startDescendantCount descendant count of the start node (only used if startIsStruct)
   */
  public void addParentHash(final long hashToAdd, final boolean startIsStruct, final long startDescendantCount) {
    switch (hashType) {
      case ROLLING:
        final Node parentNode =
            storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        final long hash = parentNode.getHash();
        parentNode.setHash(hash + hashToAdd * PRIME);
        if (startIsStruct && parentNode instanceof StructNode parentStruct) {
          parentStruct.setDescendantCount(parentStruct.getDescendantCount() + startDescendantCount + 1);
        }
        persistNode(parentNode);
        break;
      case POSTORDER:
        break;
      case NONE:
      default:
    }
  }

  /**
   * Add a hash and the descendant count. Called during postorder traversal to compute hashes from
   * leaves to root.
   */
  public void addHashAndDescendantCount() {
    switch (hashType) {
      case ROLLING -> {
        // Setup.
        final Node startNode =
            storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        final long startNodeKey = startNode.getNodeKey();
        // Capture all needed values from startNode before any subsequent prepareRecordForModification
        // calls, which may return the same write-path singleton and overwrite startNode's fields.
        final boolean startHasParent = startNode.hasParent();
        final long startParentKey = startNode.getParentKey();
        final boolean startIsStruct = startNode instanceof StructNode;
        final long oldDescendantCount = getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0
            ? 1
            : oldDescendantCount + 1;

        // Set start node's hash.
        // If hash is already set (from child processing), use it as-is.
        // The parent's computeHash() was already added when the first child was processed.
        // If hash is 0 (leaf node), compute it now.
        long hashToAdd = startNode.getHash() == 0L
            ? startNode.computeHash(bytes)
            : startNode.getHash(); // Already includes own data hash from child processing
        Node node = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setHash(hashToAdd);
        persistNode(node);

        // Set parent node's hash.
        if (startHasParent) {
          nodeReadOnlyTrx.moveTo(startParentKey);
          node = storageEngineWriter.prepareRecordForModification(nodeReadOnlyTrx.getNodeKey(), IndexType.DOCUMENT, -1);
          final long currentNodeHash = node.getHash();
          // If parent's hash is 0, initialize with its own data hash.
          // Otherwise, use existing hash (which already includes parent's data hash).
          long hash = currentNodeHash == 0L
              ? node.computeHash(bytes)
              : currentNodeHash;
          node.setHash(hash + hashToAdd * PRIME);

          setAddDescendants(startIsStruct, node, descendantCount);
          persistNode(node);
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
   * @param startIsStruct whether the start node is a StructNode (pre-captured as local)
   * @param nodeToModify node to modify
   * @param descendantCount the descendantCount to add
   */
  private static void setAddDescendants(final boolean startIsStruct, final Node nodeToModify,
      final @NonNegative long descendantCount) {
    assert nodeToModify != null;
    if (startIsStruct && nodeToModify instanceof StructNode node) {
      final long oldDescendantCount = node.getDescendantCount();
      node.setDescendantCount(oldDescendantCount + descendantCount);
    }
  }

  /**
   * Returns the shared bytes buffer used for hash computation. Exposed for callers that need to
   * pre-compute hashes before traversal.
   */
  public BytesOut<?> getBytes() {
    return bytes;
  }

  public boolean isBulkInsert() {
    return bulkInsert;
  }

}
