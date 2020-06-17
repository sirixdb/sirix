package org.sirix.access.trx.node;

import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.xml.ElementNode;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;

import javax.annotation.Nonnegative;
import java.math.BigInteger;

public abstract class AbstractNodeHashing {

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
  protected final NodeReadOnlyTrx nodeReadOnlyTrx;

  /**
   * The page write trx.
   */
  private final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx;

  /**
   * {@code true} if bulk inserting is enabled, {@code false} otherwise
   */
  private boolean bulkInsert;

  /**
   * Constructor.
   *
   * @param hashType        the hash type used
   * @param nodeReadOnlyTrx the internal read-only node trx
   * @param pageWriteTrx    the page trx
   */
  public AbstractNodeHashing(final HashType hashType, final NodeReadOnlyTrx nodeReadOnlyTrx,
      final PageTrx<Long, DataRecord, UnorderedKeyValuePage> pageWriteTrx) {
    this.hashType = hashType;
    this.nodeReadOnlyTrx = nodeReadOnlyTrx;
    this.pageWriteTrx = pageWriteTrx;
  }

  public AbstractNodeHashing setBulkInsert(boolean value) {
    this.bulkInsert = value;
    return this;
  }

  /**
   * Adapting the structure with a hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  public void adaptHashesWithAdd() {
    if (!bulkInsert) {
      switch (hashType) {
        case ROLLING:
          rollingAdd();
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
      }
    }
  }

  /**
   * Adapting the structure with a hash for all ancestors only with remove.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  public void adaptHashesWithRemove() {
    if (!bulkInsert) {
      switch (hashType) {
        case ROLLING:
          rollingRemove();
          break;
        case POSTORDER:
          postorderRemove();
          break;
        case NONE:
        default:
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
    if (!bulkInsert) {
      switch (hashType) {
        case ROLLING:
          rollingUpdate(oldHash);
          break;
        case POSTORDER:
          postorderAdd();
          break;
        case NONE:
        default:
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
    final ImmutableNode startNode = getCurrentNode();
    // long for adapting the hash of the parent
    BigInteger hashCodeForParent = BigInteger.ZERO;
    // adapting the parent if the current node is no structural one.
    if (!(startNode instanceof StructNode)) {
      final Node node =
          (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      node.setHash(getCurrentNode().computeHash());
      nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey());
    }
    // Cursor to root
    StructNode cursorToRoot;
    do {
      cursorToRoot =
          (StructNode) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
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
      if (nodeReadOnlyTrx.moveTo(getStructuralNode().getFirstChildKey()).hasMoved()) {
        do {
          hashCodeForParent = getCurrentNode().getHash().add(hashCodeForParent.multiply(PRIME));
        } while (nodeReadOnlyTrx.moveTo(getStructuralNode().getRightSiblingKey()).hasMoved());
        nodeReadOnlyTrx.moveTo(getStructuralNode().getParentKey());
      }

      // setting hash and resetting hash
      cursorToRoot.setHash(hashCodeForParent);
      hashCodeForParent = BigInteger.ZERO;
    } while (nodeReadOnlyTrx.moveTo(cursorToRoot.getParentKey()).hasMoved());

    setCurrentNode(startNode);
  }

  protected abstract StructNode getStructuralNode();

  protected abstract ImmutableNode getCurrentNode();

  protected abstract void setCurrentNode(ImmutableNode node);

  /**
   * Adapting the structure with a rolling hash for all ancestors only with update.
   *
   * @param oldHash pOldHash to be removed
   * @throws SirixIOException if anything weird happened
   */
  private void rollingUpdate(final BigInteger oldHash) {
    final ImmutableNode newNode = getCurrentNode();
    final BigInteger hash = newNode.computeHash();
    BigInteger resultNew;

    // go the path to the root
    do {
      final Node node =
          (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == newNode.getNodeKey()) {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(oldHash));
        resultNew = Node.to128BitsAtMaximumBigInteger(resultNew.add(hash));
      } else {
        resultNew = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(oldHash.multiply(PRIME)));
        resultNew = Node.to128BitsAtMaximumBigInteger(resultNew.add(hash.multiply(PRIME)));
      }
      node.setHash(resultNew);
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()).hasMoved());

    setCurrentNode(newNode);
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with remove.
   */
  private void rollingRemove() {
    final ImmutableNode startNode = getCurrentNode();
    BigInteger hashToRemove = startNode.getHash();
    BigInteger hashToAdd = BigInteger.ZERO;
    BigInteger newHash;
    // go the path to the root
    do {
      final Node node =
          (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
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
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(hashToRemove.multiply(PRIME)));
        newHash = Node.to128BitsAtMaximumBigInteger(newHash.add(hashToAdd.multiply(PRIME)));
        hashToRemove = node.getHash();
        setRemoveDescendants(startNode);
      }
      node.setHash(newHash);
      hashToAdd = newHash;
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()).hasMoved());

    setCurrentNode(startNode);
  }

  /**
   * Set new descendant count of ancestor after a remove-operation.
   *
   * @param startNode the node which has been removed
   */
  private void setRemoveDescendants(final ImmutableNode startNode) {
    assert startNode != null;
    if (startNode instanceof StructNode) {
      final StructNode node = getStructuralNode();
      node.setDescendantCount(node.getDescendantCount() - ((StructNode) startNode).getDescendantCount() - 1);
    }
  }

  /**
   * Adapting the structure with a rolling hash for all ancestors only with insert.
   *
   * @throws SirixIOException if an I/O error occurs
   */
  private void rollingAdd() {
    // start with hash to add
    final ImmutableNode startNode = getCurrentNode();
    final long oldDescendantCount = getStructuralNode().getDescendantCount();
    final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;
    BigInteger hashToAdd = startNode.getHash() == null || BigInteger.ZERO.equals(startNode.getHash())
        ? startNode.computeHash()
        : startNode.getHash();
    BigInteger newHash;
    BigInteger possibleOldHash = BigInteger.ZERO;
    // go the path to the root
    do {
      final Node node =
          (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
      if (node.getNodeKey() == startNode.getNodeKey()) {
        // first, take the hashcode of the node only
        newHash = hashToAdd;
      } else if (node.getNodeKey() == startNode.getParentKey()) {
        // at the parent level, just add the node
        possibleOldHash = node.getHash();
        newHash = Node.to128BitsAtMaximumBigInteger(possibleOldHash.add(hashToAdd.multiply(PRIME)));
        hashToAdd = newHash;
        setAddDescendants(startNode, node, descendantCount);
      } else {
        // at the rest, remove the existing old key for this element
        // and add the new one
        newHash = Node.to128BitsAtMaximumBigInteger(node.getHash().subtract(possibleOldHash.multiply(PRIME)));
        newHash = Node.to128BitsAtMaximumBigInteger(newHash.add(hashToAdd.multiply(PRIME)));
        hashToAdd = newHash;
        possibleOldHash = node.getHash();
        setAddDescendants(startNode, node, descendantCount);
      }
      node.setHash(newHash);
    } while (nodeReadOnlyTrx.moveTo(getCurrentNode().getParentKey()).hasMoved());
    setCurrentNode(startNode);
  }

  /**
   * Add a hash.
   *
   * @param startNode start node
   */
  public void addParentHash(final ImmutableNode startNode) {
    switch (hashType) {
      case ROLLING:
        final BigInteger hashToAdd = startNode.computeHash();
        final Node node =
            (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setHash(node.getHash().add(hashToAdd.multiply(PRIME)));
        if (startNode instanceof StructNode) {
          ((StructNode) node).setDescendantCount(
              ((StructNode) node).getDescendantCount() + ((StructNode) startNode).getDescendantCount() + 1);
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
      case ROLLING:
        // Setup.
        final ImmutableNode startNode = getCurrentNode();
        final long oldDescendantCount = getStructuralNode().getDescendantCount();
        final long descendantCount = oldDescendantCount == 0 ? 1 : oldDescendantCount + 1;

        // Set start node.
        final BigInteger hashToAdd = startNode.computeHash();
        Node node =
            (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
        node.setHash(hashToAdd);

        // Set parent node.
        if (startNode.hasParent()) {
          nodeReadOnlyTrx.moveTo(startNode.getParentKey());
          node =
              (Node) pageWriteTrx.prepareEntryForModification(getCurrentNode().getNodeKey(), PageKind.RECORDPAGE, -1);
          final BigInteger hash =
              node.getHash() == null || BigInteger.ZERO.equals(node.getHash()) ? node.computeHash() : node.getHash();
          node.setHash(hash.add(hashToAdd.multiply(PRIME)));

          setAddDescendants(startNode, node, descendantCount);
        }

        setCurrentNode(startNode);
        break;
      case POSTORDER:
        postorderAdd();
        break;
      case NONE:
      default:
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

  public boolean isBulkInsert() {
    return bulkInsert;
  }
}
