package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.AbstractDeweyIDManager;
import io.sirix.api.StorageEngineWriter;
import io.sirix.axis.LevelOrderAxis;
import io.sirix.exception.SirixException;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;

final class JsonDeweyIDManager extends AbstractDeweyIDManager<InternalJsonNodeTrx> {

  private final InternalJsonNodeTrx nodeTrx;

  JsonDeweyIDManager(InternalJsonNodeTrx nodeTrx) {
    super(nodeTrx);
    this.nodeTrx = nodeTrx;
  }

  /**
   * Get a new record value {@link SirixDeweyID} reference.
   *
   * @return record value {@link SirixDeweyID} reference or {@code null}
   * @throws SirixException if generating an ID fails
   */
  SirixDeweyID newRecordValueID() {
    return nodeTrx.storeDeweyIDs()
        ? nodeTrx.getDeweyID().getRecordValueRootID()
        : null;
  }

  /**
   * Compute new DeweyIDs for the moved subtree. JSON-specific: no attributes or namespaces.
   *
   * @throws SirixException if anything went wrong
   */
  void computeNewDeweyIDs() {
    final StorageEngineWriter storageEngineWriter = nodeTrx.getStorageEngineWriter();
    SirixDeweyID id;
    final long nodeKey = nodeTrx.getNodeKey();

    if (nodeTrx.hasLeftSibling() && nodeTrx.hasRightSibling()) {
      final long rightKey = nodeTrx.getRightSiblingKey();
      nodeTrx.moveToLeftSibling();
      final SirixDeweyID leftId = nodeTrx.getDeweyID();
      nodeTrx.moveTo(rightKey);
      final SirixDeweyID rightId = nodeTrx.getDeweyID();
      nodeTrx.moveTo(nodeKey);
      id = SirixDeweyID.newBetween(leftId, rightId);
    } else if (nodeTrx.hasLeftSibling()) {
      nodeTrx.moveToLeftSibling();
      final SirixDeweyID leftId = nodeTrx.getDeweyID();
      nodeTrx.moveTo(nodeKey);
      id = SirixDeweyID.newBetween(leftId, null);
    } else if (nodeTrx.hasRightSibling()) {
      nodeTrx.moveToRightSibling();
      final SirixDeweyID rightId = nodeTrx.getDeweyID();
      nodeTrx.moveTo(nodeKey);
      id = SirixDeweyID.newBetween(null, rightId);
    } else {
      nodeTrx.moveToParent();
      final SirixDeweyID parentId = nodeTrx.getDeweyID();
      nodeTrx.moveTo(nodeKey);
      id = parentId.getNewChildID();
    }

    final StructNode root = storageEngineWriter.prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    root.setDeweyID(id);
    persistUpdatedRecord(storageEngineWriter, root);

    if (root.hasFirstChild()) {
      nodeTrx.moveTo(root.getFirstChildKey());

      long previousNodeKey = nodeTrx.getNodeKey();

      final var axis = LevelOrderAxis.newBuilder(nodeTrx).includeSelf().build();
      while (axis.hasNext()) {
        axis.nextLong();
        final SirixDeweyID deweyID;
        if (previousNodeKey == nodeTrx.getLeftSiblingKey()) {
          nodeTrx.moveToLeftSibling();
          final SirixDeweyID leftId = nodeTrx.getDeweyID();
          nodeTrx.moveToRightSibling();
          deweyID = SirixDeweyID.newBetween(leftId, null);
        } else {
          nodeTrx.moveToParent();
          final SirixDeweyID parentId = nodeTrx.getDeweyID();
          nodeTrx.moveToFirstChild();
          deweyID = parentId.getNewChildID();
        }

        final Node node =
            storageEngineWriter.prepareRecordForModification(nodeTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setDeweyID(deweyID);
        persistUpdatedRecord(storageEngineWriter, node);

        previousNodeKey = node.getNodeKey();
      }

    }
    nodeTrx.moveTo(nodeKey);
  }

  private void persistUpdatedRecord(final StorageEngineWriter storageEngineWriter, final DataRecord record) {
    if (record instanceof FlyweightNode fn && fn.isWriteSingleton() && fn.getOwnerPage() != null) {
      return;
    }
    storageEngineWriter.persistRecord(record, IndexType.DOCUMENT, -1);
  }
}
