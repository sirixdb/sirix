package io.sirix.access.trx.node.xml;

import io.sirix.access.trx.node.AbstractDeweyIDManager;
import io.sirix.api.PageTrx;
import io.sirix.axis.LevelOrderAxis;
import io.sirix.exception.SirixException;
import io.sirix.index.IndexType;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.xml.ElementNode;

final class XmlDeweyIDManager extends AbstractDeweyIDManager<InternalXmlNodeTrx> {
  private final InternalXmlNodeTrx nodeTrx;

  private final PageTrx pageTrx;

  public XmlDeweyIDManager(InternalXmlNodeTrx nodeTrx) {
    super(nodeTrx);
    this.nodeTrx = nodeTrx;
    this.pageTrx = nodeTrx.getPageWtx();
  }

  /**
   * Compute the new DeweyIDs.
   *
   * @throws SirixException if anything went wrong
   */
  public void computeNewDeweyIDs() {
    SirixDeweyID id;
    if (nodeTrx.hasLeftSibling() && nodeTrx.hasRightSibling()) {
      id = SirixDeweyID.newBetween(nodeTrx.getLeftSiblingDeweyID(), nodeTrx.getRightSiblingDeweyID());
    } else if (nodeTrx.hasLeftSibling()) {
      id = SirixDeweyID.newBetween(nodeTrx.getLeftSiblingDeweyID(), null);
    } else if (nodeTrx.hasRightSibling()) {
      id = SirixDeweyID.newBetween(null, nodeTrx.getRightSiblingDeweyID());
    } else {
      id = nodeTrx.getParentDeweyID().getNewChildID();
    }

    final long nodeKey = nodeTrx.getNodeKey();

    final StructNode root =
        nodeTrx.getPageWtx().prepareRecordForModification(nodeKey, IndexType.DOCUMENT, -1);
    root.setDeweyID(id);

    adaptNonStructuralNodes(root);

    if (root.hasFirstChild()) {
      nodeTrx.moveTo(root.getFirstChildKey());

      int attributeNr = 0;
      int nspNr = 0;
      var previousNodeKey = nodeTrx.getNodeKey();

      for (final long ignored : LevelOrderAxis.newBuilder(nodeTrx).includeNonStructuralNodes().includeSelf().build()) {
        SirixDeweyID deweyID;
        if (nodeTrx.isAttribute()) {
          final long attNodeKey = nodeTrx.getNodeKey();
          if (attributeNr == 0) {
            deweyID = nodeTrx.getParentDeweyID().getNewAttributeID();
          } else {
            nodeTrx.moveTo(attributeNr - 1);
            deweyID = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
          }
          nodeTrx.moveTo(attNodeKey);
          attributeNr++;
        } else if (nodeTrx.isNamespace()) {
          final long nspNodeKey = nodeTrx.getNodeKey();
          if (nspNr == 0) {
            deweyID = nodeTrx.getParentDeweyID().getNewNamespaceID();
          } else {
            nodeTrx.moveTo(attributeNr - 1);
            deweyID = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
          }
          nodeTrx.moveTo(nspNodeKey);
          nspNr++;
        } else {
          attributeNr = 0;
          nspNr = 0;
          if (previousNodeKey == nodeTrx.getLeftSiblingKey()) {
            deweyID = SirixDeweyID.newBetween(nodeTrx.getLeftSiblingDeweyID(), null);
          } else {
            deweyID = nodeTrx.getParentDeweyID().getNewChildID();
          }
        }

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setDeweyID(deweyID);

        previousNodeKey = node.getNodeKey();
      }

      nodeTrx.moveTo(nodeKey);
    }
  }

  public void adaptNonStructuralNodes(DataRecord root) {
    if (nodeTrx.isElement()) {
      final ElementNode element = (ElementNode) root;
      for (int i = 0, attLength = element.getAttributeCount(); i < attLength; i++) {
        SirixDeweyID deweyID;
        if (i == 0) {
          nodeTrx.moveToAttribute(i);
          deweyID = nodeTrx.getParentDeweyID().getNewAttributeID();
        } else {
          nodeTrx.moveToAttribute(i - 1);
          deweyID = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
        }
        nodeTrx.moveToParent();
        nodeTrx.moveToAttribute(i);

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setDeweyID(deweyID);

        nodeTrx.moveToParent();
      }

      for (int i = 0, nspLength = element.getNamespaceCount(); i < nspLength; i++) {
        SirixDeweyID deweyID;
        if (i == 0) {
          nodeTrx.moveToNamespace(i);
          deweyID = nodeTrx.getParentDeweyID().getNewNamespaceID();
        } else {
          nodeTrx.moveToNamespace(i - 1);
          deweyID = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
        }
        nodeTrx.moveToNamespace(i);

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), IndexType.DOCUMENT, -1);
        node.setDeweyID(deweyID);

        nodeTrx.moveToParent();
      }
    }
  }

  /**
   * Get an optional namespace {@link SirixDeweyID} reference.
   *
   * @return optional namespace {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  SirixDeweyID newNamespaceID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      if (nodeTrx.hasNamespaces()) {
        nodeTrx.moveToNamespace(nodeTrx.getNamespaceCount() - 1);
        id = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
        nodeTrx.moveToParent();
      } else {
        id = nodeTrx.getDeweyID().getNewNamespaceID();
      }
    }
    return id;
  }

  /**
   * Get an optional attribute {@link SirixDeweyID} reference.
   *
   * @return optional attribute {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  SirixDeweyID newAttributeID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      if (nodeTrx.hasAttributes()) {
        nodeTrx.moveToAttribute(nodeTrx.getAttributeCount() - 1);
        id = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
        nodeTrx.moveToParent();
      } else {
        id = nodeTrx.getDeweyID().getNewAttributeID();
      }
    }
    return id;
  }

}