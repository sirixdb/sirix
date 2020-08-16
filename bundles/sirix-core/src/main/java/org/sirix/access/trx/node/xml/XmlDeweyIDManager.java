package org.sirix.access.trx.node.xml;

import org.sirix.access.trx.node.AbstractDeweyIDManager;
import org.sirix.api.PageTrx;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.xml.ElementNode;
import org.sirix.page.PageKind;

final class XmlDeweyIDManager extends AbstractDeweyIDManager {
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

    final StructNode root = nodeTrx.getPageWtx()
                                   .prepareRecordForModification(nodeKey, PageKind.RECORDPAGE, -1);
    root.setDeweyID(id);

    adaptNonStructuralNodes(root);

    if (root.hasFirstChild()) {
      nodeTrx.moveTo(root.getFirstChildKey());

      int attributeNr = 0;
      int nspNr = 0;
      var previousNodeKey = nodeTrx.getNodeKey();

      for (@SuppressWarnings("unused") final long key : LevelOrderAxis.newBuilder(nodeTrx)
                                                                      .includeNonStructuralNodes()
                                                                      .includeSelf()
                                                                      .build()) {
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

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), PageKind.RECORDPAGE, -1);
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

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), PageKind.RECORDPAGE, -1);
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

        final Node node = pageTrx.prepareRecordForModification(nodeTrx.getNodeKey(), PageKind.RECORDPAGE, -1);
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