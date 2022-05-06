package org.sirix.access.trx.node;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.SirixDeweyID;

public abstract class AbstractDeweyIDManager<W extends NodeTrx & NodeCursor> {

  /** The node transaction. */
  private final W nodeTrx;

  /**
   * Constructor.
   * @param nodeTrx the node transaction
   */
  public AbstractDeweyIDManager(W nodeTrx) {
    this.nodeTrx = nodeTrx;
  }

  /**
   * Get an optional first child {@link SirixDeweyID} reference.
   *
   * @return optional first child {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  public SirixDeweyID newFirstChildID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      if (nodeTrx.hasFirstChild()) {
        nodeTrx.moveToFirstChild();
        id = SirixDeweyID.newBetween(null, nodeTrx.getDeweyID());
      } else {
        id = nodeTrx.getDeweyID().getNewChildID();
      }
    }
    return id;
  }

  /**
   * Get an optional last child {@link SirixDeweyID} reference.
   *
   * @return optional last child {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  public SirixDeweyID newLastChildID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      if (nodeTrx.hasLastChild()) {
        nodeTrx.moveToLastChild();
        id = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), null);
      } else {
        id = nodeTrx.getDeweyID().getNewChildID();
      }
    }
    return id;
  }

  /**
   * Get an optional left sibling {@link SirixDeweyID} reference.
   *
   * @return optional left sibling {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  public SirixDeweyID newLeftSiblingID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      final SirixDeweyID currID = nodeTrx.getDeweyID();
      if (nodeTrx.hasLeftSibling()) {
        nodeTrx.moveToLeftSibling();
        id = SirixDeweyID.newBetween(nodeTrx.getDeweyID(), currID);
        nodeTrx.moveToRightSibling();
      } else {
        id = SirixDeweyID.newBetween(null, currID);
      }
    }
    return id;
  }

  /**
   * Get an optional right sibling {@link SirixDeweyID} reference.
   *
   * @return optional right sibling {@link SirixDeweyID} reference
   * @throws SirixException if generating an ID fails
   */
  public SirixDeweyID newRightSiblingID() {
    SirixDeweyID id = null;
    if (nodeTrx.storeDeweyIDs()) {
      final SirixDeweyID currID = nodeTrx.getDeweyID();
      if (nodeTrx.hasRightSibling()) {
        nodeTrx.moveToRightSibling();
        id = SirixDeweyID.newBetween(currID, nodeTrx.getDeweyID());
        nodeTrx.moveToLeftSibling();
      } else {
        id = SirixDeweyID.newBetween(currID, null);
      }
    }
    return id;
  }
}
