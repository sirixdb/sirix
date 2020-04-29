package org.sirix.access.trx.node.json;

import org.sirix.access.trx.node.AbstractDeweyIDManager;
import org.sirix.exception.SirixException;
import org.sirix.node.SirixDeweyID;

final class JsonDeweyIDManager extends AbstractDeweyIDManager {

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
    return nodeTrx.storeDeweyIDs() ? nodeTrx.getDeweyID().getRecordValueRootID() : null;
  }
}
