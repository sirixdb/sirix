package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.AbstractDeweyIDManager;
import io.sirix.exception.SirixException;
import io.sirix.node.SirixDeweyID;

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
    return nodeTrx.storeDeweyIDs() ? nodeTrx.getDeweyID().getRecordValueRootID() : null;
  }
}
