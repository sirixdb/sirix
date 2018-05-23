package org.sirix.access.conf;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.List;
import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;
import org.sirix.api.XdmNodeWriteTrx;

public final class TransactionImpl implements Transaction {

  private final List<XdmNodeWriteTrx> resourceTrxs;
  private final TransactionManager trxMgr;

  public TransactionImpl(TransactionManager trxMgr) {
    resourceTrxs = new ArrayList<>();
    this.trxMgr = checkNotNull(trxMgr);
  }

  @Override
  public Transaction commit() {
    int i = 0;
    for (boolean failure = false; i < resourceTrxs.size() && !failure; i++) {
      final XdmNodeWriteTrx trx = resourceTrxs.get(i);

      try {
        trx.commit();
      } catch (final Exception e) {
        trx.rollback();
        failure = true;
      }
    }

    if (i < resourceTrxs.size()) {
      for (int j = 0; j < i; j++) {
        final XdmNodeWriteTrx trx = resourceTrxs.get(i);
        trx.truncateTo(trx.getRevisionNumber() - 1);
      }

      for (; i < resourceTrxs.size(); i++) {
        final XdmNodeWriteTrx trx = resourceTrxs.get(i);
        trx.rollback();
      }
    }

    resourceTrxs.clear();
    trxMgr.closeTransaction(this);
    return this;
  }

  @Override
  public Transaction rollback() {
    resourceTrxs.forEach(XdmNodeWriteTrx::rollback);
    resourceTrxs.clear();
    trxMgr.closeTransaction(this);
    return this;
  }

  @Override
  public void close() {
    rollback();
  }

  @Override
  public Transaction add(XdmNodeWriteTrx writer) {
    resourceTrxs.add(checkNotNull(writer));
    return this;
  }

  @Override
  public long getId() {
    return 0;
  }
}
