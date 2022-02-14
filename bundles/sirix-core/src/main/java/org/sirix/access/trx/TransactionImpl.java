package org.sirix.access.trx;

import org.sirix.api.NodeTrx;
import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TransactionImpl implements Transaction {

  private final List<NodeTrx> resourceTrxs;
  private final TransactionManager trxMgr;

  public TransactionImpl(TransactionManager trxMgr) {
    this.resourceTrxs = new ArrayList<>();
    this.trxMgr = checkNotNull(trxMgr);
  }

  @Override
  public Transaction commit() {
    int i = 0;
    for (boolean failure = false; i < resourceTrxs.size() && !failure; i++) {
      final NodeTrx trx = resourceTrxs.get(i);

      try {
        trx.commit();
      } catch (final Exception e) {
        trx.rollback();
        failure = true;
      }
    }

    if (i < resourceTrxs.size()) {
      for (int j = 0; j < i; j++) {
        final NodeTrx trx = resourceTrxs.get(i);
        trx.truncateTo(trx.getRevisionNumber() - 1);
      }

      for (; i < resourceTrxs.size(); i++) {
        final NodeTrx trx = resourceTrxs.get(i);
        trx.rollback();
      }
    }

    resourceTrxs.clear();
    trxMgr.closeTransaction(this);
    return this;
  }

  @Override
  public Transaction rollback() {
    resourceTrxs.forEach(NodeTrx::rollback);
    resourceTrxs.clear();
    trxMgr.closeTransaction(this);
    return this;
  }

  @Override
  public void close() {
    rollback();
  }

  @Override
  public Transaction add(NodeTrx writer) {
    resourceTrxs.add(checkNotNull(writer));
    return this;
  }

  @Override
  public long getId() {
    return 0;
  }
}
