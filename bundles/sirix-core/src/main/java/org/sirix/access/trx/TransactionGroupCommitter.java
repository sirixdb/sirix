package org.sirix.access.trx;

import org.sirix.api.NodeTrx;

import java.util.Set;

public final class TransactionGroupCommitter {

  private final Set<NodeTrx> transactions;

  public TransactionGroupCommitter(Set<NodeTrx> transactions) {
    this.transactions = transactions;
  }


}
