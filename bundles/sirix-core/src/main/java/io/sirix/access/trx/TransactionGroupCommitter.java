package io.sirix.access.trx;

import io.sirix.api.NodeTrx;

import java.util.Set;

public final class TransactionGroupCommitter {

  private final Set<NodeTrx> transactions;

  public TransactionGroupCommitter(Set<NodeTrx> transactions) {
    this.transactions = transactions;
  }


}
