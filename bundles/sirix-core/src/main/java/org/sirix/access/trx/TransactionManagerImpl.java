package org.sirix.access.trx;

import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TransactionManagerImpl implements TransactionManager {

  private final Set<Transaction> mTransactions;

  public TransactionManagerImpl() {
    mTransactions = new HashSet<>();
  }

  @Override
  public Transaction beginTransaction() {
    final Transaction trx = new TransactionImpl(this);
    mTransactions.add(trx);
    return trx;
  }

  @Override
  public TransactionManager closeTransaction(final Transaction trx) {
    mTransactions.remove(checkNotNull(trx));
    return this;
  }

  @Override
  public void close() {
    mTransactions.forEach(Transaction::commit);
  }

}
