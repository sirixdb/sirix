package org.sirix.access.trx;

import org.sirix.api.Transaction;
import org.sirix.api.TransactionManager;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public final class TransactionManagerImpl implements TransactionManager {

  private final Set<Transaction> transactions;

  @Inject
  public TransactionManagerImpl() {
    transactions = new HashSet<>();
  }

  @Override
  public Transaction beginTransaction() {
    final Transaction trx = new TransactionImpl(this);
    transactions.add(trx);
    return trx;
  }

  @Override
  public TransactionManager closeTransaction(final Transaction trx) {
    transactions.remove(checkNotNull(trx));
    return this;
  }

  @Override
  public void close() {
    transactions.forEach(Transaction::commit);
  }

}
