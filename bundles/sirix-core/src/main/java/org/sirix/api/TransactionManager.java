package org.sirix.api;

public interface TransactionManager extends AutoCloseable {
  Transaction beginTransaction();

  TransactionManager closeTransaction(Transaction trx);

  @Override
  void close();
}
