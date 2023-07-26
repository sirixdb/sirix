package io.sirix.api;

public interface Transaction extends AutoCloseable {
  Transaction commit();

  Transaction rollback();

  long getId();

  @Override
  void close();

  Transaction add(NodeTrx nodeTrx);
}
