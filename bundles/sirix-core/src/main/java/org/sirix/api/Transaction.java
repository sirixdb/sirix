package org.sirix.api;

import org.sirix.api.xml.XmlNodeTrx;

public interface Transaction extends AutoCloseable {
  Transaction commit();

  Transaction rollback();

  long getId();

  @Override
  void close();

  Transaction add(XmlNodeTrx nodeWriter);
}
