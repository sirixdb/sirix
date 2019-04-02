package org.sirix.api;

import org.sirix.api.xdm.XdmNodeTrx;

public interface Transaction extends AutoCloseable {
  Transaction commit();

  Transaction rollback();

  long getId();

  @Override
  void close();

  Transaction add(XdmNodeTrx nodeWriter);
}
