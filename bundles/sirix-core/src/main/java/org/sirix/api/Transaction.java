package org.sirix.api;

import org.sirix.api.xdm.XdmNodeWriteTrx;

public interface Transaction extends AutoCloseable {
  Transaction commit();

  Transaction rollback();

  long getId();

  @Override
  void close();

  Transaction add(XdmNodeWriteTrx nodeWriter);
}
