package org.sirix.access.trx.node.json;

import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadTrx;

public interface JSONNodeReadOnlyTrx extends NodeCursor, NodeReadTrx {
  public String getValue();

  public boolean isObject();

  public boolean isObjectKey();

  public boolean isArray();

  public boolean isStringValue();

  public boolean isNumberValue();

  public boolean isNullValue();
}
