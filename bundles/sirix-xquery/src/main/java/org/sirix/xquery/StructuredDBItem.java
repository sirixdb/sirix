package org.sirix.xquery;

import org.brackit.xquery.xdm.StructuredItem;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;

public interface StructuredDBItem<R extends NodeReadOnlyTrx & NodeCursor> extends StructuredItem {
  R getTrx();

  long getNodeKey();
}
