package io.sirix.query;

import org.brackit.xquery.jdm.StructuredItem;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;

public interface StructuredDBItem<R extends NodeReadOnlyTrx & NodeCursor> extends StructuredItem {
  R getTrx();

  long getNodeKey();
}
