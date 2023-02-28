package org.sirix.xquery.json;

import org.brackit.xquery.jdm.json.JsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceSession;

public interface JsonDBItem extends JsonItem {
  JsonResourceSession getResourceSession();

  JsonNodeReadOnlyTrx getTrx();

  long getNodeKey();

  JsonDBCollection getCollection();
}
