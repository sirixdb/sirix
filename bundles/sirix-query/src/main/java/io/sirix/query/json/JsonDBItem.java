package io.sirix.query.json;

import io.brackit.query.jdm.json.JsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;

public interface JsonDBItem extends JsonItem {
  JsonResourceSession getResourceSession();

  JsonNodeReadOnlyTrx getTrx();

  long getNodeKey();

  JsonDBCollection getCollection();
}
