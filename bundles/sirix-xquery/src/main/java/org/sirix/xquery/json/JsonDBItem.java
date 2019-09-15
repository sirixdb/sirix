package org.sirix.xquery.json;

import org.brackit.xquery.xdm.json.JsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;

public interface JsonDBItem extends JsonItem {
  JsonResourceManager getResourceManager();

  JsonNodeReadOnlyTrx getTrx();

  JsonDBCollection getCollection();
}
