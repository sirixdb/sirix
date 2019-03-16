package org.sirix.xquery.json;

import org.brackit.xquery.xdm.json.JsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;

public interface JsonDBItem extends JsonItem {
  JsonNodeReadOnlyTrx getTrx();
}
