package org.sirix.index.cas.json;

import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.cas.CASIndex;

public interface JsonCASIndex extends CASIndex<JsonCASIndexBuilder, JsonCASIndexListener, JsonNodeReadOnlyTrx> {
}
