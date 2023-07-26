package io.sirix.index.cas.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.cas.CASIndex;

public interface JsonCASIndex extends CASIndex<JsonCASIndexBuilder, JsonCASIndexListener, JsonNodeReadOnlyTrx> {
}
