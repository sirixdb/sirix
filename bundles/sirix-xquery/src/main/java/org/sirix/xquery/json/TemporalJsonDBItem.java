package org.sirix.xquery.json;

import org.brackit.xquery.jdm.json.TemporalJsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.xquery.StructuredDBItem;

public interface TemporalJsonDBItem<E extends TemporalJsonDBItem<E>>
    extends TemporalJsonItem<E>, StructuredDBItem<JsonNodeReadOnlyTrx> {
}
