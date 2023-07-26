package org.sirix.query.json;

import org.brackit.xquery.jdm.json.TemporalJsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.query.StructuredDBItem;

public interface TemporalJsonDBItem<E extends TemporalJsonDBItem<E>>
    extends TemporalJsonItem<E>, StructuredDBItem<JsonNodeReadOnlyTrx> {
}
