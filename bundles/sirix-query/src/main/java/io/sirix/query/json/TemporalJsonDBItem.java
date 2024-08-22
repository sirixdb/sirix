package io.sirix.query.json;

import io.brackit.query.jdm.json.TemporalJsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.StructuredDBItem;

public interface TemporalJsonDBItem<E extends TemporalJsonDBItem<E>>
		extends
			TemporalJsonItem<E>,
			StructuredDBItem<JsonNodeReadOnlyTrx> {
}
