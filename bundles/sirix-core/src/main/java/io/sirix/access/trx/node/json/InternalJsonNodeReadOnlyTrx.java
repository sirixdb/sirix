package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalJsonNodeReadOnlyTrx extends InternalNodeReadOnlyTrx<ImmutableNode>, JsonNodeReadOnlyTrx {

}
