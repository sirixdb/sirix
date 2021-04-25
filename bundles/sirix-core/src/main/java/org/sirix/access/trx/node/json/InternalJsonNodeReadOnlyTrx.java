package org.sirix.access.trx.node.json;

import org.sirix.access.trx.node.InternalNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.node.interfaces.immutable.ImmutableNode;

public interface InternalJsonNodeReadOnlyTrx extends InternalNodeReadOnlyTrx<ImmutableNode>, JsonNodeReadOnlyTrx {

}
