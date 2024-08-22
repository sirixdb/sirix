package io.sirix.access.trx.node;

import io.sirix.api.NodeTrx;

/**
 * API-inaccessible methods for {@link NodeTrx}.
 *
 * @author Joao Sousa
 */
public interface InternalNodeTrx<N extends NodeTrx> extends NodeTrx {

	N setBulkInsertion(boolean bulkInsertion);

	void adaptHashesInPostorderTraversal();
}
