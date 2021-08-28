package org.sirix.access.trx.node;

import org.sirix.api.NodeTrx;

/**
 * API-inaccessible methods for {@link NodeTrx}.
 *
 * @author Joao Sousa
 */
public interface InternalNodeTrx<N extends NodeTrx> extends NodeTrx {

    N setBulkInsertion(boolean bulkInsertion);

    void adaptHashesInPostorderTraversal();
}
