package org.sirix.access.trx.node;

import org.sirix.api.NodeTrx;

/**
 * TODO: Class InternalNodeTrx's description.
 *
 * @author Joao Sousa
 */
public interface InternalNodeTrx<N extends NodeTrx> extends NodeTrx {

    N setBulkInsertion(boolean bulkInsertion);

    void adaptHashesInPostorderTraversal();
}
