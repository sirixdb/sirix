package io.sirix.access.trx.node;

import io.sirix.api.NodeTrx;

/**
 * API-inaccessible methods for {@link NodeTrx}.
 *
 * @author Joao Sousa
 */
public interface InternalNodeTrx<N extends NodeTrx> extends NodeTrx {

  void abort();

  void assertCanClose();

  void runStorageMaintenance(Runnable maintenance);

  N setBulkInsertion(boolean bulkInsertion);

  void adaptHashesInPostorderTraversal();
}
