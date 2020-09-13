package org.sirix.access.trx.node.json;

import org.sirix.api.json.JsonNodeTrx;

public interface InternalJsonNodeTrx extends JsonNodeTrx {
  JsonNodeTrx setBulkInsertion(boolean bulkInsertion);

  void adaptHashesInPostorderTraversal();

  JsonNodeTrx doCommit(String commitMessage);
}
