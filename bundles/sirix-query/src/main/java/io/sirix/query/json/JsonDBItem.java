package io.sirix.query.json;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.UpdatableJsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;

public interface JsonDBItem extends UpdatableJsonItem {
  JsonResourceSession getResourceSession();

  JsonNodeReadOnlyTrx getTrx();

  long getNodeKey();

  JsonDBCollection getCollection();

  /**
   * Default implementation of replaceValue that navigates to parent and performs replacement.
   * This enables the use of sdb:select-item with replace expressions.
   */
  @Override
  default void replaceValue(Sequence newValue) {
    final JsonNodeReadOnlyTrx rtx = getTrx();
    rtx.moveTo(getNodeKey());
    
    final JsonResourceSession resourceSession = getResourceSession();
    final JsonNodeTrx wtx = resourceSession.getNodeTrx().orElseGet(resourceSession::beginNodeTrx);
    wtx.moveTo(getNodeKey());
    
    // Use the JsonItemSequence utility to perform the replacement
    JsonItemSequence.replaceValue(wtx, newValue, getCollection());
  }
}
