package io.sirix.query.json;

import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.UpdatableJsonItem;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
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
    final JsonResourceSession resourceSession = getResourceSession();
    final JsonNodeTrx wtx = resourceSession.getNodeTrx().orElseGet(resourceSession::beginNodeTrx);
    wtx.moveTo(getNodeKey());
    JsonItemSequence.replaceValue(wtx, newValue, getCollection());
  }

  /**
   * Default implementation of delete that removes this item from its parent.
   * This enables the use of sdb:select-item with delete expressions.
   */
  @Override
  default void delete() {
    final JsonResourceSession resourceSession = getResourceSession();
    final JsonNodeTrx wtx = resourceSession.getNodeTrx().orElseGet(resourceSession::beginNodeTrx);
    wtx.moveTo(getNodeKey());
    wtx.remove();
  }
}
