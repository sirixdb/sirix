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
   * Gets or creates a write transaction, reverting to the source revision if needed. This enables
   * branching from historical revisions - when you open a document at an old revision and make
   * changes, the write transaction is reverted to that revision's state before applying changes. The
   * commit will create a new revision while preserving all intermediate versions (SirixDB's
   * append-only model).
   *
   * @return the write transaction, possibly reverted to the source revision
   */
  default JsonNodeTrx getOrCreateWriteTrx() {
    final JsonResourceSession resourceSession = getResourceSession();
    final JsonNodeTrx wtx = resourceSession.getNodeTrx().orElseGet(resourceSession::beginNodeTrx);

    // Register the session with the store so it can be cleaned up on close
    final var store = getCollection().getJsonDBStore();
    if (store instanceof BasicJsonDBStore basicStore) {
      basicStore.registerWriteSession(resourceSession);
    }

    // If the read transaction is from an older revision than the write transaction,
    // revert the write transaction to match the source revision.
    // This enables editing historical versions and creating new branches.
    final int sourceRevision = getTrx().getRevisionNumber();
    final int mostRecentRevision = resourceSession.getMostRecentRevisionNumber();
    if (sourceRevision < mostRecentRevision) {
      wtx.revertTo(sourceRevision);
    }

    return wtx;
  }

  /**
   * Default implementation of replaceValue that navigates to parent and performs replacement. This
   * enables the use of sdb:select-item with replace expressions.
   * 
   * If the source document is from a historical revision, the write transaction is reverted to that
   * revision before applying the change, enabling branching from history.
   */
  @Override
  default void replaceValue(Sequence newValue) {
    final JsonNodeTrx wtx = getOrCreateWriteTrx();
    wtx.moveTo(getNodeKey());
    JsonItemSequence.replaceValue(wtx, newValue, getCollection());
  }

  /**
   * Default implementation of delete that removes this item from its parent. This enables the use of
   * sdb:select-item with delete expressions.
   * 
   * If the source document is from a historical revision, the write transaction is reverted to that
   * revision before applying the change, enabling branching from history.
   */
  @Override
  default void delete() {
    final JsonNodeTrx wtx = getOrCreateWriteTrx();
    wtx.moveTo(getNodeKey());
    wtx.remove();
  }
}
