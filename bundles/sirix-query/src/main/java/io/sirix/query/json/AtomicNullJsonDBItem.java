package io.sirix.query.json;

import io.brackit.query.atomic.Null;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.StructuredDBItem;

import static java.util.Objects.requireNonNull;

public final class AtomicNullJsonDBItem extends Null
    implements JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix {@link JsonNodeReadOnlyTrx}. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Sirix node key. */
  private final long nodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public AtomicNullJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
    nodeKey = this.rtx.getNodeKey();
  }

  private void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  @Override
  public JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    moveRtx();

    return rtx;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public JsonDBCollection getCollection() {
    return collection;
  }
}
