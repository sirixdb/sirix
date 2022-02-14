package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.Null;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.xquery.StructuredDBItem;

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
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);
    nodeKey = this.rtx.getNodeKey();
  }

  private void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return rtx.getResourceManager();
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
