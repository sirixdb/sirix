package org.sirix.xquery.json;

import org.brackit.xquery.atomic.Str;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.xquery.StructuredDBItem;

import static java.util.Objects.requireNonNull;

public final class AtomicStrJsonDBItem extends Str
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
   * @param string the atomic string value delegate
   */
  public AtomicStrJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final String string) {
    super(string);
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
  public JsonDBCollection getCollection() {
    return collection;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }
}
