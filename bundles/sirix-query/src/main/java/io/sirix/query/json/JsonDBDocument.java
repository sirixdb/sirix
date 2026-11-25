package io.sirix.query.json;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.AbstractItem;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.ItemType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.StructuredDBItem;

import static java.util.Objects.requireNonNull;

public final class JsonDBDocument extends AbstractItem implements JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {
  private final JsonNodeReadOnlyTrx rtx;
  private final JsonDBCollection collection;

  JsonDBDocument(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    this.rtx = requireNonNull(rtx);
    this.collection = requireNonNull(collection);
  }

  @Override
  public JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    rtx.moveToDocumentRoot();
    return rtx;
  }

  @Override
  public long getNodeKey() {
    return 0;
  }

  @Override
  public JsonDBCollection getCollection() {
    return collection;
  }

  @Override
  public ItemType itemType() {
    return AnyJsonItemType.ANY_JSON_ITEM;
  }

  @Override
  public Atomic atomize() {
    throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE, "A JSON document has no atomic typed value");
  }

  @Override
  public boolean booleanValue() {
    return true;
  }
}
