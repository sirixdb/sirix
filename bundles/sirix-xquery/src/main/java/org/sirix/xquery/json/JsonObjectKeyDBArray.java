package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.TemporalJsonItem;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.xquery.stream.json.TemporalSirixJsonObjectKeyArrayStream;

public final class JsonObjectKeyDBArray extends AbstractJsonDBArray<JsonObjectKeyDBArray>
    implements TemporalJsonItem<JsonObjectKeyDBArray> {

  /**
   * Sirix read-only transaction.
   */
  private final JsonNodeReadOnlyTrx rtx;

  /**
   * Collection this node is part of.
   */
  private final JsonDBCollection collection;

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonObjectKeyDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    super(rtx, collection, new JsonItemFactory());
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);
    assert this.rtx.isObject();
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectKeyArrayStream(new PastAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectKeyArrayStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectKeyArrayStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
  }

  @Override
  protected JsonObjectKeyDBArray createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection) {
    return new JsonObjectKeyDBArray(rtx, collection);
  }
}
