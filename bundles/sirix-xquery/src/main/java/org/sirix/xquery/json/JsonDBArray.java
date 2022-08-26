package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.json.TemporalSirixJsonArrayStream;

public final class JsonDBArray extends AbstractJsonDBArray<JsonDBArray>
    implements TemporalJsonDBItem<JsonDBArray>, Array, JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** Sirix read-only transaction. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    super(rtx, collection, new JsonItemFactory());
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);

    if (this.rtx.isDocumentRoot()) {
      this.rtx.moveToFirstChild();
    }

    assert this.rtx.isArray();
  }

  @Override
  public Stream<JsonDBArray> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonArrayStream(new PastAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBArray> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonArrayStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBArray> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonArrayStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
  }

  @Override
  protected JsonDBArray createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection) {
    return new JsonDBArray(rtx, collection);
  }
}
