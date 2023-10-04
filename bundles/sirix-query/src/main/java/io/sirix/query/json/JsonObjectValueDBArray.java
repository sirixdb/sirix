package io.sirix.query.json;

import io.sirix.query.stream.json.TemporalSirixJsonObjectValueArrayStream;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Array;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.AllTimeAxis;
import io.sirix.axis.temporal.FutureAxis;
import io.sirix.axis.temporal.PastAxis;

import static java.util.Objects.requireNonNull;

public final class JsonObjectValueDBArray extends AbstractJsonDBArray<JsonObjectValueDBArray>
    implements TemporalJsonDBItem<JsonObjectValueDBArray> {

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
  public JsonObjectValueDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    super(rtx, collection, new JsonItemFactory());
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
    assert this.rtx.isObject();
  }

  @Override
  public Stream<JsonObjectValueDBArray> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectValueArrayStream(new PastAxis<>(rtx.getResourceSession(), rtx, include),
                                                       collection);
  }

  @Override
  public Stream<JsonObjectValueDBArray> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectValueArrayStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include),
                                                       collection);
  }

  @Override
  public Stream<JsonObjectValueDBArray> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectValueArrayStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
  }

  @Override
  protected JsonObjectValueDBArray createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection) {
    return new JsonObjectValueDBArray(rtx, collection);
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
  }
}
