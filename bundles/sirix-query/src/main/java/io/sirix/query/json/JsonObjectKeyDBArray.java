package io.sirix.query.json;

import io.sirix.query.stream.json.TemporalSirixJsonObjectKeyArrayStream;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.TemporalJsonItem;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.axis.temporal.PrefetchedFutureAxis;
import io.sirix.axis.temporal.PrefetchedPastAxis;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class JsonObjectKeyDBArray extends AbstractJsonDBArray<JsonObjectKeyDBArray>
    implements TemporalJsonItem<JsonObjectKeyDBArray> {

  private static final JsonItemFactory NAME_FACTORY = new JsonItemFactory();

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
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonObjectKeyDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    super(rtx, collection, new JsonItemFactory());
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);
    assert this.rtx.isObject();
  }

  /**
   * This array represents the NAMES (keys) of an object's fields. Under fusion, the shared
   * {@link JsonItemFactory#getSequence} method returns the inline VALUE for OBJECT_NAMED_*
   * records, which is wrong for a name-iteration. Override to always return the field name
   * regardless of whether the underlying record is fused.
   */
  @Override
  public List<Sequence> values() {
    rtx.moveTo(getNodeKey());
    final List<Sequence> out = new ArrayList<>((int) rtx.getChildCount());
    final ChildAxis axis = new ChildAxis(rtx);
    axis.forEach(nodeKey -> out.add(NAME_FACTORY.getNameSequence(rtx, collection)));
    return out;
  }

  @Override
  public Sequence at(final int index) {
    if (index < 0) {
      throw new IllegalArgumentException("index must be >= 0");
    }
    rtx.moveTo(getNodeKey());
    final ChildAxis axis = new ChildAxis(rtx);
    for (int i = 0; i < index && axis.hasNext(); i++) {
      axis.nextLong();
    }
    if (axis.hasNext()) {
      axis.nextLong();
      return NAME_FACTORY.getNameSequence(rtx, collection);
    }
    return null;
  }

  @Override
  public Sequence at(final IntNumeric numericIndex) {
    return at(numericIndex.intValue());
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectKeyArrayStream(new PrefetchedPastAxis<>(rtx.getResourceSession(), rtx, include),
        collection);
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectKeyArrayStream(new PrefetchedFutureAxis<>(rtx.getResourceSession(), rtx, include),
        collection);
  }

  @Override
  public Stream<JsonObjectKeyDBArray> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectKeyArrayStream(new PrefetchedAllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
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
