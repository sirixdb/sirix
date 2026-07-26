package io.sirix.query.json;

import io.sirix.query.stream.json.TemporalSirixJsonArraySliceStream;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Array;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.axis.temporal.PrefetchedFutureAxis;
import io.sirix.axis.temporal.PrefetchedPastAxis;
import io.sirix.settings.Fixed;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class JsonDBArraySlice extends AbstractJsonDBArray<JsonDBArraySlice> {

  /** Sirix read-only transaction. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  private final JsonItemFactory jsonUtil;

  private final int fromIndex;

  private final int toIndex;

  /**
   * Cached values.
   */
  private List<Sequence> values;

  /** Last slice-relative index served by {@link #at(int)} / {@link #at(IntNumeric)}. */
  private int cursorSliceIndex = -1;

  /** Node key positioned at {@link #cursorSliceIndex}; {@link Fixed#NULL_NODE_KEY} if invalid. */
  private long cursorNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param fromIndex the from index
   * @param toIndex the to index
   */
  public JsonDBArraySlice(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection, final int fromIndex,
      final int toIndex) {
    super(rtx, collection, new JsonItemFactory());
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);

    if (this.rtx.isDocumentRoot()) {
      this.rtx.moveToFirstChild();
    }

    assert this.rtx.isArray();

    jsonUtil = getJsonItemFactory();

    final long childCount = this.rtx.getChildCount();
    if ((fromIndex < 0) || (fromIndex > toIndex) || (fromIndex >= childCount)) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array start index: %s", fromIndex);
    }

    if (toIndex > childCount) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array end index: %s", toIndex);
    }

    this.fromIndex = fromIndex;
    this.toIndex = toIndex;
  }

  @Override
  public Stream<JsonDBArraySlice> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonArraySliceStream(new PrefetchedPastAxis<>(rtx.getResourceSession(), rtx, include), collection,
        fromIndex, toIndex);
  }

  @Override
  public Stream<JsonDBArraySlice> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonArraySliceStream(new PrefetchedFutureAxis<>(rtx.getResourceSession(), rtx, include), collection,
        fromIndex, toIndex);
  }

  @Override
  public Stream<JsonDBArraySlice> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonArraySliceStream(new PrefetchedAllTimeAxis<>(rtx.getResourceSession(), rtx), collection,
        fromIndex, toIndex);
  }

  @Override
  protected JsonDBArraySlice createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection) {
    return new JsonDBArraySlice(rtx, collection, fromIndex, toIndex);
  }

  @Override
  public List<Sequence> values() {
    moveRtx();

    if (values == null) {
      values = getValues();
    }

    return values;
  }

  private List<Sequence> getValues() {
    final int length = toIndex - fromIndex;
    final ArrayList<Sequence> out = new ArrayList<>(length);
    if (length == 0) {
      return out;
    }

    final ChildAxis axis = new ChildAxis(rtx);

    for (int skipped = 0; skipped < fromIndex; skipped++) {
      if (!axis.hasNext()) {
        return out;
      }
      axis.nextLong();
    }

    for (int collected = 0; collected < length; collected++) {
      if (!axis.hasNext()) {
        break;
      }
      axis.nextLong();
      out.add(jsonUtil.getSequence(rtx, collection));
    }

    invalidateCursor();
    return out;
  }

  private Sequence sequenceAtSliceIndex(final int sliceIndex) {
    final int absoluteIndex = fromIndex + sliceIndex;
    final long arrayKey = getNodeKey();

    if (cursorSliceIndex >= 0 && sliceIndex == cursorSliceIndex + 1
        && rtx.moveTo(cursorNodeKey) && rtx.getParentKey() == arrayKey
        && rtx.hasRightSibling()) {
      rtx.moveToRightSibling();
      cursorSliceIndex = sliceIndex;
      cursorNodeKey = rtx.getNodeKey();
      return jsonUtil.getSequence(rtx, collection);
    }

    moveRtx();
    final ChildAxis axis = new ChildAxis(rtx);

    for (int i = 0; i < absoluteIndex; i++) {
      if (!axis.hasNext()) {
        invalidateCursor();
        return null;
      }
      axis.nextLong();
    }

    if (!axis.hasNext()) {
      invalidateCursor();
      return null;
    }
    axis.nextLong();

    cursorSliceIndex = sliceIndex;
    cursorNodeKey = rtx.getNodeKey();
    return jsonUtil.getSequence(rtx, collection);
  }

  private void invalidateCursor() {
    cursorSliceIndex = -1;
    cursorNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public Sequence at(final IntNumeric numericIndex) {
    final int sliceIndex = numericIndex.intValue();
    if (fromIndex + sliceIndex >= toIndex) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array index: %s", sliceIndex);
    }

    if (values == null) {
      return sequenceAtSliceIndex(sliceIndex);
    }

    return values.get(sliceIndex);
  }

  @Override
  public Sequence at(final int index) {
    if (fromIndex + index >= toIndex) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array index: %s", index);
    }

    if (values == null) {
      return sequenceAtSliceIndex(index);
    }

    return values.get(index);
  }

  @Override
  public IntNumeric length() {
    moveRtx();

    final int length = toIndex - fromIndex;
    return (length <= 20)
        ? Int32.ZERO_TO_TWENTY[length]
        : new Int32(length);
  }

  @Override
  public int len() {
    moveRtx();

    return toIndex - fromIndex;
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
  }
}
