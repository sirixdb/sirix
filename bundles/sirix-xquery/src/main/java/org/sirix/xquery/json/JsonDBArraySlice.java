package org.sirix.xquery.json;

import java.util.ArrayList;
import java.util.List;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.array.AbstractArray;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.type.ArrayType;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FirstAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.LastAxis;
import org.sirix.axis.temporal.NextAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.axis.temporal.PreviousAxis;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.stream.json.TemporalSirixJsonArraySliceStream;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

public final class JsonDBArraySlice extends AbstractJsonDBArray<JsonDBArraySlice> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonDBArraySlice.class));

  /** Sirix read-only transaction. */
  private final JsonNodeReadOnlyTrx rtx;

  /** Sirix node key. */
  private final long nodeKey;

  /** Collection this node is part of. */
  private final JsonDBCollection collection;

  /** Determines if write-transaction is present. */
  private final boolean isWtx;

  private final JsonItemFactory jsonUtil;

  private final int fromIndex;

  private final int toIndex;

  /**
   * Cached values.
   */
  private List<Sequence> values;

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
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);
    isWtx = this.rtx instanceof JsonNodeTrx;

    if (this.rtx.isDocumentRoot())
      this.rtx.moveToFirstChild();

    assert this.rtx.isArray();

    nodeKey = this.rtx.getNodeKey();

    jsonUtil = new JsonItemFactory();

    if ((fromIndex < 0) || (fromIndex > toIndex) || (fromIndex >= this.rtx.getChildCount())) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array start index: %s", fromIndex);
    }

    if ((toIndex < 0) || (toIndex > this.rtx.getChildCount())) {
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
    return new TemporalSirixJsonArraySliceStream(new PastAxis<>(rtx.getResourceManager(), rtx, include), collection,
        fromIndex, toIndex);
  }

  @Override
  public Stream<JsonDBArraySlice> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonArraySliceStream(new FutureAxis<>(rtx.getResourceManager(), rtx, include), collection,
        fromIndex, toIndex);
  }

  @Override
  public Stream<JsonDBArraySlice> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonArraySliceStream(new AllTimeAxis<>(rtx.getResourceManager(), rtx), collection,
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
    final var values = new ArrayList<Sequence>();

    for (int i = 0, length = len(); i < length; i++) {
      values.add(at(fromIndex + i));
    }

    return values;
  }

  private Sequence getSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    moveRtx();

    final var axis = new ChildAxis(rtx);

    for (int i = 0; i < index && axis.hasNext(); i++)
      axis.next();

    if (axis.hasNext()) {
      axis.next();

      return jsonUtil.getSequence(rtx, collection);
    }

    return null;
  }

  @Override
  public Sequence at(IntNumeric numericIndex) {
    int ii = fromIndex + numericIndex.intValue();
    if (ii >= toIndex) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array index: %s", numericIndex.intValue());
    }

    if (values == null) {
      return getSequenceAtIndex(rtx, ii);
    }

    return values.get(ii);
  }

  @Override
  public Sequence at(int index) {
    int ii = fromIndex + index;
    if (ii >= toIndex) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid array index: %s", index);
    }

    if (values == null) {
      return getSequenceAtIndex(rtx, ii);
    }

    return values.get(index);
  }

  @Override
  public IntNumeric length() {
    moveRtx();

    final int length = toIndex - fromIndex;
    return (length <= 20)
        ? Int32.ZERO_TWO_TWENTY[length]
        : new Int32(length);
  }

  @Override
  public int len() {
    moveRtx();

    final int length = toIndex - fromIndex;

    return length;
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(rtx, collection, from.intValue(), to.intValue());
  }
}
