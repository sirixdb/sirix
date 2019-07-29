package org.sirix.xquery.json;

import java.util.ArrayList;
import java.util.List;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.TemporalJsonItem;
import org.brackit.xquery.xdm.type.ArrayType;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
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
import org.sirix.node.Kind;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.json.TemporalSirixJsonObjectValueArrayStream;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

public final class JsonObjectValueDBArray extends AbstractItem
    implements TemporalJsonItem<JsonObjectValueDBArray>, Array, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonObjectValueDBArray.class));

  /** Sirix {@link v}. */
  private final JsonNodeReadOnlyTrx mRtx;

  /** Sirix node key. */
  private final long mNodeKey;

  /** Kind of node. */
  private final org.sirix.node.Kind mKind;

  /** Collection this node is part of. */
  private final JsonDBCollection mCollection;

  /** Determines if write-transaction is present. */
  private final boolean mIsWtx;

  private JsonUtil mJsonUtil;

  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonObjectValueDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mIsWtx = mRtx instanceof JsonNodeTrx;
    mNodeKey = mRtx.getNodeKey();

    assert mRtx.isObject();
    mKind = Kind.ARRAY;
    mJsonUtil = new JsonUtil();
  }

  @Override
  public long getNodeKey() {
    moveRtx();

    return mRtx.getNodeKey();
  }


  /**
   * Create a new {@link IReadTransaction} and move to {@link mKey}.
   *
   * @return new read transaction instance which is moved to {@link mKey}
   */
  private final void moveRtx() {
    mRtx.moveTo(mNodeKey);
  }

  public JsonDBCollection getCollection() {
    return mCollection;
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    return mRtx;
  }

  @Override
  public JsonObjectValueDBArray getNext() {
    moveRtx();

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  private JsonObjectValueDBArray moveTemporalAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis) {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new JsonObjectValueDBArray(rtx, mCollection);
    }

    return null;
  }

  @Override
  public JsonObjectValueDBArray getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new PreviousAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonObjectValueDBArray getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new FirstAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonObjectValueDBArray getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public Stream<JsonObjectValueDBArray> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectValueArrayStream(new PastAxis<>(mRtx.getResourceManager(), mRtx, include),
        mCollection);
  }

  @Override
  public Stream<JsonObjectValueDBArray> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectValueArrayStream(new FutureAxis<>(mRtx.getResourceManager(), mRtx, include),
        mCollection);
  }

  @Override
  public Stream<JsonObjectValueDBArray> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectValueArrayStream(new AllTimeAxis<>(mRtx.getResourceManager(), mRtx), mCollection);
  }

  @Override
  public boolean isNextOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    return otherNode.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    return otherTrx.getResourceManager().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final JsonObjectValueDBArray other) {
    moveRtx();

    if (!(other instanceof JsonObjectValueDBArray))
      return false;

    final JsonObjectValueDBArray otherNode = other;
    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  @Override
  public ItemType itemType() {
    return ArrayType.ARRAY;
  }

  @Override
  public Atomic atomize() {
    throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE, "The atomized value of array items is undefined");
  }

  @Override
  public boolean booleanValue() {
    throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE, "The boolean value of array items is undefined");
  }

  @Override
  public List<Sequence> values() {
    moveRtx();

    final List<Sequence> values = new ArrayList<Sequence>();

    for (int i = 0, length = len(); i < length; i++)
      values.add(at(i));

    return values;
  }

  private Sequence getSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    moveRtx();

    final var axis = new ChildAxis(rtx);

    for (int i = 0; i < index && axis.hasNext(); i++)
      axis.next();

    if (axis.hasNext()) {
      axis.next();

      return mJsonUtil.getSequence(rtx.moveToFirstChild().trx(), mCollection);
    }

    return null;
  }

  @Override
  public Sequence at(IntNumeric numericIndex) {
    return getSequenceAtIndex(mRtx, numericIndex.intValue());
  }

  @Override
  public Sequence at(int index) {
    return getSequenceAtIndex(mRtx, index);
  }

  @Override
  public IntNumeric length() {
    moveRtx();
    return new Int64(mRtx.getChildCount());
  }

  @Override
  public int len() {
    moveRtx();
    return (int) mRtx.getChildCount();
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    return new JsonDBArraySlice(mRtx, mCollection, from.intValue(), to.intValue());
  }
}
