package org.sirix.xquery.json;

import java.util.ArrayList;
import java.util.Optional;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.array.DArray;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Record;
import org.brackit.xquery.xdm.json.TemporalJsonItem;
import org.brackit.xquery.xdm.type.ArrayType;
import org.brackit.xquery.xdm.type.ItemType;
import org.brackit.xquery.xdm.type.ListOrUnionType;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.json.JsonNameFilter;
import org.sirix.axis.temporal.AllTimeAxis;
import org.sirix.axis.temporal.FirstAxis;
import org.sirix.axis.temporal.FutureAxis;
import org.sirix.axis.temporal.LastAxis;
import org.sirix.axis.temporal.NextAxis;
import org.sirix.axis.temporal.PastAxis;
import org.sirix.axis.temporal.PreviousAxis;
import org.sirix.node.Kind;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.Pair;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.json.SirixJsonStream;
import org.sirix.xquery.stream.json.TemporalSirixJsonStream;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

public final class JsonDBItem extends AbstractItem
    implements TemporalJsonItem<JsonDBItem>, Array, Record, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonDBItem.class));

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


  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mIsWtx = mRtx instanceof JsonNodeTrx;
    mNodeKey = mRtx.getNodeKey();
    mKind = mRtx.getKind();
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
  public JsonDBItem getNext() {
    moveRtx();

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  private JsonDBItem moveTemporalAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis) {
    if (axis.hasNext()) {
      final Pair<Integer, Long> pair = axis.next();

      final ResourceManager<JsonNodeReadOnlyTrx, JsonNodeTrx> resourceManager = axis.getResourceManager();
      final Optional<JsonNodeReadOnlyTrx> optionalRtx = resourceManager.getNodeReadTrxByRevisionNumber(pair.getFirst());

      final JsonNodeReadOnlyTrx rtx;
      if (optionalRtx.isPresent()) {
        rtx = optionalRtx.get();
        rtx.moveTo(pair.getSecond());
      } else {
        rtx = resourceManager.beginNodeReadOnlyTrx(pair.getFirst());
        rtx.moveTo(pair.getSecond());
      }
      return new JsonDBItem(rtx, mCollection);
    }

    return null;
  }

  @Override
  public JsonDBItem getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new PreviousAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBItem getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new FirstAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBItem getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(mRtx.getResourceManager(), mRtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public Stream<JsonDBItem> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonStream(new PastAxis<>(mRtx.getResourceManager(), mRtx, include), mCollection);
  }

  @Override
  public Stream<JsonDBItem> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonStream(new FutureAxis<>(mRtx.getResourceManager(), mRtx, include), mCollection);
  }

  @Override
  public Stream<JsonDBItem> getAllTime() {
    moveRtx();
    return new TemporalSirixJsonStream(new AllTimeAxis<>(mRtx.getResourceManager(), mRtx), mCollection);
  }

  @Override
  public boolean isNextOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final JsonDBItem other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    return otherNode.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final JsonDBItem other) {
    moveRtx();

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    return otherTrx.getResourceManager().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final JsonDBItem other) {
    moveRtx();

    if (!(other instanceof JsonDBItem))
      return false;

    final JsonDBItem otherNode = other;
    final NodeReadOnlyTrx otherTrx = otherNode.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  @Override
  public ItemType itemType() {
    moveRtx();

    switch (mKind) {
      case OBJECT:
        return ListOrUnionType.LIST_OR_UNION;
      case ARRAY:
        return ArrayType.ARRAY;
      default:
        throw new IllegalStateException("Json item type not known.");
    }
  }

  @Override
  public Atomic atomize() {
    moveRtx();

    switch (mKind) {
      case OBJECT:
        throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
            "The atomized value of record items is undefined");
      case ARRAY:
        throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
            "The atomized value of array items is undefined");
      default:
        throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
            "The atomized value of " + mKind + " is undefined");
    }
  }

  @Override
  public boolean booleanValue() {
    moveRtx();

    switch (mKind) {
      case OBJECT:
      case OBJECT_RECORD:
        throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
            "The atomized value of record items is undefined");
      case ARRAY:
        throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE,
            "The atomized value of array items is undefined");
      default:
        return mRtx.getBooleanValue();
    }
  }

  @Override
  public Sequence get(QNm field) {
    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    final var axis = new FilterAxis<JsonNodeReadOnlyTrx>(new ChildAxis(rtx), new JsonNameFilter(rtx, field));;

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      return stream.next();
    }
  }

  @Override
  public Sequence value(IntNumeric intNumericIndex) {
    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    final int index = intNumericIndex.intValue();

    return getSequenceAtIndex(rtx, index);
  }

  private Sequence getSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    final var axis = new ChildAxis(rtx);

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      for (int i = 0; i < index && stream.next() != null; i++);
      return stream.next();
    }
  }

  @Override
  public Sequence value(final int index) {
    Preconditions.checkArgument(index > 0);

    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    return getSequenceAtIndex(rtx, index);
  }

  @Override
  public Array names() {
    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    final var axis = new ChildAxis(rtx);

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      final var sequence = new ArrayList<>();

      JsonDBItem item;
      while ((item = stream.next()) != null) {
        sequence.add(item);
      }

      return new DArray(sequence.toArray(new JsonDBItem[sequence.size()]));
    }
  }

  @Override
  public Array values() {
    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    final var axis = new ChildAxis(rtx);

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      final var sequence = new ArrayList<>();

      JsonDBItem item;
      while ((item = stream.next()) != null) {
        final JsonNodeReadOnlyTrx itemTrx = item.getTrx();
        sequence.add(new JsonDBItem(itemTrx.moveTo(itemTrx.getFirstChildKey()).getCursor(), mCollection));
      }

      return new DArray(sequence.toArray(new JsonDBItem[sequence.size()]));
    }
  }

  @Override
  public QNm name(IntNumeric numericIndex) {
    Preconditions.checkArgument(numericIndex.intValue() > 0);

    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    return getNameAtIndex(rtx, numericIndex.intValue());
  }

  @Override
  public QNm name(int index) {
    Preconditions.checkArgument(index > 0);

    moveRtx();

    final JsonNodeReadOnlyTrx rtx = getTrx();

    if (rtx.getKind() != Kind.OBJECT) {
      throw new IllegalStateException("Json item kind is not an object.");
    }

    return getNameAtIndex(rtx, index);
  }

  private QNm getNameAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    final var axis = new ChildAxis(rtx);

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      for (int i = 0; i < index && stream.next() != null; i++);
      final var jsonItem = stream.next();

      if (jsonItem != null) {
        return jsonItem.getTrx().getName();
      }

      return null;
    }
  }

  @Override
  public Sequence at(IntNumeric numericIndex) {
    if (mKind != Kind.ARRAY) {
      throw new IllegalStateException("Json item kind is not an array.");
    }

    return getSequenceAtIndex(mRtx, numericIndex.intValue());
  }

  @Override
  public Sequence at(int index) {
    if (mKind != Kind.ARRAY) {
      throw new IllegalStateException("Json item kind is not an array.");
    }

    return getSequenceAtIndex(mRtx, index);
  }

  @Override
  public IntNumeric length() {
    moveRtx();

    if (mKind == Kind.OBJECT || mKind == Kind.ARRAY) {
      return new Int64(mRtx.getChildCount());
    }

    throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid item kind: %s", mKind);
  }

  @Override
  public int len() {
    moveRtx();

    if (mKind == Kind.OBJECT || mKind == Kind.ARRAY) {
      // FIXME
      return (int) mRtx.getChildCount();
    }

    throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid item kind: %s", mKind);
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    moveRtx();

    if (mKind != Kind.ARRAY) {
      throw new IllegalStateException("Json item kind is not an array.");
    }

    final var axis = new ChildAxis(mRtx);

    try (final var stream = new SirixJsonStream(axis, mCollection)) {
      for (int i = 0, fromIndex = from.intValue(); i < fromIndex && stream.next() != null; i++);

      final var item = stream.next();

      if (item == null)
        return new DArray();

      final var items = new ArrayList<>();
      items.add(item);

      for (int i = from.intValue(), toIndex = to.intValue(); i <= toIndex && stream.next() != null; i++) {
        final var jsonItem = stream.next();

        if (jsonItem != null)
          items.add(jsonItem);
      }

      return new DArray(items.toArray(new JsonDBItem[items.size()]));
    }
  }
}
