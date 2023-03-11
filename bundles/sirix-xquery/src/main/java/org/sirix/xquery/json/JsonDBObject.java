package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.*;
import org.brackit.xquery.jdm.AbstractItem;
import org.brackit.xquery.jdm.Item;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Stream;
import org.brackit.xquery.jdm.json.Array;
import org.brackit.xquery.jdm.json.Object;
import org.brackit.xquery.jdm.type.ArrayType;
import org.brackit.xquery.jdm.type.ItemType;
import org.brackit.xquery.jdm.type.ObjectType;
import org.brackit.xquery.util.ExprUtil;
import org.sirix.access.trx.node.json.objectvalue.*;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.json.JsonNameFilter;
import org.sirix.axis.temporal.*;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.json.SirixJsonStream;
import org.sirix.xquery.stream.json.TemporalSirixJsonObjectStream;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class JsonDBObject extends AbstractItem
    implements TemporalJsonDBItem<JsonDBObject>, Object, JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

  private static final long CHILD_THRESHOLD = 1;

  /**
   * Sirix transaction.
   */
  private final JsonNodeReadOnlyTrx rtx;

  /**
   * Sirix node key.
   */
  private final long nodeKey;

  /**
   * Collection this node is part of.
   */
  private final JsonDBCollection collection;

  /**
   * The factory to create new JSON items.
   */
  private final JsonItemFactory jsonItemFactory;

  /**
   * The field names mapped to sequences.
   */
  private final Map<QNm, Sequence> fields;

  /**
   * Map with PCR <=> matching nodes.
   */
  private final Map<Long, BitSet> filterMap;

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonDBObject(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    this.collection = requireNonNull(collection);
    this.rtx = requireNonNull(rtx);

    if (this.rtx.isDocumentRoot()) {
      this.rtx.moveToFirstChild();
    }

    nodeKey = this.rtx.getNodeKey();
    jsonItemFactory = new JsonItemFactory();
    fields = new HashMap<>();
    filterMap = new HashMap<>();
  }

  @Override
  public JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
  }

  @Override
  public long getNodeKey() {
    moveRtx();

    return rtx.getNodeKey();
  }

  /**
   * Move the transaction to {@code nodeKey}.
   */
  private void moveRtx() {
    rtx.moveTo(nodeKey);
  }

  @Override
  public JsonDBCollection getCollection() {
    return collection;
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    return rtx;
  }

  @Override
  public JsonDBObject getNext() {
    moveRtx();

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  private JsonDBObject moveTemporalAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis) {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return new JsonDBObject(rtx, collection);
    }

    return null;
  }

  @Override
  public JsonDBObject getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new PreviousAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBObject getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new FirstAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBObject getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public Stream<JsonDBObject> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new PastAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new FutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectStream(new AllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
  }

  @Override
  public boolean isNextOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return other.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return other.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return other.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final JsonDBObject other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return other.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return other.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final JsonDBObject other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return other.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final JsonDBObject other) {
    moveRtx();

    if (other == null)
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    return otherTrx.getResourceSession().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final JsonDBObject other) {
    moveRtx();

    if (other == null)
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  @Override
  public ItemType itemType() {
    return ObjectType.OBJECT;
  }

  @Override
  public Atomic atomize() {
    throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE, "The atomized value of record items is undefined");
  }

  @Override
  public boolean booleanValue() {
    throw new QueryException(ErrorCode.ERR_ITEM_HAS_NO_TYPED_VALUE, "The boolean value of record items is undefined");
  }

  private JsonNodeTrx getReadWriteTrx() {
    final JsonResourceSession resourceManager = rtx.getResourceSession();
    final var trx = resourceManager.getNodeTrx().orElseGet(resourceManager::beginNodeTrx);
    trx.moveTo(nodeKey);
    return trx;
  }

  @Override
  public Object replace(QNm field, Sequence value) {
    moveRtx();
    if (rtx.hasChildren()) {
      modify(field, value);
      fields.put(field, value);
    }
    return this;
  }

  private void modify(QNm field, Sequence value) {
    final var trx = getReadWriteTrx();

    final var foundNode = findField(field, trx);

    if (!foundNode) {
      return;
    }

    if (value instanceof Array) {
      trx.replaceObjectRecordValue(new ArrayValue());
      insertSubtree(value, trx);
    } else if (value instanceof Object) {
      trx.replaceObjectRecordValue(new ObjectValue());
      insertSubtree(value, trx);
    } else if (value instanceof Str) {
      trx.replaceObjectRecordValue(new StringValue(((Str) value).stringValue()));
    } else if (value instanceof Null) {
      trx.replaceObjectRecordValue(new NullValue());
    } else if (value instanceof Bool) {
      trx.replaceObjectRecordValue(new BooleanValue(value.booleanValue()));
    } else if (value instanceof Numeric) {
      switch (value) {
        case Int anInt -> trx.replaceObjectRecordValue(new NumberValue(anInt.intValue()));
        case Int32 int32 -> trx.replaceObjectRecordValue(new NumberValue(int32.intValue()));
        case Int64 int64 -> trx.replaceObjectRecordValue(new NumberValue(int64.longValue()));
        case Flt flt -> trx.replaceObjectRecordValue(new NumberValue(flt.floatValue()));
        case Dbl dbl -> trx.replaceObjectRecordValue(new NumberValue(dbl.doubleValue()));
        case Dec dec -> trx.replaceObjectRecordValue(new NumberValue(dec.decimalValue()));
        case default -> {
        }
      }
    }
  }

  private void insertSubtree(Sequence value, JsonNodeTrx trx) {
    final Item item = ExprUtil.asItem(value);
    trx.insertSubtreeAsLastChild(item);
  }

  private boolean findField(QNm field, JsonNodeTrx trx) {
    moveRtx();
    if (rtx.getResourceSession().getResourceConfig().withPathSummary && rtx.getChildCount() > CHILD_THRESHOLD
        && hasNoMatchingPathNode(field)) {
      return false;
    }

    trx.moveToFirstChild();

    boolean isFound = false;

    do {
      if (trx.getName().equals(field)) {
        isFound = true;
        break;
      }
    } while (trx.moveToRightSibling());

    return isFound;
  }

  @Override
  public Object rename(QNm field, QNm newFieldName) {
    moveRtx();
    if (rtx.hasChildren()) {
      final var trx = getReadWriteTrx();

      final var foundField = findField(field, trx);

      if (foundField) {
        trx.setObjectKeyName(newFieldName.getLocalName());
        fields.remove(field);
        trx.moveToFirstChild();
        fields.put(newFieldName, jsonItemFactory.getSequence(trx, collection));
      }
    }
    return this;
  }

  @Override
  public Object insert(QNm field, Sequence value) {
    moveRtx();
    if (get(field) != null) {
      return this;
    }
    final var trx = getReadWriteTrx();

    insert(field, value, trx);

    fields.put(field, value);

    return this;
  }

  private void insert(QNm field, Sequence value, JsonNodeTrx trx) {
    final var fieldName = field.getLocalName();
    if (value instanceof Atomic) {
      switch (value) {
        case Str str -> trx.insertObjectRecordAsLastChild(fieldName, new StringValue(str.stringValue()));
        case Null ignored -> trx.insertObjectRecordAsLastChild(fieldName, new NullValue());
        case Numeric ignored1 -> {
          switch (value) {
            case Int anInt -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(anInt.intValue()));
            case Int32 int32 -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(int32.intValue()));
            case Int64 int64 -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(int64.longValue()));
            case Flt flt -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(flt.floatValue()));
            case Dbl dbl -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(dbl.doubleValue()));
            case Dec dec -> trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(dec.decimalValue()));
            case default -> {
            }
          }
        }
        case Bool ignored2 -> trx.insertObjectRecordAsLastChild(fieldName, new BooleanValue(value.booleanValue()));
        case default -> {
        }
      }
    } else {
      final Item item = ExprUtil.asItem(value);

      if (item.itemType() == ArrayType.ARRAY) {
        trx.insertObjectRecordAsLastChild(fieldName, new ArrayValue());
      } else if (item.itemType() == ObjectType.OBJECT) {
        trx.insertObjectRecordAsLastChild(fieldName, new ObjectValue());
      }

      trx.moveToFirstChild();
      trx.insertSubtreeAsFirstChild(item,
                                    JsonNodeTrx.Commit.NO,
                                    JsonNodeTrx.CheckParentNode.YES,
                                    JsonNodeTrx.SkipRootToken.YES);
    }
  }

  @Override
  public Object remove(QNm field) {
    moveRtx();
    if (rtx.hasChildren()) {
      final var trx = getReadWriteTrx();

      final var isFound = findField(field, trx);

      if (isFound) {
        trx.remove();
      }
    }
    return this;
  }

  @Override
  public Object remove(IntNumeric index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Sequence get(QNm field) {
    moveRtx();

    return fields.computeIfAbsent(field, (unused) -> {
      if (rtx.getResourceSession().getResourceConfig().withPathSummary && rtx.getChildCount() > CHILD_THRESHOLD
          && hasNoMatchingPathNode(field)) {
        return null;
      }

      moveRtx();
      final var axis = new FilterAxis<>(new ChildAxis(rtx), new JsonNameFilter(rtx, field));

      if (axis.hasNext()) {
        axis.nextLong();
        rtx.moveToFirstChild();

        return jsonItemFactory.getSequence(rtx, collection);
      }

      return null;
    });
  }

  private boolean hasNoMatchingPathNode(QNm field) {
    rtx.moveToParent();
    final long pcr = rtx.isDocumentRoot() ? 0 : rtx.getPathNodeKey();
    rtx.moveTo(nodeKey);
    BitSet matches = filterMap.get(pcr);
    if (matches == null) {
      try (final PathSummaryReader reader = rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber())) {
        if (pcr != 0) {
          reader.moveTo(pcr);
        }
        final int level = reader.getLevel() + 1;
        matches = reader.match(field, level);
        filterMap.put(pcr, matches);
      }
    }
    // No matches.
    return matches.cardinality() == 0;
  }

  @Override
  public Sequence value(final IntNumeric intNumericIndex) {
    moveRtx();

    final int index = intNumericIndex.intValue();

    return getValueSequenceAtIndex(rtx, index);
  }

  private Sequence getValueSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    final var axis = new ChildAxis(rtx);

    for (int i = 0; i < index && axis.hasNext(); i++) {
      axis.nextLong();
    }

    if (axis.hasNext()) {
      axis.nextLong();

      rtx.moveToFirstChild();

      return jsonItemFactory.getSequence(rtx, collection);
    }

    return null;
  }

  @Override
  public Sequence value(final int index) {
    Preconditions.checkArgument(index >= 0);

    moveRtx();

    return getValueSequenceAtIndex(rtx, index);
  }

  @Override
  public Array names() {
    moveRtx();

    return new JsonObjectKeyDBArray(rtx, collection);
  }

  @Override
  public Array values() {
    moveRtx();

    return new JsonObjectValueDBArray(rtx, collection);
  }

  @Override
  public QNm name(IntNumeric numericIndex) {
    Preconditions.checkArgument(numericIndex.intValue() >= 0);

    moveRtx();

    return getNameAtIndex(rtx, numericIndex.intValue());
  }

  @Override
  public QNm name(final int index) {
    Preconditions.checkArgument(index >= 0);

    moveRtx();

    return getNameAtIndex(rtx, index);
  }

  private QNm getNameAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    final var axis = new ChildAxis(rtx);

    try (final var stream = new SirixJsonStream(axis, collection)) {
      int i = 0;
      while (i < index && stream.next() != null) {
        i++;
      }
      final var jsonItem = (JsonDBItem) stream.next();

      if (jsonItem != null) {
        return jsonItem.getTrx().getName();
      }

      return null;
    }
  }

  @Override
  public IntNumeric length() {
    moveRtx();

    return new Int64(rtx.getChildCount());
  }

  @Override
  public int len() {
    moveRtx();

    return (int) rtx.getChildCount();
  }
}
