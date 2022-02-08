package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.*;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Object;
import org.brackit.xquery.xdm.type.ArrayType;
import org.brackit.xquery.xdm.type.ItemType;
import org.brackit.xquery.xdm.type.ObjectType;
import org.sirix.access.trx.node.json.objectvalue.*;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.axis.filter.json.JsonNameFilter;
import org.sirix.axis.temporal.*;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.stream.json.SirixJsonStream;
import org.sirix.xquery.stream.json.TemporalSirixJsonObjectStream;

import java.util.HashMap;
import java.util.Map;

public final class JsonDBObject extends AbstractItem
    implements TemporalJsonDBItem<JsonDBObject>, Object, JsonDBItem, StructuredDBItem<JsonNodeReadOnlyTrx> {

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

  private final Map<QNm, Sequence> fields;

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonDBObject(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    this.collection = Preconditions.checkNotNull(collection);
    this.rtx = Preconditions.checkNotNull(rtx);

    if (this.rtx.isDocumentRoot()) {
      this.rtx.moveToFirstChild();
    }

    nodeKey = this.rtx.getNodeKey();
    jsonItemFactory = new JsonItemFactory();
    fields = new HashMap<>();
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return rtx.getResourceManager();
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

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(rtx.getResourceManager(), rtx);
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
        new PreviousAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBObject getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new FirstAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public JsonDBObject getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public Stream<JsonDBObject> getEarlier(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new PastAxis<>(rtx.getResourceManager(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf ? IncludeSelf.YES : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new FutureAxis<>(rtx.getResourceManager(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectStream(new AllTimeAxis<>(rtx.getResourceManager(), rtx), collection);
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

    return otherTrx.getResourceManager().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
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
    final JsonResourceManager resourceManager = rtx.getResourceManager();
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
      if (value instanceof Int) {
        trx.replaceObjectRecordValue(new NumberValue(((Int) value).intValue()));
      } else if (value instanceof Int32) {
        trx.replaceObjectRecordValue(new NumberValue(((Int32) value).intValue()));
      } else if (value instanceof Int64) {
        trx.replaceObjectRecordValue(new NumberValue(((Int64) value).longValue()));
      } else if (value instanceof Flt) {
        trx.replaceObjectRecordValue(new NumberValue(((Flt) value).floatValue()));
      } else if (value instanceof Dbl) {
        trx.replaceObjectRecordValue(new NumberValue(((Dbl) value).doubleValue()));
      } else if (value instanceof Dec) {
        trx.replaceObjectRecordValue(new NumberValue(((Dec) value).decimalValue()));
      }
    }
  }

  private void insertSubtree(Sequence value, JsonNodeTrx trx) {
    final Item item = ExprUtil.asItem(value);
    trx.insertSubtreeAsLastChild(item);
  }

  private boolean findField(QNm field, JsonNodeTrx trx) {
    trx.moveToFirstChild();

    boolean isFound = false;

    do {
      if (trx.getName().equals(field)) {
        isFound = true;
        break;
      }
    } while (trx.moveToRightSibling().hasMoved());

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
        fields.put(newFieldName, jsonItemFactory.getSequence(trx.moveToFirstChild().trx(), collection));
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
      if (value instanceof Str) {
        trx.insertObjectRecordAsLastChild(fieldName, new StringValue(((Str) value).stringValue()));
      } else if (value instanceof Null) {
        trx.insertObjectRecordAsLastChild(fieldName, new NullValue());
      } else if (value instanceof Numeric) {
        if (value instanceof Int) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Int) value).intValue()));
        } else if (value instanceof Int32) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Int32) value).intValue()));
        } else if (value instanceof Int64) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Int64) value).longValue()));
        } else if (value instanceof Flt) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Flt) value).floatValue()));
        } else if (value instanceof Dbl) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Dbl) value).doubleValue()));
        } else if (value instanceof Dec) {
          trx.insertObjectRecordAsLastChild(fieldName, new NumberValue(((Dec) value).decimalValue()));
        }
      } else if (value instanceof Bool) {
        trx.insertObjectRecordAsLastChild(fieldName, new BooleanValue(value.booleanValue()));
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
      final var axis = new FilterAxis<>(new ChildAxis(rtx), new JsonNameFilter(rtx, field));

      if (axis.hasNext()) {
        axis.next();

        return jsonItemFactory.getSequence(rtx.moveToFirstChild().trx(), collection);
      }

      return null;
    });
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
      axis.next();
    }

    if (axis.hasNext()) {
      axis.next();

      return jsonItemFactory.getSequence(rtx.moveToFirstChild().trx(), collection);
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
      final var jsonItem = stream.next();

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
