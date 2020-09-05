package org.sirix.xquery.json;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.array.AbstractArray;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.type.ArrayType;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.temporal.FirstAxis;
import org.sirix.axis.temporal.LastAxis;
import org.sirix.axis.temporal.NextAxis;
import org.sirix.axis.temporal.PreviousAxis;
import org.sirix.xquery.StructuredDBItem;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractJsonDBArray<T extends AbstractJsonDBArray<T>> extends AbstractArray
    implements TemporalJsonDBItem<T>, JsonDBItem, Array, StructuredDBItem<JsonNodeReadOnlyTrx> {
  /**
   * The unique nodeKey of the current node.
   */
  private final long nodeKey;

  /**
   * The collection/database.
   */
  private final JsonDBCollection collection;

  /**
   * The read-only JSON node transaction.
   */
  private final JsonNodeReadOnlyTrx rtx;

  /**
   * The item factory.
   */
  private final JsonItemFactory jsonItemFactory;

  /**
   * Provides utility methods to process JSON item sequences.
   */
  private final JsonItemSequence jsonItemSequence;

  /**
   * Cached values.
   */
  private List<Sequence> values;

  private enum Op {
    Replace,

    Insert,

    Append
  }

  AbstractJsonDBArray(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final JsonItemFactory jsonItemFactory) {
    if (rtx.isDocumentRoot()) {
      rtx.moveToFirstChild();
    }
    this.rtx = rtx;
    this.nodeKey = rtx.getNodeKey();
    this.collection = collection;
    this.jsonItemFactory = jsonItemFactory;
    jsonItemSequence = new JsonItemSequence();
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return rtx.getResourceManager();
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
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
  public Array replaceAt(int index, Sequence value) {
    modify(index, value, Op.Replace);
    return this;
  }

  @Override
  public Array append(Sequence value) {
    final JsonNodeTrx trx = getReadWriteTrx();

    if (trx.hasChildren()) {
      trx.moveToLastChild();
    }

    jsonItemSequence.insert(value, trx, nodeKey);

    return this;
  }

  private void modify(int index, Sequence value, final Op op) {
    final JsonNodeTrx trx = getReadWriteTrx();
    if (index > trx.getChildCount()) {
      trx.close();
      throw new IllegalStateException("Index " + index + " is out of range.");
    }

    if (index != 0) {
      moveToIndex(index, trx);
    }

    if (op == Op.Replace || op == Op.Insert) {
      final long ancorNodeKey;
      if (trx.hasLeftSibling()) {
        ancorNodeKey = trx.getLeftSiblingKey();
      } else {
        ancorNodeKey = trx.getParentKey();
      }
      if (op == Op.Replace) {
        trx.remove();
      }
      trx.moveTo(ancorNodeKey);
    }

    jsonItemSequence.insert(value, trx, nodeKey);

    values = null;
  }



  private void moveToIndex(int index, JsonNodeTrx trx) {
    // must have children

    trx.moveToFirstChild();

    for (int i = 1; i <= index; i++) {
      trx.moveToRightSibling();
    }
  }

  private JsonNodeTrx getReadWriteTrx() {
    final JsonResourceManager resourceManager = rtx.getResourceManager();
    final var trx = resourceManager.getNodeTrx().orElseGet(resourceManager::beginNodeTrx);
    trx.moveTo(nodeKey);
    return trx;
  }

  @Override
  public Array replaceAt(IntNumeric index, Sequence value) {
    return replaceAt(index.intValue(), value);
  }

  @Override
  public Array insert(int index, Sequence value) {
    modify(index, value, index == rtx.getChildCount() ? Op.Append : Op.Insert);
    return this;
  }

  @Override
  public Array insert(IntNumeric index, Sequence value) {
    insert(index.intValue(), value);
    return this;
  }

  @Override
  public Array remove(IntNumeric index) {
    return remove(index.intValue());
  }

  @Override
  public Array remove(int index) {
    final JsonNodeTrx trx = getReadWriteTrx();
    moveToIndex(index, trx);

    trx.remove();

    return this;
  }

  @Override
  public T getNext() {
    moveRtx();

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  private T moveTemporalAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis) {
    if (axis.hasNext()) {
      final var rtx = axis.next();
      return createInstance(rtx, collection);
    }

    return null;
  }

  protected abstract T createInstance(JsonNodeReadOnlyTrx rtx, JsonDBCollection collection);

  @Override
  public T getPrevious() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis =
        new PreviousAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public T getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new FirstAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public T getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(rtx.getResourceManager(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public boolean isNextOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() - 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isPreviousOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() + 1 == this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() > this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final T other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() - 1 >= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() < this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final T other) {
    moveRtx();

    if (this == other)
      return true;

    if (!(other instanceof T))
      return false;

    return other.getTrx().getRevisionNumber() <= this.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final T other) {
    moveRtx();

    if (!(other instanceof T))
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    return otherTrx.getResourceManager().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final T other) {
    moveRtx();

    if (!(other instanceof T))
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  protected final void moveRtx() {
    rtx.moveTo(nodeKey);
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

    if (values == null) {
      values = getValues();
    }

    return values;
  }

  private List<Sequence> getValues() {
    final var values = new ArrayList<Sequence>();

    final var axis = new ChildAxis(rtx);
    axis.forEach(nodeKey -> values.add(jsonItemFactory.getSequence(rtx, collection)));

    return values;
  }

  private Sequence getSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    moveRtx();

    final var axis = new ChildAxis(rtx);

    for (int i = 0; i < index && axis.hasNext(); i++) {
      axis.next();
    }

    if (axis.hasNext()) {
      axis.next();

      return jsonItemFactory.getSequence(rtx, collection);
    }

    return null;
  }

  @Override
  public Sequence at(final IntNumeric numericIndex) {
    if (values == null) {
      return getSequenceAtIndex(rtx, numericIndex.intValue());
    }
    return values.get(numericIndex.intValue());
  }

  @Override
  public Sequence at(final int index) {
    if (values == null) {
      return getSequenceAtIndex(rtx, index);
    }
    return values.get(index);
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
