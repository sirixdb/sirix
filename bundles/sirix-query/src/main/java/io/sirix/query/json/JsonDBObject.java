package io.sirix.query.json;

import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.json.JsonNameFilter;
import io.sirix.axis.temporal.PrefetchedAllTimeAxis;
import io.sirix.axis.temporal.FirstAxis;
import io.sirix.axis.temporal.PrefetchedFutureAxis;
import io.sirix.axis.temporal.LastAxis;
import io.sirix.axis.temporal.NextAxis;
import io.sirix.axis.temporal.PrefetchedPastAxis;
import io.sirix.axis.temporal.PreviousAxis;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.stream.json.SirixJsonStream;
import io.sirix.query.stream.json.TemporalSirixJsonObjectStream;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Dec;
import io.brackit.query.atomic.Flt;
import io.brackit.query.atomic.Int;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.AbstractItem;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.type.ArrayType;
import io.brackit.query.jdm.type.ItemType;
import io.brackit.query.jdm.type.ObjectType;
import io.brackit.query.util.ExprUtil;

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
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
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
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new PrefetchedPastAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getFuture(final boolean includeSelf) {
    moveRtx();
    final IncludeSelf include = includeSelf
        ? IncludeSelf.YES
        : IncludeSelf.NO;
    return new TemporalSirixJsonObjectStream(new PrefetchedFutureAxis<>(rtx.getResourceSession(), rtx, include), collection);
  }

  @Override
  public Stream<JsonDBObject> getAllTimes() {
    moveRtx();
    return new TemporalSirixJsonObjectStream(new PrefetchedAllTimeAxis<>(rtx.getResourceSession(), rtx), collection);
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
    final JsonResourceSession resourceSession = rtx.getResourceSession();
    final var trx = resourceSession.getNodeTrx().orElseGet(resourceSession::beginNodeTrx);

    // Register the session with the store so it can be cleaned up on close
    final var store = collection.getJsonDBStore();
    if (store instanceof BasicJsonDBStore basicStore) {
      basicStore.registerWriteSession(resourceSession);
    }

    // If the read transaction is from an older revision than the write transaction,
    // revert the write transaction to match the source revision.
    // This enables editing historical versions and creating new branches.
    final int sourceRevision = rtx.getRevisionNumber();
    final int mostRecentRevision = resourceSession.getMostRecentRevisionNumber();
    if (sourceRevision < mostRecentRevision) {
      trx.revertTo(sourceRevision);
    }

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

    // iter#32 Phase 4: legacy OBJECT_KEY has been deleted; after findField() the cursor sits
    // on a fused OBJECT_NAMED_* record that IS the field. For LEAF kinds (BOOLEAN/NUMBER/
    // STRING/NULL) the inline primitive can be updated in-place via the setters. For the
    // STRUCTURAL kinds (OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY) the existing value is a
    // container — every replacement is a type-mismatch and must go through
    // replaceObjectRecordValue (no descent: descending into OBJECT_NAMED_OBJECT would land
    // on a FIELD of the inner object, not on a primitive value slot).
    final var currentKind = trx.getKind();

    // Check if we can do an in-place update (same type) to preserve node identity
    if (currentKind == NodeKind.OBJECT_NAMED_STRING && value instanceof Str str) {
      trx.setStringValue(str.stringValue());
      return;
    }
    if (currentKind == NodeKind.OBJECT_NAMED_NUMBER && value instanceof Numeric) {
      setNumericValue(trx, value);
      return;
    }
    if (currentKind == NodeKind.OBJECT_NAMED_BOOLEAN && value instanceof Bool bool) {
      trx.setBooleanValue(bool.booleanValue());
      return;
    }
    if (currentKind == NodeKind.OBJECT_NAMED_NULL && value instanceof Null) {
      // Null to null - no change needed
      return;
    }

    if (value instanceof Array) {
      trx.replaceObjectRecordValue(new ArrayValue());
      insertSubtree(value, trx);
    } else if (value instanceof Object) {
      trx.replaceObjectRecordValue(new ObjectValue());
      insertSubtree(value, trx);
    } else if (value instanceof Str str) {
      trx.replaceObjectRecordValue(new StringValue(str.stringValue()));
    } else if (value instanceof Null) {
      trx.replaceObjectRecordValue(new NullValue());
    } else if (value instanceof Bool bool) {
      trx.replaceObjectRecordValue(new BooleanValue(bool.booleanValue()));
    } else if (value instanceof Numeric) {
      switch (value) {
        case Int anInt -> trx.replaceObjectRecordValue(new NumberValue(anInt.intValue()));
        case Int32 int32 -> trx.replaceObjectRecordValue(new NumberValue(int32.intValue()));
        case Int64 int64 -> trx.replaceObjectRecordValue(new NumberValue(int64.longValue()));
        case Flt flt -> trx.replaceObjectRecordValue(new NumberValue(flt.floatValue()));
        case Dbl dbl -> trx.replaceObjectRecordValue(new NumberValue(dbl.doubleValue()));
        case Dec dec -> trx.replaceObjectRecordValue(new NumberValue(dec.decimalValue()));
        default -> {
        }
      }
    }
  }

  private void setNumericValue(JsonNodeTrx trx, Sequence value) {
    switch (value) {
      case Int anInt -> trx.setNumberValue(anInt.intValue());
      case Int32 int32 -> trx.setNumberValue(int32.intValue());
      case Int64 int64 -> trx.setNumberValue(int64.longValue());
      case Flt flt -> trx.setNumberValue(flt.floatValue());
      case Dbl dbl -> trx.setNumberValue(dbl.doubleValue());
      case Dec dec -> trx.setNumberValue(dec.decimalValue());
      default -> {
      }
    }
  }

  private void insertSubtree(Sequence value, JsonNodeTrx trx) {
    final Item item = ExprUtil.asItem(value);
    // Use Commit.NO to prevent auto-commit after insertion.
    // Auto-commit would cause subsequent getReadWriteTrx() calls to see
    // sourceRevision < mostRecentRevision, triggering revertTo() which
    // undoes the modifications.
    trx.insertSubtreeAsLastChild(item, JsonNodeTrx.Commit.NO);
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
        // iter#32 Phase 4: legacy OBJECT_KEY has been deleted; the cursor sits on the fused
        // OBJECT_NAMED_* record itself, which IS the value (inline primitive for leaf kinds
        // or the OBJECT/ARRAY pair for the structural kinds). JsonItemFactory dispatches on
        // the fused kind — descending into the first child here would collapse a structural
        // value to its first inner field.
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
            default -> throw new IllegalStateException("Unexpected value: " + value);
          }
        }
        case Bool ignored2 -> trx.insertObjectRecordAsLastChild(fieldName, new BooleanValue(value.booleanValue()));
        default -> throw new IllegalStateException("Unexpected value: " + value);
      }
    } else {
      final Item item = ExprUtil.asItem(value);

      if (item.itemType() == ArrayType.ARRAY) {
        trx.insertObjectRecordAsLastChild(fieldName, new ArrayValue());
      } else if (item.itemType() == ObjectType.OBJECT) {
        trx.insertObjectRecordAsLastChild(fieldName, new ObjectValue());
      }
      // The fused OBJECT_NAMED_OBJECT/ARRAY IS the container — cursor already lands on it after
      // insertObjectRecordAsLastChild. Insert inner contents directly as first child.
      trx.insertSubtreeAsFirstChild(item, JsonNodeTrx.Commit.NO, JsonNodeTrx.CheckParentNode.YES,
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
        // iter#32 Phase 4: legacy OBJECT_KEY has been deleted; the cursor lands on a fused
        // OBJECT_NAMED_* record which carries either the inline primitive value (LEAF kinds)
        // or the structural-value role (OBJECT_NAMED_OBJECT / OBJECT_NAMED_ARRAY = the inner
        // OBJECT/ARRAY itself). In every case the cursor IS the value — JsonItemFactory
        // dispatches on the fused kind and returns the right typed item (atomic for primitive
        // leaves, JsonDBObject/JsonDBArray for the structural pair). Do NOT descend into the
        // first child here — that would unwrap a structural value to its first inner field
        // (the historical "nested object collapses to its first primitive" bug).
        return jsonItemFactory.getSequence(rtx, collection);
      }

      return null;
    });
  }

  private boolean hasNoMatchingPathNode(QNm field) {
    // iter#32 P2: under fusion the cursor may sit on OBJECT_NAMED_OBJECT (kind 52) — that fused
    // record carries the OBJECT_KEY-level pathNodeKey ON ITSELF (the legacy two-level pattern
    // had a pathless inner OBJECT whose parent was OBJECT_KEY). Use OUR pathNodeKey directly so
    // pathSummary.match runs at the right pivot. For legacy bare OBJECT, the original behaviour
    // (move to parent OBJECT_KEY for its pathNodeKey) still applies.
    final long pcr;
    final NodeKind kind = rtx.getKind();
    if (kind == NodeKind.OBJECT_NAMED_OBJECT) {
      pcr = rtx.getPathNodeKey();
    } else {
      rtx.moveToParent();
      pcr = rtx.isDocumentRoot()
          ? 0
          : rtx.getPathNodeKey();
      rtx.moveTo(nodeKey);
    }
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

      // iter#32 Phase 4 — same rule as {@link #get(QNm)} above: legacy OBJECT_KEY has been
      // deleted, the cursor sits on a fused OBJECT_NAMED_* record that IS the value (inline
      // primitive for leaf kinds or the OBJECT/ARRAY pair itself for the structural kinds).
      // JsonItemFactory handles the dispatch — descending into the first child here would
      // collapse a structural value to its first inner field.
      return jsonItemFactory.getSequence(rtx, collection);
    }

    return null;
  }

  @Override
  public Sequence value(final int index) {
    if (index < 0) {
      throw new IllegalArgumentException();
    }

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
    if (numericIndex.intValue() < 0) {
      throw new IllegalArgumentException();
    }

    moveRtx();

    return getNameAtIndex(rtx, numericIndex.intValue());
  }

  @Override
  public QNm name(final int index) {
    if (index < 0) {
      throw new IllegalArgumentException();
    }

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
