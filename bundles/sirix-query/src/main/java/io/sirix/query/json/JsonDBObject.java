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
import io.sirix.settings.Fixed;
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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
   * Field values memoized on first successful lookup. Lazily allocated: a scan creates one
   * {@link JsonDBObject} per record and reads each field once, so eagerly building this map
   * allocated it and threw it away for every record.
   */
  private Map<QNm, Sequence> fields;

  /**
   * Path-summary match results, keyed by path-class record AND field name.
   *
   * <p>The field name is part of the key because the cached {@link BitSet} comes from
   * {@code PathSummaryReader.match(field, level)}, which depends on BOTH. Keying by PCR alone let
   * the FIRST field looked up on an object decide the answer for every later field on it: look up
   * a missing field first and its empty match was cached, after which every existing field was
   * reported missing too — {@code ($d.nope, $d.title)} returned the empty sequence while
   * {@code ($d.title, $d.nope)} was correct.
   */
  private Map<Long, Map<QNm, BitSet>> filterMap;

  /** {@link #firstChildKey} value meaning "re-read it from the cursor". */
  private static final long FIRST_CHILD_UNKNOWN = -2L;

  /**
   * This object's first-child key, captured at construction.
   *
   * <p>The constructor runs with the cursor already ON this node, so reading the key there is
   * free. A field lookup can then jump straight to the first child instead of re-anchoring at the
   * object and asking the cursor for the same key again -- {@code moveToFirstChild()} is exactly
   * {@code moveTo(getFirstChildKey())}, so the pair cost two full singleton binds (page lookup,
   * slot lookup, kind decode, flyweight rebind) to reach a node key this object already knew.
   *
   * <p>A scan does one field lookup per record, so that is one of roughly three moves per record
   * removed; {@code moveRtx} measured 73.8% inclusive of a warm filter scan.
   *
   * <p>Captured ONLY for a read-only transaction, which sees a fixed revision, so the key cannot
   * go stale under it. A write transaction leaves this {@link #FIRST_CHILD_UNKNOWN} and always
   * re-reads the current first child, because the object can be mutated through a DIFFERENT
   * {@link JsonDBObject} instance or through the write transaction directly -- neither of which
   * {@link #clearMemo()} can observe. A deleted first child would fail the {@code moveTo} and fall
   * back safely, but a field INSERTED before it still resolves and would silently shift the walk
   * past the new first field: a wrong answer with no error. This class is public API, so that is
   * reachable outside XQuery even though XUST0001 forbids a read nested in an updating expression.
   *
   * <p>Also reset by every mutating method, which covers mutations through this instance.
   */
  private long firstChildKey;

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
    // Read-only transactions only -- see the field's javadoc for why a writer must not cache this.
    firstChildKey = this.rtx instanceof JsonNodeTrx ? FIRST_CHILD_UNKNOWN : this.rtx.getFirstChildKey();
    jsonItemFactory = JsonItemFactory.INSTANCE;
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

  /**
   * The memoizing field map, created on first use.
   *
   * @return the map, never {@code null}
   */
  private Map<QNm, Sequence> fields() {
    Map<QNm, Sequence> memo = fields;
    if (memo == null) {
      memo = new HashMap<>(4);
      fields = memo;
    }
    return memo;
  }

  /**
   * The path-summary match cache, created on first use.
   *
   * @return the map, never {@code null}
   */
  private Map<Long, Map<QNm, BitSet>> filterMap() {
    Map<Long, Map<QNm, BitSet>> map = filterMap;
    if (map == null) {
      map = new HashMap<>(2);
      filterMap = map;
    }
    return map;
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

    return this.getTrx().getRevisionNumber() == other.getTrx().getRevisionNumber() + 1;
  }

  @Override
  public boolean isPreviousOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return this.getTrx().getRevisionNumber() + 1 == other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return this.getTrx().getRevisionNumber() > other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final JsonDBObject other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() >= other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final JsonDBObject other) {
    moveRtx();

    if (this == other || other == null)
      return false;

    return this.getTrx().getRevisionNumber() < other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final JsonDBObject other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() <= other.getTrx().getRevisionNumber();
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
      clearMemo();
      fields().put(field, value);
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
        clearMemo();
        // iter#32 Phase 4: legacy OBJECT_KEY has been deleted; the cursor sits on the fused
        // OBJECT_NAMED_* record itself, which IS the value (inline primitive for leaf kinds
        // or the OBJECT/ARRAY pair for the structural kinds). JsonItemFactory dispatches on
        // the fused kind — descending into the first child here would collapse a structural
        // value to its first inner field.
        fields().put(newFieldName, jsonItemFactory.getSequence(trx, collection));
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

    clearMemo();
    fields().put(field, value);

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
        // Drop the memo: without this a get() for the field just deleted keeps answering with
        // its old value. Pre-existing — the map was never invalidated here either.
        clearMemo();
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
    // No moveRtx() here: lookupField positions the cursor itself, and for the common case it can
    // enter directly at the first child without visiting this object at all.
    final Map<QNm, Sequence> memo = fields;
    if (memo != null) {
      final Sequence cached = memo.get(field);
      if (cached != null) {
        return cached;
      }
    }

    final Sequence value = lookupField(field);
    if (value != null) {
      fields().put(field, value);
    }
    return value;
  }

  /**
   * Drop every memoized field value. Any mutation invalidates them: without this a read after a
   * write keeps answering with the pre-write value.
   */
  private void clearMemo() {
    if (fields != null) {
      fields.clear();
    }
    // Inserting or removing a field can change which node is first, so the cached key must go too.
    firstChildKey = FIRST_CHILD_UNKNOWN;
  }

  /**
   * Find {@code field} among this object's children without allocating.
   *
   * <p>Replaces {@code new FilterAxis<>(new ChildAxis(rtx), new JsonNameFilter(rtx, field))}, which
   * cost three objects plus a capturing lambda on every field access. A scan binds a FRESH
   * {@link JsonDBObject} per record, so those allocations were paid per record and the memoizing
   * map they filled was discarded immediately. Allocation profiling of a 290k-record filter scan
   * attributed ~13% of all allocations to this path.
   *
   * <p>Matching compares NAME KEYS rather than {@link QNm} objects: the filter's
   * {@code name.equals(rtx.getName())} materialized a QNm and compared strings for every child
   * visited, i.e. ~4.5 times per record on this corpus. The name key is resolved once here and the
   * walk then compares ints.
   *
   * <p>Cursor semantics match the axis exactly: on a match the cursor is left ON the matching
   * node (which under record fusion IS the value), and on a miss it is reset to this object's node
   * key, mirroring {@code AbstractAxis.resetToStartKey()}.
   *
   * @param field the field name to look up
   * @return the field's value, or {@code null} when this object has no such field
   */
  private Sequence lookupField(final QNm field) {
    // The path-summary "this field cannot exist here, skip the walk" short circuit used to run
    // here and is gone.
    //
    // It was UNSOUND when removed: the summary collapsed field names colliding in String.hashCode
    // into ONE node, so PathSummaryReader.match returned the empty set for the name that lost the
    // collision and the guard reported an EXISTING field as missing -- on {"Aa":1,"BB":2} (both
    // hash 2112) the summary held a node for Aa and none for BB, and $obj.BB gave the empty
    // sequence though the record was right there. main only looked correct because its match cache
    // was keyed by path-class record alone and reused Aa's non-empty answer for BB.
    //
    // That summary defect is FIXED (a node per name now), so the guard would be correct again. It
    // is still not worth reinstating: it costs a moveRtx and a path-class lookup on EVERY field
    // access to save a sibling walk only on a MISS against an object wider than CHILD_THRESHOLD,
    // and the walk below is far cheaper than when the guard was introduced -- no axis objects, no
    // QNm materialization, and entry straight at the first child.

    // Compare the dictionary's raw UTF-8 name bytes, NOT keyForName.
    //
    // keyForName is NamePageHash.generateHashForString, i.e. a bare String.hashCode(). The key a
    // record actually stores is the DICTIONARY's, which probes past collisions, so for
    // {"Aa":1,"BB":2} -- both hash 2112 -- "Aa" stores 2112 and "BB" stores 2113. Comparing
    // getNameKey() against the hash therefore matched "Aa"'s record for a lookup of "BB" and
    // returned Aa's VALUE, while "BB"'s own record became unreachable. Measured: $obj.BB gave 1
    // instead of 2. Name bytes are per-name and cannot collide.
    //
    // getNameBytes hands back the dictionary's own array with no String or QNm materialized, so the
    // comparison stays allocation-free per child; only the needle is encoded, once per lookup.
    //
    // And in practice not even that: `wantedName` never escapes this method, so C2 scalar-replaces
    // it. Measured with JMH's gc profiler on a 290,184-record filter scan, replacing this with a
    // hand-rolled in-place UTF-8 comparison that cannot allocate at all moved gc.alloc.rate.norm
    // from 18,631,304 to 18,631,290 B/op -- 14 bytes, against the ~7 MB the array would have cost
    // had it been real. Arrays.equals is also an intrinsic, where a char-by-char loop is scalar.
    final byte[] wantedName = field.getLocalName().getBytes(StandardCharsets.UTF_8);

    if (enterFirstChild()) {
      do {
        // isObjectKey first, exactly as JsonNameFilter did: only object-key records carry a field
        // name, and a non-key child's name is unrelated.
        if (rtx.isObjectKey() && Arrays.equals(rtx.getNameBytes(), wantedName)) {
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
      } while (rtx.moveToRightSibling());
    }

    moveRtx();
    return null;
  }

  /**
   * Position the cursor on this object's first child.
   *
   * <p>Uses the key captured at construction, so the usual path is a single {@code moveTo} rather
   * than re-anchoring at the object and then moving to the child it names. Falls back to the
   * cursor when the cached key was invalidated by a mutation.
   *
   * @return {@code false} when this object has no children, cursor left on the object
   */
  private boolean enterFirstChild() {
    final long first = firstChildKey;
    if (first == FIRST_CHILD_UNKNOWN) {
      moveRtx();
      return rtx.moveToFirstChild();
    }
    if (first == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      moveRtx();
      return false;
    }
    if (rtx.moveTo(first)) {
      return true;
    }
    // Cached key no longer resolves — fall back rather than reporting the field missing.
    firstChildKey = FIRST_CHILD_UNKNOWN;
    moveRtx();
    return rtx.moveToFirstChild();
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
    // computeIfAbsent's lambda captures nothing, so it is a constant and costs no allocation.
    final Map<QNm, BitSet> matchesByField = filterMap().computeIfAbsent(pcr, unused -> new HashMap<>());
    BitSet matches = matchesByField.get(field);
    if (matches == null) {
      try (final PathSummaryReader reader = rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber())) {
        if (pcr != 0) {
          reader.moveTo(pcr);
        }
        final int level = reader.getLevel() + 1;
        matches = reader.match(field, level);
        matchesByField.put(field, matches);
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
