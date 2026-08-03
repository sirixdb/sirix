package io.sirix.query.json;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.type.ArrayType;
import io.brackit.query.jdm.type.ItemType;
import io.brackit.query.jsonitem.array.AbstractArray;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.temporal.FirstAxis;
import io.sirix.axis.temporal.LastAxis;
import io.sirix.axis.temporal.NextAxis;
import io.sirix.axis.temporal.PreviousAxis;
import io.sirix.query.StructuredDBItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
   * Cached values. Populated only by {@link #values()} and by RANDOM access through
   * {@link #at(int)}; a purely sequential scan never builds it.
   */
  private List<Sequence> values;

  /** {@link #childCount} value meaning "ask the cursor". */
  private static final long CHILD_COUNT_UNKNOWN = -1L;

  /**
   * This array's element count, captured at construction.
   *
   * <p>brackit's array unbox calls {@code len()} once per element, and answering that from the
   * cursor means {@code moveRtx()} — a jump back to the array node, which lives on a different page
   * from the element being read. That bounced the cursor page N -> array page -> page N for every
   * element and defeated the same-page fast path. It used to be avoided by materializing the whole
   * element list and reading its size; capturing the count here removes the need for either.
   *
   * <p>Read-only transactions only, for the reason {@code JsonDBObject#firstChildKey} records: a
   * writer can change the child count through a different item or through the transaction directly,
   * and this object would not see it. A writer leaves this {@link #CHILD_COUNT_UNKNOWN}.
   */
  private long childCount;

  /**
   * Index of the element returned by the last SEQUENTIAL {@link #at(int)} call, or -1.
   *
   * <p>Together with {@link #seqNodeKey} this turns {@code for $x in $doc[]} into a sibling walk:
   * re-anchor on the previously returned element and hop right, instead of materializing every
   * element up front. The re-anchor is needed because the cursor is shared — evaluating
   * {@code $x.field} moves it — and lands on the page the element already lives on, so it takes the
   * cursor's own same-page fast path.
   */
  /**
   * Overlaps record-page decoding with this walk; {@code null} when prefetching is disabled or the
   * resource is too small to be worth it. Created on the first sequential step and closed when the
   * walk ends, so a query that never scans never starts a worker.
   */
  private RecordPagePrefetcher prefetcher;

  /** Whether {@link #prefetcher} has been considered yet (it may legitimately resolve to null). */
  private boolean prefetcherInitialized;

  private int seqIndex = -1;

  /** Node key of the element at {@link #seqIndex}; meaningless when {@code seqIndex < 0}. */
  private long seqNodeKey;

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
    // See the field javadoc: only a read-only cursor sees a fixed revision, so only there can the
    // count be trusted for the lifetime of this item.
    childCount = rtx instanceof JsonNodeTrx ? CHILD_COUNT_UNKNOWN : rtx.getChildCount();
  }

  @Override
  public JsonResourceSession getResourceSession() {
    return rtx.getResourceSession();
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

  protected final JsonItemFactory getJsonItemFactory() {
    return jsonItemFactory;
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

    // The element list is now one short, and len()/length() answer from it.
    values = null;
    invalidateScanState();

    return this;
  }

  private void modify(int index, Sequence value, final Op op) {
    final JsonNodeTrx trx = getReadWriteTrx();
    // Do NOT close the trx on a bounds error: getReadWriteTrx may return the session's SHARED
    // write trx, so closing it (a) threw "Must commit/rollback first" when it had pending edits,
    // masking the real error, and (b) emptied session.getNodeTrx() so every OTHER pending update
    // in the same query was silently lost. Also reject a negative index — it passed `> childCount`
    // and then operated on index 0 (silent wrong target). Mirrors remove(int).
    if (index < 0 || index > trx.getChildCount()) {
      throw new QueryException(new QNm("Index " + index + " is out of range (" + trx.getChildCount() + ")."));
    }

    moveToIndex(index, trx);

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

    jsonItemSequence.insert(value, trx, nodeKey);

    values = null;
    invalidateScanState();
  }

  private void moveToIndex(int index, JsonNodeTrx trx) {
    // must have children

    trx.moveToFirstChild();

    for (int i = 1; i <= index; i++) {
      trx.moveToRightSibling();
    }
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
  public Array replaceAt(IntNumeric index, Sequence value) {
    return replaceAt(index.intValue(), value);
  }

  @Override
  public Array insert(int index, Sequence value) {
    modify(index, value, Op.Insert);
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

    if (index >= trx.getChildCount()) {
      throw new QueryException(new QNm("Index " + index + " is out of bounds (" + trx.getChildCount() + ")."));
    }

    moveToIndex(index, trx);

    trx.remove();

    // Drop the memo: it still holds the removed element, and len()/length() read its size.
    values = null;
    invalidateScanState();

    return this;
  }

  @Override
  public T getNext() {
    moveRtx();

    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new NextAxis<>(rtx.getResourceSession(), rtx);
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
        new PreviousAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public T getFirst() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new FirstAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public T getLast() {
    moveRtx();
    final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis = new LastAxis<>(rtx.getResourceSession(), rtx);
    return moveTemporalAxis(axis);
  }

  @Override
  public boolean isNextOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() == other.getTrx().getRevisionNumber() + 1;
  }

  @Override
  public boolean isPreviousOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() + 1 == other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() > other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isFutureOrSelfOf(final T other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() >= other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOf(final T other) {
    moveRtx();

    if (this == other)
      return false;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() < other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isEarlierOrSelfOf(final T other) {
    moveRtx();

    if (this == other)
      return true;

    if (other == null)
      return false;

    return this.getTrx().getRevisionNumber() <= other.getTrx().getRevisionNumber();
  }

  @Override
  public boolean isLastOf(final T other) {
    moveRtx();

    if (other == null)
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    return otherTrx.getResourceSession().getMostRecentRevisionNumber() == otherTrx.getRevisionNumber();
  }

  @Override
  public boolean isFirstOf(final T other) {
    moveRtx();

    if (other == null)
      return false;

    final NodeReadOnlyTrx otherTrx = other.getTrx();

    // Revision 0 is just the bootstrap revision and not accessed over here.
    return otherTrx.getRevisionNumber() == 1;
  }

  /**
   * Forget the sequential anchor and the cached element count. Any structural change invalidates
   * both: the anchor's right sibling may no longer be the next element, and the count has moved.
   */
  private void invalidateScanState() {
    seqIndex = -1;
    childCount = CHILD_COUNT_UNKNOWN;
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
    moveRtx();
    final var values = new ArrayList<Sequence>();

    // Single sequential scan via the installable seam: with an io_uring prefetch factory installed,
    // this overlaps cold page reads with item construction (see JsonScanAxisFactory).
    JsonScanAxisFactory.forEachChild(rtx, nodeKey -> values.add(jsonItemFactory.getSequence(rtx, collection)));

    return values;
  }

  private Sequence getSequenceAtIndex(final JsonNodeReadOnlyTrx rtx, final int index) {
    moveRtx();

    final var axis = new ChildAxis(rtx);

    for (int i = 0; i < index && axis.hasNext(); i++) {
      axis.nextLong();
    }

    if (axis.hasNext()) {
      axis.nextLong();

      return jsonItemFactory.getSequence(rtx, collection);
    }

    return null;
  }

  @Override
  public Sequence at(final IntNumeric numericIndex) {
    return at(numericIndex.intValue());
  }

  @Override
  public Sequence at(final int index) {
    if (index < 0) {
      return null;
    }
    final List<Sequence> materialized = values;
    if (materialized != null) {
      return index >= materialized.size()
          ? null
          : materialized.get(index);
    }

    // SEQUENTIAL fast path. brackit's array unbox (jn:doc()[]) drives the for-loop via
    // at(0..n-1), which is a sibling walk, not random access. Serving it as one used to mean
    // materializing every element into a List first, because each at(i) otherwise walked a fresh
    // ChildAxis to index i -- O(n^2). Materializing is O(n) but it allocates an item per element
    // AND retains all of them: on the 3,482,208-record corpus that is a 3.4 M-entry ArrayList kept
    // live for the whole query, which is also why the scan collapses once the working set stops
    // fitting in memory.
    //
    // Walking instead costs a re-anchor plus a sibling hop per element, both landing on the page
    // the element already occupies.
    if (index == seqIndex + 1 && seqIndex >= 0) {
      if (rtx.moveTo(seqNodeKey) && rtx.moveToRightSibling()) {
        seqIndex = index;
        seqNodeKey = rtx.getNodeKey();
        // Decoding the pages ahead is what makes a cold or buffer-pressured scan use more than one
        // core; see RecordPagePrefetcher. It only warms a cache, so it cannot affect the result.
        startPrefetchOnce();
        if (prefetcher != null) {
          prefetcher.advanceTo(seqNodeKey);
        }
        return jsonItemFactory.getSequence(rtx, collection);
      }
      // Ran off the end, or the anchor is gone: fall through rather than guess.
      seqIndex = -1;
      closePrefetcher();
    } else if (index == 0) {
      moveRtx();
      if (rtx.moveToFirstChild()) {
        seqIndex = 0;
        seqNodeKey = rtx.getNodeKey();
        return jsonItemFactory.getSequence(rtx, collection);
      }
      return null;
    }

    // RANDOM access (or a sequential walk that lost its anchor): materialize once and answer from
    // the list from here on, which keeps the old O(n) guarantee for index jumping.
    closePrefetcher();
    final List<Sequence> built = getValues();
    values = built;
    return index >= built.size()
        ? null
        : built.get(index);
  }

  /**
   * Starts the read-ahead on the first sequential step, once. Deferred to the first step rather
   * than done in the constructor because most arrays are never scanned end to end, and a
   * prefetcher that is created and immediately dropped costs a transaction for nothing.
   */
  private void startPrefetchOnce() {
    if (!prefetcherInitialized) {
      prefetcherInitialized = true;
      prefetcher = RecordPagePrefetcher.createOrNull(rtx);
    }
  }

  /** Ends the read-ahead and releases its worker transactions. Idempotent. */
  private void closePrefetcher() {
    final RecordPagePrefetcher running = prefetcher;
    if (running != null) {
      prefetcher = null;
      running.close();
    }
  }

  @Override
  public IntNumeric length() {
    final var materialized = values;
    if (materialized != null) {
      return new Int64(materialized.size());
    }
    if (childCount != CHILD_COUNT_UNKNOWN) {
      return new Int64(childCount);
    }
    moveRtx();
    return new Int64(rtx.getChildCount());
  }

  @Override
  public int len() {
    // Answer from the materialized element list when there is one, WITHOUT moving the cursor.
    //
    // brackit's array unbox drives the loop with len() once per element, and moveRtx() re-anchors
    // at the array node -- which lives on page 0 while the elements live on their own pages. That
    // made every iteration bounce page N -> page 0 -> page N, so the cursor's same-page fast path
    // in AbstractNodeReadOnlyTrx.moveToSingleton could never fire: measured at 290,184 records it
    // hit 319 times out of 580,369 moves (0.1%), and each of the 580,050 slow moves allocated the
    // full wrapper set (PageGuard, RecordPage, SlotLocation, PageReferenceToPage, MemorySegment
    // slice) -- 78.7% of ALL allocations in a filter scan.
    //
    // Reading the size off the cached list is exact: at() materializes that list from one child
    // scan and every element of the array is in it.
    final var materialized = values;
    if (materialized != null) {
      return materialized.size();
    }
    if (childCount != CHILD_COUNT_UNKNOWN) {
      return (int) childCount;
    }
    moveRtx();
    return (int) rtx.getChildCount();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (other == null || getClass() != other.getClass())
      return false;
    AbstractJsonDBArray<?> that = (AbstractJsonDBArray<?>) other;
    return nodeKey == that.nodeKey;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKey);
  }
}
