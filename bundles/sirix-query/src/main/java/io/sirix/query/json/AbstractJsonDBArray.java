package io.sirix.query.json;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.SplittableMembers;
import io.brackit.query.jdm.type.ArrayType;
import io.brackit.query.jdm.type.ItemType;
import io.brackit.query.jsonitem.array.AbstractArray;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.temporal.FirstAxis;
import io.sirix.axis.temporal.LastAxis;
import io.sirix.axis.temporal.NextAxis;
import io.sirix.axis.temporal.PreviousAxis;
import io.sirix.query.StructuredDBItem;
import io.sirix.settings.Constants;

import java.lang.ref.Cleaner;
import java.lang.ref.Cleaner.Cleanable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public abstract class AbstractJsonDBArray<T extends AbstractJsonDBArray<T>> extends AbstractArray
    implements TemporalJsonDBItem<T>, JsonDBItem, Array, StructuredDBItem<JsonNodeReadOnlyTrx>, SplittableMembers {
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
   * Cached values. Populated only by an explicit {@link #values()} — {@link #at(int)} never builds it
   * at any index or access pattern, because the size of this list is the size of the array and it
   * lives as long as the query does.
   */
  private List<Sequence> values;

  /** {@link #childCount} value meaning "ask the cursor". */
  private static final long CHILD_COUNT_UNKNOWN = -1L;

  /**
   * This array's element count, captured at construction.
   *
   * <p>
   * brackit's array unbox calls {@code len()} once per element, and answering that from the cursor
   * means {@code moveRtx()} — a jump back to the array node, which lives on a different page from the
   * element being read. That bounced the cursor page N -> array page -> page N for every element and
   * defeated the same-page fast path. It used to be avoided by materializing the whole element list
   * and reading its size; capturing the count here removes the need for either.
   *
   * <p>
   * Read-only transactions only, for the reason {@code JsonDBObject#firstChildKey} records: a writer
   * can change the child count through a different item or through the transaction directly, and this
   * object would not see it. A writer leaves this {@link #CHILD_COUNT_UNKNOWN}.
   */
  private long childCount;

  /**
   * Overlaps record-page decoding with this walk; {@code null} when prefetching is disabled or the
   * resource is too small to be worth it. Created on the first sequential step and closed when the
   * walk ends, so a query that never scans never starts a worker.
   */
  private RecordPagePrefetcher prefetcher;

  /**
   * Deregistration handle for the GC-triggered fallback close of {@link #prefetcher}; {@code null}
   * when no prefetcher is running.
   */
  private Cleanable prefetcherCleanup;

  /** Whether {@link #prefetcher} has been considered yet (it may legitimately resolve to null). */
  private boolean prefetcherInitialized;

  /**
   * Anchor slot A: index of an element {@link #at(int)} has served, or -1 when the slot is empty.
   *
   * <p>
   * Together with {@link #anchorKeyA} this turns {@code for $x in $doc[]} into a sibling walk:
   * re-anchor on a previously returned element and hop right, instead of walking a fresh child axis
   * per index. The re-anchor is needed because the cursor is shared — evaluating {@code $x.field}
   * moves it — and lands on the page the element already lives on, so it takes the cursor's own
   * same-page fast path.
   */
  private int anchorIndexA = -1;

  /** Node key of the element at {@link #anchorIndexA}; meaningless when that is negative. */
  private long anchorKeyA;

  /**
   * Anchor slot B, which exists because ONE anchor is not enough: two consumers can walk the same
   * array item at once, and with a single slot each one's hop destroys the other's place.
   *
   * <p>
   * That is not hypothetical. brackit decides whether a spilled tuple column is copied or held by
   * reference by RENDERING it under a 64 KiB cap ({@code TupleSerializer.serializeToJson}), and the
   * renderer walks an array with the same {@code len()}/{@code at(i)} protocol the unbox loop uses
   * ({@code StringSerializer}). So spilling a tuple that carries {@code $doc} runs a second walk from
   * index 0 across the very array the {@code for} loop is streaming, and abandons it a hundred-odd
   * elements in. With one slot the streaming walk came back to an anchor it did not set, fell into
   * the RANDOM branch below, and materialized every element of the array; measured on a 100 M-element
   * corpus that is 4.8 GB of items retained for the rest of the query. A second slot lets both walks
   * keep their place, and each stays a single sibling hop per element.
   *
   * <p>
   * Two, not more: two is what an interleaved pair costs, and a third walk still gets correct answers
   * from the positional re-anchor below — only more slowly. The slots are scalar fields rather than
   * an array because an array item is constructed per array node on a scan.
   */
  private int anchorIndexB = -1;

  /** Node key of the element at {@link #anchorIndexB}; meaningless when that is negative. */
  private long anchorKeyB;

  /**
   * Which slot served most recently. A miss evicts the OTHER one, so the walk that is making progress
   * keeps its anchor and a one-off probe cannot displace it.
   */
  private boolean lastServedB;

  /**
   * Whether {@link #prefetcher} follows slot B. Read-ahead belongs to ONE walk — the one that was
   * long enough to justify starting it — so the other slot's hops must not drag it backwards.
   */
  private boolean prefetchFollowsB;

  /**
   * Whether the cursor can see structural change, i.e. whether an anchor may become stale.
   *
   * <p>
   * An anchor is a bet that the node it names is still this array's child at that index. Under a
   * read-only cursor the bet cannot lose: the revision is fixed, so nothing can move a node out of
   * this array, and the sibling chain reachable from an anchor is by construction this array's
   * elements. Under a writer it can: an edit made through a DIFFERENT item bound to the same array,
   * or straight through the transaction, is invisible here — the same reason {@link #childCount} is
   * left unknown for a writer. A writer therefore discards every cached position and materialized
   * value list whenever its transaction's monotonic mutation sequence changes, then verifies an
   * anchor's parent before trusting it. A reader — which is every scan — pays neither check.
   */
  private final boolean cursorMayMutate;

  /** Writer mutation sequence against which all cached positional state is valid. */
  private long observedMutationSequence;

  private enum Op {
    Replace,

    Insert,

    Append
  }

  /**
   * Walk statistics.
   *
   * <p>
   * Every counter sits on a path a healthy scan never takes — a hop that lands touches none of them —
   * so measuring costs the walk nothing. They exist because the difference between "the scan walked"
   * and "the scan quietly fell back to something quadratic or unbounded" is invisible in a result and
   * enormous in cost, and a timing alone cannot tell them apart.
   */
  private static final LongAdder POSITIONAL_REANCHORS = new LongAdder();

  private static final LongAdder MATERIALIZATIONS = new LongAdder();

  /** Prints the state behind every re-anchor and materialization; costs a probe per event. */
  private static final boolean SCAN_DIAG = Boolean.getBoolean("sirix.jsonArray.scanDiag");

  /**
   * Number of {@link #at(int)} calls neither anchor could serve with a single hop, so the element had
   * to be located by walking. Every walk contributes one — its own first element — so the number to
   * read this against is the number of walks, not zero. Process-wide and monotonic; diagnostics and
   * tests only.
   */
  public static long positionalReanchors() {
    return POSITIONAL_REANCHORS.sum();
  }

  /**
   * Number of times an array's full element list was built and cached. {@link #at(int)} never does
   * this — only an explicit {@link #values()} does. Process-wide and monotonic; diagnostics and tests
   * only.
   */
  public static long materializations() {
    return MATERIALIZATIONS.sum();
  }

  /** Resets the counters. Tests only — there is no process-wide coordination. */
  public static void resetScanCounters() {
    POSITIONAL_REANCHORS.reset();
    MATERIALIZATIONS.reset();
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
    cursorMayMutate = rtx instanceof JsonNodeTrx;
    observedMutationSequence = cursorMayMutate
        ? ((JsonNodeTrx) rtx).getMutationSequence()
        : 0L;
    childCount = cursorMayMutate
        ? CHILD_COUNT_UNKNOWN
        : rtx.getChildCount();
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
    anchorIndexA = -1;
    anchorIndexB = -1;
    childCount = CHILD_COUNT_UNKNOWN;
    // A mutation ends the walk the read-ahead was serving; whatever it has in flight is for a
    // shape that no longer exists.
    closePrefetcher();
    if (cursorMayMutate) {
      observedMutationSequence = ((JsonNodeTrx) rtx).getMutationSequence();
    }
  }

  private void refreshMutableState() {
    if (!cursorMayMutate) {
      return;
    }
    final long currentSequence = ((JsonNodeTrx) rtx).getMutationSequence();
    if (currentSequence != observedMutationSequence) {
      values = null;
      invalidateScanState();
    }
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

  /**
   * Elements below which splitting is refused.
   *
   * <p>
   * A split costs a read transaction and a storage reader per piece, and a page-range scan inspects
   * every slot of every page it covers — including the ~14 non-element nodes per element on a typical
   * JSON corpus. Under a large array that trade is overwhelmingly worth it; under a small one it is
   * pure loss, and the serial sibling walk is simply better.
   */
  private static final long SPLIT_MIN_ELEMENTS = Long.getLong("sirix.morsel.minElements", 65_536L);

  /** Record pages a split must cover to be worth its transaction; keeps pieces from being trivial. */
  private static final long SPLIT_MIN_PAGES = Long.getLong("sirix.morsel.minPagesPerSplit", 64L);

  /**
   * Whether this array's elements are exactly "the children of {@link #nodeKey}".
   *
   * <p>
   * That equivalence is what makes a parent-key test a correct membership test, and it is what a
   * sub-range view breaks: a slice's elements are a positional window, which no per-node property can
   * identify. Such views decline to split and fall back to serial iteration.
   */
  protected boolean isWholeArray() {
    return true;
  }

  /** Record pages in this resource's document index — the space a split is carved out of. */
  private long documentPageCount() {
    moveRtx();
    return (rtx.getMaxNodeKey() >> Constants.NDP_NODE_COUNT_EXPONENT) + 1;
  }

  @Override
  public int memberSplitCount(final int preferred) {
    if (preferred <= 1 || !isWholeArray() || rtx instanceof JsonNodeTrx) {
      return 1;
    }
    moveRtx();
    if (rtx.getChildCount() < SPLIT_MIN_ELEMENTS) {
      return 1;
    }
    final long byPages = documentPageCount() / SPLIT_MIN_PAGES;
    if (byPages <= 1) {
      return 1;
    }
    return (int) Math.min(preferred, byPages);
  }

  @Override
  public Sequence memberSplit(final int index, final int total) {
    if (index < 0 || total <= 0 || index >= total) {
      throw new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE, "Invalid split %s of %s", index, total);
    }
    final long pages = documentPageCount();
    final long pagesPerSplit = (pages + total - 1) / total;
    final long from = (long) index * pagesPerSplit;
    final long to = Math.min(from + pagesPerSplit, pages);
    // Each piece opens its own transaction; nothing of this array's cursor state is shared, which
    // is what makes concurrent iteration of the pieces safe.
    return new ArrayPageRangeSequence(rtx.getResourceSession(), rtx.getRevisionNumber(), nodeKey, collection,
        jsonItemFactory, from, Math.max(from, to));
  }

  @Override
  public List<Sequence> values() {
    refreshMutableState();
    moveRtx();

    if (values == null) {
      values = getValues();
    }

    return values;
  }

  private List<Sequence> getValues() {
    moveRtx();
    MATERIALIZATIONS.increment();
    if (SCAN_DIAG) {
      System.err.printf("[jsonArrayScan] MATERIALIZING array nodeKey=%d childCount=%d%n", nodeKey, childCount);
    }
    final var values = new ArrayList<Sequence>();

    // Single sequential scan via the installable seam: with an io_uring prefetch factory installed,
    // this overlaps cold page reads with item construction (see JsonScanAxisFactory).
    JsonScanAxisFactory.forEachChild(rtx, nodeKey -> values.add(jsonItemFactory.getSequence(rtx, collection)));

    return values;
  }

  @Override
  public Sequence at(final IntNumeric numericIndex) {
    return at(numericIndex.intValue());
  }

  @Override
  public Sequence at(final int index) {
    refreshMutableState();
    if (index < 0) {
      return null;
    }
    final List<Sequence> materialized = values;
    if (materialized != null) {
      return index >= materialized.size()
          ? null
          : materialized.get(index);
    }

    // Out of range, and the count is already known: answer from it. A consumer that probes at(i)
    // until null -- rather than bounding its loop by len() -- must not pay a full materialization
    // (and retain the resulting list) for the one call that ends the walk. This is also the
    // teardown point for such a consumer: nothing after an out-of-range probe can consume a
    // read-ahead.
    if (childCount != CHILD_COUNT_UNKNOWN && index >= childCount) {
      closePrefetcher();
      return null;
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
    // the element already occupies. Both slots are tried: see the anchorIndexB javadoc for the
    // interleaved walk this exists to survive.
    if (index > 0) {
      if (index == anchorIndexA + 1) {
        final Sequence hopped = hopFrom(index, anchorKeyA, false);
        if (hopped != null) {
          return hopped;
        }
      } else if (index == anchorIndexB + 1) {
        final Sequence hopped = hopFrom(index, anchorKeyB, true);
        if (hopped != null) {
          return hopped;
        }
      }
    }

    // Re-reading the element an anchor already sits on: one move, and — unlike a re-anchor — it
    // leaves both slots and the read-ahead exactly as they were, so a consumer that reads an element
    // twice (peek then take, or a `for` whose body re-evaluates the binding) does not cost the walk
    // its place. An empty slot holds -1 and a negative index was rejected above, so no equality here
    // can match by accident.
    if (index == anchorIndexA && moveToAnchor(anchorKeyA)) {
      lastServedB = false;
      return jsonItemFactory.getSequence(rtx, collection);
    }
    if (index == anchorIndexB && moveToAnchor(anchorKeyB)) {
      lastServedB = true;
      return jsonItemFactory.getSequence(rtx, collection);
    }

    // Neither anchor is one hop away -- a fresh walk, an interleaved one, or a hop that did not
    // land. Locate the element by walking, from the closest anchor at or below it when there is one
    // and from the first child otherwise.
    //
    // This is where the whole element list used to be materialized and cached, "to keep the old O(n)
    // guarantee for index jumping". It is not worth its price: the cache is unbounded in the size of
    // the array, it is retained for the lifetime of the query, and one interleaved walk is enough to
    // trigger it, so a single 64 KiB render of $doc during a spill turned a streaming scan into
    // 99,999,968 live items. Walking costs O(distance) and nothing at all in memory, and the
    // anchors keep that distance at 1 for every consumer that reads forward -- which is every
    // consumer that matters. JsonDBArraySlice.sequenceAtSliceIndex has always resolved a lost
    // anchor this way.
    return atByWalking(index);
  }

  /**
   * Serves {@code index} by hopping right from an anchor known to sit at {@code index - 1}.
   *
   * @param index the index to serve
   * @param anchorKey node key of the element at {@code index - 1}
   * @param slotB whether the anchor is slot B
   * @return the element, or {@code null} if the anchor no longer resolves or has no right sibling —
   *         the caller then locates the element by walking
   */
  private Sequence hopFrom(final int index, final long anchorKey, final boolean slotB) {
    if (!moveToAnchor(anchorKey) || !rtx.moveToRightSibling()) {
      return null;
    }
    recordAnchor(index, rtx.getNodeKey(), slotB);
    onSequentialAdvance(index, anchorKey, slotB);
    return jsonItemFactory.getSequence(rtx, collection);
  }

  /**
   * Locates {@code index} by walking the sibling chain, starting from the closest anchor strictly
   * below it when there is one and from the array's first child otherwise.
   *
   * <p>
   * Allocation-free apart from the returned item: the walk is cursor moves only.
   *
   * @param index the index to serve
   * @return the element, or {@code null} when the array has fewer elements
   */
  private Sequence atByWalking(final int index) {
    POSITIONAL_REANCHORS.increment();
    if (SCAN_DIAG) {
      System.err.printf("[jsonArrayScan] re-anchor array nodeKey=%d childCount=%d index=%d anchors=[%d,%d]%n", nodeKey,
          childCount, index, anchorIndexA, anchorIndexB);
    }

    // The better anchor is the higher one still below the target; an anchor at or above it says
    // nothing about where the target is, because the chain is only walkable forwards.
    int from = -1;
    long fromKey = 0L;
    boolean fromB = false;
    if (anchorIndexA >= 0 && anchorIndexA < index) {
      from = anchorIndexA;
      fromKey = anchorKeyA;
    }
    if (anchorIndexB >= 0 && anchorIndexB < index && anchorIndexB > from) {
      from = anchorIndexB;
      fromKey = anchorKeyB;
      fromB = true;
    }

    // Evict the slot that did NOT serve last, so a walk in progress keeps its place. Reusing the
    // anchor we started from is the exception: it has just been superseded by this very walk.
    final boolean target = from >= 0
        ? fromB
        : !lastServedB;

    if (from >= 0 && moveToAnchor(fromKey)) {
      for (int i = from; i < index; i++) {
        if (!rtx.moveToRightSibling()) {
          // The anchor is stale (the chain no longer reaches the target). Start over from the top
          // rather than report the element missing on its evidence.
          return atByDescent(index, target);
        }
      }
      recordAnchor(index, rtx.getNodeKey(), target);
      return jsonItemFactory.getSequence(rtx, collection);
    }
    return atByDescent(index, target);
  }

  /**
   * Locates {@code index} by walking from the array's first child.
   *
   * @param index the index to serve
   * @param slotB the anchor slot to record the result in
   * @return the element, or {@code null} when the array has fewer elements
   */
  private Sequence atByDescent(final int index, final boolean slotB) {
    moveRtx();
    if (!rtx.moveToFirstChild()) {
      clearAnchor(slotB);
      return null;
    }
    for (int i = 0; i < index; i++) {
      if (!rtx.moveToRightSibling()) {
        clearAnchor(slotB);
        return null;
      }
    }
    recordAnchor(index, rtx.getNodeKey(), slotB);
    return jsonItemFactory.getSequence(rtx, collection);
  }

  /**
   * Positions the cursor on an anchor, refusing one that is no longer this array's child.
   *
   * <p>
   * The parent test only runs for a cursor that can see structural change — see
   * {@link #cursorMayMutate}. Making a scan pay it would be a per-element decode for a question whose
   * answer is fixed at the revision the scan reads. {@code JsonDBArraySlice.sequenceAtSliceIndex}
   * makes the same test for the same reason.
   *
   * @param anchorKey node key an anchor slot names
   * @return whether the cursor now sits on a usable anchor
   */
  private boolean moveToAnchor(final long anchorKey) {
    if (!rtx.moveTo(anchorKey)) {
      return false;
    }
    return !cursorMayMutate || rtx.getParentKey() == nodeKey;
  }

  /** Points an anchor slot at {@code index} and marks it as the one that served most recently. */
  private void recordAnchor(final int index, final long elementNodeKey, final boolean slotB) {
    if (slotB) {
      anchorIndexB = index;
      anchorKeyB = elementNodeKey;
    } else {
      anchorIndexA = index;
      anchorKeyA = elementNodeKey;
    }
    lastServedB = slotB;
  }

  /** Empties an anchor slot. */
  private void clearAnchor(final boolean slotB) {
    if (slotB) {
      anchorIndexB = -1;
    } else {
      anchorIndexA = -1;
    }
  }

  /**
   * Keep the read-ahead in step with a sequential walk that has just landed on {@code index}.
   *
   * <p>
   * On the LAST element the prefetcher is released instead. A consumer that bounds its loop by
   * {@code len()} — which brackit's array unbox does — never calls {@code at(n)}, so that is the only
   * point at which a completed scan is observably over, and the read-ahead's worker transactions have
   * to be released there or not at all. Nothing follows that could consume a prefetched page anyway.
   *
   * <p>
   * Otherwise the walk is extended. Decoding the pages ahead is what makes a cold or buffer-pressured
   * scan use more than one core; see {@code RecordPagePrefetcher}. It only warms a cache, so it
   * cannot affect the result. The two keys bracket one element, so their distance is this walk's
   * measured stride: together with the elements still to come it says how far the walk will actually
   * reach, which is what decides whether read-ahead can pay for itself here.
   *
   * @param index the index just landed on
   * @param previousNodeKey node key of the element before it
   * @param slotB whether the advancing walk holds slot B
   */
  private void onSequentialAdvance(final int index, final long previousNodeKey, final boolean slotB) {
    // Unconditional, whichever slot got here: the read-ahead belongs to the ARRAY's walk, and the
    // last element is the only point at which that walk is observably over. Gating this on the
    // owning slot leaked a worker transaction per array whenever a re-anchor moved the walk to the
    // other slot — which is exactly what a re-read of the current element used to cause.
    if (childCount != CHILD_COUNT_UNKNOWN && index == childCount - 1L) {
      closePrefetcher();
      return;
    }
    // Another walk owns the read-ahead. Steering it from here would drag it back to wherever that
    // other walk is, so leave it alone; it only warms a cache and cannot affect any answer.
    if (prefetcher != null && prefetchFollowsB != slotB) {
      return;
    }
    final long landedKey = rtx.getNodeKey();
    startPrefetchOnce(Math.max(1L, landedKey - previousNodeKey), childCount == CHILD_COUNT_UNKNOWN
        ? 0L
        : childCount - 1L - index, slotB);
    if (prefetcher != null) {
      prefetcher.advanceTo(landedKey);
    }
  }

  /**
   * Starts the read-ahead on the first sequential step of a walk, once per walk. Deferred to the
   * first step rather than done in the constructor because most arrays are never scanned end to end,
   * and a prefetcher that is created and immediately dropped costs a transaction for nothing — and
   * because only a step that has already happened can measure the walk.
   *
   * @param nodeStride node-key distance between the two elements visited so far, at least 1
   * @param remainingElements elements still to visit; {@code 0} when the count is unknown, which
   *        declines — a walk of unmeasurable length cannot be shown to amortize read-ahead, and an
   *        unknown count also means the last-element teardown can never fire for it
   * @param slotB whether the walk that earns the read-ahead holds slot B; the other slot's hops must
   *        not steer it, or an interleaved walk would drag it back to where it started
   */
  private void startPrefetchOnce(final long nodeStride, final long remainingElements, final boolean slotB) {
    if (!prefetcherInitialized) {
      prefetcherInitialized = true;
      prefetchFollowsB = slotB;
      final RecordPagePrefetcher started =
          RecordPagePrefetcher.createOrNull(rtx, rtx.getNodeKey(), nodeStride, remainingElements);
      prefetcher = started;
      if (started != null) {
        // Last resort for a walk that is simply ABANDONED -- an existential quantifier that
        // short-circuits, a top-k that stops early, an exception unwinding the pipeline. This item
        // is not AutoCloseable and brackit's iterator close is a no-op, so nothing else would ever
        // release the worker transactions of a scan that stopped in the middle. The action must
        // capture the prefetcher ONLY: capturing `this` would keep the item reachable and the
        // cleanup would never run.
        prefetcherCleanup = PrefetcherCleaner.CLEANER.register(this, started::close);
      }
    }
  }

  /**
   * Ends the read-ahead and releases its worker transactions. Idempotent.
   *
   * <p>
   * Resets the initialization latch as well, because this is the end of a WALK, not of the item: the
   * same array is walked again by a second {@code for} over the same variable, and by any walk
   * resumed after {@link #invalidateScanState()}. Leaving the latch set made the teardown at the last
   * element a one-way switch that permanently demoted every later scan of that item to the cold
   * single-core decode path the read-ahead exists to hide. Re-admission is decided afresh from the
   * new walk's own measurements, so a walk that should not prefetch still does not.
   */
  private void closePrefetcher() {
    prefetcherInitialized = false;
    final RecordPagePrefetcher running = prefetcher;
    if (running != null) {
      prefetcher = null;
      final Cleanable cleanup = prefetcherCleanup;
      prefetcherCleanup = null;
      if (cleanup != null) {
        // Runs the SAME action, on this thread, at most once -- and deregisters it, so the
        // prefetcher is not kept reachable by the cleaner after a deterministic close.
        cleanup.clean();
      } else {
        running.close();
      }
    }
  }

  /**
   * Holds the process-wide cleaner in a class of its own so its thread starts on the first array that
   * actually prefetches, not on the first one that is loaded.
   */
  private static final class PrefetcherCleaner {
    static final Cleaner CLEANER = Cleaner.create(runnable -> {
      final Thread thread = new Thread(runnable, "sirix-scan-prefetch-cleaner");
      thread.setDaemon(true);
      return thread;
    });

    private PrefetcherCleaner() {}
  }

  @Override
  public IntNumeric length() {
    refreshMutableState();
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
    refreshMutableState();
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
    // Reading the size off the cached list is exact: values() builds it from one child scan and
    // every element of the array is in it.
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
