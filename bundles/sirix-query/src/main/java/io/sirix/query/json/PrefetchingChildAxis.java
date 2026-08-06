package io.sirix.query.json;

import io.sirix.api.NodeCursor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;
import io.sirix.axis.ChildAxis;

/**
 * A {@link ChildAxis} that overlaps record-page decoding with consumption.
 *
 * <p>Traversal is identical to {@link ChildAxis} — first child, then right siblings — so the key
 * sequence this axis yields is exactly the sequence {@code ChildAxis} yields. The only addition is
 * that each emitted key is handed to a {@link RecordPagePrefetcher}, which materializes the pages
 * just beyond the cursor on background threads.
 *
 * <p>The two are deliberately not related by inheritance: {@code ChildAxis} is {@code final}, and
 * the traversal it encodes is four lines. Duplicating those four lines is cheaper than making a
 * hot, {@code final} class extensible, and it keeps the prefetching decision out of the plain path
 * entirely — installations that do not prefetch pay nothing.
 *
 * <p>Closing this axis shuts the prefetcher down and closes its worker transactions;
 * {@link JsonScanAxisFactory#forEachChild} owns that lifecycle.
 */
public final class PrefetchingChildAxis extends AbstractAxis implements AutoCloseable {

  /** Whether the next key comes from {@code getFirstChildKey} rather than a right sibling. */
  private boolean first;

  /** The cursor, typed: {@link #getCursor()} answers the erased {@link NodeCursor}. */
  private final JsonNodeReadOnlyTrx rtx;

  /** {@code null} when prefetching is disabled or not worthwhile; then this is a plain child axis. */
  private RecordPagePrefetcher prefetcher;

  /** Whether the read-ahead question has been asked for the current walk; asked exactly once. */
  private boolean prefetchDecided;

  /** Children of the node being walked, read while the cursor still sits on it. */
  private long childCount;

  /** Key emitted by the previous step, or {@code -1} before the first one. */
  private long previousKey = -1L;

  /**
   * @param rtx cursor to iterate with, positioned on the node whose children are wanted
   */
  public PrefetchingChildAxis(final JsonNodeReadOnlyTrx rtx) {
    super(rtx);
    this.rtx = rtx;
    this.first = true;
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    first = true;
    // A reset starts a DIFFERENT walk: its length is not the one the running read-ahead was
    // admitted for, and whatever that one has in flight is ahead of a cursor that has moved away.
    close();
    prefetchDecided = false;
    previousKey = -1L;
  }

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();

    final long next;
    if (!first && cursor.hasRightSibling()) {
      next = cursor.getRightSiblingKey();
    } else if (first && cursor.hasFirstChild()) {
      first = false;
      // The cursor still sits on the parent here, which is the only point at which this walk's
      // length is readable without moving it back off the page the first child lives on.
      childCount = rtx.getChildCount();
      next = cursor.getFirstChildKey();
    } else {
      return done();
    }

    if (!prefetchDecided && previousKey >= 0L) {
      prefetchDecided = true;
      // Two keys observed, so the walk can now measure itself: their distance is its stride, and
      // with the children still to come that says how far it will actually reach. Read-ahead is
      // admitted on THAT, never on how large the resource happens to be — a resource-sized gate
      // starts a full speculative window for a three-element walk that ends one step later.
      prefetcher = RecordPagePrefetcher.createOrNull(rtx, next, Math.max(1L, next - previousKey),
                                                     childCount - 2L);
    }
    previousKey = next;

    if (prefetcher != null) {
      prefetcher.advanceTo(next);
    }
    return next;
  }

  @Override
  public void close() {
    final RecordPagePrefetcher running = prefetcher;
    if (running != null) {
      prefetcher = null;
      running.close();
    }
  }
}
