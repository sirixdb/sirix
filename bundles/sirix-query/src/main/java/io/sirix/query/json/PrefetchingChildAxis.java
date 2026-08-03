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

  /** {@code null} when prefetching is disabled or not worthwhile; then this is a plain child axis. */
  private final RecordPagePrefetcher prefetcher;

  /**
   * @param rtx cursor to iterate with, positioned on the node whose children are wanted
   */
  public PrefetchingChildAxis(final JsonNodeReadOnlyTrx rtx) {
    super(rtx);
    this.first = true;
    this.prefetcher = RecordPagePrefetcher.createOrNull(rtx);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    first = true;
  }

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();

    final long next;
    if (!first && cursor.hasRightSibling()) {
      next = cursor.getRightSiblingKey();
    } else if (first && cursor.hasFirstChild()) {
      first = false;
      next = cursor.getFirstChildKey();
    } else {
      return done();
    }

    if (prefetcher != null) {
      prefetcher.advanceTo(next);
    }
    return next;
  }

  @Override
  public void close() {
    if (prefetcher != null) {
      prefetcher.close();
    }
  }
}
