package io.sirix.utils;

import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeCursor;

import static java.util.Objects.requireNonNull;

/** Exact document child counts, including resources that omit the persisted count. */
public final class ChildCounts {
  private ChildCounts() {}

  /**
   * Returns the number of linked children without changing the cursor position. Stored counts take
   * constant work; omitted counts require a sibling walk. Callers on a fixed revision should cache
   * the result when they need it repeatedly.
   *
   * @param trx document transaction positioned on the parent
   * @return its number of children
   */
  public static long get(final NodeReadOnlyTrx trx) {
    requireNonNull(trx);
    if (trx.getResourceSession().getResourceConfig().storeChildCount()) {
      return trx.getChildCount();
    }
    final long parentKey = trx.getNodeKey();
    final NodeCursor cursor = (NodeCursor) trx;
    long count = 0;
    try {
      if (cursor.moveToFirstChild()) {
        do {
          count++;
        } while (cursor.moveToRightSibling());
      }
      return count;
    } finally {
      trx.moveTo(parentKey);
    }
  }
}
