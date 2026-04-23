/*
 * Copyright (c) 2026, Sirix Contributors. All rights reserved.
 */
package io.sirix.axis;

import io.sirix.api.NodeCursor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;

/**
 * Emit the primitive value of the object-field currently under the cursor as a one-shot axis.
 *
 * <p>Unifies legacy {@code OBJECT_KEY → primitive-value-child} navigation with fused
 * {@code OBJECT_NAMED_*} direct value access (iter#31 Option B):
 * <ul>
 *   <li>On {@link NodeKind#OBJECT_KEY}, yields the first child's nodeKey (the primitive).</li>
 *   <li>On any {@code OBJECT_NAMED_*} kind, yields the fused node's own nodeKey — it IS the
 *       value. Consumers that dispatch on {@code rtx.getKind()} pick up the fused kind and
 *       read the inline primitive via {@code getValue()} / {@code getNumberValue()}.</li>
 * </ul>
 *
 * <p>HFT contract: zero per-call allocation. One boolean field for first-hit tracking, one
 * long scratch for the yielded key.
 */
public final class FieldValueAxis extends AbstractAxis {

  private boolean first;

  public FieldValueAxis(final NodeCursor cursor) {
    super(cursor);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    first = true;
  }

  @Override
  protected long nextKey() {
    if (!first) {
      return done();
    }
    first = false;

    final NodeCursor cursor = getCursor();
    if (cursor instanceof JsonNodeReadOnlyTrx rtx) {
      final NodeKind kind = rtx.getKind();
      if (kind == NodeKind.OBJECT_NAMED_BOOLEAN
          || kind == NodeKind.OBJECT_NAMED_NUMBER
          || kind == NodeKind.OBJECT_NAMED_STRING
          || kind == NodeKind.OBJECT_NAMED_NULL) {
        // Fused — the node IS the value. Emit its own key.
        return rtx.getNodeKey();
      }
    }
    // Legacy OBJECT_KEY → first child is the primitive value.
    if (cursor.hasFirstChild()) {
      return cursor.getFirstChildKey();
    }
    return done();
  }
}
