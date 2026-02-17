package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.access.trx.node.IndexController;
import io.sirix.node.NodeKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Primitive change listener contract for hot-path index notifications.
 *
 * <p>
 * This avoids forcing immutable node snapshot materialization when callers already have primitive
 * change details available.
 * </p>
 */
public interface PathNodeKeyChangeListener extends ChangeListener {

  void listen(IndexController.ChangeType type, long nodeKey, NodeKind nodeKind, long pathNodeKey, @Nullable QNm name,
      @Nullable Str value);
}
