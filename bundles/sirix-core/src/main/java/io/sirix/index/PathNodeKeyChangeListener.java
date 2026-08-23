package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.sirix.access.trx.node.IndexController;
import io.sirix.node.NodeKind;
import org.jspecify.annotations.Nullable;

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

  /**
   * Parent-aware primitive notification for callers that already hold the changed node.
   *
   * <p>
   * The default preserves source and binary behaviour for listeners that do not need ancestry: they
   * continue to receive the original primitive notification. Listeners whose classification requires
   * the parent chain can override this overload and avoid reading the changed record back from the
   * page layer merely to recover {@code parentKey}.
   * </p>
   *
   * @param type type of change
   * @param nodeKey key of the changed node
   * @param nodeKind kind of the changed node
   * @param parentKey key of the changed node's parent
   * @param pathNodeKey path node key of the changed node (or parent path key for value nodes)
   * @param name optional name (only relevant for name-indexed kinds)
   * @param value optional value (only relevant for value-indexed kinds)
   */
  default void listen(final IndexController.ChangeType type, final long nodeKey, final NodeKind nodeKind,
      final long parentKey, final long pathNodeKey, final @Nullable QNm name, final @Nullable Str value) {
    listen(type, nodeKey, nodeKind, pathNodeKey, name, value);
  }
}
