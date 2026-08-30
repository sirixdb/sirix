package io.sirix.index.name;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.hot.HOTIndexWriter;
import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/** Incremental listener for the canonical HOT-backed NAME index. */
public final class NameIndexListener {

  private final Set<QNm> includes;
  private final Set<QNm> excludes;
  private final HOTIndexWriter<QNm> hotWriter;

  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes, final HOTIndexWriter<QNm> hotWriter) {
    this.includes = requireNonNull(includes);
    this.excludes = requireNonNull(excludes);
    this.hotWriter = requireNonNull(hotWriter);
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final QNm name) {
    listen(type, node.getNodeKey(), name);
  }

  public void listen(final IndexController.ChangeType type, final long nodeKey, final QNm name) {
    // Skip if name is null (can happen when node is loaded from disk without cached name)
    if (name == null) {
      return;
    }

    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return;
    }

    switch (type) {
      case INSERT -> hotWriter.indexNodeKey(name, nodeKey);
      case DELETE -> hotWriter.remove(name, nodeKey);
      default -> {
      }
    }
  }
}
