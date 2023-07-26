package io.sirix.index.name;

import io.sirix.access.trx.node.IndexController;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.node.interfaces.immutable.ImmutableNode;

import java.util.Optional;
import java.util.Set;

public final class NameIndexListener {

  private final Set<QNm> includes;
  private final Set<QNm> excludes;
  private final RBTreeWriter<QNm, NodeReferences> indexWriter;

  public NameIndexListener(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> indexTreeWriter) {
    this.includes = includes;
    this.excludes = excludes;
    this.indexWriter = indexTreeWriter;
  }

  public void listen(IndexController.ChangeType type, @NonNull ImmutableNode node, QNm name) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return;
    }

    switch (type) {
      case INSERT -> {
        final Optional<NodeReferences> textReferences = indexWriter.get(name, SearchMode.EQUAL);
        if (textReferences.isPresent()) {
          setNodeReferences(node, textReferences.get(), name);
        } else {
          setNodeReferences(node, new NodeReferences(), name);
        }
      }
      case DELETE -> indexWriter.remove(name, node.getNodeKey());
      default -> {
      }
    }
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final QNm name) {
    indexWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
