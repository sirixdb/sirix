package io.sirix.index.name;

import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.sirix.exception.SirixIOException;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

public final class NameIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(NameIndexBuilder.class));

  public Set<QNm> includes;
  public Set<QNm> excludes;
  public RBTreeWriter<QNm, NodeReferences> indexWriter;

  public NameIndexBuilder(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> indexWriter) {
    this.includes = includes;
    this.excludes = excludes;
    this.indexWriter = indexWriter;
  }

  public VisitResultType build(QNm name, ImmutableNode node) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return VisitResultType.CONTINUE;
    }

    final Optional<NodeReferences> textReferences = indexWriter.get(name, SearchMode.EQUAL);

    try {
      textReferences.ifPresentOrElse(nodeReferences -> setNodeReferences(node, nodeReferences, name),
          () -> setNodeReferences(node, new NodeReferences(), name));
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return VisitResultType.CONTINUE;
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final QNm name) {
    indexWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
