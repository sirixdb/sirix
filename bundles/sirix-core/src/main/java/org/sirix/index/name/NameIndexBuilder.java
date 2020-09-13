package org.sirix.index.name;

import java.util.Optional;
import java.util.Set;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.exception.SirixIOException;
import org.sirix.index.SearchMode;
import org.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import org.sirix.index.redblacktree.RBTreeWriter;
import org.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public final class NameIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(NameIndexBuilder.class));

  public Set<QNm> mIncludes;
  public Set<QNm> mExcludes;
  public RBTreeWriter<QNm, NodeReferences> mAVLTreeWriter;

  public NameIndexBuilder(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> avlTreeWriter) {
    mIncludes = includes;
    mExcludes = excludes;
    mAVLTreeWriter = avlTreeWriter;
  }

  public VisitResultType build(QNm name, ImmutableNode node) {
    final boolean included = (mIncludes.isEmpty() || mIncludes.contains(name));
    final boolean excluded = (!mExcludes.isEmpty() && mExcludes.contains(name));

    if (!included || excluded) {
      return VisitResultType.CONTINUE;
    }

    final Optional<NodeReferences> textReferences = mAVLTreeWriter.get(name, SearchMode.EQUAL);

    try {
      textReferences.ifPresentOrElse(nodeReferences -> setNodeReferences(node, nodeReferences, name),
          () -> setNodeReferences(node, new NodeReferences(), name));
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return VisitResultType.CONTINUE;
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final QNm name) {
    mAVLTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
