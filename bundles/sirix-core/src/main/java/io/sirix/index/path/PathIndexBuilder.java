package io.sirix.index.path;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

public final class PathIndexBuilder {

  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(PathIndexBuilder.class));

  private final Set<Path<QNm>> paths;

  private final PathSummaryReader pathSummaryReader;

  private final RBTreeWriter<Long, NodeReferences> indexWriter;

  public PathIndexBuilder(final RBTreeWriter<Long, NodeReferences> indexWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.indexWriter = indexWriter;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      final long PCR = pathNodeKey;
      if (pathSummaryReader.getPCRsForPaths(paths).contains(PCR) || paths.isEmpty()) {
        final Optional<NodeReferences> textReferences = indexWriter.get(PCR, SearchMode.EQUAL);
        if (textReferences.isPresent()) {
          setNodeReferences(node, textReferences.get(), PCR);
        } else {
          setNodeReferences(node, new NodeReferences(), PCR);
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  private void setNodeReferences(final ImmutableNode node, final NodeReferences references, final long pathNodeKey)
      throws SirixIOException {
    indexWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }

}
