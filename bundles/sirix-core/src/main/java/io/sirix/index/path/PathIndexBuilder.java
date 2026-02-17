package io.sirix.index.path;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.utils.LogWrapper;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Builder for PATH indexes.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends.
 * </p>
 */
public final class PathIndexBuilder {

  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(PathIndexBuilder.class));

  private final Set<Path<QNm>> paths;

  private final PathSummaryReader pathSummaryReader;

  private final @Nullable RBTreeWriter<Long, NodeReferences> rbTreeWriter;

  private final @Nullable HOTLongIndexWriter hotWriter;

  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public PathIndexBuilder(final RBTreeWriter<Long, NodeReferences> indexWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public PathIndexBuilder(final HOTLongIndexWriter hotWriter, final PathSummaryReader pathSummaryReader,
      final Set<Path<QNm>> paths) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.useHOT = true;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      final long PCR = pathNodeKey;
      if (pathSummaryReader.getPCRsForPaths(paths).contains(PCR) || paths.isEmpty()) {
        if (useHOT) {
          processHOT(node, PCR);
        } else {
          processRBTree(node, PCR);
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  private void processRBTree(final ImmutableNode node, final long PCR) throws SirixIOException {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(PCR, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, textReferences.get(), PCR);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), PCR);
    }
  }

  private void processHOT(final ImmutableNode node, final long PCR) throws SirixIOException {
    assert hotWriter != null;
    NodeReferences existingRefs = hotWriter.get(PCR, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, existingRefs, PCR);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), PCR);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references,
      final long pathNodeKey) throws SirixIOException {
    assert rbTreeWriter != null;
    rbTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references, final long pathNodeKey)
      throws SirixIOException {
    assert hotWriter != null;
    hotWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
