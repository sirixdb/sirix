package io.sirix.index.path;

import io.sirix.access.trx.node.IndexController;
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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for PATH index changes.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class PathIndexListener {

  private final @Nullable RBTreeWriter<Long, NodeReferences> rbTreeWriter;
  private final @Nullable HOTLongIndexWriter hotWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
      final RBTreeWriter<Long, NodeReferences> indexWriter) {
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
      final HOTLongIndexWriter hotWriter) {
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.useHOT = true;
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    pathSummaryReader.moveTo(pathNodeKey);
    try {
      switch (type) {
        case INSERT -> {
          if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
            if (useHOT) {
              handleInsertHOT(node, pathNodeKey);
            } else {
              handleInsertRBTree(node, pathNodeKey);
            }
          }
        }
        case DELETE -> {
          if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
            if (useHOT) {
              assert hotWriter != null;
              hotWriter.remove(pathNodeKey, node.getNodeKey());
            } else {
              assert rbTreeWriter != null;
              rbTreeWriter.remove(pathNodeKey, node.getNodeKey());
            }
          }
        }
        default -> {
        }
      }
    } catch (final PathException e) {
      throw new SirixIOException(e);
    }
  }

  private void handleInsertRBTree(final ImmutableNode node, final long pathNodeKey) {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(pathNodeKey, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, textReferences.get(), pathNodeKey);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), pathNodeKey);
    }
  }

  private void handleInsertHOT(final ImmutableNode node, final long pathNodeKey) {
    assert hotWriter != null;
    // HOT writer uses primitive long - no boxing!
    NodeReferences existingRefs = hotWriter.get(pathNodeKey, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, existingRefs, pathNodeKey);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), pathNodeKey);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references,
      final long pathNodeKey) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references,
      final long pathNodeKey) {
    assert hotWriter != null;
    // HOT writer uses primitive long - no boxing!
    hotWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }
}
