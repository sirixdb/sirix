package io.sirix.index.cas;

import io.sirix.access.trx.node.IndexController;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for CAS (Content-and-Structure) index changes.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class CASIndexListener {

  private final @Nullable RBTreeWriter<CASValue, NodeReferences> rbTreeWriter;
  private final @Nullable HOTIndexWriter<CASValue> hotWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;
  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public CASIndexListener(final PathSummaryReader pathSummaryReader,
      final RBTreeWriter<CASValue, NodeReferences> indexWriter, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.paths = paths;
    this.type = type;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public CASIndexListener(final PathSummaryReader pathSummaryReader,
      final HOTIndexWriter<CASValue> hotWriter, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.paths = paths;
    this.type = type;
    this.useHOT = true;
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey, final Str value) {
    var hasMoved = pathSummaryReader.moveTo(pathNodeKey);
    assert hasMoved;
    switch (type) {
      case INSERT -> {
        if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
          insert(node, pathNodeKey, value);
        }
      }
      case DELETE -> {
        if (pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
          CASValue casValue = new CASValue(value, this.type, pathNodeKey);
          if (useHOT) {
            assert hotWriter != null;
            hotWriter.remove(casValue, node.getNodeKey());
          } else {
            assert rbTreeWriter != null;
            rbTreeWriter.remove(casValue, node.getNodeKey());
          }
        }
      }
      default -> {
      }
    }
  }

  private void insert(final ImmutableNode node, final long pathNodeKey, final Str value) throws SirixIOException {
    boolean isOfType = false;
    try {
      AtomicUtil.toType(value, type);
      isOfType = true;
    } catch (final SirixRuntimeException ignored) {
    }

    if (isOfType) {
      final CASValue indexValue = new CASValue(value, type, pathNodeKey);
      if (useHOT) {
        insertHOT(node, indexValue);
      } else {
        insertRBTree(node, indexValue);
      }
    }
  }

  private void insertRBTree(final ImmutableNode node, final CASValue indexValue) {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(indexValue, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, new NodeReferences(textReferences.get().getNodeKeys()), indexValue);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), indexValue);
    }
  }

  private void insertHOT(final ImmutableNode node, final CASValue indexValue) {
    assert hotWriter != null;
    NodeReferences existingRefs = hotWriter.get(indexValue, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, new NodeReferences(existingRefs.getNodeKeys()), indexValue);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), indexValue);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, 
      final CASValue indexValue) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references,
      final CASValue indexValue) {
    assert hotWriter != null;
    hotWriter.index(indexValue, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
