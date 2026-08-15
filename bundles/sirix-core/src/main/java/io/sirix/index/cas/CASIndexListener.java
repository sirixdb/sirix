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
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for CAS (Content-and-Structure) index changes.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends.
 * </p>
 */
public final class CASIndexListener {

  private static final Logger logger = LoggerFactory.getLogger(CASIndexListener.class);

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
  public CASIndexListener(final PathSummaryReader pathSummaryReader, final HOTIndexWriter<CASValue> hotWriter,
      final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.paths = paths;
    this.type = type;
    this.useHOT = true;
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey,
      final Str value) {
    listen(type, node.getNodeKey(), pathNodeKey, value);
  }

  public void listen(final IndexController.ChangeType type, final long nodeKey, final long pathNodeKey,
      final Str value) {
    var hasMoved = pathSummaryReader.moveTo(pathNodeKey);
    assert hasMoved;
    switch (type) {
      case INSERT -> {
        // An empty path set means "index ALL paths" (matching CASIndexBuilder); the old guard
        // checked only getPCRsForPaths(paths).contains(...), which is the empty set for an empty
        // path config — so a `jn:create-cas-index($doc,'xs:string')` indexed existing data but
        // every subsequent insert was invisible to the index. (PathIndexListener has this guard.)
        if (paths.isEmpty() || pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
          insert(nodeKey, pathNodeKey, value);
        }
      }
      case DELETE -> {
        if (paths.isEmpty() || pathSummaryReader.getPCRsForPaths(paths).contains(pathNodeKey)) {
          // Converted exactly as insert() converts, because the two must build the SAME key or the
          // delete misses the entry it is meant to remove. A value that does not convert was never
          // indexed, so there is nothing to remove and skipping is right.
          final Atomic typedValue = toTypedOrNull(value, nodeKey);
          if (typedValue == null) {
            return;
          }
          final CASValue casValue = new CASValue(typedValue, this.type, pathNodeKey);
          if (useHOT) {
            assert hotWriter != null;
            hotWriter.remove(casValue, nodeKey);
          } else {
            assert rbTreeWriter != null;
            rbTreeWriter.remove(casValue, nodeKey);
          }
        }
      }
      default -> {
      }
    }
  }

  /**
   * {@code value} as the index's content type, or {@code null} when it is not of that type.
   *
   * <p>
   * The conversion is KEPT rather than discarded, so the stored key is built from the same shape the
   * query side probes with — see {@code CASIndexBuilder#process} for the three bugs the two-shape
   * arrangement produced.
   * </p>
   *
   * @param value the node's lexical value
   * @param nodeKey the node, for the diagnostic only
   * @return the typed value, or {@code null} to skip this node
   */
  private @Nullable Atomic toTypedOrNull(final Str value, final long nodeKey) {
    if (type == Type.STR) {
      return value;
    }
    try {
      return AtomicUtil.toType(value, type);
    } catch (final SirixRuntimeException e) {
      logger.debug("Value '{}' is not of type {}, skipping CAS index entry for node {}", value, type, nodeKey, e);
      return null;
    }
  }

  private void insert(final long nodeKey, final long pathNodeKey, final Str value) throws SirixIOException {
    final Atomic typedValue = toTypedOrNull(value, nodeKey);
    final boolean isOfType = typedValue != null;

    if (isOfType) {
      final CASValue indexValue = new CASValue(typedValue, type, pathNodeKey);
      if (useHOT) {
        insertHOT(nodeKey, indexValue);
      } else {
        insertRBTree(nodeKey, indexValue);
      }
    }
  }

  private void insertRBTree(final long nodeKey, final CASValue indexValue) {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(indexValue, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(nodeKey, new NodeReferences(textReferences.get().getNodeKeys()), indexValue);
    } else {
      setNodeReferencesRBTree(nodeKey, new NodeReferences(), indexValue);
    }
  }

  /**
   * Add {@code nodeKey} to {@code indexValue}'s posting list in the HOT backend.
   *
   * <p>
   * A HOT slot write OR-merges the incoming bitmap into the stored one, so the references already
   * recorded for {@code indexValue} need neither be read back nor re-inserted — doing so cost one
   * range scan plus one full trie descent per already-stored node key, making a bulk insert of k
   * nodes sharing a value quadratic in k.
   * </p>
   */
  private void insertHOT(final long nodeKey, final CASValue indexValue) {
    assert hotWriter != null;
    hotWriter.indexNodeKey(indexValue, nodeKey);
  }

  private void setNodeReferencesRBTree(final long nodeKey, final NodeReferences references, final CASValue indexValue) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(indexValue, references.addNodeKey(nodeKey), RBTreeReader.MoveCursor.NO_MOVE);
  }

}
