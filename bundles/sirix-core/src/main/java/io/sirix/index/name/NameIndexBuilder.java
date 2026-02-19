package io.sirix.index.name;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Builder for NAME indexes.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends.
 * </p>
 */
public final class NameIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(NameIndexBuilder.class));

  public final Set<QNm> includes;
  public final Set<QNm> excludes;
  public final @Nullable RBTreeWriter<QNm, NodeReferences> rbTreeWriter;
  public final @Nullable HOTIndexWriter<QNm> hotWriter;
  public final StorageEngineReader storageEngineReader;
  private final boolean useHOT;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public NameIndexBuilder(final Set<QNm> includes, final Set<QNm> excludes,
      final RBTreeWriter<QNm, NodeReferences> indexWriter, final StorageEngineReader storageEngineReader) {
    this.includes = includes;
    this.excludes = excludes;
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.storageEngineReader = storageEngineReader;
    this.useHOT = false;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public NameIndexBuilder(final Set<QNm> includes, final Set<QNm> excludes, final HOTIndexWriter<QNm> hotWriter,
      final StorageEngineReader storageEngineReader) {
    this.includes = includes;
    this.excludes = excludes;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.storageEngineReader = storageEngineReader;
    this.useHOT = true;
  }

  public VisitResultType build(QNm name, ImmutableNode node) {
    final boolean included = (includes.isEmpty() || includes.contains(name));
    final boolean excluded = (!excludes.isEmpty() && excludes.contains(name));

    if (!included || excluded) {
      return VisitResultType.CONTINUE;
    }

    try {
      if (useHOT) {
        buildHOT(name, node);
      } else {
        buildRBTree(name, node);
      }
    } catch (final SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return VisitResultType.CONTINUE;
  }

  private void buildRBTree(QNm name, ImmutableNode node) {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(name, SearchMode.EQUAL);
    textReferences.ifPresentOrElse(nodeReferences -> setNodeReferencesRBTree(node, nodeReferences, name),
        () -> setNodeReferencesRBTree(node, new NodeReferences(), name));
  }

  private void buildHOT(QNm name, ImmutableNode node) {
    assert hotWriter != null;
    NodeReferences existingRefs = hotWriter.get(name, SearchMode.EQUAL);
    if (existingRefs != null) {
      setNodeReferencesHOT(node, existingRefs, name);
    } else {
      setNodeReferencesHOT(node, new NodeReferences(), name);
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, final QNm name) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }

  private void setNodeReferencesHOT(final ImmutableNode node, final NodeReferences references, final QNm name) {
    assert hotWriter != null;
    hotWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
