package io.sirix.index.name;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTBulkIndexLoader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import org.jspecify.annotations.Nullable;
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
   * Bulk loader for the HOT backend, non-{@code null} exactly when this builder starts against an
   * empty index tree — the normal "create an index over an already-shredded revision" case. Every
   * entry is collected and the trie is materialised once in {@link #finish()}.
   */
  private final @Nullable HOTBulkIndexLoader<QNm> bulkLoader;

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
    this.bulkLoader = null;
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
    // Bulk-load only into a virgin tree: the loader replaces the root instead of merging into it,
    // so an index that already holds entries keeps the incremental path.
    this.bulkLoader = hotWriter.isEmptyTree()
        ? hotWriter.createBulkLoader()
        : null;
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

  /**
   * Add {@code node} to {@code name}'s posting list in the HOT backend.
   *
   * <p>
   * A HOT slot write is an OR-merge of the incoming bitmap into the stored one, so adding one
   * reference needs neither a read-back of the stored references nor a re-insert of them. Doing so
   * made building an index quadratic in how many nodes share a name — which, for a name index, is the
   * point of the index.
   * </p>
   */
  private void buildHOT(QNm name, ImmutableNode node) {
    assert hotWriter != null;
    if (bulkLoader != null) {
      bulkLoader.add(name, node.getNodeKey());
    } else {
      hotWriter.indexNodeKey(name, node.getNodeKey());
    }
  }

  /**
   * Materialise everything the traversal collected. Must be called exactly once, after the document
   * traversal that feeds {@link #build} has finished; a no-op unless this builder is bulk-loading.
   */
  public void finish() {
    if (bulkLoader != null) {
      bulkLoader.flush();
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, final QNm name) {
    assert rbTreeWriter != null;
    rbTreeWriter.index(name, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }
}
