package io.sirix.index.path;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.index.SearchMode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTLongBulkIndexLoader;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeReader.MoveCursor;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;
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

  /** Path-class records covered by {@link #paths}, resolved lazily on the first indexed node. */
  private @Nullable LongSet resolvedPCRs;

  /**
   * Bulk loader for the HOT backend, non-{@code null} exactly when this builder starts against an
   * empty index tree — the normal "create an index over an already-shredded revision" case. Every
   * entry is collected and the trie is materialised once in {@link #finish()}.
   */
  private final @Nullable HOTLongBulkIndexLoader bulkLoader;

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
    this.bulkLoader = null;
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
    // Bulk-load only into a virgin tree: the loader replaces the root instead of merging into it,
    // so an index that already holds entries keeps the incremental path.
    this.bulkLoader = hotWriter.isEmptyTree() ? hotWriter.createBulkLoader() : null;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      final long PCR = pathNodeKey;
      if (matchesIndexedPath(PCR)) {
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

  /**
   * Returns the {@link PathSummaryReader} backing this builder. Used by JSON-specific dispatch
   * (e.g. fused {@code OBJECT_NAMED_ARRAY} which must index its host record at both the
   * OBJECT_KEY layer and the {@code __array__/ARRAY} layer) to navigate path-summary parents.
   */
  public PathSummaryReader getPathSummaryReader() {
    return pathSummaryReader;
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

  /**
   * Whether {@code pathNodeKey} is one of the path-class records this index covers. An empty path
   * configuration means "index every path".
   *
   * <p>The resolved PCR set is computed once and reused: the builder runs a single traversal of an
   * already-shredded revision, so the path summary cannot gain nodes underneath it, and
   * {@link PathSummaryReader#getPCRsForPaths(java.util.Collection)} allocates and fills a fresh
   * {@code LongOpenHashSet} on every call — once per visited node, on the build hot path.</p>
   */
  private boolean matchesIndexedPath(final long pathNodeKey) {
    if (paths.isEmpty()) {
      return true;
    }
    LongSet pcrs = resolvedPCRs;
    if (pcrs == null) {
      pcrs = pathSummaryReader.getPCRsForPaths(paths);
      resolvedPCRs = pcrs;
    }
    return pcrs.contains(pathNodeKey);
  }

  /**
   * Add {@code node} to {@code PCR}'s posting list in the HOT backend.
   *
   * <p>A HOT slot write is an OR-merge of the incoming bitmap into the stored one, so adding one
   * reference needs neither a read-back of the stored references nor a re-insert of them. Doing so
   * made building an index quadratic in the number of nodes sharing a key — and for a PATH index
   * every node under the indexed path shares one key, so that was the whole index.</p>
   */
  private void processHOT(final ImmutableNode node, final long PCR) throws SirixIOException {
    assert hotWriter != null;
    if (bulkLoader != null) {
      bulkLoader.add(PCR, node.getNodeKey());
    } else {
      hotWriter.indexNodeKey(PCR, node.getNodeKey());
    }
  }

  /**
   * Materialise everything the traversal collected. Must be called exactly once, after the
   * document traversal that feeds {@link #process} has finished; a no-op unless this builder is
   * bulk-loading.
   */
  public void finish() {
    if (bulkLoader != null) {
      bulkLoader.flush();
    }
  }

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references,
      final long pathNodeKey) throws SirixIOException {
    assert rbTreeWriter != null;
    rbTreeWriter.index(pathNodeKey, references.addNodeKey(node.getNodeKey()), MoveCursor.NO_MOVE);
  }

}
