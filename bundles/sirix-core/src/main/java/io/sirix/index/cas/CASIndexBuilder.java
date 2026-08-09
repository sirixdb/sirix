package io.sirix.index.cas;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTBulkIndexLoader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableValueNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.settings.Constants;
import io.sirix.utils.LogWrapper;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.index.path.summary.PathSummaryReader;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Builder for CAS indexes.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends.
 * </p>
 */
public final class CASIndexBuilder {
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(CASIndexBuilder.class));

  private final @Nullable RBTreeWriter<CASValue, NodeReferences> rbTreeWriter;
  private final @Nullable HOTIndexWriter<CASValue> hotWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;
  private final boolean useHOT;

  /** Path-class records covered by {@link #paths}, resolved lazily on the first indexed node. */
  private @Nullable LongSet resolvedPCRs;

  /**
   * Bulk loader for the HOT backend, non-{@code null} exactly when this builder starts against an
   * empty index tree — the normal "create an index over an already-shredded revision" case. Every
   * entry is collected and the trie is materialised once in {@link #finish()}; see
   * {@link HOTBulkIndexLoader} for why that is not the same cost as n incremental inserts.
   */
  private final @Nullable HOTBulkIndexLoader<CASValue> bulkLoader;

  /**
   * Constructor with RBTree writer (legacy path).
   */
  public CASIndexBuilder(final RBTreeWriter<CASValue, NodeReferences> indexWriter,
      final PathSummaryReader pathSummaryReader, final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = indexWriter;
    this.hotWriter = null;
    this.type = type;
    this.useHOT = false;
    this.bulkLoader = null;
  }

  /**
   * Constructor with HOT writer (high-performance path).
   */
  public CASIndexBuilder(final HOTIndexWriter<CASValue> hotWriter, final PathSummaryReader pathSummaryReader,
      final Set<Path<QNm>> paths, final Type type) {
    this.pathSummaryReader = pathSummaryReader;
    this.paths = paths;
    this.rbTreeWriter = null;
    this.hotWriter = hotWriter;
    this.type = type;
    this.useHOT = true;
    // Bulk-load only into a virgin tree: the loader replaces the root instead of merging into
    // it, so an index that already holds entries (a rebuild over a populated definition) keeps
    // the incremental path.
    this.bulkLoader = hotWriter.isEmptyTree() ? hotWriter.createBulkLoader() : null;
  }

  public VisitResult process(final ImmutableNode node, final long pathNodeKey) {
    try {
      if (matchesIndexedPath(pathNodeKey)) {
        final Str strValue = switch (node) {
          case ImmutableValueNode immutableValueNode -> new Str(immutableValueNode.getValue());
          case ImmutableNumberNode immutableNumberNode -> new Str(String.valueOf(immutableNumberNode.getValue()));
          case ImmutableBooleanNode immutableBooleanNode -> new Str(String.valueOf(immutableBooleanNode.getValue()));
          // Fused kinds carry primitive values inline.
          case ObjectNamedNumberNode namedNum ->
            new Str(String.valueOf(namedNum.getValue()));
          case ObjectNamedBooleanNode namedBool ->
            new Str(String.valueOf(namedBool.getValue()));
          case ObjectNamedStringNode namedStr ->
            new Str(new String(namedStr.getRawValue(), Constants.DEFAULT_ENCODING));
          case null, default -> throw new IllegalStateException("Value not supported.");
        };

        boolean isOfType = false;
        try {
          if (type != Type.STR)
            AtomicUtil.toType(strValue, type);
          isOfType = true;
        } catch (final SirixRuntimeException e) {
          LOGGER.debug("Value '{}' is not of type {}, skipping CAS index entry for node {}",
              strValue, type, node.getNodeKey(), e);
        }

        if (isOfType) {
          final CASValue value = new CASValue(strValue, type, pathNodeKey);
          if (useHOT) {
            processHOT(node, value);
          } else {
            processRBTree(node, value);
          }
        }
      }
    } catch (final PathException | SirixIOException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return VisitResultType.CONTINUE;
  }

  /**
   * Whether {@code pathNodeKey} is one of the path-class records this index covers. An empty path
   * configuration means "index every path".
   *
   * <p>The resolved PCR set is computed once and reused: the builder runs a single traversal of an
   * already-shredded revision, so the path summary cannot gain nodes underneath it, and
   * {@link PathSummaryReader#getPCRsForPaths(java.util.Collection)} allocates and fills a fresh
   * {@code LongOpenHashSet} on every call — once per value node, on the build hot path.</p>
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

  private void processRBTree(final ImmutableNode node, final CASValue value) throws SirixIOException {
    assert rbTreeWriter != null;
    final Optional<NodeReferences> textReferences = rbTreeWriter.get(value, SearchMode.EQUAL);
    if (textReferences.isPresent()) {
      setNodeReferencesRBTree(node, textReferences.get(), value);
    } else {
      setNodeReferencesRBTree(node, new NodeReferences(), value);
    }
  }

  /**
   * Add {@code node} to {@code value}'s posting list in the HOT backend.
   *
   * <p>A HOT slot write is an OR-merge of the incoming bitmap into the stored one
   * ({@code HOTLeafPage#mergeWithNodeRefs}), so adding one reference needs neither a read-back of
   * the stored references nor a re-insert of them. Doing so made building an index quadratic in
   * the number of nodes sharing a value: the n-th occurrence of a value range-scanned that value's
   * chunks and then re-inserted all n-1 node keys already stored, each through a full trie descent
   * — on a corpus where a value repeats k times, k(k+1)/2 slot writes instead of k.</p>
   */
  private void processHOT(final ImmutableNode node, final CASValue value) throws SirixIOException {
    assert hotWriter != null;
    if (bulkLoader != null) {
      bulkLoader.add(value, node.getNodeKey());
    } else {
      hotWriter.indexNodeKey(value, node.getNodeKey());
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

  private void setNodeReferencesRBTree(final ImmutableNode node, final NodeReferences references, final CASValue value)
      throws SirixIOException {
    assert rbTreeWriter != null;
    rbTreeWriter.index(value, references.addNodeKey(node.getNodeKey()), RBTreeReader.MoveCursor.NO_MOVE);
  }

}
