package io.sirix.index.path;

import io.sirix.access.trx.node.IndexController;
import io.brackit.query.atomic.QNm;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathException;
import io.sirix.exception.SirixIOException;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/** Listener for incremental persistent HOT PATH index changes. */
public final class PathIndexListener {

  private final HOTLongIndexWriter hotWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private @Nullable LongSet resolvedPCRs;
  private long maxKnownPCR = -1L;

  public PathIndexListener(final Set<Path<QNm>> paths, final PathSummaryReader pathSummaryReader,
      final HOTLongIndexWriter hotWriter) {
    this.hotWriter = requireNonNull(hotWriter);
    this.pathSummaryReader = requireNonNull(pathSummaryReader);
    this.paths = requireNonNull(paths);
  }

  /**
   * Returns the {@link PathSummaryReader} backing this listener. Used by JSON-specific dispatch (e.g.
   * fused {@code OBJECT_NAMED_ARRAY} which must mirror its index entry at both the OBJECT_KEY layer
   * and the {@code __array__/ARRAY} layer) to navigate path-summary parents.
   */
  public PathSummaryReader getPathSummaryReader() {
    return pathSummaryReader;
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey) {
    listen(type, node.getNodeKey(), pathNodeKey);
  }

  public void listen(final IndexController.ChangeType type, final long nodeKey, final long pathNodeKey) {
    try {
      final boolean shouldProcess = matchesIndexedPath(pathNodeKey);

      switch (type) {
        case INSERT -> {
          if (shouldProcess) {
            handleInsert(nodeKey, pathNodeKey);
          }
        }
        case DELETE -> {
          if (shouldProcess) {
            hotWriter.remove(pathNodeKey, nodeKey);
          }
        }
        default -> {
        }
      }
    } catch (final PathException e) {
      throw new SirixIOException(e);
    }
  }

  /** Resolve configured paths once, refreshing only when a newly minted PCR can change the answer. */
  private boolean matchesIndexedPath(final long pathNodeKey) throws PathException {
    if (paths.isEmpty()) {
      return true;
    }
    LongSet pcrs = resolvedPCRs;
    if (pcrs == null || pathNodeKey > maxKnownPCR) {
      pcrs = pathSummaryReader.getPCRsForPaths(paths);
      resolvedPCRs = pcrs;
      maxKnownPCR = Math.max(pathNodeKey, pathSummaryReader.getMaxNodeKey());
    }
    return pcrs.contains(pathNodeKey);
  }

  /**
   * Add {@code nodeKey} to {@code pathNodeKey}'s posting list.
   *
   * <p>
   * A slot write OR-merges the incoming bitmap into the stored one, so the references already
   * recorded for {@code pathNodeKey} need neither be read back nor re-inserted — doing so cost one
   * range scan plus one full trie descent per already-stored node key, and every node under an
   * indexed path shares a single PATH key, so that grew with the whole index.
   * </p>
   */
  private void handleInsert(final long nodeKey, final long pathNodeKey) {
    // The writer uses primitive longs: no key or posting-reference boxing on this path.
    hotWriter.indexNodeKey(pathNodeKey, nodeKey);
  }

}
