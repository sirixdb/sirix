package io.sirix.index.cas;

import io.sirix.access.trx.node.IndexController;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixRuntimeException;
import io.sirix.index.AtomicUtil;
import io.sirix.index.IndexDef;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.path.summary.PathSummaryReader;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Listener for CAS (Content-and-Structure) index changes.
 */
public final class CASIndexListener {

  private static final Logger logger = LoggerFactory.getLogger(CASIndexListener.class);

  private final HOTIndexWriter<CASValue> indexWriter;
  private final PathSummaryReader pathSummaryReader;
  private final Set<Path<QNm>> paths;
  private final Type type;
  private final IndexDef definition;
  private @Nullable LongSet resolvedPCRs;
  private long maxKnownPCR = -1L;

  public CASIndexListener(final PathSummaryReader pathSummaryReader, final HOTIndexWriter<CASValue> indexWriter,
      final IndexDef definition) {
    this.pathSummaryReader = requireNonNull(pathSummaryReader);
    this.indexWriter = requireNonNull(indexWriter);
    this.definition = requireNonNull(definition);
    this.paths = requireNonNull(definition.getPaths());
    this.type = requireNonNull(definition.getContentType());
  }

  public void listen(final IndexController.ChangeType type, final ImmutableNode node, final long pathNodeKey,
      final Atomic value) {
    listen(type, node.getNodeKey(), pathNodeKey, value);
  }

  public void listen(final IndexController.ChangeType type, final long nodeKey, final long pathNodeKey,
      final Atomic value) {
    final boolean matchesPath = matchesIndexedPath(pathNodeKey);
    switch (type) {
      case INSERT -> {
        // An empty path set means "index ALL paths" (matching CASIndexBuilder); the old guard
        // checked only getPCRsForPaths(paths).contains(...), which is the empty set for an empty
        // path config — so a `jn:create-cas-index($doc,'xs:string')` indexed existing data but
        // every subsequent insert was invisible to the index. (PathIndexListener has this guard.)
        if (matchesPath) {
          insert(nodeKey, pathNodeKey, value);
        }
      }
      case DELETE -> {
        if (matchesPath) {
          // Converted exactly as insert() converts, because the two must build the SAME key or the
          // delete misses the entry it is meant to remove. A value that does not convert was never
          // indexed, so there is nothing to remove and skipping is right.
          final Atomic typedValue = toTypedOrNull(value, nodeKey);
          if (typedValue == null) {
            return;
          }
          final CASValue casValue = new CASValue(typedValue, this.type, pathNodeKey);
          indexWriter.remove(casValue, nodeKey);
        }
      }
      default -> {
      }
    }
  }

  /** Resolve configured paths once, refreshing only when a newly minted PCR can change the answer. */
  private boolean matchesIndexedPath(final long pathNodeKey) {
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

  public void rejectValue(final long pathNodeKey, final boolean arrayField) {
    if (!type.isNumeric() || !definition.hasNumericValuesOnly()) {
      return;
    }
    if (matchesIndexedPath(pathNodeKey) || arrayField
        && matchesIndexedPath(pathSummaryReader.getPathNodeForPathNodeKey(pathNodeKey).getParentKey())) {
      definition.markNonNumericValue();
    }
  }

  public Type getContentType() {
    return type;
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
  private @Nullable Atomic toTypedOrNull(final Atomic value, final long nodeKey) {
    try {
      return AtomicUtil.toIndexType(value, definition);
    } catch (final SirixRuntimeException e) {
      logger.debug("Value '{}' is not of type {}, skipping CAS index entry for node {}", value, type, nodeKey, e);
      return null;
    }
  }

  private void insert(final long nodeKey, final long pathNodeKey, final Atomic value) throws SirixIOException {
    final Atomic typedValue = toTypedOrNull(value, nodeKey);
    final boolean isOfType = typedValue != null;

    if (isOfType) {
      final CASValue indexValue = new CASValue(typedValue, type, pathNodeKey);
      indexWriter.indexNodeKey(indexValue, nodeKey);
    }
  }

}
