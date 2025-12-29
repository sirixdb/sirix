package io.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.index.path.summary.PathSummaryReader;

/**
 * Factory for creating CAS index listeners.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class CASIndexListenerFactory {

  /**
   * System property to enable HOT indexes globally.
   * Set -Dsirix.index.useHOT=true to enable.
   */
  public static final String USE_HOT_PROPERTY = "sirix.index.useHOT";

  private final DatabaseType databaseType;

  public CASIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a CAS index listener using the default backend.
   */
  public CASIndexListener create(final StorageEngineWriter pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    return create(pageTrx, pathSummaryReader, indexDef, isHOTEnabled());
  }

  /**
   * Creates a CAS index listener with explicit backend selection.
   *
   * @param pageTrx         the storage engine writer
   * @param pathSummaryReader the path summary reader
   * @param indexDef        the index definition
   * @param useHOT          true to use HOT, false for RBTree
   * @return the CAS index listener
   */
  public CASIndexListener create(final StorageEngineWriter pageTrx,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef, final boolean useHOT) {
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var type = requireNonNull(indexDef.getContentType());
    final var paths = requireNonNull(indexDef.getPaths());

    if (useHOT) {
      final var hotWriter = HOTIndexWriter.create(
          pageTrx, CASKeySerializer.INSTANCE, IndexType.CAS, indexDef.getID());
      return new CASIndexListener(pathSummary, hotWriter, paths, type);
    } else {
      final var rbTreeWriter = RBTreeWriter.<CASValue, NodeReferences>getInstance(
          this.databaseType, pageTrx, indexDef.getType(), indexDef.getID());
      return new CASIndexListener(pathSummary, rbTreeWriter, paths, type);
    }
  }

  /**
   * Checks if HOT indexes are enabled globally.
   *
   * @return true if HOT is enabled via system property
   */
  public static boolean isHOTEnabled() {
    return Boolean.getBoolean(USE_HOT_PROPERTY);
  }
}
