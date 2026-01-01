package io.sirix.index.path;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.access.IndexBackendType;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;

/**
 * Factory for creating PATH index listeners.
 * 
 * <p>Supports both traditional RBTree and high-performance HOT index backends.
 * The backend is determined by the resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}
 * setting.</p>
 */
public final class PathIndexListenerFactory {

  /**
   * System property to override HOT indexes globally (for testing).
   * Set -Dsirix.index.useHOT=true to enable regardless of resource configuration.
   * If not set, the resource configuration's indexBackendType is used.
   */
  public static final String USE_HOT_PROPERTY = "sirix.index.useHOT";

  private final DatabaseType databaseType;

  public PathIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a PATH index listener using the backend configured for the resource.
   * 
   * <p>The backend type is determined by checking:
   * <ol>
   *   <li>The system property {@code sirix.index.useHOT} (for testing override)</li>
   *   <li>The resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}</li>
   * </ol></p>
   */
  public PathIndexListener create(final StorageEngineWriter pageTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return create(pageTrx, pathSummaryReader, indexDef, isHOTEnabled(pageTrx));
  }

  /**
   * Creates a PATH index listener with explicit backend selection.
   *
   * @param pageTrx         the storage engine writer
   * @param pathSummaryReader the path summary reader
   * @param indexDef        the index definition
   * @param useHOT          true to use HOT, false for RBTree
   * @return the PATH index listener
   */
  public PathIndexListener create(final StorageEngineWriter pageTrx, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef, final boolean useHOT) {
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());

    if (useHOT) {
      final var hotWriter = HOTLongIndexWriter.create(pageTrx, IndexType.PATH, indexDef.getID());
      return new PathIndexListener(paths, pathSummary, hotWriter);
    } else {
      final var rbTreeWriter = RBTreeWriter.<Long, NodeReferences>getInstance(this.databaseType,
                                                                               pageTrx,
                                                                               indexDef.getType(),
                                                                               indexDef.getID());
      return new PathIndexListener(paths, pathSummary, rbTreeWriter);
    }
  }

  /**
   * Checks if HOT indexes should be used for the given transaction.
   * 
   * <p>Priority:
   * <ol>
   *   <li>System property override (for testing)</li>
   *   <li>Resource configuration setting</li>
   * </ol></p>
   *
   * @param pageTrx the storage engine writer providing access to resource configuration
   * @return true if HOT should be used
   */
  public static boolean isHOTEnabled(final StorageEngineWriter pageTrx) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }
    
    // Fall back to resource configuration
    final var resourceConfig = pageTrx.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT_TRIE;
  }

  /**
   * Checks if HOT indexes are enabled globally via system property.
   * 
   * @return true if HOT is enabled via system property
   * @deprecated Use {@link #isHOTEnabled(StorageEngineWriter)} for proper resource-aware configuration
   */
  @Deprecated
  public static boolean isHOTEnabled() {
    return Boolean.getBoolean(USE_HOT_PROPERTY);
  }
}
