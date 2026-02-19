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
 * Factory for creating PATH index builders.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends. The backend is
 * determined by the resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}
 * setting.
 * </p>
 */
public final class PathIndexBuilderFactory {

  private final DatabaseType databaseType;

  public PathIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a PATH index builder using the backend configured for the resource.
   */
  public PathIndexBuilder create(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return create(storageEngineWriter, pathSummaryReader, indexDef, isHOTEnabled(storageEngineWriter));
  }

  /**
   * Creates a PATH index builder with explicit backend selection.
   *
   * @param storageEngineWriter the storage engine writer
   * @param pathSummaryReader the path summary reader
   * @param indexDef the index definition
   * @param useHOT true to use HOT, false for RBTree
   * @return the PATH index builder
   */
  public PathIndexBuilder create(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef, final boolean useHOT) {
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());
    assert indexDef.getType() == IndexType.PATH;

    if (useHOT) {
      final var hotWriter = HOTLongIndexWriter.create(storageEngineWriter, IndexType.PATH, indexDef.getID());
      return new PathIndexBuilder(hotWriter, pathSummary, paths);
    } else {
      final var rbTreeWriter = RBTreeWriter.<Long, NodeReferences>getInstance(this.databaseType, storageEngineWriter,
          indexDef.getType(), indexDef.getID());
      return new PathIndexBuilder(rbTreeWriter, pathSummary, paths);
    }
  }

  /**
   * Checks if HOT indexes should be used for the given transaction.
   */
  private static boolean isHOTEnabled(final StorageEngineWriter storageEngineWriter) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(PathIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineWriter.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }
}
