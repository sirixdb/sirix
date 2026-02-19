package io.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.access.IndexBackendType;
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
 * Factory for creating CAS index builders.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends. The backend is
 * determined by the resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}
 * setting.
 * </p>
 */
public final class CASIndexBuilderFactory {

  private final DatabaseType databaseType;

  public CASIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a CAS index builder using the backend configured for the resource.
   */
  public CASIndexBuilder create(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef) {
    return create(storageEngineWriter, pathSummaryReader, indexDef, isHOTEnabled(storageEngineWriter));
  }

  /**
   * Creates a CAS index builder with explicit backend selection.
   */
  public CASIndexBuilder create(final StorageEngineWriter storageEngineWriter, final PathSummaryReader pathSummaryReader,
      final IndexDef indexDef, final boolean useHOT) {
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());
    final var type = requireNonNull(indexDef.getContentType());

    if (useHOT) {
      final var hotWriter = HOTIndexWriter.create(storageEngineWriter, CASKeySerializer.INSTANCE, IndexType.CAS, indexDef.getID());
      return new CASIndexBuilder(hotWriter, pathSummary, paths, type);
    } else {
      final var rbTreeWriter = RBTreeWriter.<CASValue, NodeReferences>getInstance(this.databaseType, storageEngineWriter,
          indexDef.getType(), indexDef.getID());
      return new CASIndexBuilder(rbTreeWriter, pathSummary, paths, type);
    }
  }

  /**
   * Checks if HOT indexes should be used for the given transaction.
   */
  private static boolean isHOTEnabled(final StorageEngineWriter storageEngineWriter) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(CASIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineWriter.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }
}
