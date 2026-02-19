package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
import io.sirix.access.IndexBackendType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.hot.NameKeySerializer;
import io.sirix.index.redblacktree.RBTreeWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.brackit.query.atomic.QNm;

/**
 * Factory for creating NAME index builders.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends. The backend is
 * determined by the resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}
 * setting.
 * </p>
 */
public final class NameIndexBuilderFactory {

  private final DatabaseType databaseType;

  public NameIndexBuilderFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a NAME index builder using the backend configured for the resource.
   */
  public NameIndexBuilder create(final StorageEngineWriter storageEngineWriter, final IndexDef indexDefinition) {
    return create(storageEngineWriter, indexDefinition, isHOTEnabled(storageEngineWriter));
  }

  /**
   * Creates a NAME index builder with explicit backend selection.
   */
  public NameIndexBuilder create(final StorageEngineWriter storageEngineWriter, final IndexDef indexDefinition,
      final boolean useHOT) {
    final var includes = requireNonNull(indexDefinition.getIncluded());
    final var excludes = requireNonNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;

    if (useHOT) {
      final var hotWriter =
          HOTIndexWriter.create(storageEngineWriter, NameKeySerializer.INSTANCE, IndexType.NAME, indexDefinition.getID());
      return new NameIndexBuilder(includes, excludes, hotWriter, storageEngineWriter);
    } else {
      final var rbTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(this.databaseType, storageEngineWriter,
          indexDefinition.getType(), indexDefinition.getID());
      return new NameIndexBuilder(includes, excludes, rbTreeWriter, storageEngineWriter);
    }
  }

  /**
   * Checks if HOT indexes should be used for the given transaction.
   */
  private static boolean isHOTEnabled(final StorageEngineWriter storageEngineWriter) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(NameIndexListenerFactory.USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineWriter.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }
}
