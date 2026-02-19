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
 * Factory for creating NAME index listeners.
 * 
 * <p>
 * Supports both traditional RBTree and high-performance HOT index backends. The backend is
 * determined by the resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}
 * setting.
 * </p>
 */
public final class NameIndexListenerFactory {

  /**
   * System property to override HOT indexes globally (for testing). Set -Dsirix.index.useHOT=true to
   * enable regardless of resource configuration. If not set, the resource configuration's
   * indexBackendType is used.
   */
  public static final String USE_HOT_PROPERTY = "sirix.index.useHOT";

  private final DatabaseType databaseType;

  public NameIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a NAME index listener using the backend configured for the resource.
   * 
   * <p>
   * The backend type is determined by checking:
   * <ol>
   * <li>The system property {@code sirix.index.useHOT} (for testing override)</li>
   * <li>The resource's {@link io.sirix.access.ResourceConfiguration#indexBackendType}</li>
   * </ol>
   * </p>
   */
  public NameIndexListener create(final StorageEngineWriter pageWriteTrx, final IndexDef indexDefinition) {
    return create(pageWriteTrx, indexDefinition, isHOTEnabled(pageWriteTrx));
  }

  /**
   * Creates a NAME index listener with explicit backend selection.
   *
   * @param pageWriteTrx the storage engine writer
   * @param indexDefinition the index definition
   * @param useHOT true to use HOT, false for RBTree
   * @return the NAME index listener
   */
  public NameIndexListener create(final StorageEngineWriter pageWriteTrx, final IndexDef indexDefinition,
      final boolean useHOT) {
    final var includes = requireNonNull(indexDefinition.getIncluded());
    final var excludes = requireNonNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;

    if (useHOT) {
      final var hotWriter =
          HOTIndexWriter.create(pageWriteTrx, NameKeySerializer.INSTANCE, IndexType.NAME, indexDefinition.getID());
      return new NameIndexListener(includes, excludes, hotWriter);
    } else {
      final var rbTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(this.databaseType, pageWriteTrx,
          indexDefinition.getType(), indexDefinition.getID());
      return new NameIndexListener(includes, excludes, rbTreeWriter);
    }
  }

  /**
   * Checks if HOT indexes should be used for the given transaction.
   * 
   * <p>
   * Priority:
   * <ol>
   * <li>System property override (for testing)</li>
   * <li>Resource configuration setting</li>
   * </ol>
   * </p>
   *
   * @param storageEngineWriter the storage engine writer providing access to resource configuration
   * @return true if HOT should be used
   */
  public static boolean isHOTEnabled(final StorageEngineWriter storageEngineWriter) {
    // System property takes precedence (for testing)
    final String sysProp = System.getProperty(USE_HOT_PROPERTY);
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    // Fall back to resource configuration
    final var resourceConfig = storageEngineWriter.getResourceSession().getResourceConfig();
    return resourceConfig.indexBackendType == IndexBackendType.HOT;
  }

  /**
   * Checks if HOT indexes are enabled globally via system property.
   * 
   * @return true if HOT is enabled via system property
   * @deprecated Use {@link #isHOTEnabled(StorageEngineWriter)} for proper resource-aware
   *             configuration
   */
  @Deprecated
  public static boolean isHOTEnabled() {
    return Boolean.getBoolean(USE_HOT_PROPERTY);
  }
}
