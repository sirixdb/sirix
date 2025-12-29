package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.access.DatabaseType;
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
 * <p>Supports both traditional RBTree and high-performance HOT index backends.</p>
 */
public final class NameIndexListenerFactory {

  /**
   * System property to enable HOT indexes globally.
   * Set -Dsirix.index.useHOT=true to enable.
   */
  public static final String USE_HOT_PROPERTY = "sirix.index.useHOT";

  private final DatabaseType databaseType;

  public NameIndexListenerFactory(final DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  /**
   * Creates a NAME index listener using the default backend.
   */
  public NameIndexListener create(final StorageEngineWriter pageWriteTrx,
      final IndexDef indexDefinition) {
    return create(pageWriteTrx, indexDefinition, isHOTEnabled());
  }

  /**
   * Creates a NAME index listener with explicit backend selection.
   *
   * @param pageWriteTrx    the storage engine writer
   * @param indexDefinition the index definition
   * @param useHOT          true to use HOT, false for RBTree
   * @return the NAME index listener
   */
  public NameIndexListener create(final StorageEngineWriter pageWriteTrx,
      final IndexDef indexDefinition, final boolean useHOT) {
    final var includes = requireNonNull(indexDefinition.getIncluded());
    final var excludes = requireNonNull(indexDefinition.getExcluded());
    assert indexDefinition.getType() == IndexType.NAME;

    if (useHOT) {
      final var hotWriter = HOTIndexWriter.create(
          pageWriteTrx, NameKeySerializer.INSTANCE, IndexType.NAME, indexDefinition.getID());
      return new NameIndexListener(includes, excludes, hotWriter);
    } else {
      final var rbTreeWriter = RBTreeWriter.<QNm, NodeReferences>getInstance(
          this.databaseType, pageWriteTrx, indexDefinition.getType(), indexDefinition.getID());
      return new NameIndexListener(includes, excludes, rbTreeWriter);
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
