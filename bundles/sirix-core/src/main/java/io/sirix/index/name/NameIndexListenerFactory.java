package io.sirix.index.name;

import static java.util.Objects.requireNonNull;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.hot.NameKeySerializer;

/** Factory for creating canonical HOT-backed NAME index listeners. */
public final class NameIndexListenerFactory {

  /** Creates the incremental listener for the definition's one HOT tree. */
  public NameIndexListener create(final StorageEngineWriter storageEngineWriter, final IndexDef indexDefinition) {
    requireNonNull(storageEngineWriter);
    requireNonNull(indexDefinition);
    if (indexDefinition.getType() != IndexType.NAME) {
      throw new IllegalArgumentException("NAME listener requires an IndexType.NAME definition");
    }
    final var includes = requireNonNull(indexDefinition.getIncluded());
    final var excludes = requireNonNull(indexDefinition.getExcluded());

    final var hotWriter =
        HOTIndexWriter.create(storageEngineWriter, NameKeySerializer.INSTANCE, IndexType.NAME, indexDefinition.getID());
    return new NameIndexListener(includes, excludes, hotWriter);
  }
}
