package io.sirix.index.path;

import static java.util.Objects.requireNonNull;

import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.hot.HOTLongIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;

/** Factory for creating incremental persistent HOT PATH index listeners. */
public final class PathIndexListenerFactory {
  /** Creates a PATH index listener. */
  public PathIndexListener create(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    requireNonNull(storageEngineWriter);
    requireNonNull(indexDef);
    if (indexDef.getType() != IndexType.PATH) {
      throw new IllegalArgumentException("PATH listener requires an IndexType.PATH definition");
    }
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());

    final var hotWriter = HOTLongIndexWriter.create(storageEngineWriter, IndexType.PATH, indexDef.getID());
    return new PathIndexListener(paths, pathSummary, hotWriter);
  }
}
