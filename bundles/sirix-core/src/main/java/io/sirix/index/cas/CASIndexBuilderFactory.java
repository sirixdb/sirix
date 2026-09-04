package io.sirix.index.cas;

import static java.util.Objects.requireNonNull;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.hot.CASKeySerializer;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.path.summary.PathSummaryReader;

/**
 * Factory for creating CAS index builders.
 */
public final class CASIndexBuilderFactory {
  public CASIndexBuilder create(final StorageEngineWriter storageEngineWriter,
      final PathSummaryReader pathSummaryReader, final IndexDef indexDef) {
    requireNonNull(storageEngineWriter);
    requireNonNull(indexDef);
    if (indexDef.getType() != IndexType.CAS) {
      throw new IllegalArgumentException("CAS builder requires an IndexType.CAS definition");
    }
    final var pathSummary = requireNonNull(pathSummaryReader);
    final var paths = requireNonNull(indexDef.getPaths());
    final var type = requireNonNull(indexDef.getContentType());
    final var indexWriter =
        HOTIndexWriter.create(storageEngineWriter, CASKeySerializer.INSTANCE, IndexType.CAS, indexDef.getID());
    return new CASIndexBuilder(indexWriter, pathSummary, paths, type);
  }
}
