package io.sirix.index.aggregate;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.changelog.ChangeLogSlab;

import java.util.Objects;
import java.util.function.ObjIntConsumer;

/** First derived-index implementation, reusing the synchronous signed-interval kernel. */
final class TemporalAggregateDerivedIndex implements DerivedIndex {
  @Override
  public void build(final StorageEngineWriter destination, final JsonNodeReadOnlyTrx source, final IndexDef definition,
      final Indexes sourceIndexes) {
    TemporalAggregateIndexes.buildDerived(destination, source, definition, projection(definition, sourceIndexes));
  }

  @Override
  public void applySlab(final StorageEngineWriter destination, final IndexDef definition, final Indexes sourceIndexes,
      final ResourceConfiguration sourceConfiguration, final ChangeLogSlab slab,
      final ObjIntConsumer<byte[]> foldCandidate) {
    TemporalAggregateIndexes.applyDerived(destination, definition, projection(definition, sourceIndexes),
        Objects.requireNonNull(sourceConfiguration.getValidTimeConfig()), slab, foldCandidate);
  }

  @Override
  public void fold(final StorageEngineWriter destination, final IndexDef definition, final Iterable<byte[]> groups) {
    TemporalAggregateIndexes.foldDerived(destination, definition, groups);
  }

  private static IndexDef projection(final IndexDef definition, final Indexes sourceIndexes) {
    return Objects.requireNonNull(
        sourceIndexes.getIndexDef(definition.getTemporalAggregateSpec().projectionId(), IndexType.PROJECTION));
  }
}
