package io.sirix.query;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.ResourceSession;
import io.sirix.page.UberPage;
import java.util.Objects;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexSnapshot;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.changelog.ChangeLogGroupReader;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.jspecify.annotations.Nullable;

/** Immutable derived base and lazily built tails owned by a coordinating query context. */
public final class DerivedAggregateCatchUp implements AutoCloseable {
  private final DerivedIndexSnapshot snapshot;
  private final ResourceSession<?, ?> sourceSession;
  private final UberPage publication;
  private final int sourceRevision;
  private final ChangeLogGroupReader.Budget budget;
  private final Int2ObjectOpenHashMap<TemporalAggregateIndexes.CatchUp> tails = new Int2ObjectOpenHashMap<>();
  private final IntOpenHashSet declined = new IntOpenHashSet();

  private DerivedAggregateCatchUp(final StorageEngineReader source, final DerivedIndexSnapshot snapshot,
      final ChangeLogGroupReader.Budget budget) {
    this.snapshot = snapshot;
    this.budget = budget;
    sourceSession = source.getResourceSession();
    publication = source.getUberPage();
    sourceRevision = source.getRevisionNumber();
  }

  public static @Nullable DerivedAggregateCatchUp open(final StorageEngineReader source) {
    return open(source, new ChangeLogGroupReader.Budget());
  }

  static @Nullable DerivedAggregateCatchUp open(final StorageEngineReader source,
      final ChangeLogGroupReader.Budget budget) {
    Objects.requireNonNull(source);
    Objects.requireNonNull(budget);
    if (source instanceof StorageEngineWriter || source.hasTrxIntentLog()) {
      return null;
    }
    final DerivedIndexMaintainer.Resource resource =
        ((AbstractResourceSession<?, ?>) source.getResourceSession()).getDerivedResource();
    if (resource == null) {
      return null;
    }
    final DerivedIndexSnapshot snapshot = resource.openCatchUpSnapshot(source);
    return snapshot == null
        ? null
        : new DerivedAggregateCatchUp(source, snapshot, budget);
  }

  public StorageEngineReader reader() {
    return snapshot.reader();
  }

  @SuppressWarnings("ReferenceEquality") // Publication identity distinguishes reused revision numbers.
  public TemporalAggregateIndexes.@Nullable CatchUp tail(final StorageEngineReader source, final IndexDef definition,
      final IndexDef projection) {
    Objects.requireNonNull(definition);
    Objects.requireNonNull(projection);
    if (source.getResourceSession() != sourceSession || source.getUberPage() != publication
        || source.getRevisionNumber() != sourceRevision || reader().isClosed()) {
      throw new IllegalArgumentException("Catch-up belongs to one immutable source publication");
    }
    final int id = definition.getID();
    if (declined.contains(id)) {
      return null;
    }
    TemporalAggregateIndexes.CatchUp result = tails.get(id);
    if (result == null) {
      final JsonResourceSession session = (JsonResourceSession) reader().getResourceSession();
      final IndexDef base = session.getRtxIndexController(reader().getRevisionNumber())
                                   .getIndexes()
                                   .getIndexDef(id, IndexType.TEMPORAL_AGGREGATE);
      if (base == null || !base.hasSameDefinition(definition)) {
        declined.add(id);
        return null;
      }
      try {
        result = TemporalAggregateIndexes.catchUp(source, snapshot.sourceRevision(), definition, projection, budget);
        tails.put(id, result);
      } catch (final ChangeLogGroupReader.LimitExceeded exceeded) {
        declined.add(id);
        return null;
      }
    }
    return result;
  }

  @Override
  public void close() {
    snapshot.close();
  }
}
