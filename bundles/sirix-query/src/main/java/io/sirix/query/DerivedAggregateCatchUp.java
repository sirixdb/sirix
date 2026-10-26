package io.sirix.query;

import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.ResourceSession;
import io.sirix.page.UberPage;
import java.util.Objects;
import java.util.Arrays;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexSnapshot;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.changelog.TemporalAggregateCatchUpBudget;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.jspecify.annotations.Nullable;

/** Immutable derived base and lazily built tails owned by a coordinating query context. */
public final class DerivedAggregateCatchUp implements AutoCloseable {
  private final DerivedIndexSnapshot snapshot;
  private final ResourceSession<?, ?> sourceSession;
  private final UberPage publication;
  private final int sourceRevision;
  private TemporalAggregateCatchUpBudget.Budget budget;
  private @Nullable TailCache tailCache;
  private final Int2ObjectOpenHashMap<TemporalAggregateIndexes.CatchUp> tails = new Int2ObjectOpenHashMap<>();
  private final IntOpenHashSet declined = new IntOpenHashSet();

  private DerivedAggregateCatchUp(final StorageEngineReader source, final DerivedIndexSnapshot snapshot,
      final TemporalAggregateCatchUpBudget.Budget budget) {
    this.snapshot = snapshot;
    this.budget = budget;
    sourceSession = source.getResourceSession();
    publication = source.getUberPage();
    sourceRevision = source.getRevisionNumber();
  }

  public static @Nullable DerivedAggregateCatchUp open(final StorageEngineReader source) {
    return open(source, TemporalAggregateCatchUpBudget.Budget.complete());
  }

  static @Nullable DerivedAggregateCatchUp open(final StorageEngineReader source,
      final TemporalAggregateCatchUpBudget.Budget budget) {
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

  void useBudget(final TemporalAggregateCatchUpBudget.Budget budget) {
    if (this.budget != budget) {
      this.budget = Objects.requireNonNull(budget);
      declined.clear();
    }
  }

  void useTails(final TailCache cache) {
    tailCache = Objects.requireNonNull(cache);
  }

  boolean needsPrefix() {
    // The first historical point can be only one revision past the base even when later
    // points in the same pinned publication need its prefix. Keep that first operand reusable.
    return publication.getRevisionNumber() - snapshot.sourceRevision() > 1;
  }

  /** Bounded context-owned prefixes shared by monotonically increasing historical views. */
  static final class TailCache {
    private final TemporalAggregateIndexes.CatchUpAccumulator[] prefixes =
        new TemporalAggregateIndexes.CatchUpAccumulator[64];

    TemporalAggregateIndexes.CatchUpAccumulator borrow(final StorageEngineReader source, final int base,
        final IndexDef definition) {
      final int slot = (System.identityHashCode(source.getResourceSession()) + 31 * base + definition.getID()) & 63;
      TemporalAggregateIndexes.CatchUpAccumulator prefix = prefixes[slot];
      if (prefix == null || !prefix.matches(source, base, definition)) {
        prefix = new TemporalAggregateIndexes.CatchUpAccumulator(source, base, definition);
        prefixes[slot] = prefix;
      }
      return prefix;
    }

    void clear() {
      Arrays.fill(prefixes, null);
    }
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
        if (tailCache == null) {
          result = TemporalAggregateIndexes.catchUp(source, snapshot.sourceRevision(), definition, projection, budget);
        } else {
          final TemporalAggregateIndexes.CatchUpAccumulator prefix =
              tailCache.borrow(source, snapshot.sourceRevision(), definition);
          result = TemporalAggregateIndexes.catchUpIncrementally(source, snapshot.sourceRevision(), definition,
              projection, budget, prefix);
        }
        tails.put(id, result);
      } catch (final TemporalAggregateCatchUpBudget.LimitExceeded exceeded) {
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
