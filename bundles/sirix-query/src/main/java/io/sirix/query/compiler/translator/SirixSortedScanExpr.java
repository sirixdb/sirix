package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.util.ArrayList;

/**
 * Projection-served SORTED SCAN (P5b stage 7b; gap 1b generalized to N keys): {@code for
 * $r in P [where p] order by $r.f1 [descending], $r.f2 ... return $r}. Record keys come pre-sorted
 * from {@link SirixVectorizedExecutor#sortedScanRecordKeys} (stable, document-order tiebreaks);
 * records materialize through the document store by record key. Any decline evaluates the generic
 * pipeline compiled alongside — serving never changes an answer. Materialization is EAGER but
 * BOUNDED: a sole-consumer {@code fn:subsequence} with literal bounds caps the scan at
 * {@code limit} rows via heap selection (gap 3), so only the records that can ever be pulled are
 * materialized.
 */
public final class SirixSortedScanExpr implements Expr {

  private final SirixExecutorProvider executorProvider;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] orderFields;
  private final boolean[] descending;
  /** Top-K cap from a sole-consumer {@code fn:subsequence} ({@code -1} = unbounded). */
  private final long limit;
  /** Non-null for {@code return $r.field} (gap 1c): the single field dereffed per winner. */
  private final QNm returnFieldOrNull;
  /**
   * The same field as its RAW annotation name: a projection column is keyed by the name the detection
   * stage read off the deref, which {@link QNm}'s prefix parsing would not give back.
   */
  private final String returnFieldNameOrNull;
  /**
   * Carries the admitted source into the revision-stable evaluation lease and the runtime gate: a
   * {@link SourceRef.Kind#VARIABLE} ref cannot be judged at compile time, so this expr re-checks the
   * binding at evaluation time and declines to its generic fallback when it is foreign.
   */
  private final SourceRef sourceRef;
  private final Expr genericFallback;

  public SirixSortedScanExpr(final SirixExecutorProvider executorProvider, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] orderFields, final boolean[] descending, final long limit,
      final String returnFieldOrNull, final SourceRef sourceRef, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.orderFields = orderFields;
    this.descending = descending;
    this.limit = limit;
    this.returnFieldOrNull = returnFieldOrNull == null
        ? null
        : new QNm(returnFieldOrNull);
    this.returnFieldNameOrNull = returnFieldOrNull;
    this.sourceRef = sourceRef;
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final SirixExecutorProvider.Lease lease = executorProvider.acquire(ctx, sourceRef);
    if (lease != null) {
      try (lease) {
        final SirixVectorizedExecutor executor = lease.executor();
        final boolean sourceAccepted = sourceRef == null || executor.acceptsSource(sourceRef, ctx);
        if (sourceAccepted && executor.canExecute(ctx) && ctx instanceof SirixQueryContext sirixCtx) {
          // VALUE EMISSION (stage 7e): an unordered scan returning ONE field of the row needs no
          // record at all when that field is a projected column — the values come off the same
          // predicate mask's surviving rows, in the same document order, and not a single record
          // page is touched. Declines fall through to the record-materializing route below.
          if (orderFields == null && returnFieldOrNull != null) {
            final Sequence emitted =
                executor.predicateScanFieldValues(sourcePath, predicateOrNull, returnFieldNameOrNull, limit);
            if (emitted != null) {
              SirixVectorizedExecutor.markPredicateValueEmissionServed();
              return emitted;
            }
          }
          // A null orderFields is the PREDICATE SCAN (stage 7d): same materialization, keys in
          // document order straight from the predicate mask — no sort columns at all.
          final long[] keys = orderFields == null
              ? executor.predicateScanRecordKeys(sourcePath, predicateOrNull, limit)
              : executor.sortedScanRecordKeys(sourcePath, predicateOrNull, orderFields, descending, limit);
          if (keys != null) {
            final JsonDBCollection collection =
                (JsonDBCollection) sirixCtx.getJsonItemStore().lookup(executor.boundDatabaseName());
            if (collection != null) {
              try {
                // One coordinator-side readahead pass covers both arms: madvise is a property of
                // the SHARED mapping, not of the issuing reader, so the lanes benefit too.
                executor.prefetchWinnerRecordPages(keys);
                final Item[] slots = new Item[keys.length];
                if (keys.length >= PARALLEL_MATERIALIZE_MIN) {
                  // Each record materialization decodes the record's WHOLE slotted page on first
                  // touch (~ms each, and point-lookup winners scatter across pages), so a large
                  // result serially was the cold-path whale. Each lane gets a short-lived construction
                  // cursor; the returned items detach onto the bounded consumer pool before publication.
                  final int lanes = Math.min(executor.recordTrxLaneCount(), keys.length);
                  final int chunk = (keys.length + lanes - 1) / lanes;
                  executor.parallelRecordMaterialization(lanes, lane -> {
                    final JsonNodeReadOnlyTrx laneRtx = executor.recordTrxAt(lane);
                    try {
                      final JsonItemFactory laneFactory = JsonItemFactory.INSTANCE;
                      final int from = lane * chunk;
                      final int to = Math.min(from + chunk, keys.length);
                      for (int i = from; i < to; i++) {
                        slots[i] = materializeOne(laneRtx, laneFactory, keys[i], collection);
                      }
                    } finally {
                      // Items retain the proxy, not the construction transaction. Detach before this
                      // lane publishes completion so retirement retains no cursor per lane/revision.
                      executor.releaseRecordTrx(laneRtx);
                    }
                  });
                } else {
                  final JsonNodeReadOnlyTrx rtx = executor.recordTrx();
                  try {
                    final JsonItemFactory factory = JsonItemFactory.INSTANCE;
                    for (int i = 0; i < keys.length; i++) {
                      slots[i] = materializeOne(rtx, factory, keys[i], collection);
                    }
                  } finally {
                    executor.releaseRecordTrx(rtx);
                  }
                }
                final ArrayList<Item> items = new ArrayList<>(keys.length);
                for (final Item slot : slots) {
                  if (slot != null) {
                    items.add(slot); // null = an unbounded return-field's empty deref — skipped
                  }
                }
                if (orderFields == null) {
                  SirixVectorizedExecutor.markPredicateScanServed();
                } else {
                  SirixVectorizedExecutor.markSortedScanServed();
                }
                return new ItemSequence(items.toArray(new Item[0]));
              } catch (final RuntimeException e) {
                SirixVectorizedExecutor.markSortedScanFailed(e);
              }
            }
          }
        }
      }
    }
    return genericFallback.evaluate(ctx, tuple);
  }

  /** Results at/above this size materialize across the executor's transaction lanes. */
  private static final int PARALLEL_MATERIALIZE_MIN = 64;

  /**
   * Materialize ONE record key: the record itself, its single return field, or {@code null} for an
   * UNBOUNDED return-field scan whose deref is empty (contributes nothing — exact semantics). Every
   * fail-loud guard of the serial path holds unchanged; under the parallel fan-out a throw propagates
   * through the join into the same fallback catch.
   */
  private Item materializeOne(final JsonNodeReadOnlyTrx rtx, final JsonItemFactory factory, final long key,
      final JsonDBCollection collection) {
    if (!rtx.moveTo(key)) {
      // Revisions are immutable: an unresolvable record key means projection corruption or a
      // key-encoding bug — never a benign skip. Fail loud; the caller falls back (and counts it).
      throw new IllegalStateException("sorted-scan record key " + key + " does not resolve at the bound revision");
    }
    final Item record = factory.getSequence(rtx, collection);
    if (returnFieldOrNull == null) {
      return record;
    }
    if (!(record instanceof Object obj)) {
      throw new IllegalStateException("sorted-scan return-field over a non-object record");
    }
    final Sequence fieldValue = obj.get(returnFieldOrNull);
    if (fieldValue == null) {
      if (limit >= 0) {
        // fn:subsequence counts ITEMS, not rows: a winner missing the field shifts the window
        // past the K rows fetched. Fall back rather than answer from an under-filled window.
        throw new IllegalStateException("sorted-scan winner row lacks return field " + returnFieldOrNull);
      }
      return null; // unbounded: an empty deref contributes nothing — exact
    }
    if (!(fieldValue instanceof Item fieldItem)) {
      throw new IllegalStateException("sorted-scan return field is not a single item: " + returnFieldOrNull);
    }
    return fieldItem;
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // ExprUtil.asItem unwraps singletons and raises XPTY0004 on >1 — the PipeExpr
    // contract (a bare instanceof check silently loses served sequences).
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
