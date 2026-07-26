package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonItemFactory;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.util.ArrayList;

/**
 * Projection-served SORTED SCAN (P5b stage 7b; gap 1b generalized to N keys): {@code for
 * $r in P [where p] order by $r.f1 [descending], $r.f2 ... return $r}. Record keys come
 * pre-sorted from {@link SirixVectorizedExecutor#sortedScanRecordKeys} (stable,
 * document-order tiebreaks); records materialize through the document store by record
 * key. Any decline evaluates the generic pipeline compiled alongside — serving never
 * changes an answer. Materialization is EAGER but BOUNDED: a sole-consumer
 * {@code fn:subsequence} with literal bounds caps the scan at {@code limit} rows via
 * heap selection (gap 3), so only the records that can ever be pulled are materialized.
 */
public final class SirixSortedScanExpr implements Expr {

  private final SirixVectorizedExecutor executor;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] orderFields;
  private final boolean[] descending;
  /** Top-K cap from a sole-consumer {@code fn:subsequence} ({@code -1} = unbounded). */
  private final long limit;
  private final String databaseName;
  /** Non-null only for a VARIABLE source (external variable): re-verified per evaluation. */
  private final SourceRef runtimeSourceRef;
  private final Expr genericFallback;

  public SirixSortedScanExpr(final SirixVectorizedExecutor executor, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] orderFields, final boolean[] descending,
      final long limit, final String databaseName, final SourceRef runtimeSourceRef,
      final Expr genericFallback) {
    this.executor = executor;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.orderFields = orderFields;
    this.descending = descending;
    this.limit = limit;
    this.databaseName = databaseName;
    this.runtimeSourceRef = runtimeSourceRef;
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // Runtime source gate — see SirixGroupAggregateExpr#evaluate.
    if (runtimeSourceRef != null && !executor.acceptsSource(runtimeSourceRef, ctx)) {
      return genericFallback.evaluate(ctx, tuple);
    }
    if (executor.canExecute(ctx) && ctx instanceof SirixQueryContext sirixCtx) {
      final long[] keys =
          executor.sortedScanRecordKeys(sourcePath, predicateOrNull, orderFields, descending,
              limit);
      if (keys != null) {
        final JsonDBCollection collection =
            (JsonDBCollection) sirixCtx.getJsonItemStore().lookup(databaseName);
        if (collection != null) {
          try {
            // ONE cached read trx per executor (not per query): materialized items keep
            // reading fields through it lazily during serialization, so it cannot close
            // per-evaluate — the executor owns and closes it. Bounded leak: 1.
            final JsonNodeReadOnlyTrx rtx = executor.recordTrx();
            final JsonItemFactory factory = new JsonItemFactory();
            final ArrayList<Item> items = new ArrayList<>(keys.length);
            for (final long key : keys) {
              if (!rtx.moveTo(key)) {
                // Revisions are immutable: an unresolvable record key means projection
                // corruption or a key-encoding bug — never a benign skip. Fail loud;
                // the catch below falls back to the generic pipeline (and counts it).
                throw new IllegalStateException("sorted-scan record key " + key
                    + " does not resolve at the bound revision");
              }
              items.add(factory.getSequence(rtx, collection));
            }
            SirixVectorizedExecutor.markSortedScanServed();
            return new ItemSequence(items.toArray(new Item[0]));
          } catch (final RuntimeException e) {
            SirixVectorizedExecutor.markSortedScanFailed(e);
          }
        }
      }
    }
    return genericFallback.evaluate(ctx, tuple);
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
