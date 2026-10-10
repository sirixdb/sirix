package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.RoutedKeys;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.scan.MaskedColumns;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Objects;

/**
 * The executor-side description of one grouped aggregate over an index-routed source — the
 * parameters {@link SirixVectorizedExecutor#executeGroupByAggregate} takes, plus the computed lanes
 * — and the serving step that turns the routed source's instants into a revision and a row mask.
 * Shared by the plain and the correlated serving expressions.
 */
@SuppressWarnings("ArrayRecordComponent") // Shares the compiled request's arrays; no record equality is used.
record RoutedGroupRequest(String[] sourcePath, @Nullable PredicateNode predicate, String[] groupFields,
    String[] keyNames, String[] funcs, String[] aggFields, String[] outNames,
    SirixVectorizedExecutor.ComputedLane @Nullable [] computedLanes) {

  private static final DateTimeToInstant DATE_TIME_TO_INSTANT = new DateTimeToInstant();
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

  private static void diag(final String why) {
    if (DIAG) {
      System.err.println("[routed-serve] decline: " + why);
    }
  }

  /** The rows an index-routed source denotes: their record keys and the revision they live in. */
  @SuppressWarnings("ArrayRecordComponent") // Shares resolved keys; no record equality is used.
  record RoutedRows(long[] keys, int revision) {
  }

  /**
   * A membership filter over a second index-routed opener: rows of the main source are kept when
   * their {@code outerField} value is ({@code anti == false}) or is not ({@code anti == true}) among
   * the {@code innerField} values of the filter source's rows; a missing value matches nothing.
   */
  public record MembershipFilter(SirixGroupAggregateExpr.RoutedSource source, String innerField, String outerField,
      boolean anti) {
  }

  /**
   * Resolve the rows {@code routed} denotes under {@code tuple}: the instants are evaluated, the
   * revision is resolved from the transaction instant, the valid rows' record keys come from the
   * valid-time index (half-open, exact). {@code null} declines; the caller evaluates the generic
   * pipeline, which raises whatever the opener itself raises.
   */
  static @Nullable RoutedRows resolve(final QueryContext ctx, final Tuple tuple,
      final SirixGroupAggregateExpr.RoutedSource routed) throws QueryException {
    if (routed.indexed() != null) {
      final RoutedKeys rows = routed.indexed() instanceof SirixValidTimeScanExpr scan
          ? scan.routedKeys(ctx, tuple)
          : ValidTimeIntervalIndex.routedKeys(routed.indexed().evaluate(ctx, tuple));
      return rows == null
          ? null
          : new RoutedRows(rows.keys(), rows.revision());
    }
    final Expr txTimeExpr =
        Objects.requireNonNull(routed.txTime(), "a non-indexed routed source requires transaction time");
    final Item txItem = txTimeExpr.evaluateToItem(ctx, tuple);
    final Item validItem = routed.validTime().evaluateToItem(ctx, tuple);
    if (!(txItem instanceof DateTime txTime) || !(validItem instanceof DateTime validTime)) {
      diag("instants are not dateTimes");
      return null;
    }
    try {
      final Instant txInstant = DATE_TIME_TO_INSTANT.convert(txTime);
      final Instant validInstant = DATE_TIME_TO_INSTANT.convert(validTime);
      if (!(ctx.getJsonItemStore().lookup(routed.database()) instanceof JsonDBCollection collection)) {
        diag("no collection " + routed.database());
        return null;
      }
      final JsonDBItem document = collection.getDocument(routed.resource(), txInstant);
      if (document == null || document.getResourceSession().getResourceConfig().getValidTimeConfig() == null) {
        diag("no document or no valid-time configuration");
        return null;
      }
      return new RoutedRows(ValidTimeIntervalIndex.keys(document, validInstant, true),
          document.getTrx().getRevisionNumber());
    } catch (final RuntimeException notServable) {
      diag("row resolution failed: " + notServable);
      return null;
    }
  }

  /**
   * The main source's rows after a membership filter: the filter source's {@code innerField} values
   * are read under its own mask, then the main rows' {@code outerField} values decide which record
   * keys survive. {@code null} declines.
   */
  static @Nullable RoutedRows filterByMembership(final SirixExecutorProvider executorProvider, final QueryContext ctx,
      final Tuple tuple, final String[] sourcePath, final RoutedRows main,
      final SirixGroupAggregateExpr.RoutedSource mainSource, final MembershipFilter filter) throws QueryException {
    final RoutedRows inner = resolve(ctx, tuple, filter.source());
    if (inner == null) {
      return null;
    }
    if (main.keys().length == 0) {
      return main;
    }
    if (inner.keys().length == 0) {
      return filter.anti()
          ? main
          : new RoutedRows(inner.keys(), main.revision());
    }
    final LongOpenHashSet values;
    final SirixExecutorProvider.Lease innerLease = executorProvider.acquire(ctx,
        SourceRef.document(filter.source().database(), filter.source().resource(), inner.revision()));
    if (innerLease == null) {
      return null;
    }
    try (innerLease) {
      final SirixVectorizedExecutor executor = innerLease.executor();
      if (executor.getRevision() != inner.revision() || !executor.canExecute(ctx)) {
        return null;
      }
      final MaskedColumns columns =
          executor.maskedColumns(ARRAY_MEMBERS, inner.keys(), new String[] {filter.innerField()});
      if (columns == null || !columns.isLong(0)) {
        return null;
      }
      values = columns.presentLongValues(0);
    }
    final SirixExecutorProvider.Lease mainLease = executorProvider.acquire(ctx,
        SourceRef.document(mainSource.database(), mainSource.resource(), main.revision()));
    if (mainLease == null) {
      return null;
    }
    try (mainLease) {
      final SirixVectorizedExecutor executor = mainLease.executor();
      if (executor.getRevision() != main.revision() || !executor.canExecute(ctx)) {
        return null;
      }
      final MaskedColumns columns = executor.maskedColumns(sourcePath, main.keys(), new String[] {filter.outerField()});
      if (columns == null || !columns.isLong(0)) {
        return null;
      }
      return new RoutedRows(columns.recordKeysByMembership(0, values, filter.anti()), main.revision());
    }
  }

  private static final String[] ARRAY_MEMBERS = {"[]"};

  /**
   * Serve the request for the rows {@code routed} denotes under {@code tuple}, through the executor
   * bound to their revision. {@code null} declines.
   */
  SirixVectorizedExecutor.@Nullable ServedGroups serve(final SirixExecutorProvider executorProvider,
      final QueryContext ctx, final Tuple tuple, final SirixGroupAggregateExpr.RoutedSource routed)
      throws QueryException {
    final RoutedRows rows = resolve(ctx, tuple, routed);
    if (rows == null) {
      return null;
    }
    final long[] keys = rows.keys();
    final int revision = rows.revision();
    final SirixExecutorProvider.Lease lease =
        executorProvider.acquire(ctx, SourceRef.document(routed.database(), routed.resource(), revision));
    if (lease == null) {
      diag("no executor lease for revision " + revision);
      return null;
    }
    try (lease) {
      final SirixVectorizedExecutor executor = lease.executor();
      if (executor.getRevision() != revision || !executor.canExecute(ctx)) {
        diag("executor at revision " + executor.getRevision() + ", wanted " + revision);
        return null;
      }
      // No order plan: the caller orders the merged groups itself; no limit, no key transforms, no
      // conditional or regex keys, no having — the correlated detection admits none of them.
      return executor.executeGroupByAggregate(ctx, sourcePath, predicate, groupFields, keyNames, funcs, aggFields,
          outNames, null, null, null, -1L, null, null, null, null, null, null, null, null, null, null,
          new SirixVectorizedExecutor.GroupRouting(keys, computedLanes));
    }
  }
}
