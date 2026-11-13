package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.util.sort.Ordering;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionComputedColumn;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionTemporalCodec;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.query.compiler.translator.JoinKernels.BucketedSide;
import io.sirix.query.compiler.translator.JoinKernels.DistinctDomain;
import io.sirix.query.compiler.translator.JoinKernels.ExtremaFold;
import io.sirix.query.compiler.translator.JoinKernels.SideLanes;
import io.sirix.query.scan.DerivedCacheBudget;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import io.sirix.query.scan.MaskedColumns;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import static io.sirix.cache.ShardedPageCache.retainCleanupFailure;
import static io.sirix.cache.ShardedPageCache.rethrowCleanupFailure;
import java.util.concurrent.atomic.LongAdder;

/**
 * Serving of the COLUMN-SIDE equality join with a grouped aggregate (see
 * {@code JoinedGroupAggregateDetectionStage}): both sides' columns are read under their row masks,
 * the smaller side builds the join lookup, the larger probes it, and every matched pair folds into
 * a group keyed on fields of either side. No record object is built on either side; the group
 * records are ordered with Brackit's own {@link Ordering} under the pipeline's order-by, which
 * names every key. Anything the route cannot do exactly declines to the generic pipeline compiled
 * beside it: a string join key, an overflowing sum or program, a side without a covering
 * projection.
 */
public final class SirixJoinedGroupAggregateExpr implements Expr {

  /** One join side: its document, its routed instants (or none), its revision and its fields. */
  public record Side(String database, String resource, SirixGroupAggregateExpr.@Nullable RoutedSource routed,
      int revision, String joinField) {
  }

  private static final String[] ARRAY_MEMBERS = {"[]"};
  private static final String COMPUTED_PREFIX = "prog:";
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");
  /** Work counter: bucketed sides built for the dense extrema kernel (a cache hit builds none). */
  private static final LongAdder BUCKETED_SIDES_BUILT = new LongAdder();
  // Volatile publishes the executor reference; the executor provides its own task synchronization.
  @SuppressWarnings("java:S3077")
  private static volatile @Nullable Executor packingExecutorForTesting;

  // Volatile publishes the test callback reference; it does not protect mutable callback state.
  @SuppressWarnings("java:S3077")
  static volatile @Nullable Consumer<String> handoffHookForTesting;

  private static void handoff(final String point) {
    final Consumer<String> hook = handoffHookForTesting;
    if (hook != null) {
      hook.accept(point);
    }
  }

  static int inFlightPackingCountForTesting() {
    return BucketedSideCache.IN_FLIGHT.size();
  }

  static @Nullable CompletableFuture<BucketedSideCache.PackedSide> inFlightPackingForTesting() {
    final Iterator<CompletableFuture<BucketedSideCache.PackedSide>> futures =
        BucketedSideCache.IN_FLIGHT.values().iterator();
    return futures.hasNext()
        ? futures.next()
        : null;
  }

  /** Test seam for isolated fixtures that hold an optional packing task before execution. */
  public static @Nullable Executor setPackingExecutorForTesting(final @Nullable Executor executor) {
    final Executor previous = packingExecutorForTesting;
    packingExecutorForTesting = executor;
    return previous;
  }

  public static long bucketedSidesBuiltCount() {
    return BUCKETED_SIDES_BUILT.sum();
  }

  private static final byte COUNT = 0;
  private static final byte COUNT_DISTINCT = 1;
  private static final byte SUM = 2;
  private static final byte MIN = 3;
  private static final byte MAX = 4;

  private final SirixExecutorProvider executorProvider;
  private final Side[] sides;
  private final int[] keySides;
  private final String[] keyFields;
  private final String[] aggFuncs;
  private final byte[] aggKinds;
  private final boolean countPairs;
  private final int[] aggSides;
  private final String[] aggFields;
  private final int[][] progCode;
  private final long[][] progConsts;
  private final int[] entryKinds;
  private final int[] orderIndexes;
  private final Ordering.OrderModifier[] orderModifiers;
  /** Per ordering key: ascending; the primitive row order reads this instead of the modifiers. */
  private final boolean[] orderAsc;
  private final Expr genericFallback;
  private final boolean rowOutput;
  private final int[] residualSides;
  private final int[] residualSlots;
  private final int[] residualOps;
  private final boolean[] residualTemporal;
  private final int[] residualCode;
  private final boolean residualAnd;
  private final QNm[] outputNames;
  /** Per side, the distinct fields read from it (join key first), and each key/agg's slot in them. */
  private final String[][] sideFields;
  private final int[] keySlots;
  private final int[] aggSlots;
  private final int[] aggPrograms;
  private final int[][] progSlots;
  private final int[][] progOperandSides;

  public SirixJoinedGroupAggregateExpr(final SirixExecutorProvider executorProvider, final Side[] sides,
      final int[] keySides, final String[] keyFields, final String[] aggFuncs, final int[] aggSides,
      final String[] aggFields, final int[] progSides, final String[][] progFields, final int[][] progCode,
      final long[][] progConsts, final int[] entryKinds, final String[] entryNames, final int[] orderIndexes,
      final boolean[] orderAsc, final boolean[] orderEmptyLeast, final boolean rowOutput,
      final int @Nullable [] residualSides, final String @Nullable [] residualFields,
      final int @Nullable [] residualOps, final int @Nullable [] residualCode, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sides = sides;
    this.keySides = keySides;
    this.keyFields = keyFields;
    this.aggFuncs = aggFuncs;
    this.aggKinds = new byte[aggFuncs.length];
    this.countPairs = initializeAggregateKinds(aggFields);
    this.aggSides = aggSides;
    this.aggFields = aggFields;
    this.progCode = progCode;
    this.progConsts = progConsts;
    this.entryKinds = entryKinds;
    this.orderIndexes = orderIndexes;
    this.orderAsc = orderAsc.clone();
    this.orderModifiers = new Ordering.OrderModifier[orderIndexes.length];
    for (int i = 0; i < orderIndexes.length; i++) {
      orderModifiers[i] = new Ordering.OrderModifier(orderAsc[i], orderEmptyLeast[i], null);
    }
    this.genericFallback = genericFallback;
    this.rowOutput = rowOutput;
    this.residualSides = residualSides == null
        ? new int[0]
        : residualSides;
    this.residualOps = residualOps == null
        ? new int[0]
        : residualOps;
    this.residualTemporal = new boolean[this.residualSides.length];
    this.residualCode = residualCode == null
        ? new int[0]
        : residualCode;
    boolean conjunction = true;
    for (final int instruction : this.residualCode) {
      if (instruction < -1) {
        conjunction = false;
        break;
      }
    }
    this.residualAnd = conjunction;
    this.outputNames = new QNm[entryNames.length];
    for (int i = 0; i < entryNames.length; i++) {
      outputNames[i] = new QNm(entryNames[i]);
    }
    // Field rosters per side.
    final List<List<String>> rosters = List.of(new ArrayList<>(), new ArrayList<>());
    rosters.get(0).add(sides[0].joinField());
    rosters.get(1).add(sides[1].joinField());
    this.keySlots = new int[keyFields.length];
    for (int k = 0; k < keyFields.length; k++) {
      keySlots[k] = slot(rosters.get(keySides[k]), keyFields[k]);
    }
    this.progSlots = new int[progFields.length][];
    this.progOperandSides = new int[progFields.length][];
    initializeProgramSlots(progSides, progFields, rosters);
    this.aggSlots = new int[aggFields.length];
    this.aggPrograms = new int[aggFields.length];
    initializeAggregateSlots(rosters);
    this.residualSlots = new int[this.residualSides.length];
    if (residualSlots.length != 0) {
      final String[] fields = Objects.requireNonNull(residualFields, "residual operands require field names");
      for (int i = 0; i < residualSlots.length; i++) {
        residualTemporal[i] = fields[i].startsWith("timestamp:");
        residualSlots[i] = slot(rosters.get(this.residualSides[i]), fields[i].substring(residualTemporal[i]
            ? 10
            : 7));
      }
    }
    this.sideFields = new String[][] {rosters.get(0).toArray(new String[0]), rosters.get(1).toArray(new String[0])};
  }

  private void initializeAggregateSlots(final List<List<String>> rosters) {
    Arrays.fill(aggPrograms, -1);
    for (int a = 0; a < aggFields.length; a++) {
      if (aggFields[a] != null && aggFields[a].startsWith(COMPUTED_PREFIX)) {
        aggPrograms[a] = Integer.parseInt(aggFields[a].substring(COMPUTED_PREFIX.length()));
      }
      aggSlots[a] = aggFields[a] == null || aggFields[a].startsWith(COMPUTED_PREFIX)
          ? -1
          : slot(rosters.get(aggSides[a]), aggFields[a]);
    }
  }

  private boolean initializeAggregateKinds(final String[] aggFields) {
    boolean pairs = false;
    for (int a = 0; a < aggFuncs.length; a++) {
      aggKinds[a] = switch (aggFuncs[a]) {
        case "count" -> COUNT;
        case "count-distinct" -> COUNT_DISTINCT;
        case "sum" -> SUM;
        case "min" -> MIN;
        case "max" -> MAX;
        default -> throw new IllegalArgumentException("Unsupported joined aggregate: " + aggFuncs[a]);
      };
      pairs |= aggKinds[a] == COUNT && aggFields[a] == null;
    }
    return pairs;
  }

  private void initializeProgramSlots(final int[] progSides, final String[][] progFields,
      final List<List<String>> rosters) {
    for (int p = 0; p < progFields.length; p++) {
      progSlots[p] = new int[progFields[p].length];
      progOperandSides[p] = new int[progFields[p].length];
      for (int o = 0; o < progFields[p].length; o++) {
        final String field = progFields[p][o];
        final int side = progSides[p] < 0
            ? field.charAt(0) - '0'
            : progSides[p];
        progOperandSides[p][o] = side;
        progSlots[p][o] = slot(rosters.get(side), progSides[p] < 0
            ? field.substring(2)
            : field);
      }
    }
  }

  private static int slot(final List<String> roster, final String field) {
    int at = roster.indexOf(field);
    if (at < 0) {
      at = roster.size();
      roster.add(field);
    }
    return at;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final Sequence served = serve(ctx, tuple);
    return served != null
        ? served
        : genericFallback.evaluate(ctx, tuple);
  }

  /**
   * Diagnostics only: leaves whose rostered lanes shared one segment, per loop, since the last
   * report.
   */
  private static final LongAdder[] CAPTURED_SHARED = {new LongAdder(), new LongAdder()};
  private static final LongAdder[] CAPTURED_SPLIT = {new LongAdder(), new LongAdder()};

  private static void noteCapturedLeaf(final String loop, final boolean shared) {
    final int at = "build".equals(loop)
        ? 0
        : 1;
    (shared
        ? CAPTURED_SHARED[at]
        : CAPTURED_SPLIT[at]).increment();
  }

  private static String capturedLeavesReport() {
    return String.format("build leaves one-segment %d, split %d; probe leaves one-segment %d, split %d",
        CAPTURED_SHARED[0].sumThenReset(), CAPTURED_SPLIT[0].sumThenReset(), CAPTURED_SHARED[1].sumThenReset(),
        CAPTURED_SPLIT[1].sumThenReset());
  }

  private static @Nullable Sequence decline(final String why) {
    if (DIAG) {
      System.err.println("[join-serve] decline: " + why);
    }
    return null;
  }

  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  private @Nullable Sequence serve(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final long[][] keys = new long[2][];
    final int[] revisions = new int[2];
    for (int side = 0; side < 2; side++) {
      final Side spec = sides[side];
      if (spec.routed() != null) {
        final RoutedGroupRequest.RoutedRows rows = RoutedGroupRequest.resolve(ctx, tuple, spec.routed());
        if (rows == null) {
          return decline("side " + side + " has no routed rows");
        }
        if (rows.keys().length == 0) {
          return emptySide(side);
        }
        keys[side] = rows.keys();
        revisions[side] = rows.revision();
      } else {
        revisions[side] = spec.revision();
      }
    }
    final MaskedColumns[] columns = new MaskedColumns[2];
    Throwable primary = null;
    try {
      final String refused = loadSides(ctx, keys, revisions, columns);
      return refused != null
          ? decline(refused)
          : joinSides(columns);
    } catch (final Throwable failure) {
      primary = failure;
      throw failure;
    } finally {
      // Native views lease their resident snapshot; the result sequence no longer reads them.
      closeSides(columns, primary);
    }
  }

  /**
   * The answer for a routed side without rows: the join is empty, unless only the inner side is
   * routed, where the ordinary outer source evaluation has to run.
   */
  private @Nullable Sequence emptySide(final int side) {
    if (side == 1 && sides[0].routed() == null) {
      return decline("an empty inner mask requires the ordinary outer source evaluation");
    }
    SirixVectorizedExecutor.noteJoinGroupServed();
    return new ItemSequence();
  }

  /**
   * Loads the masked columns of both sides into {@code columns}; a side that is loaded stays there
   * for the caller to close.
   *
   * @return why the join cannot be served, or {@code null} when both sides are loaded
   */
  private @Nullable String loadSides(final QueryContext ctx, final long[][] keys, final int[] revisions,
      final MaskedColumns[] columns) {
    for (int side = 0; side < 2; side++) {
      columns[side] = columns(ctx, side, keys[side], revisions[side]);
      if (columns[side] == null) {
        return "side " + side + " has no masked columns";
      }
      if (!columns[side].isLong(0)) {
        return "join field of side " + side + " is not a long column";
      }
    }
    return null;
  }

  /**
   * Joins the two loaded sides, hashing the smaller one; declines on a column the join cannot read
   * and on an exact arithmetic overflow.
   */
  private @Nullable Sequence joinSides(final MaskedColumns[] columns) throws QueryException {
    final boolean[] stringKeys = new boolean[keyFields.length];
    final boolean[] timestampKeys = new boolean[keyFields.length];
    final String invalid = invalidColumns(columns, stringKeys, timestampKeys);
    if (invalid != null) {
      return decline(invalid);
    }
    // Hash the smaller side; string ids of both sides live in the build side's interner.
    final int build = columns[0].rows() <= columns[1].rows()
        ? 0
        : 1;
    final int probe = 1 - build;
    columns[build].adopt(columns[probe]);
    try {
      return join(columns[build], build, columns[probe], probe, stringKeys, timestampKeys);
    } catch (final ArithmeticException _) {
      return decline("exact arithmetic overflow");
    }
  }

  /**
   * Close the views of both sides; a failure of closing is kept on {@code primary} when there is one.
   */
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  @SuppressWarnings("java:S1181")
  private static void closeSides(final MaskedColumns[] columns, final @Nullable Throwable primary) {
    Throwable failure = primary;
    for (final MaskedColumns side : columns) {
      if (side != null) {
        try {
          side.close();
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
      }
    }
    rethrowCleanupFailure(failure);
  }

  /** One side's masked columns: the routed rows or every row of the literal document. */
  private @Nullable MaskedColumns columns(final QueryContext ctx, final int side, final long @Nullable [] keys,
      final int revision) {
    final Side spec = sides[side];
    final SirixExecutorProvider.Lease lease =
        executorProvider.acquire(ctx, SourceRef.document(spec.database(), spec.resource(), revision));
    if (lease == null) {
      return null;
    }
    try (lease) {
      final SirixVectorizedExecutor executor = lease.executor();
      if ((revision >= 0 && executor.getRevision() != revision) || !executor.canExecute(ctx)) {
        return null;
      }
      // A join side is the selection's only reader: its lanes go into one slab, keys or not.
      return executor.maskedColumns(ARRAY_MEMBERS, keys, sideFields[side], true);
    }
  }

  /** A build row's values: the join key and every rostered field, present or not. */
  private static final class BuildRows {
    final int fieldCount;
    final boolean densePresent;
    long[] values;
    boolean[] present;
    long[] recordKeys;
    /** Per build row, the next row of the same join key or -1; grown alongside the rows. */
    int[] nextRow = new int[16];
    int count;
    long minimumJoinKey = Long.MAX_VALUE;
    long maximumJoinKey = Long.MIN_VALUE;

    BuildRows(final int fieldCount, final long rows, final boolean densePresent) {
      this.fieldCount = fieldCount;
      this.densePresent = densePresent;
      final int capacity = (int) Math.min(Integer.MAX_VALUE - 8, Math.max(16, rows));
      values = new long[capacity * fieldCount];
      present = new boolean[capacity * fieldCount];
      if (densePresent) {
        Arrays.fill(present, true);
      }
      recordKeys = new long[capacity];
    }

    int add(final long recordKey) {
      if (count == recordKeys.length) {
        final int grown = recordKeys.length << 1;
        values = Arrays.copyOf(values, grown * fieldCount);
        final int previous = present.length;
        present = Arrays.copyOf(present, grown * fieldCount);
        if (densePresent) {
          Arrays.fill(present, previous, present.length, true);
        }
        recordKeys = Arrays.copyOf(recordKeys, grown);
      }
      recordKeys[count] = recordKey;
      return count++;
    }

    /** Add a row whose join key is present, tracking the key domain and the chain array's size. */
    int addKeyed(final long joinKey, final long recordKey) {
      minimumJoinKey = Math.min(minimumJoinKey, joinKey);
      maximumJoinKey = Math.max(maximumJoinKey, joinKey);
      final int at = add(recordKey);
      if (at == nextRow.length) {
        nextRow = Arrays.copyOf(nextRow, nextRow.length << 1);
      }
      return at;
    }
  }

  private @Nullable Sequence join(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns, final int probeSide, final boolean[] stringKeys, final boolean[] timestampKeys)
      throws QueryException {
    final int buildFieldCount = sideFields[buildSide].length;
    final boolean denseBuild = denseNumeric(buildColumns, buildFieldCount);
    if (denseBuild && !rowOutput) {
      // The dense extrema kernel reads the build lanes itself; it needs no row table or chains.
      final DenseExtrema extrema = denseExtrema(buildColumns, buildSide, probeColumns);
      if (extrema != null) {
        return joinExtrema(buildColumns, buildSide, probeColumns, extrema, stringKeys, timestampKeys);
      }
    }
    final BuildRows buildRows = new BuildRows(buildFieldCount, buildColumns.rows(), denseBuild);
    final int[] nextRow = build(buildColumns, buildFieldCount, buildRows, denseBuild);
    // A bounded dense domain can address heads directly. Sparse or overflowing domains keep the
    // ordinary primitive hash lookup; the array is at most 1 Mi entries and four per build row.
    final long joinSpan = buildRows.maximumJoinKey - buildRows.minimumJoinKey;
    final int[] denseHeads;
    if (buildRows.count != 0 && joinSpan >= 0 && joinSpan < Math.min(1L << 20, 4L * buildRows.count)) {
      denseHeads = new int[(int) joinSpan + 1];
      Arrays.fill(denseHeads, -1);
    } else {
      denseHeads = null;
    }
    final Long2IntOpenHashMap firstRow = new Long2IntOpenHashMap(denseHeads == null
        ? buildRows.count
        : 0);
    firstRow.defaultReturnValue(-1);
    for (int row = 0; row < buildRows.count; row++) {
      final long key = buildRows.values[row * buildFieldCount];
      if (denseHeads != null) {
        final int slot = (int) (key - buildRows.minimumJoinKey);
        nextRow[row] = denseHeads[slot];
        denseHeads[slot] = row;
      } else {
        nextRow[row] = firstRow.get(key);
        firstRow.put(key, row);
      }
    }
    return probeJoin(buildColumns, buildSide, probeColumns, probeSide, stringKeys, timestampKeys, buildRows, nextRow,
        denseHeads, firstRow, denseBuild);
  }

  private Sequence probeJoin(final MaskedColumns buildColumns, final int buildSide, final MaskedColumns probeColumns,
      final int probeSide, final boolean[] stringKeys, final boolean[] timestampKeys, final BuildRows buildRows,
      final int[] nextRow, final int @Nullable [] denseHeads, final Long2IntOpenHashMap firstRow,
      final boolean denseBuild) {
    // PROBE: every admitted row, every matching build row, one pair folded into its group.
    final int probeFieldCount = sideFields[probeSide].length;
    final int distinctCapacity = distinctCapacity(buildColumns, buildSide, probeColumns);
    final DistinctDomain[] distinctDomains = distinctDomains(buildColumns, buildSide, probeColumns);
    final long[] probeValues = new long[probeFieldCount];
    final boolean[] probePresent = new boolean[probeFieldCount];
    final boolean denseProbe = denseNumeric(probeColumns, probeFieldCount);
    final boolean[] probeTimestamps = new boolean[probeFieldCount];
    if (denseProbe) {
      Arrays.fill(probePresent, true);
      markTimestamps(probeColumns, probeTimestamps);
    }
    final Map<GroupKey, GroupState> groups = rowOutput
        ? Map.of()
        : new LinkedHashMap<>();
    final Long2ObjectOpenHashMap<GroupState> singleGroups = !rowOutput && keyFields.length == 1
        ? new Long2ObjectOpenHashMap<>()
        : null;
    final Ordering rowOrdering;
    final RowOrder rowOrder;
    final ColumnarRecordSequence.Builder rowColumns;
    if (rowOutput) {
      rowColumns = new ColumnarRecordSequence.Builder(outputNames, textKeys(stringKeys, timestampKeys), 256);
      if (denseBuild && denseProbe && longOrderKeys(stringKeys, timestampKeys)) {
        // Every ordering key is a long present on every row: no item, tuple or array per emitted row.
        rowOrder = new RowOrder(orderAsc, rowColumns, 256);
        rowOrdering = null;
      } else {
        final Ordering.OrderModifier[] rowModifiers = Arrays.copyOf(orderModifiers, orderModifiers.length + 2);
        rowModifiers[orderModifiers.length] = new Ordering.OrderModifier(true, true, null);
        rowModifiers[orderModifiers.length + 1] = new Ordering.OrderModifier(true, true, null);
        rowOrdering = new Ordering(new Expr[0], rowModifiers);
        rowOrder = null;
      }
    } else {
      rowOrdering = null;
      rowOrder = null;
      rowColumns = null;
    }
    final int emittedRows = probeRows(buildColumns, buildSide, probeColumns, probeSide, stringKeys, timestampKeys,
        buildRows, nextRow, denseHeads, firstRow, distinctCapacity, distinctDomains, probeValues, probePresent,
        denseProbe, probeTimestamps, groups, singleGroups, rowOrdering, rowOrder, rowColumns, denseBuild && denseProbe);
    diagServed(buildRows, probeColumns, emittedRows);
    SirixVectorizedExecutor.noteJoinGroupServed();
    if (rowOrder != null) {
      return rowOrder.columns.build(rowOrder.sorted());
    }
    return rowOrdering != null
        ? orderedRows(rowOrdering, emittedRows, rowColumns)
        : emit(groups, buildColumns, stringKeys, timestampKeys);
  }

  /** Which fields of a dense probe side are timestamps. */
  private static void markTimestamps(final MaskedColumns probeColumns, final boolean[] probeTimestamps) {
    for (int field = 0; field < probeTimestamps.length; field++) {
      probeTimestamps[field] = probeColumns.isTimestamp(field);
    }
  }

  /** The output keys that are written as text: strings and timestamps. */
  private static boolean[] textKeys(final boolean[] stringKeys, final boolean[] timestampKeys) {
    final boolean[] textKeys = new boolean[stringKeys.length];
    for (int key = 0; key < textKeys.length; key++) {
      textKeys[key] = stringKeys[key] || timestampKeys[key];
    }
    return textKeys;
  }

  // Sonar S106: an opt-in diagnostic line, written to standard error like every other one here. The
  // measuring rigs read them there.
  @SuppressWarnings("java:S106")
  private void diagServed(final BuildRows buildRows, final MaskedColumns probeColumns, final int emittedRows) {
    if (DIAG) {
      System.err.printf("[join-serve] %s join: %d build rows, %d probe rows, %d emitted; %s%n", rowOutput
          ? "row"
          : "group", buildRows.count, probeColumns.rows(), emittedRows, capturedLeavesReport());
    }
  }

  /** Whether every ordering key is a plain long output column (neither string nor timestamp). */
  private boolean longOrderKeys(final boolean[] stringKeys, final boolean[] timestampKeys) {
    for (final int key : orderIndexes) {
      if (stringKeys[key] || timestampKeys[key]) {
        return false;
      }
    }
    return true;
  }

  /**
   * The order of row output without per-row allocation, for joins whose ordering keys are all long
   * columns present on every emitted row: each row's key values and both record keys live in
   * column-major primitive buffers and the rows are ordered by stable radix passes
   * ({@link JoinKernels#sortRows}), in the order the tuple sort would produce (the keys by their
   * modifiers, then the record keys of side 0 and side 1 ascending, which are unique). A descending
   * key is stored bitwise inverted, so every pass sorts ascending.
   */
  private static final class RowOrder {
    final ColumnarRecordSequence.Builder columns;
    final long[] scratch;
    private final boolean[] ascending;
    private final int keyCount;
    /**
     * Column-major: the ordering keys as stored for an ascending pass, then the side-0 and side-1
     * record keys.
     */
    private final long[][] keys;
    private int[] rows;
    private int count;

    RowOrder(final boolean[] ascending, final ColumnarRecordSequence.Builder columns, final int capacity) {
      this.ascending = ascending;
      this.columns = columns;
      this.keyCount = ascending.length;
      this.scratch = new long[keyCount];
      this.keys = new long[keyCount + 2][];
      for (int k = 0; k < keys.length; k++) {
        keys[k] = new long[capacity];
      }
      this.rows = new int[capacity];
    }

    /** Append one emitted row: its ordering keys (from {@link #scratch}) and the two record keys. */
    void add(final int outputRow, final long side0Key, final long side1Key) {
      if (count == rows.length) {
        final int grown = rows.length << 1;
        rows = Arrays.copyOf(rows, grown);
        for (int k = 0; k < keys.length; k++) {
          keys[k] = Arrays.copyOf(keys[k], grown);
        }
      }
      for (int k = 0; k < keyCount; k++) {
        keys[k][count] = ascending[k]
            ? scratch[k]
            : ~scratch[k];
      }
      keys[keyCount][count] = side0Key;
      keys[keyCount + 1][count] = side1Key;
      rows[count++] = outputRow;
    }

    /** The output rows in order. */
    int[] sorted() {
      final int[] order = JoinKernels.sortRows(keys, count);
      final int[] result = new int[count];
      for (int i = 0; i < count; i++) {
        result[i] = rows[order[i]];
      }
      return result;
    }
  }

  /**
   * The probe loop: one method whose fold scratch lives in locals, so the pair fold inlines into the
   * row loop. A dense probe side reads its lanes in place from their segments at bases hoisted per
   * leaf, eight bytes per row; when every lane of the side lives in one segment (a selection decoded
   * into its own slab, the common case) the row read goes through that one segment, so its bounds and
   * scope are loaded once per row rather than once per field ({@link #readNumericRow}). A sparse side
   * reads through the view's per-row accessors. One representation, one loop. A row-output join reads
   * the probe record key from the key lane per emitted pair.
   */
  // Sonar S135: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S135", "java:S3776"})
  private int probeRows(final MaskedColumns buildColumns, final int buildSide, final MaskedColumns probeColumns,
      final int probeSide, final boolean[] stringKeys, final boolean[] timestampKeys, final BuildRows buildRows,
      final int[] nextRow, final int @Nullable [] denseHeads, final Long2IntOpenHashMap firstRow,
      final int distinctCapacity, final DistinctDomain[] distinctDomains, final long[] probeValues,
      final boolean[] probePresent, final boolean denseProbe, final boolean[] probeTimestamps,
      final Map<GroupKey, GroupState> groups, final @Nullable Long2ObjectOpenHashMap<GroupState> singleGroups,
      final @Nullable Ordering rowOrdering, final @Nullable RowOrder rowOrder,
      final ColumnarRecordSequence.@Nullable Builder rowColumns, final boolean allPresent) {
    final int keyCount = keyFields.length;
    final int aggCount = aggFuncs.length;
    final int buildFieldCount = buildRows.fieldCount;
    final int probeFieldCount = probeValues.length;
    final long[] keyComponents = new long[keyCount + 1]; // the last word packs the missing flags
    final long[] operands = new long[8];
    final long[] stack = new long[64];
    final long[] programValues = new long[progCode.length];
    final byte[] programState = new byte[progCode.length];
    final boolean[] residualStack = new boolean[Math.max(1, residualCode.length)];
    final GroupKey probeKey = new GroupKey(keyComponents); // Scratch key, cloned on insert.
    final int accWidth = 1 + 4 * aggCount;
    final MemorySegment[] laneSeg = new MemorySegment[probeFieldCount];
    final long[] laneOff = new long[probeFieldCount];
    final boolean emitsRows = rowOrder != null || rowOrdering != null;
    int emittedRows = 0;
    for (int leaf = 0; leaf < probeColumns.leafCount(); leaf++) {
      final long[] mask = probeColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      // Per-row reads through locals holding fresh native views; see the build loop.
      final MemorySegment keySeg = emitsRows
          ? JoinKernels.nativeView(probeColumns.keySegment(leaf))
          : MemorySegment.NULL;
      final long keyBase = emitsRows
          ? probeColumns.keyBase(leaf)
          : 0L;
      final MemorySegment captured = denseProbe
          ? captureLanes(probeColumns, leaf, laneSeg, laneOff)
          : null;
      final MemorySegment shared = captured == null
          ? null
          : JoinKernels.nativeView(captured);
      final MemorySegment joinLane = denseProbe
          ? JoinKernels.nativeView(laneSeg[0])
          : null;
      if (DIAG) {
        noteCapturedLeaf("probe", shared != null);
      }
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          if (!denseProbe && !probeColumns.present(0, leaf, row)) {
            continue;
          }
          final long joinKey = denseProbe
              ? (long) JoinKernels.SLAB_LONG.get(joinLane, laneOff[0] + 8L * row)
              : probeColumns.longValue(0, leaf, row);
          int at = firstMatch(joinKey, buildRows, denseHeads, firstRow);
          if (at < 0) {
            continue;
          }
          if (denseProbe) {
            readNumericRow(shared, laneSeg, laneOff, probeTimestamps, row, joinKey, probeValues);
          } else {
            readProbeRow(probeColumns, leaf, row, probeFieldCount, probeValues, probePresent);
          }
          for (; at >= 0; at = nextRow[at]) {
            if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack, allPresent)) {
              continue;
            }
            // The probe record key is read per emitted pair, not per matched row: most rows emit nothing.
            if (rowOrder != null) {
              emitRowPrimitive(keyCount, buildSide, buildFieldCount, buildRows, at, probeValues, stringKeys,
                  timestampKeys, buildColumns, (long) JoinKernels.SLAB_LONG.get(keySeg, keyBase + 8L * row), rowOrder);
              emittedRows++;
              continue;
            }
            if (rowOrdering != null) {
              emitRow(keyCount, buildSide, probeSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                  stringKeys, timestampKeys, buildColumns, (long) JoinKernels.SLAB_LONG.get(keySeg, keyBase + 8L * row),
                  rowOrdering, rowColumns);
              emittedRows++;
              continue;
            }
            foldPair(keyCount, aggCount, accWidth, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                keyComponents, probeKey, groups, singleGroups, distinctCapacity, distinctDomains, operands, stack,
                programValues, programState, allPresent);
          }
        }
      }
    }
    return emittedRows;
  }


  private static int firstMatch(final long joinKey, final BuildRows buildRows, final int @Nullable [] denseHeads,
      final Long2IntOpenHashMap firstRow) {
    return denseHeads == null
        ? firstRow.get(joinKey)
        : joinKey < buildRows.minimumJoinKey || joinKey > buildRows.maximumJoinKey
            ? -1
            : denseHeads[(int) (joinKey - buildRows.minimumJoinKey)];
  }

  /**
   * Bound operands for distinct count and min/max of one cross-side subtraction, with strict overlap.
   * {@code joinMinimum} and {@code joinSlots} describe the build side's dense join-key domain.
   */
  private record DenseExtrema(int distinct, int minimum, int maximum, int groupSlot, long groupMinimum, int groups,
      int distinctSlot, boolean distinctBuild, int buildMarginSlot, int probeMarginSlot, boolean buildMinusProbe,
      int buildFromSlot, int buildToSlot, int probeFromSlot, int probeToSlot, long joinMinimum, int joinSlots) {
  }

  private @Nullable DenseExtrema denseExtrema(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns) {
    if (!denseExtremaShape(buildColumns, buildSide)) {
      return declinePlan("shape");
    }
    int distinct = -1;
    int minimum = -1;
    int maximum = -1;
    for (int aggregate = 0; aggregate < aggKinds.length; aggregate++) {
      switch (aggKinds[aggregate]) {
        case COUNT_DISTINCT -> distinct = aggregate;
        case MIN -> minimum = aggregate;
        case MAX -> maximum = aggregate;
        default -> {
          return declinePlan("aggregate kind");
        }
      }
    }
    if (distinct < 0 || minimum < 0 || maximum < 0 || aggPrograms[distinct] >= 0 || aggSlots[distinct] < 0
        || aggPrograms[minimum] != 0 || aggPrograms[maximum] != 0) {
      return declinePlan("aggregate operands");
    }
    final int[] code = progCode[0];
    if (!crossSideSubtraction(code)) {
      return declinePlan("program");
    }
    return bindDenseExtrema(buildColumns, buildSide, probeColumns, distinct, minimum, maximum, code);
  }

  private static @Nullable DenseExtrema declinePlan(final String why) {
    if (DIAG) {
      System.err.println("[join-serve] dense extrema plan declined: " + why);
    }
    return null;
  }

  private boolean denseExtremaShape(final MaskedColumns buildColumns, final int buildSide) {
    return !(keyFields.length != 1 || keySides[0] != buildSide || !buildColumns.isLong(keySlots[0])
        || aggKinds.length != 3 || progCode.length != 1 || !residualAnd || residualOps.length != 2);
  }

  private @Nullable DenseExtrema bindDenseExtrema(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns, final int distinct, final int minimum, final int maximum, final int[] code) {
    int buildFrom = -1;
    int buildTo = -1;
    int probeFrom = -1;
    int probeTo = -1;
    for (int comparison = 0; comparison < 2; comparison++) {
      final int left = comparison * 2;
      final int right = left + 1;
      if (residualOps[comparison] != 2 || !residualTemporal[left] || !residualTemporal[right]
          || residualSides[left] == residualSides[right]) {
        return declinePlan("residual " + comparison);
      }
      if (residualSides[left] == buildSide) {
        buildFrom = residualSlots[left];
        probeTo = residualSlots[right];
      } else {
        probeFrom = residualSlots[left];
        buildTo = residualSlots[right];
      }
    }
    final long groupCount = groupUpperBound(buildColumns, buildSide, probeColumns);
    if (buildFrom < 0 || buildTo < 0 || groupCount == 0
        || !denseNumeric(probeColumns, sideFields[1 - buildSide].length)) {
      return declinePlan("bounds, groups or probe density");
    }
    // A bounded dense join domain is bucketed directly: at most 1 Mi slots and four per build row.
    // Sparse or overflowing domains keep the ordinary primitive hash join.
    final long joinMinimum = buildColumns.minimum(0);
    final long joinSpan = buildColumns.maximum(0) - joinMinimum;
    if (joinSpan < 0 || joinSpan >= Math.min(1L << 20, 4L * buildColumns.rows())) {
      return declinePlan("join domain " + joinSpan + " over " + buildColumns.rows() + " rows");
    }
    final boolean buildMinusProbe = progOperandSides[0][code[0]] == buildSide;
    final int buildMarginSlot = progSlots[0][code[buildMinusProbe
        ? 0
        : 1]];
    final boolean distinctBuild = aggSides[distinct] == buildSide;
    return new DenseExtrema(distinct, minimum, maximum, keySlots[0], buildColumns.minimum(keySlots[0]),
        (int) groupCount, aggSlots[distinct], distinctBuild, buildMarginSlot, progSlots[0][code[buildMinusProbe
            ? 1
            : 0]],
        buildMinusProbe, buildFrom, buildTo, probeFrom, probeTo, joinMinimum, (int) joinSpan + 1);
  }

  private boolean crossSideSubtraction(final int[] code) {
    return !(code.length != 3 || code[2] != ProjectionIndexByteScan.COMPUTED_OP_SUB || code[0] < 0 || code[1] < 0
        || code[0] >= progSlots[0].length || code[1] >= progSlots[0].length
        || progOperandSides[0][code[0]] == progOperandSides[0][code[1]]);
  }

  /**
   * A packed side held by the query: a leased cache entry, or a query-owned slab when the cache
   * refused or is disabled. Closed in {@code finally} once the fold has read it.
   */
  private static final class SideHandle implements AutoCloseable {
    final BucketedSide side;
    private final BucketedSideCache.@Nullable PackedSide entry;
    private final @Nullable MemorySegment owned;

    private SideHandle(final BucketedSide side, final BucketedSideCache.@Nullable PackedSide entry,
        final @Nullable MemorySegment owned) {
      this.side = side;
      this.entry = entry;
      this.owned = owned;
    }

    // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
    // failure is rethrown with later ones attached.
    @SuppressWarnings("java:S1181")
    static SideHandle leased(final BucketedSideCache.PackedSide entry) {
      try {
        handoff("side-handle");
        return new SideHandle(entry.side, entry, null);
      } catch (final Throwable failure) {
        try {
          entry.release();
        } catch (final Throwable cleanup) {
          retainCleanupFailure(failure, cleanup);
        }
        throw failure;
      }
    }

    static SideHandle owned(final BucketedSide side, final MemorySegment slab) {
      handoff("owned-handle");
      return new SideHandle(side, null, slab);
    }

    @Override
    public void close() {
      if (entry != null) {
        entry.release();
      } else if (owned != null) {
        DerivedCacheBudget.global().allocator().releaseSlab(owned);
      }
    }
  }

  /**
   * Bounded reuse of one immutable bucketed side per snapshot, matched on the routed keys and the
   * operand slots, so a repeated query at an immutable revision skips the bucketing pass. A side
   * being packed in the background is joinable, so a second execution that arrives before the packing
   * finished waits for it instead of packing again.
   */
  private static final class BucketedSideCache {
    private static final ConcurrentHashMap<InFlightKey, CompletableFuture<PackedSide>> IN_FLIGHT =
        new ConcurrentHashMap<>();

    /**
     * A packed side in its own slab, keyed by the row identity of the snapshot it derives from (held
     * weakly, so a side of a one-off selection is reaped) and the operand signature.
     */
    static final class PackedSide extends DerivedCacheBudget.Entry {
      private final WeakReference<Object> rows;
      private final long[] signature;
      final BucketedSide side;

      PackedSide(final Object rows, final long[] signature, final BucketedSide side) {
        this.rows = new WeakReference<>(rows);
        this.signature = signature;
        this.side = side;
      }

      boolean matches(final Object rows, final long[] signature) {
        return this.rows.get() == rows && Arrays.equals(this.signature, signature);
      }

      @Override
      protected boolean isStale() {
        return rows.get() == null;
      }

      @Override
      protected boolean dependsOn(final Object identity) {
        return rows.get() == identity;
      }
    }

    /** Store and row identity by reference, signature by content. */
    @SuppressWarnings("ArrayRecordComponent") // Signature equality and hashing are defined by content below.
    private record InFlightKey(WeakReference<ProjectionColumnStore> store, Object rows, long[] signature) {
      @Override
      @SuppressWarnings("ReferenceEquality") // The row token identifies one loaded snapshot, not equal row values.
      public boolean equals(final Object other) {
        if (other == this) {
          return true;
        }
        return other instanceof InFlightKey(WeakReference<ProjectionColumnStore> otherStore, Object otherRows, long[] otherSignature)
            && store.get() != null && otherStore.get() == store.get() && otherRows == rows
            && Arrays.equals(otherSignature, signature);
      }

      @Override
      public int hashCode() {
        return 31 * System.identityHashCode(rows) + Arrays.hashCode(signature);
      }

      @Override
      public String toString() {
        return "InFlightKey[store=" + store + ", rows=" + rows + ", signature=" + Arrays.toString(signature) + "]";
      }
    }

    private BucketedSideCache() {}

    /** The leased cached side, or {@code null}; the caller releases the entry. */
    static @Nullable PackedSide get(final MaskedColumns columns, final long[] signature) {
      if (!columns.isImmutableRevision()) {
        return null;
      }
      final Object rows = columns.rowIdentity();
      final DerivedCacheBudget.Entry entry =
          DerivedCacheBudget.global()
                            .packed()
                            .acquire(
                                candidate -> candidate instanceof PackedSide packed && packed.matches(rows, signature));
      if (entry != null) {
        return (PackedSide) entry;
      }
      final CompletableFuture<PackedSide> packing =
          IN_FLIGHT.get(new InFlightKey(new WeakReference<>(columns.store()), rows, signature));
      if (packing == null) {
        return null;
      }
      try {
        final PackedSide packed = packing.join();
        return packed != null && packed.tryAcquire()
            ? packed
            : null;
      } catch (final CompletionException | CancellationException e) {
        return null;
      }
    }

    /**
     * Reserve the exact slab now and pack in the background; a no-op unless the revision is immutable
     * and the selection is cached (only a cached selection is found again by its row identity, and only
     * its entry keeps the lanes the task reads alive after the query closed its view: the task holds a
     * lease on it), or when the same side is already being packed.
     */
    // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
    // failure is rethrown with later ones attached.
    @SuppressWarnings({"FutureReturnValueIgnored", "java:S1181"}) // Failures complete the separately tracked packing
                                                                  // future.
    static void packAsync(final MaskedColumns columns, final long[] signature, final SideLanes lanes,
        final long keyMinimum, final int slots, final long groupMinimum) {
      if (!columns.isImmutableRevision()) {
        return;
      }
      final InFlightKey key = new InFlightKey(new WeakReference<>(columns.store()), columns.rowIdentity(), signature);
      final CompletableFuture<PackedSide> packing = new CompletableFuture<>();
      final int[] bucketStart = new int[slots + 1];
      final DerivedCacheBudget.Lease lease = columns.acquireTaskLease();
      if (lease == null) {
        return;
      }
      Reservation reservation = null;
      boolean submitted = false;
      Throwable failure = null;
      try {
        if (IN_FLIGHT.putIfAbsent(key, packing) != null) {
          return;
        }
        handoff("packing");
        final int total = BucketedSide.countByBucket(lanes, keyMinimum, slots, bucketStart);
        reservation = DerivedCacheBudget.global().packed().reserve(BucketedSide.slabBytes(slots, total));
        if (reservation == null) {
          return;
        }
        BUCKETED_SIDES_BUILT.increment();
        final Reservation owned = reservation;
        final Runnable task = () -> {
          PackedSide packed = null;
          Throwable taskFailure = null;
          try {
            final BucketedSide side =
                BucketedSide.pack(owned.slab(), lanes, keyMinimum, slots, groupMinimum, bucketStart, total);
            packed = new PackedSide(key.rows(), signature, side);
            owned.publish(packed);
          } catch (final Throwable caught) {
            taskFailure = caught;
          } finally {
            if (packed != null) {
              try {
                packed.release();
              } catch (final Throwable cleanup) {
                taskFailure = retainCleanupFailure(taskFailure, cleanup);
              }
            }
            finishPacking(key, packing, lease, owned, packed, taskFailure);
          }
        };
        handoff("submission");
        final Executor executor = packingExecutorForTesting;
        if (executor == null) {
          CompletableFuture.runAsync(task);
        } else {
          CompletableFuture.runAsync(task, executor);
        }
        submitted = true;
      } catch (final Throwable caught) {
        failure = caught;
        throw caught;
      } finally {
        if (!submitted) {
          finishPacking(key, packing, lease, reservation, null, failure);
        }
      }
    }

    // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
    // failure is rethrown with later ones attached.
    @SuppressWarnings("java:S1181")
    private static void finishPacking(final InFlightKey key, final CompletableFuture<PackedSide> packing,
        final DerivedCacheBudget.Lease lease, final @Nullable Reservation reservation,
        final @Nullable PackedSide packed, final @Nullable Throwable primary) {
      Throwable failure = primary;
      if (reservation != null) {
        try {
          reservation.close();
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
      }
      try {
        lease.run();
      } catch (final Throwable cleanup) {
        failure = retainCleanupFailure(failure, cleanup);
      }
      try {
        if (failure == null) {
          packing.complete(packed);
        } else {
          packing.completeExceptionally(failure);
        }
      } catch (final Throwable cleanup) {
        failure = retainCleanupFailure(failure, cleanup);
      } finally {
        IN_FLIGHT.remove(key, packing);
      }
      rethrowCleanupFailure(failure);
    }
  }

  private static long[] sideSignature(final long keyMinimum, final int slots, final int fromSlot, final int toSlot,
      final int marginSlot, final int distinctSlot, final int groupSlot, final long groupMinimum) {
    return new long[] {keyMinimum, slots, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot, groupMinimum};
  }

  /**
   * The cached side, or one packed now (the build side is small; packing it is cheaper than a chain).
   * A refused or disabled cache packs into query-owned workspace from the same allocator.
   */
  // Sonar S107: the resolved inputs of this step, passed as they are. An object around them would
  // only move the list into a constructor.
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  @SuppressWarnings({"java:S107", "java:S1181"})
  private static SideHandle bucketedSide(final MaskedColumns columns, final long keyMinimum, final int slots,
      final int fromSlot, final int toSlot, final int marginSlot, final int distinctSlot, final int groupSlot,
      final long groupMinimum) {
    final long[] signature =
        sideSignature(keyMinimum, slots, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot, groupMinimum);
    final BucketedSideCache.PackedSide cached = BucketedSideCache.get(columns, signature);
    if (cached != null) {
      return SideHandle.leased(cached);
    }
    final SideLanes lanes = SideLanes.capture(columns, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot);
    final int[] bucketStart = new int[slots + 1];
    final int total = BucketedSide.countByBucket(lanes, keyMinimum, slots, bucketStart);
    final long bytes = BucketedSide.slabBytes(slots, total);
    final DerivedCacheBudget budget = DerivedCacheBudget.global();
    final Reservation reservation = columns.isImmutableRevision()
        ? budget.packed().reserve(bytes)
        : null;
    BUCKETED_SIDES_BUILT.increment();
    if (reservation != null) {
      try (reservation) {
        final BucketedSide side =
            BucketedSide.pack(reservation.slab(), lanes, keyMinimum, slots, groupMinimum, bucketStart, total);
        final BucketedSideCache.PackedSide packed =
            new BucketedSideCache.PackedSide(columns.rowIdentity(), signature, side);
        final SideHandle handle = SideHandle.leased(packed);
        try {
          reservation.publish(packed);
          return handle;
        } catch (final Throwable failure) {
          try {
            handle.close();
          } catch (final Throwable cleanup) {
            retainCleanupFailure(failure, cleanup);
          }
          throw failure;
        }
      }
    }
    // The synchronous build side is required query workspace even when cache admission is refused.
    final MemorySegment slab = budget.allocator().allocateSlab(bytes);
    if (slab == null) {
      throw new OutOfMemoryError(
          "dense extrema join: " + bytes + " bytes of build-side workspace exceed the allocator budget");
    }
    try {
      return SideHandle.owned(BucketedSide.pack(slab, lanes, keyMinimum, slots, groupMinimum, bucketStart, total),
          slab);
    } catch (final RuntimeException | Error failure) {
      budget.allocator().releaseSlab(slab);
      throw failure;
    }
  }

  /**
   * The dense extrema kernel: both sides are bucketed by join key, the buckets are merged key by key
   * and every strictly overlapping pair folds into flat per-group minimum/maximum lanes and one flat
   * distinct bitmap (or a per-group set when the distinct domain is unbounded). The final sort orders
   * groups by their keys. A probe side that is not cached yet is streamed from its lanes while it is
   * packed in the background for the next execution.
   */
  // Sonar S106: an opt-in diagnostic line, written to standard error like every other one here. The
  // measuring rigs read them there.
  @SuppressWarnings("java:S106")
  private Sequence joinExtrema(final MaskedColumns buildColumns, final int buildSide, final MaskedColumns probeColumns,
      final DenseExtrema plan, final boolean[] stringKeys, final boolean[] timestampKeys) throws QueryException {
    final long started = DIAG
        ? System.nanoTime()
        : 0L;
    final int slots = plan.joinSlots();
    try (SideHandle build = bucketedSide(buildColumns, plan.joinMinimum(), slots, plan.buildFromSlot(),
        plan.buildToSlot(), plan.buildMarginSlot(), plan.distinctBuild()
            ? plan.distinctSlot()
            : -1,
        plan.groupSlot(), plan.groupMinimum())) {
      final long built = DIAG
          ? System.nanoTime()
          : 0L;
      final int probeDistinctSlot = plan.distinctBuild()
          ? -1
          : plan.distinctSlot();
      final long[] probeSignature = sideSignature(plan.joinMinimum(), slots, plan.probeFromSlot(), plan.probeToSlot(),
          plan.probeMarginSlot(), probeDistinctSlot, -1, 0L);
      final BucketedSideCache.PackedSide probe = BucketedSideCache.get(probeColumns, probeSignature);
      try {
        final long packed = DIAG
            ? System.nanoTime()
            : 0L;
        final int groups = plan.groups();
        final DistinctDomain[] domains = distinctDomains(buildColumns, buildSide, probeColumns);
        final DistinctDomain domain = domains.length == 0
            ? null
            : domains[plan.distinct()];
        final ExtremaFold fold =
            new ExtremaFold(groups, domain, build.side, plan.distinctBuild(), plan.buildMinusProbe());
        final int probeRows;
        if (probe != null) {
          JoinKernels.foldMerge(fold, build.side, probe.side, slots);
          probeRows = probe.side.count;
        } else {
          final SideLanes lanes = SideLanes.capture(probeColumns, plan.probeFromSlot(), plan.probeToSlot(),
              plan.probeMarginSlot(), probeDistinctSlot, -1);
          BucketedSideCache.packAsync(probeColumns, probeSignature, lanes, plan.joinMinimum(), slots, 0L);
          probeRows = JoinKernels.foldChained(fold, build.side, lanes, plan.joinMinimum(), slots);
        }
        final long merged = DIAG
            ? System.nanoTime()
            : 0L;
        final Map<GroupKey, GroupState> result = extremaGroups(fold, plan, groups);
        if (DIAG) {
          final SideLanes probeMasks = SideLanes.capture(probeColumns, plan.probeFromSlot(), plan.probeToSlot(),
              plan.probeMarginSlot(), probeDistinctSlot, -1);
          System.err.printf(
              "[join-serve] dense extrema (%s): build %.2f ms, lookup %.2f ms, fold %.2f ms, emit %.2f ms"
                  + " (%d build rows, %d probe rows, %d slots, %d groups; probe mask words %d full, %d partial)%n",
              probe != null
                  ? "merge"
                  : "chained, packing in background",
              (built - started) / 1e6, (packed - built) / 1e6, (merged - packed) / 1e6,
              (System.nanoTime() - merged) / 1e6, build.side.count, probeRows, slots, result.size(),
              probeMasks.fullWords, probeMasks.partialWords);
        }
        SirixVectorizedExecutor.noteJoinGroupServed();
        return emit(result, buildColumns, stringKeys, timestampKeys);
      } finally {
        if (probe != null) {
          probe.release();
        }
      }
    }
  }

  /**
   * One group state for each group the fold reached: its minimum, its maximum and its distinct count.
   */
  private Map<GroupKey, GroupState> extremaGroups(final ExtremaFold fold, final DenseExtrema plan, final int groups) {
    final long[] groupMinimum = fold.groupMinimum;
    final long[] groupMaximum = fold.groupMaximum;
    final Map<GroupKey, GroupState> result = new LinkedHashMap<>();
    final int width = 1 + 4 * aggKinds.length;
    final int minBase = 1 + 4 * plan.minimum();
    final int maxBase = 1 + 4 * plan.maximum();
    for (int group = 0; group < groups; group++) {
      if (groupMaximum[group] == Long.MIN_VALUE && groupMinimum[group] == Long.MAX_VALUE) {
        continue;
      }
      final long[] acc = new long[width];
      acc[minBase] = 1;
      acc[maxBase] = 1;
      acc[minBase + 2] = groupMinimum[group];
      acc[maxBase + 3] = groupMaximum[group];
      final DistinctValues[] distinctSets = new DistinctValues[aggKinds.length];
      distinctSets[plan.distinct()] = new DistinctValues(fold.distinctCount(group));
      result.put(new GroupKey(new long[] {plan.groupMinimum() + group, 0}), new GroupState(acc, distinctSets));
    }
    return result;
  }

  private static boolean denseNumeric(final MaskedColumns columns, final int fields) {
    for (int field = 0; field < fields; field++) {
      if ((!columns.isLong(field) && !columns.isTimestamp(field)) || !columns.allSelectedPresent(field)) {
        return false;
      }
    }
    return true;
  }



  /**
   * Capture the lanes of a dense side on {@code leaf} (segment and byte base of row 0 per field) and
   * return the one segment the rostered fields after the join key all live in, or {@code null} when
   * they are spread over several. The join key is read on its own before the row, so its lane may
   * differ (a column the store holds resident beside a selection decoded into its own slab).
   */
  private static @Nullable MemorySegment captureLanes(final MaskedColumns columns, final int leaf,
      final MemorySegment[] lanes, final long[] bases) {
    MemorySegment shared = null;
    for (int field = 0; field < lanes.length; field++) {
      final MemorySegment segment = columns.laneSegment(field, leaf);
      lanes[field] = segment;
      bases[field] = columns.laneBase(field, leaf);
      if (field == 1) {
        shared = segment;
      } else if (field > 1 && shared != segment) {
        shared = null;
      }
    }
    return shared;
  }

  /**
   * The row read of a dense side: the join key, already read, then one read per further lane at the
   * row's offset past the lane's base. With {@code shared} those reads go through that one segment,
   * so its bounds and scope load once per row (the reads are back to back, nothing between them
   * clobbers them); otherwise each lane's own. The join field is a plain long column, never a
   * timestamp ({@link #serve} declines otherwise).
   */
  private static void readNumericRow(final @Nullable MemorySegment shared, final MemorySegment[] lanes,
      final long[] bases, final boolean[] timestamps, final int row, final long joinKey, final long[] values) {
    final long at = 8L * row;
    values[0] = joinKey;
    if (shared != null) {
      for (int field = 1; field < bases.length; field++) {
        final long value = (long) JoinKernels.SLAB_LONG.get(shared, bases[field] + at);
        values[field] = timestamps[field]
            ? Math.multiplyExact(value, 1000L)
            : value;
      }
    } else {
      for (int field = 1; field < bases.length; field++) {
        final long value = (long) JoinKernels.SLAB_LONG.get(lanes[field], bases[field] + at);
        values[field] = timestamps[field]
            ? Math.multiplyExact(value, 1000L)
            : value;
      }
    }
  }

  private int distinctCapacity(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns) {
    if (keyFields.length != 1) {
      return 16;
    }
    int distinctCount = 0;
    for (final byte kind : aggKinds) {
      if (kind == COUNT_DISTINCT) {
        distinctCount++;
      }
    }
    final long groups = groupUpperBound(buildColumns, buildSide, probeColumns);
    if (distinctCount == 0 || groups == 0) {
      return 16;
    }
    // Cap initial expected entries over every group and distinct lane at 128 Ki.
    return (int) Math.max(16, Math.min(16384,
        Math.min((buildColumns.rows() + probeColumns.rows()) / groups, (1L << 17) / (groups * distinctCount))));
  }

  private long groupUpperBound(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns) {
    if (keyFields.length != 1) {
      return 0;
    }
    final MaskedColumns keys = keySides[0] == buildSide
        ? buildColumns
        : probeColumns;
    if (!keys.isLong(keySlots[0])) {
      return 0;
    }
    final long minimum = keys.minimum(keySlots[0]);
    final long maximum = keys.maximum(keySlots[0]);
    final long span = maximum - minimum;
    if (minimum > maximum || span < 0 || span >= 4096) {
      return 0;
    }
    // The numeric domain bounds group count; an incomplete presence proof adds a missing group.
    return span + (keys.allSelectedPresent(keySlots[0])
        ? 1
        : 2);
  }

  private static final DistinctDomain[] NO_DOMAINS = new DistinctDomain[0];

  private DistinctDomain[] distinctDomains(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns) {
    final long groups = groupUpperBound(buildColumns, buildSide, probeColumns);
    if (groups == 0) {
      return NO_DOMAINS;
    }
    int distinctCount = 0;
    for (final byte kind : aggKinds) {
      if (kind == COUNT_DISTINCT) {
        distinctCount++;
      }
    }
    if (distinctCount == 0) {
      return NO_DOMAINS;
    }
    final long bytesPerSet = (2L << 20) / (groups * distinctCount);
    final DistinctDomain[] domains = new DistinctDomain[aggKinds.length];
    for (int aggregate = 0; aggregate < aggKinds.length; aggregate++) {
      if (aggKinds[aggregate] != COUNT_DISTINCT || aggPrograms[aggregate] >= 0 || aggSlots[aggregate] < 0) {
        continue;
      }
      final MaskedColumns columns = aggSides[aggregate] == buildSide
          ? buildColumns
          : probeColumns;
      if (!columns.isLong(aggSlots[aggregate])) {
        continue;
      }
      final long minimum = columns.minimum(aggSlots[aggregate]);
      final long maximum = columns.maximum(aggSlots[aggregate]);
      final long span = maximum - minimum;
      if (minimum <= maximum && span >= 0 && span < (1L << 20)) {
        final int words = (int) (span >>> 6) + 1;
        if (8L * words <= bytesPerSet) {
          domains[aggregate] = new DistinctDomain(minimum, words);
        }
      }
    }
    return domains;
  }

  private boolean matchesResidual(final BuildRows build, final int row, final int buildSide, final long[] probeValues,
      final boolean[] probePresent, final boolean[] stack, final boolean allPresent) {
    int top = 0;
    final int length = residualAnd
        ? residualOps.length
        : residualCode.length;
    for (int operation = 0; operation < length; operation++) {
      final int instruction = residualAnd
          ? operation
          : residualCode[operation];
      if (instruction < 0) {
        final boolean right = stack[--top];
        stack[top - 1] = instruction == -1
            ? stack[top - 1] && right
            : stack[top - 1] || right;
      } else {
        final boolean matched =
            matchesComparison(build, row, buildSide, probeValues, probePresent, allPresent, instruction);
        if (residualAnd) {
          if (!matched) {
            return false;
          }
        } else {
          stack[top++] = matched;
        }
      }
    }
    return residualAnd || top == 0 || stack[0];
  }

  private boolean matchesComparison(final BuildRows build, final int row, final int buildSide, final long[] probeValues,
      final boolean[] probePresent, final boolean allPresent, final int instruction) {
    final int left = 2 * instruction;
    final int right = left + 1;
    final boolean leftBuild = residualSides[left] == buildSide;
    final boolean rightBuild = residualSides[right] == buildSide;
    final int leftSlot = residualSlots[left];
    final int rightSlot = residualSlots[right];
    final boolean leftPresent = allPresent || (leftBuild
        ? build.present[row * build.fieldCount + leftSlot]
        : probePresent[leftSlot]);
    final boolean rightPresent = allPresent || (rightBuild
        ? build.present[row * build.fieldCount + rightSlot]
        : probePresent[rightSlot]);
    final long a = leftBuild
        ? build.values[row * build.fieldCount + leftSlot]
        : probeValues[leftSlot];
    final long b = rightBuild
        ? build.values[row * build.fieldCount + rightSlot]
        : probeValues[rightSlot];
    return leftPresent && rightPresent && switch (residualOps[instruction]) {
      case 0 -> a == b;
      case 1 -> a != b;
      case 2 -> a < b;
      case 3 -> a <= b;
      case 4 -> a > b;
      case 5 -> a >= b;
      default -> throw new IllegalStateException("Invalid residual comparison");
    };
  }

  private Sequence emit(final Map<GroupKey, GroupState> groups, final MaskedColumns interner,
      final boolean[] stringKeys, final boolean[] timestampKeys) throws QueryException {
    final List<Item> records = new ArrayList<>(groups.size());
    final int keyCount = keyFields.length;
    for (final Map.Entry<GroupKey, GroupState> group : groups.entrySet()) {
      final long[] key = group.getKey().components;
      final GroupState state = group.getValue();
      final long[] acc = state.acc;
      final long missing = key[keyCount];
      final Sequence[] values = new Sequence[entryKinds.length];
      for (int i = 0; i < entryKinds.length; i++) {
        final int kind = entryKinds[i];
        if (kind >= 0) {
          if ((missing & 1L << kind) != 0L) {
            values[i] = null;
          } else {
            // Every string id lives in the build side's interner, probe ids included.
            values[i] = keyValue(key[kind], stringKeys[kind], timestampKeys[kind], interner);
          }
        } else {
          final int a = -(kind + 1);
          final int base = 1 + 4 * a;
          values[i] = switch (aggFuncs[a]) {
            case "count" -> new Int64(aggFields[a] == null
                ? acc[0]
                : acc[base]);
            case "count-distinct" -> new Int64(state.distinct[a].size());
            case "sum" -> new Int64(acc[base + 1]);
            case "min" -> acc[base] == 0
                ? null
                : new Int64(acc[base + 2]);
            default -> acc[base] == 0
                ? null
                : new Int64(acc[base + 3]);
          };
        }
      }
      records.add(new ArrayObject(outputNames, values));
    }
    return sort(records);
  }

  private static Item keyValue(final long value, final boolean string, final boolean timestamp,
      final MaskedColumns interner) {
    if (string) {
      return new Str(interner.string((int) value));
    }
    if (timestamp) {
      return new Str(
          ProjectionTemporalCodec.formatToString(ProjectionTemporalCodec.DISPLAY_ISO_SECOND, value / 1000L) + "Z");
    }
    return new Int64(value);
  }

  private Sequence sort(final List<Item> records) throws QueryException {
    final Ordering ordering = new Ordering(new Expr[0], orderModifiers);
    for (final Item item : records) {
      final ArrayObject record = (ArrayObject) item;
      final Sequence[] keys = new Sequence[orderIndexes.length];
      for (int i = 0; i < orderIndexes.length; i++) {
        final Sequence value = record.value(orderIndexes[i]);
        keys[i] = value == null
            ? null
            : ((Item) value).atomize();
      }
      ordering.add(keys, new TupleImpl(item));
    }
    return ordered(ordering, records.size());
  }

  private static Sequence ordered(final Ordering ordering, final int size) throws QueryException {
    final List<Item> out = new ArrayList<>(size);
    if (size > 0) {
      try (final Stream<? extends Tuple> stream = ordering.sorted()) {
        for (Tuple next = stream.next(); next != null; next = stream.next()) {
          out.add((Item) next.get(0));
        }
      }
    }
    return new ItemSequence(out.toArray(new Item[0]));
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
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

  /**
   * A group's identity: the key components (long values or interned string ids) plus missing flags.
   */
  private static final class GroupState {
    private final long[] acc;
    private static final DistinctValues[] NO_DISTINCT = new DistinctValues[0];
    private final DistinctValues[] distinct;

    GroupState(final int width, final String[] funcs, final int distinctCapacity, final DistinctDomain[] domains) {
      acc = new long[width];
      DistinctValues[] sets = NO_DISTINCT;
      for (int i = 0; i < funcs.length; i++) {
        if ("count-distinct".equals(funcs[i])) {
          if (sets.length == 0) {
            sets = new DistinctValues[funcs.length];
          }
          sets[i] = new DistinctValues(distinctCapacity, domains.length == 0
              ? null
              : domains[i]);
        }
      }
      distinct = sets;
    }

    /** A finished group: its accumulator lanes and distinct counts were folded elsewhere. */
    GroupState(final long[] acc, final DistinctValues[] distinct) {
      this.acc = acc;
      this.distinct = distinct;
    }
  }

  private static final class DistinctValues {
    private final @Nullable LongOpenHashSet values;
    private final long @Nullable [] bits;
    private final long minimum;
    private long count;

    /** A finished distinct count; {@link #add} is never called on it. */
    DistinctValues(final long count) {
      values = null;
      bits = null;
      minimum = 0L;
      this.count = count;
    }

    DistinctValues(final int capacity, final @Nullable DistinctDomain domain) {
      values = domain == null
          ? new LongOpenHashSet(capacity)
          : null;
      bits = domain == null
          ? null
          : new long[domain.words()];
      minimum = domain == null
          ? 0
          : domain.minimum();
    }

    void add(final long value) {
      if (values != null) {
        values.add(value);
        return;
      }
      final long[] bitmap = Objects.requireNonNull(bits);
      final long offset = value - minimum;
      final int word = (int) (offset >>> 6);
      final long bit = 1L << offset;
      final long prior = bitmap[word];
      if ((prior & bit) == 0) {
        bitmap[word] = prior | bit;
        count++;
      }
    }

    long size() {
      return values == null
          ? count
          : values.size();
    }
  }

  private static final class GroupKey {
    private final long[] components;
    private int hash;

    GroupKey(final long[] components) {
      this.components = components;
      this.hash = Arrays.hashCode(components);
    }

    /** Recompute the hash after the scratch components changed (the probe key only). */
    void rehash() {
      hash = Arrays.hashCode(components);
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof GroupKey that && Arrays.equals(components, that.components);
    }
  }

  // Sonar S135: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S135", "java:S3776"})
  private static int[] build(final MaskedColumns buildColumns, final int buildFieldCount, final BuildRows buildRows,
      final boolean denseNumeric) {
    buildRows.nextRow = new int[Math.max(16, (int) Math.min(Integer.MAX_VALUE - 8L, buildColumns.rows()))];
    final MemorySegment[] laneSeg = new MemorySegment[buildFieldCount];
    final long[] laneOff = new long[buildFieldCount];
    final long[] values = new long[buildFieldCount];
    final boolean[] timestamps = new boolean[buildFieldCount];
    if (denseNumeric) {
      for (int field = 0; field < buildFieldCount; field++) {
        timestamps[field] = buildColumns.isTimestamp(field);
      }
    }
    // BUILD: every admitted row with a present join key, chained per key value. Dense lanes are read
    // in place at bases hoisted per leaf (through their one shared segment when they have one), the
    // record keys from the key lane the same way.
    for (int leaf = 0; leaf < buildColumns.leafCount(); leaf++) {
      final long[] mask = buildColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      final MemorySegment keyLane = buildColumns.keySegment(leaf);
      final long keyBase = buildColumns.keyBase(leaf);
      final MemorySegment captured = denseNumeric
          ? captureLanes(buildColumns, leaf, laneSeg, laneOff)
          : null;
      if (DIAG) {
        noteCapturedLeaf("build", captured != null);
      }
      if (captured != null && laneSeg[0] == captured && keyLane == captured) {
        // The common shape, a selection decoded into its own slab: the join key, every further field
        // and the record key of a row live in one segment and are read back to back through one
        // local, so the segment's bounds, address and scope load once per row.
        buildLeafOneSegment(buildRows, buildFieldCount, JoinKernels.nativeView(captured), laneOff, keyBase, timestamps,
            mask, values);
        continue;
      }
      // Lanes spread over several segments (a store-resident column beside the selection's slab) or
      // a sparse side: every lane read per row still goes through a local holding a fresh native view
      // (JoinKernels.nativeView), so the compiler knows the segment's class at the read and needs none
      // of the JDK's shared segment speculation, which heap-array reads elsewhere in the process disable.
      final MemorySegment keySeg = JoinKernels.nativeView(keyLane);
      final MemorySegment shared = captured == null
          ? null
          : JoinKernels.nativeView(captured);
      final MemorySegment joinLane = denseNumeric
          ? JoinKernels.nativeView(laneSeg[0])
          : null;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          if (!denseNumeric && !buildColumns.present(0, leaf, row)) {
            continue; // a missing join key matches nothing
          }
          final long joinKey = denseNumeric
              ? (long) JoinKernels.SLAB_LONG.get(joinLane, laneOff[0] + 8L * row)
              : buildColumns.longValue(0, leaf, row);
          final int at = buildRows.addKeyed(joinKey, (long) JoinKernels.SLAB_LONG.get(keySeg, keyBase + 8L * row));
          if (denseNumeric) {
            readNumericRow(shared, laneSeg, laneOff, timestamps, row, joinKey, values);
            System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
          } else {
            readBuildRow(buildColumns, leaf, row, buildFieldCount, buildRows, at);
          }
        }
      }
    }
    return buildRows.nextRow;
  }

  /**
   * The build loop over one leaf whose join key lane, further lanes and record keys all live in
   * {@code lane} (bases per field in {@code bases}, the record keys at {@code keyBase}): a row's
   * reads are issued back to back through that one local before the row is added, so the segment's
   * bounds, address and scope are loaded once per row and the compiler checks only the offsets; the
   * values then go straight into the row table. The same rows, chains and key domain as the general
   * loop.
   */
  // Sonar S107: the operands of a hot path are passed one by one so that they stay in registers. An
  // operand object would cost a load at every use.
  @SuppressWarnings("java:S107")
  private static void buildLeafOneSegment(final BuildRows buildRows, final int fieldCount, final MemorySegment lane,
      final long[] bases, final long keyBase, final boolean[] timestamps, final long[] mask, final long[] values) {
    final long joinBase = bases[0];
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final long at = 8L * (rowBase + bit);
        final long joinKey = (long) JoinKernels.SLAB_LONG.get(lane, joinBase + at);
        final long recordKey = (long) JoinKernels.SLAB_LONG.get(lane, keyBase + at);
        values[0] = joinKey;
        for (int field = 1; field < fieldCount; field++) {
          final long value = (long) JoinKernels.SLAB_LONG.get(lane, bases[field] + at);
          values[field] = timestamps[field]
              ? Math.multiplyExact(value, 1000L)
              : value;
        }
        final int cell = buildRows.addKeyed(joinKey, recordKey) * fieldCount;
        final long[] rows = buildRows.values; // addKeyed may have grown the table
        for (int field = 0; field < fieldCount; field++) {
          rows[cell + field] = values[field];
        }
      }
    }
  }

  private static void readBuildRow(final MaskedColumns buildColumns, final int leaf, final int row,
      final int buildFieldCount, final BuildRows buildRows, final int at) {
    for (int f = 0; f < buildFieldCount; f++) {
      final boolean present = buildColumns.present(f, leaf, row);
      buildRows.present[at * buildFieldCount + f] = present;
      if (present) {
        buildRows.values[at * buildFieldCount + f] = buildColumns.isString(f)
            ? buildColumns.stringId(f, leaf, row)
            : buildColumns.isTimestamp(f)
                ? Math.multiplyExact(buildColumns.longValue(f, leaf, row), 1000L)
                : buildColumns.longValue(f, leaf, row);
      }
    }
  }

  private static void readProbeRow(final MaskedColumns probeColumns, final int leaf, final int row,
      final int probeFieldCount, final long[] probeValues, final boolean[] probePresent) {
    for (int f = 0; f < probeFieldCount; f++) {
      probePresent[f] = probeColumns.present(f, leaf, row);
      if (probePresent[f]) {
        probeValues[f] = probeColumns.isString(f)
            ? probeColumns.sharedStringId(f, leaf, row)
            : probeColumns.isTimestamp(f)
                ? Math.multiplyExact(probeColumns.longValue(f, leaf, row), 1000L)
                : probeColumns.longValue(f, leaf, row);
      }
    }
  }

  private void emitRow(final int keyCount, final int buildSide, final int probeSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent,
      final boolean[] stringKeys, final boolean[] timestampKeys, final MaskedColumns buildColumns,
      final long probeRecordKey, final Ordering rowOrdering, final ColumnarRecordSequence.Builder rowColumns) {
    final int outputRow = rowColumns.addRow();
    for (int k = 0; k < keyCount; k++) {
      final boolean fromBuild = keySides[k] == buildSide;
      final int cell = at * buildFieldCount + keySlots[k];
      final boolean present = fromBuild
          ? buildRows.present[cell]
          : probePresent[keySlots[k]];
      if (present) {
        final long value = fromBuild
            ? buildRows.values[cell]
            : probeValues[keySlots[k]];
        if (stringKeys[k]) {
          rowColumns.setString(k, outputRow, buildColumns.string((int) value));
        } else if (timestampKeys[k]) {
          rowColumns.setString(k, outputRow,
              ProjectionTemporalCodec.formatToString(ProjectionTemporalCodec.DISPLAY_ISO_SECOND, value / 1000L) + "Z");
        } else {
          rowColumns.setLong(k, outputRow, value);
        }
      }
    }
    final Sequence[] orderKeys = new Sequence[orderIndexes.length + 2];
    for (int k = 0; k < orderIndexes.length; k++) {
      orderKeys[k] = rowColumns.value(orderIndexes[k], outputRow);
    }
    orderKeys[orderIndexes.length + buildSide] = new Int64(buildRows.recordKeys[at]);
    orderKeys[orderIndexes.length + probeSide] = new Int64(probeRecordKey);
    rowOrdering.add(orderKeys, new TupleImpl(new Int64(outputRow)));
  }

  /** {@link #emitRow} for all-long, all-present ordering keys: no item, tuple or array per row. */
  // Sonar S107: the operands of a hot path are passed one by one so that they stay in registers. An
  // operand object would cost a load at every use.
  @SuppressWarnings("java:S107")
  private void emitRowPrimitive(final int keyCount, final int buildSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] stringKeys,
      final boolean[] timestampKeys, final MaskedColumns buildColumns, final long probeKey, final RowOrder rowOrder) {
    final ColumnarRecordSequence.Builder rowColumns = rowOrder.columns;
    final int outputRow = rowColumns.addRow();
    for (int k = 0; k < keyCount; k++) {
      final long value = keySides[k] == buildSide
          ? buildRows.values[at * buildFieldCount + keySlots[k]]
          : probeValues[keySlots[k]];
      if (stringKeys[k]) {
        rowColumns.setString(k, outputRow, buildColumns.string((int) value));
      } else if (timestampKeys[k]) {
        rowColumns.setString(k, outputRow,
            ProjectionTemporalCodec.formatToString(ProjectionTemporalCodec.DISPLAY_ISO_SECOND, value / 1000L) + "Z");
      } else {
        rowColumns.setLong(k, outputRow, value);
      }
    }
    final long[] scratch = rowOrder.scratch;
    for (int k = 0; k < orderIndexes.length; k++) {
      final int key = orderIndexes[k];
      scratch[k] = keySides[key] == buildSide
          ? buildRows.values[at * buildFieldCount + keySlots[key]]
          : probeValues[keySlots[key]];
    }
    final long buildKey = buildRows.recordKeys[at];
    rowOrder.add(outputRow, buildSide == 0
        ? buildKey
        : probeKey,
        buildSide == 0
            ? probeKey
            : buildKey);
  }

  private static Sequence orderedRows(final Ordering ordering, final int size,
      final ColumnarRecordSequence.Builder columns) {
    final int[] rows = new int[size];
    if (size != 0) {
      try (final Stream<? extends Tuple> stream = ordering.sorted()) {
        int position = 0;
        for (Tuple next = stream.next(); next != null; next = stream.next()) {
          rows[position++] = ((IntNumeric) next.get(0)).intValue();
        }
      }
    }
    return columns.build(rows);
  }

  private void foldPair(final int keyCount, final int aggCount, final int accWidth, final int buildSide,
      final int buildFieldCount, final BuildRows buildRows, final int at, final long[] probeValues,
      final boolean[] probePresent, final long[] keyComponents, final GroupKey probeKey,
      final Map<GroupKey, GroupState> groups, final @Nullable Long2ObjectOpenHashMap<GroupState> singleGroups,
      final int distinctCapacity, final DistinctDomain[] distinctDomains, final long[] operands, final long[] stack,
      final long[] programValues, final byte[] programState, final boolean allPresent) {
    // The group key of this pair.
    if (keyCount == 1 && allPresent) {
      keyComponents[0] = keySides[0] == buildSide
          ? buildRows.values[at * buildFieldCount + keySlots[0]]
          : probeValues[keySlots[0]];
      keyComponents[1] = 0;
    } else {
      pairKey(keyCount, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent, keyComponents);
    }
    final GroupState existing;
    if (singleGroups != null && keyComponents[1] == 0) {
      existing = singleGroups.get(keyComponents[0]);
    } else {
      probeKey.rehash();
      existing = groups.get(probeKey);
    }
    GroupState state = existing;
    if (state == null) {
      state = new GroupState(accWidth, aggFuncs, distinctCapacity, distinctDomains);
      final long[] acc = state.acc;
      for (int a = 0; a < aggCount; a++) {
        acc[1 + 4 * a + 2] = Long.MAX_VALUE;
        acc[1 + 4 * a + 3] = Long.MIN_VALUE;
      }
      groups.put(new GroupKey(keyComponents.clone()), state);
      if (singleGroups != null && keyComponents[1] == 0) {
        singleGroups.put(keyComponents[0], state);
      }
    }
    final long[] acc = state.acc;
    if (countPairs) {
      acc[0]++;
    }
    if (programState.length == 1) {
      programState[0] = 0;
    } else if (programState.length != 0) {
      Arrays.fill(programState, (byte) 0);
    }
    foldAggregates(aggCount, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent, state, operands,
        stack, programValues, programState);
  }

  private void pairKey(final int keyCount, final int buildSide, final int buildFieldCount, final BuildRows buildRows,
      final int at, final long[] probeValues, final boolean[] probePresent, final long[] keyComponents) {
    long missing = 0L;
    for (int k = 0; k < keyCount; k++) {
      final boolean fromBuild = keySides[k] == buildSide;
      final int slot = keySlots[k];
      final boolean present = fromBuild
          ? buildRows.present[at * buildFieldCount + slot]
          : probePresent[slot];
      if (!present) {
        missing |= 1L << k;
        keyComponents[k] = 0L;
      } else {
        keyComponents[k] = fromBuild
            ? buildRows.values[at * buildFieldCount + slot]
            : probeValues[slot];
      }
    }
    keyComponents[keyCount] = missing;
  }

  private void foldAggregates(final int aggCount, final int buildSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent,
      final GroupState state, final long[] operands, final long[] stack, final long[] programValues,
      final byte[] programState) {
    final long[] acc = state.acc;
    for (int a = 0; a < aggCount; a++) {
      final int base = 1 + 4 * a;
      final String field = aggFields[a];
      if (field == null) {
        continue; // a pair count reads acc[0]
      }
      final boolean fromBuild = aggSides[a] == buildSide;
      final long v;
      final int program = aggPrograms[a];
      if (program >= 0) {
        if (programState[program] == 0) {
          if (programOperands(program, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent,
              operands)) {
            programValues[program] = runProgram(program, operands, stack);
            programState[program] = 1;
          } else {
            programState[program] = 2;
          }
        }
        if (programState[program] == 2) {
          continue;
        }
        v = programValues[program];
      } else {
        final int slot = aggSlots[a];
        final boolean present = fromBuild
            ? buildRows.present[at * buildFieldCount + slot]
            : probePresent[slot];
        if (!present) {
          continue;
        }
        v = fromBuild
            ? buildRows.values[at * buildFieldCount + slot]
            : probeValues[slot];
      }
      if (aggKinds[a] == COUNT_DISTINCT) {
        state.distinct[a].add(v);
        continue;
      }
      switch (aggKinds[a]) {
        case COUNT -> acc[base]++;
        case SUM -> acc[base + 1] = Math.addExact(acc[base + 1], v);
        case MIN -> {
          acc[base] = 1;
          if (v < acc[base + 2]) {
            acc[base + 2] = v;
          }
        }
        case MAX -> {
          acc[base] = 1;
          if (v > acc[base + 3]) {
            acc[base + 3] = v;
          }
        }
        default -> throw new IllegalStateException("Invalid joined aggregate lane");
      }
    }
  }

  private long runProgram(final int program, final long[] operands, final long[] stack) {
    final int[] code = progCode[program];
    if (code.length == 3 && code[0] >= 0 && code[1] >= 0) {
      final long a = code[0] >= ProjectionIndexByteScan.COMPUTED_CONST_BASE
          ? progConsts[program][code[0] - ProjectionIndexByteScan.COMPUTED_CONST_BASE]
          : operands[code[0]];
      final long b = code[1] >= ProjectionIndexByteScan.COMPUTED_CONST_BASE
          ? progConsts[program][code[1] - ProjectionIndexByteScan.COMPUTED_CONST_BASE]
          : operands[code[1]];
      return switch (code[2]) {
        case ProjectionIndexByteScan.COMPUTED_OP_ADD -> Math.addExact(a, b);
        case ProjectionIndexByteScan.COMPUTED_OP_SUB -> Math.subtractExact(a, b);
        case ProjectionIndexByteScan.COMPUTED_OP_MUL -> Math.multiplyExact(a, b);
        default -> ProjectionComputedColumn.run(code, progConsts[program], operands, stack);
      };
    }
    return ProjectionComputedColumn.run(code, progConsts[program], operands, stack);
  }

  private boolean programOperands(final int program, final int buildSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent,
      final long[] operands) {
    boolean allPresent = true;
    for (int o = 0; o < progSlots[program].length; o++) {
      final int slot = progSlots[program][o];
      final boolean fromBuild = progOperandSides[program][o] == buildSide;
      final boolean present = fromBuild
          ? buildRows.present[at * buildFieldCount + slot]
          : probePresent[slot];
      if (!present) {
        allPresent = false;
        break;
      }
      operands[o] = fromBuild
          ? buildRows.values[at * buildFieldCount + slot]
          : probeValues[slot];
    }
    return allPresent;
  }

  private @Nullable String invalidColumns(final MaskedColumns[] columns, final boolean[] stringKeys,
      final boolean[] timestampKeys) {
    for (int k = 0; k < keyFields.length; k++) {
      final MaskedColumns c = columns[keySides[k]];
      if (!c.isLong(keySlots[k]) && !c.isString(keySlots[k]) && !c.isTimestamp(keySlots[k])) {
        return "key " + keyFields[k] + " is neither long nor string";
      }
      stringKeys[k] = c.isString(keySlots[k]);
      timestampKeys[k] = c.isTimestamp(keySlots[k]);
    }
    for (int a = 0; a < aggFields.length; a++) {
      if (aggSlots[a] >= 0 && !columns[aggSides[a]].isLong(aggSlots[a]) && !("count-distinct".equals(aggFuncs[a])
          && (columns[aggSides[a]].isString(aggSlots[a]) || columns[aggSides[a]].isTimestamp(aggSlots[a])))) {
        return "aggregate operand " + aggFields[a] + " is not a long column";
      }
    }
    for (int p = 0; p < progSlots.length; p++) {
      for (int o = 0; o < progSlots[p].length; o++) {
        if (!columns[progOperandSides[p][o]].isLong(progSlots[p][o])) {
          return "program operand is not a long column";
        }
      }
    }
    for (int i = 0; i < residualSlots.length; i++) {
      if (residualTemporal[i]
          ? !columns[residualSides[i]].isTimestamp(residualSlots[i])
          : !columns[residualSides[i]].isLong(residualSlots[i])) {
        return "residual operand is not a long column";
      }
    }
    return null;
  }

}
