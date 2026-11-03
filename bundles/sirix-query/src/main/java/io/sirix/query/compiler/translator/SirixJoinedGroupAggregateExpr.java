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
import io.sirix.node.SegmentAccess;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.query.scan.DerivedCacheBudget;
import io.sirix.query.scan.HeapWindows;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import io.sirix.query.scan.MaskedColumns;
import io.sirix.query.scan.NativeRows;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
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
  private static volatile @Nullable Executor packingExecutorForTesting;

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

  private static @Nullable Sequence decline(final String why) {
    if (DIAG) {
      System.err.println("[join-serve] decline: " + why);
    }
    return null;
  }

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
          if (side == 1 && sides[0].routed() == null) {
            return decline("an empty inner mask requires the ordinary outer source evaluation");
          }
          SirixVectorizedExecutor.noteJoinGroupServed();
          return new ItemSequence();
        }
        keys[side] = rows.keys();
        revisions[side] = rows.revision();
      } else {
        revisions[side] = spec.revision();
      }
    }
    final MaskedColumns[] columns = new MaskedColumns[2];
    try {
      for (int side = 0; side < 2; side++) {
        columns[side] = columns(ctx, side, keys[side], revisions[side]);
        if (columns[side] == null) {
          return decline("side " + side + " has no masked columns");
        }
        if (!columns[side].isLong(0)) {
          return decline("join field of side " + side + " is not a long column");
        }
      }
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
      } catch (final ArithmeticException overflow) {
        return decline("exact arithmetic overflow");
      }
    } finally {
      // Native views lease their resident snapshot; the result sequence no longer reads them.
      for (final MaskedColumns side : columns) {
        if (side != null) {
          side.close();
        }
      }
    }
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
      return executor.maskedColumns(ARRAY_MEMBERS, keys, sideFields[side]);
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
    final long[] probeValues = new long[Math.max(NativeRows.LANES, probeFieldCount)];
    final boolean[] probePresent = new boolean[probeFieldCount];
    final boolean denseProbe = denseNumeric(probeColumns, probeFieldCount);
    final boolean[] probeTimestamps = new boolean[probeFieldCount];
    if (denseProbe) {
      Arrays.fill(probePresent, true);
      for (int field = 0; field < probeFieldCount; field++) {
        probeTimestamps[field] = probeColumns.isTimestamp(field);
      }
    }
    final Map<GroupKey, GroupState> groups = rowOutput
        ? Map.of()
        : new LinkedHashMap<>();
    final Long2ObjectOpenHashMap<GroupState> singleGroups = !rowOutput && keyFields.length == 1
        ? new Long2ObjectOpenHashMap<>()
        : null;
    final Ordering rowOrdering;
    final ColumnarRecordSequence.Builder rowColumns;
    if (rowOutput) {
      final Ordering.OrderModifier[] rowModifiers = Arrays.copyOf(orderModifiers, orderModifiers.length + 2);
      rowModifiers[orderModifiers.length] = new Ordering.OrderModifier(true, true, null);
      rowModifiers[orderModifiers.length + 1] = new Ordering.OrderModifier(true, true, null);
      rowOrdering = new Ordering(new Expr[0], rowModifiers);
      final boolean[] textKeys = new boolean[stringKeys.length];
      for (int key = 0; key < textKeys.length; key++) {
        textKeys[key] = stringKeys[key] || timestampKeys[key];
      }
      rowColumns = new ColumnarRecordSequence.Builder(outputNames, textKeys, 256);
    } else {
      rowOrdering = null;
      rowColumns = null;
    }
    final ProbeFold fold = new ProbeFold(buildColumns, buildSide, probeColumns, probeSide, stringKeys, timestampKeys,
        buildRows, nextRow, denseHeads, firstRow, distinctCapacity, distinctDomains, probeValues, probePresent, groups,
        singleGroups, rowOrdering, rowColumns, denseBuild && denseProbe);
    final int emittedRows = probeRows(fold, probeColumns, denseProbe, probeTimestamps, probeFieldCount);
    SirixVectorizedExecutor.noteJoinGroupServed();
    return rowOrdering != null
        ? orderedRows(rowOrdering, emittedRows, rowColumns)
        : emit(groups, buildColumns, stringKeys, timestampKeys);
  }

  /**
   * The per-match work of the probe loop and its scratch; the per-row value reads differ by
   * representation and live in the leaf loops, which load {@link #probeValues} before folding.
   */
  private final class ProbeFold {
    final MaskedColumns buildColumns;
    final int buildSide;
    final MaskedColumns probeColumns;
    final int probeSide;
    final boolean[] stringKeys;
    final boolean[] timestampKeys;
    final BuildRows buildRows;
    final int[] nextRow;
    final int @Nullable [] denseHeads;
    final Long2IntOpenHashMap firstRow;
    final int distinctCapacity;
    final DistinctDomain[] distinctDomains;
    final long[] probeValues;
    final boolean[] probePresent;
    final Map<GroupKey, GroupState> groups;
    final @Nullable Long2ObjectOpenHashMap<GroupState> singleGroups;
    final @Nullable Ordering rowOrdering;
    final ColumnarRecordSequence.@Nullable Builder rowColumns;
    final boolean allPresent;
    final int keyCount = keyFields.length;
    final int aggCount = aggFuncs.length;
    final int buildFieldCount;
    final int accWidth = 1 + 4 * aggFuncs.length;
    final long[] keyComponents = new long[keyFields.length + 1]; // the last word packs the missing flags
    final long[] operands = new long[8];
    final long[] stack = new long[64];
    final long[] programValues = new long[progCode.length];
    final byte[] programState = new byte[progCode.length];
    final boolean[] residualStack = new boolean[Math.max(1, residualCode.length)];
    final GroupKey probeKey = new GroupKey(keyComponents); // Scratch key, cloned on insert.
    int emittedRows;

    ProbeFold(final MaskedColumns buildColumns, final int buildSide, final MaskedColumns probeColumns,
        final int probeSide, final boolean[] stringKeys, final boolean[] timestampKeys, final BuildRows buildRows,
        final int[] nextRow, final int @Nullable [] denseHeads, final Long2IntOpenHashMap firstRow,
        final int distinctCapacity, final DistinctDomain[] distinctDomains, final long[] probeValues,
        final boolean[] probePresent, final Map<GroupKey, GroupState> groups,
        final @Nullable Long2ObjectOpenHashMap<GroupState> singleGroups, final @Nullable Ordering rowOrdering,
        final ColumnarRecordSequence.@Nullable Builder rowColumns, final boolean allPresent) {
      this.buildColumns = buildColumns;
      this.buildSide = buildSide;
      this.probeColumns = probeColumns;
      this.probeSide = probeSide;
      this.stringKeys = stringKeys;
      this.timestampKeys = timestampKeys;
      this.buildRows = buildRows;
      this.nextRow = nextRow;
      this.denseHeads = denseHeads;
      this.firstRow = firstRow;
      this.distinctCapacity = distinctCapacity;
      this.distinctDomains = distinctDomains;
      this.probeValues = probeValues;
      this.probePresent = probePresent;
      this.groups = groups;
      this.singleGroups = singleGroups;
      this.rowOrdering = rowOrdering;
      this.rowColumns = rowColumns;
      this.allPresent = allPresent;
      this.buildFieldCount = buildRows.fieldCount;
    }

    /** The first build row of {@code joinKey}, or -1. */
    int first(final long joinKey) {
      return firstMatch(joinKey, buildRows, denseHeads, firstRow);
    }

    /** Fold every build row chained from {@code at} against the loaded probe row. */
    void matches(int at, final int leaf, final int row) {
      for (; at >= 0; at = nextRow[at]) {
        if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack, allPresent)) {
          continue;
        }
        if (rowOrdering != null) {
          emitRow(keyCount, buildSide, probeSide, buildFieldCount, buildRows, at, probeValues, probePresent, stringKeys,
              timestampKeys, buildColumns, probeColumns, leaf, row, rowOrdering, rowColumns);
          emittedRows++;
          continue;
        }
        foldPair(keyCount, aggCount, accWidth, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent,
            keyComponents, probeKey, groups, singleGroups, distinctCapacity, distinctDomains, operands, stack,
            programValues, programState, allPresent);
      }
    }
  }

  /** The probe loops; the representation is chosen once per leaf, never per row. */
  private static int probeRows(final ProbeFold fold, final MaskedColumns probeColumns, final boolean denseProbe,
      final boolean[] probeTimestamps, final int probeFieldCount) {
    final long[][] probeLanes = new long[probeFieldCount][];
    for (int leaf = 0; leaf < probeColumns.leafCount(); leaf++) {
      final long[] mask = probeColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (!denseProbe) {
        probeLeafSparse(fold, probeColumns, leaf, mask, probeFieldCount);
      } else if (probeColumns.isNative()) {
        probeLeafDense(fold, probeColumns, probeFieldCount, probeTimestamps, leaf, mask);
      } else {
        for (int field = 0; field < probeFieldCount; field++) {
          probeLanes[field] = probeColumns.numericLane(field, leaf);
        }
        probeLeafDense(fold, probeLanes, probeTimestamps, leaf, mask);
      }
    }
    return fold.emittedRows;
  }

  private static void probeLeafSparse(final ProbeFold fold, final MaskedColumns probeColumns, final int leaf,
      final long[] mask, final int probeFieldCount) {
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        if (!probeColumns.present(0, leaf, row)) {
          continue;
        }
        final int at = fold.first(probeColumns.longValue(0, leaf, row));
        if (at < 0) {
          continue;
        }
        readProbeRow(probeColumns, leaf, row, probeFieldCount, fold.probeValues, fold.probePresent);
        fold.matches(at, leaf, row);
      }
    }
  }

  private static void probeLeafDense(final ProbeFold fold, final long[][] lanes, final boolean[] timestamps,
      final int leaf, final long[] mask) {
    final long[] joinLane = lanes[0];
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        final int at = fold.first(joinLane[row]);
        if (at < 0) {
          continue;
        }
        readNumericRow(lanes, timestamps, row, fold.probeValues);
        fold.matches(at, leaf, row);
      }
    }
  }

  /** A native probe leaf: one row load per selected row, the join key is its first lane. */
  /**
   * A native probe leaf: one bulk copy of its lane block, then array reads; the join key is lane 0.
   */
  private static void probeLeafDense(final ProbeFold fold, final MaskedColumns probeColumns, final int fieldCount,
      final boolean[] timestamps, final int leaf, final long[] mask) {
    final int rows = probeColumns.leafRows(leaf);
    final long[] block = HeapWindows.current().leaf(rows * fieldCount);
    HeapWindows.copy(probeColumns.slab(), probeColumns.rowBase(leaf), block, rows * fieldCount);
    final long[] values = fold.probeValues;
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        final int cell = row * fieldCount;
        final int at = fold.first(block[cell]);
        if (at < 0) {
          continue;
        }
        for (int field = 0; field < fieldCount; field++) {
          values[field] = block[cell + field];
        }
        scaleTimestamps(values, timestamps, fieldCount);
        fold.matches(at, leaf, row);
      }
    }
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
   * The lanes one side of the dense extrema kernel reads: this side's row masks (copied, so a packing
   * task may outlive the query's view) and, per leaf, the join key, the two timestamps, the margin
   * operand and, when the side carries them, the distinct operand and the group key. A heap view
   * shares its immutable decoded arrays; a native view reads its resident slab through lane offsets
   * hoisted once per side. Kernels choose the representation once per leaf, never per row. The unused
   * representation holds empty arrays, so no field is nullable but the slab.
   */
  private static final class SideLanes {
    private static final long[][] NO_LANES = new long[0][];
    private static final long[] NO_BASES = new long[0];
    private static final int[] NO_ROWS = new int[0];

    final long[] @Nullable [] masks;
    final long[][] join;
    final long[][] from;
    final long[][] to;
    final long[][] margin;
    final long @Nullable [][] distinct;
    final long @Nullable [][] group;
    final @Nullable MemorySegment slab;
    /** Per leaf: start of the row-major lane block, -1 on a skipped leaf. */
    final long[] rowBase;
    /** Per leaf: rows of the lane block. */
    final int[] leafRows;
    /** Longs per row of the lane block. */
    final int stride;
    /** Lane indices within a row; -1 when the side has no such lane. */
    final int joinLane;
    final int fromLane;
    final int toLane;
    final int marginLane;
    final int distinctLane;
    final int groupLane;

    private SideLanes(final long[] @Nullable [] masks, final long[][] join, final long[][] from, final long[][] to,
        final long[][] margin, final long @Nullable [][] distinct, final long @Nullable [][] group,
        final @Nullable MemorySegment slab, final long[] rowBase, final int[] leafRows, final int stride,
        final int joinLane, final int fromLane, final int toLane, final int marginLane, final int distinctLane,
        final int groupLane) {
      this.masks = masks;
      this.join = join;
      this.from = from;
      this.to = to;
      this.margin = margin;
      this.distinct = distinct;
      this.group = group;
      this.slab = slab;
      this.rowBase = rowBase;
      this.leafRows = leafRows;
      this.stride = stride;
      this.joinLane = joinLane;
      this.fromLane = fromLane;
      this.toLane = toLane;
      this.marginLane = marginLane;
      this.distinctLane = distinctLane;
      this.groupLane = groupLane;
    }

    static SideLanes capture(final MaskedColumns columns, final int fromSlot, final int toSlot, final int marginSlot,
        final int distinctSlot, final int groupSlot) {
      final int leaves = columns.leafCount();
      final long[][] masks = new long[leaves][];
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = columns.rowMask(leaf);
        if (mask != null) {
          masks[leaf] = mask.clone();
        }
      }
      if (columns.isNative()) {
        final long[] rowBase = new long[leaves];
        final int[] leafRows = new int[leaves];
        for (int leaf = 0; leaf < leaves; leaf++) {
          rowBase[leaf] = columns.rowBase(leaf);
          leafRows[leaf] = columns.leafRows(leaf);
        }
        return new SideLanes(masks, NO_LANES, NO_LANES, NO_LANES, NO_LANES, null, null, columns.slab(), rowBase,
            leafRows, (int) (columns.laneStride() / 8L), 0, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot);
      }
      final long[][] join = new long[leaves][];
      final long[][] from = new long[leaves][];
      final long[][] to = new long[leaves][];
      final long[][] margin = new long[leaves][];
      final long[][] distinct = distinctSlot < 0
          ? null
          : new long[leaves][];
      final long[][] group = groupSlot < 0
          ? null
          : new long[leaves][];
      for (int leaf = 0; leaf < leaves; leaf++) {
        if (masks[leaf] == null) {
          continue;
        }
        join[leaf] = columns.numericLane(0, leaf);
        from[leaf] = columns.numericLane(fromSlot, leaf);
        to[leaf] = columns.numericLane(toSlot, leaf);
        margin[leaf] = columns.numericLane(marginSlot, leaf);
        if (distinct != null) {
          distinct[leaf] = columns.numericLane(distinctSlot, leaf);
        }
        if (group != null) {
          group[leaf] = columns.numericLane(groupSlot, leaf);
        }
      }
      return new SideLanes(masks, join, from, to, margin, distinct, group, null, NO_BASES, NO_ROWS, 0, -1, -1, -1, -1,
          -1, -1);
    }

    boolean isNative() {
      return slab != null;
    }
  }

  /**
   * One side of the dense extrema kernel, bucketed by join key in one allocator slab: a counting sort
   * over the build side's dense key domain places every key's rows contiguously, so the join is a
   * merge over keys that reads both sides sequentially. The slab holds {@code slots + 1} bucket
   * starts, then 64-byte aligned records of {@value #STRIDE} longs (from, to, margin, distinct
   * operand or 0, group index or 0), so bucketing touches one line per row. Timestamps keep their
   * persisted unit (epoch seconds on both sides), which orders exactly like the millisecond residuals
   * of the generic kernel. Rows whose key lies outside the domain cannot join and are dropped while
   * bucketing. Immutable once built, so a revision's side is reusable.
   *
   * <p>
   * Packing a large side costs a first touch of its whole record region plus one scattered write per
   * row, which is why a process-cold query never packs its probe side on the critical path: it
   * streams the lanes against the bucketed build side instead ({@link #foldChained}) while the
   * packing runs in the background for the next execution.
   */
  private static final class BucketedSide {
    static final int STRIDE = 5;
    static final long STRIDE_BYTES = 8L * STRIDE;
    /** Lane ordinals within a record copied to a heap window. */
    static final int FROM_LANE = 0;
    static final int TO_LANE = 1;
    static final int MARGIN_LANE = 2;
    static final int DISTINCT_LANE = 3;
    static final int META_LANE = 4;
    static final long FROM = 0L;
    static final long TO = 8L;
    static final long MARGIN = 16L;
    static final long DISTINCT = 24L;
    /** The group index on the side carrying the group key, zero on the other. */
    static final long META = 32L;

    final MemorySegment slab;
    final long rowsBase;
    final int count;
    final int slots;

    private BucketedSide(final MemorySegment slab, final long rowsBase, final int count, final int slots) {
      this.slab = slab;
      this.rowsBase = rowsBase;
      this.count = count;
      this.slots = slots;
    }

    /** Slab bytes for {@code slots} buckets and {@code count} records. */
    static long slabBytes(final int slots, final int count) {
      return DerivedCacheBudget.saturatedAdd(DerivedCacheBudget.alignUp(4L * (slots + 1L)),
          DerivedCacheBudget.saturatedMultiply(count, STRIDE_BYTES));
    }

    /** Count the rows whose key lies in the domain into {@code bucketStart[slot + 1]}; the total. */
    static int count(final SideLanes lanes, final long keyMinimum, final int slots, final int[] bucketStart) {
      final MemorySegment slab = lanes.slab;
      final int leaves = lanes.masks.length;
      final long[] block = slab == null
          ? null
          : HeapWindows.current().leaf(HeapWindows.LEAF_LONGS);
      int total = 0;
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        if (slab == null) {
          total += countLeaf(mask, lanes.join[leaf], keyMinimum, slots, bucketStart);
        } else {
          HeapWindows.copy(slab, lanes.rowBase[leaf], block, lanes.leafRows[leaf] * lanes.stride);
          total += countLeaf(mask, block, lanes.stride, lanes.joinLane, keyMinimum, slots, bucketStart);
        }
      }
      return total;
    }

    private static int countLeaf(final long[] mask, final long[] join, final long keyMinimum, final int slots,
        final int[] bucketStart) {
      int total = 0;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long slot = join[row] - keyMinimum;
          if (slot >= 0 && slot < slots) {
            bucketStart[(int) slot + 1]++;
            total++;
          }
        }
      }
      return total;
    }

    private static int countLeaf(final long[] mask, final long[] block, final int stride, final int joinLane,
        final long keyMinimum, final int slots, final int[] bucketStart) {
      int total = 0;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long slot = block[(rowBase + bit) * stride + joinLane] - keyMinimum;
          if (slot >= 0 && slot < slots) {
            bucketStart[(int) slot + 1]++;
            total++;
          }
        }
      }
      return total;
    }

    /**
     * Write the bucket starts and scatter the records into {@code slab}, sized by {@link #slabBytes};
     * {@code bucketStart} holds the per-bucket counts from {@link #count}.
     */
    static BucketedSide pack(final MemorySegment slab, final SideLanes lanes, final long keyMinimum, final int slots,
        final long groupMinimum, final int[] bucketStart, final int total) {
      for (int slot = 0; slot < slots; slot++) {
        bucketStart[slot + 1] += bucketStart[slot];
      }
      for (int slot = 0; slot <= slots; slot++) {
        SegmentAccess.putInt(slab, 4L * slot, bucketStart[slot]);
      }
      final long rowsBase = DerivedCacheBudget.alignUp(4L * (slots + 1L));
      if (rowsBase + total * STRIDE_BYTES > slab.byteSize()) {
        throw new IllegalArgumentException("slab smaller than the packed side");
      }
      final int[] fill = Arrays.copyOf(bucketStart, slots);
      final MemorySegment lanesSlab = lanes.slab;
      final int leaves = lanes.masks.length;
      final long[] block = lanesSlab == null
          ? null
          : HeapWindows.current().leaf(HeapWindows.LEAF_LONGS);
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        if (lanesSlab == null) {
          fillLeaf(slab, rowsBase, lanes, leaf, mask, fill, keyMinimum, slots, groupMinimum);
        } else {
          HeapWindows.copy(lanesSlab, lanes.rowBase[leaf], block, lanes.leafRows[leaf] * lanes.stride);
          fillLeaf(slab, rowsBase, block, lanes, mask, fill, keyMinimum, slots, groupMinimum);
        }
      }
      return new BucketedSide(slab, rowsBase, total, slots);
    }

    private static void fillLeaf(final MemorySegment slab, final long rowsBase, final SideLanes lanes, final int leaf,
        final long[] mask, final int[] fill, final long keyMinimum, final int slots, final long groupMinimum) {
      final long[] join = lanes.join[leaf];
      final long[] fromLane = lanes.from[leaf];
      final long[] toLane = lanes.to[leaf];
      final long[] marginLane = lanes.margin[leaf];
      final long[] distinctLane = lanes.distinct == null
          ? null
          : lanes.distinct[leaf];
      final long[] groupLane = lanes.group == null
          ? null
          : lanes.group[leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long slot = join[row] - keyMinimum;
          if (slot < 0 || slot >= slots) {
            continue;
          }
          final long at = rowsBase + fill[(int) slot]++ * STRIDE_BYTES;
          SegmentAccess.putLong(slab, at + FROM, fromLane[row]);
          SegmentAccess.putLong(slab, at + TO, toLane[row]);
          SegmentAccess.putLong(slab, at + MARGIN, marginLane[row]);
          SegmentAccess.putLong(slab, at + DISTINCT, distinctLane == null
              ? 0L
              : distinctLane[row]);
          SegmentAccess.putLong(slab, at + META, groupLane == null
              ? 0L
              : groupLane[row] - groupMinimum);
        }
      }
    }

    private static void fillLeaf(final MemorySegment slab, final long rowsBase, final long[] block,
        final SideLanes lanes, final long[] mask, final int[] fill, final long keyMinimum, final int slots,
        final long groupMinimum) {
      final int stride = lanes.stride;
      final int joinLane = lanes.joinLane;
      final int fromLane = lanes.fromLane;
      final int toLane = lanes.toLane;
      final int marginLane = lanes.marginLane;
      final int distinctLane = lanes.distinctLane;
      final int groupLane = lanes.groupLane;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = (rowBase + bit) * stride;
          final long slot = block[row + joinLane] - keyMinimum;
          if (slot < 0 || slot >= slots) {
            continue;
          }
          final long at = rowsBase + fill[(int) slot]++ * STRIDE_BYTES;
          SegmentAccess.putLong(slab, at + FROM, block[row + fromLane]);
          SegmentAccess.putLong(slab, at + TO, block[row + toLane]);
          SegmentAccess.putLong(slab, at + MARGIN, block[row + marginLane]);
          SegmentAccess.putLong(slab, at + DISTINCT, distinctLane < 0
              ? 0L
              : block[row + distinctLane]);
          SegmentAccess.putLong(slab, at + META, groupLane < 0
              ? 0L
              : block[row + groupLane] - groupMinimum);
        }
      }
    }

    /** The first record ordinal of {@code slot}; {@code slot == slots} is the end. */
    int bucketStart(final int slot) {
      return SegmentAccess.getInt(slab, 4L * slot);
    }
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

    static SideHandle leased(final BucketedSideCache.PackedSide entry) {
      return new SideHandle(entry.side, entry, null);
    }

    static SideHandle owned(final BucketedSide side, final MemorySegment slab) {
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
        return other instanceof InFlightKey key && store.get() != null && key.store.get() == store.get()
            && key.rows == rows && Arrays.equals(key.signature, signature);
      }

      @Override
      public int hashCode() {
        return 31 * System.identityHashCode(rows) + Arrays.hashCode(signature);
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
     * or when the same side is already being packed. A native source keeps its own lease until the
     * packing finished.
     */
    @SuppressWarnings("FutureReturnValueIgnored") // Failures complete the separately tracked packing future.
    static void packAsync(final MaskedColumns columns, final long[] signature, final SideLanes lanes,
        final long keyMinimum, final int slots, final long groupMinimum) {
      if (!columns.isImmutableRevision()) {
        return;
      }
      final InFlightKey key = new InFlightKey(new WeakReference<>(columns.store()), columns.rowIdentity(), signature);
      final CompletableFuture<PackedSide> packing = new CompletableFuture<>();
      if (IN_FLIGHT.putIfAbsent(key, packing) != null) {
        return;
      }
      final Reservation reservation;
      final int[] bucketStart = new int[slots + 1];
      final int total;
      try {
        total = BucketedSide.count(lanes, keyMinimum, slots, bucketStart);
        reservation = DerivedCacheBudget.global().packed().reserve(BucketedSide.slabBytes(slots, total));
      } catch (final RuntimeException | Error e) {
        packing.completeExceptionally(e);
        IN_FLIGHT.remove(key, packing);
        throw e;
      }
      if (reservation == null) {
        packing.complete(null);
        IN_FLIGHT.remove(key, packing);
        return;
      }
      final DerivedCacheBudget.Lease lease = columns.isNative()
          ? columns.acquireTaskLease()
          : null;
      BUCKETED_SIDES_BUILT.increment();
      try {
        final Runnable task = () -> {
          try (reservation) {
            final BucketedSide side =
                BucketedSide.pack(reservation.slab(), lanes, keyMinimum, slots, groupMinimum, bucketStart, total);
            final PackedSide packed = new PackedSide(key.rows(), signature, side);
            reservation.publish(packed);
            packed.release();
            packing.complete(packed);
          } catch (final RuntimeException | Error e) {
            packing.completeExceptionally(e);
          } finally {
            if (lease != null) {
              lease.run();
            }
            IN_FLIGHT.remove(key, packing);
          }
        };
        final Executor executor = packingExecutorForTesting;
        if (executor == null) {
          CompletableFuture.runAsync(task);
        } else {
          CompletableFuture.runAsync(task, executor);
        }
      } catch (final RuntimeException | Error e) {
        reservation.close();
        if (lease != null) {
          lease.run();
        }
        packing.completeExceptionally(e);
        IN_FLIGHT.remove(key, packing);
        throw e;
      }
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
    final int total = BucketedSide.count(lanes, keyMinimum, slots, bucketStart);
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
        reservation.publish(packed);
        // The publisher's lease carries this query's read; the cache keeps its own.
        return SideHandle.leased(packed);
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
   * The flat fold state of the dense extrema kernel: per-group minimum/maximum of the residual and
   * distinct values per group as one value-major bitmap or, when the distinct domain is unbounded,
   * one set per group. {@link #pair} folds one probe row against the build rows of its key, read from
   * the build side's slab.
   */
  private static final class ExtremaFold {
    final long[] groupMinimum;
    final long[] groupMaximum;
    final long[] distinctCounts;
    final long @Nullable [] bits;
    final LongOpenHashSet @Nullable [] sets;
    final MemorySegment buildSlab;
    /** One build record's from, to, margin and distinct lanes, loaded with one checked access. */
    final long[] buildRecord = new long[NativeRows.LANES];
    final int groups;
    final long domainMinimum;
    final boolean distinctBuild;
    final boolean buildMinusProbe;

    ExtremaFold(final int groups, final @Nullable DistinctDomain domain, final BucketedSide build,
        final boolean distinctBuild, final boolean buildMinusProbe) {
      this.groups = groups;
      this.buildSlab = build.slab;
      this.distinctBuild = distinctBuild;
      this.buildMinusProbe = buildMinusProbe;
      groupMinimum = new long[groups];
      groupMaximum = new long[groups];
      distinctCounts = new long[groups];
      Arrays.fill(groupMinimum, Long.MAX_VALUE);
      Arrays.fill(groupMaximum, Long.MIN_VALUE);
      domainMinimum = domain == null
          ? 0L
          : domain.minimum();
      // Value-major bit layout: the bits of one distinct value's groups are adjacent.
      bits = domain == null
          ? null
          : new long[groups * domain.words()];
      sets = domain == null
          ? new LongOpenHashSet[groups]
          : null;
    }

    /**
     * Fold one probe row against the build records in {@code [buildBegin, buildEnd)} byte offsets of
     * the build slab. Each record's first four lanes arrive through one vector load; only an
     * overlapping record also reads its group lane. Used where no heap window holds the bucket.
     */
    void pair(final long from, final long to, final long margin, final long probeValue, final long buildBegin,
        final long buildEnd) {
      final MemorySegment slab = this.buildSlab;
      final long[] record = this.buildRecord;
      for (long b = buildBegin; b < buildEnd; b += BucketedSide.STRIDE_BYTES) {
        if (NativeRows.VECTOR_ROWS) {
          NativeRows.load(slab, b, record);
        } else {
          record[0] = SegmentAccess.getLong(slab, b + BucketedSide.FROM);
          record[1] = SegmentAccess.getLong(slab, b + BucketedSide.TO);
          record[2] = SegmentAccess.getLong(slab, b + BucketedSide.MARGIN);
          record[3] = SegmentAccess.getLong(slab, b + BucketedSide.DISTINCT);
        }
        if (from >= record[1] || record[0] >= to) {
          continue;
        }
        accumulate((int) SegmentAccess.getLong(slab, b + BucketedSide.META), record[2], margin, distinctBuild
            ? record[3]
            : probeValue);
      }
    }

    /**
     * Fold one probe row against the build records {@code [buildBegin, buildEnd)} (lane indices) of a
     * heap window: plain array reads, which the compiler range-checks once per loop.
     */
    void pair(final long from, final long to, final long margin, final long probeValue, final long[] buildRows,
        final int buildBegin, final int buildEnd) {
      for (int b = buildBegin; b < buildEnd; b += BucketedSide.STRIDE) {
        if (from >= buildRows[b + BucketedSide.TO_LANE] || buildRows[b + BucketedSide.FROM_LANE] >= to) {
          continue;
        }
        accumulate((int) buildRows[b + BucketedSide.META_LANE], buildRows[b + BucketedSide.MARGIN_LANE], margin,
            distinctBuild
                ? buildRows[b + BucketedSide.DISTINCT_LANE]
                : probeValue);
      }
    }

    /** One strictly overlapping pair: residual extrema and the distinct value of {@code group}. */
    private void accumulate(final int group, final long buildMargin, final long margin, final long distinctValue) {
      final long value = buildMinusProbe
          ? Math.subtractExact(buildMargin, margin)
          : Math.subtractExact(margin, buildMargin);
      if (value < groupMinimum[group]) {
        groupMinimum[group] = value;
      }
      if (value > groupMaximum[group]) {
        groupMaximum[group] = value;
      }
      if (bits != null) {
        final long bitIndex = (distinctValue - domainMinimum) * groups + group;
        final int bitWord = (int) (bitIndex >>> 6);
        final long bit = 1L << bitIndex;
        final long prior = bits[bitWord];
        if ((prior & bit) == 0L) {
          bits[bitWord] = prior | bit;
          distinctCounts[group]++;
        }
      } else {
        addToSet(group, distinctValue);
      }
    }

    /**
     * The unbounded-domain fallback, kept out of {@link #pair} so the hot fold stays small enough to
     * inline.
     */
    @SuppressWarnings("NullAway") // Called only when bits is null, which proves sets is non-null.
    private void addToSet(final int group, final long distinctValue) {
      LongOpenHashSet set = sets[group];
      if (set == null) {
        set = new LongOpenHashSet(16);
        sets[group] = set;
      }
      set.add(distinctValue);
    }

    @SuppressWarnings("NullAway") // The constructor allocates sets exactly when bits is null.
    long distinctCount(final int group) {
      return bits != null
          ? distinctCounts[group]
          : sets[group] == null
              ? 0L
              : sets[group].size();
    }
  }

  /** Consecutive buckets per heap window; the slot cap bounds the per-window start arrays. */
  private static final int WINDOW_SLOTS = HeapWindows.WINDOW_SLOTS;
  /** Build records per heap window (80 KiB) and probe records per window (320 KiB): L2-resident. */
  private static final int BUILD_WINDOW_RECORDS = HeapWindows.BUILD_RECORDS;
  private static final int PROBE_WINDOW_RECORDS = HeapWindows.PROBE_RECORDS;

  /**
   * Merge two bucketed sides key by key. Buckets are contiguous and ascending in both slabs, so a
   * window of consecutive slots is one contiguous record range per side: each window is copied once
   * into reusable heap arrays (two bulk copies, each checked once) and merged as arrays, which the
   * compiler range-checks per loop instead of per access. A single bucket larger than a window is
   * merged from the slabs directly.
   */
  private static void foldMerge(final ExtremaFold fold, final BucketedSide build, final BucketedSide probe,
      final int slots) {
    final MemorySegment buildSlab = build.slab;
    final MemorySegment probeSlab = probe.slab;
    final int stride = BucketedSide.STRIDE;
    final HeapWindows windows = HeapWindows.current();
    final long[] buildWindow = windows.build(BUILD_WINDOW_RECORDS * stride);
    final long[] probeWindow = windows.probe(PROBE_WINDOW_RECORDS * stride);
    final int[] buildStarts = windows.buildStarts(WINDOW_SLOTS);
    final int[] probeStarts = windows.probeStarts(WINDOW_SLOTS);
    int slot = 0;
    int buildBeginRow = SegmentAccess.getInt(buildSlab, 0L);
    int probeBeginRow = SegmentAccess.getInt(probeSlab, 0L);
    while (slot < slots) {
      // Extend the window over consecutive slots while both record ranges fit it.
      int count = 0;
      int buildEndRow = buildBeginRow;
      int probeEndRow = probeBeginRow;
      while (count < WINDOW_SLOTS && slot + count < slots) {
        final int nextBuild = SegmentAccess.getInt(buildSlab, 4L * (slot + count + 1));
        final int nextProbe = SegmentAccess.getInt(probeSlab, 4L * (slot + count + 1));
        if (nextBuild - buildBeginRow > BUILD_WINDOW_RECORDS || nextProbe - probeBeginRow > PROBE_WINDOW_RECORDS) {
          break;
        }
        count++;
        buildEndRow = nextBuild;
        probeEndRow = nextProbe;
        buildStarts[count] = (nextBuild - buildBeginRow) * stride;
        probeStarts[count] = (nextProbe - probeBeginRow) * stride;
      }
      if (count == 0) {
        final int nextBuild = SegmentAccess.getInt(buildSlab, 4L * (slot + 1));
        final int nextProbe = SegmentAccess.getInt(probeSlab, 4L * (slot + 1));
        foldSlotFromSlabs(fold, build, probe, buildBeginRow, nextBuild, probeBeginRow, nextProbe);
        slot++;
        buildBeginRow = nextBuild;
        probeBeginRow = nextProbe;
        continue;
      }
      if (buildEndRow != buildBeginRow && probeEndRow != probeBeginRow) {
        MemorySegment.copy(buildSlab, ValueLayout.JAVA_LONG, build.rowsBase + buildBeginRow * BucketedSide.STRIDE_BYTES,
            buildWindow, 0, (buildEndRow - buildBeginRow) * stride);
        MemorySegment.copy(probeSlab, ValueLayout.JAVA_LONG, probe.rowsBase + probeBeginRow * BucketedSide.STRIDE_BYTES,
            probeWindow, 0, (probeEndRow - probeBeginRow) * stride);
        buildStarts[0] = 0;
        probeStarts[0] = 0;
        for (int s = 0; s < count; s++) {
          final int buildBegin = buildStarts[s];
          final int buildEnd = buildStarts[s + 1];
          final int probeEnd = probeStarts[s + 1];
          if (buildBegin == buildEnd) {
            continue;
          }
          for (int p = probeStarts[s]; p < probeEnd; p += stride) {
            fold.pair(probeWindow[p + BucketedSide.FROM_LANE], probeWindow[p + BucketedSide.TO_LANE],
                probeWindow[p + BucketedSide.MARGIN_LANE], probeWindow[p + BucketedSide.DISTINCT_LANE], buildWindow,
                buildBegin, buildEnd);
          }
        }
      }
      slot += count;
      buildBeginRow = buildEndRow;
      probeBeginRow = probeEndRow;
    }
  }

  /**
   * One bucket too large for a heap window: its probe records stream from the slab, one vector load
   * each.
   */
  private static void foldSlotFromSlabs(final ExtremaFold fold, final BucketedSide build, final BucketedSide probe,
      final int buildBeginRow, final int buildEndRow, final int probeBeginRow, final int probeEndRow) {
    if (buildBeginRow == buildEndRow || probeBeginRow == probeEndRow) {
      return;
    }
    final MemorySegment probeSlab = probe.slab;
    final long stride = BucketedSide.STRIDE_BYTES;
    final long buildBegin = build.rowsBase + buildBeginRow * stride;
    final long buildEnd = build.rowsBase + buildEndRow * stride;
    final long probeEnd = probe.rowsBase + probeEndRow * stride;
    final long[] record = new long[NativeRows.LANES];
    for (long p = probe.rowsBase + probeBeginRow * stride; p < probeEnd; p += stride) {
      if (NativeRows.VECTOR_ROWS) {
        NativeRows.load(probeSlab, p, record);
      } else {
        record[0] = SegmentAccess.getLong(probeSlab, p + BucketedSide.FROM);
        record[1] = SegmentAccess.getLong(probeSlab, p + BucketedSide.TO);
        record[2] = SegmentAccess.getLong(probeSlab, p + BucketedSide.MARGIN);
        record[3] = SegmentAccess.getLong(probeSlab, p + BucketedSide.DISTINCT);
      }
      fold.pair(record[0], record[1], record[2], record[3], buildBegin, buildEnd);
    }
  }

  /**
   * Stream the probe lanes in row order against the bucketed build side: no probe record is written,
   * so a process-cold execution pays only the lane reads, with the same fold state as
   * {@link #foldMerge}.
   */
  private static int foldChained(final ExtremaFold fold, final BucketedSide build, final SideLanes lanes,
      final long keyMinimum, final int slots) {
    final MemorySegment lanesSlab = lanes.slab;
    final long[] block = lanesSlab == null
        ? null
        : HeapWindows.current().leaf(HeapWindows.LEAF_LONGS);
    final int leaves = lanes.masks.length;
    int admitted = 0;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final long[] mask = lanes.masks[leaf];
      if (mask == null) {
        continue;
      }
      if (lanesSlab == null) {
        admitted += foldChainedLeaf(fold, build, lanes, leaf, mask, keyMinimum, slots);
      } else {
        HeapWindows.copy(lanesSlab, lanes.rowBase[leaf], block, lanes.leafRows[leaf] * lanes.stride);
        admitted += foldChainedLeaf(fold, build, block, lanes, mask, keyMinimum, slots);
      }
    }
    return admitted;
  }

  private static int foldChainedLeaf(final ExtremaFold fold, final BucketedSide build, final SideLanes lanes,
      final int leaf, final long[] mask, final long keyMinimum, final int slots) {
    final MemorySegment buildSlab = build.slab;
    final long buildRows = build.rowsBase;
    final long stride = BucketedSide.STRIDE_BYTES;
    final long[] join = lanes.join[leaf];
    final long[] fromLane = lanes.from[leaf];
    final long[] toLane = lanes.to[leaf];
    final long[] marginLane = lanes.margin[leaf];
    final long[] distinctLane = lanes.distinct == null
        ? null
        : lanes.distinct[leaf];
    int admitted = 0;
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      while (word != 0L) {
        final int row = (w << 6) + Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final long slot = join[row] - keyMinimum;
        if (slot < 0 || slot >= slots) {
          continue;
        }
        admitted++;
        final int buildBeginRow = SegmentAccess.getInt(buildSlab, 4L * (int) slot);
        final int buildEndRow = SegmentAccess.getInt(buildSlab, 4L * ((int) slot + 1));
        if (buildBeginRow == buildEndRow) {
          continue;
        }
        fold.pair(fromLane[row], toLane[row], marginLane[row], distinctLane == null
            ? 0L
            : distinctLane[row], buildRows + buildBeginRow * stride, buildRows + buildEndRow * stride);
      }
    }
    return admitted;
  }

  private static int foldChainedLeaf(final ExtremaFold fold, final BucketedSide build, final long[] block,
      final SideLanes lanes, final long[] mask, final long keyMinimum, final int slots) {
    final MemorySegment buildSlab = build.slab;
    final long buildRows = build.rowsBase;
    final long stride = BucketedSide.STRIDE_BYTES;
    final int laneStride = lanes.stride;
    final int joinLane = lanes.joinLane;
    final int fromLane = lanes.fromLane;
    final int toLane = lanes.toLane;
    final int marginLane = lanes.marginLane;
    final int distinctLane = lanes.distinctLane;
    int admitted = 0;
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = (rowBase + bit) * laneStride;
        final long slot = block[row + joinLane] - keyMinimum;
        if (slot < 0 || slot >= slots) {
          continue;
        }
        admitted++;
        final int buildBeginRow = SegmentAccess.getInt(buildSlab, 4L * (int) slot);
        final int buildEndRow = SegmentAccess.getInt(buildSlab, 4L * ((int) slot + 1));
        if (buildBeginRow == buildEndRow) {
          continue;
        }
        fold.pair(block[row + fromLane], block[row + toLane], block[row + marginLane], distinctLane < 0
            ? 0L
            : block[row + distinctLane], buildRows + buildBeginRow * stride, buildRows + buildEndRow * stride);
      }
    }
    return admitted;
  }

  /**
   * The dense extrema kernel: both sides are bucketed by join key, the buckets are merged key by key
   * and every strictly overlapping pair folds into flat per-group minimum/maximum lanes and one flat
   * distinct bitmap (or a per-group set when the distinct domain is unbounded). The final sort orders
   * groups by their keys. A probe side that is not cached yet is streamed from its lanes while it is
   * packed in the background for the next execution.
   */
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
          foldMerge(fold, build.side, probe.side, slots);
          probeRows = probe.side.count;
        } else {
          final SideLanes lanes = SideLanes.capture(probeColumns, plan.probeFromSlot(), plan.probeToSlot(),
              plan.probeMarginSlot(), probeDistinctSlot, -1);
          BucketedSideCache.packAsync(probeColumns, probeSignature, lanes, plan.joinMinimum(), slots, 0L);
          probeRows = foldChained(fold, build.side, lanes, plan.joinMinimum(), slots);
        }
        final long merged = DIAG
            ? System.nanoTime()
            : 0L;
        final long[] groupMinimum = fold.groupMinimum;
        final long[] groupMaximum = fold.groupMaximum;
        final Map<GroupKey, GroupState> result = new LinkedHashMap<>();
        final int width = 1 + 4 * aggKinds.length;
        final int minBase = 1 + 4 * plan.minimum();
        final int maxBase = 1 + 4 * plan.maximum();
        int seen = 0;
        for (int group = 0; group < groups; group++) {
          if (groupMaximum[group] == Long.MIN_VALUE && groupMinimum[group] == Long.MAX_VALUE) {
            continue;
          }
          seen++;
          final long[] acc = new long[width];
          acc[minBase] = 1;
          acc[maxBase] = 1;
          acc[minBase + 2] = groupMinimum[group];
          acc[maxBase + 3] = groupMaximum[group];
          final DistinctValues[] distinctSets = new DistinctValues[aggKinds.length];
          distinctSets[plan.distinct()] = new DistinctValues(fold.distinctCount(group));
          result.put(new GroupKey(new long[] {plan.groupMinimum() + group, 0}), new GroupState(acc, distinctSets));
        }
        if (DIAG) {
          System.err.printf(
              "[join-serve] dense extrema (%s): build %.2f ms, lookup %.2f ms, fold %.2f ms, emit %.2f ms"
                  + " (%d build rows, %d probe rows, %d slots, %d groups)%n",
              probe != null
                  ? "merge"
                  : "chained, packing in background",
              (built - started) / 1e6, (packed - built) / 1e6, (merged - packed) / 1e6,
              (System.nanoTime() - merged) / 1e6, build.side.count, probeRows, slots, seen);
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

  private static boolean denseNumeric(final MaskedColumns columns, final int fields) {
    for (int field = 0; field < fields; field++) {
      if ((!columns.isLong(field) && !columns.isTimestamp(field)) || !columns.allSelectedPresent(field)) {
        return false;
      }
    }
    return true;
  }

  private static void readNumericRow(final long[][] lanes, final boolean[] timestamps, final int row,
      final long[] values) {
    for (int field = 0; field < lanes.length; field++) {
      values[field] = timestamps[field]
          ? Math.multiplyExact(lanes[field][row], 1000L)
          : lanes[field][row];
    }
  }


  private static void scaleTimestamps(final long[] values, final boolean[] timestamps, final int fieldCount) {
    for (int field = 0; field < fieldCount; field++) {
      if (timestamps[field]) {
        values[field] = Math.multiplyExact(values[field], 1000L);
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

  private record DistinctDomain(long minimum, int words) {
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

  private static int[] build(final MaskedColumns buildColumns, final int buildFieldCount, final BuildRows buildRows,
      final boolean denseNumeric) {
    buildRows.nextRow = new int[Math.max(16, (int) Math.min(Integer.MAX_VALUE - 8, buildColumns.rows()))];
    final long[][] lanes = new long[buildFieldCount][];
    final long[] values = new long[Math.max(NativeRows.LANES, buildFieldCount)];
    final boolean[] timestamps = new boolean[buildFieldCount];
    if (denseNumeric) {
      for (int field = 0; field < buildFieldCount; field++) {
        timestamps[field] = buildColumns.isTimestamp(field);
      }
    }
    // BUILD: every admitted row with a present join key, chained per key value. The representation
    // is chosen once per leaf; the row loops read arrays or the slab through hoisted lane offsets.
    for (int leaf = 0; leaf < buildColumns.leafCount(); leaf++) {
      final long[] mask = buildColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (!denseNumeric) {
        buildLeafSparse(buildColumns, leaf, mask, buildFieldCount, buildRows);
      } else if (buildColumns.isNative()) {
        buildLeafDense(buildColumns, timestamps, leaf, mask, buildFieldCount, buildRows, values);
      } else {
        for (int field = 0; field < buildFieldCount; field++) {
          lanes[field] = buildColumns.numericLane(field, leaf);
        }
        buildLeafDense(lanes, timestamps, leaf, mask, buildFieldCount, buildRows, values, buildColumns);
      }
    }
    return buildRows.nextRow;
  }

  private static void buildLeafSparse(final MaskedColumns buildColumns, final int leaf, final long[] mask,
      final int buildFieldCount, final BuildRows buildRows) {
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        if (!buildColumns.present(0, leaf, row)) {
          continue; // a missing join key matches nothing
        }
        final int at = buildRows.addKeyed(buildColumns.longValue(0, leaf, row), buildColumns.recordKey(leaf, row));
        readBuildRow(buildColumns, leaf, row, buildFieldCount, buildRows, at);
      }
    }
  }

  private static void buildLeafDense(final long[][] lanes, final boolean[] timestamps, final int leaf,
      final long[] mask, final int buildFieldCount, final BuildRows buildRows, final long[] values,
      final MaskedColumns buildColumns) {
    final long[] joinLane = lanes[0];
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        final int at = buildRows.addKeyed(joinLane[row], buildColumns.recordKey(leaf, row));
        readNumericRow(lanes, timestamps, row, values);
        System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
      }
    }
  }

  /** A native build leaf: one row load plus one record-key read per selected row. */
  /** A native build leaf: one bulk copy of its lane block and record keys, then array reads. */
  private static void buildLeafDense(final MaskedColumns buildColumns, final boolean[] timestamps, final int leaf,
      final long[] mask, final int buildFieldCount, final BuildRows buildRows, final long[] values) {
    final HeapWindows windows = HeapWindows.current();
    final MemorySegment slab = buildColumns.slab();
    final int rows = buildColumns.leafRows(leaf);
    final long[] block = windows.leaf(rows * buildFieldCount);
    HeapWindows.copy(slab, buildColumns.rowBase(leaf), block, rows * buildFieldCount);
    final long[] keys = windows.keys(rows);
    HeapWindows.copy(slab, buildColumns.keyBase(leaf), keys, rows);
    for (int w = 0; w < mask.length; w++) {
      long word = mask[w];
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        final int cell = row * buildFieldCount;
        for (int field = 0; field < buildFieldCount; field++) {
          values[field] = block[cell + field];
        }
        final int at = buildRows.addKeyed(values[0], keys[row]);
        scaleTimestamps(values, timestamps, buildFieldCount);
        System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
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
      final MaskedColumns probeColumns, final int leaf, final int row, final Ordering rowOrdering,
      final ColumnarRecordSequence.Builder rowColumns) {
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
    orderKeys[orderIndexes.length + probeSide] = new Int64(probeColumns.recordKey(leaf, row));
    rowOrdering.add(orderKeys, new TupleImpl(new Int64(outputRow)));
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
