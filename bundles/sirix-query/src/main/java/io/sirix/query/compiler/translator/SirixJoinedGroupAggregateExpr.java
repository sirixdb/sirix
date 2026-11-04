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
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import io.sirix.query.scan.MaskedColumns;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
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
    final long[] probeValues = new long[probeFieldCount];
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
    final RowOrder rowOrder;
    final ColumnarRecordSequence.Builder rowColumns;
    if (rowOutput) {
      final boolean[] textKeys = new boolean[stringKeys.length];
      for (int key = 0; key < textKeys.length; key++) {
        textKeys[key] = stringKeys[key] || timestampKeys[key];
      }
      rowColumns = new ColumnarRecordSequence.Builder(outputNames, textKeys, 256);
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
    SirixVectorizedExecutor.noteJoinGroupServed();
    if (rowOrder != null) {
      return rowOrder.columns.build(rowOrder.sorted());
    }
    return rowOrdering != null
        ? orderedRows(rowOrdering, emittedRows, rowColumns)
        : emit(groups, buildColumns, stringKeys, timestampKeys);
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
   * columns present on every emitted row: each row's key values and both record keys live in one
   * primitive buffer and the rows are sorted by index, in the order the tuple sort would produce (the
   * keys by their modifiers, then the record keys of side 0 and side 1 ascending, which are unique).
   */
  private static final class RowOrder implements IntComparator {
    final ColumnarRecordSequence.Builder columns;
    final long[] scratch;
    private final boolean[] ascending;
    private final int keyCount;
    private final int stride;
    private long[] values;
    private int[] rows;
    private int count;

    RowOrder(final boolean[] ascending, final ColumnarRecordSequence.Builder columns, final int capacity) {
      this.ascending = ascending;
      this.columns = columns;
      this.keyCount = ascending.length;
      this.stride = keyCount + 2;
      this.scratch = new long[keyCount];
      this.values = new long[capacity * stride];
      this.rows = new int[capacity];
    }

    /** Append one emitted row: its ordering keys (from {@link #scratch}) and the two record keys. */
    void add(final int outputRow, final long side0Key, final long side1Key) {
      if (count == rows.length) {
        rows = Arrays.copyOf(rows, rows.length << 1);
        values = Arrays.copyOf(values, values.length << 1);
      }
      final int base = count * stride;
      System.arraycopy(scratch, 0, values, base, keyCount);
      values[base + keyCount] = side0Key;
      values[base + keyCount + 1] = side1Key;
      rows[count++] = outputRow;
    }

    @Override
    public int compare(final int a, final int b) {
      final int x = a * stride;
      final int y = b * stride;
      for (int k = 0; k < keyCount; k++) {
        final int c = Long.compare(values[x + k], values[y + k]);
        if (c != 0) {
          return ascending[k]
              ? c
              : -c;
        }
      }
      final int c0 = Long.compare(values[x + keyCount], values[y + keyCount]);
      return c0 != 0
          ? c0
          : Long.compare(values[x + keyCount + 1], values[y + keyCount + 1]);
    }

    /** The output rows in order. */
    int[] sorted() {
      final int[] order = new int[count];
      for (int i = 0; i < count; i++) {
        order[i] = i;
      }
      IntArrays.quickSort(order, 0, count, this);
      final int[] result = new int[count];
      for (int i = 0; i < count; i++) {
        result[i] = rows[order[i]];
      }
      return result;
    }
  }

  /**
   * The probe loop: one method whose fold scratch lives in locals, so the pair fold inlines into the
   * row loop. A dense probe side reads its lanes directly, from heap arrays or from a resident slab
   * through lane offsets hoisted per leaf; the representation is a loop-invariant choice, never a
   * per-row call, and the slab read site only ever sees native segments.
   */
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
    final boolean nativeProbe = denseProbe && probeColumns.isNative();
    final MemorySegment slab = nativeProbe
        ? probeColumns.slab()
        : MemorySegment.NULL;
    final long[][] probeLanes = new long[probeFieldCount][];
    final long[] laneOffsets = new long[probeFieldCount];
    int emittedRows = 0;
    for (int leaf = 0; leaf < probeColumns.leafCount(); leaf++) {
      final long[] mask = probeColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (nativeProbe) {
        for (int field = 0; field < probeFieldCount; field++) {
          laneOffsets[field] = laneOffset(probeColumns, field, leaf);
        }
      } else if (denseProbe) {
        for (int field = 0; field < probeFieldCount; field++) {
          probeLanes[field] = probeColumns.numericLane(field, leaf);
        }
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
          final long joinKey = nativeProbe
              ? (long) SLAB_LONG.get(slab, laneOffsets[0] + 8L * row)
              : denseProbe
                  ? probeLanes[0][row]
                  : probeColumns.longValue(0, leaf, row);
          int at = firstMatch(joinKey, buildRows, denseHeads, firstRow);
          if (at < 0) {
            continue;
          }
          if (nativeProbe) {
            readNumericRow(slab, laneOffsets, probeTimestamps, row, probeValues);
          } else if (denseProbe) {
            readNumericRow(probeLanes, probeTimestamps, row, probeValues);
          } else {
            readProbeRow(probeColumns, leaf, row, probeFieldCount, probeValues, probePresent);
          }
          for (; at >= 0; at = nextRow[at]) {
            if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack, allPresent)) {
              continue;
            }
            if (rowOrder != null) {
              emitRowPrimitive(keyCount, buildSide, buildFieldCount, buildRows, at, probeValues, stringKeys,
                  timestampKeys, buildColumns, probeColumns, leaf, row, rowOrder);
              emittedRows++;
              continue;
            }
            if (rowOrdering != null) {
              emitRow(keyCount, buildSide, probeSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                  stringKeys, timestampKeys, buildColumns, probeColumns, leaf, row, rowOrdering, rowColumns);
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

  /**
   * The slab offset of a dense lane on {@code leaf}; dense admission proved every selected row has
   * it.
   */
  private static long laneOffset(final MaskedColumns columns, final int field, final int leaf) {
    final long base = columns.laneBase(field, leaf);
    if (base < 0) {
      throw new IllegalStateException("an admitted long field needs a numeric lane");
    }
    return base;
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
   * shares its immutable decoded arrays; a native view supplies the byte offsets of the same lanes in
   * its resident slab, hoisted once per leaf. The representation is one loop-invariant flag, and the
   * unused representation holds empty arrays, so nothing here is nullable but the optional lanes.
   */
  /**
   * The slab's long handle, invoked at each read site of the merge kernels rather than through a
   * helper method, so the access intrinsifies into the kernel that performs it; the layout is
   * unaligned because the slab aligns every value by construction.
   */
  private static final VarHandle SLAB_LONG = ValueLayout.JAVA_LONG_UNALIGNED.varHandle();

  private static final class SideLanes {
    private static final long[][] NO_LANES = new long[0][];
    private static final long[] NO_OFFSETS = new long[0];

    final long[] @Nullable [] masks;
    final long[][] join;
    final long[][] from;
    final long[][] to;
    final long[][] margin;
    final long @Nullable [][] distinct;
    final long @Nullable [][] group;
    /** Whether the lanes live in {@link #slab}, which is {@link MemorySegment#NULL} otherwise. */
    final boolean nativeLanes;
    final MemorySegment slab;
    /** Per leaf: byte offsets of the lanes in the slab, -1 on a skipped leaf. */
    final long[] joinBase;
    final long[] fromBase;
    final long[] toBase;
    final long[] marginBase;
    final long @Nullable [] distinctBase;
    final long @Nullable [] groupBase;

    private SideLanes(final long[] @Nullable [] masks, final long[][] join, final long[][] from, final long[][] to,
        final long[][] margin, final long @Nullable [][] distinct, final long @Nullable [][] group,
        final boolean nativeLanes, final MemorySegment slab, final long[] joinBase, final long[] fromBase,
        final long[] toBase, final long[] marginBase, final long @Nullable [] distinctBase,
        final long @Nullable [] groupBase) {
      this.masks = masks;
      this.join = join;
      this.from = from;
      this.to = to;
      this.margin = margin;
      this.distinct = distinct;
      this.group = group;
      this.nativeLanes = nativeLanes;
      this.slab = slab;
      this.joinBase = joinBase;
      this.fromBase = fromBase;
      this.toBase = toBase;
      this.marginBase = marginBase;
      this.distinctBase = distinctBase;
      this.groupBase = groupBase;
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
        final long[] joinBase = new long[leaves];
        final long[] fromBase = new long[leaves];
        final long[] toBase = new long[leaves];
        final long[] marginBase = new long[leaves];
        final long[] distinctBase = distinctSlot < 0
            ? null
            : new long[leaves];
        final long[] groupBase = groupSlot < 0
            ? null
            : new long[leaves];
        Arrays.fill(joinBase, -1L);
        Arrays.fill(fromBase, -1L);
        Arrays.fill(toBase, -1L);
        Arrays.fill(marginBase, -1L);
        for (int leaf = 0; leaf < leaves; leaf++) {
          if (masks[leaf] == null) {
            continue;
          }
          joinBase[leaf] = laneOffset(columns, 0, leaf);
          fromBase[leaf] = laneOffset(columns, fromSlot, leaf);
          toBase[leaf] = laneOffset(columns, toSlot, leaf);
          marginBase[leaf] = laneOffset(columns, marginSlot, leaf);
          if (distinctBase != null) {
            distinctBase[leaf] = laneOffset(columns, distinctSlot, leaf);
          }
          if (groupBase != null) {
            groupBase[leaf] = laneOffset(columns, groupSlot, leaf);
          }
        }
        return new SideLanes(masks, NO_LANES, NO_LANES, NO_LANES, NO_LANES, null, null, true, columns.slab(), joinBase,
            fromBase, toBase, marginBase, distinctBase, groupBase);
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
      return new SideLanes(masks, join, from, to, margin, distinct, group, false, MemorySegment.NULL, NO_OFFSETS,
          NO_OFFSETS, NO_OFFSETS, NO_OFFSETS, null, null);
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
      final boolean nativeLanes = lanes.nativeLanes;
      final MemorySegment source = lanes.slab;
      int total = 0;
      for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        final long[] join = nativeLanes
            ? LongArrays.EMPTY_ARRAY
            : lanes.join[leaf];
        final long joinBase = nativeLanes
            ? lanes.joinBase[leaf]
            : 0L;
        for (int w = 0; w < mask.length; w++) {
          long word = mask[w];
          while (word != 0L) {
            final int row = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final long key = nativeLanes
                ? SegmentAccess.getLong(source, joinBase + 8L * row)
                : join[row];
            final long slot = key - keyMinimum;
            if (slot >= 0 && slot < slots) {
              bucketStart[(int) slot + 1]++;
              total++;
            }
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
      for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
        final long[] mask = lanes.masks[leaf];
        if (mask == null) {
          continue;
        }
        fillLeaf(slab, rowsBase, lanes, leaf, mask, fill, keyMinimum, slots, groupMinimum);
      }
      return new BucketedSide(slab, rowsBase, total, slots);
    }

    private static void fillLeaf(final MemorySegment slab, final long rowsBase, final SideLanes lanes, final int leaf,
        final long[] mask, final int[] fill, final long keyMinimum, final int slots, final long groupMinimum) {
      final boolean nativeLanes = lanes.nativeLanes;
      final MemorySegment source = lanes.slab;
      final long[] join = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.join[leaf];
      final long[] fromLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.from[leaf];
      final long[] toLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.to[leaf];
      final long[] marginLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.margin[leaf];
      final long[] distinctLane = nativeLanes || lanes.distinct == null
          ? LongArrays.EMPTY_ARRAY
          : lanes.distinct[leaf];
      final long[] groupLane = nativeLanes || lanes.group == null
          ? LongArrays.EMPTY_ARRAY
          : lanes.group[leaf];
      final long joinBase = nativeLanes
          ? lanes.joinBase[leaf]
          : 0L;
      final long fromBase = nativeLanes
          ? lanes.fromBase[leaf]
          : 0L;
      final long toBase = nativeLanes
          ? lanes.toBase[leaf]
          : 0L;
      final long marginBase = nativeLanes
          ? lanes.marginBase[leaf]
          : 0L;
      final long distinctBase = lanes.distinctBase != null
          ? lanes.distinctBase[leaf]
          : -1L;
      final long groupBase = lanes.groupBase != null
          ? lanes.groupBase[leaf]
          : -1L;
      final boolean hasDistinct = nativeLanes
          ? distinctBase >= 0
          : lanes.distinct != null;
      final boolean hasGroup = nativeLanes
          ? groupBase >= 0
          : lanes.group != null;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long at8 = 8L * row;
          final long key = nativeLanes
              ? SegmentAccess.getLong(source, joinBase + at8)
              : join[row];
          final long slot = key - keyMinimum;
          if (slot < 0 || slot >= slots) {
            continue;
          }
          final long at = rowsBase + fill[(int) slot]++ * STRIDE_BYTES;
          SegmentAccess.putLong(slab, at + FROM, nativeLanes
              ? SegmentAccess.getLong(source, fromBase + at8)
              : fromLane[row]);
          SegmentAccess.putLong(slab, at + TO, nativeLanes
              ? SegmentAccess.getLong(source, toBase + at8)
              : toLane[row]);
          SegmentAccess.putLong(slab, at + MARGIN, nativeLanes
              ? SegmentAccess.getLong(source, marginBase + at8)
              : marginLane[row]);
          SegmentAccess.putLong(slab, at + DISTINCT, !hasDistinct
              ? 0L
              : nativeLanes
                  ? SegmentAccess.getLong(source, distinctBase + at8)
                  : distinctLane[row]);
          SegmentAccess.putLong(slab, at + META, !hasGroup
              ? 0L
              : (nativeLanes
                  ? SegmentAccess.getLong(source, groupBase + at8)
                  : groupLane[row]) - groupMinimum);
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
   * one set per group. {@link #pair} folds one probe row against the build records of its key, read
   * from the build side's slab through the slab handle; its body is the heap kernel's.
   */
  private static final class ExtremaFold {
    final long[] groupMinimum;
    final long[] groupMaximum;
    final long[] distinctCounts;
    final long @Nullable [] bits;
    final LongOpenHashSet @Nullable [] sets;
    final MemorySegment buildSlab;
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
     * Fold one probe row against the build records in {@code [buildBegin, buildEnd)}, byte offsets of
     * the build slab: a counted loop over linear offsets, so the slab handle's range check is one per
     * loop where the compiler hoists it.
     */
    void pair(final long from, final long to, final long margin, final long probeValue, final long buildBegin,
        final long buildEnd) {
      final MemorySegment slab = this.buildSlab;
      for (long b = buildEnd - BucketedSide.STRIDE_BYTES; b >= buildBegin; b -= BucketedSide.STRIDE_BYTES) {
        if (from >= (long) SLAB_LONG.get(slab, b + BucketedSide.TO)
            || (long) SLAB_LONG.get(slab, b + BucketedSide.FROM) >= to) {
          continue;
        }
        final int group = (int) (long) SLAB_LONG.get(slab, b + BucketedSide.META);
        final long buildMargin = (long) SLAB_LONG.get(slab, b + BucketedSide.MARGIN);
        final long value = buildMinusProbe
            ? Math.subtractExact(buildMargin, margin)
            : Math.subtractExact(margin, buildMargin);
        if (value < groupMinimum[group]) {
          groupMinimum[group] = value;
        }
        if (value > groupMaximum[group]) {
          groupMaximum[group] = value;
        }
        final long distinctValue = distinctBuild
            ? (long) SLAB_LONG.get(slab, b + BucketedSide.DISTINCT)
            : probeValue;
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

  /**
   * Merge two bucketed sides key by key: buckets are contiguous and ascending in both slabs, so every
   * key's records are one range per side and the probe records stream once, each folded against its
   * key's build range. The loop is the heap kernel's over slab offsets.
   */
  private static void foldMerge(final ExtremaFold fold, final BucketedSide build, final BucketedSide probe,
      final int slots) {
    final MemorySegment buildSlab = build.slab;
    final MemorySegment probeSlab = probe.slab;
    final long buildRows = build.rowsBase;
    final long probeRows = probe.rowsBase;
    final long stride = BucketedSide.STRIDE_BYTES;
    int buildStart = SegmentAccess.getInt(buildSlab, 0L);
    int probeStart = SegmentAccess.getInt(probeSlab, 0L);
    for (int slot = 0; slot < slots; slot++) {
      final int buildNext = SegmentAccess.getInt(buildSlab, 4L * (slot + 1));
      final int probeNext = SegmentAccess.getInt(probeSlab, 4L * (slot + 1));
      if (buildStart != buildNext && probeStart != probeNext) {
        final long buildBegin = buildRows + buildStart * stride;
        final long buildEnd = buildRows + buildNext * stride;
        final long probeEnd = probeRows + probeNext * stride;
        for (long p = probeRows + probeStart * stride; p < probeEnd; p += stride) {
          fold.pair((long) SLAB_LONG.get(probeSlab, p + BucketedSide.FROM),
              (long) SLAB_LONG.get(probeSlab, p + BucketedSide.TO),
              (long) SLAB_LONG.get(probeSlab, p + BucketedSide.MARGIN),
              (long) SLAB_LONG.get(probeSlab, p + BucketedSide.DISTINCT), buildBegin, buildEnd);
        }
      }
      buildStart = buildNext;
      probeStart = probeNext;
    }
  }

  /**
   * Stream the probe lanes in row order against the bucketed build side: no probe record is written,
   * so a process-cold execution pays only the lane reads, with the same fold state as
   * {@link #foldMerge}. The lanes' representation is a loop-invariant choice.
   */
  private static int foldChained(final ExtremaFold fold, final BucketedSide build, final SideLanes lanes,
      final long keyMinimum, final int slots) {
    final MemorySegment buildSlab = build.slab;
    final long buildRows = build.rowsBase;
    final long stride = BucketedSide.STRIDE_BYTES;
    final boolean nativeLanes = lanes.nativeLanes;
    final MemorySegment source = lanes.slab;
    final boolean hasDistinct = nativeLanes
        ? lanes.distinctBase != null
        : lanes.distinct != null;
    int admitted = 0;
    for (int leaf = 0; leaf < lanes.masks.length; leaf++) {
      final long[] mask = lanes.masks[leaf];
      if (mask == null) {
        continue;
      }
      final long[] join = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.join[leaf];
      final long[] fromLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.from[leaf];
      final long[] toLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.to[leaf];
      final long[] marginLane = nativeLanes
          ? LongArrays.EMPTY_ARRAY
          : lanes.margin[leaf];
      final long[] distinctLane = nativeLanes || lanes.distinct == null
          ? LongArrays.EMPTY_ARRAY
          : lanes.distinct[leaf];
      final long joinBase = nativeLanes
          ? lanes.joinBase[leaf]
          : 0L;
      final long fromBase = nativeLanes
          ? lanes.fromBase[leaf]
          : 0L;
      final long toBase = nativeLanes
          ? lanes.toBase[leaf]
          : 0L;
      final long marginBase = nativeLanes
          ? lanes.marginBase[leaf]
          : 0L;
      final long distinctBase = lanes.distinctBase != null
          ? lanes.distinctBase[leaf]
          : -1L;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        while (word != 0L) {
          final int row = (w << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long at8 = 8L * row;
          final long key = nativeLanes
              ? (long) SLAB_LONG.get(source, joinBase + at8)
              : join[row];
          final long slot = key - keyMinimum;
          if (slot < 0 || slot >= slots) {
            continue;
          }
          admitted++;
          final int buildBeginRow = SegmentAccess.getInt(buildSlab, 4L * (int) slot);
          final int buildEndRow = SegmentAccess.getInt(buildSlab, 4L * ((int) slot + 1));
          if (buildBeginRow == buildEndRow) {
            continue;
          }
          fold.pair(nativeLanes
              ? (long) SLAB_LONG.get(source, fromBase + at8)
              : fromLane[row],
              nativeLanes
                  ? (long) SLAB_LONG.get(source, toBase + at8)
                  : toLane[row],
              nativeLanes
                  ? (long) SLAB_LONG.get(source, marginBase + at8)
                  : marginLane[row],
              !hasDistinct
                  ? 0L
                  : nativeLanes
                      ? (long) SLAB_LONG.get(source, distinctBase + at8)
                      : distinctLane[row],
              buildRows + buildBeginRow * stride, buildRows + buildEndRow * stride);
        }
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


  /** The row read of a native dense side: one slab read per lane at offsets hoisted per leaf. */
  private static void readNumericRow(final MemorySegment slab, final long[] laneOffsets, final boolean[] timestamps,
      final int row, final long[] values) {
    final long at = 8L * row;
    for (int field = 0; field < laneOffsets.length; field++) {
      final long value = (long) SLAB_LONG.get(slab, laneOffsets[field] + at);
      values[field] = timestamps[field]
          ? Math.multiplyExact(value, 1000L)
          : value;
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
    final boolean nativeLanes = denseNumeric && buildColumns.isNative();
    final MemorySegment slab = nativeLanes
        ? buildColumns.slab()
        : MemorySegment.NULL;
    final long[][] lanes = new long[buildFieldCount][];
    final long[] laneOffsets = new long[buildFieldCount];
    final long[] values = new long[buildFieldCount];
    final boolean[] timestamps = new boolean[buildFieldCount];
    if (denseNumeric) {
      for (int field = 0; field < buildFieldCount; field++) {
        timestamps[field] = buildColumns.isTimestamp(field);
      }
    }
    // BUILD: every admitted row with a present join key, chained per key value. Dense lanes are read
    // directly, from heap arrays or from the slab through offsets hoisted per leaf; the representation
    // is a loop-invariant choice.
    for (int leaf = 0; leaf < buildColumns.leafCount(); leaf++) {
      final long[] mask = buildColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (nativeLanes) {
        for (int field = 0; field < buildFieldCount; field++) {
          laneOffsets[field] = laneOffset(buildColumns, field, leaf);
        }
      } else if (denseNumeric) {
        for (int field = 0; field < buildFieldCount; field++) {
          lanes[field] = buildColumns.numericLane(field, leaf);
        }
      }
      final long keyBase = nativeLanes
          ? buildColumns.keyBase(leaf)
          : 0L;
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
          final long joinKey = nativeLanes
              ? (long) SLAB_LONG.get(slab, laneOffsets[0] + 8L * row)
              : denseNumeric
                  ? lanes[0][row]
                  : buildColumns.longValue(0, leaf, row);
          final long recordKey = nativeLanes
              ? (long) SLAB_LONG.get(slab, keyBase + 8L * row)
              : buildColumns.recordKey(leaf, row);
          final int at = buildRows.addKeyed(joinKey, recordKey);
          if (nativeLanes) {
            readNumericRow(slab, laneOffsets, timestamps, row, values);
            System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
          } else if (denseNumeric) {
            readNumericRow(lanes, timestamps, row, values);
            System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
          } else {
            readBuildRow(buildColumns, leaf, row, buildFieldCount, buildRows, at);
          }
        }
      }
    }
    return buildRows.nextRow;
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

  /** {@link #emitRow} for all-long, all-present ordering keys: no item, tuple or array per row. */
  private void emitRowPrimitive(final int keyCount, final int buildSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] stringKeys,
      final boolean[] timestampKeys, final MaskedColumns buildColumns, final MaskedColumns probeColumns, final int leaf,
      final int row, final RowOrder rowOrder) {
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
    final long probeKey = probeColumns.recordKey(leaf, row);
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
