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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionComputedColumn;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionTemporalCodec;
import io.sirix.query.ColumnarRecordSequence;
import io.sirix.query.scan.MaskedColumns;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

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
    final long[][] probeLanes = new long[probeFieldCount][];
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
    final int emittedRows =
        probeRows(buildColumns, buildSide, probeColumns, probeSide, stringKeys, timestampKeys, buildRows, nextRow,
            denseHeads, firstRow, distinctCapacity, distinctDomains, probeValues, probePresent, denseProbe, probeLanes,
            probeTimestamps, groups, singleGroups, rowOrdering, rowColumns, denseBuild && denseProbe);
    SirixVectorizedExecutor.noteJoinGroupServed();
    return rowOrdering != null
        ? orderedRows(rowOrdering, emittedRows, rowColumns)
        : emit(groups, buildColumns, stringKeys, timestampKeys);
  }

  private int probeRows(final MaskedColumns buildColumns, final int buildSide, final MaskedColumns probeColumns,
      final int probeSide, final boolean[] stringKeys, final boolean[] timestampKeys, final BuildRows buildRows,
      final int[] nextRow, final int @Nullable [] denseHeads, final Long2IntOpenHashMap firstRow,
      final int distinctCapacity, final DistinctDomain[] distinctDomains, final long[] probeValues,
      final boolean[] probePresent, final boolean denseProbe, final long[][] probeLanes,
      final boolean[] probeTimestamps, final Map<GroupKey, GroupState> groups,
      final @Nullable Long2ObjectOpenHashMap<GroupState> singleGroups, final @Nullable Ordering rowOrdering,
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
    int emittedRows = 0;
    for (int leaf = 0; leaf < probeColumns.leafCount(); leaf++) {
      final long[] mask = probeColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (denseProbe) {
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
          final long joinKey = denseProbe
              ? probeLanes[0][row]
              : probeColumns.longValue(0, leaf, row);
          int at = firstMatch(joinKey, buildRows, denseHeads, firstRow);
          if (at < 0) {
            continue;
          }
          if (denseProbe) {
            readNumericRow(probeLanes, probeTimestamps, row, probeValues);
          } else {
            readProbeRow(probeColumns, leaf, row, probeFieldCount, probeValues, probePresent);
          }
          for (; at >= 0; at = nextRow[at]) {
            if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack, allPresent)) {
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
   * The lanes one side of the dense extrema kernel reads: the row masks (copied, so a packing task
   * may outlive the query's view) and the per-leaf numeric lanes of the join key, the two timestamps,
   * the margin operand and, when the side carries them, the distinct operand and the group key. Lanes
   * are immutable decoded arrays shared with the column store.
   */
  @SuppressWarnings("ArrayRecordComponent") // Private lane carrier; array value equality is never used.
  private record SideLanes(long[] @Nullable [] masks, long[][] join, long[][] from, long[][] to, long[][] margin,
      long @Nullable [][] distinct, long @Nullable [][] group) {

    static SideLanes capture(final MaskedColumns columns, final int fromSlot, final int toSlot, final int marginSlot,
        final int distinctSlot, final int groupSlot) {
      final int leaves = columns.leafCount();
      final long[][] masks = new long[leaves][];
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
        final long[] mask = columns.rowMask(leaf);
        if (mask == null) {
          continue;
        }
        masks[leaf] = mask.clone();
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
      return new SideLanes(masks, join, from, to, margin, distinct, group);
    }
  }

  /**
   * One side of the dense extrema kernel, bucketed by join key: a counting sort over the build side's
   * dense key domain places every key's rows contiguously, so the join is a merge over keys that
   * reads both sides sequentially. Each row is one {@value #STRIDE}-long record (from, to, margin,
   * distinct operand or 0, group index or 0), so bucketing touches one line per row. Timestamps keep
   * their persisted unit (epoch seconds on both sides), which orders exactly like the millisecond
   * residuals of the generic kernel. Rows whose key lies outside the domain cannot join and are
   * dropped while bucketing. Immutable once built, so a revision's side is reusable.
   *
   * <p>
   * Packing a large side costs a first touch of its whole record array plus one scattered write per
   * row (about 17 ms for 325k rows as a native image), which is why a process-cold query never packs
   * its probe side on the critical path: it streams the lanes against the bucketed build side instead
   * ({@link #foldChained}) while the packing runs in the background for the next execution.
   */
  private static final class BucketedSide {
    static final int STRIDE = 5;
    static final int FROM = 0;
    static final int TO = 1;
    static final int MARGIN = 2;
    static final int DISTINCT = 3;
    /** The group index on the side carrying the group key, zero on the other. */
    static final int META = 4;

    final int[] bucketStart;
    final long[] rows;
    final int count;

    BucketedSide(final SideLanes lanes, final long keyMinimum, final int slots, final long groupMinimum) {
      final int leaves = lanes.masks().length;
      bucketStart = new int[slots + 1];
      int total = 0;
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = lanes.masks()[leaf];
        if (mask == null) {
          continue;
        }
        final long[] join = lanes.join()[leaf];
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
      }
      for (int slot = 0; slot < slots; slot++) {
        bucketStart[slot + 1] += bucketStart[slot];
      }
      count = total;
      rows = new long[total * STRIDE];
      final int[] fill = Arrays.copyOf(bucketStart, slots);
      for (int leaf = 0; leaf < leaves; leaf++) {
        final long[] mask = lanes.masks()[leaf];
        if (mask == null) {
          continue;
        }
        final long[] join = lanes.join()[leaf];
        final long[] fromLane = lanes.from()[leaf];
        final long[] toLane = lanes.to()[leaf];
        final long[] marginLane = lanes.margin()[leaf];
        final long[] distinctLane = lanes.distinct() == null
            ? null
            : lanes.distinct()[leaf];
        final long[] groupLane = lanes.group() == null
            ? null
            : lanes.group()[leaf];
        for (int w = 0; w < mask.length; w++) {
          long word = mask[w];
          while (word != 0L) {
            final int row = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            final long slot = join[row] - keyMinimum;
            if (slot < 0 || slot >= slots) {
              continue;
            }
            final int at = fill[(int) slot]++ * STRIDE;
            rows[at + FROM] = fromLane[row];
            rows[at + TO] = toLane[row];
            rows[at + MARGIN] = marginLane[row];
            rows[at + DISTINCT] = distinctLane == null
                ? 0L
                : distinctLane[row];
            rows[at + META] = groupLane == null
                ? 0L
                : groupLane[row] - groupMinimum;
          }
        }
      }
    }

    long weight() {
      return 64L + 4L * bucketStart.length + 8L * rows.length;
    }
  }

  /**
   * Bounded reuse of one immutable bucketed side per snapshot, matched on the routed keys and the
   * operand slots, so a repeated query at an immutable revision skips the bucketing pass. A side
   * being packed in the background is joinable, so a second execution that arrives before the packing
   * finished waits for it instead of packing again.
   */
  private static final class BucketedSideCache {
    private static final long MAX_BYTES =
        Math.max(1L << 20, Math.min(64L << 20, Runtime.getRuntime().maxMemory() / 64));
    private static final Cache<Object, Entry> CACHE = Caffeine.newBuilder()
                                                              .weakKeys()
                                                              .maximumWeight(MAX_BYTES)
                                                              .weigher((Object snapshot, Entry entry) -> entry.weight)
                                                              .build();
    private static final ConcurrentHashMap<InFlightKey, CompletableFuture<BucketedSide>> IN_FLIGHT =
        new ConcurrentHashMap<>();

    @SuppressWarnings("ArrayRecordComponent") // Matched by identity and content below; record equality is unused.
    private record Entry(long[] signature, BucketedSide side, int weight) {
    }

    /** Store and row identity by reference, signature by content. */
    @SuppressWarnings("ArrayRecordComponent") // Signature equality and hashing are defined by content below.
    private record InFlightKey(ProjectionColumnStore store, Object rows, long[] signature) {
      @Override
      @SuppressWarnings("ReferenceEquality") // The row token identifies one loaded snapshot, not equal row values.
      public boolean equals(final Object other) {
        return other instanceof InFlightKey key && key.store == store && key.rows == rows
            && Arrays.equals(key.signature, signature);
      }

      @Override
      public int hashCode() {
        return 31 * (31 * System.identityHashCode(store) + System.identityHashCode(rows)) + Arrays.hashCode(signature);
      }
    }

    private BucketedSideCache() {}

    static @Nullable BucketedSide get(final MaskedColumns columns, final long[] signature) {
      if (!columns.isImmutableRevision()) {
        return null;
      }
      final Entry entry = CACHE.getIfPresent(columns.rowIdentity());
      if (entry != null && Arrays.equals(signature, entry.signature)) {
        return entry.side;
      }
      final CompletableFuture<BucketedSide> packing =
          IN_FLIGHT.get(new InFlightKey(columns.store(), columns.rowIdentity(), signature));
      if (packing == null) {
        return null;
      }
      try {
        return packing.join();
      } catch (final CompletionException | CancellationException e) {
        return null;
      }
    }

    static void put(final MaskedColumns columns, final long[] signature, final BucketedSide side) {
      if (!columns.isImmutableRevision()) {
        return;
      }
      final long bytes = side.weight() + 8L * signature.length;
      if (bytes <= MAX_BYTES) {
        CACHE.put(columns.rowIdentity(), new Entry(signature, side, (int) bytes));
      }
    }

    /**
     * Pack {@code lanes} in the background and publish the side; a no-op unless the revision is
     * immutable or when the same side is already being packed.
     */
    @SuppressWarnings("FutureReturnValueIgnored") // Failures complete the separately tracked packing future.
    static void packAsync(final MaskedColumns columns, final long[] signature, final SideLanes lanes,
        final long keyMinimum, final int slots, final long groupMinimum) {
      if (!columns.isImmutableRevision()) {
        return;
      }
      final InFlightKey key = new InFlightKey(columns.store(), columns.rowIdentity(), signature);
      final CompletableFuture<BucketedSide> packing = new CompletableFuture<>();
      if (IN_FLIGHT.putIfAbsent(key, packing) != null) {
        return;
      }
      BUCKETED_SIDES_BUILT.increment();
      CompletableFuture.runAsync(() -> {
        try {
          final BucketedSide side = new BucketedSide(lanes, keyMinimum, slots, groupMinimum);
          put(columns, signature, side);
          packing.complete(side);
        } catch (final RuntimeException | Error e) {
          packing.completeExceptionally(e);
        } finally {
          IN_FLIGHT.remove(key, packing);
        }
      });
    }
  }

  private static long[] sideSignature(final long keyMinimum, final int slots, final int fromSlot, final int toSlot,
      final int marginSlot, final int distinctSlot, final int groupSlot, final long groupMinimum) {
    return new long[] {keyMinimum, slots, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot, groupMinimum};
  }

  /**
   * The cached side, or one packed now (the build side is small; packing it is cheaper than a chain).
   */
  private static BucketedSide bucketedSide(final MaskedColumns columns, final long keyMinimum, final int slots,
      final int fromSlot, final int toSlot, final int marginSlot, final int distinctSlot, final int groupSlot,
      final long groupMinimum) {
    final long[] signature =
        sideSignature(keyMinimum, slots, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot, groupMinimum);
    final BucketedSide cached = BucketedSideCache.get(columns, signature);
    if (cached != null) {
      return cached;
    }
    final BucketedSide side =
        new BucketedSide(SideLanes.capture(columns, fromSlot, toSlot, marginSlot, distinctSlot, groupSlot), keyMinimum,
            slots, groupMinimum);
    BUCKETED_SIDES_BUILT.increment();
    BucketedSideCache.put(columns, signature, side);
    return side;
  }

  /**
   * The flat fold state of the dense extrema kernel: per-group minimum/maximum of the residual and
   * distinct values per group as one value-major bitmap or, when the distinct domain is unbounded,
   * one set per group. {@link #pair} folds one probe row against the build rows of its key.
   */
  private static final class ExtremaFold {
    final long[] groupMinimum;
    final long[] groupMaximum;
    final long[] distinctCounts;
    final long @Nullable [] bits;
    final LongOpenHashSet @Nullable [] sets;
    final long[] buildRows;
    final int groups;
    final long domainMinimum;
    final boolean distinctBuild;
    final boolean buildMinusProbe;

    ExtremaFold(final int groups, final @Nullable DistinctDomain domain, final long[] buildRows,
        final boolean distinctBuild, final boolean buildMinusProbe) {
      this.groups = groups;
      this.buildRows = buildRows;
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

    void pair(final long from, final long to, final long margin, final long probeValue, final int buildBegin,
        final int buildEnd) {
      final long[] buildRows = this.buildRows;
      for (int b = buildEnd - BucketedSide.STRIDE; b >= buildBegin; b -= BucketedSide.STRIDE) {
        if (from >= buildRows[b + BucketedSide.TO] || buildRows[b + BucketedSide.FROM] >= to) {
          continue;
        }
        final int group = (int) buildRows[b + BucketedSide.META];
        final long buildMargin = buildRows[b + BucketedSide.MARGIN];
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
            ? buildRows[b + BucketedSide.DISTINCT]
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

  /** Merge two bucketed sides key by key. */
  private static void foldMerge(final ExtremaFold fold, final BucketedSide build, final BucketedSide probe,
      final int slots) {
    final int[] buildStart = build.bucketStart;
    final int[] probeStart = probe.bucketStart;
    final long[] probeRows = probe.rows;
    final int stride = BucketedSide.STRIDE;
    for (int slot = 0; slot < slots; slot++) {
      final int buildBegin = buildStart[slot] * stride;
      final int buildEnd = buildStart[slot + 1] * stride;
      final int probeBegin = probeStart[slot] * stride;
      final int probeEnd = probeStart[slot + 1] * stride;
      if (buildBegin == buildEnd || probeBegin == probeEnd) {
        continue;
      }
      for (int p = probeBegin; p < probeEnd; p += stride) {
        fold.pair(probeRows[p + BucketedSide.FROM], probeRows[p + BucketedSide.TO], probeRows[p + BucketedSide.MARGIN],
            probeRows[p + BucketedSide.DISTINCT], buildBegin, buildEnd);
      }
    }
  }

  /**
   * Stream the probe lanes in row order against the bucketed build side: no probe record is written,
   * so a process-cold execution pays only the lane reads, with the same fold state as
   * {@link #foldMerge}.
   */
  private static int foldChained(final ExtremaFold fold, final BucketedSide build, final SideLanes lanes,
      final long keyMinimum, final int slots) {
    final int[] buildStart = build.bucketStart;
    final int stride = BucketedSide.STRIDE;
    final int leaves = lanes.masks().length;
    int admitted = 0;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final long[] mask = lanes.masks()[leaf];
      if (mask == null) {
        continue;
      }
      final long[] join = lanes.join()[leaf];
      final long[] fromLane = lanes.from()[leaf];
      final long[] toLane = lanes.to()[leaf];
      final long[] marginLane = lanes.margin()[leaf];
      final long[] distinctLane = lanes.distinct() == null
          ? null
          : lanes.distinct()[leaf];
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
          final int buildBegin = buildStart[(int) slot] * stride;
          final int buildEnd = buildStart[(int) slot + 1] * stride;
          if (buildBegin == buildEnd) {
            continue;
          }
          fold.pair(fromLane[row], toLane[row], marginLane[row], distinctLane == null
              ? 0L
              : distinctLane[row], buildBegin, buildEnd);
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
    final BucketedSide build = bucketedSide(buildColumns, plan.joinMinimum(), slots, plan.buildFromSlot(),
        plan.buildToSlot(), plan.buildMarginSlot(), plan.distinctBuild()
            ? plan.distinctSlot()
            : -1,
        plan.groupSlot(), plan.groupMinimum());
    final long built = DIAG
        ? System.nanoTime()
        : 0L;
    final int probeDistinctSlot = plan.distinctBuild()
        ? -1
        : plan.distinctSlot();
    final long[] probeSignature = sideSignature(plan.joinMinimum(), slots, plan.probeFromSlot(), plan.probeToSlot(),
        plan.probeMarginSlot(), probeDistinctSlot, -1, 0L);
    final BucketedSide probe = BucketedSideCache.get(probeColumns, probeSignature);
    final long packed = DIAG
        ? System.nanoTime()
        : 0L;
    final int groups = plan.groups();
    final DistinctDomain[] domains = distinctDomains(buildColumns, buildSide, probeColumns);
    final DistinctDomain domain = domains.length == 0
        ? null
        : domains[plan.distinct()];
    final ExtremaFold fold = new ExtremaFold(groups, domain, build.rows, plan.distinctBuild(), plan.buildMinusProbe());
    final int probeRows;
    if (probe != null) {
      foldMerge(fold, build, probe, slots);
      probeRows = probe.count;
    } else {
      final SideLanes lanes = SideLanes.capture(probeColumns, plan.probeFromSlot(), plan.probeToSlot(),
          plan.probeMarginSlot(), probeDistinctSlot, -1);
      BucketedSideCache.packAsync(probeColumns, probeSignature, lanes, plan.joinMinimum(), slots, 0L);
      probeRows = foldChained(fold, build, lanes, plan.joinMinimum(), slots);
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
          (built - started) / 1e6, (packed - built) / 1e6, (merged - packed) / 1e6, (System.nanoTime() - merged) / 1e6,
          build.count, probeRows, slots, seen);
    }
    SirixVectorizedExecutor.noteJoinGroupServed();
    return emit(result, buildColumns, stringKeys, timestampKeys);
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
    int[] nextRow = new int[Math.max(16, (int) Math.min(Integer.MAX_VALUE - 8, buildColumns.rows()))];
    final long[][] lanes = new long[buildFieldCount][];
    final long[] values = new long[buildFieldCount];
    final boolean[] timestamps = new boolean[buildFieldCount];
    if (denseNumeric) {
      for (int field = 0; field < buildFieldCount; field++) {
        timestamps[field] = buildColumns.isTimestamp(field);
      }
    }
    // BUILD: every admitted row with a present join key, chained per key value.
    for (int leaf = 0; leaf < buildColumns.leafCount(); leaf++) {
      final long[] mask = buildColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      if (denseNumeric) {
        for (int field = 0; field < buildFieldCount; field++) {
          lanes[field] = buildColumns.numericLane(field, leaf);
        }
      }
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
              ? lanes[0][row]
              : buildColumns.longValue(0, leaf, row);
          buildRows.minimumJoinKey = Math.min(buildRows.minimumJoinKey, joinKey);
          buildRows.maximumJoinKey = Math.max(buildRows.maximumJoinKey, joinKey);
          final int at = buildRows.add(buildColumns.recordKey(leaf, row));
          if (at == nextRow.length) {
            nextRow = Arrays.copyOf(nextRow, nextRow.length << 1);
          }
          if (denseNumeric) {
            readNumericRow(lanes, timestamps, row, values);
            System.arraycopy(values, 0, buildRows.values, at * buildFieldCount, buildFieldCount);
          } else {
            readBuildRow(buildColumns, leaf, row, buildFieldCount, buildRows, at);
          }
        }
      }
    }
    return nextRow;
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
