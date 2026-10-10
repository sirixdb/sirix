package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Int64;
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
import io.sirix.index.projection.ProjectionComputedColumn;
import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionTemporalCodec;
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
    this.countPairs = pairs;
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
    this.aggSlots = new int[aggFields.length];
    this.aggPrograms = new int[aggFields.length];
    Arrays.fill(aggPrograms, -1);
    for (int a = 0; a < aggFields.length; a++) {
      if (aggFields[a] != null && aggFields[a].startsWith(COMPUTED_PREFIX)) {
        aggPrograms[a] = Integer.parseInt(aggFields[a].substring(COMPUTED_PREFIX.length()));
      }
      aggSlots[a] = aggFields[a] == null || aggFields[a].startsWith(COMPUTED_PREFIX)
          ? -1
          : slot(rosters.get(aggSides[a]), aggFields[a]);
    }
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
    final DenseExtrema extrema = denseBuild && denseHeads != null && !rowOutput
        ? denseExtrema(buildColumns, buildSide, probeColumns)
        : null;
    if (extrema != null) {
      @SuppressWarnings("NullAway") // A non-null extrema plan above proves denseHeads is non-null.
      final Sequence result =
          joinExtrema(buildColumns, buildRows, nextRow, denseHeads, probeColumns, extrema, stringKeys, timestampKeys);
      return result;
    }
    // PROBE: every admitted row, every matching build row, one pair folded into its group.
    final int keyCount = keyFields.length;
    final int aggCount = aggFuncs.length;
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
    final long[] keyComponents = new long[keyCount + 1]; // the last word packs the missing flags
    final long[] operands = new long[8];
    final long[] stack = new long[64];
    final long[] programValues = new long[progCode.length];
    final byte[] programState = new byte[progCode.length];
    final Map<GroupKey, GroupState> groups = rowOutput
        ? Map.of()
        : new LinkedHashMap<>();
    final Long2ObjectOpenHashMap<GroupState> singleGroups = !rowOutput && keyCount == 1
        ? new Long2ObjectOpenHashMap<>()
        : null;
    final Ordering rowOrdering;
    if (rowOutput) {
      final Ordering.OrderModifier[] rowModifiers = Arrays.copyOf(orderModifiers, orderModifiers.length + 2);
      rowModifiers[orderModifiers.length] = new Ordering.OrderModifier(true, true, null);
      rowModifiers[orderModifiers.length + 1] = new Ordering.OrderModifier(true, true, null);
      rowOrdering = new Ordering(new Expr[0], rowModifiers);
    } else {
      rowOrdering = null;
    }
    final boolean[] residualStack = new boolean[Math.max(1, residualCode.length)];
    int emittedRows = 0;
    final GroupKey probeKey = new GroupKey(keyComponents); // the scratch key: hashed per pair, cloned on insert
    final int accWidth = 1 + 4 * aggCount;
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
          int at = denseHeads == null
              ? firstRow.get(joinKey)
              : joinKey < buildRows.minimumJoinKey || joinKey > buildRows.maximumJoinKey
                  ? -1
                  : denseHeads[(int) (joinKey - buildRows.minimumJoinKey)];
          if (at < 0) {
            continue;
          }
          if (denseProbe) {
            readNumericRow(probeLanes, probeTimestamps, row, probeValues);
          } else {
            readProbeRow(probeColumns, leaf, row, probeFieldCount, probeValues, probePresent);
          }
          for (; at >= 0; at = nextRow[at]) {
            if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack,
                denseBuild && denseProbe)) {
              continue;
            }
            if (rowOrdering != null) {
              emitRow(keyCount, buildSide, probeSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                  stringKeys, timestampKeys, buildColumns, probeColumns, leaf, row, rowOrdering);
              emittedRows++;
              continue;
            }
            foldPair(keyCount, aggCount, accWidth, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                keyComponents, probeKey, groups, singleGroups, distinctCapacity, distinctDomains, operands, stack,
                programValues, programState, denseBuild && denseProbe);
          }
        }
      }
    }
    SirixVectorizedExecutor.noteJoinGroupServed();
    return rowOrdering != null
        ? ordered(rowOrdering, emittedRows)
        : emit(groups, buildColumns, stringKeys, timestampKeys);
  }

  /**
   * Bound operands for distinct count and min/max of one cross-side subtraction, with strict overlap.
   */
  private record DenseExtrema(int distinct, int minimum, int maximum, int groupSlot, long groupMinimum, int groups,
      int distinctSlot, boolean distinctBuild, int buildMarginSlot, int probeMarginSlot, boolean buildMinusProbe,
      int buildFromSlot, int buildToSlot, int probeFromSlot, int probeToSlot) {
  }

  private @Nullable DenseExtrema denseExtrema(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns) {
    if (keyFields.length != 1 || keySides[0] != buildSide || !buildColumns.isLong(keySlots[0]) || aggKinds.length != 3
        || progCode.length != 1 || !residualAnd || residualOps.length != 2) {
      return null;
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
          return null;
        }
      }
    }
    if (distinct < 0 || minimum < 0 || maximum < 0 || aggPrograms[distinct] >= 0 || aggSlots[distinct] < 0
        || aggPrograms[minimum] != 0 || aggPrograms[maximum] != 0) {
      return null;
    }
    final int[] code = progCode[0];
    if (code.length != 3 || code[2] != ProjectionIndexByteScan.COMPUTED_OP_SUB || code[0] < 0 || code[1] < 0
        || code[0] >= progSlots[0].length || code[1] >= progSlots[0].length
        || progOperandSides[0][code[0]] == progOperandSides[0][code[1]]) {
      return null;
    }
    int buildFrom = -1;
    int buildTo = -1;
    int probeFrom = -1;
    int probeTo = -1;
    for (int comparison = 0; comparison < 2; comparison++) {
      final int left = comparison * 2;
      final int right = left + 1;
      if (residualOps[comparison] != 2 || !residualTemporal[left] || !residualTemporal[right]
          || residualSides[left] == residualSides[right]) {
        return null;
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
      return null;
    }
    final boolean buildMinusProbe = progOperandSides[0][code[0]] == buildSide;
    return new DenseExtrema(distinct, minimum, maximum, keySlots[0], buildColumns.minimum(keySlots[0]),
        (int) groupCount, aggSlots[distinct], aggSides[distinct] == buildSide, progSlots[0][code[buildMinusProbe
            ? 0
            : 1]],
        progSlots[0][code[buildMinusProbe
            ? 1
            : 0]],
        buildMinusProbe, buildFrom, buildTo, probeFrom, probeTo);
  }

  private Sequence joinExtrema(final MaskedColumns buildColumns, final BuildRows buildRows, final int[] nextRow,
      final int[] heads, final MaskedColumns probeColumns, final DenseExtrema plan, final boolean[] stringKeys,
      final boolean[] timestampKeys) {
    final Map<GroupKey, GroupState> groups = new LinkedHashMap<>();
    final GroupState[] states = new GroupState[plan.groups()];
    final DistinctDomain[] domains = distinctDomains(buildColumns, keySides[0], probeColumns);
    final int width = 1 + 4 * aggKinds.length;
    final int minBase = 1 + 4 * plan.minimum();
    final int maxBase = 1 + 4 * plan.maximum();
    final long[] build = buildRows.values;
    final int fields = buildRows.fieldCount;
    for (int leaf = 0; leaf < probeColumns.leafCount(); leaf++) {
      final long[] mask = probeColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      final long[] join = probeColumns.numericLane(0, leaf);
      final long[] from = probeColumns.numericLane(plan.probeFromSlot(), leaf);
      final long[] to = probeColumns.numericLane(plan.probeToSlot(), leaf);
      final long[] margin = probeColumns.numericLane(plan.probeMarginSlot(), leaf);
      final long[] distinct = plan.distinctBuild()
          ? null
          : probeColumns.numericLane(plan.distinctSlot(), leaf);
      final boolean distinctTimestamp = !plan.distinctBuild() && probeColumns.isTimestamp(plan.distinctSlot());
      for (int wordIndex = 0; wordIndex < mask.length; wordIndex++) {
        long word = mask[wordIndex];
        while (word != 0) {
          final int row = (wordIndex << 6) + Long.numberOfTrailingZeros(word);
          word &= word - 1;
          final long key = join[row];
          if (key < buildRows.minimumJoinKey || key > buildRows.maximumJoinKey) {
            continue;
          }
          int at = heads[(int) (key - buildRows.minimumJoinKey)];
          if (at < 0) {
            continue;
          }
          final long probeFrom = Math.multiplyExact(from[row], 1000L);
          final long probeTo = Math.multiplyExact(to[row], 1000L);
          final long probeMargin = margin[row];
          final long probeDistinct = distinct == null
              ? 0
              : distinctTimestamp
                  ? Math.multiplyExact(distinct[row], 1000L)
                  : distinct[row];
          for (; at >= 0; at = nextRow[at]) {
            final int base = at * fields;
            if (probeFrom >= build[base + plan.buildToSlot()] || build[base + plan.buildFromSlot()] >= probeTo) {
              continue;
            }
            final long groupKey = build[base + plan.groupSlot()];
            final int group = (int) (groupKey - plan.groupMinimum());
            GroupState state = states[group];
            if (state == null) {
              state = new GroupState(width, aggFuncs, 16, domains);
              state.acc[minBase + 2] = Long.MAX_VALUE;
              state.acc[maxBase + 3] = Long.MIN_VALUE;
              states[group] = state;
              groups.put(new GroupKey(new long[] {groupKey, 0}), state);
            }
            state.distinct[plan.distinct()].add(plan.distinctBuild()
                ? build[base + plan.distinctSlot()]
                : probeDistinct);
            final long buildMargin = build[base + plan.buildMarginSlot()];
            final long value = plan.buildMinusProbe()
                ? Math.subtractExact(buildMargin, probeMargin)
                : Math.subtractExact(probeMargin, buildMargin);
            state.acc[minBase] = 1;
            state.acc[maxBase] = 1;
            state.acc[minBase + 2] = Math.min(state.acc[minBase + 2], value);
            state.acc[maxBase + 3] = Math.max(state.acc[maxBase + 3], value);
          }
        }
      }
    }
    SirixVectorizedExecutor.noteJoinGroupServed();
    return emit(groups, buildColumns, stringKeys, timestampKeys);
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
        final boolean matched = leftPresent && rightPresent && switch (residualOps[instruction]) {
          case 0 -> a == b;
          case 1 -> a != b;
          case 2 -> a < b;
          case 3 -> a <= b;
          case 4 -> a > b;
          case 5 -> a >= b;
          default -> throw new IllegalStateException("Invalid residual comparison");
        };
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
  }

  private static final class DistinctValues {
    private final @Nullable LongOpenHashSet values;
    private final long @Nullable [] bits;
    private final long minimum;
    private long count;

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
        }
      }
    }
    return nextRow;
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
      final MaskedColumns probeColumns, final int leaf, final int row, final Ordering rowOrdering) {
    final Sequence[] values = new Sequence[keyCount];
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
        values[k] = keyValue(value, stringKeys[k], timestampKeys[k], buildColumns);
      }
    }
    final Sequence[] orderKeys = new Sequence[orderIndexes.length + 2];
    for (int k = 0; k < orderIndexes.length; k++) {
      orderKeys[k] = values[orderIndexes[k]];
    }
    orderKeys[orderIndexes.length + buildSide] = new Int64(buildRows.recordKeys[at]);
    orderKeys[orderIndexes.length + probeSide] = new Int64(probeColumns.recordKey(leaf, row));
    rowOrdering.add(orderKeys, new TupleImpl(new ArrayObject(outputNames, values)));
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
