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
import io.sirix.query.scan.MaskedColumns;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
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
 * the smaller side is hashed on its join values, the larger probes it, and every matched pair folds
 * into a group keyed on fields of either side. No record object is built on either side; the group
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

  private final SirixExecutorProvider executorProvider;
  private final Side[] sides;
  private final int[] keySides;
  private final String[] keyFields;
  private final String[] aggFuncs;
  private final int[] aggSides;
  private final String[] aggFields;
  private final int[] progSides;
  private final int[][] progCode;
  private final long[][] progConsts;
  private final int[] entryKinds;
  private final int[] orderIndexes;
  private final Ordering.OrderModifier[] orderModifiers;
  private final Expr genericFallback;
  private final boolean rowOutput;
  private final int[] residualSides;
  private final int[] residualSlots;
  private final boolean[] residualNe;
  private final int[] residualCode;
  private final QNm[] outputNames;
  /** Per side, the distinct fields read from it (join key first), and each key/agg's slot in them. */
  private final String[][] sideFields;
  private final int[] keySlots;
  private final int[] aggSlots;
  private final int[][] progSlots;

  public SirixJoinedGroupAggregateExpr(final SirixExecutorProvider executorProvider, final Side[] sides,
      final int[] keySides, final String[] keyFields, final String[] aggFuncs, final int[] aggSides,
      final String[] aggFields, final int[] progSides, final String[][] progFields, final int[][] progCode,
      final long[][] progConsts, final int[] entryKinds, final String[] entryNames, final int[] orderIndexes,
      final boolean[] orderAsc, final boolean[] orderEmptyLeast, final boolean rowOutput,
      final int @Nullable [] residualSides, final String @Nullable [] residualFields,
      final boolean @Nullable [] residualNe, final int @Nullable [] residualCode, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sides = sides;
    this.keySides = keySides;
    this.keyFields = keyFields;
    this.aggFuncs = aggFuncs;
    this.aggSides = aggSides;
    this.aggFields = aggFields;
    this.progSides = progSides;
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
    this.residualNe = residualNe == null
        ? new boolean[0]
        : residualNe;
    this.residualCode = residualCode == null
        ? new int[0]
        : residualCode;
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
    for (int p = 0; p < progFields.length; p++) {
      progSlots[p] = new int[progFields[p].length];
      for (int o = 0; o < progFields[p].length; o++) {
        progSlots[p][o] = slot(rosters.get(progSides[p]), progFields[p][o]);
      }
    }
    this.aggSlots = new int[aggFields.length];
    for (int a = 0; a < aggFields.length; a++) {
      aggSlots[a] = aggFields[a] == null || aggFields[a].startsWith(COMPUTED_PREFIX)
          ? -1
          : slot(rosters.get(aggSides[a]), aggFields[a]);
    }
    this.residualSlots = new int[this.residualSides.length];
    if (residualSlots.length != 0) {
      final String[] fields = Objects.requireNonNull(residualFields, "residual operands require field names");
      for (int i = 0; i < residualSlots.length; i++) {
        residualSlots[i] = slot(rosters.get(this.residualSides[i]), fields[i]);
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
    for (int k = 0; k < keyFields.length; k++) {
      final MaskedColumns c = columns[keySides[k]];
      if (!c.isLong(keySlots[k]) && !c.isString(keySlots[k])) {
        return decline("key " + keyFields[k] + " is neither long nor string");
      }
      stringKeys[k] = c.isString(keySlots[k]);
    }
    final String invalid = invalidOperands(columns);
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
      final Sequence served = join(columns[build], build, columns[probe], probe, stringKeys);
      return served;
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
    long[] values;
    boolean[] present;
    long[] recordKeys;
    int count;

    BuildRows(final int fieldCount, final long rows) {
      this.fieldCount = fieldCount;
      final int capacity = (int) Math.min(Integer.MAX_VALUE - 8, Math.max(16, rows));
      values = new long[capacity * fieldCount];
      present = new boolean[capacity * fieldCount];
      recordKeys = new long[capacity];
    }

    int add(final long recordKey) {
      if (count == recordKeys.length) {
        final int grown = recordKeys.length << 1;
        values = Arrays.copyOf(values, grown * fieldCount);
        present = Arrays.copyOf(present, grown * fieldCount);
        recordKeys = Arrays.copyOf(recordKeys, grown);
      }
      recordKeys[count] = recordKey;
      return count++;
    }
  }

  private @Nullable Sequence join(final MaskedColumns buildColumns, final int buildSide,
      final MaskedColumns probeColumns, final int probeSide, final boolean[] stringKeys) throws QueryException {
    final int buildFieldCount = sideFields[buildSide].length;
    final BuildRows buildRows = new BuildRows(buildFieldCount, buildColumns.rows());
    final Long2IntOpenHashMap firstRow =
        new Long2IntOpenHashMap((int) Math.min(Integer.MAX_VALUE - 8, buildColumns.rows()));
    firstRow.defaultReturnValue(-1);
    final int[] nextRow = build(buildColumns, buildFieldCount, buildRows, firstRow);
    // PROBE: every admitted row, every matching build row, one pair folded into its group.
    final int keyCount = keyFields.length;
    final int aggCount = aggFuncs.length;
    final int probeFieldCount = sideFields[probeSide].length;
    final long[] probeValues = new long[probeFieldCount];
    final boolean[] probePresent = new boolean[probeFieldCount];
    final long[] keyComponents = new long[keyCount + 1]; // the last word packs the missing flags
    final long[] operands = new long[8];
    final long[] stack = new long[64];
    final Map<GroupKey, long[]> groups = rowOutput
        ? Map.of()
        : new LinkedHashMap<>();
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
          int at = firstRow.get(probeColumns.longValue(0, leaf, row));
          if (at < 0) {
            continue;
          }
          readProbeRow(probeColumns, leaf, row, probeFieldCount, probeValues, probePresent);
          for (; at >= 0; at = nextRow[at]) {
            if (!matchesResidual(buildRows, at, buildSide, probeValues, probePresent, residualStack)) {
              continue;
            }
            if (rowOrdering != null) {
              emitRow(keyCount, buildSide, probeSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                  stringKeys, buildColumns, probeColumns, leaf, row, rowOrdering);
              emittedRows++;
              continue;
            }
            foldPair(keyCount, aggCount, accWidth, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent,
                keyComponents, probeKey, groups, operands, stack);
          }
        }
      }
    }
    SirixVectorizedExecutor.noteJoinGroupServed();
    return rowOrdering != null
        ? ordered(rowOrdering, emittedRows)
        : emit(groups, buildColumns, stringKeys);
  }

  private boolean matchesResidual(final BuildRows build, final int row, final int buildSide, final long[] probeValues,
      final boolean[] probePresent, final boolean[] stack) {
    int top = 0;
    for (final int instruction : residualCode) {
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
        final boolean leftPresent = leftBuild
            ? build.present[row * build.fieldCount + leftSlot]
            : probePresent[leftSlot];
        final boolean rightPresent = rightBuild
            ? build.present[row * build.fieldCount + rightSlot]
            : probePresent[rightSlot];
        final long a = leftBuild
            ? build.values[row * build.fieldCount + leftSlot]
            : probeValues[leftSlot];
        final long b = rightBuild
            ? build.values[row * build.fieldCount + rightSlot]
            : probeValues[rightSlot];
        stack[top++] = leftPresent && rightPresent && (residualNe[instruction]
            ? a != b
            : a == b);
      }
    }
    return top == 0 || stack[0];
  }

  private Sequence emit(final Map<GroupKey, long[]> groups, final MaskedColumns interner, final boolean[] stringKeys)
      throws QueryException {
    final List<Item> records = new ArrayList<>(groups.size());
    final int keyCount = keyFields.length;
    for (final Map.Entry<GroupKey, long[]> group : groups.entrySet()) {
      final long[] key = group.getKey().components;
      final long[] acc = group.getValue();
      final long missing = key[keyCount];
      final Sequence[] values = new Sequence[entryKinds.length];
      for (int i = 0; i < entryKinds.length; i++) {
        final int kind = entryKinds[i];
        if (kind >= 0) {
          if ((missing & 1L << kind) != 0L) {
            values[i] = null;
          } else {
            // Every string id lives in the build side's interner, probe ids included.
            values[i] = stringKeys[kind]
                ? new Str(interner.string((int) key[kind]))
                : new Int64(key[kind]);
          }
        } else {
          final int a = -(kind + 1);
          final int base = 1 + 4 * a;
          values[i] = switch (aggFuncs[a]) {
            case "count" -> new Int64(aggFields[a] == null
                ? acc[0]
                : acc[base]);
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
      final Long2IntOpenHashMap firstRow) {
    int[] nextRow = new int[Math.max(16, (int) Math.min(Integer.MAX_VALUE - 8, buildColumns.rows()))];
    // BUILD: every admitted row with a present join key, chained per key value.
    for (int leaf = 0; leaf < buildColumns.leafCount(); leaf++) {
      final long[] mask = buildColumns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
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
          final long joinKey = buildColumns.longValue(0, leaf, row);
          final int at = buildRows.add(buildColumns.recordKey(leaf, row));
          if (at == nextRow.length) {
            nextRow = Arrays.copyOf(nextRow, nextRow.length << 1);
          }
          for (int f = 0; f < buildFieldCount; f++) {
            final boolean present = buildColumns.present(f, leaf, row);
            buildRows.present[at * buildFieldCount + f] = present;
            if (present) {
              buildRows.values[at * buildFieldCount + f] = buildColumns.isString(f)
                  ? buildColumns.stringId(f, leaf, row)
                  : buildColumns.longValue(f, leaf, row);
            }
          }
          final int head = firstRow.get(joinKey);
          nextRow[at] = head;
          firstRow.put(joinKey, at);
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
            : probeColumns.longValue(f, leaf, row);
      }
    }
  }

  private void emitRow(final int keyCount, final int buildSide, final int probeSide, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent,
      final boolean[] stringKeys, final MaskedColumns buildColumns, final MaskedColumns probeColumns, final int leaf,
      final int row, final Ordering rowOrdering) {
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
        values[k] = stringKeys[k]
            ? new Str(buildColumns.string((int) value))
            : new Int64(value);
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
      final Map<GroupKey, long[]> groups, final long[] operands, final long[] stack) {
    // The group key of this pair.
    pairKey(keyCount, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent, keyComponents);
    probeKey.rehash();
    long[] acc = groups.get(probeKey);
    if (acc == null) {
      acc = new long[accWidth];
      for (int a = 0; a < aggCount; a++) {
        acc[1 + 4 * a + 2] = Long.MAX_VALUE;
        acc[1 + 4 * a + 3] = Long.MIN_VALUE;
      }
      groups.put(new GroupKey(keyComponents.clone()), acc);
    }
    acc[0]++;
    foldAggregates(aggCount, buildSide, buildFieldCount, buildRows, at, probeValues, probePresent, acc, operands,
        stack);
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
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent, final long[] acc,
      final long[] operands, final long[] stack) {
    for (int a = 0; a < aggCount; a++) {
      final int base = 1 + 4 * a;
      final String field = aggFields[a];
      if (field == null) {
        continue; // a pair count reads acc[0]
      }
      final boolean fromBuild = aggSides[a] == buildSide;
      final long v;
      if (field.startsWith(COMPUTED_PREFIX)) {
        final int program = Integer.parseInt(field.substring(COMPUTED_PREFIX.length()));
        if (!programOperands(program, fromBuild, buildFieldCount, buildRows, at, probeValues, probePresent, operands)) {
          continue;
        }
        v = ProjectionComputedColumn.run(progCode[program], progConsts[program], operands, stack);
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
      acc[base]++;
      acc[base + 1] = Math.addExact(acc[base + 1], v);
      if (v < acc[base + 2]) {
        acc[base + 2] = v;
      }
      if (v > acc[base + 3]) {
        acc[base + 3] = v;
      }
    }
  }

  private boolean programOperands(final int program, final boolean fromBuild, final int buildFieldCount,
      final BuildRows buildRows, final int at, final long[] probeValues, final boolean[] probePresent,
      final long[] operands) {
    boolean allPresent = true;
    for (int o = 0; o < progSlots[program].length; o++) {
      final int slot = progSlots[program][o];
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

  private @Nullable String invalidOperands(final MaskedColumns[] columns) {
    for (int a = 0; a < aggFields.length; a++) {
      if (aggSlots[a] >= 0 && !columns[aggSides[a]].isLong(aggSlots[a])) {
        return "aggregate operand " + aggFields[a] + " is not a long column";
      }
    }
    for (int p = 0; p < progSlots.length; p++) {
      for (final int slot : progSlots[p]) {
        if (!columns[progSides[p]].isLong(slot)) {
          return "program operand is not a long column";
        }
      }
    }
    for (int i = 0; i < residualSlots.length; i++) {
      if (!columns[residualSides[i]].isLong(residualSlots[i])) {
        return "residual operand is not a long column";
      }
    }
    return null;
  }

}
