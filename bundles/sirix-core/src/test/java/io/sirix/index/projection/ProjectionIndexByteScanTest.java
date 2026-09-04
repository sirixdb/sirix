/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parity tests: every count produced by {@link ProjectionIndexByteScan} must equal the count
 * produced by {@link ProjectionIndexScan} on the same inputs. The two paths differ only in how they
 * read the leaf — one materialises columns, the other reads bytes directly — so they should agree
 * exactly.
 */
final class ProjectionIndexByteScanTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static byte[] buildLeaf(final long baseKey, final int rowCount) {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Ops"};
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {40L + i, 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false};
      final String[] strs = {null, null, depts[i % depts.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  private static byte[] buildOrderMetadataLeaf(final boolean dense) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final long[] keys = dense
        ? new long[] {2L, 100L, 5L, 8L}
        : new long[] {2L, 5L, 8L, 11L};
    for (int row = 0; row < keys.length; row++) {
      assertTrue(page.appendExtractedUtf8Row(keys[row], new long[] {10L + row * 10L}, new boolean[1], new byte[1][],
          new int[1], new String[1][], new boolean[] {true}, new boolean[1], new boolean[1], new boolean[1],
          dense && row == 1));
    }
    return page.serialize();
  }

  @Test
  void countRowsParity() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(1000L, 10));
    leaves.add(buildLeaf(2000L, 5));
    assertEquals(ProjectionIndexScan.countRows(leaves), ProjectionIndexByteScan.countRows(leaves));
  }

  @Test
  void v0NoneAndDenseOrderMetadataPreserveByteScanColumnOffsets() {
    final byte[] none = buildOrderMetadataLeaf(false);
    final byte[] dense = buildOrderMetadataLeaf(true);
    final int orderMarkerOffset = 24 + 1 + 4 * Long.BYTES;
    assertEquals(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_NONE, none[orderMarkerOffset]);
    assertEquals(ProjectionIndexRowGroupPage.ORDER_EXCEPTIONS_DENSE, dense[orderMarkerOffset]);
    assertEquals(none.length + Long.BYTES, dense.length,
        "four DENSE rows add exactly one live exception word before the column bytes");

    final ProjectionIndexScan.ColumnPredicate[] predicates =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 25L)};
    for (final byte[] leaf : List.of(none, dense)) {
      final List<byte[]> leaves = List.of(leaf);
      assertEquals(4L, ProjectionIndexByteScan.countRows(leaves));
      assertEquals(2L, ProjectionIndexByteScan.conjunctiveCount(leaves, predicates));
      assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, predicates),
          ProjectionIndexByteScan.conjunctiveCount(leaves, predicates));
    }
  }

  @Test
  void numericGtParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void numericEqParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 42L)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanTrueParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanFalseParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {ProjectionIndexScan.ColumnPredicate.booleanEq(1, false)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqHitParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqMissParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.stringEq(2, "NotInDict".getBytes(StandardCharsets.UTF_8))};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void threeWayAndParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L),
            ProjectionIndexScan.ColumnPredicate.booleanEq(1, true),
            ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void zoneMapPruneParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 1000L)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void multiLeafParity() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 10));
    leaves.add(buildLeaf(1000L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void groupByCountUnfilteredSingleLeaf() {
    // 9 rows cycling through {Eng, Sales, Ops} → each group has 3.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, out);
    assertEquals(3L, out.getLong("Eng"));
    assertEquals(3L, out.getLong("Sales"));
    assertEquals(3L, out.getLong("Ops"));
  }

  @Test
  void groupByCountFilteredByNumeric() {
    // age > 42 filter: rows 3..9 match → 7 rows cycling through depts.
    // Row indices matching: i where 40+i > 42 ⇒ i ∈ {3,4,5,6,7,8}.
    // At i=3 dept=Eng, i=4 Sales, i=5 Ops, i=6 Eng, i=7 Sales, i=8 Ops.
    // Eng: 2, Sales: 2, Ops: 2 (6 total — i=9 excluded since rowCount=9).
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 42L)};
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, preds, 2, out);
    assertEquals(2L, out.getLong("Eng"));
    assertEquals(2L, out.getLong("Sales"));
    assertEquals(2L, out.getLong("Ops"));
  }

  @Test
  void groupByCountMultiLeafAccumulates() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 9)); // 3/3/3
    leaves.add(buildLeaf(1000L, 6)); // 2/2/2
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, out);
    assertEquals(5L, out.getLong("Eng"));
    assertEquals(5L, out.getLong("Sales"));
    assertEquals(5L, out.getLong("Ops"));
  }

  @Test
  void groupByOnNonStringColumnRejected() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 4));
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    // Column 0 is NUMERIC_LONG — should throw.
    assertThrows(IllegalStateException.class, () -> ProjectionIndexByteScan.conjunctiveCountByGroup(leaves,
        new ProjectionIndexScan.ColumnPredicate[0], 0, out));
  }

  // ---------------------------------------------------------------------
  // iter#07 range-fusion parity tests.
  //
  // For every BETWEEN op combination, the fused predicate must produce
  // byte-for-byte the same row count as two independent predicates
  // evaluated in conjunction. Tests cover the four op combos plus edge
  // cases (empty range, single-value range, all-match, no-match, mixed
  // with non-numeric predicates).
  // ---------------------------------------------------------------------

  /**
   * Build a wider synthetic leaf so between-tests can hit a variety of numeric values.
   * {@code values[i] = baseKey + i * 3 % 100} — covers 0..99 with a stride that stresses the zone-map
   * boundary.
   */
  private static byte[] buildLeafBetween(final long baseKey, final int rowCount) {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Ops"};
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {(long) ((i * 17) % 100), 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false};
      final String[] strs = {null, null, depts[i % depts.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  private static long countUnfused(final List<byte[]> leaves, final int column, final ProjectionIndexScan.Op lowOp,
      final long lowLit, final ProjectionIndexScan.Op highOp, final long highLit) {
    final ProjectionIndexScan.ColumnPredicate[] unfused =
        {ProjectionIndexScan.ColumnPredicate.numeric(column, lowOp, lowLit),
            ProjectionIndexScan.ColumnPredicate.numeric(column, highOp, highLit)};
    return ProjectionIndexByteScan.conjunctiveCount(leaves, unfused);
  }

  private static long countFused(final List<byte[]> leaves, final int column, final ProjectionIndexScan.Op lowOp,
      final long lowLit, final ProjectionIndexScan.Op highOp, final long highLit) {
    final ProjectionIndexScan.ColumnPredicate[] fused =
        {ProjectionIndexScan.ColumnPredicate.numericBetween(column, lowOp, lowLit, highOp, highLit)};
    return ProjectionIndexByteScan.conjunctiveCount(leaves, fused);
  }

  @Test
  void betweenGtLtParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[] {-10L, 0L, 10L, 25L, 50L, 99L, 110L}) {
      for (final long hi : new long[] {-5L, 5L, 30L, 60L, 99L, 200L}) {
        assertEquals(countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            "GT_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGtLeParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[] {0L, 25L, 49L, 99L}) {
      for (final long hi : new long[] {0L, 30L, 50L, 99L}) {
        assertEquals(countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LE, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LE, hi),
            "GT_LE lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGeLtParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[] {0L, 1L, 50L, 99L, 100L}) {
      for (final long hi : new long[] {0L, 2L, 50L, 99L, 100L}) {
        assertEquals(countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LT, hi),
            "GE_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGeLeParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[] {-5L, 0L, 50L, 99L, 100L}) {
      for (final long hi : new long[] {-5L, 0L, 50L, 99L, 100L}) {
        assertEquals(countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LE, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LE, hi),
            "GE_LE lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenEmptyRangeYieldsZero() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    // lo > hi → no row can satisfy.
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, 60L, ProjectionIndexScan.Op.LT, 30L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GT, 60L, ProjectionIndexScan.Op.LT, 30L);
    assertEquals(0L, unfused);
    assertEquals(0L, fused);
  }

  @Test
  void betweenSingleValueRangeGeLe() {
    // GE 50 AND LE 50 ⇒ v == 50
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, 50L, ProjectionIndexScan.Op.LE, 50L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GE, 50L, ProjectionIndexScan.Op.LE, 50L);
    assertEquals(unfused, fused);
    // Also assert it matches a direct EQ predicate.
    final ProjectionIndexScan.ColumnPredicate[] eqPred =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 50L)};
    assertEquals(ProjectionIndexByteScan.conjunctiveCount(leaves, eqPred), fused);
  }

  @Test
  void betweenAllMatchWidth() {
    // lo < min, hi > max ⇒ every row matches.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, -1000L, ProjectionIndexScan.Op.LE, 1000L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GE, -1000L, ProjectionIndexScan.Op.LE, 1000L);
    assertEquals(1024L, unfused);
    assertEquals(1024L, fused);
  }

  @Test
  void betweenNoMatchByZoneMap() {
    // hi < min ⇒ zone-map rules out the whole leaf.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    // values are (i*17)%100 ∈ [0, 99]. A BETWEEN(200, 300) excludes all.
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, 200L, ProjectionIndexScan.Op.LT, 300L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GT, 200L, ProjectionIndexScan.Op.LT, 300L);
    assertEquals(0L, unfused);
    assertEquals(0L, fused);
  }

  @Test
  void betweenMixedWithBooleanParity() {
    // age BETWEEN 30 AND 70 AND active == true.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final ProjectionIndexScan.ColumnPredicate[] unfused =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 30L),
            ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LT, 70L),
            ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)};
    final ProjectionIndexScan.ColumnPredicate[] fused =
        {ProjectionIndexScan.ColumnPredicate.numericBetween(0, ProjectionIndexScan.Op.GT, 30L,
            ProjectionIndexScan.Op.LT, 70L), ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)};
    assertEquals(ProjectionIndexByteScan.conjunctiveCount(leaves, unfused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  @Test
  void betweenMixedWithStringEqParity() {
    // age BETWEEN 25 AND 75 AND dept == "Eng".
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final ProjectionIndexScan.ColumnPredicate[] unfused =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GE, 25L),
            ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LE, 75L),
            ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    final ProjectionIndexScan.ColumnPredicate[] fused = {ProjectionIndexScan.ColumnPredicate.numericBetween(0,
        ProjectionIndexScan.Op.GE, 25L, ProjectionIndexScan.Op.LE, 75L),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    assertEquals(ProjectionIndexByteScan.conjunctiveCount(leaves, unfused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  @Test
  void betweenMultiLeafParity() {
    // Zone-map kicks in on some leaves and not others — verify we don't
    // short-circuit wrong.
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeafBetween(0L, 1024));
    leaves.add(buildLeafBetween(1024L, 1024));
    leaves.add(buildLeafBetween(2048L, 512)); // partial leaf
    for (final long lo : new long[] {0L, 40L, 99L}) {
      for (final long hi : new long[] {50L, 99L, 500L}) {
        assertEquals(countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            "multi-leaf GT_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenMaterializingScanParity() {
    // Cross-check the materialising ProjectionIndexScan (not the byte
    // scan) handles BETWEEN correctly too — both paths share the
    // test discipline.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 256));
    final ProjectionIndexScan.ColumnPredicate[] fused = {ProjectionIndexScan.ColumnPredicate.numericBetween(0,
        ProjectionIndexScan.Op.GT, 20L, ProjectionIndexScan.Op.LE, 70L)};
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, fused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  // ---------------------------------------------------------------------
  // iter#10 dense group-by parity tests.
  //
  // For every supported input shape, the dense long[N] accumulator path
  // must produce the same Object2LongOpenHashMap<String> counts as the
  // legacy hashmap path. The dense path is an optimization — never a
  // semantic change.
  // ---------------------------------------------------------------------

  /**
   * Build a leaf with a caller-specified dept/city dictionary. Columns: [numeric age, boolean active,
   * STRING_DICT dept, STRING_DICT city]. Row {@code i} gets {@code depts[i % depts.length]} and
   * {@code cities[(i * 3) % cities.length]} — stride-3 on city keeps the two dicts from moving in
   * lock-step so fallback cases actually exercise per-leaf dict divergence.
   */
  private static byte[] buildLeafWithDepts(final long baseKey, final int rowCount, final String[] depts,
      final String[] cities) {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
            ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(kinds);
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {20L + (i % 50), 0L, 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false, false};
      final String[] strs = {null, null, depts[i % depts.length], cities[(i * 3) % cities.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  /**
   * Run both dense and hashmap paths and assert the resulting Object2LongOpenHashMap<String> are
   * byte-for-byte equal.
   */
  private static void assertDenseParity(final List<byte[]> leaves, final ProjectionIndexScan.ColumnPredicate[] preds,
      final int groupColumn, final byte[][] canonicalDict) {
    // Hashmap (legacy) path — ground truth.
    final Object2LongOpenHashMap<String> hash = new Object2LongOpenHashMap<>();
    hash.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, preds, groupColumn, hash);

    // Dense path — counts + fallback accumulator.
    final long[] counts = new long[canonicalDict.length];
    final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
    fallback.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupDense(leaves, preds, groupColumn, canonicalDict, counts, fallback);
    // Merge dense counts + fallback into a single map to compare.
    final Object2LongOpenHashMap<String> dense = new Object2LongOpenHashMap<>();
    dense.defaultReturnValue(0L);
    for (int i = 0; i < canonicalDict.length; i++) {
      if (counts[i] != 0L) {
        dense.put(new String(canonicalDict[i], StandardCharsets.UTF_8), counts[i]);
      }
    }
    final var it = fallback.object2LongEntrySet().fastIterator();
    while (it.hasNext()) {
      final var e = it.next();
      dense.addTo(e.getKey(), e.getLongValue());
    }

    assertEquals(hash, dense, "dense vs hashmap group-by counts must match");
  }

  @Test
  void denseGroupBy_emptyPreds_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    assertEquals(8, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_boolEq_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    final ProjectionIndexScan.ColumnPredicate[] preds = {ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)};
    assertDenseParity(leaves, preds, 2, canonical);
  }

  @Test
  void denseGroupBy_numericBetween_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    final ProjectionIndexScan.ColumnPredicate[] preds = {ProjectionIndexScan.ColumnPredicate.numericBetween(0,
        ProjectionIndexScan.Op.GE, 30L, ProjectionIndexScan.Op.LE, 50L)};
    assertDenseParity(leaves, preds, 2, canonical);
  }

  @Test
  void denseGroupBy_n256_boundaryAccepted() {
    // 256 distinct strings — right at the limit.
    final String[] depts = new String[256];
    for (int i = 0; i < 256; i++)
      depts[i] = "D" + i;
    final String[] cities = {"C0"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    assertEquals(256, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_n257_aboveThresholdReturnsNull() {
    // 257 distinct strings — above the limit of 256.
    final String[] depts = new String[257];
    for (int i = 0; i < 257; i++)
      depts[i] = "D" + i;
    final String[] cities = {"C0"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical, "probe should return null when cardinality > cardLimit");
  }

  @Test
  void denseGroupBy_crossLeafDictVariation() {
    // Two leaves with the SAME 4 values but in different dict positions.
    // Leaf 1: D0, D1, D2, D3. Leaf 2: D3, D2, D1, D0 (rotated).
    final List<byte[]> leaves = new ArrayList<>();
    final String[] cities = {"C0"};
    leaves.add(buildLeafWithDepts(0L, 12, new String[] {"D0", "D1", "D2", "D3"}, cities));
    leaves.add(buildLeafWithDepts(1000L, 12, new String[] {"D3", "D2", "D1", "D0"}, cities));
    // Canonical dict built from probing — union is {D0,D1,D2,D3}.
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    assertEquals(4, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_missingDictValueTriggersFallback() {
    // Probe sees only 2 leaves (D0, D1). A third leaf introduces D99.
    final List<byte[]> leaves = new ArrayList<>();
    final String[] cities = {"C0"};
    leaves.add(buildLeafWithDepts(0L, 8, new String[] {"D0", "D1"}, cities));
    leaves.add(buildLeafWithDepts(100L, 8, new String[] {"D0", "D1"}, cities));
    // Build canonical from the first 2 leaves only — probeLeaves=2.
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 2, 256);
    assertEquals(2, canonical.length);
    // NOW add a third leaf with a new value.
    leaves.add(buildLeafWithDepts(200L, 8, new String[] {"D99"}, cities));
    // Dense path must fall back for the third leaf. Parity holds.
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_singleValueN1() {
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 16, new String[] {"OnlyOne"}, new String[] {"C0"}));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    assertEquals(1, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_multiLeafAccumulates() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0"};
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeafWithDepts(0L, 1024, depts, cities));
    leaves.add(buildLeafWithDepts(1024L, 1024, depts, cities));
    leaves.add(buildLeafWithDepts(2048L, 512, depts, cities)); // partial
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 2, 16, 256);
    assertEquals(8, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void probeCanonicalDict_ineligibleForNumericColumn() {
    // Group column 0 is numeric — not STRING_DICT; probe returns null.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(leaves, 0, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical);
  }

  @Test
  void probeCanonicalDict_emptyListReturnsNull() {
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(List.of(), 2, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical);
  }

  // =====================================================================
  // NUMERIC_LONG group-by kernels.
  //
  // The dense and hash arms must be indistinguishable by result, and every
  // fail-loud site must actually fire — a numeric group-by that silently
  // mis-buckets is a WRONG ANSWER, not a slow one.
  // =====================================================================

  private static final byte[] KINDS_NUM_ONLY = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  /**
   * One numeric column; {@code values[i] == Long.MIN_VALUE} marks the row as MISSING (the sentinel
   * cannot collide with a real value in any of these fixtures).
   */
  private static byte[] numericLeaf(final long baseKey, final long[] values) {
    return numericLeaf(baseKey, values, KINDS_NUM_ONLY);
  }

  private static byte[] numericLeaf(final long baseKey, final long[] values, final byte[] kinds) {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(kinds);
    final long[] nums = new long[kinds.length];
    final boolean[] bools = new boolean[kinds.length];
    final String[] strs = new String[kinds.length];
    final boolean[] present = new boolean[kinds.length];
    for (int i = 0; i < values.length; i++) {
      final boolean missing = values[i] == Long.MIN_VALUE;
      nums[0] = missing
          ? 0L
          : values[i];
      present[0] = !missing;
      assertTrue(p.appendRow(baseKey + i, nums, bools, strs, present, null));
    }
    return p.serialize();
  }

  private static Long2LongOpenHashMap hashArm(final List<byte[]> leaves, final long[] missingOut) {
    final Long2LongOpenHashMap out = new Long2LongOpenHashMap();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupNumeric(leaves, new ProjectionIndexScan.ColumnPredicate[0], 0, out,
        missingOut);
    return out;
  }

  /** Run BOTH arms over the same leaves through the zone-map union and assert they agree exactly. */
  private static Long2LongOpenHashMap assertNumericArmParity(final List<byte[]> leaves) {
    final long[] hashMissing = new long[1];
    final Long2LongOpenHashMap hash = hashArm(leaves, hashMissing);

    final long[] range = new long[3];
    assertTrue(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, range), "zone union must be known");
    final long span = range[1] - range[0];
    assertTrue(span >= 0 && span < 1 << 20, "fixture must fit the dense arm");
    final long[] counts = new long[(int) span + 1];
    final long[] denseMissing = new long[1];
    ProjectionIndexByteScan.conjunctiveCountByGroupNumericDense(leaves, new ProjectionIndexScan.ColumnPredicate[0], 0,
        range[0], counts, denseMissing);

    assertEquals(hashMissing[0], denseMissing[0], "arms disagree on the missing-field bucket");
    long denseGroups = 0;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] == 0L) {
        continue;
      }
      denseGroups++;
      assertEquals(counts[i], hash.get(range[0] + i), "arms disagree on group " + (range[0] + i));
    }
    assertEquals(hash.size(), denseGroups, "arms disagree on the group COUNT");
    return hash;
  }

  @Test
  void numericGroupByArmsAgreeOnRandomValues() {
    final java.util.Random rng = new java.util.Random(4711);
    final List<byte[]> leaves = new ArrayList<>();
    final Long2LongOpenHashMap oracle = new Long2LongOpenHashMap();
    oracle.defaultReturnValue(0L);
    for (int l = 0; l < 3; l++) {
      final long[] values = new long[500 + l];
      for (int i = 0; i < values.length; i++) {
        values[i] = 1_000L + rng.nextInt(64);
        oracle.addTo(values[i], 1L);
      }
      leaves.add(numericLeaf(l * 1000L, values));
    }
    final Long2LongOpenHashMap served = assertNumericArmParity(leaves);
    assertEquals(oracle, served);
  }

  @Test
  void numericGroupByHandlesNegativeBaseAndRangeStraddlingZero() {
    // v - base is the dense index: a negative base is the sign trap this pins.
    final long[] values = {-7L, -1L, 0L, 1L, 5L, -7L, 0L, 0L};
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    final long[] range = new long[3];
    assertTrue(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, range));
    assertEquals(-7L, range[0]);
    assertEquals(5L, range[1]);
    assertEquals(8L, range[2]);
    final Long2LongOpenHashMap served = assertNumericArmParity(leaves);
    assertEquals(2L, served.get(-7L));
    assertEquals(3L, served.get(0L));
    assertEquals(1L, served.get(5L));
  }

  @Test
  void numericGroupBySingleValueColumnHasOneCell() {
    final long[] values = new long[40];
    java.util.Arrays.fill(values, 42L);
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    final long[] range = new long[3];
    assertTrue(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, range));
    assertEquals(0L, range[1] - range[0], "gMin == gMax must yield exactly one cell");
    assertEquals(40L, assertNumericArmParity(leaves).get(42L));
  }

  @Test
  void numericGroupBySpanOverflowFallsToHashArm() {
    // Long.MIN_VALUE is this fixture's missing sentinel, so use MIN_VALUE + 1.
    final long[] values = {Long.MIN_VALUE + 1, Long.MAX_VALUE, 0L, Long.MAX_VALUE};
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    final long[] range = new long[3];
    assertTrue(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, range));
    // gMax - gMin wraps negative — the single `span >= 0` test is the complete overflow guard.
    assertTrue(range[1] - range[0] < 0, "the fixture must actually overflow the span");
    final long[] missing = new long[1];
    final Long2LongOpenHashMap hash = hashArm(leaves, missing);
    assertEquals(0L, missing[0]);
    assertEquals(1L, hash.get(Long.MIN_VALUE + 1));
    assertEquals(2L, hash.get(Long.MAX_VALUE));
    assertEquals(1L, hash.get(0L));
  }

  @Test
  void numericGroupByMissingRowsNeverGroupUnderZero() {
    // Rows 0, 3, 6, ... are MISSING; their stored default is 0, which is also a REAL value here.
    final long[] values = new long[30];
    for (int i = 0; i < values.length; i++) {
      values[i] = i % 3 == 0
          ? Long.MIN_VALUE
          : i % 7;
    }
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    long expectedMissing = 0;
    final Long2LongOpenHashMap oracle = new Long2LongOpenHashMap();
    oracle.defaultReturnValue(0L);
    for (int i = 0; i < values.length; i++) {
      if (i % 3 == 0) {
        expectedMissing++;
      } else {
        oracle.addTo((long) (i % 7), 1L);
      }
    }
    final long[] missing = new long[1];
    final Long2LongOpenHashMap hash = hashArm(leaves, missing);
    assertEquals(expectedMissing, missing[0]);
    assertEquals(oracle, hash);
    assertEquals(oracle.get(0L), hash.get(0L), "phantom zeros must not inflate the real 0 group");
    assertNumericArmParity(leaves);
  }

  @Test
  void numericGroupByValueOutsideTheZoneRangeThrows() {
    final List<byte[]> leaves = List.of(numericLeaf(0L, new long[] {0L, 1L, 2L, 9L}));
    // A two-cell accumulator cannot address 9 — corruption, not a late-arriving value.
    final IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexByteScan.conjunctiveCountByGroupNumericDense(leaves,
            new ProjectionIndexScan.ColumnPredicate[0], 0, 0L, new long[2], new long[1]));
    assertTrue(ise.getMessage().contains("outside zone range"), ise.getMessage());
  }

  @Test
  void numericGroupByWithoutPresenceTailThrows() {
    final byte[] full = numericLeaf(0L, new long[] {1L, 2L, 3L, 4L});
    final int presWords = (4 + 63) >>> 6;
    final int trailing = KINDS_NUM_ONLY.length + KINDS_NUM_ONLY.length * presWords * 8 + 9;
    final byte[] tailLess = java.util.Arrays.copyOf(full, full.length - trailing);
    final long[] missing = new long[1];
    for (final Executable arm : new Executable[] {() -> hashArm(List.of(tailLess), missing),
        () -> ProjectionIndexByteScan.conjunctiveCountByGroupNumericDense(List.of(tailLess),
            new ProjectionIndexScan.ColumnPredicate[0], 0, 0L, new long[8], missing)}) {
      final IllegalStateException ise = assertThrows(IllegalStateException.class, arm);
      assertTrue(ise.getMessage().contains("presence tail"), ise.getMessage());
    }
  }

  @Test
  void numericGroupByKindDriftMidListThrows() {
    final byte[] doubleKinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE};
    final List<byte[]> leaves =
        List.of(numericLeaf(0L, new long[] {1L, 2L}), numericLeaf(100L, new long[] {3L, 4L}, doubleKinds));
    final IllegalStateException ise = assertThrows(IllegalStateException.class, () -> hashArm(leaves, new long[1]));
    assertTrue(ise.getMessage().contains("is not NUMERIC_LONG"), ise.getMessage());
    // The union refuses the same list rather than reporting a range over mixed kinds.
    org.junit.jupiter.api.Assertions.assertFalse(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, new long[3]));
  }

  @Test
  void numericGroupByRejectsAStringSetColumn() {
    // STRING_SET must be an EXPLICIT rejection, not a fall-through from "not STRING_DICT":
    // a set column holds a run of ids per row, so reading 8 bytes at rowIdx*8 lands anywhere.
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    for (int i = 0; i < 4; i++) {
      page.appendRow(i, new long[1], new boolean[1], new String[1], new String[][] {{"t" + i % 2}},
          new boolean[] {true}, null, null, null);
    }
    final List<byte[]> leaves = List.of(page.serialize());
    final IllegalStateException ise = assertThrows(IllegalStateException.class, () -> hashArm(leaves, new long[1]));
    assertTrue(ise.getMessage().contains("is not NUMERIC_LONG"), ise.getMessage());
    org.junit.jupiter.api.Assertions.assertFalse(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, new long[3]));
  }

  @Test
  void numericZoneUnionSkipsAllMissingLeavesButKeepsTheirRows() {
    final long[] allMissing = new long[10];
    java.util.Arrays.fill(allMissing, Long.MIN_VALUE);
    final List<byte[]> leaves = List.of(numericLeaf(0L, new long[] {5L, 9L}), numericLeaf(100L, allMissing));
    final long[] range = new long[3];
    assertTrue(ProjectionIndexByteScan.numericZoneUnion(leaves, 0, range));
    assertEquals(5L, range[0], "the all-missing leaf's min>max sentinel must not widen the union");
    assertEquals(9L, range[1]);
    assertEquals(12L, range[2]);
  }

  @Test
  void numericZoneUnionIsUnknownWhenNoRowIsPresent() {
    final long[] allMissing = new long[8];
    java.util.Arrays.fill(allMissing, Long.MIN_VALUE);
    org.junit.jupiter.api.Assertions.assertFalse(
        ProjectionIndexByteScan.numericZoneUnion(List.of(numericLeaf(0L, allMissing)), 0, new long[3]));
    org.junit.jupiter.api.Assertions.assertFalse(ProjectionIndexByteScan.numericZoneUnion(List.of(), 0, new long[3]));
  }

  @Test
  void numericGroupByRespectsPredicates() {
    final long[] values = new long[64];
    for (int i = 0; i < values.length; i++) {
      values[i] = i;
    }
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    final ProjectionIndexScan.ColumnPredicate[] gt59 =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 59L)};
    final Long2LongOpenHashMap hash = new Long2LongOpenHashMap();
    hash.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupNumeric(leaves, gt59, 0, hash, new long[1]);
    assertEquals(4, hash.size());
    for (long v = 60; v <= 63; v++) {
      assertEquals(1L, hash.get(v));
    }
    final long[] counts = new long[64];
    ProjectionIndexByteScan.conjunctiveCountByGroupNumericDense(leaves, gt59, 0, 0L, counts, new long[1]);
    for (int i = 0; i < 64; i++) {
      assertEquals(i >= 60
          ? 1L
          : 0L, counts[i], "dense cell " + i);
    }
  }

  @Test
  void requireGroupSumsFitLongDeclinesA64BitIdColumn() {
    final long[] values = new long[1024];
    java.util.Arrays.fill(values, 1_000_000_000_000_000_000L);
    final List<byte[]> leaves = List.of(numericLeaf(0L, values));
    final ArithmeticException ae = assertThrows(ArithmeticException.class,
        () -> ProjectionIndexByteScan.requireGroupSumsFitLong(leaves, new int[] {0}));
    assertTrue(ae.getMessage().contains("column 0"), ae.getMessage());
    // A column whose whole-index magnitude bound fits is waved through.
    ProjectionIndexByteScan.requireGroupSumsFitLong(List.of(numericLeaf(0L, new long[] {1L, 2L, 3L})), new int[] {0});
  }

  @Test
  void requireGroupSumsFitLongDeclinesLongMinValue() {
    // Long.MIN_VALUE has no representable magnitude — absExact throws, and that IS the answer.
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS_NUM_ONLY);
    page.appendRow(1L, new long[] {Long.MIN_VALUE}, new boolean[1], new String[1]);
    assertThrows(ArithmeticException.class,
        () -> ProjectionIndexByteScan.requireGroupSumsFitLong(List.of(page.serialize()), new int[] {0}));
  }

  @Test
  void numericGroupAggregateFoldsPerGroupAndSeparatesTheMissingKey() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    // (key, amount) with a missing key on row 2 and a missing amount on row 3.
    final long[][] rows = {{7L, 10L}, {8L, 20L}, {Long.MIN_VALUE, 30L}, {7L, Long.MIN_VALUE}, {7L, 5L}};
    for (int i = 0; i < rows.length; i++) {
      final boolean keyMissing = rows[i][0] == Long.MIN_VALUE;
      final boolean amountMissing = rows[i][1] == Long.MIN_VALUE;
      page.appendRow(i, new long[] {keyMissing
          ? 0L
          : rows[i][0],
          amountMissing
              ? 0L
              : rows[i][1]},
          new boolean[2], new String[2], new boolean[] {!keyMissing, !amountMissing}, null);
    }
    final Long2ObjectOpenHashMap<long[]> out = new Long2ObjectOpenHashMap<>();
    final long[] missingAcc = ProjectionIndexByteScan.newGroupAggAcc(1, Long.MAX_VALUE);
    ProjectionIndexByteScan.conjunctiveAggregateByGroupNumeric(List.of(page.serialize()),
        new ProjectionIndexScan.ColumnPredicate[0], 0, new int[] {1}, out, missingAcc, 0);

    final long[] seven = out.get(7L);
    assertEquals(3L, seven[0], "matching rows in group 7");
    assertEquals(0L, seven[1], "group 7 first appears at leaf 0, row 0");
    assertEquals(2L, seven[2], "the row missing `amount` must not be counted into the aggregate");
    assertEquals(15L, seven[3]);
    assertEquals(5L, seven[4]);
    assertEquals(10L, seven[5]);
    assertEquals(1L, out.get(8L)[0]);
    assertEquals(1L, missingAcc[0], "the missing-key row must not group under 0");
    assertEquals(2L, missingAcc[1], "the null-key group keeps its document first-appearance ordinal");
    assertEquals(30L, missingAcc[3]);
    org.junit.jupiter.api.Assertions.assertFalse(out.containsKey(0L));
  }

  @Test
  void numericGroupAggregateDeclinesOnOverflow() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    for (int i = 0; i < 4; i++) {
      page.appendRow(i, new long[] {1L, Long.MAX_VALUE / 2}, new boolean[2], new String[2]);
    }
    assertThrows(ArithmeticException.class,
        () -> ProjectionIndexByteScan.conjunctiveAggregateByGroupNumeric(List.of(page.serialize()),
            new ProjectionIndexScan.ColumnPredicate[0], 0, new int[] {1}, new Long2ObjectOpenHashMap<>(),
            ProjectionIndexByteScan.newGroupAggAcc(1, Long.MAX_VALUE), 0));
  }

  // ==================== NE ====================

  @Test
  void numericNeParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.NE, 43L)};
    assertEquals(9L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds), "9 of the 10 values differ from 43");
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void numericNeMatchingNothingParity() {
    // A single-row leaf whose only value IS the literal — also the zone-skip case, where
    // min == max == lit lets the whole row group be discarded without reading a value.
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    page.appendRow(1L, new long[] {7L, 0L, 0L}, new boolean[3], new String[] {null, null, "Eng"});
    final List<byte[]> leaves = List.of(page.serialize());
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.NE, 7L)};
    assertEquals(0L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void neOverAMissingCellIsFalseNotTrue() {
    // THE semantic that separates NE from !EQ. In JSONiq a missing field dereferences to the
    // empty sequence, `() != 7` is the empty sequence, and `where` reads that as false — so a
    // record lacking the field must NOT match `!= 7`. Implemented as a negation it would match,
    // and every `<> ''` query in an analytical benchmark would over-count by the sparse rows.
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final boolean[] absent = {false, true, true};
    final boolean[] present = {true, true, true};
    // Row 0 HAS the numeric column and differs from the literal; rows 1..3 do not have it at all.
    page.appendRow(0L, new long[] {99L, 0L, 0L}, new boolean[3], new String[] {"", "", "Eng"}, present, null);
    for (int i = 1; i < 4; i++) {
      page.appendRow(i, new long[] {0L, 0L, 0L}, new boolean[3], new String[] {"", "", "Eng"}, absent, null);
    }
    final List<byte[]> leaves = List.of(page.serialize());
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.NE, 7L)};
    assertEquals(1L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds),
        "only the row that HAS the field may match; three missing rows must not");
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringNeParity() {
    // 9 rows cycling Eng/Sales/Ops ⇒ 6 differ from "Eng". Row count 9 also puts the mask's tail
    // bits in play: complementing the whole word would light rows 9..63, which do not exist.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.stringNe(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    assertEquals(6L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringNeAgainstAValueNotInTheDictMatchesEveryPresentRow() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.stringNe(2, "NotInDict".getBytes(StandardCharsets.UTF_8))};
    assertEquals(9L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
    assertEquals(ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringNeOverAMissingCellIsFalse() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS_NUM_BOOL_STR);
    final boolean[] absent = {true, true, false};
    final boolean[] present = {true, true, true};
    page.appendRow(0L, new long[3], new boolean[3], new String[] {"", "", "Sales"}, present, null);
    page.appendRow(1L, new long[3], new boolean[3], new String[] {"", "", ""}, absent, null);
    final List<byte[]> leaves = List.of(page.serialize());
    final ProjectionIndexScan.ColumnPredicate[] preds =
        {ProjectionIndexScan.ColumnPredicate.stringNe(2, "Eng".getBytes(StandardCharsets.UTF_8))};
    assertEquals(1L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds),
        "the row without the field must not match `!= \"Eng\"`");
  }

  @Test
  void booleanNeIsEqualityAgainstTheComplement() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    assertEquals(
        ProjectionIndexByteScan.conjunctiveCount(leaves,
            new ProjectionIndexScan.ColumnPredicate[] {ProjectionIndexScan.ColumnPredicate.booleanEq(1, false)}),
        ProjectionIndexByteScan.conjunctiveCount(leaves,
            new ProjectionIndexScan.ColumnPredicate[] {ProjectionIndexScan.ColumnPredicate.booleanNe(1, true)}));
  }

  @Test
  void integerSubstringOutsideLongDomainDeclinesInsteadOfWrapping() {
    final byte[] digits = "92233720368547758070".getBytes(StandardCharsets.US_ASCII);
    assertEquals(Long.MIN_VALUE,
        ProjectionIndexByteScan.xsIntegerOfSubstring(digits, 0, digits.length, 1, digits.length));
  }
}
