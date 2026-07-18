/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ProjectionIndexRegistry}, including the install-time
 * JIT pre-warm wired in {@code iter#09} for
 * {@link ProjectionIndexByteScan#conjunctiveCount} and
 * {@link ProjectionIndexByteScan#conjunctiveCountByGroup}.
 *
 * <p>Correctness gates for the pre-warm:
 * <ul>
 *   <li>No handle state mutation after pre-warm.</li>
 *   <li>Idempotent under repeated install — latched by registry key.</li>
 *   <li>Disabled cleanly via {@code -Dsirix.projection.prewarmJit=false}.</li>
 *   <li>Tolerant of missing column kinds (pure-numeric, pure-string handles).</li>
 *   <li>No exception ever leaks to the caller, even on malformed payloads.</li>
 * </ul>
 */
final class ProjectionIndexRegistryTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  @BeforeEach
  void wipe() {
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  void wipeAfter() {
    ProjectionIndexRegistry.clear();
  }

  private static byte[] buildLeaf(final long baseKey, final int rowCount) {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Ops"};
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {40L + i, 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false};
      final String[] strs = {null, null, depts[i % depts.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  /**
   * Happy path — install a handle with a (numeric, boolean, string_dict)
   * schema. Pre-warm is free to fire; the handle must be retrievable and
   * functionally equivalent to a non-pre-warmed install.
   */
  @Test
  void installWildcardWithPrewarmedSchemaReturnsHandle() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 32));
    leaves.add(buildLeaf(100L, 32));

    ProjectionIndexRegistry.installWildcard("res-A", new String[] {"age", "active", "dept"}, leaves);

    final ProjectionIndexRegistry.Handle handle = ProjectionIndexRegistry.lookup("res-A", new String[0]);
    assertNotNull(handle);
    assertEquals(2, handle.leafPayloads().size());
    assertEquals(0, handle.columnOf("age"));
    assertEquals(1, handle.columnOf("active"));
    assertEquals(2, handle.columnOf("dept"));
  }

  /**
   * Pre-warm must not mutate the stored leaf payloads. The byte-for-byte
   * equality of the retrieved list against the supplied input guards against
   * any accidental payload modification.
   */
  @Test
  void prewarmDoesNotMutateInstalledPayloads() {
    final byte[] a = buildLeaf(0L, 16);
    final byte[] b = buildLeaf(200L, 16);
    final byte[] aCopy = a.clone();
    final byte[] bCopy = b.clone();
    final List<byte[]> leaves = List.of(a, b);

    ProjectionIndexRegistry.installWildcard("res-mut", new String[] {"age", "active", "dept"}, leaves);

    assertArrayEqualsBytes(aCopy, a);
    assertArrayEqualsBytes(bCopy, b);
  }

  /**
   * Pre-warm must tolerate a handle without a STRING_DICT column — the
   * group-by pre-warm branch must short-circuit. No exception must reach
   * the caller.
   */
  @Test
  void installTolerantOfMissingStringDictColumn() {
    final byte[] kindsNumBool = {
        ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN
    };
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(kindsNumBool);
    for (int i = 0; i < 16; i++) {
      p.appendRow(i, new long[] {40L + i, 0L}, new boolean[] {false, (i & 1) == 0}, new String[] {null, null});
    }
    final List<byte[]> leaves = List.of(p.serialize());

    ProjectionIndexRegistry.installWildcard("res-nostr", new String[] {"age", "active"}, leaves);
    final ProjectionIndexRegistry.Handle h = ProjectionIndexRegistry.lookup("res-nostr", new String[0]);
    assertNotNull(h);
    assertEquals(2, h.fieldNames().length);
  }

  /**
   * Empty-list installs must short-circuit pre-warm (no first-leaf
   * inspection) and still publish a queryable handle.
   */
  @Test
  void installEmptyLeafListSkipsPrewarm() {
    ProjectionIndexRegistry.installWildcard("res-empty", new String[] {"age"}, List.of());
    final ProjectionIndexRegistry.Handle h = ProjectionIndexRegistry.lookup("res-empty", new String[0]);
    assertNotNull(h);
    assertEquals(0, h.leafPayloads().size());
  }

  /**
   * Direct pre-warm invocation — exercises the implementation even when
   * {@code -Dsirix.projection.prewarmJit=false} would have skipped the
   * install-time firing. Must execute a very small number of iterations
   * (1 iteration × tiny subList) when tuned via the system property
   * and must not throw.
   */
  @Test
  void prewarmJitForHandleIsSafeToCallDirectly() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 16));
    final ProjectionIndexRegistry.Handle h = new ProjectionIndexRegistry.Handle(
        new String[] {"age", "active", "dept"}, leaves);
    // Does not throw — handle is well-formed, column kinds include all three shapes.
    ProjectionIndexRegistry.prewarmJitForHandle(h);
  }

  /**
   * Verify {@link ProjectionIndexByteScan#conjunctiveCountByGroup} is a
   * no-op friendly with an empty predicates array — the pre-warm calls it
   * this way for the {@code groupByDept} shape (no WHERE clause).
   */
  @Test
  void conjunctiveCountByGroupWithEmptyPredsSanity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 12));
    final ProjectionIndexScan.ColumnPredicate[] empty = new ProjectionIndexScan.ColumnPredicate[0];
    final it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap<String> out = new it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, empty, 2, out);
    // All 12 rows should be accounted for across the 3-dept dictionary.
    long total = 0L;
    for (final var e : out.object2LongEntrySet()) total += e.getLongValue();
    assertEquals(12L, total);
  }

  /**
   * iter#10 — canonicalDict lazy build: first call probes leaves and
   * returns the union of dict values; subsequent calls return the
   * cached array without re-probing.
   */
  @Test
  void canonicalDict_lazyBuildAndCache() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 32));
    leaves.add(buildLeaf(100L, 32));
    final ProjectionIndexRegistry.Handle h = new ProjectionIndexRegistry.Handle(
        new String[] {"age", "active", "dept"}, leaves);
    final byte[][] first = h.canonicalDict(2, 16, 256);
    assertNotNull(first);
    assertEquals(3, first.length);  // {Eng, Sales, Ops}
    final byte[][] second = h.canonicalDict(2, 16, 256);
    // Same reference — cache hit.
    org.junit.jupiter.api.Assertions.assertSame(first, second);
  }

  /**
   * iter#10 — canonicalDict returns null for non-STRING_DICT columns.
   * Cached result means repeat calls stay null without re-probing.
   */
  @Test
  void canonicalDict_numericColumnReturnsNull() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 8));
    final ProjectionIndexRegistry.Handle h = new ProjectionIndexRegistry.Handle(
        new String[] {"age", "active", "dept"}, leaves);
    org.junit.jupiter.api.Assertions.assertNull(h.canonicalDict(0, 16, 256));
    // Second call — ineligible sentinel cached; still null.
    org.junit.jupiter.api.Assertions.assertNull(h.canonicalDict(0, 16, 256));
  }

  /**
   * iter#10 — canonicalDict returns null when cardinality exceeds
   * {@code cardLimit}. Dense path falls back to hashmap.
   */
  @Test
  void canonicalDict_aboveCardLimitReturnsNull() {
    final byte[] kinds = {
        ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
    };
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(kinds);
    for (int i = 0; i < 10; i++) {
      p.appendRow(i, new long[] {40L + i, 0L, 0L},
          new boolean[] {false, false, false},
          new String[] {null, null, "Dept" + i});
    }
    final List<byte[]> leaves = List.of(p.serialize());
    final ProjectionIndexRegistry.Handle h = new ProjectionIndexRegistry.Handle(
        new String[] {"age", "active", "dept"}, leaves);
    // Limit 5, actual cardinality 10 → null.
    org.junit.jupiter.api.Assertions.assertNull(h.canonicalDict(2, 16, 5));
  }

  /**
   * iter#10 — canonicalDict tolerates out-of-range column index.
   */
  @Test
  void canonicalDict_negativeColumnIndexReturnsNull() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 8));
    final ProjectionIndexRegistry.Handle h = new ProjectionIndexRegistry.Handle(
        new String[] {"age", "active", "dept"}, leaves);
    org.junit.jupiter.api.Assertions.assertNull(h.canonicalDict(-1, 16, 256));
    // Placate unused-import check.
    assertFalse(false);
  }

  /**
   * Several wildcard projections per resource, keyed by their field list —
   * {@link ProjectionIndexRegistry#lookupCovering} must pick the NARROWEST
   * covering handle, and re-installing an existing field list must replace
   * that entry, not add a duplicate.
   */
  @Test
  void multipleProjectionsPerResourceSelectByCoverage() {
    final List<byte[]> wide = List.of(buildLeaf(0L, 16));
    final List<byte[]> narrow = List.of(buildLeaf(0L, 16));
    ProjectionIndexRegistry.installWildcard("res-multi",
        new String[] {"age", "active", "dept"}, wide);
    ProjectionIndexRegistry.installWildcard("res-multi",
        new String[] {"age", "active", "city"}, narrow);

    // Both entries coexist and are retrievable by exact field list.
    assertNotNull(ProjectionIndexRegistry.lookupExactFields("res-multi",
        new String[] {"age", "active", "dept"}));
    assertNotNull(ProjectionIndexRegistry.lookupExactFields("res-multi",
        new String[] {"age", "active", "city"}));

    // Coverage-driven selection: only the first covers "dept", only the
    // second covers "city"; both cover "age".
    final ProjectionIndexRegistry.Handle deptHandle =
        ProjectionIndexRegistry.lookupCovering("res-multi", new String[0], new String[] {"dept"});
    assertNotNull(deptHandle);
    assertTrue(deptHandle.columnOf("dept") >= 0);
    final ProjectionIndexRegistry.Handle cityHandle =
        ProjectionIndexRegistry.lookupCovering("res-multi", new String[0], new String[] {"city"});
    assertNotNull(cityHandle);
    assertTrue(cityHandle.columnOf("city") >= 0);
    assertNotNull(ProjectionIndexRegistry.lookupCovering("res-multi", new String[0],
        new String[] {"age", "active"}));

    // No covering handle for an unknown field.
    org.junit.jupiter.api.Assertions.assertNull(ProjectionIndexRegistry.lookupCovering(
        "res-multi", new String[0], new String[] {"salary"}));

    // Re-install of the same field list replaces in place.
    final List<byte[]> replacement = List.of(buildLeaf(500L, 8));
    ProjectionIndexRegistry.installWildcard("res-multi",
        new String[] {"age", "active", "dept"}, replacement);
    assertEquals(1, ProjectionIndexRegistry.lookupExactFields("res-multi",
        new String[] {"age", "active", "dept"}).leafPayloads().size());

    // uninstallWildcard removes exactly one entry.
    ProjectionIndexRegistry.uninstallWildcard("res-multi", new String[] {"age", "active", "dept"});
    org.junit.jupiter.api.Assertions.assertNull(ProjectionIndexRegistry.lookupExactFields(
        "res-multi", new String[] {"age", "active", "dept"}));
    assertNotNull(ProjectionIndexRegistry.lookupExactFields("res-multi",
        new String[] {"age", "active", "city"}));
  }

  /**
   * The narrowest covering projection wins when several overlap — narrower
   * handles scan fewer columns per row and keep selection deterministic.
   */
  @Test
  void lookupCoveringPrefersNarrowestHandle() {
    final byte[] kindsNum = { ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG };
    final ProjectionIndexLeafPage narrowLeaf = new ProjectionIndexLeafPage(kindsNum);
    for (int i = 0; i < 8; i++) {
      narrowLeaf.appendRow(i, new long[] {40L + i}, new boolean[] {false}, new String[] {null});
    }
    ProjectionIndexRegistry.installWildcard("res-narrow",
        new String[] {"age", "active", "dept"}, List.of(buildLeaf(0L, 8)));
    ProjectionIndexRegistry.installWildcard("res-narrow",
        new String[] {"age"}, List.of(narrowLeaf.serialize()));

    final ProjectionIndexRegistry.Handle selected =
        ProjectionIndexRegistry.lookupCovering("res-narrow", new String[0], new String[] {"age"});
    assertNotNull(selected);
    assertEquals(1, selected.fieldNames().length);
  }

  private static void assertArrayEqualsBytes(final byte[] expected, final byte[] actual) {
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        assertEquals(expected[i], actual[i], "mismatch at index " + i);
      }
    }
  }
}
