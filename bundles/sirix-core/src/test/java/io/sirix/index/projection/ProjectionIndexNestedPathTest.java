/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Path coverage for projection indexes beyond the flat {@code /[]} bench
 * shape:
 * <ul>
 * <li><b>nested roots</b> — the record set lives under object steps
 * ({@code /east/records/[]});</li>
 * <li><b>nested field columns</b> — a column path descends below the record
 * root ({@code .../[]/nested/city});</li>
 * <li><b>multi-PCR roots</b> — one descendant path pattern
 * ({@code //records/[]}) resolving to record sets under SIBLING subtrees;
 * historically this threw ("only single-path roots are supported"), now
 * every matching PCR is a record root;</li>
 * <li><b>multi-PCR field paths</b> — the same field pattern resolving under
 * each root; historically only the first PCR was (silently) matched, which
 * made every record under the second root report the field as missing.</li>
 * </ul>
 */
final class ProjectionIndexNestedPathTest {

  private static final String JSON = """
      {
        "east": {"records": [
          {"age": 30, "name": "a"},
          {"age": 45, "name": "b"}
        ]},
        "west": {"records": [
          {"age": 50, "name": "c"},
          {"age": 20, "nested": {"city": "NYC"}}
        ]},
        "other": [1, 2]
      }""";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(JSON),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static List<byte[]> buildLeaves(final String rootPath, final String agePath,
      final String namePath, final String cityPath) {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse(rootPath, PathParser.Type.JSON),
          List.of(parse(agePath, PathParser.Type.JSON),
                  parse(namePath, PathParser.Type.JSON),
                  parse(cityPath, PathParser.Type.JSON)),
          List.of(Type.LON, Type.STR, Type.STR),
          0,
          IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);
      return leaves;
    }
  }

  private static String stringAt(final ProjectionIndexRowGroupPage leaf, final int column, final int row) {
    final byte[][] dict = leaf.stringDictionary(column);
    final int id = leaf.stringDictIdColumn(column)[row];
    return new String(dict[id], StandardCharsets.UTF_8);
  }

  private static boolean presentAt(final ProjectionIndexRowGroupPage leaf, final int column, final int row) {
    return (leaf.presenceColumnBits(column)[row >>> 6] & (1L << (row & 63))) != 0;
  }

  // ==================== nested single-PCR root ====================

  @Test
  void selfNestedRootPcrsFailFast() {
    // A record set nested inside another matched record's subtree must be
    // rejected loudly — the builder cannot emit correct rows for it (the
    // inner record's fields would overwrite the outer row's columns).
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(
          "{\"records\":[{\"age\":30,\"records\":[{\"age\":99}]}]}"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("//records/[]", PathParser.Type.JSON),
          List.of(parse("//records/[]/age", PathParser.Type.JSON)),
          List.of(Type.LON), 0, IndexDef.DbType.JSON);
      assertThrows(IllegalStateException.class,
          () -> new ProjectionIndexBuilder(def, pathSummary, payload -> { }));
    }
  }


  @Test
  void nestedRootAndNestedFieldColumn() {
    final List<byte[]> leaves = buildLeaves(
        "/east/records/[]",
        "/east/records/[]/age",
        "/east/records/[]/name",
        "/east/records/[]/nested/city");
    assertEquals(1, leaves.size());
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    assertEquals(2, leaf.getRowCount());
    assertEquals(30, leaf.numericColumn(0)[0]);
    assertEquals(45, leaf.numericColumn(0)[1]);
    assertEquals("a", stringAt(leaf, 1, 0));
    assertEquals("b", stringAt(leaf, 1, 1));
    // No east record carries nested/city — column all-missing.
    assertFalse(presentAt(leaf, 2, 0));
    assertFalse(presentAt(leaf, 2, 1));
  }

  // ==================== multi-PCR root + multi-PCR fields ====================

  @Test
  void descendantPatternRootSpansSiblingSubtrees() {
    final List<byte[]> leaves = buildLeaves(
        "//records/[]",
        "//records/[]/age",
        "//records/[]/name",
        "//records/[]/nested/city");
    assertEquals(1, leaves.size());
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    // Both sibling record sets contribute rows, in document order.
    assertEquals(4, leaf.getRowCount());
    assertEquals(30, leaf.numericColumn(0)[0]);
    assertEquals(45, leaf.numericColumn(0)[1]);
    assertEquals(50, leaf.numericColumn(0)[2]);
    assertEquals(20, leaf.numericColumn(0)[3]);
    // Field PCRs resolve under BOTH roots — the west rows must not report
    // age/name as missing (the historical first-PCR-only behavior).
    assertTrue(presentAt(leaf, 0, 2), "west row must match the age field's second PCR");
    assertEquals("c", stringAt(leaf, 1, 2));
    assertFalse(presentAt(leaf, 1, 3), "row without name stays missing");
    // Nested column below the record root, present on exactly one row.
    assertFalse(presentAt(leaf, 2, 0));
    assertTrue(presentAt(leaf, 2, 3));
    assertEquals("NYC", stringAt(leaf, 2, 3));

    // The scan stack agrees: age > 25 matches rows 0,1,2.
    final long matches = ProjectionIndexByteScan.conjunctiveCount(leaves,
        new ProjectionIndexScan.ColumnPredicate[] {
            ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 25L)
        });
    assertEquals(3, matches);
  }

  @Test
  void unresolvableFieldPathYieldsAllMissingColumn() {
    final List<byte[]> leaves = buildLeaves(
        "/west/records/[]",
        "/west/records/[]/age",
        "/west/records/[]/no_such_field",
        "/west/records/[]/nested/city");
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    assertEquals(2, leaf.getRowCount());
    assertFalse(presentAt(leaf, 1, 0));
    assertFalse(presentAt(leaf, 1, 1));
    assertTrue(presentAt(leaf, 2, 1));
    assertEquals("NYC", stringAt(leaf, 2, 1));
  }
}
