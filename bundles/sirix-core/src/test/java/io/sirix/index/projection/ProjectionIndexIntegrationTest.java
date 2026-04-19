/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end round-trip: shred {@code abc-location-stations.json} into a
 * Sirix JSON resource, drive {@link ProjectionIndexBuilder} over the
 * resource to emit leaf pages for the projection
 * {@code /features/[]} ⟶ (type, geometry_name, properties/name),
 * then run {@link ProjectionIndexScan} over the emitted leaves and check
 * the count matches an rtx-walked reference count.
 *
 * <p>This is the "does the whole stack work?" test — it stitches together
 * the three pieces ({@code ProjectionIndexLeafPage}, {@code …Builder},
 * {@code …Scan}) without touching the IndexController plumbing yet. The
 * controller/listener wiring lands in a follow-up once we've established
 * the stack holds together on real data.
 */
final class ProjectionIndexIntegrationTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void buildScanCountsAllFeatures() {
    shredAbcLocations();

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {

      final var rootPath = parse("/features/[]", PathParser.Type.JSON);
      final var typePath = parse("/features/[]/type", PathParser.Type.JSON);
      final var geomNamePath = parse("/features/[]/geometry_name", PathParser.Type.JSON);
      final var namePath = parse("/features/[]/properties/name", PathParser.Type.JSON);

      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(
          rootPath,
          List.of(typePath, geomNamePath, namePath),
          List.of(Type.STR, Type.STR, Type.STR),
          0,
          IndexDef.DbType.JSON);

      final List<byte[]> leaves = new ArrayList<>();
      final var builder = new ProjectionIndexBuilder(indexDef, pathSummary, leaves::add);
      builder.build(rtx);

      // Unconditional count — every row materialised in a leaf must appear
      // on the scan side via the header short-circuit.
      final long scanRowCount = ProjectionIndexScan.countRows(leaves);
      assertEquals(builder.rowsEmitted(), scanRowCount,
          "countRows should see every emitted row");

      // Reference: count the records by walking the resource and matching
      // the root pathNodeKey. This is what the projection index is
      // materialising a covering projection of.
      final long referenceRecordCount = countFeaturesByPath(rtx, pathSummary, rootPath);
      assertEquals(referenceRecordCount, scanRowCount,
          "projection scan must see every /features/[] record");

      // Filter: type == "Feature" — true for every record (it's GeoJSON),
      // so the conjunctive count must equal the reference count.
      final var typeEqFeature = ProjectionIndexScan.ColumnPredicate.stringEq(
          0, "Feature".getBytes(StandardCharsets.UTF_8));
      assertEquals(referenceRecordCount,
          ProjectionIndexScan.conjunctiveCount(leaves, new ProjectionIndexScan.ColumnPredicate[] {typeEqFeature}),
          "every feature has type==Feature");

      // Filter: properties/name == "ABC Radio Adelaide" — exactly one
      // record matches in this dataset.
      final var nameEq = ProjectionIndexScan.ColumnPredicate.stringEq(
          2, "ABC Radio Adelaide".getBytes(StandardCharsets.UTF_8));
      assertEquals(1L,
          ProjectionIndexScan.conjunctiveCount(leaves, new ProjectionIndexScan.ColumnPredicate[] {nameEq}));

      // Conjunctive filter: type=="Feature" AND geometry_name=="geom" AND name=="ABC Radio Adelaide"
      // Still exactly one match.
      final var geomEqGeom = ProjectionIndexScan.ColumnPredicate.stringEq(
          1, "geom".getBytes(StandardCharsets.UTF_8));
      assertEquals(1L, ProjectionIndexScan.conjunctiveCount(leaves,
          new ProjectionIndexScan.ColumnPredicate[] {typeEqFeature, geomEqGeom, nameEq}));

      // Absent literal short-circuits via dictionary lookup.
      final var nameEqMissing = ProjectionIndexScan.ColumnPredicate.stringEq(
          2, "ThisStationDoesNotExist".getBytes(StandardCharsets.UTF_8));
      assertEquals(0L, ProjectionIndexScan.conjunctiveCount(leaves,
          new ProjectionIndexScan.ColumnPredicate[] {nameEqMissing}));

      assertTrue(builder.leavesEmitted() >= 1, "at least one leaf emitted");
    }
  }

  /**
   * Walk the resource and count nodes whose pathNodeKey matches the
   * declared projection root — the reference against which the scan's
   * row count is compared.
   */
  /**
   * Mirror of {@link ProjectionIndexBuilder}'s record-root semantics: when
   * the path matches an ARRAY, the rows are its children; otherwise the
   * matched node itself is the row.
   */
  private static long countFeaturesByPath(final JsonNodeReadOnlyTrx rtx,
      final io.sirix.index.path.summary.PathSummaryReader pathSummary, final Path<QNm> rootPath) {
    final long rootPathNodeKey = pathSummary.getPCRsForPath(rootPath).iterator().next();
    final long restore = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      long count = 0;
      final Axis axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        final NodeKind kind = rtx.getKind();
        if ((kind != NodeKind.OBJECT && kind != NodeKind.ARRAY && kind != NodeKind.OBJECT_KEY)
            || rtx.getPathNodeKey() != rootPathNodeKey) {
          continue;
        }
        final long matchKey = rtx.getNodeKey();
        if (kind == NodeKind.ARRAY && rtx.moveToFirstChild()) {
          do { count++; } while (rtx.moveToRightSibling());
        } else {
          count++;
        }
        rtx.moveTo(matchKey);
      }
      return count;
    } finally {
      rtx.moveTo(restore);
    }
  }

  private static void shredAbcLocations() {
    final var jsonPath = Paths.get("src", "test", "resources", "json", "abc-location-stations.json");
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      final var shredder = new JsonShredder.Builder(wtx, JsonShredder.createFileReader(jsonPath),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
      shredder.call();
    }
  }
}
