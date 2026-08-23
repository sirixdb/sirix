/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ForwardingJsonNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.PrimitiveNumberCursor;
import io.sirix.access.trx.node.json.objectvalue.PrimitiveNumberValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.node.SirixDeweyID;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Projection parity and cold-read coverage for the unboxed fused integral-number path. */
final class ProjectionIndexPrimitiveNumberReadTest {

  private static final String RESOURCE = "projection-primitive-number-read";
  private static final String JSON = """
      [
        {
          "i": -2147483648,
          "l": 2147483648,
          "d": 1.25e0,
          "bi": 9223372036854775808,
          "bd": 1.25,
          "id": -2147483648,
          "ld": 2147483648,
          "dl": 1.25e0
        },
        {
          "i": 2147483647,
          "l": -9223372036854775808,
          "d": -2.5e0,
          "bi": -9223372036854775809,
          "bd": 0.1,
          "id": 2147483647,
          "ld": 9007199254740993,
          "dl": -2.5e0
        }
      ]
      """;

  private static final List<String> FIELD_NAMES = List.of("i", "l", "d", "bi", "bd", "id", "ld", "dl");
  private static final List<Type> FIELD_TYPES =
      List.of(Type.LON, Type.LON, Type.DBL, Type.LON, Type.DBL, Type.DBL, Type.DBL, Type.LON);

  @TempDir
  Path temporaryDirectory;

  private Path databasePath;

  @BeforeEach
  void setUp() throws Exception {
    databasePath = temporaryDirectory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeDiffs(false)
                                                   .buildPathSummary(true)
                                                   .buildPathStatistics(false)
                                                   .useDeweyIDs(true)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          var wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser(JSON)) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }
    }
    // Every assertion below starts from a cache-cold database reopen.
    Databases.clearGlobalCaches();
  }

  @AfterEach
  void tearDown() {
    Databases.clearGlobalCaches();
  }

  @Test
  void coldCursorReturnsIntegralTagsAndLeavesFallbackKindsUntouched() {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild(), "missing root array after cold reopen");
      assertTrue(rtx.moveToFirstChild(), "missing first record after cold reopen");
      assertTrue(rtx.moveToFirstChild(), "missing first fused field after cold reopen");
      final PrimitiveNumberCursor primitiveCursor = assertInstanceOf(PrimitiveNumberCursor.class, rtx);
      final long[] value = {0x1234_5678_9ABC_DEF0L};

      assertEquals("i", rtx.getName().getLocalName());
      assertEquals(PrimitiveNumberValue.INT, primitiveCursor.readFusedPrimitiveNumber(value, 0));
      assertEquals(Integer.MIN_VALUE, value[0]);

      assertTrue(rtx.moveToRightSibling());
      assertEquals("l", rtx.getName().getLocalName());
      assertEquals(PrimitiveNumberValue.LONG, primitiveCursor.readFusedPrimitiveNumber(value, 0));
      assertEquals(2_147_483_648L, value[0]);

      assertTrue(rtx.moveToRightSibling());
      assertEquals("d", rtx.getName().getLocalName());
      value[0] = 0x1234_5678_9ABC_DEF0L;
      assertEquals(PrimitiveNumberValue.NONE, primitiveCursor.readFusedPrimitiveNumber(value, 0));
      assertEquals(0x1234_5678_9ABC_DEF0L, value[0], "declined reads must not mutate caller scratch");
      assertInstanceOf(Double.class, rtx.getNumberValue());

      assertTrue(rtx.moveToRightSibling());
      assertEquals("bi", rtx.getName().getLocalName());
      assertEquals(PrimitiveNumberValue.NONE, primitiveCursor.readFusedPrimitiveNumber(value, 0));
      assertInstanceOf(BigInteger.class, rtx.getNumberValue());

      assertTrue(rtx.moveToRightSibling());
      assertEquals("bd", rtx.getName().getLocalName());
      assertEquals(PrimitiveNumberValue.NONE, primitiveCursor.readFusedPrimitiveNumber(value, 0));
      assertInstanceOf(BigDecimal.class, rtx.getNumberValue());
    }
  }

  @Test
  void loadTimeProjectionSupportsDeweyDisabledResource() throws Exception {
    final String disabled = "projection-dewey-disabled";
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(disabled).useDeweyIDs(false).build()));
      final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/i", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
      try (JsonResourceSession session = database.beginResourceSession(disabled);
          var wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser("[{\"i\":1}]")) {
        assertFalse(session.getResourceConfig().areDeweyIDsStored);
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        final ProjectionBulkLoad load = controller.createProjectionIndexAtLoadStart(definition, wtx, 1L);
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).build().call();
        wtx.commit();
        assertTrue(load.isFinished());
      }

      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
      try (JsonResourceSession session = database.beginResourceSession(disabled);
          var rtx = session.beginNodeReadOnlyTrx()) {
        assertFalse(session.getResourceConfig().areDeweyIDsStored);
        final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
        assertNotNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
        assertTrue(controller.hasProjectionIndex());
        final ProjectionIndexRegistry.Handle handle =
            ProjectionIndexCatalog.load(session, rtx.getRevisionNumber(), definition);
        assertNotNull(handle);
        final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(session,
            rtx.getRevisionNumber(), definition.getID(), handle.rowGroupCount()));
        assertEquals(1, leaves.size());
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(leaves.getFirst());
        assertEquals(1, page.getRowCount());
        assertEquals(1L, page.numericColumn(0)[0]);
      }
    }
  }

  @Test
  void primitiveAndBoxedExtractionAreByteIdenticalWithExactProvenance() {
    final BuildResult primitive = buildProjection(false);
    final BuildResult boxed = buildProjection(true);

    assertEquals(8, primitive.boxedNumberReads,
        "only Double/BigInteger/BigDecimal values should take the Number fallback");
    assertEquals(16, boxed.boxedNumberReads, "forced fallback should materialize every numeric field");
    assertArrayEquals(boxed.payload, primitive.payload,
        "unboxed INT/LONG extraction must preserve the complete projection wire");

    final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(primitive.payload);
    assertEquals(2, page.getRowCount());
    assertArrayEquals(new long[] {Integer.MIN_VALUE, Integer.MAX_VALUE}, liveValues(page, 0));
    assertArrayEquals(new long[] {2_147_483_648L, Long.MIN_VALUE}, liveValues(page, 1));
    assertArrayEquals(encoded(1.25d, -2.5d), liveValues(page, 2));
    assertArrayEquals(new long[] {Long.MIN_VALUE, Long.MAX_VALUE}, liveValues(page, 3));
    assertArrayEquals(encoded(1.25d, 0.1d), liveValues(page, 4));
    assertArrayEquals(encoded((double) Integer.MIN_VALUE, (double) Integer.MAX_VALUE), liveValues(page, 5));
    assertArrayEquals(encoded(2_147_483_648d, (double) 9_007_199_254_740_993L), liveValues(page, 6));
    assertArrayEquals(new long[] {1L, -2L}, liveValues(page, 7));

    assertFalse(page.columnNumericNonIntegral(0));
    assertFalse(page.columnNumericNonIntegral(1));
    assertFalse(page.columnNumericNonIntegral(2));
    assertTrue(page.columnNumericNonIntegral(3), "out-of-range BigInteger must poison exact long serving");
    assertTrue(page.columnNumericNonIntegral(4), "inexact BigDecimal-to-double must poison exact serving");
    assertFalse(page.columnNumericNonIntegral(5), "all int-to-double conversions are exact");
    assertTrue(page.columnNumericNonIntegral(6), "2^53+1 long-to-double conversion must be marked lossy");
    assertTrue(page.columnNumericNonIntegral(7), "fractional Double-to-long conversion must be marked");

    assertTrue(page.columnPureDoubleSource(2));
    assertFalse(page.columnPureDoubleSource(4));
    assertFalse(page.columnPureDoubleSource(5));
    assertFalse(page.columnPureDoubleSource(6));
    for (int column = 0; column < FIELD_NAMES.size(); column++) {
      assertFalse(page.columnUnrepresentable(column),
          "numeric column unexpectedly declined: " + FIELD_NAMES.get(column));
    }
  }

  private BuildResult buildProjection(final boolean forceBoxedFallback) {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx delegate = session.beginNodeReadOnlyTrx();
        var pathSummary = session.openPathSummary()) {
      final int[] boxedNumberReads = new int[1];
      final JsonNodeReadOnlyTrx cursor = new ForwardingJsonNodeReadOnlyTrx() {
        @Override
        public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
          return delegate;
        }

        @Override
        public SirixDeweyID getDeweyID() {
          return delegate.getDeweyID();
        }

        @Override
        public byte readFusedPrimitiveNumber(final long[] valueOut, final int index) {
          return forceBoxedFallback
              ? PrimitiveNumberValue.NONE
              : ForwardingJsonNodeReadOnlyTrx.super.readFusedPrimitiveNumber(valueOut, index);
        }

        @Override
        public Number getNumberValue() {
          boxedNumberReads[0]++;
          return delegate.getNumberValue();
        }
      };

      final var fields = FIELD_NAMES.stream().map(name -> parse("/[]/" + name, PathParser.Type.JSON)).toList();
      final IndexDef def = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON), fields, FIELD_TYPES, 0,
          IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(cursor);
      assertEquals(1, leaves.size());
      return new BuildResult(leaves.getFirst(), boxedNumberReads[0]);
    }
  }

  private static long[] liveValues(final ProjectionIndexRowGroupPage page, final int column) {
    final long[] values = page.numericColumn(column);
    return new long[] {values[0], values[1]};
  }

  private static long[] encoded(final double first, final double second) {
    return new long[] {ProjectionDoubleEncoding.encode(first), ProjectionDoubleEncoding.encode(second)};
  }

  private record BuildResult(byte[] payload, int boxedNumberReads) {
  }
}
