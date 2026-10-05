package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimePendingIndexDropTest {
  private static final Instant INSTANT = Instant.parse("2024-01-01T00:00:00Z");
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final String TRANSACTION = "xs:dateTime('2099-01-01T00:00:00Z')";

  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"CAS,vf,false", "CAS,vf,true", "CAS,vt,false", "CAS,vt,true", "PROJECTION,vf,false", "PROJECTION,vf,true",
      "PROJECTION,vt,false", "PROJECTION,vt,true", "VALIDTIME,vf,false", "VALIDTIME,vf,true", "VALIDTIME,vt,false",
      "VALIDTIME,vt,true"})
  void pendingEndpointEditsSurviveIndexDrops(final IndexType drop, final String field, final boolean addMatch) {
    final String outside = field.equals("vf")
        ? "2025-01-01T00:00:00Z"
        : "2023-06-01T00:00:00Z";
    final String inside = field.equals("vf")
        ? "2023-01-01T00:00:00Z"
        : "2025-01-01T00:00:00Z";
    final String from = field.equals("vf")
        ? (addMatch
            ? outside
            : inside)
        : "2023-01-01T00:00:00Z";
    final String to = field.equals("vt")
        ? (addMatch
            ? outside
            : inside)
        : "2026-01-01T00:00:00Z";
    final int oldCount = addMatch
        ? 0
        : 1;
    final int newCount = addMatch
        ? 1
        : 0;
    exerciseDrop(drop, field, from, to, addMatch
        ? inside
        : outside, new int[] {oldCount, oldCount, oldCount, oldCount},
        new int[] {newCount, newCount, newCount, newCount}, true);
  }

  @ParameterizedTest
  @CsvSource({"CAS,vf", "CAS,vt", "PROJECTION,vf", "PROJECTION,vt", "VALIDTIME,vf", "VALIDTIME,vt"})
  void pendingPrecisionChangesPublishVerificationEvidenceBeforeDrop(final IndexType drop, final String field) {
    final boolean start = field.equals("vf");
    exerciseDrop(drop, field, start
        ? INSTANT.toString()
        : "2023-01-01T00:00:00Z",
        start
            ? "2025-01-01T00:00:00Z"
            : INSTANT.toString(),
        "2024-01-01T00:00:00.000500Z", start
            ? new int[] {1, 0, 1, 0}
            : new int[] {1, 1, 0, 0},
        start
            ? new int[] {0, 0, 0, 0}
            : new int[] {1, 1, 1, 1},
        false);
  }

  private void exerciseDrop(final IndexType drop, final String field, final String from, final String to,
      final String replacement, final int[] oldCounts, final int[] newCounts, final boolean exact) {
    final Path databasePath = directory.resolve("pending");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final Set<IndexDef> validTimes = Set.of(validTime(0), validTime(1));
    final IndexDef dropped = switch (drop) {
      case CAS -> IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
          IndexDef.DbType.JSON);
      case PROJECTION -> IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/id", PathParser.Type.JSON)), List.of(Type.INR), 0, IndexDef.DbType.JSON);
      case VALIDTIME -> validTime(0);
      default -> throw new IllegalArgumentException("Unsupported drop fixture: " + drop);
    };
    final long objectKey;
    final long fieldKey;
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"id\":1,\"vf\":\"" + from + "\",\"vt\":\"" + to + "\"}]"),
            JsonNodeTrx.Commit.NO);
        final var controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(validTimes, writer);
        if (drop != IndexType.VALIDTIME) {
          controller.createIndexes(Set.of(dropped), writer);
        }
        writer.moveToDocumentRoot();
        assertTrue(writer.moveToFirstChild());
        assertTrue(writer.moveToFirstChild());
        objectKey = writer.getNodeKey();
        assertTrue(writer.moveToFirstChild());
        while (!field.equals(writer.getName().getLocalName())) {
          assertTrue(writer.moveToRightSibling());
        }
        fieldKey = writer.getNodeKey();
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("pending");
      final JsonDBItem historical = collection.getDocument("rows", 1);
      final var session = historical.getResourceSession();
      try (var writer = session.beginNodeTrx()) {
        assertTrue(writer.moveTo(fieldKey));
        writer.setStringValue(replacement);
        if (drop == IndexType.CAS) {
          session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(dropped), writer);
        } else {
          new Query(chain, "jn:drop-" + (drop == IndexType.PROJECTION
              ? "projection"
              : "valid-time") + "-index(jn:doc('pending','rows',1),0)").evaluate(context);
        }
        writer.commit();
      }
      assertRevision(chain, context, collection.getDocument("rows", 2), objectKey, newCounts, exact);
      assertRevision(chain, context, historical, objectKey, oldCounts, true);
      assertNull(session.getRtxIndexController(2).getIndexes().getIndexDef(0, drop));
      assertNotNull(session.getRtxIndexController(1).getIndexes().getIndexDef(0, drop));
      assertNotNull(session.getRtxIndexController(2).getIndexes().getIndexDef(1, IndexType.VALIDTIME));
      assertCount(chain, context, "jn:valid-at('pending','rows'," + POINT + ")", newCounts[0]);
      final String source = "jn:open-bitemporal('pending','rows'," + TRANSACTION + "," + POINT + ")";
      for (int mode = 0; mode < 4; mode++) {
        assertCount(chain, context, "for $x in " + source + " where " + comparison(mode) + " return $x",
            newCounts[mode | 2]);
      }
      final String differentPoint = "xs:dateTime('2024-01-01T00:00:00.000250Z')";
      final String residual = field.equals("vf")
          ? "xs:dateTime($x.vf) lt " + differentPoint
          : differentPoint + " lt xs:dateTime($x.vt)";
      assertCount(chain, context, "for $x in " + source + " where " + residual + " return $x", newCounts[2]);
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("pending");
      assertRevision(chain, context, collection.getDocument("rows", 2), objectKey, newCounts, exact);
      assertRevision(chain, context, collection.getDocument("rows", 1), objectKey, oldCounts, true);
    }
  }

  private static IndexDef validTime(final int id) {
    return IndexDefs.createValidTimeIdxDef(
        Set.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON)), id, IndexDef.DbType.JSON);
  }

  private static String comparison(final int mode) {
    return "xs:dateTime($x.vf) " + ((mode & 1) == 0
        ? "le"
        : "lt") + " " + POINT + " and " + POINT + " "
        + ((mode & 2) == 0
            ? "le"
            : "lt")
        + " xs:dateTime($x.vt)";
  }

  private static void assertRevision(final SirixCompileChain chain, final SirixQueryContext context,
      final JsonDBItem document, final long objectKey, final int[] counts, final boolean exact) {
    final var config = document.getResourceSession().getResourceConfig().getValidTimeConfig();
    final String source = "jn:doc('pending','rows'," + document.getTrx().getRevisionNumber() + ")";
    for (int mode = 0; mode < 4; mode++) {
      final Sequence sequence = Objects.requireNonNull(
          ValidTimeIntervalIndex.sequence(document, INSTANT, config, (mode & 1) != 0, (mode & 2) != 0));
      if (exact) {
        assertEquals(counts[mode], Objects.requireNonNull(sequence.knownSize()).intValue());
      }
      assertEquals(counts[mode], sequence.size().intValue());
      if (counts[mode] == 0) {
        assertNull(sequence.get(Int32.ONE));
      } else {
        assertEquals(1, ((Numeric) ((Object) sequence.get(Int32.ONE)).get(new QNm("id"))).intValue());
      }
      assertNull(sequence.get(new Int32(2)));
      try (var iterator = sequence.iterate()) {
        final Item item = iterator.next();
        if (counts[mode] == 0) {
          assertNull(item);
        } else {
          assertEquals(1, ((Numeric) ((Object) item).get(new QNm("id"))).intValue());
        }
        assertNull(iterator.next());
      }
      final String wrapped = "declare function local:slice($p as xs:dateTime) { for $x in " + source
          + "[] where " + comparison(mode).replace(POINT, "$p") + " return $x }; count(local:slice(" + POINT + "))";
      assertEquals(counts[mode], ((Numeric) new Query(chain, wrapped).evaluate(context)).intValue(), wrapped);
      assertCount(chain, context, "for $x in " + source + "[] where " + comparison(mode) + " return $x", counts[mode]);
    }
    assertCount(chain, context, "jn:scan-valid-time-index(" + source + "," + POINT + ")", counts[0]);
    for (final boolean strictEnd : new boolean[] {false, true}) {
      assertArrayEquals(counts[strictEnd
          ? 2
          : 0] == 0
              ? new long[0]
              : new long[] {objectKey},
          ValidTimeIntervalIndex.keys(document, INSTANT, strictEnd));
    }
    if (exact) {
      for (final IndexDef definition : document.getResourceSession()
                                               .getRtxIndexController(document.getTrx().getRevisionNumber())
                                               .getIndexes()
                                               .getIndexDefs()) {
        if (definition.isValidTimeIndex()) {
          final IntervalDomain domain = new IntervalDomain();
          final LongOpenHashSet matches = new LongOpenHashSet();
          ValidTimeIntervalIndexFactory.createReaderTree(document.getTrx().getStorageEngineReader(), definition.getID(),
              domain).stab(domain.point(INSTANT), matches::add);
          assertEquals(counts[0] == 0
              ? new LongOpenHashSet()
              : LongOpenHashSet.of(objectKey), matches);
        }
      }
    }
  }

  private static void assertCount(final SirixCompileChain chain, final SirixQueryContext context,
      final String expression, final int expected) {
    assertEquals(expected, ((Numeric) new Query(chain, "count(" + expression + ")").evaluate(context)).intValue(),
        expression);
  }
}
