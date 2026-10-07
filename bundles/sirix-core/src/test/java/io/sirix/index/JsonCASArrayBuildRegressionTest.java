package io.sirix.index;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonCASArrayBuildRegressionTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void completeTreeBuilderIndexesOrdinaryArrayValues(final VersioningType versioning) {
    final Path location = directory.resolve("source");
    Databases.createJsonDatabase(new DatabaseConfiguration(location));
    try (final var database = Databases.openJsonDatabase(location)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .build());
      try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "[{\"dept\":\"A\",\"tags\":[\"a\",\"b\"]},{\"dept\":\"B\",\"tags\":[\"b\"]}]"), JsonNodeTrx.Commit.NO);
        writer.commit();
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set
                                 .of(IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.JSON),
                                     IndexDefs.createCASIdxDef(false, Type.STR,
                                         Set.of(parse("/[]/tags/[]", PathParser.Type.JSON)), 1, IndexDef.DbType.JSON)),
                   writer);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (final var database = Databases.openJsonDatabase(location);
        final var session = database.beginResourceSession("resource");
        final var reader = session.beginNodeReadOnlyTrx()) {
      final var controller = session.getRtxIndexController(reader.getRevisionNumber());
      for (int id = 0; id < 2; id++) {
        final var definition = controller.getIndexes().getIndexDef(id, IndexType.CAS);
        assertEquals(LongSet.of(6, 10),
            collect(controller.openCASIndex(reader.getStorageEngineReader(), definition, controller.createCASFilter(
                Set.of("/[]/tags/[]"), new Str("b"), SearchMode.EQUAL, new JsonPCRCollector(reader)))));
      }
      assertTrue(reader.moveTo(6));
      assertEquals("b", reader.getValue());
      assertTrue(reader.moveTo(10));
      assertEquals("b", reader.getValue());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void sharedVisitorsPreserveEveryPrimitiveValue(final VersioningType versioning) {
    for (final boolean namedArray : new boolean[] {false, true}) {
      final Path location = directory.resolve("mixed-" + namedArray);
      final String path = namedArray
          ? "/tags/[]"
          : "/[]";
      final String array = "[\"repeat\",42,true,\"repeat\",false,42]";
      Databases.createJsonDatabase(new DatabaseConfiguration(location));
      try (final var database = Databases.openJsonDatabase(location)) {
        database.createResource(ResourceConfiguration.newBuilder("resource")
                                                     .storageType(StorageType.FILE_CHANNEL)
                                                     .versioningApproach(versioning)
                                                     .build());
        try (final var session = database.beginResourceSession("resource"); final var writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(namedArray
              ? "{\"tags\":" + array + "}"
              : array), JsonNodeTrx.Commit.NO);
          writer.commit();
          final var controller = (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
          controller.createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, IndexDef.DbType.JSON),
              IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse(path, PathParser.Type.JSON)), 1,
                  IndexDef.DbType.JSON),
              IndexDefs.createPathIdxDef(Set.of(), 0, IndexDef.DbType.JSON),
              IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON)), writer);
          assertPrimitivePostings(controller, writer, path);
          writer.commit();
        }
      }
      Databases.clearGlobalCaches();
      try (final var database = Databases.openJsonDatabase(location);
          final var session = database.beginResourceSession("resource");
          final var reader = session.beginNodeReadOnlyTrx()) {
        assertPrimitivePostings(session.getRtxIndexController(reader.getRevisionNumber()), reader, path);
      }
    }
  }

  private static void assertPrimitivePostings(final JsonIndexController controller, final JsonNodeReadOnlyTrx reader,
      final String path) {
    for (final String value : new String[] {"repeat", "42", "true", "false"}) {
      final LongSet expected = new LongOpenHashSet();
      reader.moveToDocumentRoot();
      final var axis = new DescendantAxis(reader);
      while (axis.hasNext()) {
        axis.nextLong();
        final String actual = switch (reader.getKind()) {
          case STRING_VALUE -> reader.getValue();
          case NUMBER_VALUE -> reader.getNumberValue().toString();
          case BOOLEAN_VALUE -> Boolean.toString(reader.getBooleanValue());
          default -> null;
        };
        if (value.equals(actual)) {
          expected.add(reader.getNodeKey());
        }
      }
      assertEquals(value.equals("true") || value.equals("false")
          ? 1
          : 2, expected.size());
      for (int id = 0; id < 2; id++) {
        assertEquals(expected,
            collect(controller.openCASIndex(reader.getStorageEngineReader(),
                controller.getIndexes().getIndexDef(id, IndexType.CAS), controller.createCASFilter(Set.of(path),
                    new Str(value), SearchMode.EQUAL, new JsonPCRCollector(reader)))),
            "index " + id + " value " + value);
      }
    }
  }

  private static LongSet collect(final Iterator<NodeReferences> references) {
    final LongSet keys = new LongOpenHashSet();
    while (references.hasNext()) {
      final var iterator = references.next().getNodeKeys().getLongIterator();
      while (iterator.hasNext()) {
        keys.add(iterator.next());
      }
    }
    return keys;
  }
}
