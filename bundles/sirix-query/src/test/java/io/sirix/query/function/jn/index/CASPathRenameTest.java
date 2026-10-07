package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class CASPathRenameTest {
  @TempDir
  Path directory;

  static Stream<Arguments> scenarios() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(versioning -> Stream.of("number", "string", "boolean", "null", "object", "array")
                                              .map(kind -> Arguments.of(versioning, kind)));
  }

  @ParameterizedTest
  @MethodSource("scenarios")
  void inPlaceRenameKeepsPostingsAndCoverageCorrectAfterReopen(final VersioningType versioning, final String kind) {
    final Path databasePath = directory.resolve("rename");
    final String value = switch (kind) {
      case "number" -> "1";
      case "string" -> "\"one\"";
      case "boolean" -> "true";
      case "null" -> "null";
      case "object" -> "{}";
      case "array" -> "[]";
      default -> throw new IllegalArgumentException(kind);
    };
    final Type type = kind.equals("string")
        ? Type.STR
        : kind.equals("boolean")
            ? Type.BOOL
            : Type.INR;
    final boolean scalar = List.of("number", "string", "boolean").contains(kind);
    final String probe = kind.equals("string")
        ? "'one'"
        : kind.equals("boolean")
            ? "true()"
            : "1";
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"x\":" + value + "}]"),
            JsonNodeTrx.Commit.NO);
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(
                   Set.of(IndexDefs.createCASIdxDef(false, type, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
                       IndexDef.DbType.JSON), IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON)),
                   writer);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long key = writer.getNodeKey();
        final long pathKey = writer.getPathNodeKey();
        writer.commit();
        for (final String name : List.of("id", "x", "id")) {
          writer.moveTo(key);
          writer.setObjectKeyName(name);
          assertEquals(pathKey, writer.getPathNodeKey());
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var optimized = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final CompileChain generic = new CompileChain();
      final var session = store.lookup("rename").getDatabase().beginResourceSession("rows");
      for (int revision = 1; revision <= 4; revision++) {
        final int expected = scalar && revision % 2 == 0
            ? 1
            : 0;
        if (type.isNumeric()) {
          final IndexDef definition =
              session.getRtxIndexController(revision).getIndexes().getIndexDef(0, IndexType.CAS);
          assertEquals(kind.equals("number") || revision == 1, definition.hasNumericValuesOnly());
        }
        final String document = "jn:doc('rename','rows'," + revision + ")";
        assertEquals(expected,
            ((Numeric) new Query(optimized,
                "count(jn:scan-cas-index(" + document + ",0," + probe + ",'==','/[]/id'))").execute(
                    context)).intValue());
        if (scalar) {
          for (final String rows : List.of(document + "[][?$$.id eq " + probe + "]",
              "for $c in " + document + "[] where $c.id eq " + probe + " return $c")) {
            final String count = "count(" + rows + ")";
            assertEquals(expected, ((Numeric) new Query(generic, count).execute(context)).intValue(), count);
            assertEquals(expected, ((Numeric) new Query(optimized, count).execute(context)).intValue(), count);
          }
        }
      }
    }
  }
}
