package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class CASMixedValidTimeMutationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"vf,false", "vf,true", "vt,false", "vt,true"})
  void numericDuplicateBoundsKeepRoutedAndFallbackHistoryEqual(final String field, final boolean array) {
    final Path databasePath = directory.resolve("mixed");
    final long row;
    final long duplicate;
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final String object = "{\"id\":1,\"vf\":\"2020-01-01T00:00:00Z\",\"vt\":\"2021-01-01T00:00:00Z\"}";
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(array
            ? "[" + object + "]"
            : object), JsonNodeTrx.Commit.NO);
        final String prefix = array
            ? "/[]/"
            : "/";
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(
                   IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse(prefix + "id", PathParser.Type.JSON)), 0,
                       IndexDef.DbType.JSON),
                   IndexDefs.createValidTimeIdxDef(
                       Set.of(parse(prefix + "vf", PathParser.Type.JSON), parse(prefix + "vt", PathParser.Type.JSON)),
                       0, IndexDef.DbType.JSON)),
                   writer);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        if (array) {
          writer.moveToFirstChild();
        }
        row = writer.getNodeKey();
        writer.commit();
        writer.moveTo(row);
        writer.insertObjectRecordAsFirstChild(field, new NumberValue(0));
        duplicate = writer.getNodeKey();
        writer.commit();
        writer.moveTo(duplicate);
        writer.setNumberValue(1.5d);
        writer.commit();
        writer.moveTo(row);
        writer.moveToLastChild();
        writer.moveSubtreeToRightSibling(duplicate);
        writer.commit();
        writer.moveTo(row);
        writer.moveSubtreeToFirstChild(duplicate);
        writer.commit();
        writer.moveTo(duplicate);
        writer.remove();
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    final Instant point = Instant.parse(field.equals("vf")
        ? "2019-01-01T00:00:00Z"
        : "2022-01-01T00:00:00Z");
    for (int revision = 1; revision <= 6; revision++) {
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var context = SirixQueryContext.createWithJsonStore(store);
          var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
        final var collection = requireNonNull(store.lookup("mixed"));
        final var document = requireNonNull(collection.getDocument("rows", revision));
        final var config = requireNonNull(document.getResourceSession().getResourceConfig().getValidTimeConfig());
        final List<Long> expected = revision == 2 || revision == 3 || revision == 5
            ? List.of(row)
            : List.of();
        assertEquals(expected, keys(ValidTimeFilter.linearScanSequence(document, point, config)));
        final Sequence routed = ValidTimeIntervalIndex.sequence(document, point, config, false, false);
        assertNotNull(routed);
        assertEquals(expected, keys(routed), field + " revision " + revision);
        assertEquals(expected, keys(new Query(chain,
            "jn:scan-valid-time-index(jn:doc('mixed','rows'," + revision + "),xs:dateTime('" + point + "'))").execute(
                context)));
      }
      Databases.clearGlobalCaches();
    }
  }

  private static List<Long> keys(final @Nullable Sequence sequence) {
    final List<Long> keys = new ArrayList<>();
    if (sequence != null) {
      try (Iter iterator = sequence.iterate()) {
        Item item;
        while ((item = iterator.next()) != null) {
          keys.add(((JsonDBItem) item).getNodeKey());
        }
      }
    }
    return keys;
  }
}
