package io.sirix.index;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.BulkJsonTreeAssembler;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.roaringbitmap.longlong.LongIterator;

import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASMixedNameMutationTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @CsvSource({"integer,false", "decimal,false", "double,false", "float,false", "integer,true"})
  void mixedIndexesKeepNumericPostingsAcrossMutationsAndReopen(final String kind, final boolean bulk) {
    final Type type = switch (kind) {
      case "integer" -> Type.INR;
      case "decimal" -> Type.DEC;
      case "double" -> Type.DBL;
      case "float" -> Type.FLO;
      default -> throw new IllegalArgumentException(kind);
    };
    final Path databasePath = directory.resolve("mixed");
    final IndexDef name = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    final long original;
    final long inserted;
    final long other;
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeNodeHistory(false)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final IndexDef casDefinition = IndexDefs.createCASIdxDef(false, type,
            Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
        final var controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(kind.equals("integer")
            ? Set.of(name, casDefinition)
            : Set.of(name), writer);
        final String json = "[{\"id\":1},{\"other\":20000000000}]";
        if (bulk) {
          BulkJsonTreeAssembler.assemble(writer, new StringReader(json));
        } else {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        }
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        writer.moveToFirstChild();
        original = writer.getNodeKey();
        if (!kind.equals("integer")) {
          writer.setNumberValue(number(kind, 1));
          controller.createIndexes(Set.of(casDefinition), writer);
        }
        writer.moveTo(first);
        writer.moveToRightSibling();
        final long second = writer.getNodeKey();
        writer.moveToFirstChild();
        other = writer.getNodeKey();
        writer.commit();
        writer.moveTo(original);
        writer.setNumberValue(number(kind, 2));
        writer.commit();
        writer.moveTo(first);
        writer.insertObjectRecordAsFirstChild("id", new NumberValue(number(kind, 3)));
        inserted = writer.getNodeKey();
        writer.commit();
        writer.moveTo(second);
        writer.moveSubtreeToFirstChild(inserted);
        writer.commit();
        writer.moveTo(original);
        writer.remove();
        writer.commit();
        writer.moveTo(inserted);
        writer.remove();
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      for (int revision = 1; revision <= 6; revision++) {
        try (var reader = session.beginNodeReadOnlyTrx(revision)) {
          final var controller = session.getRtxIndexController(revision);
          final IndexDef nameDefinition = controller.getIndexes().getIndexDef(name.getID(), IndexType.NAME);
          final Set<Long> expected = revision <= 2
              ? Set.of(original)
              : revision <= 4
                  ? Set.of(original, inserted)
                  : revision == 5
                      ? Set.of(inserted)
                      : Set.of();
          assertEquals(expected, keys(controller.openNameIndex(reader.getStorageEngineReader(), nameDefinition,
              controller.createNameFilter(Set.of("id")))), kind + " revision " + revision);
          assertEquals(Set.of(other), keys(controller.openNameIndex(reader.getStorageEngineReader(), nameDefinition,
              controller.createNameFilter(Set.of("other")))));
          final IndexDef cas = controller.getIndexes().getIndexDef(0, IndexType.CAS);
          assertTrue(cas.hasNumericValuesOnly());
          assertTrue(cas.hasCompleteNumericCoverage());
          for (int value = 1; value <= 3; value++) {
            final Set<Long> matches = value == 1 && revision == 1
                ? Set.of(original)
                : value == 2 && revision >= 2 && revision <= 4
                    ? Set.of(original)
                    : value == 3 && revision >= 3 && revision <= 5
                        ? Set.of(inserted)
                        : Set.of();
            assertEquals(matches,
                keys(controller.openCASIndex(reader.getStorageEngineReader(), cas,
                    controller.createCASFilter(Set.of("/[]/id"),
                        AtomicUtil.toType(AtomicUtil.fromNumber(number(kind, value)), type), SearchMode.EQUAL,
                        new JsonPCRCollector(reader)))),
                kind + " revision " + revision + " value " + value);
          }
        }
      }
    }
  }

  private static Set<Long> keys(final Iterator<NodeReferences> postings) {
    final Set<Long> keys = new TreeSet<>();
    while (postings.hasNext()) {
      final LongIterator iterator = postings.next().getNodeKeys().getLongIterator();
      while (iterator.hasNext()) {
        keys.add(iterator.next());
      }
    }
    return keys;
  }

  private static Number number(final String kind, final int value) {
    return switch (kind) {
      case "integer" -> Integer.valueOf(value);
      case "decimal" -> new BigDecimal(value + ".0");
      case "double" -> Double.valueOf(value);
      case "float" -> Float.valueOf(value);
      default -> throw new IllegalArgumentException(kind);
    };
  }
}
