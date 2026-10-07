package io.sirix.index;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASNumericMutationEvidenceTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"decimal", "double", "float"})
  void numericInsertUpdateAndDeleteKeepPersistedEvidence(final String kind) {
    final Type type = switch (kind) {
      case "decimal" -> Type.DEC;
      case "double" -> Type.DBL;
      case "float" -> Type.FLO;
      default -> throw new IllegalArgumentException(kind);
    };
    final Path databasePath = directory.resolve("mutations");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"id\":1}]"), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        final long arrayKey = writer.getNodeKey();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long idKey = writer.getNodeKey();
        writer.setNumberValue(number(kind, 1));
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(
            IndexDefs.createCASIdxDef(false, type, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit();
        writer.moveTo(idKey);
        writer.setNumberValue(number(kind, 2));
        writer.commit();
        writer.moveTo(arrayKey);
        writer.insertObjectAsLastChild();
        final long inserted = writer.getNodeKey();
        writer.insertObjectRecordAsFirstChild("id", new NumberValue(number(kind, 3)));
        writer.commit();
        writer.moveTo(inserted);
        writer.remove();
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var database = Databases.openJsonDatabase(databasePath); var session = database.beginResourceSession("rows")) {
      for (final int revision : List.of(1, 2, 3, 4)) {
        final IndexDef definition = session.getRtxIndexController(revision).getIndexes().getIndexDef(0, IndexType.CAS);
        assertTrue(definition.hasNumericValuesOnly(), kind + " revision " + revision);
        assertTrue(definition.hasCompleteNumericCoverage(), kind + " revision " + revision);
      }
    }
  }

  private static Number number(final String kind, final int value) {
    return switch (kind) {
      case "decimal" -> new BigDecimal(value + ".0");
      case "double" -> Double.valueOf(value);
      case "float" -> Float.valueOf(value);
      default -> throw new IllegalArgumentException(kind);
    };
  }
}
