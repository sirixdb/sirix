package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionUtcProvenanceTest {
  @TempDir
  Path directory;

  @Test
  void repeatedNestedStructuredCellsPoisonUtcProvenanceInEitherOrder() {
    final Path dbPath = directory.resolve("utc");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder("rows").build());
      try (JsonResourceSession session = db.beginResourceSession("rows")) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"parts":[{"vf":"2024-04-01T00:00:00Z"},{"vf":{}}]},
               {"parts":[{"vf":{}},{"vf":"2024-04-01T00:00:00Z"}]},
               {"parts":[{"vf":"2024-04-01T00:00:00Z"}]}]
              """), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
            PathSummaryReader summary = session.openPathSummary()) {
          final IndexDef def = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/parts/[]/vf", PathParser.Type.JSON)), List.of(Type.DATI), 0, IndexDef.DbType.JSON);
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(def, summary);
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          for (int row = 0; row < 3; row++) {
            final long key = rtx.getNodeKey();
            assertTrue(extractor.extractInto(rtx, key));
            assertTrue(extractor.rowUnrepresentable(0), "text still requires the original Z suffix");
            if (row < 2) {
              assertTrue(extractor.rowNonDoubleSource(0), "multi-valued timestamps cannot supply a scalar UTC lane");
            } else {
              assertFalse(extractor.rowNonDoubleSource(0), "one canonical UTC cell is the positive control");
            }
            if (row < 2) {
              rtx.moveToRightSibling();
            }
          }
        }
      }
    }
  }
}
