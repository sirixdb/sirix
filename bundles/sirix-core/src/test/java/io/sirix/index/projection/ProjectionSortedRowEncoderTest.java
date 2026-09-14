/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
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
import io.sirix.index.ProjectionSortedSpec;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.brackit.query.util.path.Path.parse;

final class ProjectionSortedRowEncoderTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void exactFilterUsesExtractorValuesAndKeyOrderUsesUtf8NotDictionaryIds() {
    final Path databasePath = temporaryDirectory.resolve("sorted-encoder");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"z","time":20},
               {"kind":"identity","op":"create","did":"b","time":10},
               {"kind":"commit","op":"create","did":"a","time":30},
               {"kind":"commit","op":"create","time":5},
               {"kind":"commit","op":"create","did":"q","time":5.5}]
              """), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx();
            PathSummaryReader pathSummary = session.openPathSummary()) {
          final IndexDef definition = IndexDefs.createProjectionIdxDef(
              parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/kind", PathParser.Type.JSON),
                  parse("/[]/op", PathParser.Type.JSON),
                  parse("/[]/did", PathParser.Type.JSON),
                  parse("/[]/time", PathParser.Type.JSON)),
              List.of(Type.STR, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
              new ProjectionSortedSpec(List.of(2, 3),
                  List.of(new ProjectionSortedSpec.Equality(0, "commit"),
                      new ProjectionSortedSpec.Equality(1, "create"))));
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(definition, pathSummary);
          final ProjectionSortedRowEncoder encoder = new ProjectionSortedRowEncoder(definition, extractor);
          final long[] records = new long[5];
          assertTrue(reader.moveToDocumentRoot());
          assertTrue(reader.moveToFirstChild());
          assertTrue(reader.moveToFirstChild());
          for (int i = 0; i < records.length; i++) {
            records[i] = reader.getNodeKey();
            if (i + 1 < records.length) {
              assertTrue(reader.moveToRightSibling());
            }
          }
          assertTrue(extractor.extractInto(reader, records[0]));
          assertTrue(encoder.writeKeyIfMatching(records[0]));
          final byte[] zKey = encoder.copyKey();
          assertEquals(zKey.length, encoder.keyLength());
          assertTrue(extractor.extractInto(reader, records[1]));
          assertFalse(encoder.writeKeyIfMatching(records[1]));
          assertTrue(extractor.extractInto(reader, records[2]));
          assertTrue(encoder.writeKeyIfMatching(records[2]));
          final byte[] aKey = encoder.copyKey();
          assertTrue(Arrays.compareUnsigned(aKey, zKey) < 0);
          assertTrue(extractor.extractInto(reader, records[3]));
          assertTrue(encoder.writeKeyIfMatching(records[3]));
          final byte[] missingKey = encoder.copyKey();
          assertTrue(Arrays.compareUnsigned(missingKey, aKey) < 0);
          assertTrue(extractor.extractInto(reader, records[4]));
          assertThrows(IllegalStateException.class, () -> encoder.writeKeyIfMatching(records[4]));
        }
      }
    }
  }
}
