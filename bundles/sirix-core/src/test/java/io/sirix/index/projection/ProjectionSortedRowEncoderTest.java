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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static io.brackit.query.util.path.Path.parse;

final class ProjectionSortedRowEncoderTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void everyRowHasOneKeyOrderedByUtf8AndUnrepresentableRowsGetTheReservedKey() {
    final Path databasePath = temporaryDirectory.resolve("sorted-encoder");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final String longDid = "x".repeat(ProjectionSortKeyCodec.MAX_KEY_BYTES + 1);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"z","time":20},
               {"kind":"identity","op":"create","did":"b","time":10},
               {"kind":"commit","op":"create","did":"a","time":30},
               {"kind":"commit","op":"create","time":5},
               {"kind":"commit","op":"create","did":"q","time":5.5},
               {"kind":"commit","op":"create","did":5,"time":7},
               {"kind":"commit","op":"create","did":"%s","time":8}]
              """.formatted(longDid)), JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx();
            PathSummaryReader pathSummary = session.openPathSummary()) {
          final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/op", PathParser.Type.JSON),
                  parse("/[]/did", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON)),
              List.of(Type.STR, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
              new ProjectionSortedSpec(List.of(0, 1, 2, 3)));
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(definition, pathSummary);
          final ProjectionSortedRowEncoder encoder = new ProjectionSortedRowEncoder(definition, extractor);
          final long[] records = new long[7];
          assertTrue(reader.moveToDocumentRoot());
          assertTrue(reader.moveToFirstChild());
          assertTrue(reader.moveToFirstChild());
          for (int i = 0; i < records.length; i++) {
            records[i] = reader.getNodeKey();
            if (i + 1 < records.length) {
              assertTrue(reader.moveToRightSibling());
            }
          }
          final byte[][] keys = new byte[records.length][];
          for (int i = 0; i < records.length; i++) {
            assertTrue(extractor.extractInto(reader, records[i]));
            encoder.writeKey(records[i]);
            keys[i] = encoder.copyKey();
            assertEquals(keys[i].length, encoder.keyLength());
            assertEquals(i >= 4, encoder.unencodable(), "row " + i);
          }
          for (int i = 0; i < 4; i++) {
            assertTrue(encoder.layout().lastFieldOffset(keys[i], keys[i].length) > 0, "row " + i);
          }
          assertTrue(Arrays.compareUnsigned(keys[2], keys[0]) < 0);
          assertTrue(Arrays.compareUnsigned(keys[3], keys[2]) < 0);
          assertTrue(Arrays.compareUnsigned(keys[0], keys[1]) < 0);
          for (int i = 4; i < records.length; i++) {
            final ProjectionSortKeyCodec.Writer reserved = new ProjectionSortKeyCodec.Writer();
            reserved.writeUnencodable(records[i]);
            assertArrayEquals(reserved.copyKey(), keys[i]);
            assertTrue(ProjectionSortKeyCodec.isUnencodable(keys[i], keys[i].length));
            assertTrue(Arrays.compareUnsigned(keys[1], keys[i]) < 0);
          }
        }
      }
    }
  }

  @Test
  void aWholeKeyLongerThanTheKeyBoundIsUnencodableEvenWhenEveryFieldFits() {
    final Path databasePath = temporaryDirectory.resolve("sorted-encoder-key-bound");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    // Three string fields (three framing bytes each), one long (nine) and the record key (eight)
    // around "commit" and "create": a did of this length fills the bound exactly.
    final int fittingDidLength = ProjectionSortKeyCodec.MAX_KEY_BYTES - 26 - 12;
    final String third = "y".repeat(ProjectionSortKeyCodec.MAX_KEY_BYTES / 3);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"kind":"commit","op":"create","did":"%s","time":1},
               {"kind":"commit","op":"create","did":"%s","time":1},
               {"kind":"%s","op":"%s","did":"%s","time":1}]
              """.formatted("x".repeat(fittingDidLength), "x".repeat(fittingDidLength + 1), third, third, third)),
              JsonNodeTrx.Commit.NO);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx();
            PathSummaryReader pathSummary = session.openPathSummary()) {
          final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/kind", PathParser.Type.JSON), parse("/[]/op", PathParser.Type.JSON),
                  parse("/[]/did", PathParser.Type.JSON), parse("/[]/time", PathParser.Type.JSON)),
              List.of(Type.STR, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON,
              new ProjectionSortedSpec(List.of(0, 1, 2, 3)));
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(definition, pathSummary);
          final ProjectionSortedRowEncoder encoder = new ProjectionSortedRowEncoder(definition, extractor);
          final long[] records = new long[3];
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
          encoder.writeKey(records[0]);
          assertFalse(encoder.unencodable());
          assertEquals(ProjectionSortKeyCodec.MAX_KEY_BYTES, encoder.keyLength());
          for (int i = 1; i < records.length; i++) {
            assertTrue(extractor.extractInto(reader, records[i]));
            encoder.writeKey(records[i]);
            assertTrue(encoder.unencodable(), "row " + i);
            final ProjectionSortKeyCodec.Writer reserved = new ProjectionSortKeyCodec.Writer();
            reserved.writeUnencodable(records[i]);
            assertArrayEquals(reserved.copyKey(), encoder.copyKey());
          }
        }
      }
    }
  }
}
