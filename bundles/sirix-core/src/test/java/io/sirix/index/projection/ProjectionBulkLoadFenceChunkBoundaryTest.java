/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A bulk load whose leaf count is an exact multiple of the fence chunk size. The fence writer used
 * to publish a chunk the moment it filled, with its tail entry PREDICTING the next leaf; when no
 * leaf followed, {@code finish} re-read the chunk and wrote it back with the link cleared. A bulk
 * load's side pages are append-only until publication, so that replace was refused and the whole
 * load failed at commit — one leaf count in every {@value ProjectionIndexFences#CHUNK_LEAVES}. The
 * writer now holds a just-filled chunk until it knows whether a leaf follows and writes every chunk
 * once.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class ProjectionBulkLoadFenceChunkBoundaryTest {

  private static final int INDEX_NUMBER = 0;

  /** One record per row, so the leaf count is exactly the chunk size. */
  private static final int RECORDS = ProjectionIndexFences.CHUNK_LEAVES * ProjectionIndexRowGroupPage.MAX_ROWS;

  private static final int LEAVES = ProjectionIndexFences.CHUNK_LEAVES;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
  }

  @AfterEach
  void tearDown() {
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("a bulk load of exactly one fence chunk of leaves commits, and its document chain terminates")
  void anExactChunkOfLeavesCommits() {
    Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      // The parallel importer supports hashType NONE only.
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final JsonIndexController controller =
              (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, RECORDS);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus()), 1 << 16, 4);
          // The regression: this commit refused to replace the pending final fence chunk.
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final int[] order =
              ProjectionIndexFences.readPhysicalOrder(rtx.getStorageEngineReader(), INDEX_NUMBER, LEAVES);
          assertArrayEquals(IntStream.rangeClosed(1, LEAVES).toArray(), order,
              "the document chain visits every leaf once and terminates at the last");
          assertEquals(LEAVES, order.length);
        }
      }
    }
  }

  private static IndexDef projectionDef() {
    final List<Path<QNm>> fieldPaths = List.of(Path.parse("/[]/v", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths, List.of(Type.LON),
        INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static byte[] corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 12);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"v\":").append(record).append('}');
    }
    json.append(']');
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }
}
