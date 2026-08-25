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
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the structural defect the one-pass parallel load fixes: the post-pass build appends a
 * resource-wide value dictionary as ONE generation, and a generation admits at most
 * {@link GlobalValueDictionaryWriter#MAX_DISTINCT_ENTRIES_PER_APPEND} distinct entries. Any elected
 * string column with more distinct values than that kills the build outright — the workaround being
 * to turn the dictionary off entirely.
 *
 * <p>
 * A load-time build has no such ceiling: it flushes a dictionary generation at every storage epoch,
 * so generations rotate naturally with the load and the column's distinct count is bounded by the
 * corpus rather than by one array's safe length.
 *
 * <p>
 * Both arms run, because a fix needs a positive AND a negative witness: the same corpus and the same
 * definition must SUCCEED one-pass and FAIL post-pass. A test that only ran the passing arm could not
 * tell a fix from a corpus that never reached the ceiling.
 */
final class ProjectionGlobalDictionaryGenerationRotationTest {

  private static final java.nio.file.Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final java.nio.file.Path POST_PASS_DATABASE_PATH = JsonTestHelper.PATHS.PATH2.getFile();
  private static final int INDEX_NUMBER = 0;

  /**
   * Comfortably past the per-append ceiling, so neither arm can squeak under it on the WHOLE corpus.
   */
  private static final int DISTINCT_CODES = GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND * 2 + 4096;
  /**
   * Each code appears on {@link #CODE_RUN} consecutive records. This is what makes the test about
   * generation ROTATION rather than about window size: the distinct values inside any one dictionary
   * window stay well under the per-append ceiling, while the corpus as a whole is far past it. A
   * column that is 100% distinct saturates a generation from its very first window and no rotation
   * scheme can help it — that is a different (and honest) limit, not the one this test is about.
   */
  private static final int CODE_RUN = 4;

  private static final int RECORDS = DISTINCT_CODES * CODE_RUN;

  private String priorGlobalDictMode;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
    priorGlobalDictMode = System.getProperty("sirix.projection.globalDict");
    // "allowed" in its strongest form: elect the dictionary regardless of the promotion heuristics,
    // so the arms differ ONLY in how the elected dictionary's generations are appended.
    System.setProperty("sirix.projection.globalDict", "always");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (priorGlobalDictMode == null) {
      System.clearProperty("sirix.projection.globalDict");
    } else {
      System.setProperty("sirix.projection.globalDict", priorGlobalDictMode);
    }
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void aColumnPastThePerAppendCeilingBuildsOnePassAndBreaksThePostPass() throws Exception {
    final byte[] corpus = corpus();

    // POSITIVE witness: the load-time build rotates a generation per storage epoch and completes.
    final ProjectionIndexMetadata onePass = loadOnePassParallel(corpus);
    assertNotNull(onePass, "the one-pass load must publish metadata");
    assertFalse(onePass.isStale(), "the one-pass build must not have abandoned the projection: a load that hits the "
        + "dictionary ceiling aborts and leaves the stale tombstone behind, which looks like success from outside");
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, onePass.columnKinds()[0],
        "the code column must actually have been built as a resource-wide dictionary column, or the ceiling was "
            + "never approached and this test proves nothing");
    final long[] dictionaryHeaderKeys = onePass.valueDictionaryHeaderKeys();
    assertNotNull(dictionaryHeaderKeys, "an elected global-dictionary column must publish its dictionary header");
    assertTrue(dictionaryHeaderKeys[0] > 0, "the code column's dictionary header key must be set");

    // NEGATIVE witness: the same corpus and definition through the post-pass build.
    Throwable postPassFailure = null;
    ProjectionIndexMetadata postPass = null;
    try {
      postPass = loadParallelThenPostPass(corpus);
    } catch (final Throwable failure) {
      postPassFailure = failure;
    }
    System.out.println("#67 WITNESS: distinct=" + DISTINCT_CODES + " (ceiling "
        + GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND + "), records=" + RECORDS
        + " | ONE-PASS: stale=" + onePass.isStale() + " columnKind=" + onePass.columnKinds()[0] + " rowGroups="
        + onePass.rowGroupCount() + " | POST-PASS: " + (postPassFailure == null
            ? "no failure, stale=" + (postPass == null ? "?" : postPass.isStale()) + " columnKind="
                + (postPass == null ? "?" : postPass.columnKinds()[0])
            : "THREW " + postPassFailure.getClass().getSimpleName() + ": " + postPassFailure.getMessage()));
    if (postPassFailure == null) {
      assertNotNull(postPass, "the post-pass arm returned no metadata and no failure");
      assertTrue(
          postPass.isStale()
              || postPass.columnKinds()[0] != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
          "the post-pass build was expected to die or decline the resource-wide dictionary for "
              + DISTINCT_CODES + " distinct values (the per-append ceiling is "
              + GlobalValueDictionaryWriter.MAX_DISTINCT_ENTRIES_PER_APPEND + "), but it published a live "
              + "STRING_GLOBAL column — the ceiling this test exists for is gone, and the test must be re-aimed");
    } else {
      assertTrue(mentionsTheCeiling(postPassFailure),
          "the post-pass arm failed for an unrelated reason: " + postPassFailure);
    }
  }

  private static boolean mentionsTheCeiling(final Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause
        ? null
        : cause.getCause()) {
      final String message = cause.getMessage();
      if (cause instanceof GlobalDictionaryBudgetExceededException
          || (message != null && (message.contains("per-append limit") || message.contains("dictionary")))) {
        return true;
      }
    }
    return false;
  }

  // ==== arms ===================================================================================

  private static ProjectionIndexMetadata loadOnePassParallel(final byte[] corpus) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(2048, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
          controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, RECORDS);
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), 1 << 20, 4);
          wtx.commit();
        }
        return metadata(session);
      }
    }
  }

  private static ProjectionIndexMetadata loadParallelThenPostPass(final byte[] corpus) throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(POST_PASS_DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(POST_PASS_DATABASE_PATH)) {
      db.createResource(resourceConfig());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx(2048, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus), 1 << 20, 4);
          wtx.commit();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        return metadata(session);
      }
    }
  }

  private static ProjectionIndexMetadata metadata(final JsonResourceSession session) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final byte[] raw =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).getBlob(0L);
      return raw == null
          ? null
          : ProjectionIndexMetadata.parse(raw);
    }
  }

  private static ResourceConfiguration resourceConfig() {
    return ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                .useDeweyIDs(false)
                                .hashKind(HashType.NONE)
                                .storeNodeHistory(false)
                                .buildPathSummary(true)
                                .build();
  }

  private static IndexDef projectionDef() {
    final List<Path<QNm>> fieldPaths = List.of(Path.parse("/[]/code", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths, List.of(Type.STR),
        INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static byte[] corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 24);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"code\":\"c").append(record / CODE_RUN).append("\"}");
    }
    json.append(']');
    return json.toString().getBytes(StandardCharsets.UTF_8);
  }
}
