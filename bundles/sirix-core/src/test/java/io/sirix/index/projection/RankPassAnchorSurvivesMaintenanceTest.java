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
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rank pass publishes a column's DICTIONARY ANCHOR, and an ordinary maintenance commit keeps
 * it.
 *
 * <p>
 * This exists because of the shape of the regression the storage acceptance is most vulnerable to:
 * a column that answers every query CORRECTLY while silently losing predicate pushdown. Every
 * result differential passes such a column — the answers are right — and it shows up only as
 * latency, which is the one thing a byte-focused campaign is least likely to attribute correctly.
 * The mechanism is `ProjectionIndexCatalog`'s note that without the anchor "a global string column
 * can only be scanned, never probed": resolving a predicate literal to an id needs it.
 * </p>
 *
 * <p>
 * So this asserts the MECHANISM rather than an answer — that a literal actually resolves to its id
 * through the published anchor.
 * </p>
 *
 * <p>
 * <b>WHY NO SINGLE MUTATION KILLS THE MAINTENANCE HALF, established rather than assumed.</b> Three
 * independent mechanisms keep the anchor, so no one of them is load-bearing alone and none can be
 * mutation-killed:
 * </p>
 * <ol>
 * <li>{@code ProjectionIndexMetadata}'s constructor REFUSES a {@code STRING_GLOBAL} column with a
 * zero or absent anchor, and every writer of slot 0 passes through it. This one IS
 * mutation-killable and is asserted directly by
 * {@code metadataRefusesAGlobalColumnWithoutAnAnchor}.</li>
 * <li>{@code flushMaintenanceGlobalDictionaries} carries the prior anchors forward with a
 * clone.</li>
 * <li>Every commit builds a {@code MaintenanceGlobalDictionary} for EVERY global column, not only
 * for the columns it touched — verified by instrumenting the listener on this fixture, which
 * reports {@code dicts=0:live 1:live} when only column 0 was written — so
 * {@code dictionary.flush()} re-supplies each anchor independently of (2).</li>
 * </ol>
 * <p>
 * Both branches of (2) were inverted, in a one-column and then a two-column fixture where only one
 * column is written, and this test passed every time — because (3) supplies what (2) was mutated to
 * drop. That makes (2) defensive rather than load-bearing today. This test therefore pins the
 * OUTCOME across all three, which is what the acceptance needs; it does not and cannot pin (2) in
 * isolation, and saying so is more useful than a mutation table that implies otherwise.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
final class RankPassAnchorSurvivesMaintenanceTest {

  private static final java.nio.file.Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final int INDEX_NUMBER = 0;

  /** Past one reverse bucket so the pass also builds a separator array over the result. */
  private static final int DISTINCT_CODES = 900;

  private String priorRankMode;

  private String priorGlobalDictMode;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    priorRankMode = System.setProperty("sirix.projection.globalDict.rank", "true");
    // The pass converts a PER-LEAF dictionary, so the load must not have elected a global one.
    priorGlobalDictMode = System.setProperty("sirix.projection.globalDict", "never");
  }

  @AfterEach
  void tearDown() {
    restore("sirix.projection.globalDict.rank", priorRankMode);
    restore("sirix.projection.globalDict", priorGlobalDictMode);
    JsonTestHelper.deleteEverything();
  }

  private static void restore(final String key, final String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @Test
  void theAnchorIsPublishedByThePassAndSurvivesAnOrdinaryMaintenanceCommit() throws Exception {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
      database.createResource(resourceConfig());
      try (JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(corpus()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, metadata(session).columnKinds()[0],
            "the fixture must start from a per-leaf dictionary or the pass has nothing to convert");

        for (int column = 0; column < 2; column++) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            ProjectionRankPass.run(wtx, INDEX_NUMBER, column, JsonTestHelper.PATHS.PATH2.getFile(), 1 << 20);
          }
        }

        final ProjectionIndexMetadata afterPass = metadata(session);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, afterPass.columnKinds()[0]);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, afterPass.columnKinds()[1]);
        final long anchor = afterPass.valueDictionaryHeaderKey(0);
        final long untouchedAnchor = afterPass.valueDictionaryHeaderKey(1);
        assertTrue(anchor > 0, "the pass must publish the code column's dictionary anchor");
        assertTrue(untouchedAnchor > 0, "the pass must publish the label column's dictionary anchor");
        assertProbesResolve(session, anchor, "c");
        assertProbesResolve(session, untouchedAnchor, "l");

        // An ordinary write, which drives the incremental maintenance path over the same index.
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.moveToFirstChild();
          wtx.moveToFirstChild();
          wtx.setStringValue("c0-touched");
          wtx.commit();
        }

        final ProjectionIndexMetadata afterMaintenance = metadata(session);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, afterMaintenance.columnKinds()[0],
            "maintenance must not un-flip the column's kind");
        assertTrue(afterMaintenance.valueDictionaryHeaderKey(0) > 0,
            "maintenance must carry the touched column's dictionary anchor forward");
        // THE DISCRIMINATING ASSERTION. The `label` column is not written by the commit above, so no
        // maintenance dictionary is flushed for it and nothing re-supplies its anchor — it survives
        // only because the anchors are CARRIED FORWARD. Drop the carry-forward and this fails.
        assertEquals(untouchedAnchor, afterMaintenance.valueDictionaryHeaderKey(1),
            "maintenance must carry an UNTOUCHED global column's dictionary anchor forward; without it that column "
                + "answers correctly and silently loses predicate pushdown, which no result differential can see");
        assertProbesResolve(session, afterMaintenance.valueDictionaryHeaderKey(0), "c");
        assertProbesResolve(session, afterMaintenance.valueDictionaryHeaderKey(1), "l");
      }
    }
  }

  /**
   * The structural half: a global column with no anchor cannot even be described.
   *
   * <p>
   * Asserted here rather than assumed, because it is what makes the failure mode above impossible to
   * introduce by accident anywhere else — every writer of slot 0 goes through this constructor.
   * </p>
   */
  @Test
  void metadataRefusesAGlobalColumnWithoutAnAnchor() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL};
    final String[] paths = {"/[]/code"};
    final String[] names = {"code"};
    final IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata("/[]", paths, names, kinds, 1, 1, null, new long[] {0L}));
    assertTrue(refused.getMessage().contains("requires a dictionary anchor"), refused.getMessage());
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionIndexMetadata("/[]", paths, names, kinds, 1, 1, null, null),
        "a null anchor array is the same loss as a zero anchor");
  }

  /** Resolving a literal to its id through the published anchor IS the pushdown mechanism. */
  private static void assertProbesResolve(final JsonResourceSession session, final long anchor, final String prefix) {
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = rtx.getStorageEngineReader();
      for (final String literal : new String[] {prefix + "0", prefix + "1", prefix + (DISTINCT_CODES - 1)}) {
        final int id = GlobalValueDictionary.probe(anchor, literal.getBytes(StandardCharsets.UTF_8), reader);
        assertTrue(id > 0, "the literal " + literal + " must resolve to an id through the anchor, not " + id);
      }
      final int absent =
          GlobalValueDictionary.probe(anchor, "nothing-like-this".getBytes(StandardCharsets.UTF_8), reader);
      assertEquals(GlobalValueDictionary.ID_ABSENT, absent,
          "an absent literal must answer ABSENT, never UNKNOWN — UNKNOWN would make the predicate decline");
    }
  }

  private static ProjectionIndexMetadata metadata(final JsonResourceSession session) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final byte[] raw = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).getBlob(0L);
      assertNotNull(raw, "the projection index must publish metadata");
      return ProjectionIndexMetadata.parse(raw);
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
    // TWO string columns on purpose. With one, a maintenance commit always carries a dictionary for
    // it, so the flush re-supplies the anchor and the carry-forward path is never exercised — which
    // is exactly why both mutations of it survived the single-column version of this test.
    final List<Path<QNm>> fieldPaths =
        List.of(Path.parse("/[]/code", PathParser.Type.JSON), Path.parse("/[]/label", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths,
        List.of(Type.STR, Type.STR), INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static String corpus() {
    final StringBuilder json = new StringBuilder(DISTINCT_CODES * 24);
    json.append('[');
    for (int record = 0; record < DISTINCT_CODES; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"code\":\"c").append(record).append("\",\"label\":\"l").append(record).append("\"}");
    }
    json.append(']');
    return json.toString();
  }
}
