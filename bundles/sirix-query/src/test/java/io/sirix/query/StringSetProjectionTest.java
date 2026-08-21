package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Array membership answered from a PROJECTION INDEX, not from the storage pages.
 *
 * <h2>Why the column kind exists</h2>
 *
 * <p>
 * A projection index had four column kinds and all of them were scalar, so an array-valued field
 * declared as a column was recorded present-but-unrepresentable and the index could answer nothing
 * about it. {@code some $g in $m.genres[] satisfies $g eq "..."} therefore had no index to use and
 * ran on the storage path, which reads every page of the resource because one page's string region
 * holds every string on it — titles included.
 *
 * <p>
 * A set column is contiguous across many records and holds only these values, so a literal resolves
 * against a leaf's dictionary once and a leaf that does not hold it is skipped whole.
 *
 * <h2>What is asserted</h2>
 *
 * <p>
 * Agreement with the record route, over literals spanning the selectivity range. The rare ones
 * carry the weight: a literal on every leaf never exercises the dictionary-miss path, and a
 * membership route that quietly answered from the wrong column would still look right on a common
 * value. One literal matches nothing at all, which is the pure miss.
 */
final class StringSetProjectionTest {

  private static final int N = 2_000;
  private static final String DB = "string-set-projection-db";
  private static final String RES = "records";

  /** Common, mid, rare, absent — see the class comment on why one literal proves nothing. */
  private static final List<String> LITERALS = List.of("Drama", "Comedy", "Silent", "Nowhere");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-string-set-projection-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"year\":").append(1900 + i % 120);
      if (i % 7 != 0) { // a seventh of the records carry NO genres field
        sb.append(",\"genres\":[");
        if (i % 5 != 0) { // and a fifth of the rest carry an EMPTY array
          sb.append(i % 3 == 0
              ? "\"Drama\""
              : "\"Comedy\"");
          if (i % 11 == 0) {
            sb.append(",\"Silent\""); // deliberately rare: exercises the dictionary miss
          }
        }
        sb.append(']');
      }
      sb.append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("a projection over an array field answers membership as the records do")
  void projectionAnswersMembershipLikeTheRecords() throws Exception {
    final long[] viaRecords = new long[LITERALS.size()];
    for (int i = 0; i < LITERALS.size(); i++) {
      viaRecords[i] = count(LITERALS.get(i));
    }

    createProjection();

    SirixVectorizedExecutor.resetProjectionCountsServed();
    for (int i = 0; i < LITERALS.size(); i++) {
      assertEquals(viaRecords[i], count(LITERALS.get(i)),
          "the projection answered differently from the records for '" + LITERALS.get(i)
              + "' — a set column that mis-segments its flat element run shifts values "
              + "onto neighbouring records rather than failing outright");
    }
    // Agreement proves nothing if the projection declined and both arms ran through the records —
    // which is exactly what happened until the column's name, the ambiguity check and the byte
    // scan all learned about set columns. Each fix was invisible to the assertions above.
    assertTrue(SirixVectorizedExecutor.projectionCountsServed() > 0,
        "no count was served from the projection index, so the agreement above is vacuous");
  }

  @Test
  @DisplayName("the corpus makes the agreement non-vacuous")
  void theLiteralsSelectRealSubsets() throws Exception {
    createProjection();
    final long drama = count("Drama");
    assertTrue(drama > 0 && drama < N, "'Drama' selected " + drama + " of " + N + " — it must select a proper subset");
    assertTrue(count("Silent") > 0, "'Silent' must select something or the rare path is untested");
    assertEquals(0L, count("Nowhere"), "a genre no record carries must select nothing");
  }

  @Test
  @DisplayName("incremental maintenance preserves and updates set summaries")
  void incrementalMaintenancePreservesAndUpdatesSetSummaries() throws Exception {
    createProjection();
    final long dramaBefore = count("Drama");
    final long comedyBefore = count("Comedy");

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir.resolve(DB));
        final JsonResourceSession session = database.beginResourceSession(RES);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToRightSibling());
      assertEquals("genres", wtx.getName().getLocalName());
      assertTrue(wtx.moveToFirstChild());
      wtx.setStringValue("Drama");
      wtx.commit();
    }

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbDir.resolve(DB));
        final JsonResourceSession session = database.beginResourceSession(RES)) {
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
          new String[] {"[]"}, new String[] {"genres"});
      assertNotNull(handle);
      assertEquals(dramaBefore + 1, handle.setValueRowCount(1, "Drama"));
      assertEquals(comedyBefore - 1, handle.setValueRowCount(1, "Comedy"));
    }

    SirixVectorizedExecutor.resetProjectionCountsServed();
    assertEquals(dramaBefore + 1, count("Drama"));
    assertEquals(comedyBefore - 1, count("Comedy"));
    assertTrue(SirixVectorizedExecutor.projectionCountsServed() > 0);
  }

  /** Declare the ELEMENTS of the array-valued field: the trailing array step is what says "set". */
  private void createProjection() throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      // The commit is load-bearing: without it the definition is never catalogued, every lookup
      // reports "no covering handle", and the query quietly runs through the records — which a
      // differential test passes, because both arms then take the same route.
      new Query(chain,
          "let $doc := jn:doc('" + DB + "','" + RES + "') " + "let $i := jn:create-projection-index($doc, '/[]', "
              + "('/[]/year','/[]/genres/[]')) " + "return {\"revision\": sdb:commit($doc)}").evaluate(ctx);
    }
  }

  private long count(final String literal) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      return ((Int64) new Query(chain, "count(for $m in jn:doc('" + DB + "','" + RES + "')[] where some "
          + "$g in $m.genres[] satisfies $g eq '" + literal + "' return $m)").evaluate(ctx)).longValue();
    }
  }
}
