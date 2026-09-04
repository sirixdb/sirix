package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.node.NodeKind;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

/**
 * Serving gate for ClickBench's high-cardinality {@code EventTime} column when AUTO selects the
 * resource-wide dictionary encoding.
 *
 * <p>
 * The generic pipeline is the differential oracle for every shape. A serving-counter delta is
 * asserted beside equality so a silent fallback cannot pass. The corpus deliberately mints
 * dictionary ids in an order unrelated to lexical timestamp order; comparing ids instead of their
 * values therefore fails the sorted queries. The q18/q42 shapes additionally require substring
 * transforms to read the dictionary entry directly, rather than treating the id as a timestamp.
 *
 * <p>
 * The final phase updates {@code EventTime} to a newly interned value, deletes a record in a
 * successor revision, and then reopens the baseline revision. This pins both incremental index
 * maintenance and revision-bound dictionary reads: a read view accidentally retained across
 * revisions cannot answer all three snapshots correctly.
 */
public final class GlobalEventTimeVectorServingTest extends AbstractJsonTest {

  private static final String GLOBAL_DICTIONARY_PROPERTY = "sirix.projection.globalDict";
  private static final String DATABASE = "json-path1";
  private static final String RESOURCE = "global-event-time.jn";
  private static final int ROWS = 12_000;

  private static final String[] PROJECTED_FIELDS =
      {"EventTime", "URL", "SearchPhrase", "UserID", "CounterID", "EventDate", "IsRefresh", "DontCountHits"};

  private String previousGlobalDictionaryMode;

  @BeforeEach
  void configureGlobalDictionaryAutoMode() {
    previousGlobalDictionaryMode = System.getProperty(GLOBAL_DICTIONARY_PROPERTY);
    System.setProperty(GLOBAL_DICTIONARY_PROPERTY, "auto");
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  void restoreGlobalDictionaryMode() {
    if (previousGlobalDictionaryMode == null) {
      System.clearProperty(GLOBAL_DICTIONARY_PROPERTY);
    } else {
      System.setProperty(GLOBAL_DICTIONARY_PROPERTY, previousGlobalDictionaryMode);
    }
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    Databases.clearGlobalCaches();
  }

  @Test
  void autoGlobalEventTimeServesClickBenchShapesAcrossUpdateAndDeleteRevisions() throws IOException {
    storeCorpusAndCreateProjection();

    final int baselineRevision = mostRecentRevision();
    assertEventTimeIsTheOnlyGlobalStringColumn(baselineRevision);

    assertDifferentialServed(q23(baselineRevision), Route.SORTED);
    assertDifferentialServed(q24(baselineRevision), Route.SORTED);
    assertDifferentialServed(q26(baselineRevision), Route.SORTED);
    assertDifferentialServed(q18(baselineRevision), Route.GROUP);
    assertDifferentialServed(q42(baselineRevision), Route.GROUP);

    final int updatedRevision = updateFirstEventTime("2013-07-13T23:59:30");
    clearProjectionCaches();
    assertEventTimeIsTheOnlyGlobalStringColumn(updatedRevision);
    assertDifferentialServed(q26(updatedRevision), Route.SORTED);
    assertDifferentialServed(q18(updatedRevision), Route.GROUP);

    final int deletedRevision = deleteSecondRecord();
    clearProjectionCaches();
    assertEventTimeIsTheOnlyGlobalStringColumn(deletedRevision);
    assertDifferentialServed(q42(deletedRevision), Route.GROUP);

    // Read the old dictionary generation after both successor revisions have been opened. This is
    // the part that rejects a process-wide/latest-revision dictionary cache masquerading as a view.
    clearProjectionCaches();
    assertEventTimeIsTheOnlyGlobalStringColumn(baselineRevision);
    assertDifferentialServed(q26(baselineRevision), Route.SORTED);
    assertDifferentialServed(q42(baselineRevision), Route.GROUP);
  }

  private void storeCorpusAndCreateProjection() {
    query("jn:store('" + DATABASE + "','" + RESOURCE + "','" + corpus() + "')");
    query("""
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/EventTime', '/[]/URL', '/[]/SearchPhrase', '/[]/UserID',
               '/[]/CounterID', '/[]/EventDate', '/[]/IsRefresh', '/[]/DontCountHits'),
              ('string', 'string', 'string', 'long', 'long', 'string', 'long', 'long'))
          return {"revision": sdb:commit($doc)}
        """.formatted(DATABASE, RESOURCE));
    Assertions.assertEquals(1, ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
        "AUTO must choose the high-cardinality EventTime column, and no repeated string column");
    clearProjectionCaches();
  }

  private static String corpus() {
    final StringBuilder json = new StringBuilder(ROWS * 180).append('[');
    for (int row = 0; row < ROWS; row++) {
      if (row > 0) {
        json.append(',');
      }

      // 7,919 is coprime to 12,000: first-seen id order is a permutation of timestamp order.
      // Rows 0 and 1 intentionally share the full timestamp, so q26 must consult its local-string
      // secondary key while EventTime still has 11,999 distinct values and wins AUTO admission.
      final int logicalMinute = row == 1
          ? 0
          : row;
      final int permutedMinute = logicalMinute * 7_919 % ROWS;
      final int day = 14 + permutedMinute / (24 * 60);
      final int minuteOfDay = permutedMinute % (24 * 60);
      final int hour = minuteOfDay / 60;
      final int minute = minuteOfDay % 60;
      final int second = logicalMinute % 60;

      json.append("{\"EventTime\":\"2013-07-");
      appendTwoDigits(json, day);
      json.append('T');
      appendTwoDigits(json, hour);
      json.append(':');
      appendTwoDigits(json, minute);
      json.append(':');
      appendTwoDigits(json, second);
      json.append("\",\"URL\":\"")
          .append(row % 4 == 0
              ? "https://google.example/a"
              : row % 4 == 1
                  ? "https://example.test/b"
                  : "")
          .append("\",\"SearchPhrase\":\"")
          .append(switch (row % 5) {
            case 0 -> "alpha";
            case 1 -> "zulu";
            case 2 -> "beta";
            case 3 -> "gamma";
            default -> "";
          })
          .append("\",\"UserID\":")
          .append(row % 47)
          .append(",\"CounterID\":62,\"EventDate\":\"2013-07-");
      appendTwoDigits(json, day);
      json.append("\",\"IsRefresh\":0,\"DontCountHits\":0}");
    }
    return json.append(']').toString();
  }

  private static void appendTwoDigits(final StringBuilder target, final int value) {
    if (value < 10) {
      target.append('0');
    }
    target.append(value);
  }

  private static String source(final int revision) {
    return "jn:doc('" + DATABASE + "','" + RESOURCE + "'," + revision + ")[]";
  }

  private static String q23(final int revision) {
    return "subsequence(for $h in " + source(revision)
        + " where contains($h.URL, \"google\") order by $h.EventTime return $h, 1, 10)";
  }

  private static String q24(final int revision) {
    return "subsequence(for $h in " + source(revision)
        + " where $h.SearchPhrase != \"\" order by $h.EventTime return $h.SearchPhrase, 1, 10)";
  }

  private static String q26(final int revision) {
    return "subsequence(for $h in " + source(revision)
        + " where $h.SearchPhrase != \"\" order by $h.EventTime, $h.SearchPhrase " + "return $h.SearchPhrase, 1, 10)";
  }

  private static String q18(final int revision) {
    return "subsequence(for $h in " + source(revision)
        + " let $u := $h.UserID, $m := xs:integer(substring($h.EventTime, 15, 2)), "
        + "$s := $h.SearchPhrase group by $u, $m, $s let $c := count($h) order by $c descending "
        + "return {\"UserID\": $u, \"m\": $m, \"SearchPhrase\": $s, \"count\": $c}, 1, 10)";
  }

  private static String q42(final int revision) {
    return "subsequence(for $h in " + source(revision) + " where $h.CounterID = 62 and $h.EventDate >= \"2013-07-14\" "
        + "and $h.EventDate <= \"2013-07-15\" and $h.IsRefresh = 0 and $h.DontCountHits = 0 "
        + "let $m := substring($h.EventTime, 1, 16) group by $m let $c := count($h) order by $m "
        + "return {\"M\": concat($m, \":00\"), \"PageViews\": $c}, 1001, 10)";
  }

  private void assertDifferentialServed(final String query, final Route route) throws IOException {
    final String generic = evaluateGeneric(query);
    final long servedBefore = route.servedCount();
    final long topKBefore = SirixVectorizedExecutor.sortedTopKAppliedCount();
    final String vectorized = evaluateVectorized(query, revisionFrom(query));
    Assertions.assertEquals(generic, vectorized, "projection result differs from the generic pipeline for: " + query);
    Assertions.assertTrue(route.servedCount() > servedBefore,
        route + " route did not serve the forced-global EventTime shape; agreement would be vacuous: " + query);
    if (route == Route.SORTED) {
      Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedTopKAppliedCount() - topKBefore,
          "the forced-global sort must execute the bounded top-K helper; a helper-level decline must not "
              + "silently fall through to another sorted implementation: " + query);
    }
  }

  private static int revisionFrom(final String query) {
    final String marker = "jn:doc('" + DATABASE + "','" + RESOURCE + "',";
    final int start = query.indexOf(marker);
    if (start < 0) {
      throw new IllegalArgumentException("query has no revision-bound source: " + query);
    }
    final int numberStart = start + marker.length();
    final int numberEnd = query.indexOf(')', numberStart);
    if (numberEnd < 0) {
      throw new IllegalArgumentException("query has an unterminated revision-bound source: " + query);
    }
    return Integer.parseInt(query.substring(numberStart, numberEnd));
  }

  private static String evaluateGeneric(final String query) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return serialize(chain, context, query);
    }
  }

  private static String evaluateVectorized(final String query, final int revision) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(DATABASE);
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE)) {
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          return serialize(chain, context, query);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws IOException {
    try (final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final PrintWriter writer = new PrintWriter(output)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return output.toString();
    }
  }

  private static int mostRecentRevision() {
    try (final Database<JsonResourceSession> database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  private static int updateFirstEventTime(final String value) {
    try (final Database<JsonResourceSession> database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx writeTransaction = session.beginNodeTrx()) {
      Assertions.assertTrue(writeTransaction.moveToDocumentRoot());
      Assertions.assertTrue(writeTransaction.moveToFirstChild(), "top-level array");
      Assertions.assertTrue(writeTransaction.moveToFirstChild(), "first record");
      Assertions.assertTrue(writeTransaction.moveToFirstChild(), "EventTime field");
      Assertions.assertEquals(NodeKind.OBJECT_NAMED_STRING, writeTransaction.getKind());
      Assertions.assertEquals("EventTime", writeTransaction.getName().getLocalName());
      writeTransaction.setStringValue(value);
      writeTransaction.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static int deleteSecondRecord() {
    try (final Database<JsonResourceSession> database = openDatabase();
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeTrx writeTransaction = session.beginNodeTrx()) {
      Assertions.assertTrue(writeTransaction.moveToDocumentRoot());
      Assertions.assertTrue(writeTransaction.moveToFirstChild(), "top-level array");
      Assertions.assertTrue(writeTransaction.moveToFirstChild(), "first record");
      Assertions.assertTrue(writeTransaction.moveToRightSibling(), "second record");
      Assertions.assertEquals(NodeKind.OBJECT, writeTransaction.getKind());
      writeTransaction.remove();
      writeTransaction.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static void assertEventTimeIsTheOnlyGlobalStringColumn(final int revision) {
    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup(DATABASE);
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE);
          final var readTransaction = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexRegistry.Handle handle =
            session.getRtxIndexController(revision)
                   .openProjectionIndex(readTransaction.getStorageEngineReader(), new String[] {"[]"},
                       PROJECTED_FIELDS);
        Assertions.assertNotNull(handle, "the maintained projection must be visible at revision " + revision);
        final int eventTimeColumn = handle.columnOf("EventTime");
        Assertions.assertTrue(eventTimeColumn >= 0);
        Assertions.assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
            handle.columnKindOf(eventTimeColumn), "EventTime must remain globally dictionary encoded");
        Assertions.assertTrue(handle.valueDictionaryHeaderKey(eventTimeColumn) > 0,
            "EventTime must retain a revision-readable dictionary header");
        Assertions.assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            handle.columnKindOf(handle.columnOf("URL")), "URL's repeated labels should stay leaf-local under AUTO");
        Assertions.assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            handle.columnKindOf(handle.columnOf("SearchPhrase")),
            "SearchPhrase's repeated labels should stay leaf-local under AUTO");
        Assertions.assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
            handle.columnKindOf(handle.columnOf("EventDate")),
            "EventDate's repeated labels should stay leaf-local under AUTO");
      }
    }
  }

  private static Database<JsonResourceSession> openDatabase() {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    return Databases.openJsonDatabase(databasePath);
  }

  private static void clearProjectionCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  private enum Route {
    SORTED {
      @Override
      long servedCount() {
        return SirixVectorizedExecutor.sortedScanServedCount();
      }
    },
    GROUP {
      @Override
      long servedCount() {
        return SirixVectorizedExecutor.groupAggServedCount();
      }
    };

    abstract long servedCount();
  }
}
