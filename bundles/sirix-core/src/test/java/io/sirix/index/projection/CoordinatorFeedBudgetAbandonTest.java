package io.sirix.index.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The coordinator-fed projection build (parallel bulk import with a load-time build armed) must
 * ABANDON the projection when a resource-wide dictionary breaches its budget mid-feed — announce it
 * once, tombstone the definition, and let the load complete — exactly like the listener-fed lane.
 * Before this arm existed the breach propagated out of the feed and poisoned the whole transaction:
 * a 100M AUTO load died with exit 1 at 674k distinct URL values.
 *
 * <p>
 * Same corpus and budget calibration as {@code ProjectionDictionaryBudgetAbandonNoticeTest}; the
 * mutation arm flips {@link ProjectionBulkLoad#ABANDON_ON_FEED_BUDGET_BREACH} to the pre-fix
 * behaviour and requires the load to FAIL, which is what makes the loud arm load-bearing.
 */
final class CoordinatorFeedBudgetAbandonTest {

  private static final int INDEX_NUMBER = 0;
  private static final String BUDGET_PROPERTY = "sirix.projection.globalDict.budgetBytes";
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";
  private static final int RECORDS = 30_000;
  private static final int ROWS_PER_VALUE = 2;
  private static final int VALUE_BYTES = 24;
  private static final int SAMPLE_DISTINCT = 16 * ProjectionIndexRowGroupPage.MAX_ROWS / ROWS_PER_VALUE;
  private static final int TOTAL_DISTINCT = RECORDS / ROWS_PER_VALUE;
  /**
   * The coordinator lane rotates the streaming dictionary GENERATION at every flush epoch, so the
   * writer never accumulates; what grows with the corpus is the resident PROBE FRONT (the
   * resource-wide id table), which is also what the 100M load breached. Its budget-derived table
   * would need ~1M distinct values to overflow, so the breach is driven deterministically through the
   * front's entry-cap seam: above the election sample, below the corpus.
   */
  private static final int FRONT_ENTRY_CAP = 12_000;
  /** Either breach form the feed lane announces: a quantified term or a structural decline. */
  private static final Pattern NOTICE = Pattern.compile("^\\[proj] PROJECTION ABANDONED during the load: index (\\d+),"
      + " column (\\d+) (needed (\\d+) B \\(([^)]+)\\) over (\\d+) distinct values, past its (\\d+) B budget"
      + " \\((\\d+) B retained\\)|declined an unsafe allocation over (\\d+) distinct values \\((\\d+) B retained\\):"
      + " [^.]*)\\. (.*)$");

  private String priorBudget;
  private String priorMode;
  private boolean priorSeam;
  private int priorFrontCap;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
    priorBudget = System.getProperty(BUDGET_PROPERTY);
    priorMode = System.getProperty(MODE_PROPERTY);
    priorSeam = ProjectionBulkLoad.ABANDON_ON_FEED_BUDGET_BREACH;
    priorFrontCap = GlobalValueDictionaryProbeFront.TEST_MAX_ENTRIES;
    System.setProperty(MODE_PROPERTY, "auto");
    // The budget never binds here: the front's entry cap is the one and only breach.
    System.setProperty(BUDGET_PROPERTY, Long.toString(Long.MAX_VALUE));
    assertTrue(FRONT_ENTRY_CAP > SAMPLE_DISTINCT && FRONT_ENTRY_CAP < TOTAL_DISTINCT,
        "the cap must sit above the election sample and below the corpus, or the arms prove nothing");
  }

  @AfterEach
  void tearDown() {
    ProjectionBulkLoad.ABANDON_ON_FEED_BUDGET_BREACH = priorSeam;
    GlobalValueDictionaryProbeFront.TEST_MAX_ENTRIES = priorFrontCap;
    restoreProperty(BUDGET_PROPERTY, priorBudget);
    restoreProperty(MODE_PROPERTY, priorMode);
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  @DisplayName("a mid-feed budget breach abandons the projection, announces it once, and the load completes")
  void aFeedLaneBudgetBreachAbandonsTheProjectionAndTheLoadCompletes() {
    GlobalValueDictionaryProbeFront.TEST_MAX_ENTRIES = FRONT_ENTRY_CAP;

    final Capture capture = loadParallel();

    final List<String> notices = capture.linesContaining("PROJECTION ABANDONED during the load");
    assertEquals(1, notices.size(),
        "the abandon must be announced exactly once; captured stderr was:\n" + capture.text());
    final Matcher parsed = NOTICE.matcher(notices.getFirst());
    assertTrue(parsed.matches(), "the notice must state index, column and the breach: " + notices.getFirst());
    assertEquals(INDEX_NUMBER, Integer.parseInt(parsed.group(1)), "the notice must name the abandoned index");
    assertEquals(0, Integer.parseInt(parsed.group(2)), "the notice must name the column whose dictionary breached");
    final String remedy = parsed.group(parsed.groupCount());
    assertTrue(remedy.contains("STALE") && remedy.contains("drop and commit this stale definition"),
        "the notice must say the projection is stale and require drop+commit: " + notices.getFirst());
    final ProjectionIndexMetadata metadata = metadata();
    assertNotNull(metadata, "the abandoned load must leave the tombstone behind");
    assertTrue(metadata.isStale(), "an abandoned projection must not read as live");
    assertEquals(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED, metadata.staleReason());
    assertEquals(RECORDS, committedRecordCount(), "the abandon must cost the projection, never the load");
  }

  @Test
  @DisplayName("mutation: with the feed-lane arm disabled the same breach kills the load")
  void withoutTheArmTheBreachPoisonsTheLoad() {
    GlobalValueDictionaryProbeFront.TEST_MAX_ENTRIES = FRONT_ENTRY_CAP;
    ProjectionBulkLoad.ABANDON_ON_FEED_BUDGET_BREACH = false;
    final RuntimeException failure = assertThrows(RuntimeException.class, this::loadParallel,
        "the pre-fix behaviour must fail the load, or the loud arm above proves nothing");
    assertTrue(hasCause(failure, GlobalDictionaryBudgetExceededException.class),
        "the load must fail on the budget breach itself, got: " + failure);
  }

  @Test
  @DisplayName("control: without the cap the coordinator-fed build publishes the global column silently")
  void insideTheBudgetNothingIsAnnounced() {
    final Capture capture = loadParallel();
    assertTrue(capture.linesContaining("PROJECTION ABANDONED").isEmpty(),
        "a healthy load must not announce an abandon; captured stderr was:\n" + capture.text());
    final ProjectionIndexMetadata metadata = metadata();
    assertNotNull(metadata, "the healthy load must publish metadata");
    assertTrue(!metadata.isStale(), "the healthy load must publish a live projection");
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, metadata.columnKinds()[0],
        "the corpus must actually elect a resource-wide dictionary, or the loud arm's breach could never happen");
    assertEquals(RECORDS, committedRecordCount());
  }

  private static boolean hasCause(final Throwable failure, final Class<? extends Throwable> type) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  /** One coordinator-fed load over the corpus, with everything written to stderr captured. */
  private Capture loadParallel() {
    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    System.setErr(new PrintStream(new TeeOutputStream(originalErr, sink), true, StandardCharsets.UTF_8));
    try {
      // The parallel importer accepts hashType=NONE only; the helper's default resource is ROLLING.
      Databases.createJsonDatabase(new DatabaseConfiguration(JsonTestHelper.PATHS.PATH1.getFile()));
      try (final Database<JsonResourceSession> creator =
          Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
        creator.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                                    .hashKind(HashType.NONE)
                                                    .useDeweyIDs(false)
                                                    .storeNodeHistory(false)
                                                    .buildPathSummary(true)
                                                    .build());
      }
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(1024, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        // -1: no row-count hint, the documented state in which the election cannot decline up front.
        controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, -1L);
        ParallelBulkJsonImporter.assemble(wtx, new ByteArrayInputStream(corpus().getBytes(StandardCharsets.UTF_8)));
        wtx.commit();
      }
    } finally {
      System.setErr(originalErr);
    }
    return new Capture(sink.toString(StandardCharsets.UTF_8));
  }

  private static long committedRecordCount() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild(), "the committed revision must hold the imported array");
      return rtx.getChildCount();
    }
  }

  private static ProjectionIndexMetadata metadata() {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final byte[] raw = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).getBlob(0L);
      return raw == null
          ? null
          : ProjectionIndexMetadata.parse(raw);
    }
  }

  private static IndexDef projectionDef() {
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
        List.of(Path.parse("/[]/code", PathParser.Type.JSON)), List.of(Type.STR), INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static void restoreProperty(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  private static String corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * (VALUE_BYTES + 16));
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"code\":\"").append(value(record / ROWS_PER_VALUE)).append("\"}");
    }
    return json.append(']').toString();
  }

  private static String value(final int ordinal) {
    final StringBuilder text = new StringBuilder(VALUE_BYTES);
    text.append('c').append(String.format("%08d", ordinal));
    while (text.length() < VALUE_BYTES) {
      text.append('x');
    }
    return text.toString();
  }

  private record Capture(String text) {
    private List<String> linesContaining(final String needle) {
      final List<String> matches = new ArrayList<>();
      for (final String line : text.split("\\R")) {
        if (line.contains(needle)) {
          matches.add(line);
        }
      }
      return matches;
    }
  }

  private static final class TeeOutputStream extends OutputStream {
    private final OutputStream first;
    private final OutputStream second;

    private TeeOutputStream(final OutputStream first, final OutputStream second) {
      this.first = first;
      this.second = second;
    }

    @Override
    public void write(final int b) throws IOException {
      first.write(b);
      second.write(b);
    }

    @Override
    public void write(final byte[] bytes, final int offset, final int length) throws IOException {
      first.write(bytes, offset, length);
      second.write(bytes, offset, length);
    }

    @Override
    public void flush() throws IOException {
      first.flush();
      second.flush();
    }
  }
}
