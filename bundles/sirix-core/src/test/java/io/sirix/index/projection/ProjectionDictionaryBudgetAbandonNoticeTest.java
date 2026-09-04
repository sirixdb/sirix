/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The one silent load-degradation that is not allowed to stay silent: a resource-wide value
 * dictionary that breaches its byte budget mid-load abandons the projection, and the operator has
 * to learn that from the process itself.
 *
 * <h2>What is under test</h2>
 *
 * The stderr line emitted by {@code ProjectionIndexChangeListener#abandonForOversizedDictionary} is
 * an OPERATOR-FACING OUTPUT CONTRACT, not an implementation detail: the load goes on to report
 * success, so without the notice a projection that never finished is indistinguishable from one
 * that did, and a long benchmark run is silently measured on the generic pipeline. It is
 * deliberately a {@code System.err.println} rather than a log call, because the shipped logback
 * configuration pins the root logger to ERROR and would swallow the warning that accompanies it.
 * The loud arm therefore runs with the root logger switched OFF — if the notice travelled by any
 * suppressible channel, that arm would capture nothing.
 *
 * <h2>How the breach is reached (no fault injection)</h2>
 *
 * Both arms run the real load-time build over the same corpus through
 * {@code createProjectionIndexAtLoadStart} plus the shredder, and differ only in the documented
 * {@code -Dsirix.projection.globalDict.budgetBytes} ceiling, which
 * {@link ProjectionIndexBuilder#globalDictionaryBudgetBytes()} reads per build. The corpus repeats
 * every value {@link #ROWS_PER_VALUE} times, which is under the election's per-leaf deduplication
 * factor, so AUTO promotes the column to a resource-wide dictionary after the leading sample — the
 * control arm asserts exactly that by checking the published column kind. The load is armed with no
 * row-count hint ({@code -1}), the documented state in which the election-time decline cannot run
 * and "the writer's runtime cap is the only protection"; see the {@code expectedRows} contract on
 * {@link ProjectionIndexBuilder#setExpectedRows(long)}. The writer/front component cap is
 * CALIBRATED from the real writer rather than hard-coded: it sits above what the sample's distinct
 * values cost to flush and below what the whole corpus costs. The configured combined envelope is
 * exactly twice that cap, so the election succeeds and a later value breaches without either
 * resident structure double-spending it. Both ends of that window are asserted before the load
 * runs, so drift in the writer's arithmetic fails as a calibration error instead of quietly turning
 * this into a test of nothing.
 */
@ResourceLock(value = Resources.SYSTEM_PROPERTIES, mode = ResourceAccessMode.READ_WRITE)
@ResourceLock(value = Resources.SYSTEM_ERR, mode = ResourceAccessMode.READ_WRITE)
final class ProjectionDictionaryBudgetAbandonNoticeTest {

  private static final int INDEX_NUMBER = 0;
  private static final String BUDGET_PROPERTY = "sirix.projection.globalDict.budgetBytes";
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";

  /** Enough row groups that the leading decision sample is only a fraction of the corpus. */
  private static final int RECORDS = 30_000;

  /**
   * Rows per distinct value. Below the election's minimum per-leaf deduplication factor of 4, so a
   * per-leaf dictionary is measured as "storing almost nothing twice" and the column goes global.
   */
  private static final int ROWS_PER_VALUE = 2;

  private static final int VALUE_BYTES = 24;

  /**
   * Distinct values the election measures, give or take a partly-filled row group: the builder holds
   * back 16 leading leaves of at most {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows. Used only to
   * calibrate the budget, whose margin absorbs the difference; the window assertions in
   * {@link #calibratedCombinedBudget()} are what guarantee the calibration is usable.
   */
  private static final int SAMPLE_DISTINCT = 16 * ProjectionIndexRowGroupPage.MAX_ROWS / ROWS_PER_VALUE;

  private static final int TOTAL_DISTINCT = RECORDS / ROWS_PER_VALUE;

  /**
   * The notice as an operator reads it. Groups: index, column, breaching bytes, the name of the term
   * that breached, entries, budget, retained, tail.
   *
   * <p>
   * The breaching quantity and the budget are BOTH in the line on purpose. Every byte-budget guard
   * weighs retention plus a reservation, so a notice quoting retention alone printed a number below
   * the budget it announced as exceeded — arithmetic an operator cannot reconcile, and no figure to
   * raise the budget to. The loud arm asserts the printed relation, not merely the presence of
   * numbers.
   * </p>
   */
  private static final Pattern NOTICE = Pattern.compile("^\\[proj] PROJECTION ABANDONED during the load: index (\\d+),"
      + " column (\\d+) needed (\\d+) B \\(([^)]+)\\) over (\\d+) distinct values, past its (\\d+) B budget"
      + " \\((\\d+) B retained\\)\\. (.*)$");

  private String priorBudget;
  private String priorMode;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
    priorBudget = System.getProperty(BUDGET_PROPERTY);
    priorMode = System.getProperty(MODE_PROPERTY);
    System.setProperty(MODE_PROPERTY, "auto");
  }

  @AfterEach
  void tearDown() {
    restoreProperty(BUDGET_PROPERTY, priorBudget);
    restoreProperty(MODE_PROPERTY, priorMode);
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /**
   * LOUD ARM. A dictionary that outgrows its budget after the election must announce the abandon on
   * stderr with the logger switched off, tombstone the projection with a machine-readable reason, and
   * let the load itself complete.
   */
  @Test
  void aDictionaryBudgetBreachAnnouncesTheAbandonedProjectionEvenWithLoggingOff() {
    final long combinedBudget = calibratedCombinedBudget();
    final long componentBudget = ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(combinedBudget);
    System.setProperty(BUDGET_PROPERTY, Long.toString(combinedBudget));

    final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    final Level priorLevel = rootLogger.getLevel();
    // Harsher than the shipped configuration's ERROR: nothing travelling by the logger can be
    // observed from here, so whatever the capture below sees came through the unsuppressible channel.
    rootLogger.setLevel(Level.OFF);
    final Capture capture;
    try {
      capture = load();
    } finally {
      rootLogger.setLevel(priorLevel);
    }

    final List<String> notices = capture.linesContaining("PROJECTION ABANDONED");
    assertEquals(1, notices.size(),
        "the abandon must be announced exactly once; captured stderr was:\n" + capture.text());
    final String notice = notices.getFirst();
    assertTrue(notice.startsWith("[proj] "),
        "the notice must reach stderr unprefixed, i.e. not through a log appender: " + notice);
    final Matcher parsed = NOTICE.matcher(notice);
    assertTrue(parsed.matches(),
        "the notice no longer states index, column, the breaching term and the budget: " + notice);
    assertEquals(INDEX_NUMBER, Integer.parseInt(parsed.group(1)), "the notice must name the abandoned index");
    assertEquals(0, Integer.parseInt(parsed.group(2)), "the notice must name the column whose dictionary breached");
    assertTrue(Integer.parseInt(parsed.group(5)) > 0, "the notice must report how many values had been admitted");
    assertEquals(componentBudget, Long.parseLong(parsed.group(6)),
        "the notice must quote the disjoint component cap that was breached");
    final String remedy = parsed.group(8);
    assertTrue(
        remedy.contains("STALE") && remedy.contains("drop and commit this stale definition")
            && remedy.contains("replacement in a new projection tree"),
        "the notice must say the projection is stale and require drop+commit before a fresh-tree replacement: "
            + notice);

    // THE POINT OF THE ARITHMETIC: the quantity the line announces as breaching must actually exceed
    // the budget the same line quotes. Reporting retention instead put a smaller number on the left
    // of a "past its ... budget" claim on every real breach.
    final long breachingBytes = Long.parseLong(parsed.group(3));
    final long retainedBytes = Long.parseLong(parsed.group(7));
    assertTrue(breachingBytes > componentBudget,
        "the notice announces a breach with a quantity that does NOT exceed the component cap it quotes ("
            + breachingBytes + " B vs " + componentBudget + " B): " + notice);
    assertFalse(parsed.group(4).isBlank(), "the breaching quantity must be named, or it cannot be acted on: " + notice);
    assertTrue(retainedBytes <= breachingBytes, "retention cannot exceed the retention-plus-reservation term that was"
        + " weighed against the budget: " + notice);

    final ProjectionIndexMetadata metadata = metadata();
    assertNotNull(metadata, "the abandoned load must leave the tombstone behind, not an empty slot");
    assertTrue(metadata.isStale(), "an abandoned projection must not read as live");
    assertEquals(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED, metadata.staleReason(),
        "the tombstone must carry the machine-readable reason the notice describes");

    // "The load completes" is the other half of the contract: the ingest must survive the decline.
    assertEquals(RECORDS, committedRecordCount(), "the abandon must cost the projection, never the load");
  }

  /**
   * CONTROL ARM. The same corpus with the aggregate budget disabled must publish a live resource-wide
   * dictionary column and say nothing at all — which is what makes the loud arm a test of the BREACH
   * rather than of a corpus that would have complained either way.
   */
  @Test
  void aLoadThatStaysInsideItsBudgetPublishesTheGlobalColumnSilently() {
    System.setProperty(BUDGET_PROPERTY, Long.toString(Long.MAX_VALUE));

    final Capture capture = load();

    assertTrue(capture.linesContaining("PROJECTION ABANDONED").isEmpty(),
        "a healthy load must not announce an abandon; captured stderr was:\n" + capture.text());

    final ProjectionIndexMetadata metadata = metadata();
    assertNotNull(metadata, "the healthy load must publish metadata");
    assertFalse(metadata.isStale(), "the healthy load must publish a live projection");
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, metadata.columnKinds()[0],
        "the corpus must actually elect a resource-wide dictionary, or the loud arm's breach could never happen");
    assertEquals(RECORDS, committedRecordCount(), "the healthy load must index every record");
  }

  /**
   * A per-component cap above the sample's flush peak and below the whole corpus's, doubled into the
   * configured writer/front envelope: the election admits the column, and a later value cannot fit.
   * Measured on the real writer, so the window is expressed in the same arithmetic the load will use.
   */
  private static long calibratedCombinedBudget() {
    final long samplePeak = flushPeakForDistinctValues(SAMPLE_DISTINCT);
    final long corpusPeak = flushPeakForDistinctValues(TOTAL_DISTINCT);
    final long componentBudget = Math.multiplyExact(samplePeak, 5L) / 4L;
    assertTrue(componentBudget >= GlobalValueDictionaryWriter.MINIMUM_BUDGET_BYTES,
        "calibration produced a component cap below what an empty dictionary already retains: " + componentBudget);
    assertTrue(corpusPeak > componentBudget,
        "the corpus can no longer outgrow a component cap the sample fits in (sample peak " + samplePeak
            + " B, corpus peak " + corpusPeak + " B) — recalibrate the corpus, this test would prove nothing");
    final long combinedBudget = Math.multiplyExact(componentBudget, 2L);
    assertEquals(componentBudget, ProjectionIndexBuilder.streamingGlobalDictionaryComponentBudget(combinedBudget),
        "combined calibration must give each simultaneously resident structure the measured component cap");
    return combinedBudget;
  }

  private static void restoreProperty(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  /** What a dictionary holding the first {@code distinct} corpus values costs at flush time. */
  private static long flushPeakForDistinctValues(final int distinct) {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter(0, Long.MAX_VALUE);
    try {
      for (int ordinal = 0; ordinal < distinct; ordinal++) {
        writer.intern(value(ordinal));
      }
      return writer.estimatedFlushPeakBytes();
    } finally {
      writer.release();
    }
  }

  /** One load-time build over the corpus, with everything the process wrote to stderr captured. */
  private static Capture load() {
    final PrintStream originalErr = System.err;
    final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    System.setErr(new PrintStream(new TeeOutputStream(originalErr, sink), true, StandardCharsets.UTF_8));
    try {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
        // -1: no row-count hint, the documented state in which the election cannot decline up front.
        controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, -1L);
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(corpus()), JsonNodeTrx.Commit.NO);
        wtx.commit();
      }
    } finally {
      System.setErr(originalErr);
    }
    return new Capture(sink.toString(StandardCharsets.UTF_8));
  }

  /** Records in the committed resource — the load's own outcome, independent of the projection. */
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

  /** Fixed-width distinct values, so the dictionary's growth is a function of the ordinal alone. */
  private static String value(final int ordinal) {
    final StringBuilder text = new StringBuilder(VALUE_BYTES);
    text.append('c').append(String.format("%08d", ordinal));
    while (text.length() < VALUE_BYTES) {
      text.append('x');
    }
    return text.toString();
  }

  /** Captured stderr, read as the lines an operator would see. */
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

  /** Keeps the real stderr readable while an arm records what was written to it. */
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
