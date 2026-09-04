package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionTemporalCodec;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Declared {@code timestamp} / {@code date} projection columns, against the interpreter.
 *
 * <p>
 * The columns store epochs, not text, so EVERY answer that shows a value is a formatter output and
 * every comparison against a string literal is a rewritten numeric one. Both are only correct if
 * they are indistinguishable from the interpreter's, which reads the document's own bytes — so
 * every assertion here is a differential, run with {@link SirixVectorizedExecutor#STRICT_SERVING}
 * on so a fail-soft decline cannot answer in the kernels' place and look like agreement.
 *
 * <p>
 * The kill switch has its own class ({@code TemporalColumnKillSwitchTest}), because the property is
 * read once per JVM.
 */
final class TemporalColumnDifferentialTest {
  private static final String DB = "temporal-col-db";
  private static final String RES = "records.jn";
  private static final int N = 6_000;
  static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  /** 2024-01-01T00:00:00Z. */
  private static final long BASE_EPOCH = 1_704_067_200L;

  private Path dbDir;

  /**
   * The fixture's row {@code i}: a timestamp spread over a year (non-monotonic in document order so
   * an order-by has work to do), the date it falls on, a URL and a long.
   */
  static long epochOf(final int i) {
    // 96 619 is coprime with the year's second count, so the values are a permutation-like spread
    // and every one of them is distinct.
    return BASE_EPOCH + (long) i * 96_619L % 31_536_000L;
  }

  static String json(final int rows) {
    final StringBuilder sb = new StringBuilder(rows * 96);
    sb.append('[');
    final byte[] scratch = new byte[ProjectionTemporalCodec.MAX_TEXT_LENGTH];
    for (int i = 0; i < rows; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final long epoch = epochOf(i);
      final int tsLength = ProjectionTemporalCodec.formatTimestamp(epoch, scratch, 0);
      final String timestamp = new String(scratch, 0, tsLength, java.nio.charset.StandardCharsets.UTF_8);
      sb.append("{\"t\":\"")
        .append(timestamp)
        .append("\",\"d\":\"")
        .append(timestamp, 0, 10)
        .append("\",\"u\":\"")
        .append(i % 17 == 0
            ? "http://www.google.com/q"
            : "http://site" + i % 401 + ".example/p")
        .append("\",\"v\":")
        .append((i * 31) % 20_011)
        .append('}');
    }
    return sb.append(']').toString();
  }

  @BeforeEach
  void setUp() throws Exception {
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-temporal-col-");
    createFixture(dbDir, json(N));
  }

  static void createFixture(final Path dir, final String json) {
    try (var store = BasicJsonDBStore.newBuilder().location(dir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + json + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/t', '/[]/d', '/[]/u', '/[]/v'),
            ('timestamp', 'date', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("the declaration builds TIMESTAMP and DATE columns, not string dictionaries")
  void declaredTypesBecomeTemporalColumns() throws Exception {
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
              session.getMostRecentRevisionNumber(), new String[] {"[]"}, new String[] {"t", "d", "u", "v"});
      assertNotNull(handle, "the projection must be loadable");
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, handle.columnKindOf(handle.columnOf("t")));
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_DATE, handle.columnKindOf(handle.columnOf("d")));
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, handle.columnKindOf(handle.columnOf("u")));
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, handle.columnKindOf(handle.columnOf("v")));
    }
  }

  @Test
  @DisplayName("every stored value emits the document's exact bytes back")
  void everyValueMakesTheRoundTrip() throws Exception {
    // The predicate matches every row, so the served route formats all N timestamps and all N dates
    // and the interpreter reads all N from the records: any formatter defect shows as a diff.
    assertDifferential("for $h in " + DOC + " where $h.v ge 0 return $h.t");
    assertDifferential("for $h in " + DOC + " where $h.v ge 0 return $h.d");
    // And through the covered-row materialisation, where the value lands inside a record.
    assertDifferential("for $h in " + DOC + " where $h.v lt 40 return {\"t\": $h.t, \"d\": $h.d, \"v\": $h.v}");
  }

  @Test
  @DisplayName("a sorted scan with LIMIT on the timestamp agrees with the interpreter")
  void sortedScanOnTheTimestamp() throws Exception {
    assertDifferentialServed("subsequence(for $h in " + DOC + " order by $h.t return $h.t, 1, 25)",
        SirixVectorizedExecutor::sortedScanServedCount);
    assertDifferentialServed("subsequence(for $h in " + DOC + " order by $h.t descending return $h.t, 1, 25)",
        SirixVectorizedExecutor::sortedScanServedCount);
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " where contains($h.u, 'google') order by $h.t " + "return $h.t, 1, 12)",
        SirixVectorizedExecutor::sortedScanServedCount);
    // Whole records: the winners are materialised from the trie, so this is where a sort key that
    // ordered the epoch differently from the text would show up as a different record.
    assertDifferentialServed("subsequence(for $h in " + DOC + " order by $h.t return $h, 1, 15)",
        SirixVectorizedExecutor::sortedScanServedCount);
    // Two keys, the date first: a date column's day granularity makes ties, and the second key
    // resolves them exactly as it does for the interpreter.
    assertDifferentialServed("subsequence(for $h in " + DOC + " order by $h.d, $h.v return $h, 1, 20)",
        SirixVectorizedExecutor::sortedScanServedCount);
  }

  @Test
  @DisplayName("min/max of the date and the timestamp agree with the interpreter")
  void ungroupedMinMax() throws Exception {
    assertDifferential("min(for $h in " + DOC + " return $h.d)");
    assertDifferential("max(for $h in " + DOC + " return $h.d)");
    assertDifferential("min(for $h in " + DOC + " return $h.t)");
    assertDifferential("max(for $h in " + DOC + " return $h.t)");
  }

  @Test
  @DisplayName("grouping on a date, on a timestamp and on ISO substrings agrees with the interpreter")
  void groupByTemporalKeys() throws Exception {
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " let $k := $h.d group by $k let $c := count($h) "
            + "order by $c descending, $k ascending return {\"k\": $k, \"c\": $c}, 1, 12)",
        SirixVectorizedExecutor::groupAggServedCount);
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " let $k := $h.t group by $k let $c := count($h) "
            + "order by $k ascending return {\"k\": $k, \"c\": $c}, 1, 12)",
        SirixVectorizedExecutor::groupAggServedCount);
    // The ISO substring windows, all arithmetic on the epoch. Uncapped, so the arm emits every
    // group; a LEADING window now carries an in-kernel order plan and the wrapper TRUSTS that order,
    // while a field window still emits first-appearance order for the wrapper to sort. Either way
    // the differential compares the whole sequence against the interpreter's.
    assertDifferentialServed("for $h in " + DOC + " where $h.d ge '2024-06-01' and $h.d lt '2024-06-04' "
        + "let $k := substring($h.t, 1, 16) group by $k let $c := count($h) order by $k ascending "
        + "return {\"k\": $k, \"c\": $c}", SirixVectorizedExecutor::groupAggServedCount);
    assertDifferentialServed(
        "for $h in " + DOC + " let $k := substring($h.t, 15, 2) group by $k "
            + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}",
        SirixVectorizedExecutor::groupAggServedCount);
    // The date part of a timestamp, and the hour of the day.
    assertDifferentialServed(
        "for $h in " + DOC + " let $k := substring($h.t, 1, 10) group by $k "
            + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}",
        SirixVectorizedExecutor::groupAggServedCount);
    assertDifferentialServed(
        "for $h in " + DOC + " let $k := substring($h.t, 12, 2) group by $k "
            + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}",
        SirixVectorizedExecutor::groupAggServedCount);
    // The CAST variant emits the integer the window spells, not its two characters — a key type the
    // formatter must stay out of.
    assertDifferentialServed(
        "for $h in " + DOC + " let $k := xs:integer(substring($h.t, 15, 2)) group by $k "
            + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}",
        SirixVectorizedExecutor::groupAggServedCount);
  }

  @Test
  @DisplayName("a monotonic key transform orders in kernel; a modulus, a shift and a cast keep declining")
  void monotonicKeyTransformOrdersInKernel() throws Exception {
    // (a)/(b) A LEADING ISO window is pure truncation of the epoch, so the group key orders as the
    // epoch does and the arm heap-selects the first `start + length - 1` groups instead of
    // materialising all of them. The subsequence carries an OFFSET, so the served sequence has to
    // agree with the interpreter's beyond the first emitted group as well.
    for (final int window : new int[] {16, 13, 10}) {
      assertDifferentialServed(
          "subsequence(for $h in " + DOC + " let $k := substring($h.t, 1, " + window + ") "
              + "group by $k let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}, 21, 10)",
          SirixVectorizedExecutor::groupAggServedCount);
      assertDifferentialServed(
          "subsequence(for $h in " + DOC + " let $k := substring($h.t, 1, " + window + ") "
              + "group by $k let $c := count($h) order by $k descending return {\"k\": $k, \"c\": $c}, 21, 10)",
          SirixVectorizedExecutor::groupAggServedCount);
    }
    // The key as a TIE-BREAKER behind an aggregate spec: the day window makes many groups share a
    // count, and every one of those ties is resolved by the key — in kernel, on the transformed
    // long, against the interpreter's comparison of the rendered text.
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " let $k := substring($h.t, 1, 10) group by $k "
            + "let $c := count($h) order by $c descending, $k ascending return {\"k\": $k, \"c\": $c}, 11, 12)",
        SirixVectorizedExecutor::groupAggServedCount);
    // The rule is about the ARITHMETIC, not about timestamps: a bare `idiv` over a numeric column is
    // the same monotonic truncation and orders in kernel on the integer it emits.
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " let $k := $h.v idiv 100 group by $k "
            + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}, 6, 8)",
        SirixVectorizedExecutor::groupAggServedCount);
    // (e) An order spec over an AGGREGATE was servable under a transformed key before and still is —
    // the key gate must not have narrowed anything.
    assertDifferentialServed(
        "subsequence(for $h in " + DOC + " let $k := substring($h.t, 1, 10) group by $k "
            + "let $c := count($h) order by $c descending, $k descending return {\"k\": $k, \"c\": $c}, 1, 9)",
        SirixVectorizedExecutor::groupAggServedCount);

    // (c)/(d) The shapes the gate refuses. Each is CAPPED: uncapped, a transformed key without a
    // plan still serves through the deferred-order arm, so only a cap makes the refusal observable.
    // The answers stay right because the interpreter produces them — which is why the decline is
    // asserted on the counters and not on the answer.
    // A two-digit FIELD window is `(epoch idiv 60) mod 60`: a modulus, so the transform is not
    // monotonic in the value it is derived from.
    assertDifferentialDeclined("subsequence(for $h in " + DOC + " let $k := substring($h.t, 15, 2) group by $k "
        + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}, 21, 10)");
    assertDifferentialDeclined("subsequence(for $h in " + DOC + " let $k := substring($h.t, 12, 2) group by $k "
        + "let $c := count($h) order by $k descending return {\"k\": $k, \"c\": $c}, 3, 6)");
    // The CAST variant of a field window carries the same modulus.
    assertDifferentialDeclined("subsequence(for $h in " + DOC + " let $k := xs:integer(substring($h.t, 15, 2)) "
        + "group by $k let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}, 5, 10)");
    // A SHIFTED key is a transform with no div/mod at all — outside the arithmetic the gate reasons
    // about, so it keeps declining. (A shift COMPOSED with a division has no witness here: the
    // detection stage never emits one, and the arm's own idiv gate refuses the pair before the order
    // plan is reached — so the gate's offset clause is a second lock on a door already shut.)
    assertDifferentialDeclined("subsequence(for $h in " + DOC + " let $k := $h.v + 5 group by $k "
        + "let $c := count($h) order by $k ascending return {\"k\": $k, \"c\": $c}, 4, 7)");
  }

  @Test
  @DisplayName("grouped count(distinct-values(timestamp)) agrees with the interpreter")
  void groupedCountDistinctOverTheTimestamp() throws Exception {
    assertDifferentialServed("subsequence(for $h in " + DOC + " let $k := $h.d group by $k "
        + "let $n := count(distinct-values($h.t)) order by $n descending, $k ascending "
        + "return {\"k\": $k, \"n\": $n}, 1, 12)", SirixVectorizedExecutor::groupAggServedCount);
  }

  @Test
  @DisplayName("full and prefix literals compare exactly on both kinds")
  void predicateLiterals() throws Exception {
    final byte[] scratch = new byte[ProjectionTemporalCodec.MAX_TEXT_LENGTH];
    final int length = ProjectionTemporalCodec.formatTimestamp(epochOf(4_321), scratch, 0);
    final String full = new String(scratch, 0, length, java.nio.charset.StandardCharsets.UTF_8);
    final String day = full.substring(0, 10);
    final String month = full.substring(0, 7);
    for (final String query : List.of(
        // FULL literals: eq / ne / lt / le / gt / ge on both kinds.
        "count(for $h in " + DOC + " where $h.t eq '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ne '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t lt '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t le '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t gt '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ge '" + full + "' return $h)",
        "count(for $h in " + DOC + " where $h.d eq '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.d ge '" + day + "' return $h)",
        // PREFIX literals: a day against a timestamp, a month and a year against a date.
        "count(for $h in " + DOC + " where $h.t eq '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ne '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t lt '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t le '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t gt '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ge '" + day + "' return $h)",
        "count(for $h in " + DOC + " where $h.t ge '" + month + "' return $h)",
        "count(for $h in " + DOC + " where $h.d eq '" + month + "' return $h)",
        "count(for $h in " + DOC + " where $h.d lt '" + month + "' return $h)",
        "count(for $h in " + DOC + " where $h.d ge '2024' return $h)",
        "count(for $h in " + DOC + " where $h.d lt '2025' return $h)",
        // Ranges and conjunctions, plus a value emission behind one.
        "count(for $h in " + DOC + " where $h.t ge '" + day + "' and $h.t lt '" + full + "' return $h)",
        "for $h in " + DOC + " where $h.t eq '" + full + "' return $h.d",
        "subsequence(for $h in " + DOC + " where $h.d eq '" + day + "' order by $h.t return $h.t, 1, 30)")) {
      assertDifferential(query);
    }
  }

  @Test
  @DisplayName("a literal that is not a canonical value or unit prefix still answers — through the interpreter")
  void unexpressibleLiteralsStillAnswer() throws Exception {
    // Every one of these declines the numeric arm; STRICT_SERVING is off for this test only,
    // because a decline is the CORRECT outcome and the point is that the answer stays right.
    SirixVectorizedExecutor.STRICT_SERVING = false;
    for (final String literal : List.of("2024-0", "2024-07-15T", "2024-07-15 10:00:00", "2024-13", "nonsense")) {
      assertDifferential("count(for $h in " + DOC + " where $h.t ge '" + literal + "' return $h)");
      assertDifferential("count(for $h in " + DOC + " where $h.d eq '" + literal + "' return $h)");
    }
    // Containment over a temporal column is not a question about a point on the timeline.
    assertDifferential("count(for $h in " + DOC + " where contains($h.t, '2024-03') return $h)");
  }

  @Test
  @DisplayName("a value that is not exactly canonical fails the build with an actionable message")
  void nonCanonicalValueFailsTheBuild() throws Exception {
    for (final String[] bad : new String[][] {{"t", "2013-7-15T10:00:00"}, {"t", "2013-07-15 10:00:00"},
        {"d", "2013-7-15"}, {"d", "2013-07-15T00:00:00"}}) {
      final Path dir = Files.createTempDirectory("sirix-temporal-bad-");
      try {
        final String json = "[{\"t\":\"2024-01-01T00:00:00\",\"d\":\"2024-01-01\",\"u\":\"a\",\"v\":1}," + "{\"t\":\""
            + ("t".equals(bad[0])
                ? bad[1]
                : "2024-01-02T00:00:00")
            + "\",\"d\":\"" + ("d".equals(bad[0])
                ? bad[1]
                : "2024-01-02")
            + "\",\"u\":\"b\",\"v\":2}]";
        final Throwable failure = assertThrows(Throwable.class, () -> createFixture(dir, json),
            "a non-canonical " + bad[0] + " value '" + bad[1] + "' must fail the build");
        final String message = rootMessage(failure);
        assertTrue(message.contains(bad[1]), "the build error must name the offending value: " + message);
        assertTrue(message.contains("declared xs:date"), "the build error must name the declared shape: " + message);
      } finally {
        deleteQuietly(dir);
      }
    }
  }

  private static String rootMessage(final Throwable failure) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable t = failure; t != null && sb.length() < 8_000; t = t.getCause()) {
      if (t.getMessage() != null) {
        sb.append(t.getMessage()).append(" | ");
      }
      for (final Throwable suppressed : t.getSuppressed()) {
        if (suppressed.getMessage() != null) {
          sb.append(suppressed.getMessage()).append(" | ");
        }
      }
    }
    return sb.toString();
  }

  private static void deleteQuietly(final Path dir) {
    try (var walk = Files.walk(dir)) {
      walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.deleteIfExists(p);
        } catch (final Exception ignored) {
          // best effort
        }
      });
    } catch (final Exception ignored) {
      // best effort
    }
  }

  private void assertDifferential(final String query) throws Exception {
    assertEquals(run(query, false), run(query, true), "served answer diverges for: " + query);
  }

  /** As above, and the named counter must have moved — otherwise the agreement is vacuous. */
  private void assertDifferentialServed(final String query, final java.util.function.LongSupplier counter)
      throws Exception {
    final String generic = run(query, false);
    final long before = counter.getAsLong();
    assertEquals(generic, run(query, true), "served answer diverges for: " + query);
    assertTrue(counter.getAsLong() > before, "not served by the projection route: " + query);
  }

  /**
   * The answer still agrees, and the group-aggregate route REFUSED it: a gate counted a decline and
   * no group-aggregate serve happened. Asserting the answer alone would prove nothing here — the
   * interpreter produces it either way, which is exactly what makes an accidentally admitted shape
   * invisible without the counters.
   */
  private void assertDifferentialDeclined(final String query) throws Exception {
    final String generic = run(query, false);
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long declinedBefore = SirixVectorizedExecutor.groupAggregateDeclinedCount();
    assertEquals(generic, run(query, true), "answer diverges for the declining shape: " + query);
    assertEquals(servedBefore, SirixVectorizedExecutor.groupAggServedCount(),
        "the group-aggregate route must not serve: " + query);
    assertTrue(SirixVectorizedExecutor.groupAggregateDeclinedCount() > declinedBefore,
        "the group-aggregate route must count a decline: " + query);
  }

  String run(final String query, final boolean vectorized) throws Exception {
    return run(dbDir, query, vectorized);
  }

  static String run(final Path dir, final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
