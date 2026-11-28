/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.Decline;
import io.sirix.query.scan.QrySeam.Refusal;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.lemire;
import static io.sirix.query.scan.QryCorpus.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-201 to QRY-204, QRY-304, QRY-503, QRY-504 and QRY-601: every query of the chapter's corpus
 * table runs with the interpreter (the oracle) and with the route, and the two outcomes, result
 * type and value or error code, must be equal. For every row the test also asserts which of
 * QRY-304's outcomes occurred (served, page-declined or refused) against the set the chapter allows
 * for that row, and that the route was reached at all.
 *
 * <p>
 * Each row is a base corpus of 1,500 Lemire-shaped records (about 14 pages, so most records are
 * served) with the row's probe records placed in the middle. The {@code T} rows run twice: once in
 * a record that satisfies the rest of the conjunction and once in one that does not (QRY-203).
 *
 * <p>
 * Allowed outcomes follow the chapter: rows whose in-scope values the route models are served; rows
 * whose values it does not model on a page are page-declined or refused (QRY-203); a page holding a
 * decimal {@code score} is declined while a contributing double refuses the sum (QRY-204, QRY-304);
 * a member path that another structure reaches is refused or decided by the record path (QRY-301,
 * row E6). Where a row could either be served or have its page declined without breaking a
 * requirement, both are allowed. Exceptional values skipped by the interpreter can also be served;
 * the result or error must still equal the interpreter's outcome.
 *
 * <p>
 * {@link #interpreterObservations} is not planned: it records the interpreter's results for T2, T3
 * and the sum of T5, as QRY-203's gap note asks, and keeps that record honest from today.
 */
final class RegionPredicatedAggregateDifferentialTest {

  private static final String DB = "qry-differential-db";
  private static final int BASE_RECORDS = 1_500;
  private static final int PROBE_AT = 700;
  private static final long SEED = 0x9E3779B97F4A7C15L;
  private static final int THREADS = 2;
  private static final String W = "$r.active and $r.user.tags[] = \"admin\"";

  private static final Set<RouteOutcome> SERVED = EnumSet.of(RouteOutcome.SERVED);
  private static final Set<RouteOutcome> SERVED_OR_DECLINED =
      EnumSet.of(RouteOutcome.SERVED, RouteOutcome.PAGE_DECLINED);
  private static final Set<RouteOutcome> REACHED =
      EnumSet.of(RouteOutcome.SERVED, RouteOutcome.PAGE_DECLINED, RouteOutcome.REFUSED);
  private static final Set<RouteOutcome> NOT_SERVED = EnumSet.of(RouteOutcome.PAGE_DECLINED, RouteOutcome.REFUSED);
  private static final Set<RouteOutcome> DECLINED = EnumSet.of(RouteOutcome.PAGE_DECLINED);
  private static final Set<RouteOutcome> REFUSED = EnumSet.of(RouteOutcome.REFUSED);

  private static Path dir;
  private static QryFixture fixture;
  private static final Set<String> STORED = Collections.synchronizedSet(new HashSet<>());

  @BeforeAll
  static void createDatabase() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-differential-");
    fixture = new QryFixture(dir, DB);
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  // -----------------------------------------------------------------------------------------------
  // The corpus table
  // -----------------------------------------------------------------------------------------------

  /** One corpus row: probe records, the where clause, and the outcomes allowed per form. */
  record Row(String id, String what, List<String> probes, String where, Map<Form, Set<RouteOutcome>> allowed) {

    String resource() {
      // The id's punctuation is dropped, so a hash keeps "T2+\"\"" and "T2+[]" apart.
      return id.replaceAll("[^A-Za-z0-9]", "_") + "-" + Integer.toHexString(id.hashCode()) + ".jn";
    }

    @Override
    public String toString() {
      return id + " " + what;
    }
  }

  private static Map<Form, Set<RouteOutcome>> all(final Set<RouteOutcome> allowed) {
    final Map<Form, Set<RouteOutcome>> map = new EnumMap<>(Form.class);
    for (final Form form : Form.values()) {
      map.put(form, allowed);
    }
    return map;
  }

  private static Map<Form, Set<RouteOutcome>> sumAndCounts(final Set<RouteOutcome> sum,
      final Set<RouteOutcome> counts) {
    final Map<Form, Set<RouteOutcome>> map = new EnumMap<>(Form.class);
    map.put(Form.SUM, sum);
    map.put(Form.COUNT_RECORDS, counts);
    map.put(Form.COUNT_FIELD, counts);
    return map;
  }

  private static Row row(final String id, final String what, final List<String> probes,
      final Map<Form, Set<RouteOutcome>> allowed) {
    return new Row(id, what, probes, W, allowed);
  }

  private static String probe(final String active, final List<String> tags, final String score) {
    return lemire(-1, active, tags, score);
  }

  private static List<String> tags(final String... jsonElements) {
    return List.of(jsonElements);
  }

  static List<Row> corpus() {
    final List<Row> rows = new ArrayList<>();
    final String a = str(ADMIN);
    final String x = str("guest");
    // --- QRY-202: existential per-record semantics ---
    rows.add(row("E1", "the literal once in the array", List.of(probe("true", tags(a), "777")), all(SERVED)));
    rows.add(row("E2", "the literal twice or more in the same array",
        List.of(probe("true", tags(a, a), "777"), probe("true", tags(a, x, a, a), "778")), all(SERVED)));
    rows.add(row("E3a", "non-matching elements only", List.of(probe("true", tags(x, str("dev")), "777")), all(SERVED)));
    rows.add(row("E3b", "an empty array", List.of(probe("true", tags(), "777")), all(SERVED)));
    rows.add(row("E4a", "tags missing",
        List.of("{\"id\":-1,\"active\":true,\"user\":{\"name\":\"p\"},\"score\":777,\"note\":\"n\"}"),
        all(SERVED_OR_DECLINED)));
    rows.add(row("E4b", "user missing", List.of("{\"id\":-1,\"active\":true,\"score\":777,\"note\":\"n\"}"),
        all(SERVED_OR_DECLINED)));
    rows.add(row("E4c", "user an empty object",
        List.of("{\"id\":-1,\"active\":true,\"user\":{},\"score\":777,\"note\":\"n\"}"), all(SERVED_OR_DECLINED)));
    rows.add(row("E5", "tags is the string L itself",
        List.of(
            "{\"id\":-1,\"active\":true,\"user\":{\"name\":\"p\",\"tags\":\"admin\"},\"score\":777,\"note\":\"n\"}"),
        all(SERVED_OR_DECLINED)));
    rows.add(row("E6", "user is an array of objects that carry tags",
        List.of("{\"id\":-1,\"active\":true,\"user\":[{\"name\":\"p\",\"tags\":[\"admin\"]},{\"tags\":[\"guest\"]}],"
            + "\"score\":777,\"note\":\"n\"}"),
        all(NOT_SERVED)));
    rows.add(row("E7a", "prefix, suffix and case variants of L",
        List.of(probe("true", tags(str("admi"), str("dmin"), str("Admin"), str("admins"), str("ADMIN")), "777")),
        all(SERVED)));
    rows.add(
        new Row(
            "E7b", "L is the empty string", List.of(probe("true", tags(str(""), x), "777"),
                probe("true", tags(x), "778"), probe("true", tags(str(" ")), "9")),
            "$r.active and $r.user.tags[] = \"\"", all(SERVED)));
    rows.add(new Row(
        "E7c", "a non-ASCII L, raw and escaped in the JSON", List.of(probe("true", tags(str("café")), "777"),
            probe("true", tags("\"caf\\u00e9\""), "778"), probe("true", tags(str("cafe")), "779")),
        "$r.active and $r.user.tags[] = \"café\"", all(SERVED)));
    rows.add(new Row("E7d", "an L with an escaped quote and backslash",
        List.of(probe("true", tags("\"a\\\"b\\\\c\""), "777"), probe("true", tags("\"a\\\"b\""), "778")),
        "$r.active and $r.user.tags[] = \"a\"\"b\\c\"", all(SERVED)));
    rows.add(row("E8a", "an array of more than 1024 elements, literal last",
        List.of(probe("true", longArray(1_500, false), "777")), all(SERVED_OR_DECLINED)));
    rows.add(row("E8b", "an array of more than 1024 elements, literal first",
        List.of(probe("true", longArray(1_500, true), "777")), all(SERVED_OR_DECLINED)));
    rows.add(new Row("E9a", "the literal on no page at all", List.of(probe("true", tags(x), "777")),
        "$r.active and $r.user.tags[] = \"nobody\"", all(SERVED)));
    rows.add(new Row("E9b", "the literal on exactly one page", List.of(probe("true", tags(str("solo")), "777")),
        "$r.active and $r.user.tags[] = \"solo\"", all(SERVED)));
    // --- QRY-203: missing, NULL and type-mismatch values, in a matching (+) and a non-matching (-)
    // record ---
    for (final boolean match : new boolean[] {true, false}) {
      final String sign = match
          ? "+"
          : "-";
      final List<String> member = match
          ? tags(a)
          : tags(x);
      rows.add(row("T1" + sign, "flag field missing", List.of(probe(null, member, "777")), all(SERVED_OR_DECLINED)));
      for (final String flag : new String[] {"null", "\"x\"", "\"\"", "0", "2", "{\"a\":1}", "[true]", "[]"}) {
        rows.add(row("T2" + sign + flag, "flag field " + flag, List.of(probe(flag, member, "777")), all(NOT_SERVED)));
      }
      final String active = match
          ? "true"
          : "false";
      for (final String element : new String[] {"1", "null", "true", "[\"admin\"]", "{\"a\":\"admin\"}"}) {
        rows.add(row("T3" + sign + "before" + element, "element " + element + " before the matching element",
            List.of(probe(active, tags(element, a), "777")), all(match
                ? NOT_SERVED
                : REACHED)));
        rows.add(row("T3" + sign + "after" + element, "element " + element + " after the matching element",
            List.of(probe(active, tags(a, element), "777")), all(REACHED)));
      }
      rows.add(row("T4" + sign, "N missing", List.of(probe(active, tags(a), null)), all(SERVED_OR_DECLINED)));
      for (final String n : new String[] {"null", "\"7\"", "true", "{\"a\":1}", "[1]"}) {
        rows.add(row("T5" + sign + n, "N is " + n, List.of(probe(active, tags(a), n)), sumAndCounts(match
            ? NOT_SERVED
            : REACHED, SERVED_OR_DECLINED)));
      }
      rows.add(row("T6" + sign + "decimal", "N a decimal", List.of(probe(active, tags(a), "1.5")), sumAndCounts(match
          ? DECLINED
          : REACHED, SERVED_OR_DECLINED)));
      rows.add(row("T6" + sign + "double", "N a shredder-typed double", List.of(probe(active, tags(a), "1.5e0")),
          sumAndCounts(match
              ? REFUSED
              : REACHED, SERVED_OR_DECLINED)));
    }
    rows.add(row("T7", "the same field names nested elsewhere in the record", List.of(
        "{\"id\":-1,\"active\":true,\"user\":{\"name\":\"p\",\"active\":false,\"score\":5,\"tags\":[\"admin\"]},"
            + "\"profile\":{\"tags\":[\"admin\"]},\"score\":777,\"note\":\"n\"}",
        "{\"id\":-2,\"user\":{\"name\":\"p\",\"active\":true,\"score\":6,\"tags\":[\"admin\"]},\"score\":778,\"note\":\"n\"}",
        "{\"id\":-3,\"active\":true,\"user\":{\"name\":\"p\",\"tags\":[\"guest\"]},\"profile\":{\"tags\":[\"admin\"]},"
            + "\"score\":779,\"note\":\"n\"}",
        "{\"id\":-4,\"active\":true,\"user\":{\"name\":\"p\",\"score\":8,\"tags\":[\"admin\"]},\"note\":\"n\"}"),
        all(SERVED_OR_DECLINED)));
    rows.add(row("T8", "records whose keys come in different orders", List.of(
        "{\"id\":-1,\"active\":true,\"user\":{\"tags\":[\"admin\"],\"name\":\"p\"},\"score\":777,\"note\":\"n\"}",
        "{\"id\":-2,\"active\":true,\"score\":778,\"note\":\"n\",\"user\":{\"name\":\"p\",\"tags\":[\"admin\"]}}",
        "{\"id\":-3,\"user\":{\"name\":\"p\",\"tags\":[\"admin\"]},\"active\":true,\"score\":779,\"note\":\"n\"}",
        "{\"score\":780,\"user\":{\"tags\":[\"admin\",\"guest\"],\"name\":\"p\"},\"note\":\"n\",\"active\":true,\"id\":-4}"),
        all(SERVED_OR_DECLINED)));
    // --- QRY-204: numeric fold exactness ---
    rows.add(row("N1", "values near Long.MAX_VALUE and Long.MIN_VALUE whose total overflows a long",
        List.of(probe("true", tags(a), Long.toString(Long.MAX_VALUE)),
            probe("true", tags(a), Long.toString(Long.MAX_VALUE)),
            probe("true", tags(a), Long.toString(Long.MAX_VALUE - 1)),
            probe("true", tags(a), Long.toString(Long.MIN_VALUE))),
        all(SERVED)));
    rows.add(row("N2", "a contributing double", List.of(probe("true", tags(a), "2.5e0")),
        sumAndCounts(REFUSED, SERVED_OR_DECLINED)));
    rows.add(new Row("N3", "an empty selection", List.of(probe("false", tags(a), "777")),
        "$r.active and $r.user.tags[] = \"nobody\"", all(SERVED)));
    return rows;
  }

  private static List<String> longArray(final int n, final boolean literalFirst) {
    final List<String> elements = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      elements.add(str("e" + (i % 97)));
    }
    elements.set(literalFirst
        ? 0
        : n - 1, str(ADMIN));
    return elements;
  }

  static Stream<Arguments> rowsAndForms() {
    final List<Arguments> arguments = new ArrayList<>();
    for (final Row row : corpus()) {
      for (final Form form : Form.values()) {
        arguments.add(Arguments.of(row, form));
      }
    }
    return arguments.stream();
  }

  private static String resourceOf(final Row row) throws Exception {
    final String resource = row.resource();
    if (STORED.add(resource)) {
      final List<String> records = QryCorpus.lemireRecords(BASE_RECORDS, SEED);
      records.addAll(PROBE_AT, row.probes());
      fixture.store(resource, QryCorpus.array(records));
    }
    return resource;
  }

  // -----------------------------------------------------------------------------------------------
  // QRY-201..204, QRY-304, QRY-503
  // -----------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} {1}")
  @MethodSource("rowsAndForms")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-201", "QRY-202", "QRY-203", "QRY-204", "QRY-304", "QRY-503"})
  @DisplayName("QRY-201: the route's outcome equals the interpreter's on every corpus row, by an allowed outcome")
  void routeEqualsInterpreter(final Row row, final Form form) throws Exception {
    final String resource = resourceOf(row);
    final String query = form.query(fixture.src(resource), row.where(), "score");
    final Outcome expected = fixture.interpreter(query);
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, query, THREADS));
    assertEquals(expected, actual[0], row + " " + form + ": route vs interpreter (" + counters + ")");
    final RouteOutcome outcome = counters.outcome();
    assertTrue(row.allowed().get(form).contains(outcome),
        row + " " + form + ": outcome " + outcome + " not in " + row.allowed().get(form) + " (" + counters + ")");
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-202")
  @DisplayName("QRY-202: a matching record is folded once, however often its array repeats the literal")
  void repeatedLiteralFoldsOnce() throws Exception {
    final List<Outcome> sums = new ArrayList<>();
    for (final int repeats : new int[] {1, 2, 5}) {
      final List<String> tags = new ArrayList<>();
      for (int i = 0; i < repeats; i++) {
        tags.add(str(ADMIN));
      }
      final Row row = row("E2-repeat-" + repeats, "the literal " + repeats + " times",
          List.of(probe("true", tags, "1000003")), all(SERVED));
      final String resource = resourceOf(row);
      for (final Form form : Form.values()) {
        final String query = form.query(fixture.src(resource), W, "score");
        final Outcome route = fixture.route(resource, query, THREADS);
        assertEquals(fixture.interpreter(query), route, row + " " + form);
        if (form == Form.SUM) {
          sums.add(route);
        }
      }
    }
    assertEquals(1, new HashSet<>(sums).size(), "the sum changed with the number of matching elements: " + sums);
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-204")
  @DisplayName("QRY-204: an empty selection gives xs:integer 0 for sum and both counts")
  void emptySelectionIsIntegerZero() throws Exception {
    final Row row = corpus().stream().filter(r -> r.id().equals("N3")).findFirst().orElseThrow();
    final String resource = resourceOf(row);
    for (final Form form : Form.values()) {
      final String query = form.query(fixture.src(resource), row.where(), "score");
      assertEquals(Outcome.integer(0), fixture.interpreter(query), "the oracle: " + form);
      assertEquals(Outcome.integer(0), fixture.route(resource, query, THREADS), form.toString());
    }
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-304", "QRY-204"})
  @DisplayName("QRY-304: one page holding a decimal score declines exactly that page; a contributing double refuses")
  void mixedSumDeclinesOnePageAndDoubleRefuses() throws Exception {
    final Row decimal =
        row("QRY304-decimal", "one decimal score", List.of(probe("true", tags(str(ADMIN)), "2.25")), all(SERVED));
    final String decimalResource = resourceOf(decimal);
    final String decimalSum = Form.SUM.query(fixture.src(decimalResource), W, "score");
    final Outcome[] actual = new Outcome[1];
    final Counters declined = QrySeam.around(() -> actual[0] = fixture.route(decimalResource, decimalSum, THREADS));
    assertEquals(fixture.interpreter(decimalSum), actual[0]);
    assertEquals(1, declined.declined(), "exactly the decimal's page declines: " + declined);
    assertEquals(1, declined.declined(Decline.VALUE_TYPE), "for the value type: " + declined);
    assertEquals(0, declined.refused(), declined.toString());
    assertTrue(declined.served() > 0, "the other pages are served: " + declined);

    final Row dbl =
        row("QRY304-double", "one double score", List.of(probe("true", tags(str(ADMIN)), "2.25e0")), all(SERVED));
    final String doubleResource = resourceOf(dbl);
    final String doubleSum = Form.SUM.query(fixture.src(doubleResource), W, "score");
    final Counters refused = QrySeam.around(() -> actual[0] = fixture.route(doubleResource, doubleSum, THREADS));
    assertEquals(fixture.interpreter(doubleSum), actual[0]);
    assertEquals(1, refused.refused(Refusal.DOUBLE_SUM), "a contributing double refuses the sum: " + refused);
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-304")
  @DisplayName("QRY-304: a refused query publishes nothing; repeating it gives the interpreter's answer again")
  void refusalPublishesNoPartialResult() throws Exception {
    // The double sits in the last record, so a partial result folded before the refusal differs
    // from the full answer: a cached partial result would show on the second run.
    final List<String> records = QryCorpus.lemireRecords(BASE_RECORDS, SEED);
    records.add(probe("true", tags(str(ADMIN)), "0.5e0"));
    final String resource = "refusal-last.jn";
    if (STORED.add(resource)) {
      fixture.store(resource, QryCorpus.array(records));
    }
    final String sum = Form.SUM.query(fixture.src(resource), W, "score");
    final Outcome expected = fixture.interpreter(sum);
    try (var session = fixture.open(resource)) {
      for (int run = 0; run < 3; run++) {
        final Outcome[] actual = new Outcome[1];
        final Counters counters = QrySeam.around(() -> actual[0] = session.route(-1, sum, THREADS));
        assertEquals(expected, actual[0], "run " + run + " (" + counters + ")");
        assertEquals(1, counters.refused(Refusal.DOUBLE_SUM), "run " + run + " was not refused again: " + counters);
      }
    }
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-304")
  @DisplayName("QRY-304: a claimed query over an open write transaction is refused")
  void openWriteTransactionRefuses() throws Exception {
    final Row row = corpus().getFirst();
    final String resource = resourceOf(row);
    for (final Form form : Form.values()) {
      final String query = form.query(fixture.src(resource), W, "score");
      final Outcome expected = fixture.interpreter(query);
      final Outcome[] actual = new Outcome[1];
      final Counters counters =
          QrySeam.around(() -> actual[0] = fixture.routeOverWriteTransaction(resource, query, THREADS));
      assertEquals(expected, actual[0], form + " (" + counters + ")");
      assertEquals(0, counters.served(), form + ": served over an open write transaction: " + counters);
      assertTrue(counters.refused(Refusal.WRITE_TRANSACTION) >= 1, form + ": not refused: " + counters);
    }
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("variants")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-201", "QRY-101"})
  @DisplayName("QRY-201: every claimed spelling of the shape is served and equals the interpreter")
  void claimedSpellingsEqualInterpreter(final String what, final String query) throws Exception {
    final String resource = resourceOf(corpus().getFirst());
    final String expanded = query.replace(QryFixture.SRC, fixture.src(resource));
    final Outcome expected = fixture.interpreter(expanded);
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, expanded, THREADS));
    assertEquals(expected, actual[0], what + " (" + counters + ")");
    assertEquals(RouteOutcome.SERVED, counters.outcome(), what + ": " + counters);
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-504", "QRY-503"})
  @DisplayName("QRY-504: the full object query serves its admin_sum member from regions")
  void composedObjectServesAdminSum() throws Exception {
    final String resource = "composed-lemire.jn";
    fixture.store(resource, QryCorpus.array(QryCorpus.lemireRecords(BASE_RECORDS, SEED)));
    final String source = fixture.src(resource);
    final String count = "count(" + source + ")";
    final String active = "count(for $r in " + source + " where $r.active return $r)";
    final String sum = QryCorpus.adminSum(source);
    final String otherMembers = "{\"count\":" + count + ",\"active\":" + active + "}";
    final String full = "{\"count\":" + count + ",\"active\":" + active + ",\"admin_sum\":" + sum + "}";
    final Outcome expected = Outcome.object(Map.of("count", fixture.interpreter(count), "active",
        fixture.interpreter(active), "admin_sum", fixture.interpreter(sum)));
    assertEquals(expected, fixture.interpreter(full));
    final Outcome[] actual = new Outcome[1];
    final Counters scalar = QrySeam.around(() -> actual[0] = fixture.route(resource, sum, THREADS));
    assertEquals(fixture.interpreter(sum), actual[0]);
    assertEquals(RouteOutcome.SERVED, scalar.outcome(), scalar.toString());
    final Outcome expectedOthers = fixture.interpreter(otherMembers);
    final Counters others = QrySeam.around(() -> actual[0] = fixture.route(resource, otherMembers, THREADS));
    assertEquals(expectedOthers, actual[0]);
    final Counters composed = QrySeam.around(() -> actual[0] = fixture.route(resource, full, THREADS));
    assertEquals(expected, actual[0], composed.toString());
    assertEquals(others.served() + scalar.served(), composed.served(),
        "admin_sum must account for the added region servings: " + composed);
    assertEquals(0, composed.declined(), composed.toString());
    assertEquals(0, composed.refused(), composed.toString());
  }

  static Stream<Arguments> variants() {
    final String s = QryFixture.SRC;
    return Stream.of(Arguments.of("flag only, sum", "sum(for $r in " + s + " where $r.active return $r.score)"),
        Arguments.of("membership only, sum",
            "sum(for $r in " + s + " where $r.user.tags[] = \"admin\" return $r.score)"),
        Arguments.of("membership only, count", QryCorpus.nestedMembershipCount(s)),
        Arguments.of("reversed operands",
            "sum(for $r in " + s + " where \"admin\" = $r.user.tags[] and $r.active " + "return $r.score)"),
        Arguments.of("some … satisfies eq",
            "sum(for $r in " + s + " where $r.active and (some $g in $r.user.tags[] "
                + "satisfies $g eq \"admin\") return $r.score)"),
        Arguments.of("some … satisfies =",
            "count(for $r in " + s + " where (some $g in $r.user.tags[] "
                + "satisfies \"admin\" = $g) and $r.active return $r)"),
        Arguments.of("two where clauses",
            "count(for $r in " + s + " where $r.active where $r.user.tags[] = \"guest\" " + "return $r.score)"),
        Arguments.of("two memberships",
            "sum(for $r in " + s + " where $r.user.tags[] = \"admin\" and "
                + "$r.user.tags[] = \"dev\" return $r.score)"),
        Arguments.of("member path proven absent (depth 1)",
            "sum(for $r in " + s + " where $r.tags[] = \"admin\" " + "return $r.score)"),
        Arguments.of("member path proven absent (depth 3)",
            "count(for $r in " + s + " where $r.active and " + "$r.user.profile.tags[] = \"admin\" return $r)"),
        Arguments.of("other N", "sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"ops\" return $r.id)"));
  }

  // -----------------------------------------------------------------------------------------------
  // QRY-601: the kill switch
  // -----------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} {1}")
  @MethodSource("rowsAndForms")
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-601")
  @DisplayName("QRY-601: with the route switched off its counters stay at zero and every result is unchanged")
  void switchedOffRouteKeepsResults(final Row row, final Form form) throws Exception {
    final String resource = resourceOf(row);
    final String query = form.query(fixture.src(resource), row.where(), "score");
    final Outcome expected = fixture.interpreter(query);
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, -1, query, THREADS, false));
    assertEquals(expected, actual[0], row + " " + form + " with the route off");
    assertEquals(0, counters.served(), counters.toString());
    assertEquals(0, counters.declined(), counters.toString());
    assertEquals(0, counters.seamRecords(), counters.toString());
    assertEquals(0, counters.refused(), counters.toString());
  }

  // -----------------------------------------------------------------------------------------------
  // QRY-203 gap note: the interpreter's observed results (runs today)
  // -----------------------------------------------------------------------------------------------

  static Stream<Arguments> observations() {
    // {document, sum, count(return $r), count(return $r.score)}, observed on brackit
    // 1.0-alpha10-SNAPSHOT
    // (2026-11-28) with W = $r.active and $r.user.tags[] = "admin". The route must reproduce these.
    final String fotyRecord = "error err:FOTY0012";
    return Stream.of(
        // T2: flag values (EBV: null false, strings by length, numbers by value, objects/arrays raise)
        obs("[{\"active\":null,\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", "xs:integer 0", "xs:integer 0",
            "xs:integer 0"),
        obs("[{\"active\":\"x\",\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", "xs:integer 1", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":\"\",\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", "xs:integer 0", "xs:integer 0",
            "xs:integer 0"),
        obs("[{\"active\":0,\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", "xs:integer 0", "xs:integer 0",
            "xs:integer 0"),
        obs("[{\"active\":2,\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", "xs:integer 1", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":{\"a\":1},\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", fotyRecord, fotyRecord, fotyRecord),
        obs("[{\"active\":{\"a\":1},\"user\":{\"tags\":[\"guest\"]},\"score\":1}]", fotyRecord, fotyRecord, fotyRecord),
        obs("[{\"active\":[true],\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", fotyRecord, fotyRecord, fotyRecord),
        obs("[{\"active\":[],\"user\":{\"tags\":[\"admin\"]},\"score\":1}]", fotyRecord, fotyRecord, fotyRecord),
        // T3: elements before the match are compared and raise; after it the existential stops
        obs("[{\"active\":true,\"user\":{\"tags\":[1,\"admin\"]},\"score\":1}]", "error err:XPTY0004",
            "error err:XPTY0004", "error err:XPTY0004"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\",1]},\"score\":1}]", "xs:integer 1", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[null,\"admin\"]},\"score\":1}]", "xs:integer 1", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[true,\"admin\"]},\"score\":1}]", "error err:XPTY0004",
            "error err:XPTY0004", "error err:XPTY0004"),
        obs("[{\"active\":true,\"user\":{\"tags\":[[\"admin\"],\"x\"]},\"score\":1}]", fotyRecord, fotyRecord,
            fotyRecord),
        obs("[{\"active\":true,\"user\":{\"tags\":[{\"a\":\"admin\"},\"admin\"]},\"score\":1}]", fotyRecord, fotyRecord,
            fotyRecord),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\",{\"a\":\"admin\"}]},\"score\":1}]", "xs:integer 1",
            "xs:integer 1", "xs:integer 1"),
        obs("[{\"active\":false,\"user\":{\"tags\":[1,\"admin\"]},\"score\":1}]", "xs:integer 0", "xs:integer 0",
            "xs:integer 0"),
        // T5: sum raises, both counts add one
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":null}]", "error err:FORG0006", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":\"7\"}]", "error err:FORG0006", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":true}]", "error err:FORG0006", "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":{\"a\":1}}]", fotyRecord, "xs:integer 1",
            "xs:integer 1"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":[1]}]", fotyRecord, "xs:integer 1",
            "xs:integer 1"),
        // E5, E6: no element reached
        obs("[{\"active\":true,\"user\":{\"tags\":\"admin\"},\"score\":1}]", "xs:integer 0", "xs:integer 0",
            "xs:integer 0"),
        obs("[{\"active\":true,\"user\":[{\"tags\":[\"admin\"]},{\"tags\":[\"x\"]}],\"score\":1}]", "xs:integer 0",
            "xs:integer 0", "xs:integer 0"),
        // T4, T6, N1, N3
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]}}]", "xs:integer 0", "xs:integer 1", "xs:integer 0"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":1.5},{\"active\":true,\"user\":{\"tags\":[\"admin\"]},"
            + "\"score\":2}]", "xs:decimal 3.5", "xs:integer 2", "xs:integer 2"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":1.5e0},{\"active\":true,\"user\":{\"tags\":"
            + "[\"admin\"]},\"score\":2}]", "xs:double 3.5", "xs:integer 2", "xs:integer 2"),
        obs("[{\"active\":true,\"user\":{\"tags\":[\"admin\"]},\"score\":9223372036854775807},{\"active\":true,\"user\":"
            + "{\"tags\":[\"admin\"]},\"score\":9223372036854775807},{\"active\":true,\"user\":{\"tags\":[\"admin\"]},"
            + "\"score\":-9223372036854775808}]", "xs:integer 9223372036854775806", "xs:integer 3", "xs:integer 3"),
        obs("[]", "xs:integer 0", "xs:integer 0", "xs:integer 0"));
  }

  private static Arguments obs(final String document, final String sum, final String countRecords,
      final String countField) {
    return Arguments.of(document, sum, countRecords, countField);
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("observations")
  @DisplayName("QRY-203: the interpreter's results for T2, T3 and the sum of T5 are the recorded observations")
  void interpreterObservations(final String document, final String sum, final String countRecords,
      final String countField) throws Exception {
    final String resource = "obs-" + Integer.toHexString(document.hashCode()) + ".jn";
    if (STORED.add(resource)) {
      fixture.store(resource, document);
    }
    final String src = fixture.src(resource);
    assertEquals(sum, render(fixture.interpreter(Form.SUM.query(src, W, "score"))), "sum over " + document);
    assertEquals(countRecords, render(fixture.interpreter(Form.COUNT_RECORDS.query(src, W, "score"))),
        "count($r) over " + document);
    assertEquals(countField, render(fixture.interpreter(Form.COUNT_FIELD.query(src, W, "score"))),
        "count($r.score) over " + document);
  }

  private static String render(final Outcome outcome) {
    return outcome.toString();
  }
}
