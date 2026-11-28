/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import io.sirix.query.scan.QrySeam.ScopeProof;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-301: columns are found by name key and tagged by path node key, so a value in a column does
 * not say which field path holds it. Every fixture here puts {@code tags} under exactly one of
 * {@code /[]/tags}, {@code /[]/user/tags}, {@code /[]/user/profile/tags}, {@code /[]/other/tags}
 * and {@code /[]/user/[]/tags}, and is queried by every member path; one more fixture holds the
 * literal under the four named object paths at different frequencies, with same-name flag and
 * {@code N} fields nested in {@code user}, so a value counted from the wrong path changes the
 * answer.
 *
 * <p>
 * Each answer equals the interpreter's, and the counters show where the route served, where it
 * refused or declined, and that a path the summary proves absent contributed nothing without a
 * name-only scan (no full record-page read, no refusal). The mutation seams bypass each proof; with
 * a seam bypassed the answer must be wrong, which shows the proof is load-bearing.
 *
 * <p>
 * {@link #fixturesHoldTheirLocations} is not planned: it checks, through the interpreter alone,
 * that each fixture holds the literal where it says, so the planned table cannot pass on empty
 * fixtures.
 */
final class RegionPredicatedAggregateScopeTest {

  private static final String DB = "qry-scope-db";
  private static final int RECORDS = 1_200;
  private static final int THREADS = 2;

  /** Where a fixture puts {@code tags}. */
  enum Location {
    TOP("tags", "/[]/tags"), USER("user.tags", "/[]/user/tags"), PROFILE("user.profile.tags",
        "/[]/user/profile/tags"), OTHER("other.tags", "/[]/other/tags"), USER_ARRAY(null, "/[]/user/[]/tags");

    /** The member path that queries this location, {@code null} when no claimed path does. */
    final String memberPath;
    final String summaryPath;

    Location(final String memberPath, final String summaryPath) {
      this.memberPath = memberPath;
      this.summaryPath = summaryPath;
    }
  }

  private static final List<String> MEMBER_PATHS = List.of("tags", "user.tags", "user.profile.tags", "other.tags");

  private static Path dir;
  private static QryFixture fixture;
  private static QryFixture withoutSummary;

  @BeforeAll
  static void storeFixtures() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-scope-");
    fixture = new QryFixture(dir, DB);
    for (final Location location : Location.values()) {
      fixture.store(resource(location), single(location));
    }
    fixture.store("every.jn", everyLocation());
    withoutSummary = new QryFixture(dir, DB + "-nosummary", VersioningType.FULL, null, false);
    withoutSummary.store("every.jn", everyLocation());
  }

  @AfterAll
  static void dropFixtures() {
    if (fixture != null) {
      fixture.drop();
    }
    if (withoutSummary != null) {
      withoutSummary.drop();
    }
  }

  @AfterEach
  void restoreSeams() {
    QrySeam.resetScopeProofsQuietly();
  }

  private static String resource(final Location location) {
    return location.name().toLowerCase() + ".jn";
  }

  /** Records with {@code tags} only at {@code location}; {@code "admin"} on every fourth record. */
  static String single(final Location location) {
    final List<String> records = new ArrayList<>(RECORDS);
    for (int i = 0; i < RECORDS; i++) {
      final String tags = "[\"" + (i % 4 == 0
          ? "admin"
          : "guest") + "\",\"dev\"]";
      final StringBuilder sb = new StringBuilder(128);
      sb.append("{\"id\":").append(i).append(",\"active\":").append(i % 3 != 0);
      if (location == Location.TOP) {
        sb.append(",\"tags\":").append(tags);
      }
      if (location == Location.USER_ARRAY) {
        sb.append(",\"user\":[{\"name\":\"u").append(i).append("\",\"tags\":").append(tags).append("}]");
      } else {
        sb.append(",\"user\":{\"name\":\"u").append(i).append('"');
        if (location == Location.USER) {
          sb.append(",\"tags\":").append(tags);
        }
        if (location == Location.PROFILE) {
          sb.append(",\"profile\":{\"tags\":").append(tags).append('}');
        }
        sb.append('}');
      }
      if (location == Location.OTHER) {
        sb.append(",\"other\":{\"tags\":").append(tags).append('}');
      }
      sb.append(",\"score\":").append(i % 100).append('}');
      records.add(sb.toString());
    }
    return QryCorpus.array(records);
  }

  /**
   * The literal under every location except the array of objects, each at its own frequency, and
   * same-name {@code active} and {@code score} fields nested in {@code user} with other values.
   */
  static String everyLocation() {
    final List<String> records = new ArrayList<>(RECORDS);
    for (int i = 0; i < RECORDS; i++) {
      final boolean active = i % 3 != 0;
      final StringBuilder sb = new StringBuilder(256);
      sb.append("{\"id\":").append(i).append(",\"active\":").append(active);
      sb.append(",\"tags\":[\"")
        .append(i % 9 == 0
            ? "admin"
            : "x")
        .append("\"]");
      sb.append(",\"user\":{\"name\":\"u").append(i).append('"');
      if (i % 5 == 0) {
        sb.append(",\"active\":").append(!active);
      }
      sb.append(",\"tags\":[\"")
        .append(i % 4 == 0
            ? "admin"
            : "guest")
        .append("\"]");
      sb.append(",\"profile\":{\"tags\":[\"")
        .append(i % 7 == 0
            ? "admin"
            : "dev")
        .append("\"]}");
      if (i % 6 == 2) {
        sb.append(",\"score\":").append(10_000 + i);
      }
      sb.append('}');
      sb.append(",\"other\":{\"tags\":[\"")
        .append(i % 2 == 0
            ? "admin"
            : "ops")
        .append("\"]}");
      sb.append(",\"score\":").append(i % 100).append('}');
      records.add(sb.toString());
    }
    return QryCorpus.array(records);
  }

  private static String where(final String memberPath) {
    return "$r.active and $r." + memberPath + "[] = \"admin\"";
  }

  static Stream<Arguments> fixturesByPaths() {
    final List<Arguments> rows = new ArrayList<>();
    for (final Location location : Location.values()) {
      for (final String memberPath : MEMBER_PATHS) {
        for (final Form form : new Form[] {Form.SUM, Form.COUNT_RECORDS}) {
          rows.add(Arguments.of(location, memberPath, form));
        }
      }
    }
    return rows.stream();
  }

  @ParameterizedTest(name = "[{index}] tags under {0}, queried by {1}, {2}")
  @MethodSource("fixturesByPaths")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-301", "QRY-503"})
  @DisplayName("QRY-301: each answer equals the interpreter's; the counters show served, refused and proven absent")
  void eachLocationByEachPath(final Location location, final String memberPath, final Form form) throws Exception {
    final String resource = resource(location);
    final String query = form.query(fixture.src(resource), where(memberPath), "score");
    final Outcome expected = fixture.interpreter(query);
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, query, THREADS));
    assertEquals(expected, actual[0], location.summaryPath + " by " + memberPath + " (" + counters + ")");
    if (memberPath.equals(location.memberPath)) {
      assertEquals(RouteOutcome.SERVED, counters.outcome(), "a uniquely resolved path is served: " + counters);
    } else if (location == Location.USER_ARRAY && memberPath.equals("user.tags")) {
      assertTrue(EnumSet.of(RouteOutcome.REFUSED, RouteOutcome.PAGE_DECLINED).contains(counters.outcome()),
          "a path another structure reaches is refused or decided by the record path: " + counters);
    } else {
      assertEquals(0, counters.refused(), "a path proven absent is not an error: " + counters);
      assertEquals(0, counters.recordPageReads(),
          "proving the path absent turned into a scan of the resource's records: " + counters);
    }
  }

  @ParameterizedTest(name = "[{index}] queried by {0}")
  @MethodSource("memberPaths")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-301", "QRY-503"})
  @DisplayName("QRY-301: with the literal under every location, each path counts only its own values")
  void everyLocationIsScopedPerPath(final String memberPath) throws Exception {
    for (final Form form : Form.values()) {
      final String query = form.query(fixture.src("every.jn"), where(memberPath), "score");
      final Outcome expected = fixture.interpreter(query);
      final Outcome[] actual = new Outcome[1];
      final Counters counters = QrySeam.around(() -> actual[0] = fixture.route("every.jn", query, THREADS));
      assertEquals(expected, actual[0], memberPath + " " + form + " (" + counters + ")");
      assertEquals(RouteOutcome.SERVED, counters.outcome(), memberPath + " " + form + ": " + counters);
    }
  }

  static Stream<String> memberPaths() {
    return MEMBER_PATHS.stream();
  }

  @ParameterizedTest(name = "[{index}] queried by {0}")
  @MethodSource("memberPaths")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-301", "QRY-503"})
  @DisplayName("QRY-301: with no path summary the query is refused or decided by the record path")
  void noSummaryIsRefusedOrDecidedByRecords(final String memberPath) throws Exception {
    for (final Form form : Form.values()) {
      final String query = form.query(withoutSummary.src("every.jn"), where(memberPath), "score");
      final Outcome expected = withoutSummary.interpreter(query);
      final Outcome[] actual = new Outcome[1];
      final Counters counters = QrySeam.around(() -> actual[0] = withoutSummary.route("every.jn", query, THREADS));
      assertEquals(expected, actual[0], memberPath + " " + form + " (" + counters + ")");
      assertEquals(0, counters.served(), "a page served without a scope proof: " + counters);
    }
  }

  static Stream<Arguments> proofs() {
    return Stream.of(
        // The member path: user.tags shares its name key with three other tags arrays.
        Arguments.of(ScopeProof.MEMBER, Form.COUNT_RECORDS, "$r.active and $r.user.tags[] = \"admin\"", "score"),
        // The flag path: user.active shares its name key with active and holds the opposite value.
        Arguments.of(ScopeProof.FLAG, Form.COUNT_RECORDS, "$r.active and $r.other.tags[] = \"admin\"", "score"),
        // The N path: user.score shares its name key with score and holds other values.
        Arguments.of(ScopeProof.VALUE, Form.SUM, "$r.active and $r.other.tags[] = \"admin\"", "score"));
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("proofs")
  @QryPlanned(step = QrySteps.ROUTE, value = {"QRY-301", "QRY-503"})
  @DisplayName("QRY-301: each scope proof is load-bearing: bypassed, the answer is wrong")
  void bypassedProofGivesAWrongAnswer(final ScopeProof proof, final Form form, final String where, final String n)
      throws Exception {
    final String query = form.query(fixture.src("every.jn"), where, n);
    final Outcome expected = fixture.interpreter(query);
    assertEquals(expected, fixture.route("every.jn", query, THREADS), "with every proof in place");
    QrySeam.setScopeProofBypassed(proof, true);
    final Outcome[] bypassed = new Outcome[1];
    final Counters counters = QrySeam.around(() -> bypassed[0] = fixture.route("every.jn", query, THREADS));
    assertTrue(counters.served() > 0, "the bypassed run served no page, so it cannot show anything: " + counters);
    assertNotEquals(expected, bypassed[0], "the " + proof + " proof is not load-bearing: bypassing it kept the answer");
  }

  @Test
  @QryPlanned(step = QrySteps.ROUTE, value = "QRY-301")
  @DisplayName("QRY-301: competing nested scores contribute when the value scope proof is bypassed")
  void nestedScoresOccurOnMatchingRecords() throws Exception {
    final String where = "$r.active and $r.other.tags[] = \"admin\"";
    final Outcome nested = fixture.interpreter(Form.SUM.query(fixture.src("every.jn"), where, "user.score"));
    assertEquals("xs:integer", nested.type());
    assertTrue(Long.parseLong(nested.value()) > 0, nested.toString());
  }

  @Test
  @DisplayName("QRY-301: the fixture puts each location's tags at the summary path it names")
  void fixturesHoldTheirLocations() throws Exception {
    // The interpreter's own reading of the fixtures: the literal is reachable at each claimed path
    // of its location and nowhere else, so the table above tests scope rather than an empty file.
    for (final Location location : Location.values()) {
      for (final String memberPath : MEMBER_PATHS) {
        final String query = Form.COUNT_RECORDS.query(fixture.src(resource(location)), where(memberPath), "score");
        final Outcome outcome = fixture.interpreter(query);
        if (memberPath.equals(location.memberPath)) {
          assertNotEquals(Outcome.integer(0), outcome, location + " by " + memberPath);
        } else {
          assertEquals(Outcome.integer(0), outcome, location + " by " + memberPath);
        }
      }
    }
  }
}
