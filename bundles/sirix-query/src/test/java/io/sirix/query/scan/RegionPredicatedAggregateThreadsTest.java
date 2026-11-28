/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.Refusal;
import io.sirix.query.scan.RegionPredicatedAggregateSeamTest.Placement;
import io.sirix.query.scan.RegionPredicatedAggregateSeamTest.Side;
import io.sirix.query.scan.RegionPredicatedAggregateSeamTest.Target;
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
import java.util.List;
import java.util.stream.Stream;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.lemire;
import static io.sirix.query.scan.QryCorpus.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-401: the result must not depend on the executor's thread count or on scheduling order, each
 * page is handled (served or declined) exactly once, and a refusal on any worker leads to query
 * refusal, never to a partial result. The QRY-303 seam fixtures and the QRY-501 fixture run at 1,
 * 2, 3, 4 and 8 executor threads, 20 times each so that work-stealing order varies; every run must
 * give a bit-identical result equal to the interpreter's and the same served-plus-declined page
 * total.
 */
@QryPlanned(step = QrySteps.ROUTE, value = {"QRY-401", "QRY-503"})
final class RegionPredicatedAggregateThreadsTest {

  private static final String DB = "qry-threads-db";
  private static final int[] THREADS = {1, 2, 3, 4, 8};
  private static final int RUNS = 20;
  private static final String W = "$r.active and $r.user.tags[] = \"admin\"";

  private static Path dir;
  private static QryFixture fixture;

  @BeforeAll
  static void storeFixtures() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-threads-");
    fixture = new QryFixture(dir, DB);
    fixture.store("lemire.jn", QryCorpus.array(QryCorpus.lemireRecords(20_000, 0x1E31AEL)));
    for (final Arguments seam : seamFixtures().toList()) {
      final Object[] p = seam.get();
      final Placement placement =
          RegionPredicatedAggregateSeamTest.place((Target) p[0], (Side) p[1], true, (Boolean) p[2]);
      fixture.store(seamResource(p), placement.json());
    }
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  static Stream<Arguments> seamFixtures() {
    return Stream.of(Arguments.of(Target.MATCHING_ELEMENT, Side.FIRST_SLOT, false),
        Arguments.of(Target.TAGS, Side.LAST_SLOT, false), Arguments.of(Target.SCORE, Side.FIRST_SLOT, false),
        Arguments.of(Target.USER, Side.LAST_SLOT, true));
  }

  private static String seamResource(final Object[] p) {
    return "seam-" + p[0] + "-" + p[1] + "-" + p[2] + ".jn";
  }

  static Stream<Arguments> queries() {
    final List<Arguments> rows = new ArrayList<>();
    rows.add(Arguments.of("lemire.jn", QryCorpus.adminSum(QryFixture.SRC)));
    rows.add(Arguments.of("lemire.jn", QryCorpus.nestedMembershipCount(QryFixture.SRC)));
    for (final Arguments seam : seamFixtures().toList()) {
      for (final Form form : Form.values()) {
        rows.add(Arguments.of(seamResource(seam.get()), form.query(QryFixture.SRC, W, "score")));
      }
    }
    return rows.stream();
  }

  @ParameterizedTest(name = "[{index}] {0}: {1}")
  @MethodSource("queries")
  @DisplayName("QRY-401: every thread count and schedule gives the interpreter's result and handles each page once")
  void resultIsIndependentOfThreads(final String resource, final String template) throws Exception {
    final String query = template.replace(QryFixture.SRC, fixture.src(resource));
    final Outcome expected = fixture.interpreter(query);
    long handled = -1;
    for (final int threads : THREADS) {
      for (int run = 0; run < RUNS; run++) {
        final Outcome[] actual = new Outcome[1];
        final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, query, threads));
        final String context = resource + " at " + threads + " threads, run " + run + " (" + counters + ")";
        assertEquals(expected, actual[0], context);
        assertEquals(0, counters.refused(), context);
        assertTrue(counters.handled() > 0, "the route handled no page: " + context);
        if (handled < 0) {
          handled = counters.handled();
        }
        assertEquals(handled, counters.handled(), "pages handled more or less than once: " + context);
      }
    }
  }

  @Test
  @DisplayName("QRY-401: a refusal on any worker refuses the query; no worker's partial result is returned")
  void refusalOnOneWorkerRefusesTheQuery() throws Exception {
    // One contributing double on a middle page: the worker that meets it must refuse the whole query
    // while the others have already folded their pages.
    final List<String> records = QryCorpus.lemireRecords(20_000, 0x1E31AEL);
    records.add(10_000, lemire(-1, "true", List.of(str(ADMIN)), "3.25e0"));
    fixture.store("double-mid.jn", QryCorpus.array(records));
    final String query = QryCorpus.adminSum(fixture.src("double-mid.jn"));
    final Outcome expected = fixture.interpreter(query);
    for (final int threads : THREADS) {
      for (int run = 0; run < RUNS; run++) {
        final Outcome[] actual = new Outcome[1];
        final Counters counters = QrySeam.around(() -> actual[0] = fixture.route("double-mid.jn", query, threads));
        final String context = "at " + threads + " threads, run " + run + " (" + counters + ")";
        assertEquals(expected, actual[0], context);
        assertEquals(1, counters.refused(Refusal.DOUBLE_SUM), context);
      }
    }
  }
}
