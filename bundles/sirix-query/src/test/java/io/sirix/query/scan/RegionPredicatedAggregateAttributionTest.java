/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.Decline;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.lemire;
import static io.sirix.query.scan.QryCorpus.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-302: from columns alone the route must attribute each flag bit, matching element and
 * {@code N} value to exactly its record, and decline a page where it cannot prove that, never serve
 * it approximately. Each fixture is the 1,500-record base corpus with one page's worth of records
 * built so that one certificate fails on its own, as the chapter lists them: a duplicated flag
 * field in one record, a missing {@code user} on one record, {@code tags} before {@code name}, a
 * periodic key order of the {@code (a,c)/(b,d)} kind from {@code RecordOrdinalRegion}'s Javadoc,
 * and an object element inside {@code tags}. Each answer equals the interpreter's, the counter of
 * the expected reason increments, nothing is refused, and the other pages are still served.
 *
 * <p>
 * The expected reason of each fixture is the chapter's; the reasons are bound to the
 * implementation's enum in {@link QrySeam}. An object element inside {@code tags} breaks both the
 * element-count certificate (QRY-302 item 4) and the modelled value types (QRY-203), so either
 * reason is accepted there. If an implementation proves the attribution of one of these shapes
 * exactly and serves it, that is a change to QRY-302's verification note, not to this test.
 *
 * <p>
 * The last test is QRY-302's second planned check: the Lemire-shaped fixture of QRY-501 is served
 * with no page declines.
 */
@QryPlanned(step = QrySteps.ROUTE, value = {"QRY-302", "QRY-503"})
final class RegionPredicatedAggregateAttributionTest {

  private static final String DB = "qry-attribution-db";
  private static final int BASE_RECORDS = 1_500;
  private static final int BREAK_AT = 700;
  private static final long SEED = 0x51A7E5L;
  private static final int THREADS = 2;
  private static final String W = "$r.active and $r.user.tags[] = \"admin\"";

  private static Path dir;
  private static QryFixture fixture;

  @BeforeAll
  static void createDatabase() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-attribution-");
    fixture = new QryFixture(dir, DB);
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  static Stream<Arguments> certificates() {
    final String a = str(ADMIN);
    final List<Arguments> rows = new ArrayList<>();
    rows.add(Arguments.of("a duplicated flag field in one record",
        List.of("{\"id\":-1,\"active\":true,\"active\":false,\"user\":{\"name\":\"p\",\"tags\":[\"admin\"]},"
            + "\"score\":777,\"note\":\"n\"}"),
        EnumSet.of(Decline.DUPLICATE_FIELD)));
    rows.add(Arguments.of("a missing user on one record",
        List.of("{\"id\":-1,\"active\":true,\"score\":777,\"note\":\"n\"}"), EnumSet.of(Decline.ATTRIBUTION)));
    final List<String> tagsFirst = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      tagsFirst.add("{\"id\":" + (-1 - i) + ",\"active\":" + (i % 3 != 0) + ",\"user\":{\"tags\":[" + (i % 2 == 0
          ? a
          : str("guest")) + "],\"name\":\"p\"},\"score\":" + i + ",\"note\":\"n\"}");
    }
    rows.add(Arguments.of("tags before name", tagsFirst, EnumSet.of(Decline.ATTRIBUTION)));
    final List<String> periodic = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      // (a,c)/(b,d): the flag and N on even records, the member path on odd ones. The name-key
      // sequence is perfectly periodic, yet no record holds both the flag and the member path.
      periodic.add(i % 2 == 0
          ? "{\"active\":true,\"score\":" + i + "}"
          : "{\"user\":{\"name\":\"p\",\"tags\":[\"admin\"]},\"note\":\"n\"}");
    }
    rows.add(Arguments.of("a periodic key order of the (a,c)/(b,d) kind", periodic, EnumSet.of(Decline.ATTRIBUTION)));
    rows.add(Arguments.of("an object element inside tags",
        List.of(lemire(-1, "true", List.of(a, "{\"a\":\"admin\"}"), "777")),
        EnumSet.of(Decline.ELEMENT_CERTIFICATE, Decline.VALUE_TYPE)));
    return rows.stream();
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("certificates")
  @DisplayName("QRY-302: a page whose attribution cannot be proven is declined for its reason, never served")
  void unprovenAttributionDeclinesThePage(final String what, final List<String> breaking, final Set<Decline> reasons)
      throws Exception {
    final List<String> records = QryCorpus.lemireRecords(BASE_RECORDS, SEED);
    records.addAll(BREAK_AT, breaking);
    final String resource = "cert-" + Integer.toHexString(what.hashCode()) + ".jn";
    fixture.store(resource, QryCorpus.array(records));
    for (final Form form : Form.values()) {
      final String query = form.query(fixture.src(resource), W, "score");
      final Outcome expected = fixture.interpreter(query);
      final Outcome[] actual = new Outcome[1];
      final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, query, THREADS));
      assertEquals(expected, actual[0], what + " " + form + " (" + counters + ")");
      long forReason = 0;
      for (final Decline reason : reasons) {
        forReason += counters.declined(reason);
      }
      assertTrue(forReason >= 1, what + " " + form + ": no page declined for " + reasons + ": " + counters);
      assertEquals(0, counters.refused(),
          what + " " + form + ": an attribution failure is a page decline: " + counters);
      assertTrue(counters.served() > 0, what + " " + form + ": the other pages are served: " + counters);
    }
  }

  @Test
  @DisplayName("QRY-302: the Lemire-shaped fixture of QRY-501 is served with no page declines")
  void lemireFixtureIsServedWithoutDeclines() throws Exception {
    fixture.store("lemire.jn", QryCorpus.array(QryCorpus.lemireRecords(20_000, 0x1E31AEL)));
    for (final String query : new String[] {QryCorpus.adminSum(fixture.src("lemire.jn")),
        QryCorpus.nestedMembershipCount(fixture.src("lemire.jn"))}) {
      final Outcome expected = fixture.interpreter(query);
      for (final int threads : new int[] {1, 4}) {
        final Outcome[] actual = new Outcome[1];
        final Counters counters = QrySeam.around(() -> actual[0] = fixture.route("lemire.jn", query, threads));
        assertEquals(expected, actual[0], query + " at " + threads + " threads (" + counters + ")");
        assertEquals(0, counters.declined(), "Lemire pages declined at " + threads + " threads: " + counters);
        assertEquals(0, counters.refused(), counters.toString());
        assertTrue(counters.served() > 0, counters.toString());
      }
    }
  }
}
