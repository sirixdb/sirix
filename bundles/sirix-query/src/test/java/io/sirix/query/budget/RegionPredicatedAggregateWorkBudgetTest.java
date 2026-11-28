/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.budget;

import io.sirix.access.Databases;
import io.sirix.io.StorageType;
import io.sirix.query.scan.QryCorpus;
import io.sirix.query.scan.QryFixture;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QryPlanned;
import io.sirix.query.scan.QrySeam;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.Decline;
import io.sirix.query.scan.QrySteps;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.SLOTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-501's work budget, in counted work and never in time (CONV-301): on 20,000 Lemire-shaped
 * records (FILE_CHANNEL, FULL, path summary on, a fixed seed; about 180 pages at ~9.2 nodes per
 * record, seams on most of them), cold, at one executor thread and then four, the admin-sum query
 * and the nested membership count must
 *
 * <ol>
 * <li>serve every scheduled page: zero page declines and zero query refusals;</li>
 * <li>read at most as many full record pages as seam records were handed to the record path, and
 * hand over at most two seam records per served page;</li>
 * <li>read no region beyond the name-key column on a page where the anchor field does not
 * occur.</li>
 * </ol>
 *
 * <p>
 * A positive control shows the counters see the work: with the route switched off (QRY-601) the
 * same queries read at least one full record page per scheduled page. A negative control shows the
 * bound detects a declined page: one page holding a decimal {@code score} in a matching record
 * declines exactly that page and reads at least one more full record page.
 *
 * <p>
 * Every page of the fixture holds records with the anchor fields, so every page is scheduled; the
 * page count is the document's node-key range over the 1,024 slots of a page. Item 3 compares the
 * fixture with itself plus a block of records lacking both {@code active} and {@code user} appended
 * at the end: the added pages must add no column read beyond what one boundary page shared with the
 * Lemire records reads, and no full record-page read.
 */
@Isolated
@QryPlanned(step = QrySteps.ROUTE, value = {"QRY-501", "QRY-503"})
final class RegionPredicatedAggregateWorkBudgetTest {

  private static final String DB = "qry-work-budget-db";
  private static final int RECORDS = 20_000;
  private static final long SEED = 0x1E31AEL;
  private static final int ANCHORLESS = 6_000;

  private static Path dir;
  private static QryFixture fixture;

  @BeforeAll
  static void storeFixtures() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-work-budget-");
    fixture = new QryFixture(dir, DB, VersioningType.FULL, StorageType.FILE_CHANNEL, true);
    final List<String> lemire = QryCorpus.lemireRecords(RECORDS, SEED);
    fixture.store("lemire.jn", QryCorpus.array(lemire));
    final List<String> withAnchorless = new ArrayList<>(lemire);
    for (int i = 0; i < ANCHORLESS; i++) {
      withAnchorless.add("{\"id\":" + (RECORDS + i) + ",\"score\":" + i + ",\"note\":\"n\"}");
    }
    fixture.store("anchorless-tail.jn", QryCorpus.array(withAnchorless));
    final List<String> withDecimal = new ArrayList<>(lemire);
    withDecimal.add(RECORDS / 2, QryCorpus.lemire(-1, "true", List.of(QryCorpus.str(ADMIN)), "12.5"));
    fixture.store("decimal-page.jn", QryCorpus.array(withDecimal));
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  private static String[] queries(final String resource) {
    return new String[] {QryCorpus.adminSum(fixture.src(resource)),
        QryCorpus.nestedMembershipCount(fixture.src(resource))};
  }

  private static long scheduledPages(final String resource) {
    return fixture.maxNodeKey(resource) / SLOTS + 1;
  }

  private static Counters cold(final String resource, final String query, final int threads, final boolean route,
      final Outcome expected) throws Exception {
    Databases.clearGlobalCaches();
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, -1, query, threads, route));
    assertEquals(expected, actual[0], query + " at " + threads + " threads, route " + (route
        ? "on"
        : "off") + " (" + counters + ")");
    return counters;
  }

  @ParameterizedTest(name = "[{index}] {0} thread(s)")
  @ValueSource(ints = {1, 4})
  @DisplayName("QRY-501 items 1 and 2: every page served, record-page reads bounded by seam handovers")
  void everyPageIsServedAndRecordReadsAreSeamsOnly(final int threads) throws Exception {
    final long pages = scheduledPages("lemire.jn");
    assertTrue(pages >= 150, "fixture: about 180 pages expected, got " + pages);
    for (final String query : queries("lemire.jn")) {
      final Counters c = cold("lemire.jn", query, threads, true, fixture.interpreter(query));
      assertEquals(0, c.declined(), "item 1: a page was declined: " + c);
      assertEquals(0, c.refused(), "item 1: the query was refused: " + c);
      assertEquals(pages, c.served(), "item 1: not every scheduled page was served: " + c);
      assertTrue(c.recordPageReads() <= c.seamRecords(),
          "item 2: more full record-page reads than seam records handed over: " + c);
      assertTrue(c.seamRecords() <= 2 * c.served(), "item 2: more than two seam records per served page: " + c);
    }
  }

  @ParameterizedTest(name = "[{index}] {0} thread(s)")
  @ValueSource(ints = {1, 4})
  @DisplayName("QRY-501 item 3: a page without the anchor field reads no region beyond the name-key column")
  void anchorlessPagesReadOnlyTheNameKeyColumn(final int threads) throws Exception {
    final long addedPages = scheduledPages("anchorless-tail.jn") - scheduledPages("lemire.jn");
    assertTrue(addedPages >= 10, "fixture: the anchorless tail must fill whole pages, got " + addedPages);
    final String[] base = queries("lemire.jn");
    final String[] tail = queries("anchorless-tail.jn");
    for (int q = 0; q < base.length; q++) {
      final Counters without = cold("lemire.jn", base[q], threads, true, fixture.interpreter(base[q]));
      final Counters with = cold("anchorless-tail.jn", tail[q], threads, true, fixture.interpreter(tail[q]));
      final long perPage = (without.columnReads() + without.served() - 1) / Math.max(1, without.served());
      assertTrue(with.columnReads() - without.columnReads() <= perPage,
          "anchorless pages read columns beyond the name key: " + without + " vs " + with);
      assertEquals(without.recordPageReads(), with.recordPageReads(),
          "anchorless pages read full record pages: " + without + " vs " + with);
      assertEquals(0, with.declined(), with.toString());
      assertEquals(0, with.refused(), with.toString());
    }
  }

  @Test
  @DisplayName("QRY-501 positive control: with the route off, every scheduled page is read in full")
  void routeOffReadsEveryPage() throws Exception {
    final long pages = scheduledPages("lemire.jn");
    for (final String query : queries("lemire.jn")) {
      final Outcome expected = fixture.interpreter(query);
      for (final int threads : new int[] {1, 4}) {
        final Counters off = cold("lemire.jn", query, threads, false, expected);
        assertTrue(off.recordPageReads() >= pages,
            "the record-page counter does not see the switched-off route's reads: " + off + ", pages " + pages);
        assertEquals(0, off.served(), off.toString());
      }
    }
  }

  @Test
  @DisplayName("QRY-501 negative control: one page with a decimal score declines once and reads more record pages")
  void declinedPageIsDetected() throws Exception {
    final String clean = QryCorpus.adminSum(fixture.src("lemire.jn"));
    final String decimal = QryCorpus.adminSum(fixture.src("decimal-page.jn"));
    for (final int threads : new int[] {1, 4}) {
      final Counters baseline = cold("lemire.jn", clean, threads, true, fixture.interpreter(clean));
      final Counters declined = cold("decimal-page.jn", decimal, threads, true, fixture.interpreter(decimal));
      assertEquals(1, declined.declined(), "exactly the decimal's page declines: " + declined);
      assertEquals(1, declined.declined(Decline.VALUE_TYPE), declined.toString());
      assertTrue(declined.recordPageReads() >= baseline.recordPageReads() + 1,
          "the declined page read no full record page: " + baseline + " vs " + declined);
    }
  }
}
