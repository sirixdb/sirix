/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.access.Databases;
import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.OpenSession;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.lemire;
import static io.sirix.query.scan.QryCorpus.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-402 and QRY-403: the route reads the page versions, region columns and path summary of the
 * revision its source resolves to and no other, merged fragment columns never resurrect or
 * double-count a value, and no cached result or plan is reused for another revision, member path,
 * literal, flag set, aggregate function or {@code N}.
 *
 * <p>
 * Revision 1 is 3,000 Lemire-shaped records with two matching probe records at known positions.
 * Revisions 2 to 9 each change the data in one of the chapter's ways: flip {@code active}, add the
 * literal to arrays, remove it, replace {@code score}, delete a matching record, insert a matching
 * record mid-array, rename {@code tags}, and turn {@code tags} into a string. Only after every
 * revision is written is each one queried, by {@code jn:doc(db, res, n)} and by its commit
 * timestamp, cold and warm, under all four versioning types (FULL, INCREMENTAL, DIFFERENTIAL and
 * SLIDING_SNAPSHOT), so pages are assembled from several fragments.
 */
@QryPlanned(step = QrySteps.ROUTE, value = {"QRY-402", "QRY-403", "QRY-503"})
final class RegionPredicatedAggregateVersioningTest {

  private static final String RES = "versioned.jn";
  private static final int RECORDS = 3_000;
  private static final int THREADS = 2;
  private static final String W = "$r.active and $r.user.tags[] = \"admin\"";

  private Path dir;
  private QryFixture fixture;

  @AfterEach
  void drop() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  private void build(final VersioningType versioning) throws Exception {
    dir = Files.createTempDirectory("sirix-qry-versioning-");
    fixture = new QryFixture(dir, "qry-versioning-" + versioning.name().toLowerCase(), versioning, null, true);
    final List<String> records = QryCorpus.lemireRecords(RECORDS, 0xF00DL);
    records.add(1_500, lemire(-1, "true", List.of(str(ADMIN)), "5000"));
    records.add(2_500, lemire(-2, "true", List.of(str("guest"), str(ADMIN)), "6000"));
    fixture.store(RES, QryCorpus.array(records));
    final String src = fixture.src(RES);
    final String doc = "jn:doc('" + fixture.db() + "','" + RES + "')";
    // 2: flip active
    fixture.update("for $r in " + src + " where $r.id mod 97 eq 0 return replace json value of $r.active with "
        + "not($r.active)");
    // 3: add the literal to arrays
    fixture.update("for $r in " + src + " where $r.id mod 89 eq 1 return append json \"admin\" into $r.user.tags");
    // 4: remove it again from arrays that hold it
    fixture.update("for $r in " + src + " where $r.id mod 7 eq 2 and $r.user.tags[] = \"admin\" "
        + "return replace json value of $r.user.tags with [for $t in $r.user.tags[] where $t ne \"admin\" return $t]");
    // 5: replace score
    fixture.update("for $r in " + src + " where $r.id mod 79 eq 3 return replace json value of $r.score with 100000");
    // 6: delete a matching record (the probe at position 1500)
    fixture.update("delete json " + doc + "[1500]");
    // 7: insert a matching record mid-array
    fixture.update(
        "insert json " + lemire(-3, "true", List.of(str(ADMIN)), "7000") + " into " + doc + " at position 700");
    // 8: rename tags of the matching probe record. One record per revision: renaming the fused
    // named-array key of two records in one update throws an NPE under INCREMENTAL, DIFFERENTIAL and
    // SLIDING_SNAPSHOT today (ObjectNamedArrayNode.setNameKey, ownerPage null; reported separately),
    // and this test is about the route, not that defect.
    fixture.update("for $r in " + src + " where $r.id eq -2 return rename json $r.user.tags as \"labels\"");
    // 9: turn tags into a string
    fixture.update(
        "for $r in " + src + " where $r.id mod 67 eq 5 return replace json value of $r.user.tags with " + "\"admin\"");
    assertEquals(9, fixture.mostRecentRevision(RES), "fixture: one revision per change");
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @EnumSource(VersioningType.class)
  @DisplayName("QRY-402: every revision, by number and by timestamp, cold and warm, gives that revision's answer")
  void everyRevisionGivesItsOwnAnswer(final VersioningType versioning) throws Exception {
    build(versioning);
    Outcome previous = null;
    boolean changed = false;
    for (int revision = 1; revision <= 9; revision++) {
      final Instant at = fixture.revisionTimestamp(RES, revision);
      for (final Form form : Form.values()) {
        final String byNumber = form.query(fixture.srcAt(RES, revision), W, "score");
        final Outcome expected = fixture.interpreter(byNumber);
        final String byTime = form.query(fixture.srcAtTime(RES, at), W, "score");
        assertEquals(expected, fixture.interpreter(byTime), "fixture: the timestamp names revision " + revision);
        for (final boolean cold : new boolean[] {true, false}) {
          if (cold) {
            Databases.clearGlobalCaches();
          }
          final int bound = revision;
          final Outcome[] actual = new Outcome[1];
          final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(RES, bound, byNumber, THREADS));
          final String context = versioning + " revision " + revision + " " + form + (cold
              ? " cold"
              : " warm") + " (" + counters + ")";
          assertEquals(expected, actual[0], context);
          assertTrue(counters.handled() > 0, "the route was not reached: " + context);
          try (OpenSession session = fixture.open(RES)) {
            assertEquals(expected, session.autoWired(byTime), "by timestamp: " + context);
          }
        }
        if (form == Form.SUM) {
          if (previous != null && !previous.equals(expected)) {
            changed = true;
          }
          previous = expected;
        }
      }
    }
    assertTrue(changed, "fixture: no revision changed the admin sum, so the revisions are indistinguishable");
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @EnumSource(VersioningType.class)
  @DisplayName("QRY-403: revisions 1, 2, 1, 2 in one session each give their own answer")
  void interleavedRevisionsInOneSession(final VersioningType versioning) throws Exception {
    build(versioning);
    for (final Form form : Form.values()) {
      final String r1 = form.query(fixture.srcAt(RES, 1), W, "score");
      final String r2 = form.query(fixture.srcAt(RES, 2), W, "score");
      final Outcome e1 = fixture.interpreter(r1);
      final Outcome e2 = fixture.interpreter(r2);
      assertNotEquals(e1, e2, "fixture: revisions 1 and 2 must differ for " + form);
      try (OpenSession session = fixture.open(RES)) {
        for (int round = 0; round < 2; round++) {
          assertEquals(e1, session.route(1, r1, THREADS), versioning + " " + form + " revision 1, round " + round);
          assertEquals(e2, session.route(2, r2, THREADS), versioning + " " + form + " revision 2, round " + round);
          assertEquals(e1, session.autoWired(r1), versioning + " " + form + " auto-wired revision 1, round " + round);
          assertEquals(e2, session.autoWired(r2), versioning + " " + form + " auto-wired revision 2, round " + round);
        }
      }
    }
  }

  @Test
  @DisplayName("QRY-403: no cached answer is reused for another literal, member path, flag, function or N")
  void cachedAnswersAreKeyedByTheWholeQuery() throws Exception {
    build(VersioningType.FULL);
    final String s = fixture.srcAt(RES, 1);
    final String[][] sequence = {
        // literal "admin" then "guest"
        {"sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)",
            "sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"guest\" return $r.score)"},
        // path user.tags then tags
        {"count(for $r in " + s + " where $r.user.tags[] = \"admin\" return $r)",
            "count(for $r in " + s + " where $r.tags[] = \"admin\" return $r)"},
        // flag active then not-flagged
        {"sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"dev\" return $r.score)",
            "sum(for $r in " + s + " where $r.user.tags[] = \"dev\" return $r.score)"},
        // function sum then count
        {"sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)",
            "count(for $r in " + s + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)"},
        // N score then id
        {"sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)",
            "sum(for $r in " + s + " where $r.active and $r.user.tags[] = \"admin\" return $r.id)"}};
    final Outcome[][] expected = new Outcome[sequence.length][];
    for (int i = 0; i < sequence.length; i++) {
      expected[i] = new Outcome[] {fixture.interpreter(sequence[i][0]), fixture.interpreter(sequence[i][1])};
      assertNotEquals(expected[i][0], expected[i][1],
          "fixture: the pair must have different answers: " + sequence[i][1]);
    }
    try (OpenSession session = fixture.open(RES)) {
      for (int i = 0; i < sequence.length; i++) {
        for (int round = 0; round < 2; round++) {
          assertEquals(expected[i][0], session.route(1, sequence[i][0], THREADS), sequence[i][0]);
          assertEquals(expected[i][1], session.route(1, sequence[i][1], THREADS),
              "an answer was reused for " + sequence[i][1]);
        }
      }
    }
  }
}
