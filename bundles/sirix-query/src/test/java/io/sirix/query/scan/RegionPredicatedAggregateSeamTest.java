/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.node.NodeKind;
import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.sirix.query.scan.QryCorpus.ADMIN;
import static io.sirix.query.scan.QryCorpus.SLOTS;
import static io.sirix.query.scan.QryCorpus.lemire;
import static io.sirix.query.scan.QryCorpus.lemireNodes;
import static io.sirix.query.scan.QryCorpus.str;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * QRY-303: a seam record, one whose nodes lie on more than one page, must be decided exactly once
 * across the whole scan, by one page or by the record path, with its flag, membership and {@code N}
 * all from the same record; a declining page must clear the seam state it set.
 *
 * <p>
 * {@code jn:store} shreds sequentially, so node keys follow document order. Each fixture uses that
 * to put one node of a matching target record on the last slot of page 1 (key 2047) or on the first
 * slot of page 2 (key 2048): the record object, its {@code active} key, its {@code user} key, its
 * {@code tags} array node, the matching element, the non-matching element, or its {@code score}
 * key. Each position is covered with the literal before and after the other element (so the match
 * falls before and after the seam), with and without a declining page right before the seam (a
 * matching record with a decimal {@code score} on page 1), and at 1 and 4 threads. Every fixture
 * asserts the node kind at the computed key before it trusts the arithmetic.
 */
@QryPlanned(step = QrySteps.ROUTE, value = {"QRY-303", "QRY-503"})
final class RegionPredicatedAggregateSeamTest {

  private static final String DB = "qry-seam-db";
  private static final long BOUNDARY = 2L * SLOTS;
  private static final long END = 4L * SLOTS;
  private static final String W = "$r.active and $r.user.tags[] = \"admin\"";
  private static final String TARGET_SCORE = "4242";

  /** The node of the target record placed on the seam. */
  enum Target {
    RECORD, ACTIVE, USER, TAGS, MATCHING_ELEMENT, NON_MATCHING_ELEMENT, SCORE
  }

  /** Which side of the page boundary the target node takes. */
  enum Side {
    LAST_SLOT, FIRST_SLOT
  }

  private static Path dir;
  private static QryFixture fixture;

  @BeforeAll
  static void createDatabase() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-seam-");
    fixture = new QryFixture(dir, DB);
  }

  @AfterAll
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  static Stream<Arguments> placements() {
    final List<Arguments> rows = new ArrayList<>();
    for (final Target target : Target.values()) {
      for (final Side side : Side.values()) {
        for (final boolean literalFirst : new boolean[] {true, false}) {
          for (final boolean declineBefore : new boolean[] {false, true}) {
            rows.add(Arguments.of(target, side, literalFirst, declineBefore));
          }
        }
      }
    }
    return rows.stream();
  }

  /** One seam fixture: its document, the key of the target node, and that node's kind and name. */
  record Placement(String json, long key, NodeKind kind, String name) {
  }

  /**
   * Builds the fixture: known-size Lemire records up to the boundary, the target record (two tags, so
   * 10 keys) placed so that its {@code target} node takes the slot of {@code side}, then records up
   * to page 4.
   */
  static Placement place(final Target target, final Side side, final boolean literalFirst,
      final boolean declineBefore) {
    final List<String> records = new ArrayList<>();
    long nextKey = QryCorpus.FIRST_RECORD_KEY;
    boolean declined = false;
    int i = 0;
    // Known-size records, leaving room for the placement's fillers and shim.
    while (nextKey < BOUNDARY - 64) {
      final int k = i % 4;
      final List<String> tags = new ArrayList<>(k);
      for (int t = 0; t < k; t++) {
        tags.add(str((i + t) % 3 == 0
            ? ADMIN
            : "dev"));
      }
      String score = Integer.toString(i % 50);
      if (declineBefore && !declined && nextKey >= SLOTS + 64 && k > 0 && tags.contains(str(ADMIN))) {
        score = "1.5"; // a matching record with a decimal N: page 1 declines (for sum)
        declined = true;
      }
      records.add(lemire(i, i % 3 != 0
          ? "true"
          : "false", tags, score));
      nextKey += lemireNodes(k);
      i++;
    }
    if (declineBefore && !declined) {
      throw new IllegalStateException("fixture: no declining record placed on page 1");
    }
    final List<String> tags = literalFirst
        ? List.of(str(ADMIN), str("guest"))
        : List.of(str("guest"), str(ADMIN));
    final int matchOffset = literalFirst
        ? QryCorpus.OFF_FIRST_ELEMENT
        : QryCorpus.OFF_FIRST_ELEMENT + 1;
    final int nonMatchOffset = literalFirst
        ? QryCorpus.OFF_FIRST_ELEMENT + 1
        : QryCorpus.OFF_FIRST_ELEMENT;
    final int offset;
    final NodeKind kind;
    final String name;
    switch (target) {
      case RECORD -> {
        offset = QryCorpus.OFF_OBJECT;
        kind = NodeKind.OBJECT;
        name = null;
      }
      case ACTIVE -> {
        offset = QryCorpus.OFF_ACTIVE;
        kind = NodeKind.OBJECT_NAMED_BOOLEAN;
        name = "active";
      }
      case USER -> {
        offset = QryCorpus.OFF_USER;
        kind = NodeKind.OBJECT_NAMED_OBJECT;
        name = "user";
      }
      case TAGS -> {
        offset = QryCorpus.OFF_TAGS;
        kind = NodeKind.OBJECT_NAMED_ARRAY;
        name = "tags";
      }
      case MATCHING_ELEMENT -> {
        offset = matchOffset;
        kind = NodeKind.STRING_VALUE;
        name = ADMIN;
      }
      case NON_MATCHING_ELEMENT -> {
        offset = nonMatchOffset;
        kind = NodeKind.STRING_VALUE;
        name = "guest";
      }
      case SCORE -> {
        offset = QryCorpus.scoreOffset(2);
        kind = NodeKind.OBJECT_NAMED_NUMBER;
        name = "score";
      }
      default -> throw new IllegalArgumentException(target.toString());
    }
    final long key = side == Side.LAST_SLOT
        ? BOUNDARY - 1
        : BOUNDARY;
    final long start = QryCorpus.placeNextRecord(records, nextKey, key, offset);
    records.add(lemire(-42, "true", tags, TARGET_SCORE));
    nextKey = start + lemireNodes(2);
    while (nextKey < END) {
      final int k = i % 3;
      final List<String> more = new ArrayList<>(k);
      for (int t = 0; t < k; t++) {
        more.add(str(t == 0 && i % 2 == 0
            ? ADMIN
            : "ops"));
      }
      records.add(lemire(i, "true", more, Integer.toString(i % 70)));
      nextKey += lemireNodes(k);
      i++;
    }
    return new Placement(QryCorpus.array(records), key, kind, name);
  }

  @ParameterizedTest(name = "[{index}] {0} on the {1}, literal first={2}, declining page before={3}")
  @MethodSource("placements")
  @DisplayName("QRY-303: a seam record is decided exactly once, at 1 and 4 threads")
  void seamRecordIsDecidedOnce(final Target target, final Side side, final boolean literalFirst,
      final boolean declineBefore) throws Exception {
    final Placement placement = place(target, side, literalFirst, declineBefore);
    final String resource = "seam-" + target + "-" + side + "-" + literalFirst + "-" + declineBefore + ".jn";
    fixture.store(resource, placement.json());
    fixture.assertNodeAt(resource, placement.key(), placement.kind(), placement.name());
    for (final Form form : Form.values()) {
      final String query = form.query(fixture.src(resource), W, "score");
      final Outcome expected = fixture.interpreter(query);
      for (final int threads : new int[] {1, 4}) {
        final Outcome[] actual = new Outcome[1];
        final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, query, threads));
        final String context =
            target + " on the " + side + " " + form + " at " + threads + " threads (" + counters + ")";
        assertEquals(expected, actual[0], context);
        assertEquals(0, counters.refused(), context);
        assertTrue(counters.served() > 0, "no page served, so no seam was crossed by the route: " + context);
        if (!(declineBefore && form == Form.SUM)) {
          assertEquals(RouteOutcome.SERVED, counters.outcome(), context);
        } else {
          assertEquals(1, counters.declined(), "only the decimal's page declines: " + context);
        }
      }
    }
  }
}
