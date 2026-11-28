/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.query.scan.QryCorpus.Form;
import io.sirix.query.scan.QryFixture.Outcome;
import io.sirix.query.scan.QrySeam.Counters;
import io.sirix.query.scan.QrySeam.RouteOutcome;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.ShrinkingMode;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * QRY-201's property: on random documents over the corpus's value domain (0 to 3,000 records, so
 * page counts and seams vary) and random claimed queries, the route's outcome equals the
 * interpreter's, and a claimed query always reaches the route.
 *
 * <p>
 * The value domain is the one of the QRY-202/203/204 corpus: Lemire-shaped records, and, at a rate
 * drawn per document (none, rare or frequent), the odd values of rows E4–E8, T1–T8 and N1–N2 in
 * random fields. A document is generated from a seed, so a failing try is reproducible from the
 * seed jqwik reports.
 */
@QryPlannedProperty(step = QrySteps.ROUTE, value = "QRY-201")
final class RegionPredicatedAggregateProperty {

  private static final String DB = "qry-property-db";
  private static final AtomicInteger RESOURCES = new AtomicInteger();
  private static final String[] LITERALS = {"admin", "guest", "dev", "", "café"};
  private static final List<List<String>> PATHS =
      List.of(List.of("user", "tags"), List.of("tags"), List.of("user", "profile", "tags"));
  private static final String[] ODD_FLAGS = {"null", "\"x\"", "0", "1", "{\"a\":1}", "[true]"};
  private static final String[] ODD_ELEMENTS = {"1", "null", "true", "[\"admin\"]", "{\"a\":\"admin\"}"};
  private static final String[] ODD_SCORES =
      {"null", "\"7\"", "true", "1.5", "2.5e0", "9223372036854775807", "-9223372036854775808", "{\"a\":1}", "[1]"};

  private static Path dir;
  private static QryFixture fixture;

  @BeforeContainer
  static void createDatabase() throws Exception {
    dir = Files.createTempDirectory("sirix-qry-property-");
    fixture = new QryFixture(dir, DB);
  }

  @AfterContainer
  static void dropDatabase() {
    if (fixture != null) {
      fixture.drop();
    }
  }

  /** One claimed query: its form, flags, member tests and {@code N}. */
  record ClaimedQuery(Form form, List<String> flags, List<List<String>> paths, List<String> literals, String n) {

    String render(final String source) {
      final List<String> conjuncts = new ArrayList<>();
      for (final String flag : flags) {
        conjuncts.add("$r." + flag);
      }
      for (int i = 0; i < paths.size(); i++) {
        conjuncts.add("$r." + String.join(".", paths.get(i)) + "[] = \"" + literals.get(i) + "\"");
      }
      return form.query(source, String.join(" and ", conjuncts), n);
    }
  }

  @Provide
  Arbitrary<ClaimedQuery> queries() {
    final Arbitrary<Form> form = Arbitraries.of(Form.class);
    final Arbitrary<List<String>> flags = Arbitraries.of("active", "verified").list().uniqueElements().ofMaxSize(2);
    final Arbitrary<Integer> members = Arbitraries.integers().between(0, 2);
    final Arbitrary<List<List<String>>> paths = Arbitraries.of(PATHS).list().ofSize(2);
    final Arbitrary<List<String>> literals = Arbitraries.of(LITERALS).list().ofSize(2);
    final Arbitrary<String> n = Arbitraries.of("score", "id");
    return Combinators.combine(form, flags, members, paths, literals, n)
                      .as((f, fl, m, p, l, field) -> new ClaimedQuery(f, fl, p.subList(0, m), l.subList(0, m), field))
                      .filter(q -> !q.flags().isEmpty() || !q.paths().isEmpty());
  }

  @Property(tries = 24, shrinking = ShrinkingMode.OFF)
  void routeEqualsInterpreter(@ForAll @IntRange(min = 0, max = 3_000) final int records, @ForAll final long seed,
      @ForAll @IntRange(min = 0, max = 2) final int oddness, @ForAll("queries") final ClaimedQuery query)
      throws Exception {
    final String resource = "p" + RESOURCES.incrementAndGet() + ".jn";
    fixture.store(resource, document(records, seed, oddness));
    final String rendered = query.render(fixture.src(resource));
    final Outcome expected = fixture.interpreter(rendered);
    final Outcome[] actual = new Outcome[1];
    final Counters counters = QrySeam.around(() -> actual[0] = fixture.route(resource, rendered, 2));
    assertEquals(expected, actual[0],
        "records=" + records + " seed=" + seed + " oddness=" + oddness + " query=" + rendered + " (" + counters + ")");
    if (records > 0) {
      assertNotEquals(RouteOutcome.NOT_REACHED, counters.outcome(),
          "a claimed query never reached the route: " + rendered);
    }
  }

  /**
   * A document of {@code records} records; odd values at rate 0, 1/2000 or 1/200 by {@code oddness}.
   */
  static String document(final int records, final long seed, final int oddness) {
    final Random rng = new Random(seed);
    final int oddOneIn = switch (oddness) {
      case 0 -> Integer.MAX_VALUE;
      case 1 -> 2_000;
      default -> 200;
    };
    final StringBuilder sb = new StringBuilder(records * 110 + 2);
    sb.append('[');
    for (int i = 0; i < records; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final boolean odd = rng.nextInt(oddOneIn) == 0;
      final int oddField = odd
          ? rng.nextInt(6)
          : -1;
      sb.append("{\"id\":").append(i);
      if (oddField != 0 || rng.nextBoolean()) {
        sb.append(",\"active\":")
          .append(oddField == 0
              ? ODD_FLAGS[rng.nextInt(ODD_FLAGS.length)]
              : Boolean.toString(rng.nextInt(3) != 0));
      }
      if (rng.nextInt(4) == 0) {
        sb.append(",\"verified\":").append(rng.nextBoolean());
      }
      if (oddField == 1) {
        sb.append(",\"user\":[{\"tags\":[").append(QryCorpus.str(LITERALS[rng.nextInt(LITERALS.length)])).append("]}]");
      } else if (oddField != 2 || rng.nextBoolean()) {
        sb.append(",\"user\":{\"name\":\"u").append(i).append('"');
        if (oddField != 3) {
          sb.append(",\"tags\":");
          appendTags(sb, rng, oddField == 4);
        } else {
          sb.append(",\"tags\":").append(QryCorpus.str(LITERALS[rng.nextInt(LITERALS.length)]));
        }
        if (rng.nextInt(5) == 0) {
          sb.append(",\"profile\":{\"tags\":");
          appendTags(sb, rng, false);
          sb.append('}');
        }
        sb.append('}');
      }
      if (rng.nextInt(10) == 0) {
        sb.append(",\"tags\":");
        appendTags(sb, rng, false);
      }
      if (oddField == 5) {
        sb.append(",\"score\":").append(ODD_SCORES[rng.nextInt(ODD_SCORES.length)]);
      } else if (rng.nextInt(20) != 0) {
        sb.append(",\"score\":").append(rng.nextInt(2001) - 1000);
      }
      sb.append(",\"note\":\"n\"}");
    }
    return sb.append(']').toString();
  }

  private static void appendTags(final StringBuilder sb, final Random rng, final boolean oddElement) {
    final int k = rng.nextInt(400) == 0
        ? 1_100 + rng.nextInt(200)
        : rng.nextInt(4);
    sb.append('[');
    for (int t = 0; t < k; t++) {
      if (t > 0) {
        sb.append(',');
      }
      sb.append(QryCorpus.str(LITERALS[rng.nextInt(LITERALS.length)]));
    }
    if (oddElement) {
      if (k > 0) {
        sb.append(',');
      }
      sb.append(ODD_ELEMENTS[rng.nextInt(ODD_ELEMENTS.length)]);
    }
    sb.append(']');
  }
}
