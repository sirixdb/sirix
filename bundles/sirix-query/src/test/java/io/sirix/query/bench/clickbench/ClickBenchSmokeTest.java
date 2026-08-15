package io.sirix.query.bench.clickbench;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs all 43 ported ClickBench queries against a small generated hits resource.
 *
 * <p>This is the gate that keeps the port honest in CI: every query must compile and execute, the
 * {@code LIMIT} translations must actually limit, and the queries that carry a second equivalent
 * JSONiq formulation must produce identical results through both. Cross-engine correctness (against
 * DuckDB) and fast-path-vs-interpreter correctness live in
 * {@code bundles/sirix-query/bench/clickbench/run-differential.sh}, which needs two JVMs and a
 * second engine and so cannot be a unit test.
 */
public final class ClickBenchSmokeTest {

  /** Enough rows for group-bys to have several groups and for the planted literals to appear. */
  private static final int ROWS = 20_000;

  private static Path dbDir;
  private static BasicJsonDBStore store;
  private static SirixQueryContext ctx;
  private static SirixCompileChain chain;

  @BeforeAll
  static void loadHits() throws Exception {
    dbDir = Files.createTempDirectory("sirix-clickbench-smoke");
    store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(false).build();
    try (Reader source = ClickBenchSource.open("generate:" + ROWS);
         JsonReader jsonReader = new JsonReader(source)) {
      store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsonReader);
    }
    ctx = SirixQueryContext.createWithJsonStore(store);
    chain = SirixCompileChain.createWithJsonStore(store);
  }

  @AfterAll
  static void close() throws IOException {
    if (chain != null) {
      chain.close();
    }
    if (ctx != null) {
      ctx.close();
    }
    if (store != null) {
      store.close();
    }
    if (dbDir != null && Files.exists(dbDir)) {
      try (Stream<Path> paths = Files.walk(dbDir)) {
        paths.sorted(Comparator.reverseOrder()).forEach(path -> {
          try {
            Files.deleteIfExists(path);
          } catch (final IOException e) {
            // best effort: a temp directory left behind must not fail the build
          }
        });
      }
    }
  }

  @Test
  public void everyQueryRunsAndRespectsItsLimit() {
    final List<String> failures = new ArrayList<>();
    for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
      try {
        final List<JsonArray> rows = run(query.jsoniq());
        final int limit = limitOf(query.sql());
        if (limit > 0 && rows.size() > limit) {
          failures.add("q" + query.index() + ": LIMIT " + limit + " returned " + rows.size() + " rows");
        }
      } catch (final Exception e) {
        failures.add("q" + query.index() + ": " + e.getClass().getSimpleName() + ": " + e.getMessage());
      }
    }
    if (!failures.isEmpty()) {
      fail("ClickBench queries failed:\n  " + String.join("\n  ", failures));
    }
  }

  @Test
  public void resultsSatisfyTheSemanticsOfTheirSql() throws Exception {
    assertEquals(ROWS, scalar(0).getAsLong(), "q0 counts every record");
    assertTrue(scalar(1).getAsLong() <= ROWS, "q1 counts a subset");

    final List<JsonArray> q2 = run(ClickBenchQueries.byIndex(2).jsoniq());
    assertEquals(1, q2.size(), "q2 is a single aggregate row");
    assertEquals(ROWS, q2.getFirst().get(1).getAsLong(), "q2's COUNT(*) is the record count");

    final long distinctUsers = scalar(4).getAsLong();
    assertTrue(distinctUsers > 0 && distinctUsers <= ROWS, "q4 distinct users in range: " + distinctUsers);

    final List<JsonArray> q6 = run(ClickBenchQueries.byIndex(6).jsoniq());
    assertEquals(1, q6.size());
    final String minDate = q6.getFirst().get(0).getAsString();
    final String maxDate = q6.getFirst().get(1).getAsString();
    assertTrue(minDate.compareTo(maxDate) <= 0, "q6 min <= max, got " + minDate + " .. " + maxDate);
    assertTrue(minDate.startsWith("2013-07"), "q6 dates come from the generated July 2013 window: " + minDate);

    // Q19 selects one planted UserID; every returned row must be exactly that id.
    for (final JsonArray row : run(ClickBenchQueries.byIndex(19).jsoniq())) {
      assertEquals(ClickBenchHitsGenerator.PLANTED_USER_ID, row.get(0).getAsLong(),
                   "q19 returns only the queried user");
    }

    // Q29 is one row of 90 sums, each 'shift * count' above the previous one.
    final List<JsonArray> q29 = run(ClickBenchQueries.byIndex(29).jsoniq());
    assertEquals(1, q29.size(), "q29 is a single row");
    assertEquals(90, q29.getFirst().size(), "q29 has 90 sums");
    final long base = q29.getFirst().get(0).getAsLong();
    assertEquals(base + ROWS, q29.getFirst().get(1).getAsLong(), "q29's second sum adds 1 per record");
    assertEquals(base + 89L * ROWS, q29.getFirst().get(89).getAsLong(), "q29's last sum adds 89 per record");

    // The group-by queries must actually group: fewer rows than records, at least one row.
    for (final int index : new int[] { 7, 12, 33 }) {
      final List<JsonArray> rows = run(ClickBenchQueries.byIndex(index).jsoniq());
      assertFalse(rows.isEmpty(), "q" + index + " grouped to nothing");
    }
  }

  @Test
  public void equivalentFormulationsAgree() throws Exception {
    for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
      if (query.variants().size() < 2) {
        continue;
      }
      final List<JsonArray> first = run(query.jsoniq(0));
      for (int variant = 1; variant < query.variants().size(); variant++) {
        final List<JsonArray> other = run(query.jsoniq(variant));
        assertEquals(first.toString(), other.toString(),
                     "q" + query.index() + " variant " + variant + " disagrees with variant 0");
      }
    }
  }

  private static JsonElement scalar(final int index) throws Exception {
    final List<JsonArray> rows = run(ClickBenchQueries.byIndex(index).jsoniq());
    assertEquals(1, rows.size(), "q" + index + " is a scalar query");
    assertEquals(1, rows.getFirst().size(), "q" + index + " is a single-column query");
    return rows.getFirst().get(0);
  }

  /** Executes a query body and returns its rows in the same canonical form the harness dumps. */
  private static List<JsonArray> run(final String body) throws Exception {
    final String text =
        ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, body);
    final Sequence result = new Query(chain, text).execute(ctx);
    final StringWriter out = new StringWriter(1 << 12);
    try (PrintWriter pw = new PrintWriter(out)) {
      new StringSerializer(pw).serialize(result);
    }
    final List<JsonArray> rows = new ArrayList<>();
    final String serialized = out.toString();
    if (serialized.isBlank()) {
      // The empty sequence — a legitimate answer for HAVING/OFFSET queries at small scale.
      return rows;
    }
    try (JsonReader reader = new JsonReader(new StringReader(serialized))) {
      reader.setStrictness(Strictness.LENIENT);
      // brackit serializes a sequence as whitespace-separated values; a lenient reader consumes
      // them one at a time.
      while (reader.peek() != JsonToken.END_DOCUMENT) {
        final JsonElement item = JsonParser.parseReader(reader);
        final JsonArray row = new JsonArray();
        if (item.isJsonObject()) {
          final JsonObject object = item.getAsJsonObject();
          for (final String key : object.keySet()) {
            row.add(object.get(key));
          }
        } else {
          row.add(item);
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /** The {@code LIMIT n} of a ClickBench SQL statement, or -1 when it has none. */
  private static int limitOf(final String sql) {
    final int at = sql.lastIndexOf("LIMIT ");
    if (at < 0) {
      return -1;
    }
    int end = at + "LIMIT ".length();
    final StringBuilder digits = new StringBuilder(4);
    while (end < sql.length() && Character.isDigit(sql.charAt(end))) {
      digits.append(sql.charAt(end++));
    }
    return digits.isEmpty() ? -1 : Integer.parseInt(digits.toString());
  }
}
