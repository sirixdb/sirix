package io.sirix.query.bench.jsonbench;

import java.util.List;

/**
 * The <a href="https://github.com/ClickHouse/JSONBench">JSONBench</a> Bluesky schema as SirixDB
 * stores it: one JSON object per firehose event, the whole file as a single JSON array.
 *
 * <h2>What a record looks like</h2> Unlike ClickBench's fixed 105-column table, the Bluesky corpus
 * is genuinely semi-structured — that is the point of the benchmark. Only three fields are present
 * on every event:
 * <ul>
 * <li>{@code did} — the actor's decentralised identifier, a string;</li>
 * <li>{@code time_us} — the firehose timestamp in <em>microseconds</em> since the epoch, an
 * unquoted JSON integer (see the encoding contract below);</li>
 * <li>{@code kind} — {@code "commit"}, {@code "identity"} or {@code "account"}.</li>
 * </ul>
 * Only {@code kind = "commit"} events carry a {@code commit} object ({@code operation},
 * {@code collection}, {@code rev}, {@code rkey} and a {@code record} whose shape varies with the
 * collection). The other two kinds have no {@code commit} at all, which is what makes JSONBench's
 * Q1 interesting: it groups all events by {@code commit.collection}, so the absent-path group has
 * to survive the grouping rather than disappear.
 *
 * <h2>Encoding contract</h2> {@code time_us} must shred as a JSON <em>number</em>. Quoted, it
 * becomes a string node and Q3's integer arithmetic ({@code idiv}) and Q4/Q5's
 * {@code min}/{@code max} either throw or compare lexicographically — the same failure mode that
 * cost ClickBench's 64-bit id columns their queries. {@link JsonBenchLoadMain} checks this on the
 * first {@code commit} record it can find and fails the load rather than let it become a wrong
 * benchmark result.
 *
 * <h2>How ClickHouse sees the same data</h2> The reference side loads each event into a single
 * {@code JSON} column named {@code data} (see {@code jsonbench/clickhouse/ddl.sql}), with
 * {@code kind}, {@code commit.operation}, {@code commit.collection} typed
 * {@code LowCardinality(String)} and {@code time_us} typed {@code UInt64}. The typed-subcolumn
 * declaration matters for the differential: reading a <em>missing</em>
 * {@code data.commit.collection} yields the String default {@code ''}, not SQL NULL, so
 * ClickHouse's Q1 prints the absent-path group as an empty string. Our side reproduces that with an
 * explicit empty-string substitution rather than by accident — see {@link JsonBenchQueries}.
 */
public final class JsonBenchSchema {

  /** The SirixDB database the Bluesky dataset is loaded into. */
  public static final String DATABASE = "jsonbench";

  /** The JSON resource inside {@link #DATABASE} that holds the event array. */
  public static final String RESOURCE = "bluesky.jn";

  /** Root path of the record set: the top-level array's elements. */
  public static final String ROOT_PATH = "/[]";

  /** The {@code kind} value that carries a {@code commit} object. */
  public static final String KIND_COMMIT = "commit";

  /** The {@code commit.operation} value all filtering queries (Q2-Q5) select. */
  public static final String OPERATION_CREATE = "create";

  /** The collection Q4 and Q5 restrict to. */
  public static final String COLLECTION_POST = "app.bsky.feed.post";

  /** The three collections Q3 restricts to, in the SQL's {@code IN} order. */
  public static final List<String> Q3_COLLECTIONS =
      List.of("app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like");

  /** Fields present on <em>every</em> event, whatever its kind. */
  public static final List<String> UNIVERSAL_FIELDS = List.of("did", "time_us", "kind");

  /** Fields present only on {@code kind = "commit"} events, relative to the record root. */
  public static final List<String> COMMIT_FIELDS = List.of("commit/operation", "commit/collection");

  /** Microseconds per hour, for Q3's timezone-free hour-of-day arithmetic. */
  public static final long MICROS_PER_HOUR = 3_600_000_000L;

  /** Microseconds per millisecond, for Q5's span in milliseconds. */
  public static final long MICROS_PER_MILLI = 1_000L;

  private JsonBenchSchema() {
    throw new AssertionError("no instances");
  }
}
