package io.sirix.query.bench.jsonbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The five <a href="https://github.com/ClickHouse/JSONBench">JSONBench</a> queries, ported from the
 * ClickHouse reference dialect ({@code jsonbench/clickhouse/queries.sql}) to JSONiq over a SirixDB
 * JSON resource holding the Bluesky events as an array of objects.
 *
 * <p>
 * Every query body assumes a variable {@code $events} bound to the document node of the resource;
 * {@link #wrap(String, String, String)} produces the executable query text. The binding is emitted
 * as a literal {@code jn:doc(...)} let-binding rather than an externally bound variable because
 * that is the shape the analytical fast paths detect — an externally bound document silently gets
 * the generic pipeline.
 *
 * <h2>Translation rules</h2> Each one is a place where the obvious translation would be wrong
 * against the ClickHouse reference:
 * <ul>
 * <li><b>Absent paths print as the empty string.</b> The reference schema types
 * {@code data.commit.collection} as {@code LowCardinality(String)}, so an event without a
 * {@code commit} object reads as {@code ''} — not NULL — and Q1's group for the 5328
 * {@code kind='identity'} events prints with an empty first column. A JSONiq deref of an absent
 * field yields the empty sequence, which would either form a group with no value or drop out
 * entirely, so Q1 wraps the key in {@code fn:string(...)}: {@code string(())} is {@code ""}, which
 * is exactly ClickHouse's substitution. It is also a single unary function over a column deref
 * rather than an {@code if (exists(...))} branch, which keeps the key expression in reach of the
 * group-by fast path.</li>
 * <li><b>Hour-of-day is integer arithmetic, not a date function.</b> ClickHouse's
 * {@code toHour(fromUnixTimestamp64Micro(time_us))} reads the session timezone, so the same query
 * answers 17 in Europe/Berlin and 16 in UTC. {@code (time_us idiv 3600000000) mod 24} is
 * timezone-free and equals the UTC hour by construction; the reference TSV is regenerated with
 * {@code SETTINGS session_timezone='UTC'} to match (see {@code bench/jsonbench/README.md}).</li>
 * <li><b>Q5's span truncates each end to milliseconds before subtracting.</b> ClickHouse's
 * {@code date_diff('milliseconds', a, b)} counts unit boundaries crossed, so it is
 * {@code (max idiv 1000) - (min idiv 1000)} and not {@code (max - min) idiv 1000}. The two differ:
 * for the top user (min {@code ...582101}, max {@code ...589060}) they give 813007 and 813006, and
 * the reference says 813007. Measured, not assumed.</li>
 * <li><b>{@code LIMIT n} becomes {@code fn:subsequence(expr, 1, n)}.</b> Brackit parses
 * {@code [...]} as a JSONiq array index, not as an XPath positional predicate.</li>
 * <li><b>{@code COUNT(DISTINCT x)} becomes {@code count(distinct-values($g.x))}</b> over the
 * grouped variable. The post-group path form is deliberate: an equivalent nested
 * {@code for $r in $g return $r.x} inside an aggregate is mis-served by the vectorized executor,
 * which folds the whole ungrouped input per group.</li>
 * <li><b>Aggregates are bound by post-group {@code let}s</b>, one builtin per binding, with any
 * arithmetic in a further derived {@code let} (Q5). That keeps
 * {@code min}/{@code max}/{@code count} in the shape the group-aggregate route pattern-matches
 * instead of burying them inside an expression tree.</li>
 * </ul>
 *
 * <h2>Ordering determinism</h2> Q1-Q3 order by values that are unique in the corpus, and neither
 * LIMIT-3 boundary ties: exactly one actor holds Q4's third-smallest first-post timestamp, and Q5's
 * third span (811404) is well clear of the fourth (811016). The differential can therefore require
 * an exact row-for-row match rather than tolerating an arbitrary tie-completion.
 */
public final class JsonBenchQueries {

  /** A single JSONBench query: its reference SQL and one or more equivalent JSONiq formulations. */
  public record Query(int index, String sql, List<String> variants) {

    public Query {
      Objects.requireNonNull(sql, "sql");
      Objects.requireNonNull(variants, "variants");
      if (index < 1) {
        throw new IllegalArgumentException("index must be >= 1: " + index);
      }
      if (variants.isEmpty()) {
        throw new IllegalArgumentException("query " + index + " has no JSONiq variant");
      }
      variants = List.copyOf(variants);
    }

    /** The default (measured) formulation. */
    public String jsoniq() {
      return variants.getFirst();
    }

    /**
     * @param variant zero-based variant index; clamped to the last available variant so a harness
     *        sweeping "variant 1" over all queries does not have to know which ones have a second
     *        formulation
     */
    public String jsoniq(final int variant) {
      if (variant < 0) {
        throw new IllegalArgumentException("variant must be >= 0: " + variant);
      }
      return variants.get(Math.min(variant, variants.size() - 1));
    }
  }

  /** Query indexes are 1-based, matching {@code queries.sql} line numbers and the reference TSVs. */
  public static final int FIRST_INDEX = 1;

  private static final List<Query> QUERIES = build();

  private JsonBenchQueries() {
    throw new AssertionError("no instances");
  }

  /** All five queries in JSONBench order (index 1 == first line of {@code queries.sql}). */
  public static List<Query> all() {
    return QUERIES;
  }

  public static Query byIndex(final int index) {
    for (final Query query : QUERIES) {
      if (query.index() == index) {
        return query;
      }
    }
    throw new IndexOutOfBoundsException("no JSONBench query with index " + index);
  }

  /** How many queries the suite holds. */
  public static int count() {
    return QUERIES.size();
  }

  /**
   * Wraps a query body in the {@code $events} binding the bodies assume.
   *
   * @param database the SirixDB database name
   * @param resource the JSON resource inside it
   * @param body a body written against {@code $events}
   * @return executable query text
   */
  public static String wrap(final String database, final String resource, final String body) {
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(resource, "resource");
    Objects.requireNonNull(body, "body");
    return "let $events := jn:doc('" + database + "','" + resource + "') return (" + body + ")";
  }

  /** The filter Q2-Q5 share: a create commit. Q3-Q5 narrow it further by collection. */
  private static final String CREATE_COMMIT = "$e.kind = \"commit\" and $e.commit.operation = \"create\"";

  private static List<Query> build() {
    final List<Query> qs = new ArrayList<>(5);

    // Q1: SELECT data.commit.collection AS event, count() AS count FROM bluesky
    // GROUP BY event ORDER BY count DESC;
    add(qs, 1,
        "SELECT data.commit.collection AS event, count() AS count FROM bluesky GROUP BY event ORDER BY count DESC;", """
            for $e in $events[]
            let $k := string($e.commit.collection)
            group by $k
            let $c := count($e)
            order by $c descending
            return {"event": $k, "count": $c}""");

    // Q2: SELECT data.commit.collection AS event, count() AS count, uniqExact(data.did) AS users
    // FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create'
    // GROUP BY event ORDER BY count DESC;
    add(qs, 2, "SELECT data.commit.collection AS event, count() AS count, uniqExact(data.did) AS users FROM bluesky "
        + "WHERE data.kind = 'commit' AND data.commit.operation = 'create' GROUP BY event ORDER BY count DESC;", """
            for $e in $events[]
            where %s
            let $k := $e.commit.collection
            group by $k
            let $c := count($e)
            let $u := count(distinct-values($e.did))
            order by $c descending
            return {"event": $k, "count": $c, "users": $u}""".formatted(CREATE_COMMIT));

    // Q3: SELECT data.commit.collection AS event, toHour(fromUnixTimestamp64Micro(data.time_us)) AS
    // hour_of_day, count() AS count FROM bluesky WHERE data.kind = 'commit' AND
    // data.commit.operation = 'create' AND data.commit.collection IN [...3...]
    // GROUP BY event, hour_of_day ORDER BY hour_of_day, event;
    add(qs, 3,
        "SELECT data.commit.collection AS event, toHour(fromUnixTimestamp64Micro(data.time_us)) as hour_of_day, "
            + "count() AS count FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND "
            + "data.commit.collection in ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like'] "
            + "GROUP BY event, hour_of_day ORDER BY hour_of_day, event;",
        """
            for $e in $events[]
            where %s
              and ($e.commit.collection = "app.bsky.feed.post"
                or $e.commit.collection = "app.bsky.feed.repost"
                or $e.commit.collection = "app.bsky.feed.like")
            let $k := $e.commit.collection, $hour := ($e.time_us idiv %d) mod 24
            group by $k, $hour
            let $c := count($e)
            order by $hour, $k
            return {"event": $k, "hour_of_day": $hour, "count": $c}""".formatted(CREATE_COMMIT,
            JsonBenchSchema.MICROS_PER_HOUR));

    // Q4: SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as
    // first_post_ts FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create'
    // AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id
    // ORDER BY first_post_ts ASC LIMIT 3;
    add(qs, 4,
        "SELECT data.did::String as user_id, min(fromUnixTimestamp64Micro(data.time_us)) as first_post_ts "
            + "FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation = 'create' AND "
            + "data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;",
        """
            subsequence(
              for $e in $events[]
              where %s and $e.commit.collection = "%s"
              let $k := $e.did
              group by $k
              let $first := min($e.time_us)
              order by $first
              return {"user_id": $k, "first_post_ts": $first}, 1, 3)""".formatted(CREATE_COMMIT,
            JsonBenchSchema.COLLECTION_POST));

    // Q5: SELECT data.did::String as user_id, date_diff('milliseconds',
    // min(fromUnixTimestamp64Micro(data.time_us)), max(fromUnixTimestamp64Micro(data.time_us)))
    // AS activity_span FROM bluesky WHERE data.kind = 'commit' AND data.commit.operation =
    // 'create' AND data.commit.collection = 'app.bsky.feed.post' GROUP BY user_id
    // ORDER BY activity_span DESC LIMIT 3;
    add(qs, 5,
        "SELECT data.did::String as user_id, date_diff( 'milliseconds', min(fromUnixTimestamp64Micro(data.time_us)), "
            + "max(fromUnixTimestamp64Micro(data.time_us))) AS activity_span FROM bluesky WHERE data.kind = 'commit' "
            + "AND data.commit.operation = 'create' AND data.commit.collection = 'app.bsky.feed.post' "
            + "GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;",
        """
            subsequence(
              for $e in $events[]
              where %s and $e.commit.collection = "%s"
              let $k := $e.did
              group by $k
              let $first := min($e.time_us)
              let $last := max($e.time_us)
              let $span := ($last idiv %d) - ($first idiv %d)
              order by $span descending
              return {"user_id": $k, "activity_span": $span}, 1, 3)""".formatted(CREATE_COMMIT,
            JsonBenchSchema.COLLECTION_POST, JsonBenchSchema.MICROS_PER_MILLI, JsonBenchSchema.MICROS_PER_MILLI));

    return Collections.unmodifiableList(qs);
  }

  private static void add(final List<Query> qs, final int index, final String sql, final String... variants) {
    qs.add(new Query(index, sql, List.of(variants)));
  }
}
