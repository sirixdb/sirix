package io.sirix.query.bench.clickbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The 43 <a href="https://github.com/ClickHouse/ClickBench">ClickBench</a> queries, ported from SQL
 * (the {@code duckdb/queries.sql} dialect, which is the plainest of the reference dialects) to
 * JSONiq over a SirixDB JSON resource holding the {@code hits} records as an array of objects.
 *
 * <p>
 * Every query body assumes a variable {@code $hits} bound to the document node of the hits
 * resource; {@link #wrap(String, String, String)} produces the executable query text. The binding
 * is emitted as a literal {@code jn:doc(...)} let-binding rather than an externally bound variable
 * because that is the shape the analytical fast paths detect (see {@code ScaleBenchMain} and
 * {@code SirixVsDuckBenchMain}, which do the same).
 *
 * <h2>Translation rules</h2> These are forced by the engine, and each one is a place where a naive
 * translation would be wrong or would silently fall off a fast path:
 * <ul>
 * <li>{@code LIMIT n} / {@code OFFSET k} become {@code fn:subsequence(expr, k + 1, n)}. Brackit
 * parses {@code [...]} as a JSONiq array index, not as an XPath positional predicate, so
 * {@code [position() le 10]} is not available.</li>
 * <li>{@code HAVING} becomes a {@code where} clause placed after {@code group by} (XQuery 3.0 free
 * clause ordering).</li>
 * <li>{@code COUNT(DISTINCT x)} becomes {@code count(distinct-values($g.x))} over the grouped
 * variable. The post-group path form is used deliberately: an equivalent nested
 * {@code for $r in $g return $r.x} inside an aggregate is mis-served by the vectorized executor (it
 * folds the whole ungrouped input, see docs/CLICKBENCH.md).</li>
 * <li>{@code AVG(...)} is wrapped in {@code xs:double(...)}: brackit's {@code fn:avg} returns
 * {@code xs:integer} when the quotient is exact and {@code xs:decimal} otherwise, while SQL
 * {@code AVG} is a double. The cast makes the two engines' results comparable.</li>
 * <li>{@code LIKE '%x%'} becomes {@code fn:contains}, {@code NOT LIKE} becomes
 * {@code fn:not(fn:contains(...))}.</li>
 * <li>{@code REGEXP_REPLACE} becomes {@code fn:replace} with the non-capturing group
 * {@code (?:www\.)?} rewritten as a capturing group (brackit's XSD-to-Java regex translation
 * rejects every {@code (?...)} construct with FORX0002) and the replacement renumbered to
 * {@code $2}. The regex sits in a single-quoted literal because brackit applies JSON escape rules
 * inside double-quoted literals, where {@code \.} is not a legal escape.</li>
 * <li>Timestamps are ISO-8601 strings (see {@link ClickBenchSchema}), so
 * {@code extract(minute FROM EventTime)} is {@code xs:integer(substring($t, 15, 2))} and
 * {@code DATE_TRUNC('minute', EventTime)} is {@code substring($t, 1, 16)}; ISO-8601 orders
 * lexicographically, so {@code ORDER BY EventTime} and the {@code EventDate} range predicates are
 * plain string comparisons.</li>
 * </ul>
 *
 * <p>
 * Where a second formulation of the same query is legitimate (SQL-equivalent, no pre-aggregation,
 * no changed semantics) it is carried as an additional variant so the harness can measure both and
 * the differential test can require that they agree. Variant 0 is the default.
 */
public final class ClickBenchQueries {

  /** A single ClickBench query: its official SQL and one or more equivalent JSONiq formulations. */
  public record Query(int index, String sql, List<String> variants) {

    public Query {
      Objects.requireNonNull(sql, "sql");
      Objects.requireNonNull(variants, "variants");
      if (index < 0) {
        throw new IllegalArgumentException("index must be >= 0: " + index);
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
     *        sweeping "variant 1" over all 43 queries does not have to know which ones have a second
     *        formulation
     */
    public String jsoniq(final int variant) {
      if (variant < 0) {
        throw new IllegalArgumentException("variant must be >= 0: " + variant);
      }
      return variants.get(Math.min(variant, variants.size() - 1));
    }
  }

  private static final List<Query> QUERIES = build();

  private ClickBenchQueries() {
    throw new AssertionError("no instances");
  }

  /** All 43 queries in ClickBench order (index 0 == first line of {@code queries.sql}). */
  public static List<Query> all() {
    return QUERIES;
  }

  public static Query byIndex(final int index) {
    if (index < 0 || index >= QUERIES.size()) {
      throw new IndexOutOfBoundsException("no ClickBench query with index " + index);
    }
    return QUERIES.get(index);
  }

  /** Matches {@code $var.Column} — the only way these queries reach a field. */
  private static final Pattern FIELD_DEREF = Pattern.compile("\\$\\w+\\.([A-Za-z_][A-Za-z0-9_]*)");

  /**
   * Every column name the queries dereference, across ALL variants.
   *
   * <p>
   * Read off the query text rather than maintained by hand: this drives which columns
   * {@link ClickBenchProjection} projects, and a hand-kept list would drift the moment a query is
   * edited — silently, as a decline at run time instead of a build error.
   *
   * <p>
   * Names that are not columns are NOT filtered here (a group key bound to a literal, say). The
   * caller cross-checks against {@link ClickBenchSchema#COLUMNS}, so an unknown name surfaces as an
   * error rather than being quietly dropped.
   *
   * @return the dereferenced names, in first-appearance order
   */
  public static Set<String> referencedColumns() {
    final Set<String> columns = new LinkedHashSet<>();
    for (final Query query : QUERIES) {
      for (final String variant : query.variants()) {
        final Matcher matcher = FIELD_DEREF.matcher(variant);
        while (matcher.find()) {
          columns.add(matcher.group(1));
        }
      }
    }
    return columns;
  }

  /**
   * Wraps a query body into executable query text binding {@code $hits} to the resource.
   *
   * @param database the SirixDB database name
   * @param resource the JSON resource name
   * @param body a JSONiq body referring to {@code $hits}
   * @return the executable query
   */
  public static String wrap(final String database, final String resource, final String body) {
    Objects.requireNonNull(database, "database");
    Objects.requireNonNull(resource, "resource");
    Objects.requireNonNull(body, "body");
    return "let $hits := jn:doc('" + database + "','" + resource + "') return (" + body + ")";
  }

  /** The July-2013 window shared by Q36-Q41; Q42 uses its own two-day window. */
  private static final String JULY_2013 =
      "$h.CounterID = 62 and $h.EventDate >= \"2013-07-01\" and $h.EventDate <= \"2013-07-31\"";

  private static List<Query> build() {
    final List<Query> qs = new ArrayList<>(43);

    // Q0: SELECT COUNT(*) FROM hits;
    add(qs, "SELECT COUNT(*) FROM hits;", "count($hits[])");

    // Q1: SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;
    add(qs, "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;",
        "count(for $h in $hits[] where $h.AdvEngineID != 0 return $h)");

    // Q2: SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;
    add(qs, "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;",
        // variant 0: three independent aggregates, each of which can take an aggregate fast path
        """
            {"sum_AdvEngineID": sum(for $h in $hits[] return $h.AdvEngineID),
             "count": count($hits[]),
             "avg_ResolutionWidth": xs:double(avg(for $h in $hits[] return $h.ResolutionWidth))}""",
        // variant 1: one pass, grouping on a constant
        """
            for $h in $hits[]
            let $g := 1, $adv := $h.AdvEngineID, $rw := $h.ResolutionWidth
            group by $g
            return {"sum_AdvEngineID": sum($adv), "count": count($h),
                    "avg_ResolutionWidth": xs:double(avg($rw))}""");

    // Q3: SELECT AVG(UserID) FROM hits;
    add(qs, "SELECT AVG(UserID) FROM hits;", "xs:double(avg(for $h in $hits[] return $h.UserID))");

    // Q4: SELECT COUNT(DISTINCT UserID) FROM hits;
    add(qs, "SELECT COUNT(DISTINCT UserID) FROM hits;", "count(distinct-values(for $h in $hits[] return $h.UserID))");

    // Q5: SELECT COUNT(DISTINCT SearchPhrase) FROM hits;
    add(qs, "SELECT COUNT(DISTINCT SearchPhrase) FROM hits;",
        "count(distinct-values(for $h in $hits[] return $h.SearchPhrase))");

    // Q6: SELECT MIN(EventDate), MAX(EventDate) FROM hits;
    add(qs, "SELECT MIN(EventDate), MAX(EventDate) FROM hits;", """
        {"min_EventDate": min(for $h in $hits[] return $h.EventDate),
         "max_EventDate": max(for $h in $hits[] return $h.EventDate)}""");

    // Q7: SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY
    // COUNT(*) DESC;
    add(qs,
        "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;",
        """
            for $h in $hits[]
            where $h.AdvEngineID != 0
            let $k := $h.AdvEngineID
            group by $k
            let $c := count($h)
            order by $c descending
            return {"AdvEngineID": $k, "count": $c}""");

    // Q8: SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC
    // LIMIT 10;
    add(qs, "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          let $k := $h.RegionID
          group by $k
          let $u := count(distinct-values($h.UserID))
          order by $u descending
          return {"RegionID": $k, "u": $u}, 1, 10)""");

    // Q9: SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT
    // UserID)
    // FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) "
        + "FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              let $k := $h.RegionID
              group by $k
              let $c := count($h)
              order by $c descending
              return {"RegionID": $k, "sum_AdvEngineID": sum($h.AdvEngineID), "c": $c,
                      "avg_ResolutionWidth": xs:double(avg($h.ResolutionWidth)),
                      "uniq_UserID": count(distinct-values($h.UserID))}, 1, 10)""");

    // Q10: SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> ''
    // GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;
    add(qs, "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' "
        + "GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.MobilePhoneModel != ""
              let $k := $h.MobilePhoneModel
              group by $k
              let $u := count(distinct-values($h.UserID))
              order by $u descending
              return {"MobilePhoneModel": $k, "u": $u}, 1, 10)""");

    // Q11: SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits
    // WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;
    add(qs, "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' "
        + "GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.MobilePhoneModel != ""
              let $p := $h.MobilePhone, $m := $h.MobilePhoneModel
              group by $p, $m
              let $u := count(distinct-values($h.UserID))
              order by $u descending
              return {"MobilePhone": $p, "MobilePhoneModel": $m, "u": $u}, 1, 10)""");

    // Q12: SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' "
        + "GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.SearchPhrase != ""
              let $k := $h.SearchPhrase
              group by $k
              let $c := count($h)
              order by $c descending
              return {"SearchPhrase": $k, "c": $c}, 1, 10)""");

    // Q13: SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;
    add(qs, "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' "
        + "GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.SearchPhrase != ""
              let $k := $h.SearchPhrase
              group by $k
              let $u := count(distinct-values($h.UserID))
              order by $u descending
              return {"SearchPhrase": $k, "u": $u}, 1, 10)""");

    // Q14: SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> ''
    // GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' "
        + "GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.SearchPhrase != ""
              let $e := $h.SearchEngineID, $s := $h.SearchPhrase
              group by $e, $s
              let $c := count($h)
              order by $c descending
              return {"SearchEngineID": $e, "SearchPhrase": $s, "c": $c}, 1, 10)""");

    // Q15: SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;
    add(qs, "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          let $k := $h.UserID
          group by $k
          let $c := count($h)
          order by $c descending
          return {"UserID": $k, "count": $c}, 1, 10)""");

    // Q16: SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase
    // ORDER BY COUNT(*) DESC LIMIT 10;
    add(qs, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase "
        + "ORDER BY COUNT(*) DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              let $u := $h.UserID, $s := $h.SearchPhrase
              group by $u, $s
              let $c := count($h)
              order by $c descending
              return {"UserID": $u, "SearchPhrase": $s, "count": $c}, 1, 10)""");

    // Q17: SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;
    add(qs, "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          let $u := $h.UserID, $s := $h.SearchPhrase
          group by $u, $s
          return {"UserID": $u, "SearchPhrase": $s, "count": count($h)}, 1, 10)""");

    // Q18: SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits
    // GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;
    add(qs, "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits "
        + "GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              let $u := $h.UserID, $m := xs:integer(substring($h.EventTime, 15, 2)), $s := $h.SearchPhrase
              group by $u, $m, $s
              let $c := count($h)
              order by $c descending
              return {"UserID": $u, "m": $m, "SearchPhrase": $s, "count": $c}, 1, 10)""");

    // Q19: SELECT UserID FROM hits WHERE UserID = 435090932899640449;
    add(qs, "SELECT UserID FROM hits WHERE UserID = 435090932899640449;",
        "for $h in $hits[] where $h.UserID = 435090932899640449 return $h.UserID");

    // Q20: SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';
    add(qs, "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';",
        "count(for $h in $hits[] where contains($h.URL, \"google\") return $h)");

    // Q21: SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%'
    // AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' "
        + "GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where contains($h.URL, "google") and $h.SearchPhrase != ""
              let $k := $h.SearchPhrase
              group by $k
              let $c := count($h)
              order by $c descending
              return {"SearchPhrase": $k, "min_URL": min($h.URL), "c": $c}, 1, 10)""");

    // Q22: SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits
    // WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> ''
    // GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;
    add(qs,
        "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits "
            + "WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' "
            + "GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
        """
            subsequence(
              for $h in $hits[]
              where contains($h.Title, "Google") and not(contains($h.URL, ".google.")) and $h.SearchPhrase != ""
              let $k := $h.SearchPhrase
              group by $k
              let $c := count($h)
              order by $c descending
              return {"SearchPhrase": $k, "min_URL": min($h.URL), "min_Title": min($h.Title), "c": $c,
                      "uniq_UserID": count(distinct-values($h.UserID))}, 1, 10)""");

    // Q23: SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;
    add(qs, "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          where contains($h.URL, "google")
          order by $h.EventTime
          return $h, 1, 10)""");

    // Q24: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;
    add(qs, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          where $h.SearchPhrase != ""
          order by $h.EventTime
          return $h.SearchPhrase, 1, 10)""");

    // Q25: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;
    add(qs, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          where $h.SearchPhrase != ""
          order by $h.SearchPhrase
          return $h.SearchPhrase, 1, 10)""");

    // Q26: SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase
    // LIMIT 10;
    add(qs, "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          where $h.SearchPhrase != ""
          order by $h.EventTime, $h.SearchPhrase
          return $h.SearchPhrase, 1, 10)""");

    // Q27: SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> ''
    // GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
    add(qs, "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' "
        + "GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;", """
            subsequence(
              for $h in $hits[]
              where $h.URL != ""
              let $k := $h.CounterID, $len := jn:utf8-length($h.URL)
              group by $k
              let $c := count($h)
              where $c > 100000
              let $l := xs:double(avg($len))
              order by $l descending
              return {"CounterID": $k, "l": $l, "c": $c}, 1, 25)""");

    // Q28: SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k,
    // AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> ''
    // GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;
    add(qs,
        "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(STRLEN(Referer)) AS l, "
            + "COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k "
            + "HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25;",
        """
            subsequence(
              for $h in $hits[]
              where $h.Referer != ""
              let $k := replace($h.Referer, '^https?://(www\\.)?([^/]+)/.*$', '$2'), $len := jn:utf8-length($h.Referer)
              group by $k
              let $c := count($h)
              where $c > 100000
              let $l := xs:double(avg($len))
              order by $l descending
              return {"k": $k, "l": $l, "c": $c, "min_Referer": min($h.Referer)}, 1, 25)""");

    // Q29: SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM
    // hits;
    qs.add(new Query(29, resolutionWidthSumSql(),
        List.of(resolutionWidthSumsOnePass(), resolutionWidthSumsIndependent())));

    // Q30: SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM
    // hits
    // WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits "
        + "WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.SearchPhrase != ""
              let $e := $h.SearchEngineID, $ip := $h.ClientIP
              group by $e, $ip
              let $c := count($h)
              order by $c descending
              return {"SearchEngineID": $e, "ClientIP": $ip, "c": $c, "sum_IsRefresh": sum($h.IsRefresh),
                      "avg_ResolutionWidth": xs:double(avg($h.ResolutionWidth))}, 1, 10)""");

    // Q31: SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits
    // WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits "
        + "WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              where $h.SearchPhrase != ""
              let $w := $h.WatchID, $ip := $h.ClientIP
              group by $w, $ip
              let $c := count($h)
              order by $c descending
              return {"WatchID": $w, "ClientIP": $ip, "c": $c, "sum_IsRefresh": sum($h.IsRefresh),
                      "avg_ResolutionWidth": xs:double(avg($h.ResolutionWidth))}, 1, 10)""");

    // Q32: SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits
    // GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits "
        + "GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              let $w := $h.WatchID, $ip := $h.ClientIP
              group by $w, $ip
              let $c := count($h)
              order by $c descending
              return {"WatchID": $w, "ClientIP": $ip, "c": $c, "sum_IsRefresh": sum($h.IsRefresh),
                      "avg_ResolutionWidth": xs:double(avg($h.ResolutionWidth))}, 1, 10)""");

    // Q33: SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          let $k := $h.URL
          group by $k
          let $c := count($h)
          order by $c descending
          return {"URL": $k, "c": $c}, 1, 10)""");

    // Q34: SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;", """
        subsequence(
          for $h in $hits[]
          let $one := 1, $k := $h.URL
          group by $one, $k
          let $c := count($h)
          order by $c descending
          return {"one": $one, "URL": $k, "c": $c}, 1, 10)""");

    // Q35: SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits
    // GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;
    add(qs, "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits "
        + "GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10;", """
            subsequence(
              for $h in $hits[]
              let $a := $h.ClientIP, $b := $h.ClientIP - 1, $c2 := $h.ClientIP - 2, $d := $h.ClientIP - 3
              group by $a, $b, $c2, $d
              let $c := count($h)
              order by $c descending
              return {"ClientIP": $a, "m1": $b, "m2": $c2, "m3": $d, "c": $c}, 1, 10)""");

    // Q36: SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >=
    // '2013-07-01'
    // AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> ''
    // GROUP BY URL ORDER BY PageViews DESC LIMIT 10;
    add(qs,
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' "
            + "AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' "
            + "GROUP BY URL ORDER BY PageViews DESC LIMIT 10;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.DontCountHits = 0 and $h.IsRefresh = 0 and $h.URL != ""
              let $k := $h.URL
              group by $k
              let $c := count($h)
              order by $c descending
              return {"URL": $k, "PageViews": $c}, 1, 10)""".formatted(JULY_2013));

    // Q37: same as Q36 but on Title.
    add(qs,
        "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' "
            + "AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' "
            + "GROUP BY Title ORDER BY PageViews DESC LIMIT 10;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.DontCountHits = 0 and $h.IsRefresh = 0 and $h.Title != ""
              let $k := $h.Title
              group by $k
              let $c := count($h)
              order by $c descending
              return {"Title": $k, "PageViews": $c}, 1, 10)""".formatted(JULY_2013));

    // Q38: ... AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL
    // ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;
    add(qs,
        "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' "
            + "AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 "
            + "GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.IsRefresh = 0 and $h.IsLink != 0 and $h.IsDownload = 0
              let $k := $h.URL
              group by $k
              let $c := count($h)
              order by $c descending
              return {"URL": $k, "PageViews": $c}, 1001, 10)""".formatted(JULY_2013));

    // Q39: SELECT TraficSourceID, SearchEngineID, AdvEngineID,
    // CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src,
    // URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE ... GROUP BY ... LIMIT 10 OFFSET 1000;
    add(qs,
        "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) "
            + "THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 "
            + "AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 "
            + "GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst "
            + "ORDER BY PageViews DESC LIMIT 10 OFFSET 1000;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.IsRefresh = 0
              let $t := $h.TraficSourceID, $e := $h.SearchEngineID, $a := $h.AdvEngineID,
                  $src := (if ($h.SearchEngineID = 0 and $h.AdvEngineID = 0) then $h.Referer else ""),
                  $dst := $h.URL
              group by $t, $e, $a, $src, $dst
              let $c := count($h)
              order by $c descending
              return {"TraficSourceID": $t, "SearchEngineID": $e, "AdvEngineID": $a, "Src": $src, "Dst": $dst,
                      "PageViews": $c}, 1001, 10)""".formatted(JULY_2013));

    // Q40: SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE ... AND TraficSourceID IN
    // (-1, 6)
    // AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate
    // ORDER BY PageViews DESC LIMIT 10 OFFSET 100;
    add(qs,
        "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 "
            + "AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 "
            + "AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 "
            + "GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.IsRefresh = 0 and $h.TraficSourceID = (-1, 6)
                    and $h.RefererHash = 3594120000172545465
              let $u := $h.URLHash, $d := $h.EventDate
              group by $u, $d
              let $c := count($h)
              order by $c descending
              return {"URLHash": $u, "EventDate": $d, "PageViews": $c}, 101, 10)""".formatted(JULY_2013));

    // Q41: SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE ...
    // AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight
    // ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;
    add(qs,
        "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 "
            + "AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 "
            + "AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight "
            + "ORDER BY PageViews DESC LIMIT 10 OFFSET 10000;",
        """
            subsequence(
              for $h in $hits[]
              where %s and $h.IsRefresh = 0 and $h.DontCountHits = 0 and $h.URLHash = 2868770270353813622
              let $w := $h.WindowClientWidth, $ht := $h.WindowClientHeight
              group by $w, $ht
              let $c := count($h)
              order by $c descending
              return {"WindowClientWidth": $w, "WindowClientHeight": $ht, "PageViews": $c}, 10001, 10)""".formatted(
            JULY_2013));

    // Q42: SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID
    // = 62
    // AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits =
    // 0
    // GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET
    // 1000;
    add(qs,
        "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 "
            + "AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 "
            + "GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000;",
        """
            subsequence(
              for $h in $hits[]
              where $h.CounterID = 62 and $h.EventDate >= "2013-07-14" and $h.EventDate <= "2013-07-15"
                    and $h.IsRefresh = 0 and $h.DontCountHits = 0
              let $m := substring($h.EventTime, 1, 16)
              group by $m
              let $c := count($h)
              order by $m
              return {"M": concat($m, ":00"), "PageViews": $c}, 1001, 10)""");

    if (qs.size() != 43) {
      throw new IllegalStateException("expected 43 ClickBench queries, built " + qs.size());
    }
    return Collections.unmodifiableList(qs);
  }

  private static void add(final List<Query> qs, final String sql, final String... variants) {
    qs.add(new Query(qs.size(), sql, List.of(variants)));
  }

  /** Q29's 90 shifted sums, as SQL, for the record. */
  private static String resolutionWidthSumSql() {
    final StringBuilder sb = new StringBuilder(4096).append("SELECT SUM(ResolutionWidth)");
    for (int i = 1; i <= 89; i++) {
      sb.append(", SUM(ResolutionWidth + ").append(i).append(')');
    }
    return sb.append(" FROM hits;").toString();
  }

  /**
   * Q29's second formulation: 90 independent aggregates. Each one is a shape the aggregate fast paths
   * recognise, but the cost is 90 passes over the column — measured 74 s against the single-pass
   * form's 9 s at 1 M records, which is why that one is the default.
   */
  private static String resolutionWidthSumsIndependent() {
    final StringBuilder sb = new StringBuilder(8192).append('{');
    for (int i = 0; i <= 89; i++) {
      if (i > 0) {
        sb.append(",\n ");
      }
      sb.append('"').append('s').append(i).append("\": sum(for $h in $hits[] return $h.ResolutionWidth");
      if (i > 0) {
        sb.append(" + ").append(i);
      }
      sb.append(')');
    }
    return sb.append('}').toString();
  }

  /**
   * Q29's default formulation: a single pass. The 90 shifted values are computed per record in
   * {@code let} clauses before {@code group by}, which rebinds each of them to the sequence of that
   * group's values — so the 90 sums fold 90 already-materialised sequences instead of rescanning the
   * column 90 times. Same answer (the smoke test requires every variant to agree), 8x faster.
   */
  private static String resolutionWidthSumsOnePass() {
    final StringBuilder sb = new StringBuilder(8192).append("for $h in $hits[]\nlet $g := 1");
    for (int i = 0; i <= 89; i++) {
      sb.append(", $w").append(i).append(" := $h.ResolutionWidth");
      if (i > 0) {
        sb.append(" + ").append(i);
      }
    }
    sb.append("\ngroup by $g\nreturn {");
    for (int i = 0; i <= 89; i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('"').append('s').append(i).append("\": sum($w").append(i).append(')');
    }
    return sb.append('}').toString();
  }
}
