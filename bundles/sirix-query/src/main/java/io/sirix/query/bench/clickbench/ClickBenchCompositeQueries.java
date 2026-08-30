package io.sirix.query.bench.clickbench;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Partition-decomposed ("composite") formulations of the 43 ClickBench queries for a corpus loaded
 * as {@code N} partitioned resources ({@code hits-0 … hits-(N-1)}) instead of one resource.
 *
 * <p>
 * Every partition leg is emitted as a literal {@code let $hits := jn:doc('db','hits-i') return
 * (…)} sub-expression — the shape the analytical fast paths detect — so each leg resolves its own
 * vectorized executor through the per-source hook and is served from that partition's projection. A
 * single {@code let $hits := (docA, docB, …)} binding would compile and answer correctly while
 * silently dropping every query onto the generic pipeline; gate any change here on the
 * {@code # served:} counters, never on timings.
 *
 * <h2>Decomposition algebra</h2>
 * <ul>
 * <li>{@code COUNT → sum} of per-partition counts, {@code SUM → sum} of sums, {@code MIN/MAX} of
 * per-partition minima/maxima.</li>
 * <li>{@code AVG} decomposes to {@code (sum, count)} partials combined as
 * {@code xs:double(Σsum div Σcount)} — averaging averages would weight partitions, not rows. The
 * denominator is the group's row count, which equals the value count because ClickBench columns are
 * dense (NOT NULL); a nullable column would need its own value-count partial.</li>
 * <li>{@code COUNT(DISTINCT x)} is NOT decomposable over counts — each partition ships its
 * per-group DISTINCT VALUE LIST (as an array field) and the merge counts distinct over the union of
 * the lists.</li>
 * <li>{@code HAVING} and {@code OFFSET/LIMIT} apply only at the merge: a partition-local filter or
 * cut would discard groups/rows that survive globally.</li>
 * <li>Raw-row top-k (no grouping) merges per-partition top-k lists: top-k of a union equals top-k
 * of the concatenated per-partition top-ks, with sort keys riding along when the output column
 * differs from the sort column.</li>
 * </ul>
 *
 * <p>
 * For each query this class provides the PRODUCTION text (original order/limit semantics) and a
 * FULL-LIST text (no order, no limit, HAVING kept) whose result is compared order-insensitively
 * against the single-resource answer by {@code ClickBenchCompositeDifferentialMain} — comparing
 * complete group lists validates the merge algebra for every group and every aggregate while
 * sidestepping top-k boundary-tie nondeterminism. For spec-generated queries it also provides the
 * spec's single-resource full-list text, which the differential pins against the ORIGINAL query
 * text so a spec that mis-models a query cannot pass by both arms sharing the error.
 */
public final class ClickBenchCompositeQueries {

  /**
   * One query's composite texts. {@code singleFull} is {@code null} where the composite legs embed
   * the original body verbatim (scalars, row-top-k) and the spec-fidelity arm would be identical to
   * the original by construction.
   */
  public record CompositeQuery(int index, String compositeFull, String compositeProduction, String singleFull) {
  }

  private ClickBenchCompositeQueries() {}

  /** All 43 queries decomposed over {@code partitions} resources {@code prefix-0 … prefix-(n-1)}. */
  public static List<CompositeQuery> all(final String database, final String prefix, final int partitions,
      final String singleDatabase, final String singleResource) {
    if (partitions < 1) {
      throw new IllegalArgumentException("partitions must be >= 1, got " + partitions);
    }
    final List<CompositeQuery> out = new ArrayList<>(43);
    for (int q = 0; q < 43; q++) {
      out.add(build(q, database, prefix, partitions, singleDatabase, singleResource));
    }
    return out;
  }

  // ==== per-query construction ==================================================================

  private static CompositeQuery build(final int q, final String db, final String prefix, final int n, final String sdb,
      final String sres) {
    return switch (q) {
      // -- scalar aggregates ---------------------------------------------------------------------
      case 0 -> scalarSum(q, db, prefix, n, "count($hits[])");
      case 1 -> scalarSum(q, db, prefix, n, "count(for $h in $hits[] where $h.AdvEngineID != 0 return $h)");
      case 2 -> {
        final String leg = """
            {"s": sum(for $h in $hits[] return $h.AdvEngineID),
             "c": count($hits[]),
             "r": sum(for $h in $hits[] return $h.ResolutionWidth)}""";
        final String outer = "let $ps := (\n" + legs(db, prefix, n, leg, ",\n") + "\n)\n"
            + "return {\"sum_AdvEngineID\": sum($ps.s), \"count\": sum($ps.c), "
            + "\"avg_ResolutionWidth\": xs:double(sum($ps.r) div sum($ps.c))}";
        yield new CompositeQuery(q, outer, outer, null);
      }
      case 3 -> {
        final String leg = "{\"s\": sum(for $h in $hits[] return $h.UserID), \"c\": count($hits[])}";
        final String outer = "let $ps := (\n" + legs(db, prefix, n, leg, ",\n") + "\n)\n"
            + "return xs:double(sum($ps.s) div sum($ps.c))";
        yield new CompositeQuery(q, outer, outer, null);
      }
      case 4 -> distinctCount(q, db, prefix, n, "$h.UserID");
      case 5 -> distinctCount(q, db, prefix, n, "$h.SearchPhrase");
      case 6 -> {
        final String leg = """
            {"mn": min(for $h in $hits[] return $h.EventDate),
             "mx": max(for $h in $hits[] return $h.EventDate)}""";
        final String outer = "let $ps := (\n" + legs(db, prefix, n, leg, ",\n") + "\n)\n"
            + "return {\"min_EventDate\": min($ps.mn), \"max_EventDate\": max($ps.mx)}";
        yield new CompositeQuery(q, outer, outer, null);
      }
      case 19 -> {
        final String leg = "for $h in $hits[] where $h.UserID = 435090932899640449 return $h.UserID";
        final String outer = "(\n" + legs(db, prefix, n, leg, ",\n") + "\n)";
        yield new CompositeQuery(q, outer, outer, null);
      }
      case 20 -> scalarSum(q, db, prefix, n, "count(for $h in $hits[] where contains($h.URL, \"google\") return $h)");
      case 29 -> ninetySums(q, db, prefix, n);

      // -- raw-row top-k -------------------------------------------------------------------------
      case 23 -> {
        final String prodLeg = "subsequence(for $h in $hits[] where contains($h.URL, \"google\") "
            + "order by $h.EventTime return $h, 1, 10)";
        final String prod = "subsequence(for $r in (\n" + legs(db, prefix, n, prodLeg, ",\n")
            + "\n) order by $r.EventTime return $r, 1, 10)";
        final String full = "(\n"
            + legs(db, prefix, n, "for $h in $hits[] where contains($h.URL, \"google\") return $h", ",\n") + "\n)";
        yield new CompositeQuery(q, full, prod, null);
      }
      case 24 -> topKPhrase(q, db, prefix, n, "order by $h.EventTime", "{\"t\": $h.EventTime, \"v\": $h.SearchPhrase}",
          "order by $r.t return $r.v");
      case 25 -> topKPhrase(q, db, prefix, n, "order by $h.SearchPhrase", "$h.SearchPhrase", "order by $r return $r");
      case 26 -> topKPhrase(q, db, prefix, n, "order by $h.EventTime, $h.SearchPhrase",
          "{\"t\": $h.EventTime, \"s\": $h.SearchPhrase}", "order by $r.t, $r.s return $r.s");

      // -- grouped aggregations (spec-generated) -------------------------------------------------
      default -> grouped(spec(q), db, prefix, n, sdb, sres);
    };
  }

  private static CompositeQuery scalarSum(final int q, final String db, final String prefix, final int n,
      final String legBody) {
    final String text = legs(db, prefix, n, legBody, "\n+ ");
    return new CompositeQuery(q, text, text, null);
  }

  private static CompositeQuery distinctCount(final int q, final String db, final String prefix, final int n,
      final String src) {
    final String leg = "distinct-values(for $h in $hits[] return " + src + ")";
    final String text = "count(distinct-values((\n" + legs(db, prefix, n, leg, ",\n") + "\n)))";
    return new CompositeQuery(q, text, text, null);
  }

  private static CompositeQuery topKPhrase(final int q, final String db, final String prefix, final int n,
      final String innerOrder, final String innerReturn, final String outerOrderReturn) {
    final String prodLeg = "subsequence(for $h in $hits[] where $h.SearchPhrase != \"\" " + innerOrder + " return "
        + innerReturn + ", 1, 10)";
    final String prod =
        "subsequence(for $r in (\n" + legs(db, prefix, n, prodLeg, ",\n") + "\n) " + outerOrderReturn + ", 1, 10)";
    final String full = "(\n"
        + legs(db, prefix, n, "for $h in $hits[] where $h.SearchPhrase != \"\" return $h.SearchPhrase", ",\n") + "\n)";
    return new CompositeQuery(q, full, prod, null);
  }

  private static CompositeQuery ninetySums(final int q, final String db, final String prefix, final int n) {
    final StringBuilder leg = new StringBuilder(8192).append("for $h in $hits[]\nlet $g := 1");
    for (int i = 0; i <= 89; i++) {
      leg.append(", $w").append(i).append(" := $h.ResolutionWidth");
      if (i > 0) {
        leg.append(" + ").append(i);
      }
    }
    leg.append("\ngroup by $g\nreturn {");
    for (int i = 0; i <= 89; i++) {
      if (i > 0) {
        leg.append(", ");
      }
      leg.append("\"s").append(i).append("\": sum($w").append(i).append(')');
    }
    leg.append('}');
    final StringBuilder outer = new StringBuilder(16384).append("let $ps := (\n")
                                                        .append(legs(db, prefix, n, leg.toString(), ",\n"))
                                                        .append("\n)\nreturn {");
    for (int i = 0; i <= 89; i++) {
      if (i > 0) {
        outer.append(", ");
      }
      outer.append("\"s").append(i).append("\": sum($ps.s").append(i).append(')');
    }
    final String text = outer.append('}').toString();
    return new CompositeQuery(q, text, text, null);
  }

  // ==== grouped-aggregation spec model ==========================================================

  private enum Kind {
    COUNT, SUM, AVG, MIN, COUNT_DISTINCT
  }

  /** One output aggregate: its result-object field name, kind, and grouped source expression. */
  private record Agg(String out, Kind kind, String src) {
  }

  /** One group key: result-object field name, per-record expression, optional return expression. */
  private record Key(String out, String expr, String retExpr) {
    Key(final String out, final String expr) {
      this(out, expr, null);
    }
  }

  /**
   * A grouped ClickBench query in decomposable form. {@code lets} are pre-group computed bindings
   * (referenced by aggregate sources), {@code havingCountAbove} filters on the COUNT aggregate at the
   * merge, {@code orderRef} names the output field ordered on ({@code null} = unordered),
   * {@code offset/limit} are the subsequence bounds ({@code -1} = none).
   */
  private record GroupSpec(int index, String where, String lets, List<Key> keys, List<Agg> aggs, long havingCountAbove,
      String orderRef, boolean orderDesc, int offset, int limit, List<String> returnOrder) {
  }

  private static final String JULY_2013 =
      "$h.CounterID = 62 and $h.EventDate >= \"2013-07-01\" and $h.EventDate <= \"2013-07-31\"";

  private static GroupSpec spec(final int q) {
    final GroupSpec throughQ18 = specThroughQ18(q);
    if (throughQ18 != null) {
      return throughQ18;
    }
    final GroupSpec throughQ33 = specThroughQ33(q);
    return throughQ33 != null
        ? throughQ33
        : specFromQ34(q);
  }

  /** Spec table for queries up to 18; {@code null} when {@code q} belongs to a later block. */
  private static @Nullable GroupSpec specThroughQ18(final int q) {
    return switch (q) {
      case 7 -> new GroupSpec(q, "$h.AdvEngineID != 0", null, List.of(new Key("AdvEngineID", "$h.AdvEngineID")),
          List.of(new Agg("count", Kind.COUNT, null)), -1, "count", true, -1, -1, List.of("AdvEngineID", "count"));
      case 8 -> new GroupSpec(q, null, null, List.of(new Key("RegionID", "$h.RegionID")),
          List.of(new Agg("u", Kind.COUNT_DISTINCT, "$h.UserID")), -1, "u", true, 1, 10, List.of("RegionID", "u"));
      case 9 -> new GroupSpec(q, null, null, List.of(new Key("RegionID", "$h.RegionID")),
          List.of(new Agg("sum_AdvEngineID", Kind.SUM, "$h.AdvEngineID"), new Agg("c", Kind.COUNT, null),
              new Agg("avg_ResolutionWidth", Kind.AVG, "$h.ResolutionWidth"),
              new Agg("uniq_UserID", Kind.COUNT_DISTINCT, "$h.UserID")),
          -1, "c", true, 1, 10, List.of("RegionID", "sum_AdvEngineID", "c", "avg_ResolutionWidth", "uniq_UserID"));
      case 10 -> new GroupSpec(q, "$h.MobilePhoneModel != \"\"", null,
          List.of(new Key("MobilePhoneModel", "$h.MobilePhoneModel")),
          List.of(new Agg("u", Kind.COUNT_DISTINCT, "$h.UserID")), -1, "u", true, 1, 10,
          List.of("MobilePhoneModel", "u"));
      case 11 -> new GroupSpec(q, "$h.MobilePhoneModel != \"\"", null,
          List.of(new Key("MobilePhone", "$h.MobilePhone"), new Key("MobilePhoneModel", "$h.MobilePhoneModel")),
          List.of(new Agg("u", Kind.COUNT_DISTINCT, "$h.UserID")), -1, "u", true, 1, 10,
          List.of("MobilePhone", "MobilePhoneModel", "u"));
      case 12 -> new GroupSpec(q, "$h.SearchPhrase != \"\"", null, List.of(new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10, List.of("SearchPhrase", "c"));
      case 13 -> new GroupSpec(q, "$h.SearchPhrase != \"\"", null, List.of(new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("u", Kind.COUNT_DISTINCT, "$h.UserID")), -1, "u", true, 1, 10, List.of("SearchPhrase", "u"));
      case 14 -> new GroupSpec(q, "$h.SearchPhrase != \"\"", null,
          List.of(new Key("SearchEngineID", "$h.SearchEngineID"), new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10,
          List.of("SearchEngineID", "SearchPhrase", "c"));
      case 15 -> new GroupSpec(q, null, null, List.of(new Key("UserID", "$h.UserID")),
          List.of(new Agg("count", Kind.COUNT, null)), -1, "count", true, 1, 10, List.of("UserID", "count"));
      case 16 -> new GroupSpec(q, null, null,
          List.of(new Key("UserID", "$h.UserID"), new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("count", Kind.COUNT, null)), -1, "count", true, 1, 10,
          List.of("UserID", "SearchPhrase", "count"));
      case 17 -> new GroupSpec(q, null, null,
          List.of(new Key("UserID", "$h.UserID"), new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("count", Kind.COUNT, null)), -1, null, false, 1, 10,
          List.of("UserID", "SearchPhrase", "count"));
      case 18 -> new GroupSpec(q, null, null,
          List.of(new Key("UserID", "$h.UserID"), new Key("m", "xs:integer(substring($h.EventTime, 15, 2))"),
              new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("count", Kind.COUNT, null)), -1, "count", true, 1, 10,
          List.of("UserID", "m", "SearchPhrase", "count"));
      default -> null;
    };
  }

  /** Spec table for queries 21 through 33; {@code null} when {@code q} belongs to a later block. */
  private static @Nullable GroupSpec specThroughQ33(final int q) {
    return switch (q) {
      case 21 -> new GroupSpec(q, "contains($h.URL, \"google\") and $h.SearchPhrase != \"\"", null,
          List.of(new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("min_URL", Kind.MIN, "$h.URL"), new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10,
          List.of("SearchPhrase", "min_URL", "c"));
      case 22 -> new GroupSpec(q,
          "contains($h.Title, \"Google\") and not(contains($h.URL, \".google.\")) and $h.SearchPhrase != \"\"", null,
          List.of(new Key("SearchPhrase", "$h.SearchPhrase")),
          List.of(new Agg("min_URL", Kind.MIN, "$h.URL"), new Agg("min_Title", Kind.MIN, "$h.Title"),
              new Agg("c", Kind.COUNT, null), new Agg("uniq_UserID", Kind.COUNT_DISTINCT, "$h.UserID")),
          -1, "c", true, 1, 10, List.of("SearchPhrase", "min_URL", "min_Title", "c", "uniq_UserID"));
      case 27 -> new GroupSpec(q, "$h.URL != \"\"", "$len := jn:utf8-length($h.URL)",
          List.of(new Key("CounterID", "$h.CounterID")),
          List.of(new Agg("l", Kind.AVG, "$len"), new Agg("c", Kind.COUNT, null)), 100000, "l", true, 1, 25,
          List.of("CounterID", "l", "c"));
      case 28 -> new GroupSpec(q, "$h.Referer != \"\"", "$len := jn:utf8-length($h.Referer)",
          List.of(new Key("k", "replace($h.Referer, '^https?://(www\\.)?([^/]+)/.*$', '$2')")),
          List.of(new Agg("l", Kind.AVG, "$len"), new Agg("c", Kind.COUNT, null),
              new Agg("min_Referer", Kind.MIN, "$h.Referer")),
          100000, "l", true, 1, 25, List.of("k", "l", "c", "min_Referer"));
      case 30 -> new GroupSpec(q, "$h.SearchPhrase != \"\"", null,
          List.of(new Key("SearchEngineID", "$h.SearchEngineID"), new Key("ClientIP", "$h.ClientIP")),
          List.of(new Agg("c", Kind.COUNT, null), new Agg("sum_IsRefresh", Kind.SUM, "$h.IsRefresh"),
              new Agg("avg_ResolutionWidth", Kind.AVG, "$h.ResolutionWidth")),
          -1, "c", true, 1, 10, List.of("SearchEngineID", "ClientIP", "c", "sum_IsRefresh", "avg_ResolutionWidth"));
      case 31 -> new GroupSpec(q, "$h.SearchPhrase != \"\"", null,
          List.of(new Key("WatchID", "$h.WatchID"), new Key("ClientIP", "$h.ClientIP")),
          List.of(new Agg("c", Kind.COUNT, null), new Agg("sum_IsRefresh", Kind.SUM, "$h.IsRefresh"),
              new Agg("avg_ResolutionWidth", Kind.AVG, "$h.ResolutionWidth")),
          -1, "c", true, 1, 10, List.of("WatchID", "ClientIP", "c", "sum_IsRefresh", "avg_ResolutionWidth"));
      case 32 ->
        new GroupSpec(q, null, null, List.of(new Key("WatchID", "$h.WatchID"), new Key("ClientIP", "$h.ClientIP")),
            List.of(new Agg("c", Kind.COUNT, null), new Agg("sum_IsRefresh", Kind.SUM, "$h.IsRefresh"),
                new Agg("avg_ResolutionWidth", Kind.AVG, "$h.ResolutionWidth")),
            -1, "c", true, 1, 10, List.of("WatchID", "ClientIP", "c", "sum_IsRefresh", "avg_ResolutionWidth"));
      case 33 -> new GroupSpec(q, null, null, List.of(new Key("URL", "$h.URL")),
          List.of(new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10, List.of("URL", "c"));
      default -> null;
    };
  }

  /** Spec table for queries 34 and above; rejects any query that is not spec-shaped. */
  private static GroupSpec specFromQ34(final int q) {
    return switch (q) {
      case 34 -> new GroupSpec(q, null, null, List.of(new Key("one", "1"), new Key("URL", "$h.URL")),
          List.of(new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10, List.of("one", "URL", "c"));
      case 35 -> new GroupSpec(q, null, null,
          List.of(new Key("ClientIP", "$h.ClientIP"), new Key("m1", "$h.ClientIP - 1"),
              new Key("m2", "$h.ClientIP - 2"), new Key("m3", "$h.ClientIP - 3")),
          List.of(new Agg("c", Kind.COUNT, null)), -1, "c", true, 1, 10, List.of("ClientIP", "m1", "m2", "m3", "c"));
      case 36 -> new GroupSpec(q, JULY_2013 + " and $h.DontCountHits = 0 and $h.IsRefresh = 0 and $h.URL != \"\"", null,
          List.of(new Key("URL", "$h.URL")), List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews", true, 1,
          10, List.of("URL", "PageViews"));
      case 37 -> new GroupSpec(q, JULY_2013 + " and $h.DontCountHits = 0 and $h.IsRefresh = 0 and $h.Title != \"\"",
          null, List.of(new Key("Title", "$h.Title")), List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews",
          true, 1, 10, List.of("Title", "PageViews"));
      case 38 -> new GroupSpec(q, JULY_2013 + " and $h.IsRefresh = 0 and $h.IsLink != 0 and $h.IsDownload = 0", null,
          List.of(new Key("URL", "$h.URL")), List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews", true,
          1001, 10, List.of("URL", "PageViews"));
      case 39 -> new GroupSpec(q, JULY_2013 + " and $h.IsRefresh = 0", null,
          List.of(new Key("TraficSourceID", "$h.TraficSourceID"), new Key("SearchEngineID", "$h.SearchEngineID"),
              new Key("AdvEngineID", "$h.AdvEngineID"),
              new Key("Src", "(if ($h.SearchEngineID = 0 and $h.AdvEngineID = 0) then $h.Referer else \"\")"),
              new Key("Dst", "$h.URL")),
          List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews", true, 1001, 10,
          List.of("TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst", "PageViews"));
      case 40 -> new GroupSpec(q,
          JULY_2013 + " and $h.IsRefresh = 0 and $h.TraficSourceID = (-1, 6) and $h.RefererHash = 3594120000172545465",
          null, List.of(new Key("URLHash", "$h.URLHash"), new Key("EventDate", "$h.EventDate")),
          List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews", true, 101, 10,
          List.of("URLHash", "EventDate", "PageViews"));
      case 41 -> new GroupSpec(q,
          JULY_2013 + " and $h.IsRefresh = 0 and $h.DontCountHits = 0 and $h.URLHash = 2868770270353813622", null,
          List.of(new Key("WindowClientWidth", "$h.WindowClientWidth"),
              new Key("WindowClientHeight", "$h.WindowClientHeight")),
          List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "PageViews", true, 10001, 10,
          List.of("WindowClientWidth", "WindowClientHeight", "PageViews"));
      case 42 -> new GroupSpec(q,
          "$h.CounterID = 62 and $h.EventDate >= \"2013-07-14\" and $h.EventDate <= \"2013-07-15\""
              + " and $h.IsRefresh = 0 and $h.DontCountHits = 0",
          null, List.of(new Key("M", "substring($h.EventTime, 1, 16)", "concat($g0, \":00\")")),
          List.of(new Agg("PageViews", Kind.COUNT, null)), -1, "M", false, 1001, 10, List.of("M", "PageViews"));
      default -> throw new IllegalArgumentException("query " + q + " is not spec-shaped");
    };
  }

  // ==== grouped-aggregation text generation =====================================================

  private static CompositeQuery grouped(final GroupSpec spec, final String db, final String prefix, final int n,
      final String sdb, final String sres) {
    final String inner = innerPartial(spec);
    final String outerCore = outerCore(spec, legs(db, prefix, n, inner, ",\n"));
    final String full = outerCore;
    final String production = productionWrap(spec, outerCore);
    final String singleFull =
        "let $hits := jn:doc('" + sdb + "','" + sres + "')\nreturn (\n" + singleFullBody(spec) + "\n)";
    return new CompositeQuery(spec.index(), full, production, singleFull);
  }

  /** The per-partition leg: same scan/group as the original, partial aggregates, no order/limit. */
  private static String innerPartial(final GroupSpec spec) {
    final StringBuilder sb = new StringBuilder(512).append("for $h in $hits[]\n");
    if (spec.where() != null) {
      sb.append("where ").append(spec.where()).append('\n');
    }
    sb.append("let ");
    if (spec.lets() != null) {
      sb.append(spec.lets()).append(", ");
    }
    appendKeyBindings(sb, spec);
    sb.append("\ngroup by ");
    appendKeyReferences(sb, spec);
    sb.append("\nreturn {");
    for (int i = 0; i < spec.keys().size(); i++) {
      sb.append("\"g").append(i).append("\": $g").append(i).append(", ");
    }
    appendPartialAggregates(sb, spec);
    return sb.append('}').toString();
  }

  /** {@code $g0 := <expr>, $g1 := <expr>, …} — the per-key let bindings. */
  private static void appendKeyBindings(final StringBuilder sb, final GroupSpec spec) {
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i).append(" := ").append(spec.keys().get(i).expr());
    }
  }

  /** {@code $g0, $g1, …} — the grouping references. */
  private static void appendKeyReferences(final StringBuilder sb, final GroupSpec spec) {
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i);
    }
  }

  /** The {@code "pN": <partial>} aggregate fields, plus the AVG denominator when one is needed. */
  private static void appendPartialAggregates(final StringBuilder sb, final GroupSpec spec) {
    final List<Agg> aggs = spec.aggs();
    for (int i = 0; i < aggs.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      final Agg agg = aggs.get(i);
      sb.append("\"p").append(i).append("\": ").append(switch (agg.kind()) {
        case COUNT -> "count($h)";
        case SUM, AVG -> "sum(" + agg.src() + ")";
        case MIN -> "min(" + agg.src() + ")";
        case COUNT_DISTINCT -> "[distinct-values(" + agg.src() + ")]";
      });
    }
    // AVG needs a row-count denominator at the merge; emit one even when the query has no COUNT.
    if (countAggIndex(spec) < 0 && aggs.stream().anyMatch(a -> a.kind() == Kind.AVG)) {
      sb.append(", \"pc\": count($h)");
    }
  }

  /** The merge: regroup the concatenated partials, combine, HAVING — no order, no subsequence. */
  private static String outerCore(final GroupSpec spec, final String legsText) {
    final StringBuilder sb = new StringBuilder(1024).append("for $p in (\n").append(legsText).append("\n)\nlet ");
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i).append(" := $p.g").append(i);
    }
    sb.append("\ngroup by ");
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i);
    }
    final int countIdx = countAggIndex(spec);
    final String countRef = countIdx >= 0
        ? "$f" + countIdx
        : "sum($p.pc)";
    sb.append('\n');
    // COUNT first so AVG combiners can reference it.
    if (countIdx >= 0) {
      sb.append("let $f").append(countIdx).append(" := sum($p.p").append(countIdx).append(")\n");
    }
    final List<Agg> aggs = spec.aggs();
    for (int i = 0; i < aggs.size(); i++) {
      if (i == countIdx) {
        continue;
      }
      final Agg agg = aggs.get(i);
      sb.append("let $f").append(i).append(" := ").append(switch (agg.kind()) {
        case COUNT -> throw new IllegalStateException("second COUNT aggregate in query " + spec.index());
        case SUM -> "sum($p.p" + i + ")";
        case AVG -> "xs:double(sum($p.p" + i + ") div " + countRef + ")";
        case MIN -> "min($p.p" + i + ")";
        case COUNT_DISTINCT -> "count(distinct-values(for $q in $p.p" + i + " return $q[]))";
      }).append('\n');
    }
    if (spec.havingCountAbove() >= 0) {
      sb.append("where ").append(countRef).append(" > ").append(spec.havingCountAbove()).append('\n');
    }
    return sb.append(returnClause(spec)).toString();
  }

  /** Adds the original order-by and subsequence on top of the merge core. */
  private static String productionWrap(final GroupSpec spec, final String core) {
    String text = core;
    if (spec.orderRef() != null) {
      final String orderVar = refVar(spec, spec.orderRef());
      final int returnAt = text.lastIndexOf("return {");
      text = text.substring(0, returnAt) + "order by " + orderVar + (spec.orderDesc()
          ? " descending"
          : "") + '\n' + text.substring(returnAt);
    }
    if (spec.offset() >= 0) {
      text = "subsequence(\n" + text + ", " + spec.offset() + ", " + spec.limit() + ')';
    }
    return text;
  }

  /** The spec's single-resource formulation with final aggregates — full list, HAVING kept. */
  private static String singleFullBody(final GroupSpec spec) {
    final StringBuilder sb = new StringBuilder(512).append("for $h in $hits[]\n");
    if (spec.where() != null) {
      sb.append("where ").append(spec.where()).append('\n');
    }
    sb.append("let ");
    if (spec.lets() != null) {
      sb.append(spec.lets()).append(", ");
    }
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i).append(" := ").append(spec.keys().get(i).expr());
    }
    sb.append("\ngroup by ");
    for (int i = 0; i < spec.keys().size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("$g").append(i);
    }
    sb.append('\n');
    final List<Agg> aggs = spec.aggs();
    for (int i = 0; i < aggs.size(); i++) {
      final Agg agg = aggs.get(i);
      sb.append("let $f").append(i).append(" := ").append(switch (agg.kind()) {
        case COUNT -> "count($h)";
        case SUM -> "sum(" + agg.src() + ")";
        case AVG -> "xs:double(avg(" + agg.src() + "))";
        case MIN -> "min(" + agg.src() + ")";
        case COUNT_DISTINCT -> "count(distinct-values(" + agg.src() + "))";
      }).append('\n');
    }
    if (spec.havingCountAbove() >= 0) {
      sb.append("where $f").append(countAggIndex(spec)).append(" > ").append(spec.havingCountAbove()).append('\n');
    }
    return sb.append(returnClause(spec)).toString();
  }

  private static String returnClause(final GroupSpec spec) {
    final StringBuilder sb = new StringBuilder(128).append("return {");
    boolean first = true;
    for (final String out : spec.returnOrder()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append('"').append(out).append("\": ").append(refExpr(spec, out));
    }
    return sb.append('}').toString();
  }

  /** The value expression for an output field: a key's return expression/var or an aggregate var. */
  private static String refExpr(final GroupSpec spec, final String out) {
    for (int i = 0; i < spec.keys().size(); i++) {
      final Key key = spec.keys().get(i);
      if (key.out().equals(out)) {
        return key.retExpr() != null
            ? key.retExpr()
            : "$g" + i;
      }
    }
    return refVar(spec, out);
  }

  /** The variable an output field is bound to (order-by needs the var, not a return expression). */
  private static String refVar(final GroupSpec spec, final String out) {
    for (int i = 0; i < spec.keys().size(); i++) {
      if (spec.keys().get(i).out().equals(out)) {
        return "$g" + i;
      }
    }
    for (int i = 0; i < spec.aggs().size(); i++) {
      if (spec.aggs().get(i).out().equals(out)) {
        return "$f" + i;
      }
    }
    throw new IllegalArgumentException("query " + spec.index() + " references unknown output '" + out + "'");
  }

  private static int countAggIndex(final GroupSpec spec) {
    for (int i = 0; i < spec.aggs().size(); i++) {
      if (spec.aggs().get(i).kind() == Kind.COUNT) {
        return i;
      }
    }
    return -1;
  }

  /** Partition legs, each a literal {@code jn:doc} let-binding (the fast-path shape), joined. */
  private static String legs(final String database, final String prefix, final int partitions, final String innerBody,
      final String join) {
    final StringBuilder sb = new StringBuilder(partitions * (innerBody.length() + 64));
    for (int i = 0; i < partitions; i++) {
      if (i > 0) {
        sb.append(join);
      }
      sb.append("(let $hits := jn:doc('")
        .append(database)
        .append("','")
        .append(prefix)
        .append('-')
        .append(i)
        .append("')\nreturn (")
        .append(innerBody)
        .append("))");
    }
    return sb.toString();
  }
}
