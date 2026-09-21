package io.sirix.query.bench.bitemporal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** The twelve normative SH1 JSONiq queries and their canonical result schemas. */
public final class BitemporalQueries {

  private static final String PROLOG = """
      declare variable $A := xs:dateTime('2024-06-29T00:00:00Z');
      declare variable $B := xs:dateTime('2024-12-26T00:00:00Z');
      declare variable $D := xs:dateTime('2024-09-27T00:00:00Z');
      declare variable $V := xs:dateTime('2024-06-15T00:00:00Z');
      declare variable $L := xs:dateTime('2024-05-30T00:00:00Z');
      declare variable $U := xs:dateTime('2024-07-29T00:00:00Z');
      declare function local:slice($resource as xs:string,
                                   $system as xs:dateTime,
                                   $valid as xs:dateTime) {
        for $r in jn:open-bitemporal('bt', $resource, $system, $valid)
        where $valid lt xs:dateTime($r.vt)
        return $r
      };
      """;

  /** One query, including the schema used to reject accidental representation repairs. */
  public record Query(int index, String name, List<String> columns, int keyColumns, String body, String route,
      boolean strictEndResidual) {
    public Query {
      if (index < 1 || index > 12 || keyColumns < 0 || keyColumns > columns.size()) {
        throw new IllegalArgumentException("invalid query metadata for Q" + index);
      }
      Objects.requireNonNull(name, "name");
      Objects.requireNonNull(columns, "columns");
      Objects.requireNonNull(body, "body");
      Objects.requireNonNull(route, "route");
      columns = List.copyOf(columns);
    }

    public String text() {
      return PROLOG + body;
    }
  }

  private static final List<Query> QUERIES = build();

  private BitemporalQueries() {
    throw new AssertionError("no instances");
  }

  public static List<Query> all() {
    return QUERIES;
  }

  private static List<Query> build() {
    final List<Query> queries = new ArrayList<>(12);
    add(queries, 1, "present belief", 1, "revision-scan+half-open-predicate", false,
        List.of("id", "cost", "qty", "grade"), """
            for $c in jn:open('bt','contracts',$B)[]
            where $c.id eq 1 and xs:dateTime($c.vf) le $V and $V lt xs:dateTime($c.vt)
            order by $c.id
            return {"id":$c.id,"cost":$c.cost,"qty":$c.qty,"grade":$c.grade}
            """);
    add(queries, 2, "earlier belief", 1, "revision-scan+half-open-predicate", false,
        List.of("id", "cost", "qty", "grade"), """
            for $c in jn:open('bt','contracts',$A)[]
            where $c.id eq 1 and xs:dateTime($c.vf) le $V and $V lt xs:dateTime($c.vt)
            order by $c.id
            return {"id":$c.id,"cost":$c.cost,"qty":$c.qty,"grade":$c.grade}
            """);
    add(queries, 3, "valid range prices", 0, "revision-scan+strict-overlap", false,
        List.of("min_cost", "max_cost", "prices"), """
            let $rows := (for $c in jn:open('bt','contracts',$B)[]
                          where $c.id eq 1 and xs:dateTime($c.vf) lt $U and $L lt xs:dateTime($c.vt)
                          return $c)
            return {"min_cost":min($rows.cost),"max_cost":max($rows.cost),
                    "prices":count(distinct-values($rows.cost))}
            """);
    add(queries, 4, "corrections", 1, "VALIDTIME+strict-residual", true,
        List.of("id", "old_cost", "new_cost", "old_qty", "new_qty"), """
            for $a in local:slice('contracts',$A,$V)
            for $b in local:slice('contracts',$B,$V)
            where $a.id eq $b.id and ($a.cost ne $b.cost or $a.qty ne $b.qty)
            order by $a.id
            return {"id":$a.id,"old_cost":$a.cost,"new_cost":$b.cost,
                    "old_qty":$a.qty,"new_qty":$b.qty}
            """);
    add(queries, 5, "publication history", 1, "revision-scan-per-publication+half-open-predicate", false,
        List.of("epoch", "cost", "qty"), """
            for $e in jn:doc('bt','epochs')[]
            for $c in jn:open('bt','contracts',xs:dateTime($e.ts))[]
            where $c.id eq 1 and xs:dateTime($c.vf) le $V and $V lt xs:dateTime($c.vt)
            order by $e.epoch
            return {"epoch":$e.epoch,"cost":$c.cost,"qty":$c.qty}
            """);
    add(queries, 6, "grouped publication evolution", 2, "VALIDTIME+strict-residual+generic-group", true,
        List.of("epoch", "grade", "n", "qty_sum"), """
            for $e in jn:doc('bt','epochs')[]
            for $c in local:slice('contracts',xs:dateTime($e.ts),$V)
            let $epoch := $e.epoch, $grade := $c.grade, $qty := $c.qty
            group by $epoch,$grade
            let $n := count($qty), $qty_sum := sum($qty)
            order by $epoch,$grade
            return {"epoch":$epoch,"grade":$grade,"n":$n,"qty_sum":$qty_sum}
            """);
    add(queries, 7, "latest exposure by grade", 1, "VALIDTIME+strict-residual+generic-group", true,
        List.of("grade", "n", "qty_sum", "exposure"), """
            for $c in local:slice('contracts',$B,$V)
            let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty
            group by $grade
            let $n := count($qty), $qty_sum := sum($qty), $exposure := sum($value)
            order by $grade
            return {"grade":$grade,"n":$n,"qty_sum":$qty_sum,"exposure":$exposure}
            """);
    add(queries, 8, "supplier grade distribution", 2, "VALIDTIME+strict-residual+generic-group", true,
        List.of("sid", "grade", "n", "min_cost", "max_cost"), """
            for $c in local:slice('contracts',$A,$V)
            let $sid := $c.sid, $grade := $c.grade, $cost := $c.cost
            group by $sid,$grade
            let $n := count($cost), $min_cost := min($cost), $max_cost := max($cost)
            order by $sid,$grade
            return {"sid":$sid,"grade":$grade,"n":$n,"min_cost":$min_cost,"max_cost":$max_cost}
            """);
    add(queries, 9, "supplier temporal join", 2, "VALIDTIME+strict-residual+generic-join-group", true,
        List.of("region", "grade", "n", "exposure"), """
            for $c in local:slice('contracts',$B,$V)
            for $s in local:slice('suppliers',$B,$V)
            where $c.sid eq $s.id
            let $region := $s.region, $grade := $c.grade, $value := $c.cost * $c.qty
            group by $region,$grade
            let $n := count($value), $exposure := sum($value)
            order by $region,$grade
            return {"region":$region,"grade":$grade,"n":$n,"exposure":$exposure}
            """);
    add(queries, 10, "interval overlap product join", 1, "revision-scan+strict-overlap+generic-join-group", false,
        List.of("category", "contracts", "min_margin", "max_margin"), """
            for $c in jn:open('bt','contracts',$B)[]
            for $p in jn:open('bt','products',$B)[]
            where $c.pid eq $p.id
              and xs:dateTime($c.vf) lt $U and $L lt xs:dateTime($c.vt)
              and xs:dateTime($p.vf) lt $U and $L lt xs:dateTime($p.vt)
              and xs:dateTime($c.vf) lt xs:dateTime($p.vt)
              and xs:dateTime($p.vf) lt xs:dateTime($c.vt)
            let $category := $p.category, $cid := $c.id, $margin := $p.retail - $c.cost
            group by $category
            let $contracts := count(distinct-values($cid)),
                $min_margin := min($margin), $max_margin := max($margin)
            order by $category
            return {"category":$category,"contracts":$contracts,
                    "min_margin":$min_margin,"max_margin":$max_margin}
            """);
    add(queries, 11, "daily grouped exposure", 2, "VALIDTIME+strict-residual+generic-group", true,
        List.of("day_no", "grade", "n", "exposure"), """
            for $d in jn:doc('bt','days')[]
            where $d.day_no ge 150 and $d.day_no lt 210
            for $c in local:slice('contracts',$B,xs:dateTime($d.ts))
            let $day_no := $d.day_no, $grade := $c.grade, $value := $c.cost * $c.qty
            group by $day_no,$grade
            let $n := count($value), $exposure := sum($value)
            order by $day_no,$grade
            return {"day_no":$day_no,"grade":$grade,"n":$n,"exposure":$exposure}
            """);
    add(queries, 12, "retroactive disappearance", 1, "VALIDTIME+strict-residual+generic-anti-join-group", true,
        List.of("grade", "n", "old_exposure"), """
            let $new := local:slice('contracts',$D,$V)
            for $a in local:slice('contracts',$A,$V)
            where empty(for $b in $new where $b.id eq $a.id return $b.id)
            let $grade := $a.grade, $value := $a.cost * $a.qty
            group by $grade
            let $n := count($value), $old_exposure := sum($value)
            order by $grade
            return {"grade":$grade,"n":$n,"old_exposure":$old_exposure}
            """);
    return Collections.unmodifiableList(queries);
  }

  private static void add(final List<Query> queries, final int index, final String name, final int keyColumns,
      final String route, final boolean strictEndResidual, final List<String> columns, final String body) {
    queries.add(new Query(index, name, columns, keyColumns, body, route, strictEndResidual));
  }
}
