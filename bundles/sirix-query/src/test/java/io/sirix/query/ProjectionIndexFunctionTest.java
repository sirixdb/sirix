package io.sirix.query;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * End-to-end coverage of {@code jn:create-projection-index}: create a
 * columnar projection over stored records via JSONiq and run the analytical
 * query shapes it serves. Results must be identical with and without the
 * projection installed — the function's job is acceleration, not semantics.
 */
public final class ProjectionIndexFunctionTest extends AbstractJsonTest {

  private static final String STORE_QUERY = """
        jn:store('json-path1','sales.jn','[
          {"age": 30, "active": true,  "dept": "Eng"},
          {"age": 45, "active": false, "dept": "Sales"},
          {"age": 52, "active": true,  "dept": "Eng"},
          {"age": 23, "active": true,  "dept": "HR"},
          {"age": 61, "active": false, "dept": "Eng"}
        ]')
      """;

  private static final String CREATE_INDEX_QUERY = """
        let $doc := jn:doc('json-path1','sales.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/age', '/[]/active', '/[]/dept'),
            ('long', 'boolean', 'string'))
        return {"revision": sdb:commit($doc)}
      """;

  @Test
  public void createProjectionIndexAndAggregate() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "211");
  }

  @Test
  public void createProjectionIndexAndFilterCount() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          return count(for $r in $doc[] where $r.age > 40 and $r.active return $r)
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "1");
  }

  @Test
  public void createProjectionIndexAndGroupBy() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          order by $d
          return {"dept": $d, "count": count($r)}
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query,
        "{\"dept\":\"Eng\",\"count\":3} {\"dept\":\"HR\",\"count\":1} {\"dept\":\"Sales\",\"count\":1}");
  }

  @Test
  public void createProjectionIndexIsIdempotent() throws IOException {
    // Second call hydrates the persisted projection instead of rebuilding.
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return count($doc[])
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "5");
  }
}
