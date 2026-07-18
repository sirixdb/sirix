package io.sirix.query;

import io.brackit.query.QueryException;
import io.sirix.index.projection.ProjectionIndexRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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

  // The registry is a JVM-wide static — the on-disk stores are wiped between
  // tests, so a surviving entry would serve stale leaves for a fresh
  // same-named resource.
  @BeforeEach
  public void clearProjectionRegistryBefore() {
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  public void clearProjectionRegistryAfter() {
    ProjectionIndexRegistry.clear();
  }

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
    // Second call short-circuits on the already-installed registry entry.
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return count($doc[])
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "5");
  }

  @Test
  public void hydratesFromPersistedMetadataAfterRegistryClear() throws IOException {
    // Simulates a fresh process: the in-memory registry is empty but the
    // projection (metadata at slot 0 + compact leaves) is persisted —
    // re-creating with the same shape must hydrate, not rebuild, and the
    // hydrated projection must serve correct results.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    ProjectionIndexRegistry.clear();
    final String hydrateAndAggregate = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return sum(for $r in $doc[] return $r.age)
        """;
    test(hydrateAndAggregate, "211");
  }

  @Test
  public void rehydratingWithDifferentShapeFails() throws IOException {
    // A persisted projection carries its shape in the metadata payload —
    // re-creating under a different field list must fail loudly instead of
    // silently mislabeling the persisted columns.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    ProjectionIndexRegistry.clear();
    final String mismatched = """
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/dept'),
              ('long', 'string'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(mismatched));
    Assertions.assertTrue(e.getMessage().contains("different shape"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  public void duplicateFieldNamesAreRejected() {
    query(STORE_QUERY);
    final String duplicate = """
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/nested/age'),
              ('long', 'long'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(duplicate));
    Assertions.assertTrue(e.getMessage().contains("Duplicate projected field name"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  public void floatingPointColumnTypesAreRejected() {
    query(STORE_QUERY);
    final String floating = """
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age'), ('double'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(floating));
    Assertions.assertTrue(e.getMessage().contains("Unsupported projection column type"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  public void ambiguousFieldNameUnderRecordSetIsRejected() {
    // "age" exists both at /[]/age and /[]/addr/age — the executor resolves
    // columns by trailing field name, so the projection cannot distinguish
    // the two occurrences and creation must refuse.
    final String storeAmbiguous = """
          jn:store('json-path1','amb.jn','[
            {"age": 30, "addr": {"age": 99}},
            {"age": 45, "addr": {"age": 12}}
          ]')
        """;
    query(storeAmbiguous);
    final String create = """
          let $doc := jn:doc('json-path1','amb.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age'), ('long'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(create));
    Assertions.assertTrue(e.getMessage().contains("ambiguous"),
        () -> "unexpected message: " + e.getMessage());
  }
}
