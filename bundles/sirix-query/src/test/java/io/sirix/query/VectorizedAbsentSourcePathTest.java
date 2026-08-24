package io.sirix.query;

import org.junit.jupiter.api.Test;

/**
 * Regression: an aggregate over a record-set path the PATH SUMMARY proves absent must answer
 * EMPTY, never scan unscoped. {@code resolveTargetPathNodeKey} returns {@code -1} both for
 * "cannot scope — match by name" (legitimate) and for "this path does not exist", and the
 * kernels treat {@code -1} as name-only matching across the WHOLE resource — so
 * {@code sum($doc.a[].age)} after {@code delete json $doc.a} (or over a never-existing field)
 * summed OTHER record sets' values. Surfaced when the base-leaf recycle made a fully-deleted
 * record set's projection truthfully empty: the executor declined it and fell through to the
 * fail-open route this test pins shut.
 */
public final class VectorizedAbsentSourcePathTest extends AbstractJsonTest {

  @Test
  public void aggregateOverDeletedRecordSetAnswersEmpty() throws Exception {
    query("""
          jn:store('json-path1','two.jn','{
            "a": [{"age": 10}, {"age": 20}],
            "b": [{"age": 1}, {"age": 2}]
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          return delete json $doc.a
        """);
    test("""
          let $doc := jn:doc('json-path1','two.jn')
          return {"a": sum(for $r in $doc.a[] return $r.age),
                  "b": sum(for $r in $doc.b[] return $r.age)}
        """, "{\"a\":0,\"b\":3}");
  }

  @Test
  public void aggregateOverNeverExistingRecordSetAnswersEmpty() throws Exception {
    query("""
          jn:store('json-path1','two.jn','{
            "a": [{"age": 10}, {"age": 20}],
            "b": [{"age": 1}, {"age": 2}]
          }')
        """);
    test("""
          let $doc := jn:doc('json-path1','two.jn')
          return {"x": sum(for $r in $doc.x[] return $r.age),
                  "n": count(for $r in $doc.x[] return $r.age)}
        """, "{\"x\":0,\"n\":0}");
  }
}
