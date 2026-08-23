/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An aggregate whose source path does not exist must fold over NOTHING.
 *
 * <p>
 * The analytical fast paths resolve {@code VECTORIZED_SOURCE_PATH_PREFIX + field} to one path node
 * and scope the scan to it. That resolution answers {@code -1} both for a path it cannot SCOPE (no
 * path summary, an ambiguous name, an array-valued field) and for a path that is provably ABSENT,
 * and the kernels degrade {@code -1} to matching the field by NAME across the whole resource. That
 * degradation is right for the first group and a wrong ANSWER for the second: after
 * {@code delete json $doc.a}, {@code sum(for $r in $doc.a[] return $r.age)} folded every
 * {@code age} still living under {@code $doc.b} into a sum whose source sequence is empty.
 *
 * <p>
 * The oracle here is the engine's own generic pipeline, compiled through
 * {@link SirixCompileChain#createWithJsonStoreWithoutAutoWiring} — the factory that exists so a
 * differential has something to compare against. Serving may change what a query COSTS, never what
 * it answers, so every case asserts the served answer against both a literal expectation and the
 * generic one.
 */
public final class AbsentSourcePathAggregateTest extends AbstractJsonTest {

  private static final String STORE = """
        jn:store('json-path1','two.jn','{
          "a": [{"age": 10}, {"age": 20}],
          "b": [{"age": 1}, {"age": 2}]
        }')
      """;

  private static final String CREATE_PROJECTION = """
        let $doc := jn:doc('json-path1','two.jn')
        let $stats := jn:create-projection-index($doc, '/a/[]', ('/a/[]/age'), ('long'))
        return {"revision": sdb:commit($doc)}
      """;

  private static final String DELETE_RECORD_SET = """
        let $doc := jn:doc('json-path1','two.jn')
        return delete json $doc.a
      """;

  private static final String PREAMBLE = "let $doc := jn:doc('json-path1','two.jn') return ";

  /** The registry is a JVM-wide static and the on-disk store is wiped per test. */
  @BeforeEach
  public void clearProjectionRegistryBefore() {
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  public void clearProjectionRegistryAfter() {
    ProjectionIndexRegistry.clear();
  }

  @Test
  public void deletedRecordSetAggregatesOverNothing() throws IOException {
    query(STORE);
    query(DELETE_RECORD_SET);
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.a[] return $r.age)", "0");
    // The sibling record set is untouched — the guard must not cost a scan that IS scoped.
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.b[] return $r.age)", "3");
  }

  @Test
  public void aggregateOverAFieldThatNeverExistedIsEmpty() throws IOException {
    query(STORE);
    // Nothing was deleted here: `zzz` simply never existed, so name-only matching had no absence
    // to blame and folded `age` from BOTH record sets.
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.zzz[] return $r.age)", "0");
    assertServedAndGenericAgree(PREAMBLE + "count(for $r in $doc.zzz[] return $r.age)", "0");
  }

  @Test
  public void avgAndMinMaxOverAnAbsentSourcePathReturnEmpty() throws IOException {
    query(STORE);
    query(DELETE_RECORD_SET);
    // fn:avg/fn:min/fn:max over the empty sequence are the EMPTY sequence, not 0.
    assertServedAndGenericAgree(PREAMBLE + "avg(for $r in $doc.a[] return $r.age)", "");
    assertServedAndGenericAgree(PREAMBLE + "min(for $r in $doc.a[] return $r.age)", "");
    assertServedAndGenericAgree(PREAMBLE + "max(for $r in $doc.a[] return $r.age)", "");
  }

  @Test
  public void aMaintainedProjectionOverTheDeletedRecordSetStillAggregatesOverNothing() throws IOException {
    query(STORE);
    query(CREATE_PROJECTION);
    // Read-your-writes on the projection before the delete, so the index is demonstrably live.
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.a[] return $r.age)", "30");
    query(DELETE_RECORD_SET);
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.a[] return $r.age)", "0");
    assertServedAndGenericAgree(PREAMBLE + "sum(for $r in $doc.b[] return $r.age)", "3");
  }

  /**
   * Asserts the query's answer through the auto-wired (fast-path) chain AND through the generic
   * pipeline, and that both equal {@code expected}.
   */
  private static void assertServedAndGenericAgree(final String query, final String expected) throws IOException {
    final String served = serialize(query, true);
    final String generic = serialize(query, false);
    assertEquals(expected, generic, "the generic pipeline is the oracle and must answer " + expected);
    assertEquals(generic, served, "serving changed the ANSWER, not just the cost, for: " + query);
  }

  private static String serialize(final String query, final boolean autoWired) throws IOException {
    try (final BasicJsonDBStore store =
             BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = autoWired
             ? SirixCompileChain.createWithJsonStore(store)
             : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
         final var out = new ByteArrayOutputStream();
         final var printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
