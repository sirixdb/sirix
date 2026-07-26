package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The §11-8 pure-double-source lift, end to end
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md): double projection columns whose every cell
 * came from {@code Double}/{@code Float} sources SERVE sum/avg/min/max (as {@code xs:double},
 * digit-identical to the interpreted pipeline), while integer- or decimal-fed double columns
 * keep failing closed to the exact fallback — proven via
 * {@link SirixVectorizedExecutor#doubleValueAggregatesServed()}, which counts value-aggregate
 * servings only (the catalog's {@code servedCount} increments on handle LOOKUPS and cannot
 * distinguish served from looked-up-then-declined).
 *
 * <p>Every assertion is differential: the expectation string is produced by running the SAME
 * query through the generic pipeline first, so serialization semantics are pinned by the
 * interpreter, never hand-written.
 */
public final class ProjectionDoubleAggregateServingTest extends AbstractJsonTest {

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  private static final String[] VALUE_FUNCS = { "sum", "avg", "min", "max" };

  @Test
  public void pureDoubleColumnServesValueAggregates() throws IOException {
    // Exponent-form literals on purpose: JsonNumber.stringToNumber tags plain decimals
    // ("1.25") as BigDecimal — only exponent-form numbers that round-trip through
    // Double.toString shred as Double. Purity is SOURCE typing, so this is the shape that
    // legitimately unlocks value serving; the plain-decimal shape is pinned as declining in
    // decimalLiteralDoubleColumnStaysCountOnly below.
    query("""
          jn:store('json-path1','prices.jn','[
            {"p": 1.25E0, "q": 2.0E0},
            {"p": 2.5E0,  "q": 3.5E0},
            {"p": -0.75E0,"q": 1.0E0},
            {"p": 10.0E0, "q": 0.5E0},
            {"p": 4.125E0,"q": 8.25E0}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','prices.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/p', '/[]/q'), ('double', 'double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("prices.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        for (final String func : VALUE_FUNCS) {
          final String queryStr = "let $doc := jn:doc('json-path1','prices.jn')\n"
              + "return " + func + "(for $r in $doc[] return $r.p)";
          final String expected = evaluate(chain, ctx, queryStr);
          final long servedBefore = SirixVectorizedExecutor.doubleValueAggregatesServed();
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
          final String actual = evaluate(chain, ctx, queryStr);
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          Assertions.assertEquals(expected, actual, func + " must be digit-identical");
          Assertions.assertTrue(SirixVectorizedExecutor.doubleValueAggregatesServed() > servedBefore,
              func + " over a pure-double column must be SERVED, not answered by the fallback");
        }
        // Predicated value aggregate — the predicated call site is purity-gated too.
        final String predicated = "let $doc := jn:doc('json-path1','prices.jn')\n"
            + "return sum(for $r in $doc[] where $r.q gt 1.0e0 return $r.p)";
        final String expected = evaluate(chain, ctx, predicated);
        final long servedBefore = SirixVectorizedExecutor.doubleValueAggregatesServed();
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        final String actual = evaluate(chain, ctx, predicated);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        Assertions.assertEquals(expected, actual, "predicated sum must be digit-identical");
        Assertions.assertTrue(SirixVectorizedExecutor.doubleValueAggregatesServed() > servedBefore,
            "predicated sum over a pure-double column must be SERVED");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void integerFedDoubleColumnStaysCountOnly() throws IOException {
    // {"p": 3} carries an INTEGER source: the double conversion is exact, but the fallback
    // types the whole aggregate through its long/decimal accumulators — purity is strict
    // source typing, so value aggregates must keep falling back (and stay correct).
    query("""
          jn:store('json-path1','mixed.jn','[
            {"p": 1.25},
            {"p": 3},
            {"p": 2.75}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','mixed.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/p'), ('double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("mixed.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        for (final String func : VALUE_FUNCS) {
          final String queryStr = "let $doc := jn:doc('json-path1','mixed.jn')\n"
              + "return " + func + "(for $r in $doc[] return $r.p)";
          final String expected = evaluate(chain, ctx, queryStr);
          final long servedBefore = SirixVectorizedExecutor.doubleValueAggregatesServed();
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
          final String actual = evaluate(chain, ctx, queryStr);
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          Assertions.assertEquals(expected, actual, func + " must stay correct via the fallback");
          Assertions.assertEquals(servedBefore, SirixVectorizedExecutor.doubleValueAggregatesServed(),
              func + " over an integer-fed double column must NOT be value-served (fail closed)");
        }
        // count is exact and always servable regardless of purity.
        final String countQuery = "let $doc := jn:doc('json-path1','mixed.jn')\n"
            + "return count(for $r in $doc[] return $r.p)";
        final String expected = evaluate(chain, ctx, countQuery);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        final String actual = evaluate(chain, ctx, countQuery);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        Assertions.assertEquals(expected, actual);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void illConditionedSumMatchesInterpreterExactly() throws IOException {
    // Double addition is not associative: 1e16 + 1 + 1 gives different digits under
    // different fold orders. Serving streams the matched cells through brackit's OWN
    // SumAvgAggregator (same seeding, batching, SIMD reduction as the interpreter), so
    // digits must match BY CONSTRUCTION — this dataset is the regression guard for the
    // review finding that refuted the hand-rolled sequential fold.
    query("""
          jn:store('json-path1','illcond.jn','[
            {"p": 1.0E16},
            {"p": 1.0E0},
            {"p": 1.0E0}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','illcond.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/p'), ('double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    assertValueAggregatesServedAndIdentical("illcond.jn", "sum", "avg");
  }

  @Test
  public void negativeZeroEdgesMatchInterpreterExactly() throws IOException {
    // -0.0 is where naive kernels diverge: IEEE < / > treat ±0.0 as equal (first-seen
    // wins) while the interpreter's min/max compare via Double.compare (-0.0 < 0.0), and
    // a 0.0-seeded sum absorbs a lone -0.0 while the interpreter's seed-first sum keeps
    // it. Serving uses Double.compare in the kernel and brackit's own summation, so all
    // three shapes must serialize identically to the fallback ("0" vs "-0" included).
    query("""
          jn:store('json-path1','negzero.jn','[
            {"p": 0.0E0,  "q": -0.0E0},
            {"p": -0.0E0, "q": 0.0E0}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','negzero.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/p', '/[]/q'), ('double', 'double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    assertValueAggregatesServedAndIdentical("negzero.jn", "sum", "avg", "min", "max");

    // Lone -0.0: the discriminating case between a 0.0-seeded accumulator (absorbs the
    // sign: 0.0 + -0.0 = +0.0 → "0") and the interpreter's seed-first pairwise fold
    // (keeps it → "-0"). The serving fold is seed-first precisely for this.
    query("""
          jn:store('json-path1','negzero1.jn','[
            {"p": -0.0E0}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','negzero1.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/p'), ('double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    assertValueAggregatesServedAndIdentical("negzero1.jn", "sum", "avg", "min", "max");
  }

  /**
   * For each function: evaluate through the generic pipeline first (the oracle), then with
   * the executor; assert byte-identical serialization AND that the served counter moved.
   * Both columns (p, q) are checked when present.
   */
  private void assertValueAggregatesServedAndIdentical(final String resource,
      final String... funcs) throws IOException {
    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(resource);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        for (final String func : funcs) {
          final String queryStr = "let $doc := jn:doc('json-path1','" + resource + "')\n"
              + "return " + func + "(for $r in $doc[] return $r.p)";
          final String expected = evaluate(chain, ctx, queryStr);
          final long servedBefore = SirixVectorizedExecutor.doubleValueAggregatesServed();
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
          final String actual = evaluate(chain, ctx, queryStr);
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          Assertions.assertEquals(expected, actual,
              func + "(" + resource + ") must be byte-identical to the interpreter");
          Assertions.assertTrue(SirixVectorizedExecutor.doubleValueAggregatesServed() > servedBefore,
              func + "(" + resource + ") must be SERVED");
        }
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void decimalLiteralDoubleColumnStaysCountOnly() throws IOException {
    // Plain decimal literals shred as BigDecimal (JsonNumber.stringToNumber) — the
    // interpreter aggregates them EXACTLY and surfaces Dec, which a double kernel cannot
    // reproduce digit-for-digit. The purity bit must therefore stay clear and value
    // aggregates must keep falling back for the most common real-world dataset shape.
    query("""
          jn:store('json-path1','decs.jn','[
            {"p": 1.25},
            {"p": 2.75},
            {"p": 12.125}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','decs.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/p'), ('double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("decs.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      try {
        final String queryStr = "let $doc := jn:doc('json-path1','decs.jn')\n"
            + "return sum(for $r in $doc[] return $r.p)";
        final String expected = evaluate(chain, ctx, queryStr);
        final long servedBefore = SirixVectorizedExecutor.doubleValueAggregatesServed();
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        final String actual = evaluate(chain, ctx, queryStr);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        Assertions.assertEquals(expected, actual, "decimal-exact sum digits must be preserved");
        Assertions.assertEquals(servedBefore, SirixVectorizedExecutor.doubleValueAggregatesServed(),
            "sum over a BigDecimal-fed double column must NOT be value-served");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String queryStr) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
         final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, queryStr).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
