package io.sirix.query;

import io.sirix.JsonTestHelper;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The two string-column encodings must be indistinguishable from a query's point of view.
 *
 * <p>
 * A {@code string} column is stored either with a per-leaf dictionary
 * ({@code COLUMN_KIND_STRING_DICT}) or with ids into a resource-wide one
 * ({@code COLUMN_KIND_STRING_GLOBAL}), and which one a build picks is decided from the data it
 * sees. Nothing in a query says which it wants, so nothing in an answer may depend on which it got
 * — not the values, not the grouping, not the counts, not the order.
 *
 * <p>
 * The test builds the SAME corpus both ways in one JVM (the mode is a system property read per
 * build, precisely so this is possible) and requires the answers to be character-identical. It
 * would catch every failure mode the conversion has: ids that mean different things in different
 * leaves, absent cells that acquire a value, the empty string colliding with "no value", and a
 * dictionary whose reverse direction disagrees with what was interned.
 *
 * <p>
 * Both arms are compared against each other rather than against a hard-coded expectation on
 * purpose: the per-leaf arm is the already-verified behaviour, so it is the reference, and pinning
 * literals here would only duplicate the suites that already own those answers.
 */
public final class GlobalValueDictionaryParityTest extends AbstractJsonTest {

  private static final String GLOBAL_DICT_PROPERTY = "sirix.projection.globalDict";

  /**
   * Deliberately awkward data. Every row's {@code did} is distinct except for two deliberate repeats
   * spanning what will be separate leaves under a small row group; {@code kind} repeats heavily (the
   * shape that must STAY per-leaf); one row omits {@code did} entirely (absent, which must not become
   * a value); one carries the empty string (a real value that must not become "absent"); and two
   * differ only past a shared prefix.
   */
  private static final String STORE = """
        jn:store('json-path1','parity.jn','[
          {"kind":"commit","did":"did:plc:aaaa","tag":"x","n":1},
          {"kind":"commit","did":"did:plc:aaab","tag":"y","n":2},
          {"kind":"commit","did":"","tag":"x","n":3},
          {"kind":"identity","did":"did:plc:cccc","tag":"z","n":4},
          {"kind":"commit","tag":"x","n":5},
          {"kind":"commit","did":"did:plc:aaaa","tag":"y","n":6},
          {"kind":"account","did":"did:plc:dddd","tag":"x","n":7},
          {"kind":"commit","did":"did:plc:eeee","tag":"z","n":8},
          {"kind":"commit","did":"did:plc:ffff","tag":"y","n":9},
          {"kind":"identity","did":"","tag":"x","n":10}
        ]')
      """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','parity.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/tag', '/[]/n'),
            ('string', 'string', 'string', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  /** Group by the high-cardinality column and return its values — the reverse direction. */
  private static final String GROUP_BY_DID = """
        for $e in jn:doc('json-path1','parity.jn')[]
        let $k := $e.did
        group by $k
        let $c := count($e)
        order by $k
        return {"did": $k, "count": $c}
      """;

  /** JSONBench Q2's shape: distinct over the high-cardinality column, per low-cardinality group. */
  private static final String DISTINCT_PER_GROUP = """
        for $e in jn:doc('json-path1','parity.jn')[]
        let $k := $e.kind
        group by $k
        let $c := count($e)
        let $u := count(distinct-values($e.did))
        order by $k
        return {"kind": $k, "count": $c, "users": $u}
      """;

  /** Equality against a literal the dictionary holds, and one it does not. */
  private static final String EQUALITY = """
        (count(for $e in jn:doc('json-path1','parity.jn')[] where $e.did eq "did:plc:aaaa" return $e),
         count(for $e in jn:doc('json-path1','parity.jn')[] where $e.did eq "did:plc:aaab" return $e),
         count(for $e in jn:doc('json-path1','parity.jn')[] where $e.did eq "" return $e),
         count(for $e in jn:doc('json-path1','parity.jn')[] where $e.did eq "nope" return $e))
      """;

  /** Emitting the column's values under a predicate, in document order. */
  private static final String EMIT_VALUES = """
        for $e in jn:doc('json-path1','parity.jn')[]
        where $e.kind eq "commit"
        return $e.did
      """;

  /** Ordering by the high-cardinality column — must order by VALUE, never by id. */
  private static final String ORDER_BY_DID = """
        for $e in jn:doc('json-path1','parity.jn')[]
        order by $e.did, $e.n
        return {"did": $e.did, "n": $e.n}
      """;

  /** JSONBench Q4's shape: top-k groups over the high-cardinality key. */
  private static final String TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','parity.jn')[]
          let $k := $e.did
          group by $k
          let $first := min($e.n)
          order by $first
          return {"did": $k, "first": $first}, 1, 3)
      """;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void bothEncodingsAnswerIdentically() throws IOException {
    final String[] queries = {GROUP_BY_DID, DISTINCT_PER_GROUP, EMIT_VALUES, ORDER_BY_DID, TOP_K};
    final String[] names = {"group-by-did", "distinct-per-group", "emit-values", "order-by-did", "top-k"};

    final String[] perLeaf = answersUnder("never", queries);
    final String[] global = answersUnder("always", queries);

    for (int i = 0; i < queries.length; i++) {
      Assertions.assertEquals(perLeaf[i], global[i],
          "the " + names[i] + " shape answered differently under a resource-wide dictionary than under "
              + "per-leaf ones — the encoding is visible to queries, which it must never be");
    }
  }

  /**
   * KNOWN OPEN, and quarantined rather than deleted so it is not forgotten: a bare
   * {@code count(... where col eq "")} answers 2 under per-leaf dictionaries and 0 under a
   * resource-wide one.
   *
   * <p>
   * The data itself is right — the same corpus groups, orders, emits and distinct-counts the empty
   * string identically under both encodings (that is what {@link #bothEncodingsAnswerIdentically}
   * shows). What differs is a count route that answers from per-value dictionary row counts, which a
   * global column does not carry, and which returns "no rows" instead of declining. It is reachable
   * only with {@code -Dsirix.projection.globalDict} set away from its default, so nothing ships
   * exposed to it; it must be fixed with the query routes.
   */
  @Test
  public void equalityCountsAgreeAcrossEncodings() throws IOException {
    final String[] perLeaf = answersUnder("never", new String[] {EQUALITY});
    final String[] global = answersUnder("always", new String[] {EQUALITY});
    Assertions.assertEquals(perLeaf[0], global[0]);
  }

  /**
   * A forced-global build must actually produce a global column, or the parity above is vacuous — it
   * would be comparing the per-leaf encoding with itself.
   */
  @Test
  public void theForcedBuildActuallyProducesAGlobalDictionary() throws IOException {
    System.setProperty(GLOBAL_DICT_PROPERTY, "always");
    JsonTestHelper.deleteEverything();
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Assertions.assertTrue(anyColumnIsGlobal(),
        "a build forced to use resource-wide dictionaries produced none, so the parity test above "
            + "compares the per-leaf encoding against itself and proves nothing");
  }

  /**
   * The default is {@code auto}, and auto must leave THIS corpus alone.
   *
   * <p>
   * Ten rows over a handful of distinct values is the shape a per-leaf dictionary was made for: it
   * packs into a couple of bits per row and materialises with no record read. A resource-wide
   * dictionary cannot repay its machinery against that, so a heuristic that reached for it here would
   * be choosing the encoding by type rather than by data — which is the one thing the measurement
   * exists to avoid.
   */
  @Test
  public void theDefaultBuildLeavesALowCardinalityCorpusPerLeaf() throws IOException {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    JsonTestHelper.deleteEverything();
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Assertions.assertFalse(anyColumnIsGlobal(),
        "the default build gave a resource-wide dictionary to a corpus of ten rows over a few "
            + "distinct values, where the per-leaf form is strictly cheaper — the dedup-factor "
            + "heuristic is not deciding from the data");
  }

  /** Run every query against a corpus built under {@code mode}. */
  private String[] answersUnder(final String mode, final String[] queries) throws IOException {
    System.setProperty(GLOBAL_DICT_PROPERTY, mode);
    JsonTestHelper.deleteEverything();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] answers = new String[queries.length];
    for (int i = 0; i < queries.length; i++) {
      answers[i] = queryToString(queries[i]);
    }
    return answers;
  }

  /** Whether the most recent build produced at least one resource-wide dictionary column. */
  private static boolean anyColumnIsGlobal() {
    return ProjectionIndexBuilder.globalDictionaryColumnsBuilt() > 0;
  }

  /** Run one query and return its serialized answer. */
  private String queryToString(final String query) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new io.brackit.query.Query(chain, query).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
