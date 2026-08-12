package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.Databases;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Auto-wiring for a query whose document arrives as an external variable rather than as
 * {@code jn:doc}.
 *
 * <h2>The gap</h2>
 *
 * <p>
 * A store-only compile chain reads the resource off the query's own {@code jn:doc}/{@code jn:open}
 * ({@link StoreBoundExecutorCache}). {@code declare variable $doc external} with the document bound
 * through the {@link SirixQueryContext} puts no such call in the tree — and that is the ordinary
 * embedding shape: bind once, compile once, run many. Auto-vectorization being ON by default was
 * therefore indistinguishable from it being off for exactly the callers most likely to rely on it;
 * on a 3.5M-record corpus the same query with the same answer took 705 ms that way against 1.1 ms
 * written with a literal {@code jn:doc}.
 *
 * <h2>What is asserted</h2>
 *
 * <p>
 * Two halves, and the second is the one that keeps the first honest. Speed: the columnar path must
 * actually run for an externally bound document — an agreement test alone would pass just as well
 * with the fix removed. Safety: {@link BoundDocumentHint} is a HINT, taken from whatever the caller
 * bound most recently on this thread, and a caller may go on to run a query over a different
 * document entirely. That must still produce the right answer, because the compile-time hint only
 * decides which executor is built, while the runtime source gate re-checks the actual binding per
 * evaluation.
 */
final class ExternalDocumentAutoWiringTest {

  private static final int N = 6_000;
  private static final String DB_A = "external-doc-a";
  private static final String DB_B = "external-doc-b";
  private static final String RES = "records.jn";
  private static final QNm DOC = new QNm("doc");
  private static final QNm OTHER = new QNm("other");

  /**
   * Anchored so a record lacking the field cannot satisfy it — the shape the columnar route serves.
   */
  private static final String PREDICATE = "$u.year gt 1990 and $u.year lt 2010";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-external-doc-");
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB_A + "','" + RES + "','" + corpus(1980) + "')").evaluate(ctx);
      // A different base, so the two resources answer the SAME predicate with different non-zero
      // counts — which is what makes "the foreign hint served the wrong document" detectable.
      new Query(chain, "jn:store('" + DB_B + "','" + RES + "','" + corpus(1995) + "')").evaluate(ctx);
    }
    BoundDocumentHint.clear();
  }

  /**
   * Records whose years start at {@code base}, so the two resources answer the predicate differently.
   */
  private static String corpus(final int base) {
    final StringBuilder sb = new StringBuilder(N * 40);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"year\":").append(base + i % 60).append(",\"title\":\"t").append(i).append("\"}");
    }
    return sb.append(']').toString();
  }

  @AfterEach
  void tearDown() {
    BoundDocumentHint.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB_A));
      Databases.removeDatabase(dbDir.resolve(DB_B));
    }
  }

  @Test
  @DisplayName("an externally bound document reaches the columnar path and agrees with the generic one")
  void externallyBoundDocumentIsAutoWired() throws Exception {
    final long viaGeneric = countExternal(DB_A, DB_A, false);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long viaColumns = countExternal(DB_A, DB_A, true);
    assertEquals(viaGeneric, viaColumns, "auto-wired answer differs from the generic pipeline's");
    assertTrue(viaGeneric > 0, "predicate matches nothing, so it proves nothing");
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
        "no page was served from columns for an externally bound document — the chain has no "
            + "jn:doc to read the resource off, so without the binding hint the query "
            + "compiles the generic pipeline exactly as it did before");
  }

  @Test
  @DisplayName("a hint left over from another document does not change the answer")
  void aStaleHintCannotProduceAWrongAnswer() throws Exception {
    // The query reads B while the LAST document bound — and therefore the hint the compile sees —
    // is A's. The hint picks which executor gets built; the runtime source gate checks the binding
    // it is actually asked to serve, and A's executor must refuse B's document.
    final long expected = countExternal(DB_B, DB_B, false);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long withForeignHint = countExternal(DB_B, DB_A, true);
    assertEquals(expected, withForeignHint,
        "a compile-time hint naming another resource changed the ANSWER — the runtime "
            + "source gate is what makes the hint safe, and it did not decline");
    assertTrue(expected > 0, "predicate matches nothing on B, so it proves nothing");
  }

  /**
   * Count through an externally bound {@code $doc}.
   *
   * @param queryDatabase the document {@code $doc} is bound to — what the query reads
   * @param hintDatabase the document bound LAST, and therefore the one the compile-time hint names;
   *        equal to {@code queryDatabase} for the ordinary case, different to reach the mismatch
   */
  private long countExternal(final String queryDatabase, final String hintDatabase, final boolean autoWire)
      throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = autoWire
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      BoundDocumentHint.clear();
      ctx.bind(DOC, (Sequence) store.lookup(queryDatabase).getDocument());
      // A second bound document, which a query is free to have. It is the most recent binding, so
      // it is what the hint carries into the compile below.
      ctx.bind(OTHER, (Sequence) store.lookup(hintDatabase).getDocument());
      final String q = "declare variable $doc external; declare variable $other external; "
          + "count(for $u in $doc[] where " + PREDICATE + " return $u)";
      return ((Int64) new Query(chain, q).evaluate(ctx)).longValue();
    }
  }
}
