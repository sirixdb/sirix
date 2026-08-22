package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code !=} against a field that can hold a JSON null must NOT be served.
 *
 * <p>
 * This is the one comparison that looks equality-shaped and is not. The interpreter answers
 * {@code null ne "x"} with {@code !(leftIsNull && rightIsNull)} — TRUE ({@code Cmp.java:88}) —
 * while every sirix kernel reads a JSON null as MISSING and answers false. A served {@code !=}
 * would therefore silently DROP exactly the null-valued rows the interpreter counts: an under-count
 * with no error anywhere.
 *
 * <p>
 * {@code acceptsPredicate} is the gate that prevents it, by declining any predicate that compares a
 * null-bearing path with an operator whose null semantics the kernels do not reproduce. {@code eq}
 * is exempt because it genuinely agrees (both sides answer false); {@code ne} was exempt too, which
 * was harmless only while no {@code ne} operator string could be produced. It became reachable the
 * moment brackit's detection started emitting one.
 *
 * <p>
 * Tested through {@code acceptsPredicate} directly rather than through a query: sirix's own routes
 * (sorted scan, row materialize, group aggregate) never consult this gate, so a query-level
 * differential over a projection would decline for unrelated reasons and prove nothing.
 */
public final class NotEqualNullGateTest {

  private Path dbDir;

  /** Records where "nullable" is JSON null on a third, and "dense" is never null. */
  private static final String DATA = """
      [{"id":1,"nullable":"a","dense":1},
       {"id":2,"nullable":null,"dense":2},
       {"id":3,"nullable":"c","dense":3}]""";

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-ne-null-gate-");
    // Path STATISTICS, not just the summary: nullCount is maintained by the statistics, and with
    // them off the gate cannot prove anything and passes everything by design.
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).buildPathStatistics(true).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('gate-db','r.jn','" + DATA.replace("\n", "") + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  private void withExecutor(final java.util.function.Consumer<SirixVectorizedExecutor> body) {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build()) {
      final var db = Databases.openJsonDatabase(dbDir.resolve("gate-db"));
      final var session = db.beginResourceSession("r.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
      try {
        body.accept(executor);
      } finally {
        executor.close();
      }
    }
  }

  private static final String[] SOURCE = {"[]"};

  @Test
  void notEqualOverANullBearingFieldIsDeclined() {
    withExecutor(
        executor -> assertFalse(executor.acceptsPredicate(SOURCE, new PredicateNode.NumCmp("nullable", "ne", 7L)),
            "`nullable != 7` must DECLINE: the interpreter counts the JSON-null row, the kernels drop it"));
  }

  @Test
  void equalOverTheSameFieldIsStillAccepted() {
    // The contrast that shows the gate discriminates by OPERATOR, not merely by "this path has
    // nulls". `null eq 7` is false on both sides, so equality is safe over the very same column.
    withExecutor(
        executor -> assertTrue(executor.acceptsPredicate(SOURCE, new PredicateNode.NumCmp("nullable", "eq", 7L)),
            "`nullable = 7` agrees on null rows and must still be served"));
  }

  @Test
  void notEqualOverANullFreeFieldIsAccepted() {
    // And that it discriminates by COLUMN: "dense" never holds a null, so `!=` over it is safe.
    withExecutor(executor -> assertTrue(executor.acceptsPredicate(SOURCE, new PredicateNode.NumCmp("dense", "ne", 7L)),
        "`dense != 7` has no null rows to disagree about and must be served"));
  }
}
