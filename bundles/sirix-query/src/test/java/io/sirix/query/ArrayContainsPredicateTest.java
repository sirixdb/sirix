package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code some $g in $m.field[] satisfies $g eq "literal"} — membership in an array-valued field.
 *
 * <h2>Why the shape needed its own predicate node</h2>
 *
 * <p>The optimizer's predicate vocabulary had no quantified form, so a query with this {@code where}
 * clause was never claimed and ran entirely on the generic pipeline. On a 3.48M-record corpus that
 * meant a full record scan — the one shape still an order of magnitude behind DuckDB one-shot.
 *
 * <h2>What is asserted</h2>
 *
 * <p>Agreement with the generic pipeline, which is the only ground truth that cannot drift, over a
 * corpus built to break the three things this can get wrong: records with NO such field at all
 * (they satisfy nothing, and that is what lets the shape anchor on the field), records with an
 * EMPTY array (likewise), and arrays holding non-string elements beside the strings (comparing
 * {@code 3} to {@code "3"} is a match the interpreter does not make).
 */
final class ArrayContainsPredicateTest {

  private static final int N = 3_000;
  private static final String DB = "array-contains-db";
  private static final String RES = "records.jn";

  private static final List<String> SHAPES = List.of(
      "some $g in $m.genres[] satisfies $g eq 'Drama'",
      "some $g in $m.genres[] satisfies $g eq 'Nowhere'",
      "$m.year gt 1950 and (some $g in $m.genres[] satisfies $g eq 'Drama')",
      "(some $g in $m.genres[] satisfies $g eq 'Drama') and $m.year lt 1950",
      // A universal is NOT the anchorable direction — it is vacuously true on a record with no
      // array at all — so it must fall through to the generic pipeline and still be right.
      "every $g in $m.genres[] satisfies $g eq 'Drama'");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-array-contains-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"year\":").append(1900 + i % 120);
      if (i % 7 != 0) {                       // a seventh of the records have NO genres field
        sb.append(",\"genres\":[");
        if (i % 5 != 0) {                     // and a fifth of the rest have an EMPTY array
          sb.append(i % 3 == 0 ? "\"Drama\"" : "\"Comedy\"");
          if (i % 4 == 0) {
            sb.append(",\"Short\"");
          }
        }
        sb.append(']');
      }
      // Non-string elements beside the strings in the SAME array, so the element loader is
      // exercised on a mixed array. Not queried directly: a value comparison across types is a
      // type error in the interpreter, so no valid query can ask about it.
      sb.append(",\"tags\":[3,true]");
      sb.append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("array membership agrees with the generic pipeline on every shape")
  void arrayMembershipAgreesWithTheGenericPipeline() throws Exception {
    for (final String predicate : SHAPES) {
      assertEquals(count(predicate, false), count(predicate, true),
                   "the vectorized answer differs from the generic pipeline's for: " + predicate
                       + " — a record with no such field, or an empty array, satisfies no "
                       + "existential, and a non-string element is not a string match");
    }
  }

  @Test
  @DisplayName("the shape matches something, so agreement is not vacuous")
  void thePredicateSelectsARealSubset() throws Exception {
    final long drama = count(SHAPES.get(0), true);
    assertTrue(drama > 0 && drama < N,
               "the Drama predicate selected " + drama + " of " + N
                   + " — it must select a proper subset or it proves nothing");
    assertEquals(0L, count(SHAPES.get(1), true), "a genre no record carries must select nothing");
  }

  private long count(final String predicate, final boolean autoWire) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = autoWire
             ? SirixCompileChain.createWithJsonStore(store)
             : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return ((Int64) new Query(chain,
                                "count(for $m in jn:doc('" + DB + "','" + RES + "')[] where "
                                    + predicate + " return $m)").evaluate(ctx)).longValue();
    }
  }
}
