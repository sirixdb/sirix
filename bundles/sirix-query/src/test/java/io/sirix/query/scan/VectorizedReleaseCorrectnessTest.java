package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.compiler.optimizer.VectorizedExecutor;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Release gate: the generic evaluator owns missing-group, type-error and floating-point semantics.
 * Each fixture is evaluated before and after closing every store/session/executor. An SPI delegate
 * records actual execution, so agreement between two accidentally generic executions cannot pass.
 */
final class VectorizedReleaseCorrectnessTest {
  private static final String DB = "release-correctness";
  private static final String RESOURCE = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RESOURCE + "')";

  @TempDir
  Path directory;

  @BeforeEach
  @AfterEach
  void clearExecutors() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    SequentialPipelineStrategy.clearThreadVectorizedExecutor();
    ProjectionIndexRegistry.clear();
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"engineering\"", "17"})
  void nestedMissingGroupSurvivesReopen(final String key) throws Exception {
    final String document = "{\"rows\":[{\"dept\":" + key + "},{\"other\":1},{\"dept\":" + key + "}]}";
    final String query =
        "for $r in " + DOC + ".rows[] let $d := $r.dept group by $d " + "return {\"dept\":$d,\"count\":count($r)}";
    checkAcrossReopen(document, query, false, null, 4, -1, true);
  }

  @Test
  void nestedCompositeMissingGroupSurvivesReopen() throws Exception {
    final String document =
        "{\"rows\":[{\"dept\":\"Eng\",\"city\":\"A\"}," + "{\"city\":\"B\"},{\"dept\":\"Eng\",\"city\":\"B\"}]}";
    final String query = "for $r in " + DOC + ".rows[] let $d := $r.dept, $c := $r.city group by $d, $c "
        + "return {\"department\":$d,\"city\":$c,\"total\":count($r)}";
    checkAcrossReopen(document, query, false, null, 4, -1, true);
  }

  @ParameterizedTest
  @CsvSource({"min,false", "max,false", "min,true", "max,true"})
  void pureStringExtremaRemainAdmitted(final String function, final boolean predicated) throws Exception {
    checkAcrossReopen("[{\"selected\":true,\"value\":\"alpha\"}," + "{\"selected\":true,\"value\":\"omega\"}]",
        aggregate(function, predicated), false, null, 4);
  }

  @Test
  void emptyTopLevelGroupSourceIsKnown() throws Exception {
    final String query =
        "for $r in " + DOC + "[] let $d := $r.dept group by $d " + "return {\"dept\":$d,\"count\":count($r)}";
    checkAcrossReopen("[]", query, false, "", 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,null", "avg,null", "min,null", "max,null", "sum,true", "avg,true", "min,true", "max,true",
      "sum,\"invalid\"", "avg,\"invalid\"", "min,\"invalid\"", "max,\"invalid\""})
  void unfilteredMixedValuesPreserveGenericError(final String function, final String bad) throws Exception {
    checkAcrossReopen(mixedDocument(bad, true), aggregate(function, false), true, null, 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,30.75", "avg,15.375", "min,10.25", "max,20.5"})
  void exactDecimalAggregatesRemainAdmitted(final String function, final String expected) throws Exception {
    checkAcrossReopen("[{\"selected\":true,\"value\":10.25},{\"selected\":true,\"value\":20.5}]",
        aggregate(function, true), false, expected, 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,1", "avg,0.5", "min,0", "max,1"})
  void integerValuesSharingBooleanEncodingsRemainExact(final String function, final String expected) throws Exception {
    checkAcrossReopen("[{\"value\":0},{\"value\":1}]", aggregate(function, false), false, expected, 4);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sum", "avg", "min", "max"})
  void falseBetweenNegativeNumbersIsNotNumericZero(final String function) throws Exception {
    checkAcrossReopen("[{\"value\":-20},{\"value\":false},{\"value\":-10}]", aggregate(function, false), true, null, 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,string", "sum,null", "sum,boolean", "avg,string", "avg,null", "avg,boolean", "min,string",
      "min,null", "min,boolean", "max,string", "max,null", "max,boolean"})
  void matchingMixedValuesPreserveGenericError(final String function, final String kind) throws Exception {
    final String bad = switch (kind) {
      case "string" -> "\"not-a-number\"";
      case "null" -> "null";
      case "boolean" -> "true";
      default -> throw new IllegalArgumentException(kind);
    };
    checkAcrossReopen(mixedDocument(bad, true), aggregate(function, true), true, null, 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,30", "avg,15", "min,10", "max,20", "count,2"})
  void excludedNonNumericValueDoesNotCauseError(final String function, final String expected) throws Exception {
    checkAcrossReopen(mixedDocument("\"not-a-number\"", false), aggregate(function, true), false, expected, 4);
  }

  @Test
  void countIncludesMatchingNonNumericValues() throws Exception {
    checkAcrossReopen(mixedDocument("null", true), aggregate("count", true), false, "3", 4);
  }

  @ParameterizedTest
  @CsvSource({"sum,false,1", "sum,false,4", "sum,true,1", "sum,true,4", "avg,false,1", "avg,false,4", "avg,true,1",
      "avg,true,4"})
  void cancellationFollowsGenericFoldOrder(final String function, final boolean predicated, final int threads)
      throws Exception {
    // A left fold loses every small contribution between the cancelling large values.
    // Parallel middle chunks retain their small contributions, so merging changes the answer.
    final StringBuilder json = new StringBuilder(200_000).append('[');
    for (int i = 0; i < 4096; i++) {
      if (i != 0) {
        json.append(',');
      }
      final String value = i == 0
          ? "1.0e16"
          : i == 4095
              ? "-1.0e16"
              : "1.0e0";
      json.append("{\"selected\":true,\"value\":").append(value).append('}');
    }
    checkAcrossReopen(json.append(']').toString(), aggregate(function, predicated), false, null, threads, 4096);
  }

  @ParameterizedTest
  @CsvSource({"sum,1", "sum,1.00", "avg,1", "avg,1.00"})
  void mixedNumericLanesPreservePromotionOrder(final String function, final String middle) throws Exception {
    final String document = "[{\"selected\":true,\"value\":1.0e16},{\"selected\":true,\"value\":" + middle
        + "},{\"selected\":true,\"value\":-1.0e16}]";
    checkAcrossReopen(document, aggregate(function, true), false, null, 1, 2);
  }

  private static String mixedDocument(final String bad, final boolean selected) {
    return "[{\"selected\":true,\"value\":10},{\"selected\":true,\"value\":20}," + "{\"selected\":" + selected
        + ",\"value\":" + bad + "},{\"selected\":true}]";
  }

  private static String aggregate(final String function, final boolean predicated) {
    return function + "(for $r in " + DOC + "[]" + (predicated
        ? " where $r.selected"
        : "") + " return $r.value)";
  }

  private void checkAcrossReopen(final String document, final String query, final boolean expectError,
      final String expected, final int threads) throws Exception {
    checkAcrossReopen(document, query, expectError, expected, threads, -1, expectError);
  }

  private void checkAcrossReopen(final String document, final String query, final boolean expectError,
      final String expected, final int threads, final int expectedDoubleCount) throws Exception {
    checkAcrossReopen(document, query, expectError, expected, threads, expectedDoubleCount, true);
  }

  private void checkAcrossReopen(final String document, final String query, final boolean expectError,
      final String expected, final int threads, final int expectedDoubleCount, final boolean expectRefusal)
      throws Exception {
    final Outcome[] oracle = new Outcome[1];
    // Run the reopened comparison even while the warm regression is still red.
    assertAll(() -> {
      try (final BasicJsonDBStore store = newStore();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
        if (expectedDoubleCount >= 0) {
          store.create(DB, RESOURCE, JsonShredder.createStringReader(document));
          assertEquals(new Outcome(false, Integer.toString(expectedDoubleCount)),
              evaluate(chain, context, "count(for $r in " + DOC + "[] where $r.value instance of xs:double return $r)"),
              "the cancellation fixture must contain actual doubles, not exact decimals");
        } else {
          new Query(chain, "jn:store('" + DB + "','" + RESOURCE + "','" + document + "')").evaluate(context);
        }
        oracle[0] = evaluate(chain, context, query);
        check(store, context, query, expectError, expected, threads, "warm", expectRefusal, oracle[0]);
      }
    }, () -> {
      ProjectionIndexRegistry.clear();
      try (final BasicJsonDBStore store = newStore();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        // Reuse the immutable fixture's oracle: the fast execution must be the FIRST reader after
        // reopen, not benefit from caches warmed by another generic evaluation in this store.
        check(store, context, query, expectError, expected, threads, "reopened", expectRefusal, oracle[0]);
      }
    });
  }

  private void check(final BasicJsonDBStore store, final SirixQueryContext context, final String query,
      final boolean expectError, final String expected, final int threads, final String phase,
      final boolean expectRefusal, final Outcome generic) throws Exception {
    assertEquals(expectError, generic.error(), "generic oracle: " + phase + ": " + query);
    if (expected != null) {
      assertEquals(expected, generic.text(), "independent expected value");
    }
    if (!expectError && !"".equals(expected)) {
      assertFalse(generic.text().isEmpty(), "the fixture must contribute values");
    }

    final JsonDBCollection collection = store.lookup(DB);
    final JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE);
    final AtomicInteger executions = new AtomicInteger();
    final AtomicInteger refusals = new AtomicInteger();
    final SirixVectorizedExecutor executor =
        new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), threads);
    try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // Forward the entire SPI unchanged. Reflection is test instrumentation only, outside kernels.
      final VectorizedExecutor observed =
          (VectorizedExecutor) Proxy.newProxyInstance(VectorizedExecutor.class.getClassLoader(),
              new Class<?>[] {VectorizedExecutor.class}, (proxy, method, arguments) -> {
                if (method.getName().startsWith("execute")) {
                  executions.incrementAndGet();
                }
                try {
                  final Object result = method.invoke(executor, arguments);
                  // Source binding must retain instrumentation rather than hand the compiler the
                  // underlying executor, which would make actual execution invisible to this probe.
                  return result == executor
                      ? proxy
                      : result;
                } catch (final InvocationTargetException failure) {
                  if (failure.getCause() instanceof VectorizedRouteRefusedException) {
                    refusals.incrementAndGet();
                  }
                  throw failure.getCause();
                }
              });
      SequentialPipelineStrategy.setVectorizedExecutor(observed);
      final Outcome actual = evaluate(chain, context, query);
      // Field-count controls need not route; the regressions are the grouping/value aggregates.
      if (!query.startsWith("count(")) {
        assertTrue(executions.get() > 0, "fast path was never admitted: " + phase + ": " + query);
      }
      assertEquals(generic, actual, phase + ": " + query);
      assertEquals(expectRefusal, refusals.get() > 0, "route refusal: " + phase + ": " + query);
    } finally {
      SequentialPipelineStrategy.setVectorizedExecutor(null);
      executor.close();
    }

    // Also exercise the ordinary store-resolved executor provider, without instrumentation.
    try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      assertEquals(generic, evaluate(chain, context, query), "auto-wired: " + phase + ": " + query);
    }
  }

  private BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(directory).build();
  }

  private static Outcome evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String query) {
    try {
      final Sequence sequence = new Query(chain, query).execute(context);
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        new StringSerializer(writer).serialize(sequence);
      }
      // Group iteration order is unspecified; values, keys, multiplicity and error codes are not.
      final String[] rows = output.toString().replace("} {", "}\n{").split("\n");
      Arrays.sort(rows);
      return new Outcome(false, String.join("\n", rows).trim());
    } catch (final QueryException failure) {
      return new Outcome(true, failure.getCode().toString());
    } catch (final ClassCastException failure) {
      // Pinned Brackit can cast a boolean between numeric items to Numeric in sum/avg.
      // Preserve that observable failure too; the fast path must never replace it with a value.
      return new Outcome(true, failure.getClass().getName());
    }
  }

  private record Outcome(boolean error, String text) {
  }
}
