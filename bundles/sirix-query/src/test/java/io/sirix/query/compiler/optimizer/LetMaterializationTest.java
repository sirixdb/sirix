package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBObject;
import io.sirix.query.json.JsonDBStore;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class LetMaterializationTest {
  @ParameterizedTest
  @ValueSource(strings = {"current-dateTime()", "current-date()", "current-time()", "local:unknown($n)"})
  void unprovenCallsNeverMaterialize(final String expression) {
    final String text = "declare function local:unknown($n) {$n};" + " let $rows := (for $n in 1 to 3 return "
        + expression + ") return (count($rows),count($rows))";
    assertPlanAndAnswer(text, 0, "3 3");
  }

  @Test
  void effectfulLazyDependencyNeverMaterializes() {
    assertPlanAndAnswer(
        "declare function local:unknown($n) {$n};" + " let $source := (for $n in 1 to 3 return local:unknown($n))"
            + " let $rows := (for $n in $source return $n) return (count($rows),count($rows))",
        0, "3 3");
  }

  @Test
  void functionParametersDoNotProveTheirCallersLazySourcesPure() {
    assertPlanAndAnswer(
        "declare function local:use($input) {"
            + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))};" + "local:use(1 to 3)",
        0, "3 3");
  }

  static Stream<Arguments> parameterPaths() {
    return Stream.of("direct", "let-alias", "for-alias")
        .flatMap(shape -> Stream.of(Arguments.of(shape, false, false), Arguments.of(shape, true, false),
            Arguments.of(shape, true, true)));
  }

  @ParameterizedTest
  @MethodSource("parameterPaths")
  void functionParametersNeverUseSameNamedOuterPurity(final String shape, final boolean inline,
      final boolean outerLocal) {
    final AtomicInteger iterations = new AtomicInteger();
    final boolean composite = shape.equals("for-alias");
    final Sequence input = composite
        ? new ArrayObject(new QNm[] {new QNm("value")}, new Sequence[] {increasing(iterations)})
        : increasing(iterations);
    final String body = switch (shape) {
      case "direct" -> "let $rows := (for $n in $c return $n) return (sum($rows),sum($rows))";
      case "let-alias" -> "let $alias := $c let $rows := (for $n in $alias return $n)"
          + " return (sum($rows),sum($rows))";
      case "for-alias" -> "for $alias in $c let $rows := (for $n in 1 to 2 return $alias.value)"
          + " return (sum($rows),sum($rows))";
      default -> throw new IllegalArgumentException(shape);
    };
    final String text = "declare variable $c := 0; declare variable $input external; " + (inline
        ? (outerLocal
            ? "let $c := 0 "
            : "") + "let $f := function($c) {" + body + "} return $f($input)"
        : "declare function local:f($c) {" + body + "}; local:f($input)");
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, text);
      context.bind(new QNm("input"), input);
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals(composite
          ? "3 7"
          : "1 2", output.toString().trim());
      assertEquals(composite
          ? 4
          : 2, iterations.get());
      assertEquals(0, markers(chain.getOptimizedAST()));
    }
  }

  @Test
  void localBindingStillShadowsAFunctionParameterAndGlobal() {
    assertPlanAndAnswer("declare variable $c := 0; declare function local:f($c) {"
        + "let $c := 2 let $rows := (for $n in 1 to 2 return $c)"
        + " return (sum($rows),sum($rows))}; local:f(9)", 1, "4 4");
  }

  @Test
  void parameterReferencesDoNotEagerlyEvaluateAnUnusedOuterLet() {
    assertPlanAndAnswer("let $c := (for $n in 1 to 2 return $n div 0)"
        + " let $f := function($c) {($c,$c)} return $f(1)", 0, "1 1");
  }

  static Stream<Arguments> capturedPaths() {
    return Stream.of("let", "for", "let-alias", "for-alias", "global", "global-alias", "global-field")
        .flatMap(scope -> Stream.of(false, true)
            .flatMap(array -> (array
                ? Stream.of(false)
                : Stream.of(false, true)).map(stored -> Arguments.of(scope, array, stored))));
  }

  @ParameterizedTest
  @MethodSource("capturedPaths")
  void escapedCapturesKeepEvaluationPerReference(final String scope, final boolean array, final boolean stored,
      @TempDir final Path directory) {
    final AtomicInteger iterations = new AtomicInteger();
    final String literal = array
        ? "[0]"
        : "{\"value\":0}";
    final String source = stored
        ? "jn:doc('input','rows')"
        : array && scope.startsWith("for")
            ? "[" + literal + "]"
            : literal;
    final String field = array
        ? "[0]"
        : ".value";
    final String dependency = scope.endsWith("alias")
        ? "$alias" + field
        : scope.equals("global-field")
            ? "$alias"
            : "$a" + field;
    final String rows = "(let $rows := (for $n in 1 to 2 return " + dependency
        + ") return (sum($rows),sum($rows)))";
    final String text = switch (scope) {
      case "let", "for" -> scope + " $a " + (scope.equals("let") ? ":= " : "in ") + source
          + " return ($a," + rows + ")";
      case "let-alias", "for-alias" -> (scope.startsWith("let") ? "let $a := " : "for $a in ") + source
          + " let $alias := $a return ($a," + rows + ")";
      case "global" -> "declare variable $a := " + source + "; ($a," + rows + ")";
      case "global-alias", "global-field" -> "declare variable $a := " + source
          + "; declare variable $alias := $a" + (scope.equals("global-field") ? field : "")
          + "; ($a," + rows + ")";
      default -> throw new IllegalArgumentException(scope);
    };
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      if (stored)
        store.create("input", "rows", literal);
      final Query query = new Query(chain, text);
      try (final Iter result = query.execute(context).iterate()) {
        final Item first = result.next();
        if (array)
          assertInstanceOf(Array.class, first).replaceAt(0, increasing(iterations));
        else
          assertInstanceOf(Object.class, first).replace(new QNm("value"), increasing(iterations));
        iterations.set(0);
        assertEquals("3", result.next().toString());
        assertEquals("7", result.next().toString());
        assertEquals(4, iterations.get());
        assertNull(result.next());
      }
    }
  }

  static Stream<Arguments> escapedResultPaths() {
    return Stream.of("object", "array", "stored")
        .flatMap(producer -> Stream.of("direct", "alias", "object-field", "function", "prefix", "repeated-prefix")
            .flatMap(consumer -> Stream.of(1, 2)
                .flatMap(rows -> Stream.of(false, true)
                    .map(enabled -> Arguments.of(producer, consumer, rows, enabled)))));
  }

  @ParameterizedTest
  @MethodSource("escapedResultPaths")
  void escapedResultsAreFreshForLaterReferences(final String producer, final String consumer, final int rows,
      final boolean enabled, @TempDir final Path directory) {
    withMaterialization(enabled, () -> {
      final boolean array = producer.equals("array");
      final String source = switch (producer) {
        case "object" -> "{\"value\":0}";
        case "array" -> "[0]";
        case "stored" -> "jn:doc('input','rows')";
        default -> throw new IllegalArgumentException(producer);
      };
      final String sum = "sum($rows" + (array ? "[0]" : ".value") + ")";
      final String result = switch (consumer) {
        case "direct" -> "return ($rows," + sum + "," + sum + ")";
        case "alias" -> "let $alias := $rows return ($alias," + sum + "," + sum + ")";
        case "object-field" -> "return ({\"row\":subsequence($rows,1,1)}," + sum + "," + sum + ")";
        case "function" -> "let $f := function() {" + sum + "} return ($rows,$f(),$f())";
        case "prefix" -> "return (subsequence($rows,1,1)," + sum + "," + sum + ")";
        case "repeated-prefix" -> "return (subsequence($rows,1,1),subsequence($rows,1,1))";
        default -> throw new IllegalArgumentException(consumer);
      };
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        if (producer.equals("stored"))
          store.create("input", "rows", "{\"value\":0}");
        final Query query = new Query(chain, "let $rows := (for $n in 1 to " + rows + " return " + source
            + ") " + result);
        try (final Iter output = query.execute(context).iterate()) {
          final Item first = output.next();
          final Item escaped = consumer.equals("object-field")
              ? assertInstanceOf(Item.class, assertInstanceOf(Object.class, first).get(new QNm("row")))
              : first;
          if (array)
            assertInstanceOf(Array.class, escaped).replaceAt(0, Int32.ONE);
          else
            assertInstanceOf(Object.class, escaped).replace(new QNm("value"), Int32.ONE);
          if (consumer.equals("repeated-prefix")) {
            final Item next = output.next();
            assertEquals(Int32.ZERO, array
                ? assertInstanceOf(Array.class, next).at(0)
                : assertInstanceOf(Object.class, next).get(new QNm("value")));
          } else {
            if (!consumer.equals("object-field") && !consumer.equals("prefix")) {
              for (int i = 1; i < rows; i++) {
                final Item next = output.next();
                assertEquals(Int32.ZERO, array
                    ? assertInstanceOf(Array.class, next).at(0)
                    : assertInstanceOf(Object.class, next).get(new QNm("value")));
              }
            }
            assertEquals("0", output.next().toString());
            assertEquals("0", output.next().toString());
          }
          assertNull(output.next());
        }
        assertEquals(0, markers(chain.getOptimizedAST()));
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aLongerPrefixPreservesInterleavingWithinItsProducer(final boolean enabled) {
    withMaterialization(enabled, () -> {
      try (final SirixCompileChain chain = SirixCompileChain.create();
          final SirixQueryContext context = SirixQueryContext.create()) {
        final Query query = new Query(chain, "let $rows := (let $a := {\"value\":0} for $n in 1 to 2"
            + " return if ($n eq 1) then $a else {\"value\":$a.value})"
            + " return (subsequence($rows,1,2),count($rows),count($rows))");
        try (final Iter output = query.execute(context).iterate()) {
          assertInstanceOf(Object.class, output.next()).replace(new QNm("value"), Int32.ONE);
          assertEquals(Int32.ONE, assertInstanceOf(Object.class, output.next()).get(new QNm("value")));
          assertEquals("2", output.next().toString());
          assertEquals("2", output.next().toString());
          assertNull(output.next());
        }
        assertEquals(0, markers(chain.getOptimizedAST()));
      }
    });
  }

  static Stream<Arguments> globalUsePaths() {
    return Stream.of(false, true)
        .flatMap(alias -> Stream.of(false, true)
            .flatMap(prefix -> Stream.of(false, true).map(enabled -> Arguments.of(alias, prefix, enabled))));
  }

  @ParameterizedTest
  @MethodSource("globalUsePaths")
  void globalsAreReadAgainAfterCallerInterleaving(final boolean alias, final boolean prefix,
      final boolean enabled) {
    withMaterialization(enabled, () -> {
      final AtomicInteger iterations = new AtomicInteger();
      final String text = "declare variable $c := 0; " + (alias
          ? "declare variable $d := (for $n in 1 to 3 return if ($n le 2) then $c else ()); "
          : "") + "let $rows := (for $n in 1 to 2 return " + (alias ? "$d" : "$c")
          + ") return (" + (prefix ? "0," : "") + "sum($rows),sum($rows))";
      try (final SirixCompileChain chain = SirixCompileChain.create();
          final SirixQueryContext context = SirixQueryContext.create()) {
        final Query query = new Query(chain, text);
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals("0", output.next().toString());
          context.bind(new QNm("c"), increasing(iterations));
          assertEquals(alias ? "10" : "3", output.next().toString());
          if (prefix)
            assertEquals(alias ? "26" : "7", output.next().toString());
          assertNull(output.next());
          assertEquals((alias ? 4 : 2) * (prefix ? 2 : 1), iterations.get());
        }
        assertEquals(0, markers(chain.getOptimizedAST()));
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void deferredFunctionsKeepTheirGlobalReads(final boolean enabled) {
    withMaterialization(enabled, () -> {
      final AtomicInteger iterations = new AtomicInteger();
      try (final SirixCompileChain chain = SirixCompileChain.create();
          final SirixQueryContext context = SirixQueryContext.create()) {
        final Query query = new Query(chain, "declare variable $c := 0;"
            + " let $rows := (for $n in 1 to 2 return $c)"
            + " let $f := function() {(sum($rows),sum($rows))} return (0,$f())");
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals("0", output.next().toString());
          context.bind(new QNm("c"), increasing(iterations));
          assertEquals("3", output.next().toString());
          assertEquals("7", output.next().toString());
          assertNull(output.next());
          assertEquals(4, iterations.get());
        }
        assertEquals(0, markers(chain.getOptimizedAST()));
      }
    });
  }

  static Stream<Arguments> revisionReadPaths() {
    return Stream.of("doc", "latest", "optional", "option", "temporal", "collection", "pinned", "future")
        .flatMap(read -> (read.equals("doc")
            ? Stream.of("direct", "nested-for", "inner-let", "outer-let")
            : Stream.of("direct"))
            .flatMap(producer -> (read.equals("future")
                ? Stream.of("zero")
                : Stream.of("zero", "sum", "value"))
                .flatMap(prefix -> Stream.of(false, true)
                    .map(enabled -> Arguments.of(read, producer, prefix, enabled)))));
  }

  @ParameterizedTest
  @MethodSource("revisionReadPaths")
  void storedReadsRespectCommitsBetweenResultItems(final String read, final String producer, final String prefix,
      final boolean enabled, @TempDir final Path directory) {
    withMaterialization(enabled, () -> {
      final String open = switch (read) {
        case "doc" -> "jn:doc('input','rows')";
        case "latest" -> "jn:doc('input','rows',-1)";
        case "optional" -> "jn:doc('input','rows',())";
        case "option" -> "jn:doc('input','rows',(),true())";
        case "temporal" -> "jn:open('input','rows',xs:dateTime('2100-01-01T00:00:00Z'))";
        case "collection" -> "jn:collection('input')";
        case "pinned" -> "jn:doc('input','rows',1)";
        case "future" -> "jn:doc('input','rows',2)";
        default -> throw new IllegalArgumentException(read);
      };
      final String scan = "for $n in " + open + "[] return $n";
      final String binding = switch (producer) {
        case "direct" -> "let $rows := (" + scan + ")";
        case "nested-for" -> "let $rows := (for $source in (" + scan + ") return $source)";
        case "inner-let" -> "let $rows := (let $source := (" + scan + ") for $n in $source return $n)";
        case "outer-let" -> "let $source := (" + scan + ") let $rows := (for $n in $source return $n)";
        default -> throw new IllegalArgumentException(producer);
      };
      final String result = switch (prefix) {
        case "zero" -> "(0,sum($rows),sum($rows))";
        case "sum" -> "(sum($rows),sum($rows))";
        case "value" -> "(subsequence($rows,1,1),sum($rows),sum($rows))";
        default -> throw new IllegalArgumentException(prefix);
      };
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = store.create("input", "rows", "[1]");
        final Query query = new Query(chain, binding + " return " + result);
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals(prefix.equals("zero") ? "0" : "1", output.next().toString());
          commitNumber(collection, 2);
          final String expected = read.equals("pinned") ? "1" : "2";
          assertEquals(expected, output.next().toString());
          if (!prefix.equals("sum"))
            assertEquals(expected, output.next().toString());
          assertNull(output.next());
        }
        assertEquals(0, markers(chain.getOptimizedAST()));
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void eagerStoredReadBindingsOpenEachTuplesCurrentRevision(final boolean enabled, @TempDir final Path directory) {
    withMaterialization(enabled, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = store.create("input", "rows", "[1]");
        final Query query = new Query(chain, "for $outer in 1 to 2"
            + " let $rows := (for $n in jn:doc('input','rows')[] return $n)"
            + " return {\"sum\":sum($rows),\"count\":count($rows)}");
        try (final Iter output = query.execute(context).iterate()) {
          final Object first = assertInstanceOf(Object.class, output.next());
          assertEquals(Int32.ONE, first.get(new QNm("sum")));
          commitNumber(collection, 2);
          final Object second = assertInstanceOf(Object.class, output.next());
          assertEquals(new Int32(2), second.get(new QNm("sum")));
          assertEquals(Int32.ONE, second.get(new QNm("count")));
          assertNull(output.next());
        }
        assertEquals(enabled ? 1 : 0, markers(chain.getOptimizedAST()));
      }
    });
  }

  static Stream<Arguments> outerTuplePaths() {
    return Stream.of(false, true)
        .flatMap(retained -> Stream.of(false, true).map(enabled -> Arguments.of(retained, enabled)));
  }

  @ParameterizedTest
  @MethodSource("outerTuplePaths")
  void globalRebindingRespectsTheBindingTuple(final boolean retained, final boolean enabled) {
    withMaterialization(enabled, () -> {
      final AtomicInteger iterations = new AtomicInteger();
      final String binding = "let $rows := (for $n in 1 to 2 return $c) ";
      final String outer = "for $outer in 1 to 2 ";
      final String text = "declare variable $c := 0; " + (retained ? binding + outer : outer + binding)
          + "return {\"total\":sum($rows),\"count\":count($rows)}";
      try (final SirixCompileChain chain = SirixCompileChain.create();
          final SirixQueryContext context = SirixQueryContext.create()) {
        final Query query = new Query(chain, text);
        try (final Iter output = query.execute(context).iterate()) {
          final Object first = assertInstanceOf(Object.class, output.next());
          assertEquals(Int32.ZERO, first.get(new QNm("total")));
          context.bind(new QNm("c"), increasing(iterations));
          final Object second = assertInstanceOf(Object.class, output.next());
          assertEquals(new Int32(3), second.get(new QNm("total")));
          assertEquals(new Int32(2), second.get(new QNm("count")));
          assertEquals(4, iterations.get());
          assertNull(output.next());
        }
        assertEquals(enabled && !retained ? 1 : 0, markers(chain.getOptimizedAST()));
      }
    });
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  void nonEscapingArrayResultsRetainTheirSequenceCardinality(final int rows) {
    assertPlanAndAnswer("let $rows := (for $n in 1 to " + rows
        + " return [1,2]) return (count($rows),sum($rows[0]))", 1, rows + " " + rows);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void eagerResultFieldsCannotHideOpaqueCallbacks(final boolean enabled) {
    withMaterialization(enabled, () -> {
      final AtomicInteger iterations = new AtomicInteger();
      try (final SirixCompileChain chain = SirixCompileChain.create();
          final SirixQueryContext context = SirixQueryContext.create()) {
        final Query query = new Query(chain, "declare variable $c := 0; let $a := {\"value\":0}"
            + " return ($a,(let $rows := (for $n in 1 to 2 return $c)"
            + " return {\"effect\":$a.value,\"total\":sum($rows),\"count\":count($rows)}))");
        try (final Iter output = query.execute(context).iterate()) {
          final Object first = assertInstanceOf(Object.class, output.next());
          first.replace(new QNm("value"), new LazySequence() {
            @Override
            public Iter iterate() {
              context.bind(new QNm("c"), increasing(iterations));
              return Int32.ZERO.iterate();
            }
          });
          final Object second = assertInstanceOf(Object.class, output.next());
          assertEquals(Int32.ZERO, second.get(new QNm("effect")));
          assertEquals(new Int32(3), second.get(new QNm("total")));
          assertEquals(new Int32(2), second.get(new QNm("count")));
          assertEquals(4, iterations.get());
          assertNull(output.next());
        }
      }
    });
  }

  @Test
  void effectfulGlobalInitializersDoNotProveLazyDependenciesPure() {
    assertPlanAndAnswer("declare variable $input := (for $n in 1 to 3 return current-dateTime());"
        + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void externalSequencesHaveNoStaticPurityProof() {
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, "declare variable $input external;"
          + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))");
      context.bind(new QNm("input"), Int32.ONE);
      assertEquals(0, markers(chain.getOptimizedAST()));
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals("1 1", output.toString().trim());
    }
  }

  @Test
  void positionalAllowingEmptyBindingsStillProveTheirActualSource() {
    assertPlanAndAnswer(
        "declare function local:source() { [{\"v\":1}][] };" + "for $r allowing empty at $position in local:source()"
            + " let $rows := (for $n in 1 to 2 return $r.v) return (sum($rows),count($rows))",
        0, "2 2");
  }

  @Test
  void allowingEmptyRebindingKeepsAnEmptyOuterTuple() {
    assertPlanAndAnswer("for $outer allowing empty in ()"
        + " let $rows := (for $n in 1 to 2 return $outer) return (count($rows),count($rows))", 1, "0 0");
  }

  @Test
  void externalDefaultsDoNotProveCallerBindingsPure() {
    assertPlanAndAnswer("declare variable $input external := 1 to 3;"
        + "let $rows := (for $n in $input return $n) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void callerOverrideOfADeclaredGlobalSkipsMaterializationAtTheBinding() {
    final AtomicInteger iterations = new AtomicInteger();
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, "declare variable $input := 1 to 2;"
          + "let $rows := (for $n in $input return $n)"
          + " return {\"first\":count($rows),\"second\":count($rows)}");
      assertEquals(1, markers(chain.getOptimizedAST()), "the default initializer is statically pure");
      context.bind(new QNm("input"), new LazySequence() {
        @Override
        public Iter iterate() {
          iterations.incrementAndGet();
          return new BaseIter() {
            private int next = 1;

            @Override
            public Item next() {
              return next <= 2
                  ? new Int32(next++)
                  : null;
            }

            @Override
            public void close() {}
          };
        }
      });
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals("{\"first\":2,\"second\":2}", output.toString().trim());
      assertEquals(2, iterations.get(), "a caller's unproven lazy value retains evaluation per reference");
    }
  }

  @Test
  void userFunctionShadowingAReadFunctionStillHasNoPurityProof() {
    assertPlanAndAnswer(
        "declare function jn:doc($collection,$resource) { 1 to 3 };"
            + "let $rows := (for $n in jn:doc('unused','unused') return $n)" + " return (count($rows),count($rows))",
        0, "3 3");
  }

  @Test
  void implicitContextArgumentsHaveNoPurityProof() {
    assertPlanAndAnswer("declare context item := 'value';"
        + "let $rows := (for $n in 1 to 3 return string()) return (count($rows),count($rows))", 0, "3 3");
  }

  @Test
  void customProvidersRetainPerReferenceReads(@TempDir final Path directory) {
    final AtomicInteger lookups = new AtomicInteger();
    try (final BasicJsonDBStore actual = BasicJsonDBStore.newBuilder().location(directory).build()) {
      actual.create("input", "rows", "[1,2,3]");
      final JsonDBStore custom = mock(JsonDBStore.class, withSettings().stubOnly().defaultAnswer(invocation -> {
        if (invocation.getMethod().getName().equals("lookup"))
          lookups.incrementAndGet();
        try {
          return invocation.getMethod().invoke(actual, invocation.getArguments());
        } catch (final InvocationTargetException exception) {
          throw exception.getCause();
        }
      }));
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(custom);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(custom)) {
        final Query query = new Query(chain,
            "let $rows := (for $n in jn:doc('input','rows')[] return $n)" + " return [count($rows),sum($rows)][]");
        assertEquals(1, markers(chain.getOptimizedAST()));
        lookups.set(0);
        final StringWriter output = new StringWriter();
        try (final PrintWriter writer = new PrintWriter(output)) {
          query.serialize(context, writer);
        }
        assertEquals("3 6", output.toString().trim());
        assertEquals(2, lookups.get(), "the purity of a custom provider's reads has not been established");
      }
    }
  }

  static Stream<Arguments> registeredCollectionPaths() {
    return Stream.of(false, true)
        .flatMap(enabled -> Stream.of(false, true)
            .flatMap(afterCompilation -> Stream.of(false, true)
                .map(revision -> Arguments.of(enabled, afterCompilation, revision))));
  }

  @ParameterizedTest
  @MethodSource("registeredCollectionPaths")
  void registeredCollectionsRetainChangingFieldsPerReference(final boolean enabled, final boolean afterCompilation,
      final boolean revision, @TempDir final Path directory) {
    withMaterialization(enabled, () -> {
      final AtomicInteger iterations = new AtomicInteger();
      final AtomicInteger documents = new AtomicInteger();
      final AtomicInteger fields = new AtomicInteger();
      final AtomicInteger inspections = new AtomicInteger();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build()) {
        final JsonDBCollection actual = store.create("input", "rows", "{\"value\":0}");
        final JsonDBObject object = (JsonDBObject) actual.getDocument("rows");
        object.replace(new QNm("value"), increasing(iterations));
        final JsonDBObject counted = mock(JsonDBObject.class, withSettings().stubOnly().defaultAnswer(invocation -> {
          final String method = invocation.getMethod().getName();
          if (method.equals("iterate"))
            return new ItemSequence((Item) invocation.getMock()).iterate();
          if (method.equals("evaluate") || method.equals("evaluateToItem"))
            return invocation.getMock();
          if (method.equals("get"))
            fields.incrementAndGet();
          else if (method.equals("values") || method.equals("names") || method.equals("len") || method.equals("length"))
            inspections.incrementAndGet();
          try {
            return invocation.getMethod().invoke(object, invocation.getArguments());
          } catch (final InvocationTargetException exception) {
            throw exception.getCause();
          }
        }));
        final JsonDBCollection custom = mock(JsonDBCollection.class,
            withSettings().stubOnly().defaultAnswer(invocation -> {
              if (invocation.getMethod().getName().equals("getDocument")) {
                documents.incrementAndGet();
                return counted;
              }
              try {
                return invocation.getMethod().invoke(actual, invocation.getArguments());
              } catch (final InvocationTargetException exception) {
                throw exception.getCause();
              }
            }));
        if (!afterCompilation)
          store.addDatabase(custom, actual.getDatabase());
        try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
          final Query query = new Query(chain,
              "let $rows := (for $n in 1 to 2 return jn:doc('input','rows'" + (revision
                  ? ",1"
                  : "") + ").value) return {\"first\":sum($rows),\"second\":sum($rows)}");
          assertEquals(enabled
              ? 1
              : 0, markers(chain.getOptimizedAST()));
          if (afterCompilation)
            store.addDatabase(custom, actual.getDatabase());
          for (int evaluation = 0; evaluation < 2; evaluation++) {
            iterations.set(0);
            documents.set(0);
            fields.set(0);
            inspections.set(0);
            final StringWriter output = new StringWriter();
            try (final PrintWriter writer = new PrintWriter(output)) {
              query.serialize(context, writer);
            }
            assertEquals("{\"first\":3,\"second\":7}", output.toString().trim());
            assertEquals(4, iterations.get(), "both references retain the changing lazy field");
            assertEquals(4, documents.get(), "the guard must not open custom documents");
            assertEquals(4, fields.get(), "the guard must not read custom fields");
            assertEquals(0, inspections.get(), "the guard must not inspect custom containers");
          }
        }
      }
    });
  }

  @Test
  void singleReferenceAndUnusedBindingStayLazy() {
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n) return count($rows)", 0, "3");
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n div 0) return 7", 0, "7");
  }

  @Test
  void shadowedNamesReferToTheirOwnBinding() {
    assertPlanAndAnswer("let $rows := (for $n in 1 to 3 return $n)"
        + " return (count($rows), (let $rows := (for $n in 1 to 2 return $n)" + " return (sum($rows),count($rows))))",
        1, "3 3 2");
  }

  @Test
  void updatingInitializerIsRejected() {
    // Exercise the optimizer with the actual parsed and analyzed updating expression.
    try (final SirixCompileChain chain = SirixCompileChain.create()) {
      new Query(chain,
          "let $rows := (for $n in 1 to 2 return jn:store('x','r','{}'))" + " return (count($rows),count($rows))");
      assertEquals(0, markers(chain.getOptimizedAST()));
    }
  }

  private static void assertPlanAndAnswer(final String text, final int markers, final String expected) {
    try (final SirixCompileChain chain = SirixCompileChain.create();
        final SirixQueryContext context = SirixQueryContext.create()) {
      final Query query = new Query(chain, text);
      assertEquals(markers, markers(chain.getOptimizedAST()));
      final StringWriter output = new StringWriter();
      try (final PrintWriter writer = new PrintWriter(output)) {
        query.serialize(context, writer);
      }
      assertEquals(expected, output.toString().trim());
    }
  }

  private static Sequence increasing(final AtomicInteger iterations) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Item value = new Int32(iterations.incrementAndGet());
        return new BaseIter() {
          private boolean emitted;

          @Override
          public Item next() {
            if (emitted)
              return null;
            emitted = true;
            return value;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private static void withMaterialization(final boolean enabled, final Runnable action) {
    final String property = LetMaterializationStage.ENABLED_PROPERTY;
    final String previous = System.getProperty(property);
    System.setProperty(property, Boolean.toString(enabled));
    try {
      action.run();
    } finally {
      if (previous == null)
        System.clearProperty(property);
      else
        System.setProperty(property, previous);
    }
  }

  private static void commitNumber(final JsonDBCollection collection, final int value) {
    final JsonResourceSession resource = collection.getDatabase().beginResourceSession("rows");
    final int previousRevision = resource.getMostRecentRevisionNumber();
    try (final JsonNodeTrx writer = resource.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      assertTrue(writer.moveToFirstChild());
      assertTrue(writer.moveToFirstChild());
      writer.setNumberValue(value);
      writer.commit();
    }
    assertEquals(previousRevision + 1, resource.getMostRecentRevisionNumber());
  }

  private static int markers(final AST node) {
    int count = Boolean.TRUE.equals(node.getProperty("SIRIX_MATERIALIZE_LET"))
        ? 1
        : 0;
    for (int i = 0; i < node.getChildCount(); i++)
      count += markers(node.getChild(i));
    return count;
  }
}
