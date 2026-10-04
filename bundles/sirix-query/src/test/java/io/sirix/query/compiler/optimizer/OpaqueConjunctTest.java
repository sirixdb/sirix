package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBObject;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class OpaqueConjunctTest {
  @TempDir
  Path directory;

  private enum Shape {
    SCALAR, OBJECT, NESTED, ARRAY, MEMBER, SEQUENCE, ITEM_SEQUENCE, CONTEXT
  }

  private record Case(String prolog, String prefix, String value, Shape shape, int fields, int members) {
  }

  static Stream<Arguments> inputsAndSwitches() {
    return Stream.of(new Case("declare variable $c external;", "", "$c", Shape.SCALAR, 0, 0),
        new Case("declare variable $c external := 1;", "", "$c", Shape.SCALAR, 0, 0),
        new Case("declare variable $c := 1;", "", "$c", Shape.SCALAR, 0, 0),
        new Case("declare variable $c external; declare variable $a := $c; declare variable $b := $a;", "", "$b",
            Shape.SCALAR, 0, 0),
        new Case("declare variable $c external := 1;", "let $a := $c let $b := $a return", "$b", Shape.SCALAR, 0, 0),
        new Case("declare variable $input external;", "", "$input.value", Shape.OBJECT, 2, 0),
        new Case("declare variable $input external;", "let $a := $input let $b := $a return", "$b.value", Shape.OBJECT,
            2, 0),
        new Case("declare variable $input external;", "let $a := $input.value let $b := $a return", "$b", Shape.OBJECT,
            1, 0),
        new Case("declare variable $input external;", "", "$input.child.value", Shape.NESTED, 4, 0),
        new Case("declare variable $input external;", "", "$input.child[0].value", Shape.MEMBER, 4, 2),
        new Case("declare variable $input external;", "for $a in $input[] let $b := $a return", "$b.value", Shape.ARRAY,
            2, 1),
        new Case("declare variable $input external;", "for $a in $input let $b := $a return", "$b.value",
            Shape.SEQUENCE, 2, 1),
        new Case("declare variable $input external;", "for $a in $input let $b := $a return", "$b.value",
            Shape.ITEM_SEQUENCE, 2, 0),
        new Case("", "", "$$.value", Shape.CONTEXT, 2, 0),
        new Case("", "let $a := $$ let $b := $a return", "$b.value", Shape.CONTEXT, 2, 0))
                 .flatMap(input -> Stream.of(false, true).map(cheap -> Arguments.of(input, cheap)));
  }

  @ParameterizedTest
  @MethodSource("inputsAndSwitches")
  void opaqueValuesKeepTheirOriginalTwoReads(final Case input, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final Sequence source = source(input.shape, reads);
        if (input.shape == Shape.CONTEXT) {
          context.setContextItem((Item) source);
        } else {
          context.bind(new QNm(input.shape == Shape.SCALAR
              ? "c"
              : "input"), source);
        }
        final String query =
            input.prolog + " " + input.prefix + " xs:integer(" + input.value + ") gt 0 and " + input.value + " eq 2";
        assertEquals("true", serialize(chain, context, query));
        assertEquals(2, reads.scalar);
        assertEquals(input.fields, reads.fields);
        assertEquals(input.members, reads.members);
        assertEquals(0, reads.inspections);
        if (cheap) {
          assertTrue(guarded(chain.getOptimizedAST()) > 0);
        }
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void provenAtomicOverridesKeepTheSavedCast(final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      final Str input = new Str("0") {
        @Override
        public String stringValue() {
          reads.scalar++;
          return super.stringValue();
        }
      };
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.bind(new QNm("c"), input);
        assertEquals("false",
            serialize(chain, context, "declare variable $c external := '0'; xs:integer($c) gt 0 and $c eq '1'"));
        assertEquals(cheap
            ? 0
            : 1, reads.scalar);
        assertEquals(cheap
            ? 1
            : 0, guarded(chain.getOptimizedAST()));
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void prefixConsumptionStopsAfterTheFirstExactConjunction(final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.bind(new QNm("c"), increasing(reads));
        assertEquals("true", serialize(chain, context, "declare variable $c external := 1;"
            + " exists(for $n in 1 to 1000 where xs:integer($c) ge $n and $c eq ($n + 1) return $n)"));
        assertEquals(2, reads.scalar);
      }
    });
  }

  @Test
  void reusedPlanChecksTheCurrentOverride() {
    withSwitch(true, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final Query query = new Query(chain, "declare variable $c external := 1; xs:integer($c) gt 0 and $c eq 2");
        context.bind(new QNm("c"), Int32.ZERO);
        assertEquals("false", serialize(query, context));
        context.bind(new QNm("c"), increasing(reads));
        assertEquals("true", serialize(query, context));
        assertEquals(2, reads.scalar);
        assertEquals(1, guarded(chain.getOptimizedAST()));
      }
    });
  }

  @Test
  void localBindingShadowsAnOpaqueExternalWithoutReadingIt() {
    withSwitch(true, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.bind(new QNm("c"), increasing(reads));
        assertEquals("false", serialize(chain, context,
            "declare variable $c external := 1;" + " let $c := 0 return xs:integer($c) gt 0 and $c eq 2"));
        assertEquals(0, reads.scalar);
        assertEquals(1, guarded(chain.getOptimizedAST()));
      }
    });
  }

  @Test
  void admissionDoesNotReadUnusedFieldsOrUnboundDefaults() {
    final Reads reads = new Reads();
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
      context.bind(new QNm("input"), object("value", increasing(reads), reads));
      assertEquals("false",
          serialize(chain, context,
              "declare variable $input external;" + " declare variable $unused := xs:integer('invalid');"
                  + " 0 eq 1 and xs:integer($input.value) gt 0 and $unused eq 2"));
      assertEquals(0, reads.scalar);
      assertEquals(0, reads.fields);
      assertEquals(0, reads.inspections);
    }
  }

  static Stream<Arguments> parameterPaths() {
    return Stream.of("scalar", "object", "alias", "local-shadow", "inline")
                 .flatMap(shape -> Stream.of(false, true).map(cheap -> Arguments.of(shape, cheap)));
  }

  @ParameterizedTest
  @MethodSource("parameterPaths")
  void functionParametersUseTheirTupleValuesInsteadOfGlobalDefaults(final String shape, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.bind(new QNm("input"), shape.equals("scalar")
            ? increasing(reads)
            : object("value", increasing(reads), reads));
        final String value = shape.equals("scalar")
            ? "$c"
            : shape.equals("alias")
                ? "$b.value"
                : "$c.value";
        final String predicate = "xs:integer(" + value + ") gt 0 and " + value + " eq 2";
        final String body = shape.equals("alias")
            ? "let $b := $c return " + predicate
            : shape.equals("local-shadow")
                ? "let $input := 0 return " + predicate
                : predicate;
        final String text = "declare variable $input external; declare variable $c := 0; " + (shape.equals("inline")
            ? "let $f := function($c) { " + body + " } return $f($input)"
            : "declare function local:f($c) { " + body + " }; local:f($input)");
        assertEquals("true", serialize(chain, context, text));
        assertEquals(2, reads.scalar);
        assertEquals(shape.equals("scalar")
            ? 0
            : 2, reads.fields);
        assertEquals(0, reads.inspections);
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void callerSuppliedStoredViewsKeepTheirOpaqueMemoReads(final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBObject object = (JsonDBObject) store.create("data", "rows", "{\"value\":0}").getDocument("rows");
        object.replace(new QNm("value"), increasing(reads));
        reads.scalar = 0;
        context.bind(new QNm("input"), object);
        assertEquals("true", serialize(chain, context,
            "declare variable $input external;" + "xs:integer($input.value) gt 0 and $input.value eq 2"));
        assertEquals(2, reads.scalar);
        final Query captured = new Query(chain, "declare variable $input external; for $a in $input"
            + " return (1, xs:integer($a.value) gt 0 and $a.value eq 2)");
        try (final Iter result = captured.execute(context).iterate()) {
          assertEquals("1", result.next().toString());
          reads.scalar = 0;
          context.bind(new QNm("input"), Int32.ZERO);
          assertEquals("true", result.next().toString());
          assertEquals(2, reads.scalar);
          assertNull(result.next());
        }
      }
    });
  }

  static Stream<Arguments> providerPaths() {
    return Stream.of("fresh", "direct", "nested", "default", "nested-default", "alias", "literal", "composed",
        "composed-array").flatMap(shape -> Stream.of(false, true).map(cheap -> Arguments.of(shape, cheap)));
  }

  @ParameterizedTest
  @MethodSource("providerPaths")
  void aCustomDocumentProviderKeepsItsChangingFieldsInOriginalOrder(final String shape, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore actual = BasicJsonDBStore.newBuilder().location(directory).build()) {
        final JsonDBObject object = (JsonDBObject) actual.create("data", "rows", "{\"value\":0}").getDocument("rows");
        object.replace(new QNm("value"), increasing(reads));
        reads.scalar = 0;
        final JsonDBCollection collection = mock(JsonDBCollection.class,
            withSettings().stubOnly()
                          .defaultAnswer(invocation -> invocation.getMethod().getName().equals("getDocument")
                              ? object
                              : null));
        final JsonDBStore provider = mock(JsonDBStore.class,
            withSettings().stubOnly()
                          .defaultAnswer(invocation -> invocation.getMethod().getName().equals("lookup")
                              ? collection
                              : null));
        try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(provider);
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(provider)) {
          context.bind(new QNm("keep"), Int32.ONE);
          final String doc = "jn:doc('data','rows')";
          final String predicate = "xs:integer($r.value) gt 0 and $r.value eq 2";
          final String text = switch (shape) {
            case "fresh" ->
              "for $r in " + doc + " where if ($keep eq 1) then (" + predicate + ") else false() return true()";
            case "direct" -> "xs:integer(" + doc + ".value) gt 0 and " + doc + ".value eq 2";
            case "nested" -> "(for $r in " + doc + " return xs:integer($r.value) gt 0) and " + doc + ".value eq 2";
            case "default" -> "declare variable $c := " + doc + ".value; xs:integer($c) gt 0 and $c eq 2";
            case "nested-default" ->
              "declare variable $c := (for $r in " + doc + " return $r.value); xs:integer($c) gt 0 and $c eq 2";
            case "alias" -> "let $c := " + doc + ".value return xs:integer($c) gt 0 and $c eq 2";
            case "literal" -> "for $r in [{\"child\":" + doc + "}][] where xs:integer($r.child.value) gt 0"
                + " and $r.child.value eq 2 return true()";
            case "composed", "composed-array" -> "for $r at $p in " + (shape.equals("composed")
                ? "(" + doc + ", {\"value\":0})"
                : "[" + doc + ", {\"value\":0}][]") + " where if ($keep eq 1) then (" + predicate
                + ") else false() return $p";
            default -> throw new IllegalArgumentException(shape);
          };
          assertEquals(shape.startsWith("composed")
              ? "1"
              : "true", serialize(chain, context, "declare variable $keep external;" + text));
          assertEquals(2, reads.scalar);
        }
      }
    });
  }

  static Stream<Arguments> escapedRows() {
    return Stream.of("native-return", "native-selected-return", "literal-return", "literal-selected-return",
        "native-inner", "literal-inner", "native-join")
                 .flatMap(shape -> Stream.of(false, true).map(cheap -> Arguments.of(shape, cheap)));
  }

  @ParameterizedTest
  @MethodSource("escapedRows")
  void anEscapedCompositeCannotBecomeAFreshFilterRow(final String shape, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        store.create("data", "rows", "{\"id\":1,\"value\":2}");
        final String source = shape.startsWith("literal")
            ? "[{\"id\":1,\"value\":2}][]"
            : "jn:doc('data','rows')";
        final String test = "xs:integer($a.value) gt 0 and $a.value eq 2";
        final String text = "for $a in " + source + (shape.equals("native-join")
            ? " for $n in (1,1) where $a.id eq $n and (if ($n gt 0) then (" + test + ") else false()) return $a"
            : shape.endsWith("return")
                ? (shape.contains("selected")
                    ? " where $a.id eq 1"
                    : "") + " return ($a, " + test + ")"
                : " for $n in 1 to 2 where if ($n gt 0) then (" + test + ") else false() return $a");
        try (final Iter result = new Query(chain, text).execute(context).iterate()) {
          final Object first = assertInstanceOf(Object.class, result.next());
          first.replace(new QNm("value"), increasing(reads));
          reads.scalar = 0;
          if (shape.endsWith("return"))
            assertEquals("true", result.next().toString());
          else
            assertInstanceOf(Object.class, result.next());
          assertEquals(2, reads.scalar);
          assertNull(result.next());
        }
      }
    });
  }

  private static Sequence source(final Shape shape, final Reads reads) {
    final Sequence value = increasing(reads);
    final ArrayObject object = object("value", value, reads);
    return switch (shape) {
      case SCALAR -> value;
      case OBJECT, CONTEXT -> object;
      case NESTED -> object("child", object, reads);
      case MEMBER -> object("child", array(object, reads), reads);
      case ARRAY -> array(object, reads);
      case ITEM_SEQUENCE -> new ItemSequence(object);
      case SEQUENCE -> new LazySequence() {
        @Override
        public Iter iterate() {
          final Iter items = new ItemSequence(object).iterate();
          return new BaseIter() {
            @Override
            public Item next() {
              final Item item = items.next();
              if (item != null) {
                reads.members++;
              }
              return item;
            }

            @Override
            public void close() {
              items.close();
            }
          };
        }
      };
    };
  }

  private static Sequence increasing(final Reads reads) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Item value = new Int32(++reads.scalar);
        return new BaseIter() {
          private boolean emitted;

          @Override
          public Item next() {
            if (emitted) {
              return null;
            }
            emitted = true;
            return value;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private static ArrayObject object(final String field, final Sequence value, final Reads reads) {
    final ArrayObject object = new ArrayObject(new QNm[] {new QNm(field)}, new Sequence[] {value});
    return mock(ArrayObject.class, withSettings().stubOnly().defaultAnswer(invocation -> {
      final String method = invocation.getMethod().getName();
      if (method.equals("iterate")) {
        return new ItemSequence((Item) invocation.getMock()).iterate();
      }
      if (method.equals("evaluate") || method.equals("evaluateToItem")) {
        return invocation.getMock();
      }
      if (method.equals("get")) {
        reads.fields++;
      } else if (method.equals("values") || method.equals("names") || method.equals("len") || method.equals("length")) {
        reads.inspections++;
      }
      try {
        return invocation.getMethod().invoke(object, invocation.getArguments());
      } catch (final InvocationTargetException exception) {
        throw exception.getCause();
      }
    }));
  }

  private static DArray array(final Sequence value, final Reads reads) {
    final DArray array = new DArray(List.of(value));
    return mock(DArray.class, withSettings().stubOnly().defaultAnswer(invocation -> {
      final String method = invocation.getMethod().getName();
      if (method.equals("at")) {
        reads.members++;
      } else if (method.equals("values")) {
        reads.inspections++;
      }
      try {
        return invocation.getMethod().invoke(array, invocation.getArguments());
      } catch (final InvocationTargetException exception) {
        throw exception.getCause();
      }
    }));
  }

  private static String serialize(final SirixCompileChain chain, final SirixQueryContext context, final String text) {
    return serialize(new Query(chain, text), context);
  }

  private static String serialize(final Query query, final SirixQueryContext context) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      query.serialize(context, writer);
    }
    return out.toString().trim();
  }

  private static int guarded(final AST node) {
    int count = node.getProperty(CheapFirstConjunctStage.ORIGINAL) instanceof AST
        ? 1
        : 0;
    for (int i = 0; i < node.getChildCount(); i++) {
      count += guarded(node.getChild(i));
    }
    return count;
  }

  private static void withSwitch(final boolean cheap, final Runnable body) {
    final String oldCheap = System.getProperty(CheapFirstConjunctStage.ENABLED_PROPERTY);
    System.setProperty(CheapFirstConjunctStage.ENABLED_PROPERTY, Boolean.toString(cheap));
    try {
      body.run();
    } finally {
      restore(CheapFirstConjunctStage.ENABLED_PROPERTY, oldCheap);
    }
  }

  private static void restore(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  private static final class Reads {
    private int scalar;
    private int fields;
    private int members;
    private int inspections;
  }
}
