package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class ConjunctInputTest {
  private static final QNm INPUT = new QNm("input");
  @TempDir
  Path directory;

  static Stream<Arguments> captures() {
    return Stream.of("for-field", "let-field", "let-scalar", "shadow-field", "context-for", "context-let")
                 .flatMap(shape -> switches().map(flags -> Arguments.of(shape, flags[0])));
  }

  @ParameterizedTest
  @MethodSource("captures")
  void capturedAliasesUseTheirOwnValues(final String shape, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final Sequence scalar = increasing(reads);
        final Sequence object = counted(new ArrayObject(new QNm[] {new QNm("value")}, new Sequence[] {scalar}), reads);
        final boolean contextual = shape.startsWith("context");
        if (contextual) {
          context.setContextItem((Item) object);
        } else {
          context.bind(INPUT, shape.equals("let-scalar")
              ? scalar
              : object);
        }
        final String body = switch (shape) {
          case "for-field" -> "for $a in $input return (1, xs:integer($a.value) gt 0 and $a.value eq 2)";
          case "let-field" -> "let $a := $input let $b := $a return (1, xs:integer($b.value) gt 0 and $b.value eq 2)";
          case "let-scalar" -> "let $a := $input let $b := $a return (1, xs:integer($b) gt 0 and $b eq 2)";
          case "shadow-field" ->
            "for $a in $input let $input := 0 return (1, xs:integer($a.value) gt 0 and $a.value eq 2)";
          case "context-for" -> "for $a in $$ return (1, xs:integer($a.value) gt 0 and $a.value eq 2)";
          case "context-let" -> "let $a := $$.value let $b := $a return (1, xs:integer($b) gt 0 and $b eq 2)";
          default -> throw new IllegalArgumentException(shape);
        };
        final Query query = new Query(chain, (contextual
            ? ""
            : "declare variable $input external;") + body);
        try (final Iter result = query.execute(context).iterate()) {
          assertEquals("1", result.next().toString());
          assertEquals(0, reads.scalar);
          if (contextual) {
            context.setContextItem(Int32.ZERO);
          } else {
            context.bind(INPUT, Int32.ZERO);
          }
          assertEquals("0", new Query(chain, "0").execute(context).toString());
          assertEquals("true", result.next().toString());
          assertNull(result.next());
          assertEquals(2, reads.scalar);
          assertEquals(shape.equals("let-scalar")
              ? 0
              : shape.equals("context-let")
                  ? 1
                  : 2,
              reads.fields);
          assertEquals(0, reads.views);
        }
      }
    });
  }

  static Stream<Arguments> defaultAliasesAndSwitches() {
    return Stream.of("$input", "(let $input := $input return $input)")
                 .flatMap(source -> switches().map(flags -> Arguments.of(source, flags[0])));
  }

  @ParameterizedTest
  @MethodSource("defaultAliasesAndSwitches")
  void globalsRemainDistinctFromShadowingTupleBindings(final String source, final boolean cheap) {
    withSwitch(cheap, () -> {
      final Reads reads = new Reads();
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.bind(INPUT, increasing(reads));
        final Query query = new Query(chain, "declare variable $input external; declare variable $a := " + source
            + "; let $input := 0 return xs:integer($a) gt 0 and $a eq 2");
        try (final Iter result = query.execute(context).iterate()) {
          assertEquals("true", result.next().toString());
          assertNull(result.next());
        }
        assertEquals(2, reads.scalar);
        assertEquals(0, reads.views);

      }
    });
  }

  static Stream<Arguments> contextEffects() {
    return Stream.of("direct", "conditional", "alias", "transitive", "unused")
                 .flatMap(shape -> switches().map(flags -> Arguments.of(shape, flags[0])));
  }

  @ParameterizedTest
  @MethodSource("contextEffects")
  void declaredContextDefaultsRetainTheirOrdinaryEffects(final String shape, final boolean cheap) {
    withSwitch(cheap, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = store.create("inventory", "rows", "{\"id\":1}");
        final JsonResourceSession session = collection.getDatabase().beginResourceSession("rows");
        context.setContextItem(Int32.ZERO);
        context.bind(new QNm("keep"), Int32.ZERO);
        final String commit = "sdb:commit(jn:doc('inventory','rows'))";
        final String prolog = shape.equals("transitive")
            ? "declare variable $c := " + commit + "; declare variable $b := $c; declare context item := $b;"
            : "declare context item := " + commit + ";";
        final String body = switch (shape) {
          case "direct", "transitive" -> "xs:integer($$) gt 0 and $keep eq 1";
          case "conditional" -> "if (xs:integer($$) gt 0 and $keep eq 1) then true() else false()";
          case "alias" -> "let $a := $$ return xs:integer($a) gt 0 and $keep eq 1";
          case "unused" -> "0 eq 1 and xs:integer($$) gt 0 and $keep eq 1";
          default -> throw new IllegalArgumentException(shape);
        };
        final Query query = new Query(chain, prolog + "declare variable $keep external;" + body);
        assertEquals(1, session.getMostRecentRevisionNumber());
        try (final Iter result = query.execute(context).iterate()) {
          assertEquals("false", result.next().toString());
          assertNull(result.next());
        }
        assertEquals(shape.equals("unused")
            ? 1
            : 2, session.getMostRecentRevisionNumber());
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pureAndUnusedContextDefaultsStayExactAndLazy(final boolean cheap) {
    withSwitch(cheap, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        context.setContextItem(Int32.ZERO);
        for (final String declaration : List.of("declare context item := 2;", "declare context item external := 2;")) {
          try (final Iter result = new Query(chain, declaration + "$$").execute(context).iterate()) {
            assertEquals(declaration.contains("external")
                ? "0"
                : "2", result.next().toString());
            assertNull(result.next());
          }
        }
        final Query date = new Query(chain, "declare context item := xs:dateTime('2024-01-01T00:00:00Z');"
            + " xs:dateTime($$) lt xs:dateTime('2025-01-01T00:00:00Z') and 0 eq 1");
        try (final Iter result = date.execute(context).iterate()) {
          assertEquals("false", result.next().toString());
          assertNull(result.next());
        }
        try (final Iter result =
            new Query(chain, "declare context item := error();" + "0 eq 1 and xs:integer($$) gt 0").execute(context)
                                                                                                   .iterate()) {
          assertEquals("false", result.next().toString());
          assertNull(result.next());
        }
        assertThrows(QueryException.class,
            () -> new Query(chain, "declare context item := error(); $$").execute(context));
      }
    });
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

  private static Stream<boolean[]> switches() {
    return Stream.of(new boolean[] {false}, new boolean[] {true});
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
    if (value == null)
      System.clearProperty(name);
    else
      System.setProperty(name, value);
  }

  private static Sequence counted(final ArrayObject object, final Reads reads) {
    return mock(ArrayObject.class, withSettings().stubOnly().defaultAnswer(call -> {
      final String method = call.getMethod().getName();
      if (method.equals("get"))
        reads.fields++;
      if (method.equals("values") || method.equals("names"))
        reads.views++;
      if (method.equals("evaluate") || method.equals("evaluateToItem"))
        return call.getMock();
      if (method.equals("iterate"))
        return new ItemSequence((Item) call.getMock()).iterate();
      try {
        return call.getMethod().invoke(object, call.getArguments());
      } catch (final InvocationTargetException exception) {
        throw exception.getCause();
      }
    }));
  }

  private static final class Reads {
    private int scalar;
    private int fields;
    private int views;
  }
}
