package io.sirix.query.compiler.optimizer;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Iter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.XmlDBStore;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class IndexedLetMaterializationTest {
  @TempDir
  Path directory;

  static Stream<Arguments> indexedResults() {
    return Stream.of(IndexType.PATH, IndexType.NAME)
                 .flatMap(type -> Stream.of(false,
                     true).flatMap(eager -> Stream.of(false, true).map(enabled -> Arguments.of(type, eager, enabled))));
  }

  @ParameterizedTest
  @MethodSource("indexedResults")
  void physicalIndexReadsRespectResultLifetime(final IndexType type, final boolean eager, final boolean enabled) {
    withMaterialization(enabled, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = indexedCollection(store, context, type);
        final AtomicInteger reads = new AtomicInteger();
        final CompileChain chain = indexedChain(store, type, reads);
        final String binding = "let $rows := (for $n in " + source(type) + " return $n.v + 0)";
        final Query query = new Query(chain, binding + (eager
            ? " return [sum($rows),sum($rows)][]"
            : " return (sum($rows),sum($rows))"));
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals("1", output.next().toString());
          commitValue(collection, type, 2);
          assertEquals(eager
              ? "1"
              : "2", output.next().toString());
          assertNull(output.next());
        }
        assertEquals(eager && enabled
            ? 1
            : 2, reads.get(), "actual executions of the intended physical index");
      }
    });
  }

  static Stream<Arguments> dependencyPaths() {
    return Stream.of("global", "transitive-global", "captured", "transitive-captured")
                 .flatMap(path -> Stream.of(false, true).map(enabled -> Arguments.of(path, enabled)));
  }

  @ParameterizedTest
  @MethodSource("dependencyPaths")
  void indexedDependenciesRemainEvaluationLocal(final String path, final boolean enabled) {
    withMaterialization(enabled, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = indexedCollection(store, context, IndexType.PATH);
        final AtomicInteger reads = new AtomicInteger();
        final String producer = "(for $n in " + source(IndexType.PATH) + " return $n.v + 0)";
        final String prefix = switch (path) {
          case "global" -> "declare variable $source := " + producer + ";";
          case "transitive-global" -> "declare variable $base := " + producer + "; declare variable $source := $base;";
          case "captured" -> "let $source := " + producer;
          case "transitive-captured" -> "let $base := " + producer + " let $source := (for $n in $base return $n + 0)";
          default -> throw new IllegalArgumentException(path);
        };
        final Query query = new Query(indexedChain(store, IndexType.PATH, reads),
            prefix + " let $rows := (for $n in $source return $n + 0) return (sum($rows),sum($rows))");
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals("1", output.next().toString());
          commitValue(collection, IndexType.PATH, 2);
          assertEquals("2", output.next().toString());
          assertNull(output.next());
        }
        assertEquals(2, reads.get());
      }
    });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void indexedMaterializationChecksProvidersRegisteredAfterCompilation(final boolean enabled) {
    withMaterialization(enabled, () -> {
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(directory).build();
          final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store)) {
        final JsonDBCollection collection = indexedCollection(store, context, IndexType.PATH);
        final AtomicInteger reads = new AtomicInteger();
        final Query query = new Query(indexedChain(store, IndexType.PATH, reads),
            "let $rows := (for $n in " + source(IndexType.PATH) + " return $n.v + 0) return [sum($rows),sum($rows)][]");
        store.addDatabase(mock(JsonDBCollection.class, withSettings().stubOnly().defaultAnswer(invocation -> {
          if (invocation.getMethod().getName().equals("getDocument"))
            throw new AssertionError("The index provider guard must not inspect documents");
          try {
            return invocation.getMethod().invoke(collection, invocation.getArguments());
          } catch (final InvocationTargetException exception) {
            throw exception.getCause();
          }
        })), collection.getDatabase());
        try (final Iter output = query.execute(context).iterate()) {
          assertEquals("1", output.next().toString());
          assertEquals("1", output.next().toString());
          assertNull(output.next());
        }
        assertEquals(2, reads.get(), "an unproven registered provider must retain generic index execution");
      }
    });
  }

  private static JsonDBCollection indexedCollection(final BasicJsonDBStore store, final SirixQueryContext context,
      final IndexType type) {
    final StringBuilder json = new StringBuilder(16000).append(type == IndexType.NAME
        ? "{\"root\":{\"item\":{\"v\":1}},\"padding\":["
        : "{\"root\":{\"items\":[{\"v\":1}]},\"padding\":[");
    for (int i = 0; i < 1000; i++) {
      if (i > 0)
        json.append(',');
      json.append("{\"unrelated\":").append(i).append('}');
    }
    final JsonDBCollection collection = store.create("input", "rows", json.append("]}").toString());
    final String create = switch (type) {
      case PATH -> "jn:create-path-index($doc,('/root/items','/root/items/[]'))";
      case NAME -> "jn:create-name-index($doc,'item')";
      default -> throw new IllegalArgumentException(type.toString());
    };
    JNFun.register();
    SDBFun.register();
    new Query(indexedChain(store, type, new AtomicInteger()),
        "let $doc := jn:doc('input','rows') let $index := " + create + " return sdb:commit($doc)").evaluate(context);
    collection.getDatabase().beginResourceSession("rows").getNodeTrx().orElseThrow().close();
    return collection;
  }

  private static String source(final IndexType type) {
    return type == IndexType.NAME
        ? "jn:doc('input','rows').root.item"
        : "jn:doc('input','rows').root.items[]";
  }

  private static CompileChain indexedChain(final BasicJsonDBStore store, final IndexType type,
      final AtomicInteger reads) {
    return new CompileChain() {
      @Override
      protected Optimizer getOptimizer(final Map<QNm, Str> options) {
        return new SirixOptimizer(options, mock(XmlDBStore.class), store);
      }

      @Override
      protected Translator getTranslator(final Map<QNm, Str> options) {
        return new SirixTranslator(options) {
          @Override
          protected Expr anyExpr(final AST node) {
            final Expr actual = super.anyExpr(node);
            if (node.getType() != XQExt.IndexExpr)
              return actual;
            assertEquals(type, node.getProperty("indexType"));
            return mock(Expr.class, withSettings().stubOnly().defaultAnswer(invocation -> {
              final String method = invocation.getMethod().getName();
              if (method.equals("evaluate") || method.equals("evaluateToItem"))
                reads.incrementAndGet();
              try {
                return invocation.getMethod().invoke(actual, invocation.getArguments());
              } catch (final InvocationTargetException exception) {
                throw exception.getCause();
              }
            }));
          }
        };
      }
    };
  }

  private static void commitValue(final JsonDBCollection collection, final IndexType type, final int value) {
    final JsonResourceSession resource = collection.getDatabase().beginResourceSession("rows");
    try (final JsonNodeTrx writer = resource.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      final int depth = type == IndexType.NAME
          ? 4
          : 5;
      for (int i = 0; i < depth; i++)
        assertTrue(writer.moveToFirstChild());
      assertTrue(writer.isNumberValue());
      writer.setNumberValue(value);
      writer.commit();
    }
  }

  private static void withMaterialization(final boolean enabled, final Runnable action) {
    final String previous = System.getProperty(LetMaterializationStage.ENABLED_PROPERTY);
    System.setProperty(LetMaterializationStage.ENABLED_PROPERTY, Boolean.toString(enabled));
    try {
      action.run();
    } finally {
      if (previous == null)
        System.clearProperty(LetMaterializationStage.ENABLED_PROPERTY);
      else
        System.setProperty(LetMaterializationStage.ENABLED_PROPERTY, previous);
    }
  }
}
