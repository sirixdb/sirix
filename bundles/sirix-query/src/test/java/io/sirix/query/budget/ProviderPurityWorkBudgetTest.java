package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.optimizer.CheapFirstConjunctStage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollectionImpl;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

@Isolated
final class ProviderPurityWorkBudgetTest {
  private static final int ROWS = 128;
  private static final String QUERY = "declare variable $keep external; for $r in jn:doc('data','rows')[]"
      + " where if ($keep eq 1) then (xs:integer($r.cost) gt 0 and $r.cost eq 1) else false() return true()";

  @ParameterizedTest
  @ValueSource(ints = {0, 1000})
  void rowAdmissionDoesNotClassifyUnrelatedCollections(final int unrelated, @TempDir final Path directory) {
    final String property = CheapFirstConjunctStage.ENABLED_PROPERTY;
    final String previous = System.getProperty(property);
    final AtomicInteger guards = new AtomicInteger();
    try (final BasicJsonDBStore actual = BasicJsonDBStore.newBuilder().location(directory).build()) {
      actual.create("data", "rows", "[" + "{\"cost\":1},".repeat(ROWS - 1) + "{\"cost\":1}]");
      for (int index = 0; index < unrelated; index++) {
        final String name = "unrelated" + index;
        final Database<JsonResourceSession> database = unusedDatabase(directory.resolve(name));
        actual.addDatabase(new JsonDBCollectionImpl(name, database, actual), database);
      }
      final BasicJsonDBStore counted =
          mock(BasicJsonDBStore.class, withSettings().stubOnly().defaultAnswer(invocation -> {
            if (invocation.getMethod().getName().equals("close"))
              return null;
            if (invocation.getMethod().getName().equals("hasOnlyStockCollections"))
              guards.incrementAndGet();
            try {
              return invocation.getMethod().invoke(actual, invocation.getArguments());
            } catch (final InvocationTargetException exception) {
              throw exception.getCause();
            }
          }));
      assertTrue(actual.getCollectionClassificationCount() >= unrelated + 1,
          "registration proves the classification counter is live");
      final String expected = "true ".repeat(ROWS).trim();
      for (final boolean enabled : new boolean[] {false, true}) {
        System.setProperty(property, Boolean.toString(enabled));
        try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(counted);
            final SirixQueryContext context = SirixQueryContext.createWithJsonStore(counted)) {
          final Query query = new Query(chain, QUERY);
          context.bind(new QNm("keep"), Int32.ONE);
          for (int evaluation = 0; evaluation < 2; evaluation++) {
            guards.set(0);
            final long classifications = actual.getCollectionClassificationCount();
            final StringWriter output = new StringWriter();
            try (final PrintWriter writer = new PrintWriter(output)) {
              query.serialize(context, writer);
            }
            assertEquals(expected, output.toString().trim());
            assertEquals(0, actual.getCollectionClassificationCount() - classifications,
                "guarded rows must not reclassify the registry");
            assertEquals(enabled
                ? ROWS
                : 0, guards.get(), "the counted guard runs on every optimized row");
          }
        }
      }
    } finally {
      if (previous == null)
        System.clearProperty(property);
      else
        System.setProperty(property, previous);
    }
  }

  @SuppressWarnings("unchecked")
  private static Database<JsonResourceSession> unusedDatabase(final Path path) {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(path);
    final AtomicBoolean open = new AtomicBoolean(true);
    return mock(Database.class,
        withSettings().stubOnly().defaultAnswer(invocation -> switch (invocation.getMethod().getName()) {
          case "getDatabaseConfig" -> configuration;
          case "isOpen" -> open.get();
          case "close" -> {
            open.set(false);
            yield null;
          }
          default -> throw new AssertionError("Unrelated database accessed: " + invocation.getMethod().getName());
        }));
  }
}
