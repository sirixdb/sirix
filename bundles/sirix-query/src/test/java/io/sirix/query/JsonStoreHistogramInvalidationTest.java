package io.sirix.query;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.Str;
import io.brackit.query.node.stream.ArrayStream;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.compiler.optimizer.stats.Histogram;
import io.sirix.query.compiler.optimizer.stats.HistogramCollector;
import io.sirix.query.compiler.optimizer.stats.StatisticsCatalog;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class JsonStoreHistogramInvalidationTest {
  private static final String RESOURCE = "resource1";
  private static final List<String> FIELDS = List.of("price", "quantity");
  private static final int[] REVISIONS = {1, 2, StatisticsCatalog.LATEST_REVISION};

  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"loader", "readers", "strings", "paths", "drop", "canonical-drop"})
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void symbolicAliasRemovalInvalidatesOnlyAffectedHistograms(final String operation) throws Exception {
    aliasRemovalInvalidatesOnlyAffectedHistograms(operation, true);
  }

  @ParameterizedTest
  @ValueSource(strings = {"loader", "readers", "strings", "paths", "drop", "canonical-drop"})
  void lexicalAliasRemovalInvalidatesOnlyAffectedHistograms(final String operation) throws Exception {
    aliasRemovalInvalidatesOnlyAffectedHistograms(operation, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"loader", "readers", "strings", "paths", "drop", "canonical-drop"})
  void closedAliasSurvivesUnrelatedReplacement(final String operation) throws Exception {
    aliasStatisticsSurviveHandleRemoval("handle", operation);
  }

  @ParameterizedTest
  @ValueSource(strings = {"loader", "readers", "strings", "paths", "drop", "canonical-drop"})
  void collectionClosePreservesAliasAssociation(final String operation) throws Exception {
    aliasStatisticsSurviveHandleRemoval("collection", operation);
  }

  @Test
  void unregisteredOpenAliasIsNotReused() throws Exception {
    aliasStatisticsSurviveHandleRemoval("unregister", "loader");
  }

  @Test
  void collectionDeleteInvalidatesEveryAliasBeforeRecreation() throws Exception {
    final Path storePath = directory.resolve("store");
    final Path otherPath = directory.resolve("other");
    final Path localDatabasePath = storePath.resolve("orders");
    final Path targetDatabasePath = otherPath.resolve("orders");
    final String firstAlias = "../other/./orders";
    final String secondAlias = "../other/orders";
    final List<String> affectedNames = List.of(firstAlias, secondAlias);
    final var catalog = StatisticsCatalog.getInstance();
    try (
        final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).buildPathSummary(true).build();
        final BasicJsonDBStore other =
            BasicJsonDBStore.newBuilder().location(otherPath).buildPathSummary(true).build()) {
      final JsonDBCollection local = store.create("orders", RESOURCE, "{\"price\":[100,101],\"quantity\":[7,8]}");
      advanceToRevisionTwo(local, 200);
      final Histogram[] unrelatedHistograms = collectHistograms(store, "orders");
      final JsonDBCollection target = other.create("orders", RESOURCE, "{\"price\":[10,11],\"quantity\":[3,4]}");
      advanceToRevisionTwo(target, 20);
      target.close();
      for (final String name : affectedNames) {
        assertHistogramValues(collectHistograms(store, name), 10, 20, 3);
      }
      requireNonNull(store.lookup(firstAlias)).delete();
      assertFalse(Files.exists(targetDatabasePath));
      assertMissingHistograms(affectedNames);
      assertRegisteredHistograms("orders", unrelatedHistograms);
      final JsonDBCollection replacement =
          store.create(secondAlias, RESOURCE, "{\"price\":[900,901],\"quantity\":[9,10]}");
      advanceToRevisionTwo(replacement, 901);
      assertHistogramValues(collectHistograms(store, secondAlias), 900, 901, 9);
      assertMissingHistograms(List.of(firstAlias));
      assertRegisteredHistograms("orders", unrelatedHistograms);
    } finally {
      for (final String name : affectedNames) {
        catalog.invalidateDatabase(name);
      }
      catalog.invalidateDatabase("orders");
      Databases.removeDatabase(localDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  private void aliasStatisticsSurviveHandleRemoval(final String removal, final String operation) throws Exception {
    final Path storePath = directory.resolve("store");
    final Path otherPath = directory.resolve("other");
    final Path localDatabasePath = storePath.resolve("orders");
    final Path targetDatabasePath = otherPath.resolve("orders");
    final String firstAlias = "../other/./orders";
    final String secondAlias = "../other/orders";
    final List<String> affectedNames = List.of(firstAlias, secondAlias);
    final var catalog = StatisticsCatalog.getInstance();
    try (
        final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).buildPathSummary(true).build();
        final BasicJsonDBStore other =
            BasicJsonDBStore.newBuilder().location(otherPath).buildPathSummary(true).build()) {
      final JsonDBCollection local = store.create("orders", RESOURCE, "{\"price\":[100,101],\"quantity\":[7,8]}");
      advanceToRevisionTwo(local, 200);
      final JsonDBCollection target = other.create("orders", RESOURCE, "{\"price\":[10,11],\"quantity\":[3,4]}");
      advanceToRevisionTwo(target, 20);
      target.close();
      final JsonDBCollection alias = requireNonNull(store.lookup(firstAlias));
      final Histogram[] aliasHistograms = collectHistograms(store, firstAlias);
      assertHistogramValues(aliasHistograms, 10, 20, 3);
      switch (removal) {
        case "collection" -> alias.close();
        case "handle" -> alias.getDatabase().close();
        case "unregister" -> {
          store.removeDatabase(alias.getDatabase());
          assertNotSame(alias, store.lookup(firstAlias));
          alias.getDatabase().close();
        }
        default -> throw new IllegalArgumentException(removal);
      }
      assertRegisteredHistograms(firstAlias, aliasHistograms);
      if (removal.equals("handle")) {
        final JsonDBCollection unrelated = store.create("orders", RESOURCE, "{\"price\":[400,401],\"quantity\":[7,8]}");
        advanceToRevisionTwo(unrelated, 500);
      }
      final Histogram[] unrelatedHistograms = collectHistograms(store, "orders");
      assertHistogramValues(unrelatedHistograms, removal.equals("handle")
          ? 400
          : 100,
          removal.equals("handle")
              ? 500
              : 200,
          7);
      assertRegisteredHistograms(firstAlias, aliasHistograms);
      assertHistogramValues(collectHistograms(store, secondAlias), 10, 20, 3);

      final String replacementData = "{\"price\":[900,901],\"quantity\":[9,10]}";
      final JsonDBCollection replacement;
      switch (operation) {
        case "readers" ->
          replacement = store.create(secondAlias, Set.of(new JsonReader(new StringReader(replacementData))));
        case "strings" -> replacement =
            store.createFromJsonStrings(secondAlias, new ArrayStream<>(new Str[] {new Str(replacementData)}));
        case "paths" -> replacement = store.createFromPaths(secondAlias,
            new ArrayStream<>(new Path[] {Files.writeString(directory.resolve("replacement.json"), replacementData)}));
        case "drop", "canonical-drop" -> {
          store.drop(operation.equals("drop")
              ? secondAlias
              : targetDatabasePath.toRealPath().toString());
          assertMissingHistograms(affectedNames);
          assertRegisteredHistograms("orders", unrelatedHistograms);
          replacement = store.create(secondAlias, RESOURCE, replacementData);
        }
        case "loader" -> replacement = store.create(secondAlias, RESOURCE, replacementData);
        default -> throw new IllegalArgumentException(operation);
      }
      assertMissingHistograms(affectedNames);
      assertRegisteredHistograms("orders", unrelatedHistograms);
      assertEquals(secondAlias, replacement.getName());
      assertSame(replacement, store.lookup(secondAlias));
      advanceToRevisionTwo(replacement, 901);
      assertHistogramValues(collectHistograms(store, secondAlias), 900, 901, 9);
      assertMissingHistograms(List.of(firstAlias));
      assertRegisteredHistograms("orders", unrelatedHistograms);
    } finally {
      for (final String name : affectedNames) {
        catalog.invalidateDatabase(name);
      }
      catalog.invalidateDatabase("orders");
      Databases.removeDatabase(localDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  private void aliasRemovalInvalidatesOnlyAffectedHistograms(final String operation, final boolean symbolic)
      throws Exception {
    final Path storePath = directory.resolve("store");
    final Path otherPath = directory.resolve("other");
    final Path localDatabasePath = storePath.resolve("orders");
    final Path targetDatabasePath = otherPath.resolve("orders");
    final String requestedName = symbolic
        ? "alias"
        : "../other/orders";
    final String closedAliasName = symbolic
        ? "alias-two"
        : "../other/../other/orders";
    final List<String> affectedNames = List.of(requestedName, closedAliasName, "../other/./orders");
    final var catalog = StatisticsCatalog.getInstance();
    try (
        final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(storePath).buildPathSummary(true).build();
        final BasicJsonDBStore other =
            BasicJsonDBStore.newBuilder().location(otherPath).buildPathSummary(true).build()) {
      final JsonDBCollection local = store.create("orders", RESOURCE, "{\"price\":[100,101],\"quantity\":[7,8]}");
      advanceToRevisionTwo(local, 200);
      final Histogram[] unrelatedHistograms = collectHistograms(store, "orders");
      assertHistogramValues(unrelatedHistograms, 100, 200, 7);
      local.getDatabase().close();

      final JsonDBCollection target = other.create("orders", RESOURCE, "{\"price\":[10,11],\"quantity\":[3,4]}");
      advanceToRevisionTwo(target, 20);
      target.close();
      if (symbolic) {
        final Path canonicalTarget = targetDatabasePath.toRealPath();
        Files.createSymbolicLink(storePath.resolve(requestedName), canonicalTarget);
        Files.createSymbolicLink(storePath.resolve(closedAliasName), canonicalTarget);
      }

      for (final String name : affectedNames) {
        assertHistogramValues(collectHistograms(store, name), 10, 20, 3);
      }
      final JsonDBCollection requested = requireNonNull(store.lookup(requestedName));
      if (!operation.equals("canonical-drop")) {
        store.removeDatabase(requested.getDatabase());
      }
      requested.getDatabase().close();
      requireNonNull(store.lookup(closedAliasName)).getDatabase().close();

      final String replacementData = "{\"price\":[900,901],\"quantity\":[9,10]}";
      final JsonDBCollection replacement;
      switch (operation) {
        case "readers" ->
          replacement = store.create(requestedName, Set.of(new JsonReader(new StringReader(replacementData))));
        case "strings" -> replacement =
            store.createFromJsonStrings(requestedName, new ArrayStream<>(new Str[] {new Str(replacementData)}));
        case "paths" -> replacement = store.createFromPaths(requestedName,
            new ArrayStream<>(new Path[] {Files.writeString(directory.resolve("replacement.json"), replacementData)}));
        case "drop", "canonical-drop" -> {
          store.drop(operation.equals("drop")
              ? requestedName
              : targetDatabasePath.toRealPath().toString());
          assertFalse(Files.exists(targetDatabasePath));
          assertMissingHistograms(affectedNames);
          assertRegisteredHistograms("orders", unrelatedHistograms);
          final JsonDBCollection recreated = other.create("orders", RESOURCE, replacementData);
          recreated.close();
          replacement = requireNonNull(store.lookup(requestedName));
        }
        case "loader" -> replacement = store.create(requestedName, RESOURCE, replacementData);
        default -> throw new IllegalArgumentException(operation);
      }

      assertMissingHistograms(affectedNames);
      assertRegisteredHistograms("orders", unrelatedHistograms);
      assertEquals(requestedName, replacement.getName());
      assertSame(replacement, store.lookup(requestedName));
      advanceToRevisionTwo(replacement, 901);
      assertHistogramValues(collectHistograms(store, requestedName), 900, 901, 9);
      assertRegisteredHistograms("orders", unrelatedHistograms);
      final Histogram stillLocal = requireNonNull(new HistogramCollector(store).collect("orders", RESOURCE, "price",
          HistogramCollector.DEFAULT_SAMPLE_SIZE, Histogram.DEFAULT_BUCKET_COUNT, 2));
      assertEquals(200, stillLocal.minValue());
      assertEquals(201, stillLocal.maxValue());
      assertMissingHistograms(affectedNames.subList(1, affectedNames.size()));
    } finally {
      for (final String name : affectedNames) {
        catalog.invalidateDatabase(name);
      }
      catalog.invalidateDatabase("orders");
      Databases.removeDatabase(localDatabasePath);
      Databases.removeDatabase(targetDatabasePath);
    }
  }

  private static void advanceToRevisionTwo(final JsonDBCollection collection, final int price) {
    try (final JsonResourceSession session = collection.getDatabase().beginResourceSession(RESOURCE);
        final JsonNodeTrx writer = session.beginNodeTrx()) {
      assertEquals(1, session.getMostRecentRevisionNumber());
      assertTrue(writer.moveToFirstChild());
      assertTrue(writer.moveToFirstChild());
      assertEquals("price", requireNonNull(writer.getName()).getLocalName());
      assertTrue(writer.moveToFirstChild());
      writer.setNumberValue(price);
      assertTrue(writer.moveToRightSibling());
      writer.setNumberValue(price + 1);
      writer.commit();
      assertEquals(2, session.getMostRecentRevisionNumber());
    }
  }

  private static Histogram[] collectHistograms(final BasicJsonDBStore store, final String name) {
    final var collector = new HistogramCollector(store);
    final var catalog = StatisticsCatalog.getInstance();
    final Histogram[] histograms = new Histogram[REVISIONS.length * FIELDS.size()];
    int index = 0;
    for (final int revision : REVISIONS) {
      for (final String field : FIELDS) {
        assertTrue(collector.collectAndRegister(name, RESOURCE, field, revision));
        final Histogram histogram = requireNonNull(catalog.get(name, RESOURCE, field, revision));
        assertEquals(2, histogram.totalCount());
        histograms[index++] = histogram;
      }
    }
    return histograms;
  }

  private static void assertHistogramValues(final Histogram[] histograms, final int initialPrice,
      final int currentPrice, final int quantity) {
    for (int index = 0; index < histograms.length; index++) {
      final int expected = index % 2 == 1
          ? quantity
          : index == 0
              ? initialPrice
              : currentPrice;
      assertEquals(expected, histograms[index].minValue());
      assertEquals(expected + 1, histograms[index].maxValue());
    }
  }

  private static void assertMissingHistograms(final List<String> names) {
    final var catalog = StatisticsCatalog.getInstance();
    for (final String name : names) {
      for (final int revision : REVISIONS) {
        for (final String field : FIELDS) {
          assertNull(catalog.get(name, RESOURCE, field, revision), name + "/" + field + " revision " + revision);
        }
      }
    }
  }

  private static void assertRegisteredHistograms(final String name, final Histogram[] histograms) {
    final var catalog = StatisticsCatalog.getInstance();
    int index = 0;
    for (final int revision : REVISIONS) {
      for (final String field : FIELDS) {
        assertSame(histograms[index++], catalog.get(name, RESOURCE, field, revision));
      }
    }
  }
}
