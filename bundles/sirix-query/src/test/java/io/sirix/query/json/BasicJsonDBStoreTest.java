package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.node.stream.ArrayStream;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BasicJsonDBStoreTest {

  private BasicJsonDBStore.Builder builder;
  private Path jsonTestDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    jsonTestDir = Files.createTempDirectory("sirix-json-store-test");
    builder = BasicJsonDBStore.newBuilder().location(jsonTestDir);
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
    if (jsonTestDir != null) {
      Databases.removeDatabase(jsonTestDir);
    }
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  @DisplayName("Should create a new collection with provided JSON")
  void shouldCreateNewCollectionWithProvidedJson() {
    String collName = "testCollection";
    String optResName = "testResource";
    String json = "{\"key\":\"value\"}";
    store = builder.build();
    store.create(collName, optResName, json);
    JsonDBCollection collection = store.lookup(collName);
    JsonDBItem testResource = collection.getDocument("testResource");
    final var stringWriter = new StringWriter();
    final var writer = new PrintWriter(stringWriter);
    new StringSerializer(writer).serialize(testResource);
    assertEquals(json, stringWriter.toString());
  }

  @Test
  @DisplayName("Should set correct number of nodes before auto commit")
  void shouldSetCorrectNumberOfNodesBeforeAutoCommit() {
    int expectedNodes = 500;
    builder.numberOfNodesBeforeAutoCommit(expectedNodes);
    store = builder.build();
    assertEquals(expectedNodes, store.options().numberOfNodesBeforeAutoCommit());
  }

  @Test
  void defaultStoreResourcesSupportLoadTimeProjectionsWithoutDeweyIds() {
    store = builder.build();
    final ProjectionSpec projection = new ProjectionSpec("/[]", List.of("/[]/value"), List.of("long"));
    final JsonDBCollection collection =
        store.create("defaultProjection", "resource", new JsonReader(new StringReader("[{\"value\":1}]")), projection);

    try (final var session = collection.getDatabase().beginResourceSession("resource");
        final var rtx = session.beginNodeReadOnlyTrx()) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
      final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNotNull(controller.getIndexes().getIndexDef(0, projection.toIndexDef().getType()));
      assertTrue(controller.hasProjectionIndex());
      final var handle =
          controller.openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"}, new String[] {"value"});
      assertNotNull(handle);
      assertTrue(handle.columnOf("value") >= 0);
    }
  }

  @Test
  void genericResourcesKeepTheOptInDeweyDefault() {
    store = builder.build();
    final JsonDBCollection collection = store.create("defaultGeneric", "resource", "[1]");

    try (final var session = collection.getDatabase().beginResourceSession("resource")) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
    }
  }

  @Test
  void explicitDeweyDisableSupportsLoadTimeProjections() {
    store = builder.storeDeweyIds(false).build();
    final ProjectionSpec projection = new ProjectionSpec("/[]", List.of("/[]/value"), List.of("long"));
    final JsonDBCollection collection =
        store.create("disabledDewey", "resource", new JsonReader(new StringReader("[{\"value\":1}]")), projection);

    try (final var session = collection.getDatabase().beginResourceSession("resource");
        final var rtx = session.beginNodeReadOnlyTrx()) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
      final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNotNull(controller.getIndexes().getIndexDef(0, projection.toIndexDef().getType()));
      assertTrue(controller.hasProjectionIndex());
      final var handle =
          controller.openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"}, new String[] {"value"});
      assertNotNull(handle);
      assertTrue(handle.columnOf("value") >= 0);
    }
  }

  @Test
  @DisplayName("create(Set) shards each reader into its own resource1..N with correct content")
  void createWithMultipleReadersShardsIntoResources() {
    store = builder.build();
    final Set<JsonReader> readers = new LinkedHashSet<>();
    readers.add(new JsonReader(new StringReader("[1,2,3]")));
    readers.add(new JsonReader(new StringReader("{\"k\":\"v\"}")));

    final JsonDBCollection collection = store.create("multiColl", readers);

    assertEquals(2, collection.getDatabase().listResources().size());
    assertEquals(Set.of("[1,2,3]", "{\"k\":\"v\"}"),
        Set.of(serialize(collection, "resource1"), serialize(collection, "resource2")));
  }

  @Test
  @DisplayName("createFromPaths shards each path into its own resource1..N with correct content")
  void createFromPathsShardsIntoResources() throws Exception {
    store = builder.build();
    final Path first = Files.writeString(jsonTestDir.resolve("first.json"), "[1,2,3]");
    final Path second = Files.writeString(jsonTestDir.resolve("second.json"), "{\"k\":\"v\"}");

    final JsonDBCollection collection =
        store.createFromPaths("pathsColl", new ArrayStream<>(new Path[] {first, second}));

    assertEquals(2, collection.getDatabase().listResources().size());
    assertEquals(Set.of("[1,2,3]", "{\"k\":\"v\"}"),
        Set.of(serialize(collection, "resource1"), serialize(collection, "resource2")));
  }

  private static String serialize(JsonDBCollection collection, String resourceName) {
    final var stringWriter = new StringWriter();
    new StringSerializer(new PrintWriter(stringWriter)).serialize(collection.getDocument(resourceName));
    return stringWriter.toString();
  }

  @Test
  void classificationTracksMultipleRegistrationsAndClosedDatabaseCleanup() {
    store = builder.build();
    final JsonDBCollection first = store.create("first", "rows", "{}");
    final JsonDBCollection second = store.create("second", "rows", "{}");
    final JsonDBCollection custom = mock(JsonDBCollection.class);
    when(custom.getName()).thenReturn("first");
    assertTrue(store.hasOnlyStockCollections());
    store.addDatabase(custom, first.getDatabase());
    store.addDatabase(custom, second.getDatabase());
    assertFalse(store.hasOnlyStockCollections());
    store.addDatabase(first, first.getDatabase());
    assertFalse(store.hasOnlyStockCollections());
    store.removeDatabase(first.getDatabase());
    assertFalse(store.hasOnlyStockCollections());
    first.getDatabase().close();
    second.getDatabase().close();
    store.create("third", "rows", "{}");
    assertTrue(store.hasOnlyStockCollections());
    final JsonDBCollection reopened = store.lookup("first");
    assertTrue(store.hasOnlyStockCollections());
    store.addDatabase(custom, reopened.getDatabase());
    assertFalse(store.hasOnlyStockCollections());
    store.create("first", "rows", "{\"value\":1}");
    assertTrue(store.hasOnlyStockCollections());
    final JsonDBCollection replacement = store.lookup("first");
    store.addDatabase(custom, replacement.getDatabase());
    store.drop("first");
    assertTrue(store.hasOnlyStockCollections());
    final JsonDBCollection third = store.lookup("third");
    store.addDatabase(custom, third.getDatabase());
    assertFalse(store.hasOnlyStockCollections());
    store.close();
    assertTrue(store.hasOnlyStockCollections());
  }

  @Test
  void concurrentRegistryMutationsRetainExactClassificationAfterPublication() throws Exception {
    store = builder.build();
    final Database<JsonResourceSession> database = testDatabase(jsonTestDir.resolve("concurrent"), () -> {});
    final JsonDBCollection stock = new JsonDBCollectionImpl("concurrent", database, store);
    final JsonDBCollection custom = mock(JsonDBCollection.class);
    when(custom.getName()).thenReturn("concurrent");
    final CountDownLatch start = new CountDownLatch(1);
    try (final ExecutorService executor = Executors.newFixedThreadPool(4)) {
      final List<Future<?>> mutations = new ArrayList<>(4);
      for (int worker = 0; worker < 4; worker++) {
        mutations.add(executor.submit(() -> {
          start.await();
          for (int iteration = 0; iteration < 128; iteration++) {
            store.addDatabase(custom, database);
            store.addDatabase(stock, database);
            store.addDatabase(custom, database);
            store.removeDatabase(database);
          }
          return null;
        }));
      }
      start.countDown();
      for (final Future<?> mutation : mutations) {
        mutation.get();
      }
    }
    assertTrue(store.hasOnlyStockCollections());
    store.addDatabase(custom, database);
    assertFalse(store.hasOnlyStockCollections());
    store.addDatabase(stock, database);
    assertTrue(store.hasOnlyStockCollections());
  }

  @Test
  void unprovenClassificationIsPublishedBeforeTheCollection() throws Exception {
    store = builder.build();
    final CountDownLatch inserting = new CountDownLatch(1);
    final CountDownLatch publish = new CountDownLatch(1);
    final Database<JsonResourceSession> database = testDatabase(jsonTestDir.resolve("pending"), () -> {
      inserting.countDown();
      try {
        publish.await();
      } catch (final InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new AssertionError(exception);
      }
    });
    final JsonDBCollection custom = mock(JsonDBCollection.class);
    when(custom.getName()).thenReturn("pending");
    try (final ExecutorService executor = Executors.newSingleThreadExecutor()) {
      final Future<?> registration = executor.submit(() -> store.addDatabase(custom, database));
      try {
        inserting.await();
        assertFalse(store.hasOnlyStockCollections());
      } finally {
        publish.countDown();
      }
      registration.get();
    }
    assertFalse(store.hasOnlyStockCollections());
    store.removeDatabase(database);
    assertTrue(store.hasOnlyStockCollections());
    database.close();
  }

  @Test
  void failedPublicationRestoresClassification() {
    store = builder.build();
    final Database<JsonResourceSession> database = testDatabase(jsonTestDir.resolve("failed"), () -> {
      throw new IllegalStateException("Failed map insertion");
    });
    final JsonDBCollection custom = mock(JsonDBCollection.class);
    when(custom.getName()).thenReturn("failed");
    assertThrows(IllegalStateException.class, () -> store.addDatabase(custom, database));
    assertTrue(store.hasOnlyStockCollections());
    store.addDatabase(custom, database);
    assertFalse(store.hasOnlyStockCollections());
    store.removeDatabase(database);
    assertTrue(store.hasOnlyStockCollections());
    database.close();
  }

  @SuppressWarnings("unchecked")
  private static Database<JsonResourceSession> testDatabase(final Path path, final Runnable inserting) {
    final DatabaseConfiguration configuration = new DatabaseConfiguration(path);
    final AtomicInteger hashes = new AtomicInteger();
    final AtomicBoolean open = new AtomicBoolean(true);
    return (Database<JsonResourceSession>) Proxy.newProxyInstance(Database.class.getClassLoader(),
        new Class<?>[] {Database.class}, (proxy, method, arguments) -> switch (method.getName()) {
          case "getDatabaseConfig" -> configuration;
          case "isOpen" -> open.get();
          case "close" -> {
            open.set(false);
            yield null;
          }
          case "hashCode" -> {
            if (hashes.incrementAndGet() == 2)
              inserting.run();
            yield System.identityHashCode(proxy);
          }
          case "equals" -> proxy == arguments[0];
          case "toString" -> path.toString();
          default -> throw new AssertionError(method.getName());
        });
  }
}
