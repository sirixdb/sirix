package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.node.stream.ArrayStream;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    final JsonDBCollection collection = store.create("defaultProjection", "resource",
        new JsonReader(new StringReader("[{\"value\":1}]")), projection);

    try (final var session = collection.getDatabase().beginResourceSession("resource");
         final var rtx = session.beginNodeReadOnlyTrx()) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
      final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNotNull(controller.getIndexes().getIndexDef(0, projection.toIndexDef().getType()));
      assertTrue(controller.hasProjectionIndex());
      final var handle = controller.openProjectionIndex(rtx.getStorageEngineReader(),
          new String[] { "[]" }, new String[] { "value" });
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
    final JsonDBCollection collection = store.create("disabledDewey", "resource",
        new JsonReader(new StringReader("[{\"value\":1}]")), projection);

    try (final var session = collection.getDatabase().beginResourceSession("resource");
         final var rtx = session.beginNodeReadOnlyTrx()) {
      assertFalse(session.getResourceConfig().areDeweyIDsStored);
      final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNotNull(controller.getIndexes().getIndexDef(0, projection.toIndexDef().getType()));
      assertTrue(controller.hasProjectionIndex());
      final var handle = controller.openProjectionIndex(rtx.getStorageEngineReader(),
          new String[] { "[]" }, new String[] { "value" });
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
}
