package io.sirix.query.function.jn.io;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class LoadIntegrationTest {

  private final Path sirixPath = PATHS.PATH1.getFile();

  private final Path testResources = Paths.get("src", "test", "resources");

  private final Path jsonArray = testResources.resolve("json").resolve("array.json");

  private final Path jsonObject = testResources.resolve("json").resolve("object.json");

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to load a JSON file into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('json-path1','mydoc.jn','" + str + "')";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessage() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to load a JSON file into the store with a commit message.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query =
          "jn:load('json-path1','mydoc.jn','" + str + "',true(),{\"commitMessage\": \"commitMessage\"})";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessageAndCommitTimestamp() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to load a JSON file into the store with commit message and timestamp.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('json-path1','mydoc.jn','" + str
          + "',true(),{\"commitMessage\": \"commitMessage\",\"commitTimestamp\": \"2021-05-01T00:00:00\"})";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath.getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to load multiple JSON files into the store.
      final var array = jsonArray.toAbsolutePath().toString();
      final var object = jsonObject.toAbsolutePath().toString();
      final String query = "jn:load('json-path1',(),('" + array + "','" + object + "'))";
      new Query(chain, query).evaluate(ctx);

      // Use Query to add a JSON file to the collection.
      final String queryAdd = "jn:load('json-path1',(),'" + array + "',false())";
      new Query(chain, queryAdd).evaluate(ctx);

      // Use Query to add JSON files to the collection.
      final String queryAddStrings = "jn:load('json-path1',(),('" + array + "','" + object + "'),false())";
      new Query(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
