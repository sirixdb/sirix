package io.sirix.query.function.jn.io;

import java.nio.file.Path;
import java.nio.file.Paths;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.brackit.query.Query;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import junit.framework.TestCase;

public final class LoadIntegrationTest extends TestCase {

  final Path testResources = Paths.get("src", "test", "resources");

  final Path jsonArray = testResources.resolve("json").resolve("array.json");

  final Path jsonObject = testResources.resolve("json").resolve("object.json");

  @BeforeEach
  protected void setUp() throws Exception {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  protected void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('mycol.jn','mydoc.jn','" + str + "')";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessage() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = STR."jn:load('mycol.jn','mydoc.jn','\{str}',true(),{\"commitMessage\": \"commitMessage\"})";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessageAndCommitTimestamp() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = STR."jn:load('mycol.jn','mydoc.jn','\{str}',true(),{\"commitMessage\": \"commitMessage\",\"commitTimestamp\": \"2021-05-01T00:00:00\"})";
      new Query(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use Query to load multiple JSON files into the store.
      final var array = jsonArray.toAbsolutePath().toString();
      final var object = jsonObject.toAbsolutePath().toString();
      final String query = STR."jn:load('mycol.jn',(),('\{array}','\{object}'))";
      new Query(chain, query).evaluate(ctx);

      // Use Query to add a JSON file to the collection.
      final String queryAdd = STR."jn:load('mycol.jn',(),'\{array}',false())";
      new Query(chain, queryAdd).evaluate(ctx);

      // Use Query to add JSON files to the collection.
      final String queryAddStrings = STR."jn:load('mycol.jn',(),('\{array}','\{object}'),false())";
      new Query(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
