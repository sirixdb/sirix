package org.sirix.xquery.function.jn.io;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.brackit.xquery.XQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.sirix.JsonTestHelper;
import org.sirix.JsonTestHelper.PATHS;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
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

      // Use XQuery to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('mycol.jn','mydoc.jn','" + str + "')";
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessage() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('mycol.jn','mydoc.jn','" + str + "',true(),'commitMessage')";
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testWithCommitMessageAndCommitTimestamp() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      final String query = "jn:load('mycol.jn','mydoc.jn','" + str + "',true(),'commitMessage',xs:dateTime(\"2021-05-01T00:00:00\"))";
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(PATHS.PATH1.getFile()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to load multiple JSON files into the store.
      final var array = jsonArray.toAbsolutePath().toString();
      final var object = jsonObject.toAbsolutePath().toString();
      final String query = "jn:load('mycol.jn',(),('" + array + "','" + object + "'))";
      new XQuery(chain, query).evaluate(ctx);

      // Use XQuery to add a JSON file to the collection.
      final String queryAdd = "jn:load('mycol.jn',(),'" + array + "',false())";
      new XQuery(chain, queryAdd).evaluate(ctx);

      // Use XQuery to add JSON files to the collection.
      final String queryAddStrings = "jn:load('mycol.jn',(),('" + array + "','" + object + "'),false())";
      new XQuery(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
