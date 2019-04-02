package org.sirix.xquery.function.jn.io;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import junit.framework.TestCase;

public final class LoadIntegrationTest extends TestCase {

  final Path testResources = Paths.get("src").resolve("test").resolve("resources");

  final Path jsonArray = testResources.resolve("json").resolve("array.json");

  final Path jsonObject = testResources.resolve("json").resolve("object.json");

  @Test
  public void test() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final var str = jsonArray.toAbsolutePath().toString();
      System.out.println("Loading from a path:");
      final String query = "jn:load('mycol.jn','mydoc.jn','" + str + "')";
      System.out.println(query);
      new XQuery(chain, query).evaluate(ctx);
    }
  }

  @Test
  public void testMultipleStrings() {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to load multiple JSON files into the store.
      final var array = jsonArray.toAbsolutePath().toString();
      final var object = jsonObject.toAbsolutePath().toString();
      System.out.println("Loading from paths:");
      final String query = "jn:load('mycol.jn',(),('" + array + "','" + object + "'))";
      System.out.println(query);
      new XQuery(chain, query).evaluate(ctx);

      // Use XQuery to add a JSON file to the collection.
      System.out.println("Loading from a path (add):");
      final String queryAdd = "jn:load('mycol.jn',(),'" + array + "',false())";
      System.out.println(queryAdd);
      new XQuery(chain, queryAdd).evaluate(ctx);

      // Use XQuery to add JSON files to the collection.
      System.out.println("Loading from paths (add):");
      final String queryAddStrings = "jn:load('mycol.jn',(),('" + array + "','" + object + "'),false())";
      System.out.println(queryAddStrings);
      new XQuery(chain, queryAddStrings).evaluate(ctx);
    }
  }
}
