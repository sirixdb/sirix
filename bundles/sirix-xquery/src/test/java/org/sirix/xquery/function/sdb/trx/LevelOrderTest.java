package org.sirix.xquery.function.sdb.trx;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import junit.framework.TestCase;

public final class LevelOrderTest extends TestCase {

  final Path testResources = Paths.get("src", "test", "resources");

  final Path twitter = testResources.resolve("json").resolve("twitter.json");

  private Path sirixPath;

  @Override
  protected void setUp() throws Exception {
    sirixPath = Files.createTempDirectory("sirix");
  }

  @Override
  protected void tearDown() {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build()) {
      store.drop("mycol.jn");
    }
  }

  @Test
  public void testJson() throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final var str = twitter.toAbsolutePath().toString();
      final String loadQuery = "jn:load('mycol.jn','mydoc.jn','" + str + "')";
      new XQuery(chain, loadQuery).evaluate(ctx);

      final String levelOrderQuery = "sdb:level-order(jn:doc('mycol.jn','mydoc.jn'))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, levelOrderQuery).serialize(ctx, printWriter);
      }
    }
  }
}
