package org.sirix.xquery.function.jn.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.brackit.xquery.XQuery;
import org.junit.Test;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;
import junit.framework.TestCase;

public final class DocIntegrationTest extends TestCase {

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
  public void test() throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(sirixPath).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      final String storeQuery = "jn:store('mycol.jn','mydoc.jn','[\"bla\", \"blubb\"]')";
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      final String openQuery = "jn:doc('mycol.jn','mydoc.jn')";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, openQuery).serialize(ctx, printWriter);
        assertEquals("[\"bla\",\"blubb\"]", out.toString());
      }
    }
  }
}
