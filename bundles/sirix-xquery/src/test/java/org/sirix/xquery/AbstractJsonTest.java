package org.sirix.xquery;

import org.brackit.xquery.XQuery;
import org.sirix.JsonTestHelper;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;

public abstract class AbstractJsonTest {
  protected void setUp() {
    JsonTestHelper.deleteEverything();
  }

  protected void tearDown() {
    JsonTestHelper.closeEverything();
  }

  public void test(String storeQuery, String query, String assertion) throws IOException {
    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {

      // Use XQuery to store a JSON string into the store.
      new XQuery(chain, storeQuery).evaluate(ctx);

      // Use XQuery to load a JSON database/resource.
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, query).serialize(ctx, printWriter);
        assertEquals(assertion, out.toString());
      }
    }
  }

  public void test(String storeQuery, String updateOrIndexQuery, String query, String assertion) throws IOException {
    query(storeQuery);
    query(updateOrIndexQuery);

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, query).serialize(ctx, printWriter);
        assertEquals(assertion, out.toString());
      }
    }
  }

  public void test(String storeQuery, String updateOrIndexQuery1, String updateOrIndexQuery2, String query,
      String assertion) throws IOException {
    query(storeQuery);
    query(updateOrIndexQuery1);
    query(updateOrIndexQuery2);

    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, query).serialize(ctx, printWriter);
        assertEquals(assertion, out.toString());
      }
    }
  }

  public void test(final String query, final String assertionString) throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
         final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
      new XQuery(chain, query).serialize(ctx, printWriter);
      assertEquals(assertionString, out.toString());
    }
  }

  protected void query(final String query) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new XQuery(chain, query).evaluate(ctx);
    }
  }
}
