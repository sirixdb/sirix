package io.sirix.query;

import io.brackit.query.Query;
import io.sirix.JsonTestHelper;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public abstract class AbstractJsonTest {

  @BeforeEach
  protected void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
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

      // Use Query to store a JSON string into the store.
      new Query(chain, storeQuery).evaluate(ctx);

      // Use Query to load a JSON database/resource.
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(assertion, out.toString());
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
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(assertion, out.toString());
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
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(assertion, out.toString());
      }
    }
  }

  public void test(final String query, final String assertionString) throws IOException {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
         final var out = new ByteArrayOutputStream();
         final var printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      Assertions.assertEquals(assertionString, out.toString());
    }
  }

  protected void query(final String query) {
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }
}
