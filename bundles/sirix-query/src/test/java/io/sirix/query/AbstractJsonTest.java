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
import java.util.regex.Pattern;

public abstract class AbstractJsonTest {

  /**
   * Fusion-mode flag. When {@code true} the shredder emits fused OBJECT_NAMED_* records for
   * primitive-valued object fields, halving the node count for such fields. Tests that embed
   * concrete {@code nodeKey} values in their expected output must pick the fused variant.
   */
  protected static final boolean FUSED_NAMED_PRIMITIVES = true;

  /**
   * Pattern matching integer nodeKey annotations inside serialized output, e.g. {@code
   * "nodeKey":11} or {@code "nodekey": 7}. Used to strip keys when comparing mode-invariant
   * shapes.
   */
  private static final Pattern NODE_KEY_NUMERIC = Pattern.compile(
      "(\"(?:nodeKey|nodekey)\"\\s*:\\s*)(-?\\d+)");

  @BeforeEach
  protected void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  protected void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  public void test(String storeQuery, String query, String assertion) throws IOException {
    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
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

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
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

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(assertion, out.toString());
      }
    }
  }

  public void test(final String query, final String assertionString) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store);
        final var out = new ByteArrayOutputStream();
        final var printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      Assertions.assertEquals(assertionString, out.toString());
    }
  }

  protected void query(final String query) {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, query).evaluate(ctx);
    }
  }

  /**
   * Runs the 3-argument test variant (store, query, expected) but asserts on a representation
   * that strips concrete nodeKey integers so the test matches in both fusion modes. Useful when
   * a query embeds {@code sdb:nodekey($item)} in the output — fusion halves node counts for
   * primitive-valued object fields so these integers shift lower under fusion.
   */
  public void testIgnoreNodeKeys(String storeQuery, String query, String assertion) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, storeQuery).evaluate(ctx);
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(stripNodeKeys(assertion), stripNodeKeys(out.toString()));
      }
    }
  }

  /** Four-arg variant (store, updateOrIndex, query, expected) with nodeKey stripping. */
  public void testIgnoreNodeKeys(String storeQuery, String updateOrIndexQuery, String query, String assertion)
      throws IOException {
    query(storeQuery);
    query(updateOrIndexQuery);

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, query).serialize(ctx, printWriter);
        Assertions.assertEquals(stripNodeKeys(assertion), stripNodeKeys(out.toString()));
      }
    }
  }

  /**
   * Replaces every {@code "nodeKey":<int>} or {@code "nodekey":<int>} with a placeholder so two
   * outputs differing only in their embedded nodeKey integers compare equal. The remaining
   * JSON shape (field order, values, structure) must still match exactly.
   */
  protected static String stripNodeKeys(final String s) {
    if (s == null) {
      return null;
    }
    return NODE_KEY_NUMERIC.matcher(s).replaceAll("$1<nk>");
  }
}
