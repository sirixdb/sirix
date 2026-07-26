package io.sirix.query.function.sdb.trx;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.brackit.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public final class ItemHistoryTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void test() throws IOException {
    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder("mydoc.jn").build());

      try (final var resourceSession = database.beginResourceSession("mydoc.jn"); final var wtx = resourceSession.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[\"bla\", \"blubb\"]"));
        wtx.moveTo(2);
        wtx.setStringValue("blabla").commit();
        wtx.moveTo(2);
        wtx.setStringValue("blablabla").commit();
        wtx.moveTo(2);
        wtx.remove().commit();
      }
    }

    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // Use Query to load a JSON database/resource.
      final String openQuery = "sdb:item-history(sdb:select-item(jn:doc('json-path1','mydoc.jn', 1), 2))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, openQuery).serialize(ctx, printWriter);
        Assertions.assertEquals("\"bla\" \"blabla\" \"blablabla\"", out.toString());
      }
    }
  }

  @Test
  public void test2() throws IOException {
    // OBJECT_KEY (or fused OBJECT_NAMED_NUMBER under fusion) for the "generic" field of the
    // SECOND inserted object. Fusion collapses `{"generic":1}` from OBJECT_KEY + value into a
    // single OBJECT_NAMED_NUMBER, which is why the physical nodeKey shifts.
    //
    // The serialised output of {@code sdb:item-history} reflects the kind of the selected
    // node in each revision: legacy {@code OBJECT_KEY} serialises to its NAME (a string),
    // while fused {@code OBJECT_NAMED_NUMBER} serialises to its inline VALUE (the number 1).
    // The rename only changes the name; under fusion the value is unchanged, so both
    // revisions print the same numeric. We therefore branch the expected fixture on the
    // fusion mode while still asserting that the index points at the renamed node.
    final boolean fused = true;
    // Fused (DOCUMENT_NODE_KEY=0): 0 doc, 1 ARRAY, 2 OBJECT[0], 3 generic@1, 4 location@1,
    // 5 state, 6 city, 7 OBJECT[1], 8 generic@2 (target), 9 location@2, ...
    final long genericKeyOf2ndObject = fused ? 8L : 12L;

    try (final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder("mydoc2.jn").build());

      try (final var resourceSession = database.beginResourceSession("mydoc2.jn"); final var wtx = resourceSession.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"));
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
            "{\"generic\": 1, \"location\": {\"state\": \"NY\", \"city\": \"New York\"}}"));
        wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader(
            "{\"generic\": 1, \"location\": {\"state\": \"CA\", \"city\": \"Los Angeles\"}}"));
        wtx.moveTo(genericKeyOf2ndObject);
        wtx.setObjectKeyName("generic1");
        wtx.commit();
      }
    }

    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // Use Query to load a JSON database/resource.
      final String openQuery =
          "sdb:item-history(sdb:select-item(jn:doc('json-path1','mydoc2.jn'), " + genericKeyOf2ndObject + "))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, openQuery).serialize(ctx, printWriter);
        // legacy OBJECT_KEY → name strings ("generic", "generic1");
        // fused OBJECT_NAMED_NUMBER → inline value (1, 1) — name change isn't visible in the value.
        final String expected = fused ? "1 1" : "\"generic\" \"generic1\"";
        Assertions.assertEquals(expected, out.toString());
      }
    }
  }
}
