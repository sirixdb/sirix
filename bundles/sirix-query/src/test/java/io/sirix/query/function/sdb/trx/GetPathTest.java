package io.sirix.query.function.sdb.trx;

import io.sirix.JsonTestHelper;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.brackit.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetPathTest {
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
    JsonTestHelper.createTestDocument();
    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(JsonTestHelper.PATHS.PATH1.getFile());
        final var resourceSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = resourceSession.beginNodeTrx()) {
      // iter#32 P2 fusion: legacy key 6 was the last foo-array element (2.33 NUMBER_VALUE);
      // under structural fusion the key shifts to 5 (foo array elements 3,4,5).
      wtx.moveTo(5);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\":[]}"));
    }

    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      // iter#32 P2 fusion: legacy "tada[4]" empty ARRAY at key 25 -> 17 in fused mode (8 records
      // collapsed in the prefix path summary).
      final String firstPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 17))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, firstPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[]/[4]", out.toString());
      }

      // iter#32 P2 fusion: legacy OBJECT_KEY "helloo" was at key 11; in fused mode it is the
      // collapsed OBJECT_NAMED_BOOLEAN at key 8.
      final String secondPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 8))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, secondPathQuery).serialize(ctx, printWriter);
        assertEquals("/bar/helloo", out.toString());
      }

      // iter#32 P2 fusion: legacy OBJECT_KEY "baz" inside tada[1] was at key 21; in fused mode
      // it is the collapsed OBJECT_NAMED_BOOLEAN at key 14.
      final String thirdPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 14))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, thirdPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[1]/baz", out.toString());
      }

      // iter#32 P2 fusion: insertion of {"foo":[]} as right sibling of foo[2] adds 2 records
      // (OBJECT key=18, OBJECT_NAMED_ARRAY foo key=19). The inserted nested empty array is the
      // OBJECT_NAMED_ARRAY itself.
      final String fourthPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 19))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, fourthPathQuery).serialize(ctx, printWriter);
        assertEquals("/foo/[3]/foo/[]", out.toString());
      }
    }
  }
}

