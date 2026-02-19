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
      wtx.moveTo(6);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\":[]}"));
    }

    // Initialize query context and store.
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final String firstPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 25))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, firstPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[]/[4]", out.toString());
      }

      final String secondPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 11))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, secondPathQuery).serialize(ctx, printWriter);
        assertEquals("/bar/helloo", out.toString());
      }

      final String thirdPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 21))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, thirdPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[1]/baz", out.toString());
      }

      final String fourthPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 28))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new Query(chain, fourthPathQuery).serialize(ctx, printWriter);
        assertEquals("/foo/[3]/foo/[]", out.toString());
      }
    }
  }
}

