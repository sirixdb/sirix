package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.XQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sirix.JsonTestHelper;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

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
         final var resourceManager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = resourceManager.beginNodeTrx()) {
      wtx.moveTo(6);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"foo\":[]}"));
    }

    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final String firstPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 25))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, firstPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[0]/[4]", out.toString());
      }

      final String secondPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 11))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, secondPathQuery).serialize(ctx, printWriter);
        assertEquals("/bar/helloo", out.toString());
      }

      final String thirdPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 21))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, thirdPathQuery).serialize(ctx, printWriter);
        assertEquals("/tada/[1]/baz", out.toString());
      }

      final String fourthPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 28))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, fourthPathQuery).serialize(ctx, printWriter);
        assertEquals("/foo/[3]/foo/[0]", out.toString());
      }
    }
  }
}

