package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.XQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sirix.JsonTestHelper;
import org.sirix.xquery.SirixCompileChain;
import org.sirix.xquery.SirixQueryContext;
import org.sirix.xquery.json.BasicJsonDBStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class GetPathTest {
  @Before
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @After
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @Test
  public void test() throws IOException {
    JsonTestHelper.createTestDocument();

    // Initialize query context and store.
    try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                        .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                                                        .build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final String firstPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 25))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, firstPathQuery).serialize(ctx, printWriter);
        Assert.assertEquals("/tada/[0]/[4]", out.toString());
      }

      final String secondPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 11))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, secondPathQuery).serialize(ctx, printWriter);
        Assert.assertEquals("/bar/helloo", out.toString());
      }

      final String thirdPathQuery = "sdb:path(sdb:select-item(jn:doc('json-path1','shredded'), 21))";

      try (final var out = new ByteArrayOutputStream(); final var printWriter = new PrintWriter(out)) {
        new XQuery(chain, thirdPathQuery).serialize(ctx, printWriter);
        Assert.assertEquals("/tada/[1]/baz", out.toString());
      }
    }
  }
}

