package org.sirix.query.function.jn.temporal;

import org.brackit.xquery.XQuery;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.JsonDocumentCreator;
import org.sirix.query.SirixCompileChain;
import org.sirix.query.SirixQueryContext;
import org.sirix.query.json.JsonDBObject;

import java.io.IOException;

public final class SetupRevisions {
  private SetupRevisions() {}

  public static void setupRevisions(final SirixQueryContext ctx, final SirixCompileChain chain) throws IOException {
    final var storeQuery = "jn:store('json-path1','mydoc.jn','" + JsonDocumentCreator.JSON + "')";
    new XQuery(chain, storeQuery).evaluate(ctx);

    final var openDocQuery = "jn:doc('json-path1','mydoc.jn')";
    final var object = (JsonDBObject) new XQuery(chain, openDocQuery).evaluate(ctx);

    try (final var wtx = object.getTrx().getResourceSession().beginNodeTrx()) {
      wtx.moveTo(3);

      try (final var reader = JsonShredder.createStringReader("{\"foo\":\"bar\"}")) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();

        wtx.moveTo(11);
        wtx.remove();
        wtx.commit();
      }
    }
  }
}
