package io.sirix.query.function.jn.temporal;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBObject;
import org.brackit.xquery.XQuery;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.JsonDocumentCreator;

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
