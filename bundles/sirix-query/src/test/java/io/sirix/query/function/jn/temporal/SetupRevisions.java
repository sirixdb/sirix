package io.sirix.query.function.jn.temporal;

import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.JsonDBObject;
import io.brackit.query.Query;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.JsonDocumentCreator;

import java.io.IOException;

public final class SetupRevisions {
  private SetupRevisions() {}

  /**
   * Fusion flag — drives the physical nodeKey layout of the shredded document. When fusion is
   * on, primitive-valued object fields collapse into a single OBJECT_NAMED_* record, so the
   * hardcoded nodeKeys below shift to lower values.
   */
  public static final boolean FUSED = true;

  /** Root "foo" array nodeKey. LEGACY: ARRAY (3 because OBJECT_KEY at 2). FUSED: OBJECT_NAMED_ARRAY
   *  collapses OBJECT_KEY+ARRAY into one record at 2. */
  public static final long ARRAY_KEY = FUSED ? 2L : 3L;

  /** LEGACY: OBJECT_KEY "helloo" at 11. FUSED: OBJECT_NAMED_BOOLEAN "helloo" at 8 (fused
   *  OBJECT_KEY+BOOLEAN_VALUE pair). */
  public static final long HELLOO_KEY = FUSED ? 8L : 11L;

  /** Last item (empty ARRAY) inside the "tada" array — legacy 25, fused 17. */
  public static final long TADA_LAST_ITEM_KEY = FUSED ? 17L : 25L;

  public static void setupRevisions(final SirixQueryContext ctx, final SirixCompileChain chain) throws IOException {
    final var storeQuery = "jn:store('json-path1','mydoc.jn','" + JsonDocumentCreator.JSON + "')";
    new Query(chain, storeQuery).evaluate(ctx);

    final var openDocQuery = "jn:doc('json-path1','mydoc.jn')";
    final var object = (JsonDBObject) new Query(chain, openDocQuery).evaluate(ctx);

    try (final var wtx = object.getTrx().getResourceSession().beginNodeTrx()) {
      wtx.moveTo(ARRAY_KEY);

      try (final var reader = JsonShredder.createStringReader("{\"foo\":\"bar\"}")) {
        wtx.insertSubtreeAsFirstChild(reader);
        wtx.commit();

        wtx.moveTo(HELLOO_KEY);
        wtx.remove();
        wtx.commit();

        wtx.moveTo(TADA_LAST_ITEM_KEY);
        wtx.insertArrayAsRightSibling();
        wtx.commit();
      }
    }
  }
}
