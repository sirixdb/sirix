package io.sirix.query.function.sdb.trx;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.path.Path;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.query.StructuredDBItem;
import io.sirix.query.function.sdb.SDBFun;

import java.util.ArrayDeque;

/**
 * <p>
 * Function for getting a path. The result is the path. Supported signature is:
 * </p>
 * <ul>
 * <li><code>sdb:path($doc as xs:structured-item) as xs:string</code></li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 */
public final class GetPath extends AbstractFunction {

  /**
   * Get path function name.
   */
  public final static QNm GET_PATH = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "path");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public GetPath(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> document = ((StructuredDBItem<?>) args[0]);

    final NodeReadOnlyTrx rtx = document.getTrx();

    if (rtx.getResourceSession().getResourceConfig().withPathSummary) {
      try (final PathSummaryReader pathSummaryReader =
          rtx.getResourceSession().openPathSummary(rtx.getRevisionNumber())) {
        if (!pathSummaryReader.moveTo(rtx.getPathNodeKey())) {
          return null;
        }
        assert pathSummaryReader.getPathNode() != null;
        var path = pathSummaryReader.getPath();

        if (path == null) {
          return null;
        }

        if (!(rtx instanceof final JsonNodeReadOnlyTrx trx))
          return new Str(path.toString());

        if (!path.toString().contains("[]"))
          return new Str(path.toString());

        final var steps = path.steps();
        final var positions = new ArrayDeque<Integer>();

        // track when the previous step's moveToParent landed on a fused
        // OBJECT_NAMED_* — its single tree level represents BOTH the OBJECT_KEY axis-layer
        // AND its inner OBJECT/ARRAY value. The next FIELD step's moveToParent would then
        // overshoot the containing OBJECT, dropping us at its parent. Skip the FIELD-step
        // moveToParent in that case.
        boolean fusedSkipNextFieldMTP = false;
        for (int i = steps.size() - 1; i != -1; i--) {
          final var step = steps.get(i);

          if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
            positions.addFirst(addArrayPosition(trx));
            trx.moveToParent();
            // If after CHILD_ARRAY moveToParent we're at OBJECT (i.e. came from a fused
            // OBJECT_NAMED_ARRAY whose children are JSON OBJECT array elements), the next
            // FIELD step has nothing left to do for path-accounting. Same idea covers
            // OBJECT_NAMED_OBJECT (children are nested fused records).
            // For LEGACY ARRAY → OBJECT_KEY transition, kind is OBJECT_KEY (not OBJECT) so
            // the flag stays false and the FIELD step's moveToParent runs as before.
            final NodeKind postKind = trx.getKind();
            fusedSkipNextFieldMTP = postKind == NodeKind.OBJECT
                || postKind == NodeKind.OBJECT_NAMED_OBJECT;
          } else {
            // CHILD_OBJECT_FIELD step. Skip the moveToParent if we just collapsed the
            // OBJECT_KEY layer through a fused record (see flag above).
            if (!fusedSkipNextFieldMTP) {
              trx.moveToParent();
            }
            fusedSkipNextFieldMTP = false;
          }
        }

        var stringPath = path.toString();

        for (Integer pos : positions) {
          positions.remove();
          stringPath = stringPath.replaceFirst("/\\[]", "/[" + pos + "]");
        }

        stringPath = stringPath.replaceAll("/\\[-1]", "/[]");

        return new Str(stringPath);
      }
    }

    return null;
  }

  private int addArrayPosition(JsonNodeReadOnlyTrx rtx) {
    // legacy OBJECT_KEY+ARRAY pair → check `parent==OBJECT_KEY && isArray`.
    // Fused OBJECT_NAMED_ARRAY collapses both layers into one record — when we're sitting on
    // a fused named array, semantically it IS the named-field layer (not an array element).
    // Return -1 to render as "/[]" in both modes; it preserves the "no specific element index"
    // intent the legacy check encoded.
    if (rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY) {
      return -1;
    }
    if (rtx.getParentKind() == NodeKind.OBJECT_NAMED_OBJECT && rtx.isArray()) {
      return -1;
    }

    int j = 0;
    while (rtx.hasLeftSibling()) {
      rtx.moveToLeftSibling();
      j++;
    }
    return j;
  }
}
