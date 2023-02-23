package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.xquery.StructuredDBItem;
import org.sirix.xquery.function.sdb.SDBFun;

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
   * @param name      the name of the function
   * @param signature the signature of the function
   */
  public GetPath(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    final NodeReadOnlyTrx rtx = doc.getTrx();

    if (rtx.getResourceSession().getResourceConfig().withPathSummary) {
      try (final PathSummaryReader pathSummaryReader = rtx.getResourceSession()
                                                          .openPathSummary(rtx.getRevisionNumber())) {
        if (!pathSummaryReader.moveTo(rtx.getPathNodeKey())) {
          return null;
        }
        assert pathSummaryReader.getPathNode() != null;
        final var path = pathSummaryReader.getPathNode().getPath(pathSummaryReader);

        if (!(rtx instanceof final JsonNodeReadOnlyTrx trx))
          return new Str(path.toString());

        if (!path.toString().contains("[]"))
          return new Str(path.toString());

        final var steps = path.steps();
        final var positions = new ArrayDeque<Integer>();

        for (int i = steps.size() - 1; i != -1; i--) {
          final var step = steps.get(i);

          if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
            positions.addFirst(addArrayPosition(trx));
            trx.moveToParent();
          } else {
            trx.moveToParent();
          }
        }

        var stringPath = path.toString();

        for (Integer pos : positions) {
          positions.remove();
          stringPath = stringPath.replaceFirst("/\\[]", "/[" + pos + "]");
        }

        return new Str(stringPath);
      }
    }

    return null;
  }

  private int addArrayPosition(JsonNodeReadOnlyTrx trx) {
    int j = 0;
    while (trx.hasLeftSibling()) {
      trx.moveToLeftSibling();
      j++;
    }
    return j;
  }
}
