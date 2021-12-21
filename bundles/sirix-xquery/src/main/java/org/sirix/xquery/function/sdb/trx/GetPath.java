package org.sirix.xquery.function.sdb.trx;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.path.Path;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
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
 *
 */
public final class GetPath extends AbstractFunction {

  /** Move to function name. */
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
    final StructuredDBItem<?> doc = ((StructuredDBItem<?>) args[0]);

    final NodeReadOnlyTrx rtx = doc.getTrx();

    if (rtx.getResourceManager().getResourceConfig().withPathSummary) {
      try (final PathSummaryReader pathSummaryReader =
               rtx.getResourceManager().openPathSummary(rtx.getRevisionNumber())) {
        if (!pathSummaryReader.moveTo(rtx.getPathNodeKey()).hasMoved()) {
          return null;
        }
        assert pathSummaryReader.getPathNode() != null;
        final var path = pathSummaryReader.getPathNode().getPath(pathSummaryReader);

        if (!path.toString().contains("[]"))
          return new Str(path.toString());

        final var steps = path.steps();
        final var positions = new ArrayDeque<Integer>();

        for (int i = steps.size() - 1; i != -1; i--) {
          final var step = steps.get(i);

          if (step.getAxis() == Path.Axis.CHILD_ARRAY) {
            if (rtx instanceof JsonNodeReadOnlyTrx trx) {
              positions.addFirst(addArrayPosition(trx));
              trx.moveToParent();
            } else if (rtx instanceof XmlNodeReadOnlyTrx trx) {
              positions.addFirst(addArrayPosition(trx));
              trx.moveToParent();
            } else {
              throw new IllegalStateException("Not supported.");
            }
          } else {
            if (rtx instanceof JsonNodeReadOnlyTrx trx) {
              trx.moveToParent();
            } else if (rtx instanceof XmlNodeReadOnlyTrx trx) {
              trx.moveToParent();
            } else {
              throw new IllegalStateException("Not supported.");
            }
          }
        }

        var stringPath = path.toString();

        final var positionsIter = positions.iterator();

        while (positionsIter.hasNext()) {
          final var pos = positionsIter.next();
          positions.remove();
          stringPath = stringPath.replaceFirst("/\\[\\]", "/[" + pos + "]");
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

  private int addArrayPosition(XmlNodeReadOnlyTrx trx) {
    int j = 0;
    while (trx.hasLeftSibling()) {
      trx.moveToLeftSibling();
      j++;
    }
    return j;
  }
}
