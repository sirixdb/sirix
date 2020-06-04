package org.sirix.xquery.function.jn.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.DTD;
import org.brackit.xquery.atomic.DateTime;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.sequence.ItemSequence;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.xquery.json.JsonDBCollection;
import org.sirix.xquery.json.JsonDBItem;

import java.time.Instant;
import java.util.ArrayList;

public final class OpenRevisions extends AbstractFunction {

  /** Doc function name. */
  public final static QNm OPEN_REVISIONS = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open-revisions");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public OpenRevisions(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 4) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final var col = (JsonDBCollection) ctx.getJsonItemStore().lookup(((Str) args[0]).stringValue());

    if (col == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final var expResName = ((Str) args[1]).stringValue();
    final var millis = ((DateTime) args[2]).subtract(new DateTime("1970-01-01T00:00:00-00:00"))
                                           .divide(new DTD(false, (byte) 0, (byte) 0, (byte) 0, 1000))
                                           .longValue();

    final var startPointInTime = Instant.ofEpochMilli(millis);
    final var endDateTime = ((DateTime) args[3]).stringValue();
    final var endPointInTime = Instant.parse(endDateTime);

    if (!startPointInTime.isBefore(endPointInTime))
      throw new QueryException(new QNm("No valid arguments specified!"));

    final var startDocNode = col.getDocument(expResName, startPointInTime);
    final var endDocNode = col.getDocument(expResName, endPointInTime);

    var startRevision = startDocNode.getTrx().getRevisionNumber();
    final int endRevision = endDocNode.getTrx().getRevisionNumber();

    final var documentNodes = new ArrayList<JsonDBItem>();
    documentNodes.add(startDocNode);

    while (++startRevision < endRevision) {
      documentNodes.add(col.getDocument(expResName, startRevision));
    }

    documentNodes.add(endDocNode);

    return new ItemSequence(documentNodes.toArray(new JsonDBItem[0]));
  }
}
