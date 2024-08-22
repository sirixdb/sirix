package io.sirix.query.function.xml.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DTD;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;

import java.time.Instant;
import java.util.ArrayList;

public final class OpenRevisions extends AbstractFunction {

	/** Doc function name. */
	public final static QNm OPEN_REVISIONS = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "open-revisions");

	/**
	 * Constructor.
	 *
	 * @param name
	 *            the name of the function
	 * @param signature
	 *            the signature of the function
	 */
	public OpenRevisions(final QNm name, final Signature signature) {
		super(name, signature, true);
	}

	@Override
	public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
		if (args.length != 4) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final var col = (XmlDBCollection) ctx.getNodeStore().lookup(((Str) args[0]).stringValue());

		if (col == null) {
			throw new QueryException(new QNm("No valid arguments specified!"));
		}

		final var expResName = ((Str) args[1]).stringValue();
		final var millis = ((DateTime) args[2]).subtract(new DateTime("1970-01-01T00:00:00-00:00"))
				.divide(new DTD(false, (byte) 0, (byte) 0, (byte) 0, 1000)).longValue();

		final var startPointInTime = Instant.ofEpochMilli(millis);
		final var endDateTime = ((DateTime) args[3]).stringValue();
		final var endPointInTime = Instant.parse(endDateTime);

		if (!startPointInTime.isBefore(endPointInTime))
			throw new QueryException(new QNm("No valid arguments specified!"));

		final var startDocNode = col.getDocument(expResName, startPointInTime);
		final var endDocNode = col.getDocument(expResName, endPointInTime);

		var startRevision = startDocNode.getTrx().getRevisionNumber();
		final int endRevision = endDocNode.getTrx().getRevisionNumber();

		final var documentNodes = new ArrayList<XmlDBNode>();
		documentNodes.add(startDocNode);

		while (++startRevision < endRevision) {
			documentNodes.add(col.getDocument(expResName, startRevision));
		}

		documentNodes.add(endDocNode);

		return new ItemSequence(documentNodes.toArray(new XmlDBNode[0]));
	}
}
