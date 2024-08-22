package io.sirix.query.function.xml.index.scan;

import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.AnyNodeType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.name.NameFilter;

import java.util.Set;

/**
 * Scan the name index.
 *
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the given name index for matching nodes.", parameters = {"$doc", "$idx-no",
		"$names"})
public final class ScanNameIndex extends AbstractScanIndex {

	/** Default function name. */
	public final static QNm DEFAULT_NAME = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "scan-name-index");

	/**
	 * Constructor.
	 */
	public ScanNameIndex() {
		super(DEFAULT_NAME,
				new Signature(new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany), SequenceType.NODE,
						new SequenceType(AtomicType.INR, Cardinality.One),
						new SequenceType(AtomicType.QNM, Cardinality.ZeroOrOne)),
				true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
		final XmlDBNode doc = ((XmlDBNode) args[0]);
		final NodeReadOnlyTrx rtx = doc.getTrx();
		final XmlIndexController controller = (XmlIndexController) rtx.getResourceSession()
				.getRtxIndexController(rtx.getRevisionNumber());

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
		}

		final int idx = FunUtil.getInt(args, 1, "$idx-no", -1, null, true);
		final IndexDef indexDef = controller.getIndexes().getIndexDef(idx, IndexType.NAME);

		if (indexDef == null) {
			throw new QueryException(SDBFun.ERR_INDEX_NOT_FOUND,
					"Index no %s for collection %s and document %s not found.", idx, doc.getCollection().getName(),
					doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
		}
		if (indexDef.getType() != IndexType.NAME) {
			throw new QueryException(SDBFun.ERR_INVALID_INDEX_TYPE,
					"Index no %s for collection %s and document %s is not a path index.", idx,
					doc.getCollection().getName(),
					doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
		}

		final String names = FunUtil.getString(args, 2, "$names", null, null, false);
		final NameFilter filter = (names != null) ? controller.createNameFilter(Set.of(names.split(";"))) : null;

		return getSequence(doc, controller.openNameIndex(doc.getTrx().getPageTrx(), indexDef, filter));
	}
}
