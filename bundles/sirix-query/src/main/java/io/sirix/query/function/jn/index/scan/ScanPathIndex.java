package io.sirix.query.function.jn.index.scan;

import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.AnyNodeType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.path.PathFilter;

import java.util.Set;

/**
 * Scan the path index.
 *
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the given path index for matching nodes.", parameters = {"$doc", "$idx-no",
		"$paths"})
public final class ScanPathIndex extends AbstractScanIndex {

	/** Default function name. */
	public final static QNm DEFAULT_NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-path-index");

	/**
	 * Constructor.
	 */
	public ScanPathIndex() {
		super(DEFAULT_NAME,
				new Signature(new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany), SequenceType.NODE,
						new SequenceType(AtomicType.INR, Cardinality.One),
						new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
				true);
	}

	@Override
	public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
		final JsonDBItem doc = (JsonDBItem) args[0];
		final JsonNodeReadOnlyTrx rtx = doc.getTrx();
		final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

		if (controller == null) {
			throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
		}

		final int idx = FunUtil.getInt(args, 1, "$idx-no", -1, null, true);
		final IndexDef indexDef = controller.getIndexes().getIndexDef(idx, IndexType.PATH);

		if (indexDef == null) {
			throw new QueryException(SDBFun.ERR_INDEX_NOT_FOUND,
					"Index no %s for collection %s and document %s not found.", idx, doc.getCollection().getName(),
					doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
		}
		if (indexDef.getType() != IndexType.PATH) {
			throw new QueryException(SDBFun.ERR_INVALID_INDEX_TYPE,
					"Index no %s for collection %s and document %s is not a path index.", idx,
					doc.getCollection().getName(),
					doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
		}
		final String paths = FunUtil.getString(args, 2, "$paths", null, null, false);
		final PathFilter filter = (paths != null)
				? controller.createPathFilter(Set.of(paths.split(";")), doc.getTrx())
				: null;

		return getSequence(doc, controller.openPathIndex(doc.getTrx().getPageTrx(), indexDef, filter));
	}
}
