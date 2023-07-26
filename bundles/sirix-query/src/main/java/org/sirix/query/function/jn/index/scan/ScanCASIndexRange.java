package org.sirix.query.function.jn.index.scan;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.expr.Cast;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.Type;
import org.brackit.xquery.jdm.type.AnyJsonItemType;
import org.brackit.xquery.jdm.type.AtomicType;
import org.brackit.xquery.jdm.type.Cardinality;
import org.brackit.xquery.jdm.type.SequenceType;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.annotation.FunctionAnnotation;
import org.sirix.access.trx.node.json.JsonIndexController;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.cas.CASFilterRange;
import org.sirix.index.path.json.JsonPCRCollector;
import org.sirix.query.function.FunUtil;
import org.sirix.query.function.sdb.SDBFun;
import org.sirix.query.json.JsonDBItem;

import java.util.Set;

/**
 * Function for scanning for an index range in a CAS index.
 *
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the given CAS index for matching nodes.", parameters = {"$coll", "$document",
    "$idx-no", "$low-key", "$high-key", "$include-low-key", "$include-high-key", "$paths"})
public final class ScanCASIndexRange extends AbstractScanIndex {

  public final static QNm DEFAULT_NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-cas-index-range");

  public ScanCASIndexRange() {
    super(DEFAULT_NAME,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany), SequenceType.NODE,
            new SequenceType(AtomicType.INR, Cardinality.One), new SequenceType(AtomicType.ANA, Cardinality.One),
            new SequenceType(AtomicType.ANA, Cardinality.One), new SequenceType(AtomicType.BOOL, Cardinality.One),
            new SequenceType(AtomicType.BOOL, Cardinality.One),
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

    final IndexDef indexDef = controller.getIndexes().getIndexDef(idx, IndexType.CAS);

    if (indexDef == null) {
      throw new QueryException(SDBFun.ERR_INDEX_NOT_FOUND, "Index no %s for collection %s and document %s not found.",
          idx, doc.getCollection().getName(),
          doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
    }
    if (indexDef.getType() != IndexType.CAS) {
      throw new QueryException(SDBFun.ERR_INVALID_INDEX_TYPE,
          "Index no %s for collection %s and document %s is not a CAS index.", idx, doc.getCollection().getName(),
          doc.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
    }

    final Type keyType = indexDef.getContentType();
    final Atomic min = Cast.cast(sctx, (Atomic) args[2], keyType, true);
    final Atomic max = Cast.cast(sctx, (Atomic) args[3], keyType, true);
    final boolean incMin = FunUtil.getBoolean(args, 4, "$include-low-key", true, true);
    final boolean incMax = FunUtil.getBoolean(args, 5, "$include-high-key", true, true);
    final String paths = FunUtil.getString(args, 6, "$paths", null, null, false);
    final Set<String> setOfPaths = paths == null
        ? Set.of()
        : Set.of(paths.split(";"));
    final CASFilterRange filter =
        controller.createCASFilterRange(setOfPaths, min, max, incMin, incMax, new JsonPCRCollector(rtx));

    return getSequence(doc, controller.openCASIndex(doc.getTrx().getPageTrx(), indexDef, filter));
  }
}
