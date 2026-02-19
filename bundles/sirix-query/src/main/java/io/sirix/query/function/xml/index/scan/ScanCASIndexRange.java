package io.sirix.query.function.xml.index.scan;

import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.XmlDBNode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.type.AnyNodeType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.path.xml.XmlPCRCollector;

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

  public final static QNm DEFAULT_NAME = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "scan-cas-index-range");

  public ScanCASIndexRange() {
    super(DEFAULT_NAME, new Signature(new SequenceType(AnyNodeType.ANY_NODE, Cardinality.ZeroOrMany), SequenceType.NODE,
        new SequenceType(AtomicType.INR, Cardinality.One), new SequenceType(AtomicType.ANA, Cardinality.One),
        new SequenceType(AtomicType.ANA, Cardinality.One), new SequenceType(AtomicType.BOOL, Cardinality.One),
        new SequenceType(AtomicType.BOOL, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
        true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
    final XmlDBNode doc = (XmlDBNode) args[0];
    final XmlNodeReadOnlyTrx rtx = doc.getTrx();
    final XmlIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

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
        controller.createCASFilterRange(setOfPaths, min, max, incMin, incMax, new XmlPCRCollector(rtx));

    return getSequence(doc, controller.openCASIndex(doc.getTrx().getStorageEngineReader(), indexDef, filter));
  }
}
