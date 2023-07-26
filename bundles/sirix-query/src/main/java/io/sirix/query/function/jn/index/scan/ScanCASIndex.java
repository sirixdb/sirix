package io.sirix.query.function.jn.index.scan;

import com.google.common.collect.ImmutableSet;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
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
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.cas.CASFilter;
import io.sirix.index.path.json.JsonPCRCollector;

import java.util.Set;

/**
 * Scan the CAS-index for matching nodes.
 *
 * @author Sebastian Baechle
 * @author Johannes Lichtenberger
 */
@FunctionAnnotation(description = "Scans the given CAS index for matching nodes.",
    parameters = { "$doc", "$idx-no", "$key", "$search-mode", "$paths" })
public final class ScanCASIndex extends AbstractScanIndex {

  public final static QNm DEFAULT_NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-cas-index");

  public ScanCASIndex() {
    super(DEFAULT_NAME,
          new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany),
                        SequenceType.NODE,
                        new SequenceType(AtomicType.INR, Cardinality.One),
                        new SequenceType(AtomicType.ANA, Cardinality.One),
                        new SequenceType(AtomicType.STR, Cardinality.One),
                        new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
          true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final JsonDBItem doc = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = doc.getTrx();
    final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final int idx = FunUtil.getInt(args, 1, "$idx-no", -1, null, true);

    final IndexDef indexDef = controller.getIndexes().getIndexDef(idx, IndexType.CAS);

    if (indexDef == null) {
      throw new QueryException(SDBFun.ERR_INDEX_NOT_FOUND,
                               "Index no %s for collection %s and document %s not found.",
                               idx,
                               doc.getCollection().getName(),
                               doc.getTrx()
                                  .getResourceSession()
                                  .getResourceConfig()
                                  .getResource()
                                  .getFileName()
                                  .toString());
    }
    if (indexDef.getType() != IndexType.CAS) {
      throw new QueryException(SDBFun.ERR_INVALID_INDEX_TYPE,
                               "Index no %s for collection %s and document %s is not a CAS index.",
                               idx,
                               doc.getCollection().getName(),
                               doc.getTrx()
                                  .getResourceSession()
                                  .getResourceConfig()
                                  .getResource()
                                  .getFileName()
                                  .toString());
    }

    final Type keyType = indexDef.getContentType();
    final Atomic key = Cast.cast(sctx, (Atomic) args[2], keyType, true);
    final String[] searchModes = { "<", "<=", "==", ">", ">=" };
    final String searchMode = FunUtil.getString(args, 3, "$search-mode", "==", searchModes, true);

    final SearchMode mode = switch (searchMode) {
      case "<" -> SearchMode.LOWER;
      case "<=" -> SearchMode.LOWER_OR_EQUAL;
      case "==" -> SearchMode.EQUAL;
      case ">" -> SearchMode.GREATER;
      case ">=" -> SearchMode.GREATER_OR_EQUAL;
      default ->
        // May never happen.
          SearchMode.EQUAL;
    };

    final String paths = FunUtil.getString(args, 4, "$paths", null, null, false);
    final CASFilter filter = (paths != null)
        ? controller.createCASFilter(Set.of(paths.split(";")), key, mode, new JsonPCRCollector(rtx))
        : controller.createCASFilter(ImmutableSet.of(), key, mode, new JsonPCRCollector(rtx));

    return getSequence(doc, controller.openCASIndex(doc.getTrx().getPageTrx(), indexDef, filter));
  }

}
