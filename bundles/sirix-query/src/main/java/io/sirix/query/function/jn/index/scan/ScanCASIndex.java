package io.sirix.query.function.jn.index.scan;

import com.google.common.collect.ImmutableSet;
import io.sirix.query.function.FunUtil;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBItem;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.Cast;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.type.AnyJsonItemType;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.annotation.FunctionAnnotation;
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
    parameters = {"$doc", "$idx-no", "$key", "$search-mode", "$paths"})
public final class ScanCASIndex extends AbstractScanIndex {

  public final static QNm DEFAULT_NAME = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-cas-index");

  public ScanCASIndex() {
    super(DEFAULT_NAME,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany), SequenceType.NODE,
            new SequenceType(AtomicType.INR, Cardinality.One), new SequenceType(AtomicType.ANA, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
        true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    final JsonDBItem document = (JsonDBItem) args[0];
    final JsonNodeReadOnlyTrx rtx = document.getTrx();
    final JsonIndexController controller = rtx.getResourceSession().getRtxIndexController(rtx.getRevisionNumber());

    if (controller == null) {
      throw new QueryException(new QNm("Document not found: " + ((Str) args[1]).stringValue()));
    }

    final int idx = FunUtil.getInt(args, 1, "$idx-no", -1, null, true);

    final IndexDef indexDef = controller.getIndexes().getIndexDef(idx, IndexType.CAS);

    if (indexDef == null) {
      throw new QueryException(SDBFun.ERR_INDEX_NOT_FOUND, "Index no %s for collection %s and document %s not found.",
          idx, document.getCollection().getName(),
          document.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
    }
    if (indexDef.getType() != IndexType.CAS) {
      throw new QueryException(SDBFun.ERR_INVALID_INDEX_TYPE,
          "Index no %s for collection %s and document %s is not a CAS index.", idx, document.getCollection().getName(),
          document.getTrx().getResourceSession().getResourceConfig().getResource().getFileName().toString());
    }

    final Type keyType = indexDef.getContentType();
    final Atomic key = Cast.cast(sctx, (Atomic) args[2], keyType, true);
    final String[] searchModes = {"<", "<=", "==", ">", ">="};
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

    return getSequence(document, controller.openCASIndex(document.getTrx().getStorageEngineReader(), indexDef, filter));
  }

}
