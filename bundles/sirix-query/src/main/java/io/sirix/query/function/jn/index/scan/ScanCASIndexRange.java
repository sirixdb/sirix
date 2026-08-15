package io.sirix.query.function.jn.index.scan;

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
import io.sirix.index.cas.CASFilterRange;
import io.sirix.index.path.json.JsonPCRCollector;

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
    super(DEFAULT_NAME, new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrMany),
        SequenceType.NODE, new SequenceType(AtomicType.INR, Cardinality.One),
        // Bounds are ZeroOrOne, not One: an empty sequence means "unbounded on this end". The
        // index has always supported a one-sided range — CASFilterRange treats a null bound as
        // unbounded, and the valid-time scan relies on exactly that — but this signature made
        // the shape unreachable from a query, so `$x >= 'a'` could not be expressed as a range
        // scan and the one-sided code path had no query-level coverage at all.
        new SequenceType(AtomicType.ANA, Cardinality.ZeroOrOne),
        new SequenceType(AtomicType.ANA, Cardinality.ZeroOrOne), new SequenceType(AtomicType.BOOL, Cardinality.One),
        new SequenceType(AtomicType.BOOL, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne)),
        true);
  }

  /**
   * Cast a bound to the index's content type, mapping an absent argument to {@code null}.
   *
   * @param arg the argument, or {@code null} when the caller passed {@code ()}
   * @return the cast bound, or {@code null} for an unbounded end
   */
  private static Atomic castBound(final StaticContext sctx, final Sequence arg, final Type keyType) {
    return arg == null
        ? null
        : Cast.cast(sctx, (Atomic) arg, keyType, true);
  }

  @Override
  public Sequence execute(StaticContext sctx, QueryContext ctx, Sequence[] args) {
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
    final Atomic min = castBound(sctx, args[2], keyType);
    final Atomic max = castBound(sctx, args[3], keyType);
    if (min == null && max == null) {
      throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT,
          "At least one of $low-key / $high-key must be given; an unbounded range is a full index scan.");
    }
    final boolean incMin = FunUtil.getBoolean(args, 4, "$include-low-key", true, true);
    final boolean incMax = FunUtil.getBoolean(args, 5, "$include-high-key", true, true);
    final String paths = FunUtil.getString(args, 6, "$paths", null, null, false);
    final Set<String> setOfPaths = paths == null
        ? Set.of()
        : Set.of(paths.split(";"));
    final CASFilterRange filter =
        controller.createCASFilterRange(setOfPaths, min, max, incMin, incMax, new JsonPCRCollector(rtx));

    return getSequence(document, controller.openCASIndex(document.getTrx().getStorageEngineReader(), indexDef, filter));
  }
}
