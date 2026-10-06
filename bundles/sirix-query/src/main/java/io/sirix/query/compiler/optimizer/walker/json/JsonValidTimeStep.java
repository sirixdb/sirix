package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Str;
import io.sirix.query.function.jn.temporal.OpenBitemporal;
import io.sirix.query.function.jn.index.scan.ScanValidTimeIndex;
import java.util.ArrayList;
import java.util.List;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.utils.LogWrapper;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Folds plain-FLWOR valid-time comparisons and the first bitemporal residual into indexed slice
 * sources. Query shapes and admission rules are documented in
 * {@code docs/VALID_TIME_KEY_SLICES.md}.
 *
 * <p>
 * Plain-FLWOR points stay deferred until row demand. Runtime admission uses the evaluated revision;
 * unsafe coverage or order retains the original comparisons. Bitemporal folding consumes only the
 * first conjunct so it cannot expose a cast error suppressed by an earlier comparison.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonValidTimeStep extends Walker {

  private static final LogWrapper LOG_WRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonValidTimeStep.class));

  private static final QNm DOC = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "doc");
  private static final QNm OPEN = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "open");
  private static final QNm SCAN_VALID_TIME_INDEX =
      new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "scan-valid-time-index");

  /** XML-Schema namespace URI — used to recognise an {@code xs:dateTime(...)} cast around a field. */
  private static final String XS_NSURI = "http://www.w3.org/2001/XMLSchema";

  private final JsonDBStore jsonDBStore;

  public JsonValidTimeStep(final JsonDBStore jsonDBStore) {
    this.jsonDBStore = jsonDBStore;
  }

  /** Apply the source rewrites, then remove only their now-redundant identity pipelines. */
  public AST rewrite(final AST ast) {
    return collapseIdentitySlices(walk(ast));
  }

  @SuppressWarnings("ReferenceEquality") // AST replacement is determined by node identity.
  private static AST collapseIdentitySlices(final AST node) {
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST child = node.getChild(i);
      final AST replacement = collapseIdentitySlices(child);
      if (replacement != child) {
        node.replaceChild(i, replacement);
      }
    }
    if (node.getType() != XQ.PipeExpr || node.getChildCount() != 1) {
      return node;
    }
    final AST start = node.getChild(0);
    if (start.getType() != XQ.Start || start.getChildCount() != 1) {
      return node;
    }
    final AST binding = start.getChild(0);
    if (binding.getType() != XQ.ForBind || binding.getChildCount() != 3 || binding.checkProperty("allowingEmpty")
        || binding.getProperty("check") != null) {
      return node;
    }
    final AST variable = binding.getChild(0);
    final AST source = binding.getChild(1);
    final AST end = binding.getChild(2);
    if (variable.getType() != XQ.TypedVariableBinding || variable.getChildCount() != 1
        || source.getType() != XQ.FunctionCall
        || !((OpenBitemporal.OPEN_BITEMPORAL_SLICE.equals(source.getValue())
            && source.checkProperty(OpenBitemporal.INTERNAL_SLICE))
            || (OpenBitemporal.OPEN_BITEMPORAL.equals(source.getValue()) && source.getChildCount() == 4)
            || SCAN_VALID_TIME_INDEX.equals(source.getValue()))
        || end.getType() != XQ.End || end.getChildCount() != 1) {
      return node;
    }
    final AST returned = end.getChild(0);
    return returned.getType() == XQ.VariableRef && returned.getValue().equals(variable.getChild(0).getValue())
        ? source.copyTree()
        : node;
  }

  @Override
  protected AST visit(final AST forBind) {
    if (forBind.getType() != XQ.ForBind || forBind.getChildCount() != 3 || forBind.checkProperty("allowingEmpty")) {
      return forBind;
    }

    // ForBind children: [0] TypedVariableBinding, [1] binding source, [2] Selection (the "where").
    final AST binding = forBind.getChild(0);
    final AST source = forBind.getChild(1);
    final AST selection = forBind.getChild(2);

    if (binding.getType() != XQ.TypedVariableBinding || binding.getChildCount() != 1
        || selection.getType() != XQ.Selection || selection.getChildCount() < 1) {
      return forBind;
    }

    final Object loopVar = binding.getChild(0).getValue();
    if (loopVar == null) {
      return forBind;
    }

    if (source.getType() == XQ.FunctionCall && OpenBitemporal.OPEN_BITEMPORAL.equals(source.getValue())
        && source.getChildCount() == 4) {
      foldBitemporalResidual(forBind, selection, source, loopVar);
      return forBind;
    }

    // The predicate must be EXACTLY a binary AndExpr of two ComparisonExprs.
    final AST predicate = selection.getChild(0);
    if (predicate.getType() != XQ.AndExpr || predicate.getChildCount() != 2) {
      return forBind;
    }
    final AST cmpA = predicate.getChild(0);
    final AST cmpB = predicate.getChild(1);
    if (cmpA.getType() != XQ.ComparisonExpr || cmpB.getType() != XQ.ComparisonExpr) {
      return forBind;
    }

    // The loop source must be jn:doc(DB,RES)[] (ArrayAccess over a document function + empty []).
    final AST docFn = unwrapIndexedSource(source);
    if (docFn == null) {
      return forBind;
    }

    // Decode each comparison into a (field-name, bound-direction-on-field, point) triple.
    final Bound boundA = decodeBound(cmpA, loopVar);
    final Bound boundB = decodeBound(cmpB, loopVar);
    if (boundA == null || boundB == null) {
      return forBind;
    }

    // One must bound a field from ABOVE (field <= P), the other a (possibly different) field from
    // BELOW (P <= field). Pair them up.
    final Bound upperBoundOnField; // field <= P
    final Bound lowerBoundOnField; // P <= field
    if (boundA.fieldUpperBounded && !boundB.fieldUpperBounded) {
      upperBoundOnField = boundA;
      lowerBoundOnField = boundB;
    } else if (boundB.fieldUpperBounded && !boundA.fieldUpperBounded) {
      upperBoundOnField = boundB;
      lowerBoundOnField = boundA;
    } else {
      return forBind; // both bound from the same side — not a stabbing interval
    }

    // The SAME point P must appear in both comparisons.
    if (!astEquals(upperBoundOnField.point, lowerBoundOnField.point)) {
      return forBind;
    }

    // Resolve the resource and verify: VALIDTIME index exists AND the two dereferenced fields are the
    // configured valid-time fields, with field<=P on validFrom and P<=field on validTo.
    final RevisionData revisionData = revisionDataOf(docFn);
    if (revisionData == null) {
      return forBind;
    }
    final ValidTimeConfig validTimeConfig = resolveValidTimeConfigWithIndex(revisionData);
    if (validTimeConfig == null) {
      return forBind;
    }
    final String validFrom = validTimeConfig.getNormalizedValidFromPath();
    final String validTo = validTimeConfig.getNormalizedValidToPath();

    // field<=P must be on validFrom, P<=field must be on validTo.
    if (!validFrom.equals(upperBoundOnField.fieldName) || !validTo.equals(lowerBoundOnField.fieldName)) {
      return forBind;
    }

    // The point P must be invariant w.r.t. the loop variable.
    if (referencesVar(upperBoundOnField.point, loopVar) || !stablePoint(upperBoundOnField.point)) {
      return forBind;
    }

    // ---- All conditions met: rewrite. ----
    // Mark the source for direct translation; fields and comparison modes stay internal.
    final AST scanCall = new AST(XQ.FunctionCall, SCAN_VALID_TIME_INDEX);
    scanCall.setProperty(ScanValidTimeIndex.DEFERRED_POINT, true);
    scanCall.addChild(docFn.copyTree());
    scanCall.addChild(upperBoundOnField.point.copyTree());

    scanCall.addChild(new AST(XQ.Str, new Str(validFrom)));
    scanCall.addChild(new AST(XQ.Str, new Str(validTo)));
    final int mode = (upperBoundOnField.strict
        ? 1
        : 0)
        | (lowerBoundOnField.strict
            ? 2
            : 0)
        | (boundA == lowerBoundOnField
            ? 4
            : 0)
        | (upperBoundOnField.general
            ? 8
            : 0)
        | (lowerBoundOnField.general
            ? 16
            : 0)
        | (upperBoundOnField.fieldOnLeft
            ? 0
            : 32)
        | (lowerBoundOnField.fieldOnLeft
            ? 64
            : 0);
    scanCall.addChild(new AST(XQ.Int, new Int32(mode)));
    forBind.replaceChild(1, scanCall);
    // The helper checks exactness at this evaluation's revision; an unsafe shape executes the
    // original two comparisons over the full array with their original short-circuit order.
    forBind.replaceChild(2, selection.getChild(selection.getChildCount() - 1).copyTree());

    return forBind;
  }

  /** Works in user-function bodies too: collection/resource/revision are resolved per evaluation. */
  private static void foldBitemporalResidual(final AST forBind, final AST selection, final AST source,
      final Object loopVar) {
    final AST point = source.getChild(3);
    if (!stablePoint(point) || referencesVar(point, loopVar)) {
      return;
    }
    final List<AST> conjuncts = new ArrayList<>();
    conjuncts(selection.getChild(0), conjuncts);
    // Moving a later comparison ahead of an earlier conjunct can expose a suppressed cast error.
    for (int i = 0; i < Math.min(1, conjuncts.size()); i++) {
      final AST conjunct = conjuncts.get(i);
      if (conjunct.getType() != XQ.ComparisonExpr) {
        continue;
      }
      final Bound bound = decodeBound(conjunct, loopVar);
      if (bound == null || !astEquals(point, bound.point)) {
        continue;
      }
      final AST call = new AST(XQ.FunctionCall, OpenBitemporal.OPEN_BITEMPORAL_SLICE);
      call.setProperty(OpenBitemporal.INTERNAL_SLICE, true);
      for (int argument = 0; argument < 4; argument++) {
        call.addChild(source.getChild(argument).copyTree());
      }
      call.addChild(new AST(XQ.Str, new Str(bound.fieldName)));
      call.addChild(new AST(XQ.Int, new Int32((bound.fieldUpperBounded
          ? 1
          : 3)
          + (bound.strict
              ? 1
              : 0)
          + (bound.fieldOnLeft == bound.fieldUpperBounded
              ? 0
              : 4)
          + (bound.general
              ? 8
              : 0))));
      call.addChild(bound.point.copyTree());
      forBind.replaceChild(1, call);
      conjuncts.remove(i);
      if (conjuncts.isEmpty()) {
        forBind.replaceChild(2, selection.getChild(selection.getChildCount() - 1).copyTree());
      } else {
        AST remaining = conjuncts.getFirst().copyTree();
        for (int c = 1; c < conjuncts.size(); c++) {
          final AST and = new AST(XQ.AndExpr);
          and.addChild(remaining);
          and.addChild(conjuncts.get(c).copyTree());
          remaining = and;
        }
        selection.replaceChild(0, remaining);
      }
      return;
    }
  }

  private static void conjuncts(final AST node, final List<AST> output) {
    if (node.getType() == XQ.AndExpr && node.getChildCount() == 2) {
      conjuncts(node.getChild(0), output);
      conjuncts(node.getChild(1), output);
    } else {
      output.add(node);
    }
  }

  private static boolean stablePoint(final AST point) {
    return point.getType() == XQ.VariableRef
        || (point.getType() == XQ.FunctionCall && point.getValue() instanceof QNm name
            && XS_NSURI.equals(name.getNamespaceURI()) && "dateTime".equals(name.getLocalName())
            && point.getChildCount() == 1 && point.getChild(0).getType() == XQ.Str);
  }

  /**
   * A single comparison decoded relative to the loop variable: the dereferenced FIELD name, whether
   * the field is bounded from ABOVE ({@code field <= point}) or BELOW ({@code point <= field}), and
   * the other (point) operand AST.
   */
  private static final class Bound {
    final String fieldName;
    final boolean fieldUpperBounded; // true: field <= point ; false: point <= field
    final AST point;
    final boolean strict;
    final boolean general;
    final boolean fieldOnLeft;

    Bound(final String fieldName, final boolean fieldUpperBounded, final AST point, final boolean strict,
        final boolean general, final boolean fieldOnLeft) {
      this.fieldName = fieldName;
      this.fieldUpperBounded = fieldUpperBounded;
      this.point = point;
      this.strict = strict;
      this.general = general;
      this.fieldOnLeft = fieldOnLeft;
    }
  }

  /**
   * Decode a {@code ComparisonExpr} [comparator, operandA, operandB] into a {@link Bound} relative to
   * {@code loopVar}, or {@code null} if it is not a {@code <=}/{@code <} (or swapped
   * {@code >=}/{@code >}) comparison between exactly one {@code deref($loopVar, field)} and one
   * non-field operand.
   *
   * <p>
   * Only the inclusive/half-open ordering operators that define interval containment are accepted:
   * LE/LT and GE/GT (value or general). EQ/NE and anything else fail the match. Strictness is
   * preserved for both rewrites. The internal plain-FLWOR helper runs the original comparisons when
   * its revision cannot prove complete, exact, ordered array coverage.
   * </p>
   */
  private static @Nullable Bound decodeBound(final AST cmp, final Object loopVar) {
    if (cmp.getChildCount() != 3) {
      return null;
    }
    final int op = cmp.getChild(0).getType();
    final boolean strict =
        op == XQ.ValueCompLT || op == XQ.ValueCompGT || op == XQ.GeneralCompLT || op == XQ.GeneralCompGT;
    final boolean general =
        op == XQ.GeneralCompLT || op == XQ.GeneralCompLE || op == XQ.GeneralCompGT || op == XQ.GeneralCompGE;
    final AST left = cmp.getChild(1);
    final AST right = cmp.getChild(2);

    final String leftField = derefFieldOfVar(left, loopVar);
    final String rightField = derefFieldOfVar(right, loopVar);

    // Exactly one side must be deref($loopVar, field); the other is the point.
    if (leftField != null && rightField == null) {
      final Boolean fieldLe = fieldUpperBoundedForLeftField(op);
      if (fieldLe == null) {
        return null;
      }
      return new Bound(leftField, fieldLe, right, strict, general, true);
    }
    if (rightField != null && leftField == null) {
      // field is on the RIGHT: invert the operator's sense.
      final Boolean fieldLeLeft = fieldUpperBoundedForLeftField(op);
      if (fieldLeLeft == null) {
        return null;
      }
      // If "left OP right" means left<=right (fieldLeLeft semantics computed for left-field), then for
      // right-field the relation field-vs-point is the mirror: point OP-relation field.
      return new Bound(rightField, !fieldLeLeft, left, strict, general, false);
    }
    return null;
  }

  /**
   * For an operator in {@code left OP right}, when the FIELD is the LEFT operand: returns
   * {@code TRUE} if the relation bounds the field from ABOVE ({@code field <= point}: LE/LT),
   * {@code FALSE} if from BELOW ({@code field >= point}: GE/GT), {@code null} for any other operator.
   */
  private static @Nullable Boolean fieldUpperBoundedForLeftField(final int op) {
    return switch (op) {
      case XQ.ValueCompLE, XQ.ValueCompLT, XQ.GeneralCompLE, XQ.GeneralCompLT -> Boolean.TRUE;
      case XQ.ValueCompGE, XQ.ValueCompGT, XQ.GeneralCompGE, XQ.GeneralCompGT -> Boolean.FALSE;
      default -> null;
    };
  }

  /**
   * Recognizes only {@code xs:dateTime($loopVar.fieldName)} with a static field name. Bare or
   * computed dereferences and other casts retain their original evaluation.
   */
  private static @Nullable String derefFieldOfVar(final AST node, final Object loopVar) {
    if (node.getType() != XQ.FunctionCall || node.getChildCount() != 1 || !(node.getValue() instanceof QNm name)
        || !XS_NSURI.equals(name.getNamespaceURI()) || !"dateTime".equals(name.getLocalName())) {
      return null;
    }
    final AST deref = node.getChild(0);
    if (deref.getType() != XQ.DerefExpr || deref.getChildCount() != 2) {
      return null;
    }
    final AST base = deref.getChild(0);
    final AST field = deref.getChild(1);
    if (base.getType() != XQ.VariableRef || !loopVar.equals(base.getValue()) || field.getType() != XQ.QNm) {
      return null;
    }
    final Object fieldVal = field.getValue();
    return fieldVal == null
        ? null
        : fieldVal.toString();
  }

  /**
   * The {@code jn:doc(...)} / {@code jn:open(...)} function node if {@code source} is exactly
   * {@code <docFn>[]} (an {@code ArrayAccess} whose first child is a document function and whose
   * second child is an empty {@code SequenceExpr}). Otherwise {@code null}.
   */
  private static @Nullable AST unwrapIndexedSource(final AST source) {
    if (source.getType() != XQ.ArrayAccess || source.getChildCount() != 2) {
      return null;
    }
    final AST docFn = source.getChild(0);
    final AST arrayIndex = source.getChild(1);
    if (arrayIndex.getType() != XQ.SequenceExpr || arrayIndex.getChildCount() != 0) {
      return null; // a specific index like [0] is not a full-array iteration
    }
    if (docFn.getType() != XQ.FunctionCall) {
      return null;
    }
    final Object fnName = docFn.getValue();
    if (!DOC.equals(fnName) && !OPEN.equals(fnName)) {
      return null;
    }
    if (docFn.getChildCount() < 2) {
      return null;
    }
    return docFn;
  }

  /** True if {@code node}'s subtree references {@code loopVar} (a VariableRef or a deref of it). */
  private static boolean referencesVar(final AST node, final Object loopVar) {
    if (node == null) {
      return false;
    }
    if (node.getType() == XQ.VariableRef && loopVar.equals(node.getValue())) {
      return true;
    }
    if (node.getType() == XQ.ContextItemExpr) {
      // A context-item ('.') inside the point is treated as variant — be conservative.
      return true;
    }
    for (int i = 0, n = node.getChildCount(); i < n; i++) {
      if (referencesVar(node.getChild(i), loopVar)) {
        return true;
      }
    }
    return false;
  }

  /** Structural equality of two AST subtrees (type + value + children, recursively). */
  private static boolean astEquals(final AST a, final AST b) {
    if (a == null || b == null) {
      return a == b;
    }
    if (a.getType() != b.getType() || a.getChildCount() != b.getChildCount()) {
      return false;
    }
    final Object va = a.getValue();
    final Object vb = b.getValue();
    if (va == null
        ? vb != null
        : !va.equals(vb)) {
      return false;
    }
    for (int i = 0, n = a.getChildCount(); i < n; i++) {
      if (!astEquals(a.getChild(i), b.getChild(i))) {
        return false;
      }
    }
    return true;
  }

  /** Resolve the {@code (databaseName, resourceName, revision)} of a {@code jn:doc/open} call. */
  private static @Nullable RevisionData revisionDataOf(final AST docFn) {
    final String databaseName = docFn.getChild(0).getStringValue();
    final String resourceName = docFn.getChild(1).getStringValue();
    if (databaseName == null || resourceName == null) {
      return null;
    }
    int revision = -1;
    if (docFn.getChildCount() > 2) {
      final Object revVal = docFn.getChild(2).getValue();
      if (revVal instanceof Number num) {
        revision = num.intValue();
      }
    }
    return new RevisionData(databaseName, resourceName, revision);
  }

  /**
   * Open the resource (revision-aware) and return its {@link ValidTimeConfig} IFF the resource has a
   * valid-time configuration AND a VALIDTIME interval index; otherwise {@code null}. Mirrors the
   * revision-aware index-existence check the CAS walker performs.
   */
  private @Nullable ValidTimeConfig resolveValidTimeConfigWithIndex(final RevisionData revisionData) {
    try {
      // BORROW, never close: lookup() returns the store's CACHED collection and
      // beginResourceSession() the database's cached open session. Closing either from a compile
      // step (JsonDBCollection.close() closes the WHOLE database) tears shared state out from
      // under concurrent evaluations and forces a close/reopen cycle per compiled query. The
      // store/database own these objects and close them on their own shutdown.
      final JsonDBCollection collection = jsonDBStore.lookup(revisionData.databaseName());
      if (collection == null) {
        return null;
      }
      final var resourceSession = collection.getDatabase().beginResourceSession(revisionData.resourceName());
      final ValidTimeConfig validTimeConfig = resourceSession.getResourceConfig().getValidTimeConfig();
      if (validTimeConfig == null) {
        return null;
      }
      final int revision = revisionData.revision() == -1
          ? resourceSession.getMostRecentRevisionNumber()
          : revisionData.revision();
      final JsonIndexController controller = resourceSession.getRtxIndexController(revision);
      if (controller == null) {
        return null;
      }
      for (final IndexDef indexDef : controller.getIndexes().getIndexDefs()) {
        if (indexDef.getType() == IndexType.VALIDTIME) {
          return validTimeConfig;
        }
      }
      return null;
    } catch (final RuntimeException e) {
      // Any resolution failure means "don't rewrite" — normal evaluation still runs — but it must
      // not be silent: an invisible transient failure here disables the index without a trace.
      LOG_WRAPPER.warn("VALIDTIME index resolution failed during optimization; skipping rewrite for {}/{}",
          revisionData.databaseName(), revisionData.resourceName(), e);
      return null;
    }
  }
}
