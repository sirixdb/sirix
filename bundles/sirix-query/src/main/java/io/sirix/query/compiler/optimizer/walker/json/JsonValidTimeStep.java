package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.sirix.access.ValidTimeConfig;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.utils.LogWrapper;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

/**
 * Optimizer step that auto-selects the persistent VALIDTIME interval index for a plain FLWOR
 * stabbing predicate, rewriting it to draw the loop variable from {@code jn:scan-valid-time-index}.
 *
 * <p>It recognises (after the optimizer has pipelined the FLWOR into {@code ForBind}/{@code Selection}):
 *
 * <pre>{@code  for $x in jn:doc(DB,RES)[]
 *   where $x.validFrom <= P and P <= $x.validTo
 *   return ...  }</pre>
 *
 * and rewrites the loop source to {@code jn:scan-valid-time-index(jn:doc(DB,RES), P)} (dropping the
 * matched predicate). The two comparisons may use any equivalent operator/operand-order form: the
 * lower-field bound {@code deref($x,validFrom) <= P} as {@code <=}, or {@code P >= deref($x,validFrom)},
 * with value- or general-comparison operators; likewise the upper-field bound
 * {@code P <= deref($x,validTo)}. {@code P} must be INVARIANT w.r.t. {@code $x} (it must not
 * dereference {@code $x}); the SAME {@code P} must appear in both comparisons; the two
 * dereferenced fields must be the resource's configured {@code validFrom}/{@code validTo} fields; and
 * a VALIDTIME index must exist on that resource (revision-aware).
 *
 * <h2>Safety</h2>
 * <p>A NEW, dedicated walker (the CAS path in {@link JsonCASStep} is untouched). It only ever matches
 * the exact binary {@code AndExpr(cmp1, cmp2)} of the two stabbing comparisons over a
 * {@code jn:doc(...)[]} source with a VALIDTIME index; anything else is left unchanged and evaluated
 * normally (still correct). Extra conjuncts, a missing bound, a {@code $x}-dependent {@code P},
 * mismatched points, non-valid-time fields, or a missing index all fail the match and fall through.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonValidTimeStep extends Walker {

  private static final LogWrapper LOG_WRAPPER =
      new LogWrapper(LoggerFactory.getLogger(JsonValidTimeStep.class));

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

  @Override
  protected AST visit(final AST forBind) {
    if (forBind.getType() != XQ.ForBind || forBind.getChildCount() < 3) {
      return forBind;
    }

    // ForBind children: [0] TypedVariableBinding, [1] binding source, [2] Selection (the "where").
    final AST binding = forBind.getChild(0);
    final AST source = forBind.getChild(1);
    final AST selection = forBind.getChild(2);

    if (binding.getType() != XQ.TypedVariableBinding || binding.getChildCount() < 1
        || selection.getType() != XQ.Selection || selection.getChildCount() < 1) {
      return forBind;
    }

    final Object loopVar = binding.getChild(0).getValue();
    if (loopVar == null) {
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
    if (referencesVar(upperBoundOnField.point, loopVar)) {
      return forBind;
    }

    // ---- All conditions met: rewrite. ----
    // New loop source: jn:scan-valid-time-index(jn:doc(DB,RES), P).
    final AST scanCall = new AST(XQ.FunctionCall, SCAN_VALID_TIME_INDEX);
    scanCall.addChild(docFn.copyTree());
    scanCall.addChild(upperBoundOnField.point.copyTree());

    forBind.replaceChild(1, scanCall);
    // The whole predicate was consumed — replace the Selection with its End (drop the where).
    final AST end = selection.getChild(selection.getChildCount() - 1);
    forBind.replaceChild(2, end.copyTree());

    return forBind;
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

    Bound(final String fieldName, final boolean fieldUpperBounded, final AST point) {
      this.fieldName = fieldName;
      this.fieldUpperBounded = fieldUpperBounded;
      this.point = point;
    }
  }

  /**
   * Decode a {@code ComparisonExpr} [comparator, operandA, operandB] into a {@link Bound} relative to
   * {@code loopVar}, or {@code null} if it is not a {@code <=}/{@code <} (or swapped {@code >=}/{@code >})
   * comparison between exactly one {@code deref($loopVar, field)} and one non-field operand.
   *
   * <p>Only the inclusive/half-open ordering operators that define interval containment are accepted:
   * LE/LT and GE/GT (value or general). EQ/NE and anything else fail the match. Strictness ({@code <}
   * vs {@code <=}) does not change the rewrite — the index scan re-verifies exact instants — but we
   * accept LT/GT too so equivalent user phrasings still rewrite; the re-verification keeps results
   * correct regardless.</p>
   */
  private static @Nullable Bound decodeBound(final AST cmp, final Object loopVar) {
    if (cmp.getChildCount() != 3) {
      return null;
    }
    final int op = cmp.getChild(0).getType();
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
      return new Bound(leftField, fieldLe, right);
    }
    if (rightField != null && leftField == null) {
      // field is on the RIGHT: invert the operator's sense.
      final Boolean fieldLeLeft = fieldUpperBoundedForLeftField(op);
      if (fieldLeLeft == null) {
        return null;
      }
      // If "left OP right" means left<=right (fieldLeLeft semantics computed for left-field), then for
      // right-field the relation field-vs-point is the mirror: point OP-relation field.
      return new Bound(rightField, !fieldLeLeft, left);
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
   * The field name if {@code node} is {@code deref($loopVar, fieldName)} or a single XSD-type cast
   * wrapping it (e.g. {@code xs:dateTime($loopVar.fieldName)} — the canonical phrasing since JSON
   * valid-time fields are strings that must be cast to compare against an {@code xs:dateTime} point),
   * else {@code null}.
   */
  private static @Nullable String derefFieldOfVar(final AST node, final Object loopVar) {
    final AST deref = unwrapXsCast(node);
    if (deref.getType() != XQ.DerefExpr || deref.getChildCount() != 2) {
      return null;
    }
    final AST base = deref.getChild(0);
    final AST field = deref.getChild(1);
    if (base.getType() != XQ.VariableRef || !loopVar.equals(base.getValue())) {
      return null;
    }
    final Object fieldVal = field.getValue();
    return fieldVal == null ? null : fieldVal.toString();
  }

  /**
   * Strip a single XSD-namespace constructor/cast wrapping {@code node} (e.g.
   * {@code xs:dateTime(...)}). Returns {@code node} unchanged when there is no such wrapper. Only one
   * level is stripped — the field cast in a valid-time predicate is a single {@code xs:dateTime(...)}.
   */
  private static AST unwrapXsCast(final AST node) {
    if (node.getType() == XQ.FunctionCall && node.getChildCount() == 1
        && node.getValue() instanceof QNm fn && XS_NSURI.equals(fn.getNamespaceURI())) {
      return node.getChild(0);
    }
    return node;
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
    if (va == null ? vb != null : !va.equals(vb)) {
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
