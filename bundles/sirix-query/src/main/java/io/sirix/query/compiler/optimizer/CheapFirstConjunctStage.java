package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Stable, static cost ordering of read-only conjuncts after Brackit's predicate merge. Literal and
 * field comparisons precede arithmetic, calls/casts and nested pipelines; equal costs retain their
 * plan order. Unknown/effectful expressions form barriers. As with predicate pushdown, a cheaper
 * false conjunct can suppress a dynamic error in a later conjunct: XQuery permits this evaluation
 * order. Join keys, binding scopes and disjunctions are never exchanged.
 */
public final class CheapFirstConjunctStage implements Stage {
  public static final String ORIGINAL = "SIRIX_ORIGINAL_CONJUNCTION";
  public static final String INPUTS = "SIRIX_CONJUNCTION_INPUTS";
  public static final String CAPTURED = "SIRIX_CONJUNCTION_CAPTURED";
  public static final String DEFAULTS = "SIRIX_CONJUNCTION_DEFAULTS";
  public static final String NATIVE_STORE = "SIRIX_CONJUNCTION_NATIVE_STORE";
  public static final String ENABLED_PROPERTY = "sirix.optimizer.cheapFirstConjuncts";
  private static final Set<String> PURE_FUNCTIONS = Set.of("count", "exists", "empty", "min", "max", "sum", "avg",
      "distinct-values", "string", "data", "boolean", "not", "abs", "floor", "ceiling", "round", "string-length",
      "contains", "starts-with", "ends-with", "substring", "lower-case", "upper-case", "current-dateTime",
      "current-date", "current-time", "implicit-timezone");
  private static final Set<String> READ_FUNCTIONS = Set.of("doc", "open", "open-bitemporal", "collection");
  private static final Set<String> INDEX_SCAN_FUNCTIONS =
      Set.of("scan-valid-time-index", "scan-path-index", "scan-cas-index", "scan-cas-index-range", "scan-name-index");

  public static boolean enabled() {
    return !"false".equalsIgnoreCase(System.getProperty(ENABLED_PROPERTY, "true").trim());
  }

  @Override
  public AST rewrite(final StaticContext sctx, final AST ast) {
    if (!hasConjunction(ast)) {
      return ast;
    }
    return new BindingDependencies(sctx) {
      @Override
      protected AST visit(final AST node) {
        // Flatten each maximal conjunction once, rather than walking every overlapping prefix
        // of a left-associated conjunction again.
        if (node.getType() == XQ.AndExpr && (node.getParent() == null || node.getParent().getType() != XQ.AndExpr)) {
          final List<AST> terms = new ArrayList<>(4);
          flatten(node, terms);
          final int size = terms.size();
          // One cost per term, not one per comparison: cost() walks a term's whole subtree.
          final int[] costs = new int[size];
          final Set<QNm> inputs = new LinkedHashSet<>();
          final Set<QNm> captured = new LinkedHashSet<>();
          final Set<QNm> defaults = new LinkedHashSet<>();
          boolean nativeStore = false;
          for (int i = 0; i < size; i++) {
            final AST term = terms.get(i);
            final int cost = cost(term);
            costs[i] = cost >= 0 && dependencies(term, term, inputs, captured, defaults, new HashSet<>())
                ? cost
                : -1;
            nativeStore |= costs[i] >= 0 && Boolean.TRUE.equals(term.getProperty(NATIVE_STORE));
          }
          // Unknown calls/expressions are barriers: never move a conjunct across one.
          boolean reordered = false;
          int start = 0;
          for (int i = 0; i <= size; i++) {
            if (i == size || costs[i] < 0) {
              reordered |= sortByCost(terms, costs, start, i);
              start = i + 1;
            }
          }
          if (!reordered) {
            return node;
          }
          if (!inputs.isEmpty() || !captured.isEmpty() || nativeStore) {
            node.setProperty(ORIGINAL, node.copyTree());
            node.setProperty(INPUTS, inputs.toArray(QNm[]::new));
            node.setProperty(CAPTURED, captured.toArray(QNm[]::new));
            node.setProperty(DEFAULTS, defaults.toArray(QNm[]::new));
            node.setProperty(NATIVE_STORE, nativeStore);
          }
          // The old spine is discarded, so the term subtrees move rather than being copied.
          AST ordered = terms.get(0);
          for (int i = 1; i < size - 1; i++) {
            final AST and = new AST(XQ.AndExpr);
            and.addChild(ordered);
            and.addChild(terms.get(i));
            ordered = and;
          }
          node.replaceChild(0, ordered);
          node.replaceChild(1, terms.get(size - 1));
        }
        return node;
      }
    }.walk(ast);
  }

  private static boolean hasConjunction(final AST node) {
    if (node.getType() == XQ.AndExpr) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasConjunction(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Stable insertion sort of one barrier-free run, cheapest first. Equal costs keep their plan order;
   * conjunctions are short, so this beats a comparator that re-walks every subtree.
   *
   * @return whether any term moved
   */
  private static boolean sortByCost(final List<AST> terms, final int[] costs, final int start, final int end) {
    boolean moved = false;
    for (int i = start + 1; i < end; i++) {
      final AST term = terms.get(i);
      final int cost = costs[i];
      int j = i - 1;
      while (j >= start && costs[j] > cost) {
        terms.set(j + 1, terms.get(j));
        costs[j + 1] = costs[j];
        j--;
        moved = true;
      }
      terms.set(j + 1, term);
      costs[j + 1] = cost;
    }
    return moved;
  }

  private static void flatten(final AST node, final List<AST> terms) {
    if (node.getType() == XQ.AndExpr && node.getChildCount() == 2) {
      flatten(node.getChild(0), terms);
      flatten(node.getChild(1), terms);
    } else {
      terms.add(node);
    }
  }

  /**
   * Negative means unknown/effectful; otherwise field/literal < arithmetic < call/cast < pipeline.
   */
  public static int cost(final AST node) {
    return cost(node, false);
  }

  static boolean storedReadCall(final AST node) {
    return node.getType() == XQ.FunctionCall && node.getValue() instanceof QNm name
        && JSONFun.JSON_NSURI.equals(name.getNamespaceURI())
        && (READ_FUNCTIONS.contains(name.getLocalName()) || INDEX_SCAN_FUNCTIONS.contains(name.getLocalName()));
  }

  /** Classifies initializer sources, including the optimizer's read-only index scans. */
  static int cost(final AST node, final boolean admitIndexRewrites) {
    final int type = node.getType();
    int own = 0;
    if (type == XQ.FunctionCall) {
      if (!admittedFunction(node, admitIndexRewrites)) {
        return -1;
      }
      own = 20;
    } else if (type == XQ.ArithmeticExpr) {
      own = 10;
    } else if (type == XQ.CastExpr || type == XQ.CastableExpr) {
      own = 20;
    } else if (type == XQ.PipeExpr || type == XQ.QuantifiedExpr) {
      own = 30;
    } else if (node.getChildCount() != 0 && type != XQ.ComparisonExpr && type != XQ.DerefExpr && type != XQ.ArrayAccess
        && type != XQ.SequenceExpr && type != XQ.AndExpr && type != XQ.OrExpr && type != XQ.IfExpr
        && type != XQ.RangeExpr && type != XQ.FilterExpr && type != XQ.Predicate && type != XQ.Start && type != XQ.End
        && type != XQ.ForBind && type != XQ.LetBind && type != XQ.Selection && type != XQ.TypedVariableBinding
        && type != XQ.SequenceType && type != XQ.ParenthesizedExpr && type != XQ.Join && type != XQ.OrderBy
        && type != XQ.OrderBySpec && type != XQ.OrderByKind && type != XQ.OrderByEmptyMode && type != XQ.GroupBy
        && type != XQ.GroupBySpec && type != XQ.AggregateSpec && type != XQ.AggregateBinding
        && type != XQ.DftAggregateSpec && type != XQ.Count && type != XQ.QuantifiedBinding
        && type != XQ.AtomicOrUnionType && type != XQ.ArrayConstructor && type != XQ.SequenceField
        && type != XQ.FlattenedField && type != XQ.ObjectConstructor && type != XQ.KeyValueField) {
      return -1;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final int child = cost(node.getChild(i), admitIndexRewrites);
      if (child < 0) {
        return -1;
      }
      own = Math.max(own, child);
    }
    return own;
  }

  private static boolean admittedFunction(final AST node, final boolean admitIndexRewrites) {
    if (!(node.getValue() instanceof QNm name)) {
      return false;
    }
    final String ns = name.getNamespaceURI();
    final String local = name.getLocalName();
    return Namespaces.XS_NSURI.equals(ns)
        || ((Namespaces.FN_NSURI.equals(ns) || Namespaces.DEFAULT_FN_NSURI.equals(ns))
            && PURE_FUNCTIONS.contains(local))
        || (JSONFun.JSON_NSURI.equals(ns)
            && (READ_FUNCTIONS.contains(local) || (admitIndexRewrites && INDEX_SCAN_FUNCTIONS.contains(local))));
  }
}
