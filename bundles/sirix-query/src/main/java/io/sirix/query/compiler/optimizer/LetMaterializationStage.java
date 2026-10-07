package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.Bits;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Stage;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.Namespaces;
import io.brackit.query.function.UDF;
import io.brackit.query.jdm.Function;
import io.brackit.query.module.StaticContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Final-stage admission for repeatedly referenced, proven-pure lazy let initializers. Only the
 * initializer is annotated: translation consumes it once per binding tuple, without cached state in
 * the compiled expression, deferred replay, or invalidation.
 */
public final class LetMaterializationStage implements Stage {
  public static final String MATERIALIZE = "SIRIX_MATERIALIZE_LET";
  public static final String NATIVE_STORE = "SIRIX_MATERIALIZE_LET_NATIVE_STORE";
  public static final String GLOBAL_DEFAULTS = "SIRIX_MATERIALIZE_LET_GLOBAL_DEFAULTS";
  public static final String CAPTURED = "SIRIX_MATERIALIZE_LET_CAPTURED";
  public static final String ENABLED_PROPERTY = "sirix.optimizer.materializeLets";

  @Override
  public AST rewrite(final StaticContext context, final AST ast) {
    if (hasLazyLet(ast) && !"false".equalsIgnoreCase(System.getProperty(ENABLED_PROPERTY, "true").trim())) {
      new Admission(context).walk(ast);
    }
    return ast;
  }

  private static boolean hasLazyLet(final AST node) {
    if (node.getType() == XQ.LetBind && Admission.initializer(node).getType() == XQ.PipeExpr) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasLazyLet(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private static final class Admission extends ScopeWalker {
    private static final int MAX_PROOF_WORK = 1024;
    private static final int NON_SCALAR = 0;
    private static final int PARTIAL_CONSUMPTION = 1;
    private static final int FULL_CONSUMPTION = 2;
    private final List<AST> candidates = new ArrayList<>();
    private final Map<AST, Integer> references = new IdentityHashMap<>();
    private final Set<AST> nonScalarConsumers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<AST> fullConsumers = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<QNm, AST> declarations = new HashMap<>();
    private final Set<QNm> defaults = new LinkedHashSet<>();
    private final Set<QNm> captured = new LinkedHashSet<>();
    private AST candidateBinding;
    private AST candidateSource;
    private int remaining;
    private boolean nativeStore;

    Admission(final StaticContext context) {
      super(context);
    }

    @Override
    protected AST prepare(final AST ast) {
      AST module = ast;
      while (module.getParent() != null && module.getType() != XQ.MainModule && module.getType() != XQ.LibraryModule) {
        module = module.getParent();
      }
      // Optimizers receive individual bodies, while lazy global initializers live in the prolog.
      for (int i = 0; i < module.getChildCount(); i++) {
        final AST prolog = module.getChild(i);
        if (prolog.getType() != XQ.Prolog)
          continue;
        for (int j = 0; j < prolog.getChildCount(); j++) {
          final AST declaration = prolog.getChild(j);
          if (declaration.getType() != XQ.TypedVariableDeclaration)
            continue;
          for (int k = 0; k < declaration.getChildCount(); k++) {
            final AST child = declaration.getChild(k);
            if (child.getType() == XQ.Variable) {
              declarations.put((QNm) child.getValue(), declaration);
            }
          }
        }
      }
      super.prepare(module);
      return ast;
    }

    @Override
    protected AST visit(final AST node) {
      if (node.getType() == XQ.LetBind && initializer(node).getType() == XQ.PipeExpr) {
        candidates.add(node);
      } else if (node.getType() == XQ.VariableRef) {
        final QNm name = (QNm) node.getValue();
        final Var variable = findScope(node).resolve(name);
        if (variable != null && variable.scope.getNode().getType() == XQ.LetBind
            && initializer(variable.scope.getNode()).getType() == XQ.PipeExpr
            && !BindingDependencies.parameter(node, name, variable.scope.getNode())) {
          final AST binding = variable.scope.getNode();
          references.put(binding, Math.min(2, references.getOrDefault(binding, 0) + 1));
          final int consumption = scalarConsumer(node, binding);
          if (consumption == NON_SCALAR)
            nonScalarConsumers.add(binding);
          else if (consumption == FULL_CONSUMPTION)
            fullConsumers.add(binding);
        }
      }
      return node;
    }

    @Override
    protected AST finish(final AST ast) {
      for (final AST binding : candidates) {
        if (references.getOrDefault(binding, 0) > 1 && !nonScalarConsumers.contains(binding)
            && fullConsumers.contains(binding)) {
          remaining = MAX_PROOF_WORK;
          defaults.clear();
          captured.clear();
          nativeStore = false;
          final AST source = initializer(binding);
          candidateBinding = binding;
          candidateSource = source;
          if (pureSource(source, new HashSet<>(), false)
              && ((defaults.isEmpty() && !nativeStore) || eagerResult(binding))) {
            source.setProperty(MATERIALIZE, true);
            source.setProperty(GLOBAL_DEFAULTS, defaults.toArray(QNm[]::new));
            source.setProperty(CAPTURED, captured.toArray(QNm[]::new));
            source.setProperty(NATIVE_STORE, nativeStore);
          }
        }
      }
      return ast;
    }

    private int scalarConsumer(final AST reference, final AST binding) {
      final AST end = binding.getLastChild();
      if (end.getType() != XQ.End)
        return NON_SCALAR;
      int consumption = NON_SCALAR;
      boolean deferred = false;
      AST child = reference;
      for (AST parent = reference.getParent(); parent != null; child = parent, parent = parent.getParent()) {
        if (parent == end)
          return deferred && consumption == FULL_CONSUMPTION
              ? PARTIAL_CONSUMPTION
              : consumption;
        final int type = parent.getType();
        if (type == XQ.PipeExpr || type == XQ.End || type == XQ.InlineFuncItem || type == XQ.FunctionDecl)
          return NON_SCALAR;
        if (consumption != NON_SCALAR) {
          final boolean consumes = switch (type) {
            case XQ.ParenthesizedExpr, XQ.SequenceField, XQ.FlattenedField, XQ.KeyValueField -> true;
            case XQ.SequenceExpr -> {
              deferred |= parent.getChild(0) != child;
              yield true;
            }
            case XQ.ArrayConstructor, XQ.ObjectConstructor -> {
              deferred = false;
              yield true;
            }
            case XQ.ArrayAccess -> parent.getChild(0) == child && child.getType() == XQ.ArrayConstructor;
            case XQ.DerefExpr -> parent.getChild(0) == child && child.getType() == XQ.ObjectConstructor;
            case XQ.FunctionCall -> {
              final boolean full = parent.getChild(0) == child && scalarFunction(functionName(parent));
              if (full)
                deferred = false;
              yield full;
            }
            default -> false;
          };
          if (!consumes)
            consumption = PARTIAL_CONSUMPTION;
          continue;
        }
        if (type == XQ.ParenthesizedExpr)
          continue;
        if ((type == XQ.DerefExpr || type == XQ.ArrayAccess) && parent.getChild(0) == child) {
          continue;
        }
        final String name = functionName(parent);
        if (name == null || parent.getChild(0) != child)
          return NON_SCALAR;
        if (scalarFunction(name)) {
          consumption = FULL_CONSUMPTION;
        } else if (name.equals("exists")) {
          consumption = PARTIAL_CONSUMPTION;
        } else if ((name.equals("data") || name.equals("distinct-values")) && parent.getChildCount() == 1) {
          continue;
        } else {
          return NON_SCALAR;
        }
      }
      return NON_SCALAR;
    }

    private boolean eagerResult(final AST binding) {
      AST result = binding.getLastChild().getChild(0);
      while (result.getType() == XQ.ParenthesizedExpr && result.getChildCount() == 1)
        result = result.getChild(0);
      final boolean constructedArray = result.getType() == XQ.ArrayConstructor
          || result.getType() == XQ.ArrayAccess && result.getChild(0).getType() == XQ.ArrayConstructor;
      if (result.getType() != XQ.ObjectConstructor && !constructedArray && !scalarFunction(functionName(result)))
        return false;
      candidateSource = result;
      return pureSource(result, new HashSet<>(), false);
    }

    private String functionName(final AST node) {
      if (node.getType() != XQ.FunctionCall || !(node.getValue() instanceof QNm name)
          || !(Namespaces.FN_NSURI.equals(name.getNamespaceURI())
              || Namespaces.DEFAULT_FN_NSURI.equals(name.getNamespaceURI())))
        return null;
      final Function function = sctx.getFunctions().resolve(name, node.getChildCount());
      return function != null && function.isBuiltIn() && !(function instanceof UDF) && !function.isUpdating()
          ? name.getLocalName()
          : null;
    }

    private static boolean scalarFunction(final String name) {
      return name != null && switch (name) {
        case "count", "sum", "avg", "min", "max" -> true;
        default -> false;
      };
    }

    private boolean pureSource(final AST source, final Set<AST> visited, final boolean globalDefault) {
      if (remaining-- <= 0 || !visited.add(source)) {
        return false;
      }
      if (CheapFirstConjunctStage.cost(source, true) < 0) {
        return false;
      }
      final boolean pure = pure(source, source, visited, globalDefault);
      visited.remove(source);
      return pure;
    }

    private boolean pure(final AST node, final AST source, final Set<AST> visited, final boolean globalDefault) {
      if (remaining-- <= 0) {
        return false;
      }
      if (globalDefault && (node.getType() == XQ.ObjectConstructor || node.getType() == XQ.ArrayConstructor
          || CheapFirstConjunctStage.storedReadCall(node))) {
        return false;
      }
      nativeStore |= CheapFirstConjunctStage.storedReadCall(node);
      if (node.getType() == XQ.FunctionCall && node.getValue() instanceof QNm name) {
        final Function function = sctx.getFunctions().resolve(name, node.getChildCount());
        // A user declaration can shadow a JSON read function's otherwise admitted QName.
        if (function == null || function instanceof UDF || function.isUpdating()
            || node.getChildCount() == 0 && function.getSignature().defaultCtxItemType() != null) {
          return false;
        }
      }
      if (node.getType() == XQ.FunctionCall && node.getValue() instanceof QNm name
          && (Namespaces.FN_NSURI.equals(name.getNamespaceURI())
              || Namespaces.DEFAULT_FN_NSURI.equals(name.getNamespaceURI()))
          && (name.getLocalName().startsWith("current-") || name.getLocalName().equals("implicit-timezone"))) {
        return false;
      }
      if (node.getType() == XQ.VariableRef || node.getType() == XQ.ContextItemExpr) {
        final QNm name = node.getType() == XQ.ContextItemExpr
            ? Bits.FS_DOT
            : (QNm) node.getValue();
        final Var variable = findScope(node).resolve(name);
        if (BindingDependencies.parameter(node, name, variable == null
            ? null
            : variable.scope.getNode()))
          return false;
        if (variable == null) {
          final AST declared = declarations.get(name);
          if (declared == null)
            return false;
          for (int i = 0; i < declared.getChildCount(); i++) {
            if (declared.getChild(i).getType() == XQ.ExternalVariable)
              return false;
          }
          if (!pureSource(declared.getLastChild(), visited, true))
            return false;
          defaults.add(name);
          return true;
        }
        final AST binding = variable.scope.getNode();
        if (binding == candidateBinding)
          return true;
        // Every local binding's producer is already part of this source proof. This includes
        // variables rebound by a join and a filter's context item; both are engine-produced values.
        for (AST ancestor = binding; ancestor != null; ancestor = ancestor.getParent()) {
          if (ancestor == source)
            return true;
        }
        if (binding.getType() == XQ.LetBind || binding.getType() == XQ.ForBind) {
          // A positional for variable is generated by the engine, not a lazy source.
          if (binding.getType() == XQ.ForBind && !name.equals(binding.getChild(0).getChild(0).getValue()))
            return true;
          if (source == candidateSource)
            captured.add(name);
          return pureSource(initializer(binding), visited, globalDefault);
        }
        return binding.getType() == XQ.Count;
      }
      for (int i = 0; i < node.getChildCount(); i++) {
        if (!pure(node.getChild(i), source, visited, globalDefault)) {
          return false;
        }
      }
      return true;
    }

    private static AST initializer(final AST binding) {
      int sourceIndex = 1;
      if (binding.getType() == XQ.ForBind) {
        // A for binding may carry both allowing-empty and positional-variable markers.
        while (binding.getChild(sourceIndex).getType() == XQ.AllowingEmpty
            || binding.getChild(sourceIndex).getType() == XQ.TypedVariableBinding) {
          sourceIndex++;
        }
      }
      AST source = binding.getChild(sourceIndex);
      while (source.getType() == XQ.ParenthesizedExpr && source.getChildCount() == 1) {
        source = source.getChild(0);
      }
      return source;
    }
  }
}
