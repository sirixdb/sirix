package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.Bits;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.StaticContext;
import io.brackit.query.function.json.JSONFun;
import java.util.HashSet;
import java.util.Set;

abstract class BindingDependencies extends ScopeWalker {
  private AST module;

  BindingDependencies(final StaticContext sctx) {
    super(sctx);
  }

  @Override
  protected AST prepare(final AST ast) {
    module = ast;
    while (module.getParent() != null && module.getType() != XQ.MainModule && module.getType() != XQ.LibraryModule) {
      module = module.getParent();
    }
    super.prepare(module);
    return ast;
  }

  protected final boolean dependencies(final AST node, final AST candidate, final Set<QNm> inputs,
      final Set<QNm> captured, final Set<QNm> defaults, final Set<AST> visited) {
    if (node.getType() == XQ.VariableRef || node.getType() == XQ.ContextItemExpr) {
      final QNm name = node.getType() == XQ.ContextItemExpr
          ? Bits.FS_DOT
          : (QNm) node.getValue();
      final Var variable = findScope(node).resolve(name);
      if (variable == null) {
        if (inputs != null) {
          inputs.add(name);
        }
        final AST declaration = declaration(name);
        if (declaration == null) {
          return name.equals(Bits.FS_DOT) || inputs != null;
        }
        final AST source = declaration.getLastChild();
        if (source.getType() == XQ.ExternalVariable) {
          return true;
        }
        if (!visited.add(declaration)) {
          return true;
        }
        if (!source(source, candidate, inputs, captured, defaults, visited)) {
          return false;
        }
        if (defaults != null) {
          defaults.add(name);
        }
        return true;
      }
      final AST binding = variable.scope.getNode();
      if (binding.getType() == XQ.LetBind || binding.getType() == XQ.ForBind) {
        if (binding.getType() == XQ.ForBind && !name.equals(binding.getChild(0).getChild(0).getValue())) {
          return true;
        }
        final AST source = initializer(binding);
        AST ancestor = binding;
        while (ancestor != null && ancestor != candidate) {
          ancestor = ancestor.getParent();
        }
        if (ancestor == null && captured != null && findScope(candidate).resolve(name) == variable) {
          final Set<QNm> sourceInputs = new HashSet<>();
          final Set<QNm> sourceCaptured = new HashSet<>();
          final Set<QNm> sourceDefaults = new HashSet<>();
          if (!source(source, candidate, sourceInputs, sourceCaptured, sourceDefaults, new HashSet<>())) {
            return false;
          }
          // A direct filter sees its current freshly produced row before it can escape to
          // the consumer. Other captured composites may have been exposed or mutated already.
          if (freshRow(binding, candidate) && storedRead(source)) {
            // Opening arguments have already been evaluated to produce this row. They are
            // not inputs to its field predicates, even in a correlated temporal scan.
            candidate.setProperty(CheapFirstConjunctStage.NATIVE_STORE, true);
          } else if (freshRow(binding, candidate) && literalSource(source)) {
            inputs.addAll(sourceInputs);
            captured.addAll(sourceCaptured);
            defaults.addAll(sourceDefaults);
          } else {
            captured.add(name);
          }
          return true;
        }
        return !visited.add(source) || source(source, candidate, inputs, captured, defaults, visited);
      }
      if (binding.getType() == XQ.Count) {
        return true;
      }
      AST ancestor = binding;
      while (ancestor != null && ancestor != candidate) {
        ancestor = ancestor.getParent();
      }
      return ancestor == candidate;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!dependencies(node.getChild(i), candidate, inputs, captured, defaults, visited)) {
        return false;
      }
    }
    return true;
  }

  private boolean source(final AST source, final AST candidate, final Set<QNm> inputs, final Set<QNm> captured,
      final Set<QNm> defaults, final Set<AST> visited) {
    return CheapFirstConjunctStage.cost(source, true) >= 0
        && dependencies(source, candidate, inputs, captured, defaults, visited);
  }

  private static boolean freshRow(final AST binding, final AST candidate) {
    if (binding.getType() != XQ.ForBind) {
      return false;
    }
    AST parent = candidate;
    boolean selection = false;
    while (parent != null && parent != binding) {
      if (parent.getType() == XQ.ForBind || parent.getType() == XQ.Join || parent.getType() == XQ.GroupBy) {
        return false;
      }
      selection |= parent.getType() == XQ.Selection;
      parent = parent.getParent();
    }
    return parent == binding && selection;
  }

  private static boolean literalSource(final AST source) {
    if (source.getType() == XQ.VariableRef || source.getType() == XQ.ContextItemExpr) {
      return false;
    }
    for (int i = 0; i < source.getChildCount(); i++) {
      if (!literalSource(source.getChild(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean storedRead(final AST source) {
    AST root = source;
    while ((root.getType() == XQ.ArrayAccess || root.getType() == XQ.DerefExpr
        || root.getType() == XQ.ParenthesizedExpr) && root.getChildCount() > 0) {
      root = root.getChild(0);
    }
    if (root.getType() != XQ.FunctionCall || !(root.getValue() instanceof QNm name)
        || !JSONFun.JSON_NSURI.equals(name.getNamespaceURI())) {
      return false;
    }
    return switch (name.getLocalName()) {
      case "doc", "open", "open-bitemporal", "collection", "scan-valid-time-index", "scan-path-index", "scan-cas-index",
          "scan-cas-index-range", "scan-name-index" ->
        true;
      default -> false;
    };
  }

  private AST declaration(final QNm name) {
    for (int i = 0; i < module.getChildCount(); i++) {
      final AST prolog = module.getChild(i);
      if (prolog.getType() != XQ.Prolog) {
        continue;
      }
      for (int j = 0; j < prolog.getChildCount(); j++) {
        final AST declaration = prolog.getChild(j);
        if (name.equals(Bits.FS_DOT) && declaration.getType() == XQ.ContextItemDeclaration) {
          return declaration;
        }
        if (declaration.getType() != XQ.TypedVariableDeclaration) {
          continue;
        }
        for (int k = 0; k < declaration.getChildCount(); k++) {
          final AST child = declaration.getChild(k);
          if (child.getType() == XQ.Variable && name.equals(child.getValue())) {
            return declaration;
          }
        }
      }
    }
    return null;
  }

  protected static AST initializer(final AST binding) {
    AST initializer = binding.getChild(binding.getType() == XQ.ForBind && binding.getChildCount() == 4
        ? 2
        : 1);
    while (initializer.getType() == XQ.ParenthesizedExpr && initializer.getChildCount() == 1) {
      initializer = initializer.getChild(0);
    }
    return initializer;
  }
}
