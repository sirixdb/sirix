package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.Bits;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.topdown.ScopeWalker;
import io.brackit.query.module.StaticContext;
import java.util.HashSet;
import java.util.Set;

abstract class BindingDependencies extends ScopeWalker {
  // One term shares this budget across recursive source proofs: duplicated alias inputs must not
  // expand exponentially. Exhaustion declines the proof; BindingDependencyWorkTest counts visits.
  private static final int MAX_PROOF_WORK = 1024;
  private AST module;
  private int remainingWork;

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
    remainingWork = MAX_PROOF_WORK;
    return collect(node, candidate, inputs, captured, defaults, visited);
  }

  private boolean collect(final AST node, final AST candidate, final Set<QNm> inputs, final Set<QNm> captured,
      final Set<QNm> defaults, final Set<AST> visited) {
    if (remainingWork-- <= 0) {
      return false;
    }
    if (CheapFirstConjunctStage.storedReadCall(node)) {
      candidate.setProperty(CheapFirstConjunctStage.NATIVE_STORE, true);
    }
    if (node.getType() == XQ.VariableRef || node.getType() == XQ.ContextItemExpr) {
      return collectVariable(node, candidate, inputs, captured, defaults, visited);
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (!collect(node.getChild(i), candidate, inputs, captured, defaults, visited)) {
        return false;
      }
    }
    return true;
  }

  private boolean collectVariable(final AST node, final AST candidate, final Set<QNm> inputs, final Set<QNm> captured,
      final Set<QNm> defaults, final Set<AST> visited) {
    final QNm name = node.getType() == XQ.ContextItemExpr
        ? Bits.FS_DOT
        : (QNm) node.getValue();
    final Var variable = findScope(node).resolve(name);
    if (parameter(node, name, variable == null
        ? null
        : variable.scope.getNode())) {
      captured.add(name);
      return true;
    }
    if (variable == null) {
      return collectUnresolved(name, candidate, inputs, captured, defaults, visited);
    }
    final AST binding = variable.scope.getNode();
    if (binding.getType() == XQ.LetBind || binding.getType() == XQ.ForBind) {
      if (binding.getType() == XQ.ForBind && !name.equals(binding.getChild(0).getChild(0).getValue())) {
        return true;
      }
      final AST source = initializer(binding);
      if (!withinCandidate(binding, candidate) && captured != null && findScope(candidate).resolve(name) == variable) {
        return collectCaptured(name, binding, source, candidate, inputs, captured, defaults);
      }
      return !visited.add(source) || source(source, candidate, inputs, captured, defaults, visited);
    }
    return binding.getType() == XQ.Count || withinCandidate(binding, candidate);
  }

  private boolean collectUnresolved(final QNm name, final AST candidate, final Set<QNm> inputs,
      final Set<QNm> captured, final Set<QNm> defaults, final Set<AST> visited) {
    if (inputs != null) {
      inputs.add(name);
    }
    final AST declaration = declaration(name);
    if (declaration == null) {
      return name.equals(Bits.FS_DOT) || inputs != null;
    }
    final AST source = declaration.getLastChild();
    if (source.getType() == XQ.ExternalVariable || !visited.add(declaration)) {
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

  private boolean collectCaptured(final QNm name, final AST binding, final AST source, final AST candidate,
      final Set<QNm> inputs, final Set<QNm> captured, final Set<QNm> defaults) {
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
      return true;
    }
    if (freshRow(binding, candidate) && literalSource(source)) {
      inputs.addAll(sourceInputs);
      captured.addAll(sourceCaptured);
      defaults.addAll(sourceDefaults);
    } else {
      captured.add(name);
    }
    return true;
  }

  private static boolean withinCandidate(final AST binding, final AST candidate) {
    AST ancestor = binding;
    while (ancestor != null && ancestor != candidate) {
      ancestor = ancestor.getParent();
    }
    return ancestor == candidate;
  }

  private boolean source(final AST source, final AST candidate, final Set<QNm> inputs, final Set<QNm> captured,
      final Set<QNm> defaults, final Set<AST> visited) {
    return CheapFirstConjunctStage.cost(source, true) >= 0
        && collect(source, candidate, inputs, captured, defaults, visited);
  }

  static boolean parameter(final AST node, final QNm name, final AST binding) {
    for (AST parent = node.getParent(); parent != null; parent = parent.getParent()) {
      if (parent == binding) {
        return false;
      }
      if (parent.getType() == XQ.FunctionDecl || parent.getType() == XQ.InlineFuncItem) {
        for (int i = 0; i < parent.getChildCount(); i++) {
          final AST child = parent.getChild(i);
          if (child.getType() == XQ.TypedVariableDeclaration && child.getChildCount() > 0
              && name.equals(child.getChild(0).getValue())) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private static boolean freshRow(final AST binding, final AST candidate) {
    if (binding.getType() != XQ.ForBind) {
      return false;
    }
    AST parent = candidate;
    AST child = null;
    boolean selection = false;
    while (parent != null && parent != binding) {
      if (parent.getType() == XQ.ForBind || parent.getType() == XQ.Join || parent.getType() == XQ.GroupBy
          || parent.getType() == XQ.End || parent.getType() == XQ.PipeExpr) {
        return false;
      }
      selection |= parent.getType() == XQ.Selection && parent.getChild(0) == child;
      child = parent;
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
    return CheapFirstConjunctStage.storedReadCall(root);
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
