package io.sirix.query;

import io.brackit.query.atomic.AnyURI;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.translator.Translator;
import io.brackit.query.expr.DeclVariable;
import io.brackit.query.expr.Variable;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.MainModule;
import io.brackit.query.module.Module;
import io.brackit.query.module.StaticContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Detached code and declaration metadata. No translated expression or execution unit is cached. */
final class OptimizedQueryPlan {
  private record Declaration(QNm name, SequenceType type) {
  }

  private record Target(QNm declaration, AST tree, boolean allowUpdate) {
  }

  private record Settings(AnyURI baseURI, String collation, String functionNamespace, String elementNamespace,
                          boolean boundaryStrip, boolean constructionStrip, boolean ordered,
                          boolean emptyGreatest, boolean preserveNS, boolean inheritNS,
                          Map<QNm, Str> options, Map<String, String> namespaces) {
    void apply(final StaticContext context) {
      context.setBaseURI(baseURI);
      context.setDefaultCollation(collation);
      context.setBoundarySpaceStrip(boundaryStrip);
      context.setConstructionModeStrip(constructionStrip);
      context.setOrderingModeOrdered(ordered);
      context.setEmptyOrderGreatest(emptyGreatest);
      context.setCopyNSPreserve(preserveNS);
      context.setCopyNSInherit(inheritNS);
      context.getNamespaces().setDefaultFunctionNamespace(functionNamespace);
      context.getNamespaces().setDefaultElementNamespace(elementNamespace);
      namespaces.forEach(context.getNamespaces()::declare);
      options.forEach(context::addOption);
    }
  }

  private final Settings settings;
  private final Map<QNm, Str> options;
  private final Declaration[] declarations;
  private final Target[] targets;

  private OptimizedQueryPlan(final Settings settings, final Map<QNm, Str> options,
      final Declaration[] declarations, final Target[] targets) {
    this.settings = settings;
    this.options = options;
    this.declarations = declarations;
    this.targets = targets;
  }

  MainModule translate(final Translator translator) {
    final MainModule module = new MainModule();
    final StaticContext context = module.getStaticContext();
    settings.apply(context);
    options.forEach(module::addOption);
    for (final Declaration declaration : declarations) {
      module.getVariables().declare(declaration.name(), declaration.type(), true);
    }
    for (final Target target : targets) {
      final AST tree = target.tree().copyTree();
      bindContext(tree, context);
      final Expr expression = translator.expression(module, context, tree, target.allowUpdate());
      if (target.declaration() == null) {
        module.setExpr(expression);
      } else {
        ((DeclVariable) module.getVariables().resolve(target.declaration())).setExpr(expression);
      }
    }
    return module;
  }

  Map<QNm, Str> options() {
    return options;
  }

  private static void bindContext(final AST tree, final StaticContext context) {
    tree.setStaticContext(context);
    for (int i = 0; i < tree.getChildCount(); i++) {
      bindContext(tree.getChild(i), context);
    }
  }

  /** Captures targets through the original compiler's hooks, before translation mutates units. */
  static final class Builder {
    private final List<Target> targets = new ArrayList<>(8);
    private boolean supported = true;
    private int translated;

    void prepare(final AST input) {
      if (!supported) return;
      QNm declaration = null;
      boolean body = false;
      for (AST ancestor = input.getParent(); ancestor != null; ancestor = ancestor.getParent()) {
        if (ancestor.getType() == XQ.TypedVariableDeclaration) {
          for (int i = 0; i < ancestor.getChildCount(); i++) {
            if (ancestor.getChild(i).getValue() instanceof QNm name) {
              declaration = name;
              break;
            }
          }
          break;
        }
        if (ancestor.getType() == XQ.QueryBody) {
          body = true;
          break;
        }
      }
      if (declaration == null && !body) {
        supported = false;
        return;
      }
      targets.add(new Target(declaration, null, body));
    }

    void optimized(final StaticContext context, final AST optimized) {
      if (!supported || !reusable(optimized, context)) {
        supported = false;
        return;
      }
      final AST detached = optimized.copyTree();
      bindContext(detached, null);
      final int index = targets.size() - 1;
      final Target pending = targets.get(index);
      targets.set(index, new Target(pending.declaration(), detached, pending.allowUpdate()));
    }

    void translated(final boolean allowUpdate) {
      if (supported && (translated >= targets.size() || targets.get(translated).allowUpdate() != allowUpdate)) {
        supported = false;
      }
      translated++;
    }

    OptimizedQueryPlan finish(final Module module, final AST parsed) {
      if (!supported || targets.isEmpty() || translated != targets.size() || !(module instanceof MainModule)) {
        return null;
      }
      final var variables = module.getVariables().getDeclaredVariables();
      final Declaration[] declarations = new Declaration[variables.size()];
      int index = 0;
      for (final Variable variable : variables) {
        if (!(variable instanceof DeclVariable)) return null;
        declarations[index++] = new Declaration(variable.getName(), variable.getType());
      }
      int bodies = 0;
      for (final Target target : targets) {
        if (target.declaration() == null) bodies++;
        else if (!module.getVariables().isDeclared(target.declaration())) return null;
      }
      if (bodies != 1) return null;
      final Map<String, String> namespaces = new HashMap<>();
      collectNamespaces(parsed, namespaces);
      final StaticContext context = module.getStaticContext();
      final Settings settings = new Settings(context.getBaseURI(), context.getDefaultCollation(),
          context.getNamespaces().getDefaultFunctionNamespace(), context.getNamespaces().getDefaultElementNamespace(),
          context.isBoundarySpaceStrip(), context.isConstructionModeStrip(), context.isOrderingModeOrdered(),
          context.isEmptyOrderGreatest(), context.isCopyNSPreserve(), context.isCopyNSInherit(),
          Map.copyOf(context.getOptions()), Map.copyOf(namespaces));
      return new OptimizedQueryPlan(settings, Map.copyOf(module.getOptions()), declarations, targets.toArray(Target[]::new));
    }

    private static boolean reusable(final AST node, final StaticContext context) {
      if ((node.getStaticContext() != null && node.getStaticContext() != context) || node.getProperty("udf") != null) {
        return false;
      }
      for (int i = 0; i < node.getChildCount(); i++) {
        if (!reusable(node.getChild(i), context)) return false;
      }
      return true;
    }

    private static void collectNamespaces(final AST node, final Map<String, String> namespaces) {
      if (node.getType() == XQ.NamespaceDeclaration) {
        namespaces.put(node.getChild(0).getStringValue(), node.getChild(1).getStringValue());
      }
      for (int i = 0; i < node.getChildCount(); i++) collectNamespaces(node.getChild(i), namespaces);
    }
  }
}
