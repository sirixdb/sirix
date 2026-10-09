package io.sirix.query;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Expr;
import io.brackit.query.module.Module;
import io.brackit.query.module.StaticContext;
import io.brackit.query.module.Variables;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Claims the initially translated body once. Subsequent executions translate a fresh body from
 * the fenced plan before QueryExecution starts; no dispatch wrapper enters expression evaluation.
 */
final class SchemaFencedModule implements Module {
  private final SirixCompileChain chain;
  private final String text;
  private final CompiledQueryCache.Key key;
  private final Module delegate;
  private final AtomicReference<Module> initial;

  SchemaFencedModule(final SirixCompileChain chain, final String text, final CompiledQueryCache.Key key,
      final Module delegate) {
    this.chain = Objects.requireNonNull(chain);
    this.text = Objects.requireNonNull(text);
    this.key = Objects.requireNonNull(key);
    this.delegate = Objects.requireNonNull(delegate);
    initial = new AtomicReference<>(delegate);
  }

  @Override
  public Expr getBody() {
    Module current = this;
    while (current instanceof SchemaFencedModule fenced) {
      final Module first = fenced.initial.getAndSet(null);
      if (first != null && fenced.chain.matchesExecutionBinding(fenced.key)) {
        return first.getBody();
      }
      current = fenced.chain.compile(fenced.text);
    }
    return current.getBody();
  }

  @Override
  public String getTargetNS() {
    return delegate.getTargetNS();
  }

  @Override
  public void importModule(final Module module) {
    delegate.importModule(module);
  }

  @Override
  public List<Module> getImportedModules() {
    return delegate.getImportedModules();
  }

  @Override
  public void addOption(final QNm name, final Str value) {
    delegate.addOption(name, value);
  }

  @Override
  public Map<QNm, Str> getOptions() {
    return delegate.getOptions();
  }

  @Override
  public StaticContext getStaticContext() {
    return delegate.getStaticContext();
  }

  @Override
  public Variables getVariables() {
    return delegate.getVariables();
  }
}
