package io.sirix.query;

import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Expr;
import io.brackit.query.module.Module;
import io.brackit.query.module.StaticContext;
import io.brackit.query.module.Variables;
import io.sirix.query.compiler.optimizer.PlanCache;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Revalidates a retained Query before QueryExecution starts. Returning the original translated
 * body keeps schema checks and dispatch wrappers out of the expression evaluation path.
 */
final class SchemaFencedModule implements Module {
  private final SirixCompileChain chain;
  private final String text;
  private final long schemaVersion;
  private final Module delegate;

  SchemaFencedModule(final SirixCompileChain chain, final String text, final long schemaVersion, final Module delegate) {
    this.chain = Objects.requireNonNull(chain);
    this.text = Objects.requireNonNull(text);
    this.schemaVersion = schemaVersion;
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public Expr getBody() {
    Module current = this;
    while (current instanceof SchemaFencedModule fenced) {
      if (fenced.schemaVersion == PlanCache.indexSchemaVersion()) {
        return fenced.delegate.getBody();
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
