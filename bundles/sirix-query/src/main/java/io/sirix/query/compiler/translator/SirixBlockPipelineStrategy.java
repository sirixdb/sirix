package io.sirix.query.compiler.translator;

import io.brackit.query.QueryException;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.translator.BlockPipelineStrategy;
import io.brackit.query.compiler.translator.Compiler;
import io.brackit.query.jdm.Expr;

public final class SirixBlockPipelineStrategy extends BlockPipelineStrategy {
  @Override
  public Expr compilePipeExpr(final AST node, final Compiler compiler) throws QueryException {
    final Expr routed = super.compilePipeExpr(node, compiler);
    if (SirixPipelineStrategy.needsGroupingFallback(node)) {
      return new VectorizedFallbackExpr(routed, super.compileGenericPipeExpr(node, compiler));
    }
    return routed;
  }
}
