package io.sirix.query.compiler.optimizer;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.Optimizer;
import io.brackit.query.compiler.optimizer.TopDownOptimizer;
import io.brackit.query.module.StaticContext;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class BindingDependencyWorkTest {
  @ParameterizedTest
  @ValueSource(ints = {12, 24})
  void duplicatedAliasInputsHaveBoundedProofWorkAndExactAnswers(final int aliases) {
    final QNm seed = new QNm("seed");
    final int[] reads = new int[1];
    final int[] proofReads = new int[1];
    final CompileChain chain = new CompileChain() {
      @Override
      protected Optimizer getOptimizer(final Map<QNm, Str> options) {
        return new TopDownOptimizer(options) {
          @Override
          public AST optimize(final StaticContext context, final AST ast) {
            final AST optimized = super.optimize(context, ast);
            countReferences(optimized, seed, reads);
            reads[0] = 0;
            final AST result = new CheapFirstConjunctStage().rewrite(context, optimized);
            proofReads[0] += reads[0];
            return result;
          }
        };
      }
    };
    final StringBuilder text = new StringBuilder("declare variable $seed external; let $a0 := $seed ");
    for (int i = 1; i <= aliases; i++) {
      text.append("let $a").append(i).append(" := $a").append(i - 1).append(" + $a").append(i - 1).append(' ');
    }
    text.append("return ($a")
        .append(aliases)
        .append(", xs:integer($a")
        .append(aliases)
        .append(") gt 0 and $seed eq 1)");
    final Query query = new Query(chain, text.toString());
    assertTrue(proofReads[0] > 0, "the proof reaches the counted external input");
    assertTrue(proofReads[0] <= 1024, "duplicate aliases must not multiply terminal proof visits: " + proofReads[0]);
    final BrackitQueryContext context = new BrackitQueryContext();
    context.bind(seed, Int32.ONE);
    assertEquals((1 << aliases) + " true", serialize(query, context));
    context.bind(seed, Int32.ZERO);
    assertEquals("0 false", serialize(query, context));
  }

  private static void countReferences(final AST node, final QNm seed, final int[] reads) {
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST child = node.getChild(i);
      if (child.getType() == XQ.VariableRef && seed.equals(child.getValue())) {
        node.replaceChild(i, new AST(XQ.VariableRef, seed) {
          @Override
          public Object getValue() {
            reads[0]++;
            return super.getValue();
          }
        });
      } else {
        countReferences(child, seed, reads);
      }
    }
  }

  private static String serialize(final Query query, final BrackitQueryContext context) {
    final StringWriter out = new StringWriter();
    try (final PrintWriter writer = new PrintWriter(out)) {
      query.serialize(context, writer);
    }
    return out.toString().trim();
  }
}
