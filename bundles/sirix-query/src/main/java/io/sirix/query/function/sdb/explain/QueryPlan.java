package io.sirix.query.function.sdb.explain;

import io.brackit.query.Query;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.CompileChain;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.XmlDBStore;

/**
 * Programmatic API for query plan inspection.
 *
 * <p>Compiles a query through the full SirixDB optimizer pipeline and exposes
 * the resulting AST with all cost annotations. Designed for plan verification
 * tests and tooling integration.</p>
 *
 * <p>Usage:
 * <pre>{@code
 * QueryPlan plan = QueryPlan.explain(
 *     "for $x in jn:doc('db','res')[][] where $x.price > 50 return $x",
 *     jsonStore, xmlStore);
 * assertTrue(plan.usesIndex());
 * assertEquals("columnar", plan.vectorizedRoute());
 * System.out.println(plan.toJSON());
 * }</pre>
 *
 * @param optimizedAST the optimized AST after all 10 optimizer stages
 * @param parsedAST    the parsed AST before optimization (null if not requested)
 */
public record QueryPlan(AST optimizedAST, AST parsedAST) {

  /**
   * Compile a query through the full optimizer pipeline and return the plan.
   *
   * <p>The provided stores are borrowed (not closed). The caller retains ownership.</p>
   *
   * @param query     the JSONiq query string
   * @param jsonStore the JSON database store
   * @param xmlStore  the XML node store (may be null)
   * @return the query plan
   */
  public static QueryPlan explain(String query, JsonDBStore jsonStore, XmlDBStore xmlStore) {
    // Create a compile chain that borrows the caller's stores.
    // We intentionally do NOT close this chain — close() would close the borrowed stores.
    final var chain = new SirixCompileChain(xmlStore, jsonStore);
    chain.compile(query);
    return new QueryPlan(chain.getOptimizedAST(), chain.getParsedAST());
  }

  /**
   * Serialize the optimized plan to pretty-printed JSON.
   */
  public String toJSON() {
    return QueryPlanSerializer.serialize(optimizedAST);
  }

  /**
   * Serialize both parsed and optimized plans to JSON.
   */
  public String toVerboseJSON() {
    return QueryPlanSerializer.serializeBoth(parsedAST, optimizedAST);
  }

  /**
   * Get Brackit's raw JSON serialization of the optimized AST (includes all properties).
   */
  public String toRawJSON() {
    return optimizedAST != null ? optimizedAST.toJSON() : "null";
  }

  /**
   * Get the estimated cardinality at the root of the optimized plan.
   *
   * @return estimated cardinality, or -1 if not annotated
   */
  public long estimatedCardinality() {
    if (optimizedAST == null) {
      return -1L;
    }
    return CostProperties.extractCardinality(optimizedAST, 3);
  }

  /**
   * Check if the optimizer decided to use an index for any node in the plan.
   */
  public boolean usesIndex() {
    return searchProperty(optimizedAST, CostProperties.PREFER_INDEX, Boolean.TRUE, 20);
  }

  /**
   * Check if any node in the plan has the index gate closed.
   */
  public boolean hasClosedGate() {
    return searchProperty(optimizedAST, CostProperties.INDEX_GATE_CLOSED, Boolean.TRUE, 20);
  }

  /**
   * Get the vectorized route if the plan uses vectorized execution, or null.
   */
  public String vectorizedRoute() {
    if (optimizedAST == null) {
      return null;
    }
    return findStringProperty(optimizedAST, "vectorized.route", 20);
  }

  /**
   * Check if join reordering was applied anywhere in the plan.
   */
  public boolean isJoinReordered() {
    return searchProperty(optimizedAST, CostProperties.JOIN_REORDERED, Boolean.TRUE, 20);
  }

  private static boolean searchProperty(AST node, String key, Object expectedValue, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return false;
    }
    if (expectedValue.equals(node.getProperty(key))) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (searchProperty(node.getChild(i), key, expectedValue, maxDepth - 1)) {
        return true;
      }
    }
    return false;
  }

  private static String findStringProperty(AST node, String key, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return null;
    }
    final Object value = node.getProperty(key);
    if (value instanceof String str) {
      return str;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final String found = findStringProperty(node.getChild(i), key, maxDepth - 1);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
