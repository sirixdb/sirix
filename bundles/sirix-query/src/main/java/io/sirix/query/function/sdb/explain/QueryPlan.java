package io.sirix.query.function.sdb.explain;

import io.brackit.query.compiler.AST;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
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
 * @param mesh         the Mesh containing plan alternatives (null if not available)
 */
public record QueryPlan(AST optimizedAST, AST parsedAST, Mesh mesh) {

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
    return new QueryPlan(chain.getOptimizedAST(), chain.getParsedAST(), chain.getMesh());
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
   * Serialize the chosen plan together with all candidate plans from the Mesh.
   */
  public String toCandidatesJSON() {
    return QueryPlanSerializer.serializeWithCandidates(optimizedAST, mesh);
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
   * Check if the optimizer actually rewrote the plan to use an index.
   *
   * <p>Detects {@code IndexExpr} AST nodes created by IndexMatching
   * (JsonCASStep/JsonPathStep/JsonObjectKeyNameStep). This is the ground truth
   * of index usage — the cost model's {@code PREFER_INDEX} hint may or may not
   * result in an actual rewrite depending on whether a matching index exists.</p>
   */
  public boolean usesIndex() {
    return searchNodeType(optimizedAST, XQExt.IndexExpr, 20);
  }

  /**
   * Check if the cost model prefers an index scan over sequential scan.
   *
   * <p>This reflects the cost model's recommendation, which may differ from
   * actual index usage. For example, the cost model may prefer an index but
   * no matching CAS/PATH index exists for the specific predicate pattern.</p>
   */
  public boolean prefersIndex() {
    return searchProperty(optimizedAST, CostProperties.PREFER_INDEX, Boolean.TRUE, 20);
  }

  /**
   * Get the index type used in the plan, or null if no index is used.
   *
   * <p>Extracts from {@code IndexExpr} nodes created by IndexMatching.</p>
   */
  public String indexType() {
    if (optimizedAST == null) {
      return null;
    }
    return findIndexType(optimizedAST, 20);
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

  /**
   * Check if the plan contains an intersection join (Rule 6 decomposition).
   *
   * <p>An intersection join is created when both sides of a join have different
   * indexes. Both sides become index scans and the join intersects their results.</p>
   */
  public boolean isIntersectionJoin() {
    return searchProperty(optimizedAST, CostProperties.INTERSECTION_JOIN, Boolean.TRUE, 20);
  }

  /**
   * Check if any join decomposition restructuring was applied (Rules 5 or 6).
   */
  public boolean isDecompositionRestructured() {
    return searchProperty(optimizedAST, CostProperties.DECOMPOSITION_RESTRUCTURED, Boolean.TRUE, 20);
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

  private static boolean searchNodeType(AST node, int type, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return false;
    }
    if (node.getType() == type) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (searchNodeType(node.getChild(i), type, maxDepth - 1)) {
        return true;
      }
    }
    return false;
  }

  private static String findIndexType(AST node, int maxDepth) {
    if (node == null || maxDepth <= 0) {
      return null;
    }
    if (node.getType() == XQExt.IndexExpr) {
      final Object type = node.getProperty("indexType");
      if (type != null) {
        return type.toString();
      }
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final String found = findIndexType(node.getChild(i), maxDepth - 1);
      if (found != null) {
        return found;
      }
    }
    return null;
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
