package io.sirix.query.function.sdb.explain;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.optimizer.VectorizedRoutingStage;
import io.sirix.query.compiler.optimizer.mesh.EquivalenceClass;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.mesh.PlanAlternative;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.vectorized.VectorizedPredicate;

import java.util.List;

/**
 * Serializes an optimized AST into a structured, human-readable JSON query plan.
 *
 * <p>Unlike {@link AST#toJSON()}, this serializer:
 * <ul>
 *   <li>Resolves SirixDB's custom AST types ({@link XQExt}) instead of outputting "UNKNOWN"</li>
 *   <li>Groups cost-based properties into logical sections (cost, index, join, vectorized)</li>
 *   <li>Adds human-readable summary lines for quick plan inspection</li>
 *   <li>Pretty-prints with indentation for readability</li>
 * </ul>
 */
public final class QueryPlanSerializer {

  private static final String INDENT = "  ";

  private QueryPlanSerializer() {}

  /**
   * Serialize an optimized AST to a pretty-printed JSON plan string.
   *
   * @param ast the optimized AST root
   * @return JSON string
   */
  public static String serialize(AST ast) {
    if (ast == null) {
      return "null";
    }
    final var sb = new StringBuilder(4096);
    serializeNode(ast, sb, 0);
    return sb.toString();
  }

  /**
   * Serialize both parsed and optimized ASTs to a JSON object with two fields.
   *
   * @param parsed    the parsed AST (before optimization)
   * @param optimized the optimized AST (after optimization)
   * @return JSON string with "parsed" and "optimized" fields
   */
  public static String serializeBoth(AST parsed, AST optimized) {
    final var sb = new StringBuilder(8192);
    sb.append("{\n");
    indent(sb, 1);
    sb.append("\"parsed\": ");
    if (parsed != null) {
      serializeNode(parsed, sb, 1);
    } else {
      sb.append("null");
    }
    sb.append(",\n");
    indent(sb, 1);
    sb.append("\"optimized\": ");
    if (optimized != null) {
      serializeNode(optimized, sb, 1);
    } else {
      sb.append("null");
    }
    sb.append('\n');
    sb.append('}');
    return sb.toString();
  }

  /**
   * Serialize the optimized plan together with all candidate plans from the Mesh.
   *
   * @param optimized the optimized AST
   * @param mesh      the Mesh containing equivalence classes with plan alternatives
   * @return JSON string with "chosenPlan" and "candidates" fields
   */
  public static String serializeWithCandidates(AST optimized, Mesh mesh) {
    final var sb = new StringBuilder(8192);
    sb.append("{\n");
    indent(sb, 1);
    sb.append("\"chosenPlan\": ");
    if (optimized != null) {
      serializeNode(optimized, sb, 1);
    } else {
      sb.append("null");
    }
    sb.append(",\n");
    indent(sb, 1);
    sb.append("\"candidates\": ");
    if (mesh != null && mesh.classCount() > 0) {
      serializeMesh(mesh, sb, 1);
    } else {
      sb.append("[]");
    }
    sb.append('\n');
    sb.append('}');
    return sb.toString();
  }

  private static void serializeMesh(Mesh mesh, StringBuilder sb, int depth) {
    sb.append("[\n");
    boolean firstClass = true;
    for (int classId = 0; classId < mesh.nextClassId(); classId++) {
      final EquivalenceClass eqClass = mesh.getClass(classId);
      if (eqClass == null || eqClass.size() <= 1) {
        continue; // skip classes with only one alternative (no choice was made)
      }
      if (!firstClass) {
        sb.append(",\n");
      }
      firstClass = false;
      indent(sb, depth + 1);
      sb.append("{\n");
      indent(sb, depth + 2);
      sb.append("\"equivalenceClassId\": ").append(eqClass.classId()).append(",\n");
      indent(sb, depth + 2);
      sb.append("\"alternativeCount\": ").append(eqClass.size()).append(",\n");
      indent(sb, depth + 2);
      sb.append("\"bestCost\": ").append(eqClass.getBestCost()).append(",\n");
      indent(sb, depth + 2);
      sb.append("\"alternatives\": [\n");
      for (int i = 0; i < eqClass.size(); i++) {
        if (i > 0) {
          sb.append(",\n");
        }
        final PlanAlternative alt = eqClass.getAlternative(i);
        indent(sb, depth + 3);
        sb.append("{\n");
        indent(sb, depth + 4);
        sb.append("\"cost\": ").append(alt.cost()).append(",\n");
        indent(sb, depth + 4);
        sb.append("\"chosen\": ").append(alt.cost() == eqClass.getBestCost()).append(",\n");
        indent(sb, depth + 4);
        sb.append("\"plan\": ");
        serializeNode(alt.plan(), sb, depth + 4);
        sb.append('\n');
        indent(sb, depth + 3);
        sb.append('}');
      }
      sb.append('\n');
      indent(sb, depth + 2);
      sb.append("]\n");
      indent(sb, depth + 1);
      sb.append('}');
    }
    sb.append('\n');
    indent(sb, depth);
    sb.append(']');
  }

  private static void serializeNode(AST node, StringBuilder sb, int depth) {
    sb.append("{\n");
    final int d = depth + 1;

    // Operator type
    final String typeName = resolveTypeName(node.getType());
    jsonField(sb, d, "operator", typeName);

    // Value (if meaningful)
    final String value = node.getStringValue();
    if (value != null && !value.equals(typeName)) {
      sb.append(",\n");
      jsonField(sb, d, "value", value);
    }

    // Cost section
    if (hasCostProperties(node)) {
      sb.append(",\n");
      serializeCostSection(node, sb, d);
    }

    // Index section
    if (hasIndexProperties(node)) {
      sb.append(",\n");
      serializeIndexSection(node, sb, d);
    }

    // Join section
    if (hasJoinProperties(node)) {
      sb.append(",\n");
      serializeJoinSection(node, sb, d);
    }

    // Vectorized section
    if (hasVectorizedProperties(node)) {
      sb.append(",\n");
      serializeVectorizedSection(node, sb, d);
    }

    // Decomposition section
    if (node.getProperty(CostProperties.DECOMPOSITION_APPLICABLE) != null) {
      sb.append(",\n");
      serializeDecompositionSection(node, sb, d);
    }

    // Fusion section
    if (node.getProperty(CostProperties.JOIN_FUSED) != null
        || node.getProperty(CostProperties.FUSED_COUNT) != null) {
      sb.append(",\n");
      serializeFusionSection(node, sb, d);
    }

    // Summary line
    final String summary = buildSummary(node, typeName);
    if (summary != null) {
      sb.append(",\n");
      jsonField(sb, d, "summary", summary);
    }

    // Children
    if (node.getChildCount() > 0) {
      sb.append(",\n");
      indent(sb, d);
      sb.append("\"children\": [\n");
      for (int i = 0; i < node.getChildCount(); i++) {
        if (i > 0) {
          sb.append(",\n");
        }
        indent(sb, d + 1);
        serializeNode(node.getChild(i), sb, d + 1);
      }
      sb.append('\n');
      indent(sb, d);
      sb.append(']');
    }

    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Cost section ---

  private static boolean hasCostProperties(AST node) {
    return node.getProperty(CostProperties.ESTIMATED_CARDINALITY) != null
        || node.getProperty(CostProperties.PATH_CARDINALITY) != null
        || node.getProperty(CostProperties.INDEX_SCAN_COST) != null
        || node.getProperty(CostProperties.TOTAL_NODE_COUNT) != null;
  }

  private static void serializeCostSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"cost\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "estimatedCardinality",
        node.getProperty(CostProperties.ESTIMATED_CARDINALITY));
    first = emitIfPresent(sb, depth + 1, first, "pathCardinality",
        node.getProperty(CostProperties.PATH_CARDINALITY));
    first = emitIfPresent(sb, depth + 1, first, "totalNodeCount",
        node.getProperty(CostProperties.TOTAL_NODE_COUNT));
    first = emitIfPresent(sb, depth + 1, first, "indexScanCost",
        node.getProperty(CostProperties.INDEX_SCAN_COST));
    emitIfPresent(sb, depth + 1, first, "seqScanCost",
        node.getProperty(CostProperties.SEQ_SCAN_COST));
    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Index section ---

  private static boolean hasIndexProperties(AST node) {
    return node.getProperty(CostProperties.PREFER_INDEX) != null
        || node.getProperty(CostProperties.INDEX_ID) != null
        || node.getProperty(CostProperties.INDEX_GATE_CLOSED) != null;
  }

  private static void serializeIndexSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"index\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "preferIndex",
        node.getProperty(CostProperties.PREFER_INDEX));
    first = emitIfPresent(sb, depth + 1, first, "indexId",
        node.getProperty(CostProperties.INDEX_ID));
    first = emitIfPresent(sb, depth + 1, first, "indexType",
        node.getProperty(CostProperties.INDEX_TYPE));
    emitIfPresent(sb, depth + 1, first, "gateClosed",
        node.getProperty(CostProperties.INDEX_GATE_CLOSED));
    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Join section ---

  private static boolean hasJoinProperties(AST node) {
    return node.getProperty(CostProperties.JOIN_REORDERED) != null
        || node.getProperty(CostProperties.JOIN_COST) != null;
  }

  private static void serializeJoinSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"join\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "reordered",
        node.getProperty(CostProperties.JOIN_REORDERED));
    first = emitIfPresent(sb, depth + 1, first, "leftCardinality",
        node.getProperty(CostProperties.JOIN_LEFT_CARD));
    first = emitIfPresent(sb, depth + 1, first, "rightCardinality",
        node.getProperty(CostProperties.JOIN_RIGHT_CARD));
    first = emitIfPresent(sb, depth + 1, first, "cost",
        node.getProperty(CostProperties.JOIN_COST));
    emitIfPresent(sb, depth + 1, first, "swapped",
        node.getProperty(CostProperties.JOIN_SWAPPED));
    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Vectorized section ---

  private static boolean hasVectorizedProperties(AST node) {
    return node.getProperty(VectorizedRoutingStage.VECTORIZED_ROUTE) != null
        || node.getProperty("vectorized.vectorizable") != null;
  }

  @SuppressWarnings("unchecked")
  private static void serializeVectorizedSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"vectorized\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "route",
        node.getProperty(VectorizedRoutingStage.VECTORIZED_ROUTE));

    final Object predicatesObj = node.getProperty(VectorizedRoutingStage.VECTORIZED_PREDICATES);
    if (predicatesObj instanceof List<?> predicatesList && !predicatesList.isEmpty()) {
      if (!first) {
        sb.append(",\n");
      }
      indent(sb, depth + 1);
      sb.append("\"predicates\": [\n");
      for (int i = 0; i < predicatesList.size(); i++) {
        if (i > 0) {
          sb.append(",\n");
        }
        final Object item = predicatesList.get(i);
        if (item instanceof VectorizedPredicate pred) {
          indent(sb, depth + 2);
          sb.append("{\"field\": \"").append(escapeJson(pred.fieldName()))
              .append("\", \"op\": \"").append(pred.op())
              .append("\", \"constant\": ").append(jsonValue(pred.constant()))
              .append(", \"type\": \"").append(pred.type())
              .append("\"}");
        }
      }
      sb.append('\n');
      indent(sb, depth + 1);
      sb.append(']');
      first = false;
    }

    // Database/resource context (on VectorizedPipelineExpr nodes)
    first = emitIfPresent(sb, depth + 1, first, "databaseName",
        node.getProperty("databaseName"));
    emitIfPresent(sb, depth + 1, first, "resourceName",
        node.getProperty("resourceName"));

    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Decomposition section ---

  private static void serializeDecompositionSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"decomposition\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "applicable",
        node.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    first = emitIfPresent(sb, depth + 1, first, "type",
        node.getProperty(CostProperties.DECOMPOSITION_TYPE));
    first = emitIfPresent(sb, depth + 1, first, "rule5",
        node.getProperty(CostProperties.DECOMPOSITION_RULE_5));
    first = emitIfPresent(sb, depth + 1, first, "rule6",
        node.getProperty(CostProperties.DECOMPOSITION_RULE_6));
    emitIfPresent(sb, depth + 1, first, "intersect",
        node.getProperty(CostProperties.DECOMPOSITION_INTERSECT));
    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Fusion section ---

  private static void serializeFusionSection(AST node, StringBuilder sb, int depth) {
    indent(sb, depth);
    sb.append("\"fusion\": {\n");
    boolean first = true;
    first = emitIfPresent(sb, depth + 1, first, "joinFused",
        node.getProperty(CostProperties.JOIN_FUSED));
    first = emitIfPresent(sb, depth + 1, first, "groupId",
        node.getProperty(CostProperties.JOIN_FUSION_GROUP_ID));
    first = emitIfPresent(sb, depth + 1, first, "predicateCount",
        node.getProperty(CostProperties.FUSED_COUNT));
    first = emitIfPresent(sb, depth + 1, first, "operator",
        node.getProperty(CostProperties.FUSED_OPERATOR));
    emitIfPresent(sb, depth + 1, first, "fieldName",
        node.getProperty(CostProperties.FUSED_FIELD_NAME));
    sb.append('\n');
    indent(sb, depth);
    sb.append('}');
  }

  // --- Summary ---

  private static String buildSummary(AST node, String typeName) {
    final int type = node.getType();

    if (type == XQExt.IndexExpr) {
      return buildIndexExprSummary(node);
    }
    if (type == XQExt.VectorizedPipelineExpr) {
      return buildVectorizedSummary(node);
    }

    final Object preferIdx = node.getProperty(CostProperties.PREFER_INDEX);
    final Object estCard = node.getProperty(CostProperties.ESTIMATED_CARDINALITY);

    if (preferIdx != null || estCard != null) {
      final var parts = new StringBuilder();
      parts.append(typeName);
      if (estCard instanceof Number n) {
        parts.append(", ~").append(n.longValue()).append(" rows");
      }
      if (Boolean.TRUE.equals(preferIdx)) {
        final Object idxType = node.getProperty(CostProperties.INDEX_TYPE);
        parts.append(", index scan");
        if (idxType != null) {
          parts.append(" (").append(idxType).append(')');
        }
      } else if (Boolean.FALSE.equals(preferIdx)) {
        parts.append(", seq scan");
      }
      if (Boolean.TRUE.equals(node.getProperty(CostProperties.INDEX_GATE_CLOSED))) {
        parts.append(" [gate closed]");
      }
      return parts.toString();
    }

    if (Boolean.TRUE.equals(node.getProperty(CostProperties.JOIN_REORDERED))) {
      return typeName + ", reordered by DPhyp";
    }

    return null;
  }

  private static String buildIndexExprSummary(AST node) {
    final var sb = new StringBuilder("IndexScan");
    final Object idxType = node.getProperty(CostProperties.INDEX_TYPE);
    final Object idxId = node.getProperty(CostProperties.INDEX_ID);
    if (idxType != null) {
      sb.append(": ").append(idxType);
    }
    if (idxId != null) {
      sb.append(" index #").append(idxId);
    }
    final Object dbName = node.getProperty("databaseName");
    final Object resName = node.getProperty("resourceName");
    if (dbName != null && resName != null) {
      sb.append(" on ").append(dbName).append('/').append(resName);
    }
    return sb.toString();
  }

  private static String buildVectorizedSummary(AST node) {
    final var sb = new StringBuilder("VectorizedPipeline");
    final Object route = node.getProperty(VectorizedRoutingStage.VECTORIZED_ROUTE);
    if (route != null) {
      sb.append(": ").append(route).append(" route");
    }
    final Object preds = node.getProperty(VectorizedRoutingStage.VECTORIZED_PREDICATES);
    if (preds instanceof List<?> list) {
      sb.append(", ").append(list.size()).append(" predicate(s)");
    }
    return sb.toString();
  }

  // --- Helpers ---

  static String resolveTypeName(int type) {
    // Check XQExt range first (SirixDB custom types)
    if (type >= XQExt.MultiStepExpr && type <= XQExt.VectorizedPipelineExpr) {
      try {
        return (String) XQExt.toName(type);
      } catch (ArrayIndexOutOfBoundsException e) {
        // fall through
      }
    }
    // Standard Brackit types
    if (type >= 0 && type < XQ.NAMES.length) {
      return XQ.NAMES[type];
    }
    return "Unknown(" + type + ")";
  }

  /**
   * Emit a property field if the value is non-null. Returns false if a field was emitted
   * (so the next call knows to prepend a comma).
   */
  private static boolean emitIfPresent(StringBuilder sb, int depth, boolean first,
      String key, Object value) {
    if (value == null) {
      return first;
    }
    if (!first) {
      sb.append(",\n");
    }
    indent(sb, depth);
    sb.append('"').append(key).append("\": ").append(jsonValue(value));
    return false;
  }

  private static String jsonValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Boolean || value instanceof Number) {
      return value.toString();
    }
    return "\"" + escapeJson(value.toString()) + "\"";
  }

  private static void jsonField(StringBuilder sb, int depth, String key, String value) {
    indent(sb, depth);
    sb.append('"').append(key).append("\": \"").append(escapeJson(value)).append('"');
  }

  private static void indent(StringBuilder sb, int depth) {
    for (int i = 0; i < depth; i++) {
      sb.append(INDENT);
    }
  }

  private static String escapeJson(String str) {
    if (str == null) {
      return "";
    }
    final var sb = new StringBuilder(str.length());
    for (int i = 0; i < str.length(); i++) {
      final char c = str.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < ' ') {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }
}
