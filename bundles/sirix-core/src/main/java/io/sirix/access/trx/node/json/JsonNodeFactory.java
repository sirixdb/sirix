package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.NodeFactory;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.StringNode;

/**
 * Node factory for creating nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface JsonNodeFactory extends NodeFactory {
  /**
   * Create a {@link ArrayNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param pathNodeKey the path node key
   */
  ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id);

  /**
   * Create a {@link ObjectNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id);

  /**
   * Create a {@link StringNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the value to store
   * @param isCompressed {@code true}, if the value is compressed, {@code false} otherwise
   */
  StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean isCompressed, SirixDeweyID id);

  StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey,
      byte[] value, int valueOff, int valueLen, boolean isCompressed, SirixDeweyID id);

  /**
   * Create a {@link BooleanNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param boolValue the boolean value
   */
  BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id);

  /**
   * Create a {@link NumberNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the number value
   */
  NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id);

  /**
   * Create a {@link NullNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedBooleanNode} — fused OBJECT_KEY + BOOLEAN value.
   *
   * @param parentKey   parent node key
   * @param leftSibKey  left sibling key
   * @param rightSibKey right sibling key
   * @param pathNodeKey path node key of node
   * @param name        the name of the key
   * @param value       the boolean value
   */
  ObjectNamedBooleanNode createJsonObjectNamedBooleanNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, boolean value, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedNumberNode} — fused OBJECT_KEY + NUMBER value.
   */
  ObjectNamedNumberNode createJsonObjectNamedNumberNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, Number value, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedStringNode} — fused OBJECT_KEY + STRING value.
   */
  ObjectNamedStringNode createJsonObjectNamedStringNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, byte[] value, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedStringNode} — fused OBJECT_KEY + STRING value
   * accepting a slice of a (possibly reusable) UTF-8 buffer.
   */
  ObjectNamedStringNode createJsonObjectNamedStringNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name,
      byte[] value, int off, int len, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedNullNode} — fused OBJECT_KEY + NULL value.
   */
  ObjectNamedNullNode createJsonObjectNamedNullNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedObjectNode} — fused OBJECT_KEY + nested OBJECT.
   *
   * <p><b>Phase 1 stub</b>: implementation throws {@link UnsupportedOperationException}.
   * Phase 2 will provide a real implementation that emits a single fused record
   * (kindId 52) and pre-computes the {@code pathNodeKey} via the path-summary writer.
   */
  ObjectNamedObjectNode createJsonObjectNamedObjectNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, SirixDeweyID id);

  /**
   * Create a {@link ObjectNamedArrayNode} — fused OBJECT_KEY + nested ARRAY.
   *
   * <p><b>Phase 1 stub</b>: implementation throws {@link UnsupportedOperationException}.
   * Phase 2 will provide a real implementation that emits a single fused record
   * (kindId 53) and pre-computes the {@code pathNodeKey} via the path-summary writer.
   */
  ObjectNamedArrayNode createJsonObjectNamedArrayNode(long parentKey, long leftSibKey,
      long rightSibKey, long pathNodeKey, String name, SirixDeweyID id);

  /**
   * Create a {@link DeweyIDNode}.
   *
   * @param nodeKey node key
   */
  DeweyIDNode createDeweyIdNode(long nodeKey, SirixDeweyID id);
}
