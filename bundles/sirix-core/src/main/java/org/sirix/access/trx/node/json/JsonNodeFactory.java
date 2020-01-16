package org.sirix.access.trx.node.json;

import javax.annotation.Nonnegative;
import org.sirix.access.trx.node.NodeFactory;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.StringNode;

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
  ArrayNode createJsonArrayNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey);

  /**
   * Create a {@link ObjectNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  ObjectNode createJsonObjectNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey);

  /**
   * Create a {@link ObjectKeyNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param pathNodeKey path node key of node
   * @param name the name of the key
   * @param objectValueKey the value of the object key value
   */
  ObjectKeyNode createJsonObjectKeyNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, long objectValueKey);

  /**
   * Create a {@link StringNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the value to store
   * @param isCompressed {@code true}, if the value is compressed, {@code false} otherwise
   */
  StringNode createJsonStringNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean isCompressed);

  /**
   * Create a {@link BooleanNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param boolValue the boolean value
   */
  BooleanNode createJsonBooleanNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey, boolean boolValue);

  /**
   * Create a {@link NumberNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the number value
   */
  NumberNode createJsonNumberNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey, Number value);

  /**
   * Create a {@link NullNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  NullNode createJsonNullNode(@Nonnegative long parentKey, long leftSibKey, long rightSibKey);
}
