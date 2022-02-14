package org.sirix.access.trx.node.json;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.NodeFactory;
import org.sirix.node.DeweyIDNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.NullNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectBooleanNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;
import org.sirix.node.json.ObjectNullNode;
import org.sirix.node.json.ObjectNumberNode;
import org.sirix.node.json.ObjectStringNode;
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
  ArrayNode createJsonArrayNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey, SirixDeweyID id);

  /**
   * Create a {@link ObjectNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  ObjectNode createJsonObjectNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id);

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
  ObjectKeyNode createJsonObjectKeyNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, long objectValueKey, SirixDeweyID id);

  /**
   * Create a {@link StringNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the value to store
   * @param isCompressed {@code true}, if the value is compressed, {@code false} otherwise
   */
  StringNode createJsonStringNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean isCompressed, SirixDeweyID id);

  /**
   * Create a {@link BooleanNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param boolValue the boolean value
   */
  BooleanNode createJsonBooleanNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, boolean boolValue, SirixDeweyID id);

  /**
   * Create a {@link NumberNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value the number value
   */
  NumberNode createJsonNumberNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, Number value, SirixDeweyID id);

  /**
   * Create a {@link NullNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   */
  NullNode createJsonNullNode(@NonNegative long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id);

  /**
   * Create a {@link StringNode}.
   *
   * @param parentKey parent node key
   * @param value the value to store
   * @param isCompressed {@code true}, if the value is compressed, {@code false} otherwise
   */
  ObjectStringNode createJsonObjectStringNode(@NonNegative long parentKey, byte[] value,
      boolean isCompressed, SirixDeweyID id);

  /**
   * Create a {@link ObjectBooleanNode}.
   *
   * @param parentKey parent node key
   * @param boolValue the boolean value
   */
  ObjectBooleanNode createJsonObjectBooleanNode(@NonNegative long parentKey, boolean boolValue, SirixDeweyID id);

  /**
   * Create a {@link NumberNode}.
   *
   * @param parentKey parent node key
   * @param value the number value
   */
  ObjectNumberNode createJsonObjectNumberNode(@NonNegative long parentKey, Number value, SirixDeweyID id);

  /**
   * Create a {@link NullNode}.
   *
   * @param parentKey parent node key
   */
  ObjectNullNode createJsonObjectNullNode(@NonNegative long parentKey, SirixDeweyID id);

  /**
   * Create a {@link DeweyIDNode}.
   *
   * @param nodeKey node key
   */
  DeweyIDNode createDeweyIdNode(@NonNegative long nodeKey, @NonNull SirixDeweyID id);
}
