package org.sirix.access.trx.node.json;

import javax.annotation.Nonnegative;
import javax.xml.namespace.QName;
import org.brackit.xquery.atomic.QNm;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.Kind;
import org.sirix.node.json.JSONArrayNode;
import org.sirix.node.json.JSONBooleanNode;
import org.sirix.node.json.JSONNumberNode;
import org.sirix.node.json.JSONObjectKeyNode;
import org.sirix.node.json.JSONObjectNode;
import org.sirix.node.json.JSONStringNode;

/**
 * Node factory for creating nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface JSONNodeFactory {
  /**
   * Create a {@link PathNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param name {@link QName} of the node
   * @param kind the kind of path node
   * @param level the level
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  PathNode createPathNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey, long hash,
      QNm name, Kind kind, @Nonnegative int level);

  /**
   * Create a {@link JSONArrayNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   */
  JSONArrayNode createJSONArrayNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash);

  /**
   * Create a {@link JSONObjectNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   */
  JSONObjectNode createJSONObjectNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash);

  /**
   * Create a {@link JSONObjectKeyNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param pathNodeKey path node key of node
   * @param name the name of the key
   */
  JSONObjectKeyNode createJSONObjectKeyNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash, long pathNodeKey, String name);

  /**
   * Create a {@link JSONStringNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param pathNodeKey path node key of node
   * @param value the value to store
   * @param isCompressed {@code true}, if the value is compressed, {@code false} otherwise
   */
  JSONStringNode createJSONStringNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash, byte[] value, boolean isCompressed);

  /**
   * Create a {@link JSONBooleanNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param pathNodeKey path node key of node
   * @param boolValue the boolean value
   */
  JSONBooleanNode createJSONBooleanNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash, boolean boolValue);

  /**
   * Create a {@link JSONNumberNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param pathNodeKey path node key of node
   * @param dblValue the number value
   */
  JSONNumberNode createJSONNumberNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, long rightSibKey,
      long hash, double dblValue);


}
