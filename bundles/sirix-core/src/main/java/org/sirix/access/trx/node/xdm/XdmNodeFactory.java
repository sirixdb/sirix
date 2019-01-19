package org.sirix.access.trx.node.xdm;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.exception.SirixIOException;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.xdm.AttributeNode;
import org.sirix.node.xdm.CommentNode;
import org.sirix.node.xdm.ElementNode;
import org.sirix.node.xdm.NamespaceNode;
import org.sirix.node.xdm.PINode;
import org.sirix.node.xdm.TextNode;

/**
 * Node factory for creating nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface XdmNodeFactory extends NodeFactory {
  /**
   * Create a {@link PINode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param target target of the processing instruction
   * @param content content of the processing instruction
   * @param isCompressed determines if the value is compressed or not
   * @param pathNodeKey path node key of node
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  PINode createPINode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, @Nonnegative long rightSibKey,
      QNm target, byte[] content, boolean isCompressed, @Nonnegative long pathNodeKey, SirixDeweyID id);

  /**
   * Create a {@link CommentNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value value of the node
   * @param isCompressed determines if the value is compressed or not
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  CommentNode createCommentNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey,
      @Nonnegative long rightSibKey, @Nonnull byte[] value, boolean isCompressed, SirixDeweyID id);

  /**
   * Create an {@link ElementNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param hash hash value associated with the node
   * @param name {@link QNm} of the node
   * @param pathNodeKey path node key associated with the element node
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  ElementNode createElementNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey,
      @Nonnegative long rightSibKey, long hash, QNm name, @Nonnegative long pathNodeKey, SirixDeweyID id);

  /**
   * Create a {@link TextNode}.
   *
   * @param parentKey parent node key
   * @param leftSibKey left sibling key
   * @param rightSibKey right sibling key
   * @param value value of the node
   * @param isCompressed determines if the value should be compressed or not
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  TextNode createTextNode(@Nonnegative long parentKey, @Nonnegative long leftSibKey, @Nonnegative long rightSibKey,
      byte[] value, boolean isCompressed, SirixDeweyID id);

  /**
   * Create an {@link AttributeNode}.
   *
   * @param parentKey parent node key
   * @param name the {@link QNm} of the attribute
   * @param value the value
   * @param pathNodeKey the path class record
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  AttributeNode createAttributeNode(@Nonnegative long parentKey, QNm name, byte[] value, @Nonnegative long pathNodeKey,
      SirixDeweyID id);

  /**
   * Create a {@link NamespaceNode}.
   *
   * @param parentKey parent node key
   * @param name the {@link QNm} of the namespace
   * @param pathNodeKey the path class record
   * @param id optional DeweyID
   * @return the created node
   * @throws SirixIOException if an I/O error occurs
   */
  NamespaceNode createNamespaceNode(@Nonnegative long parentKey, QNm name, @Nonnegative long pathNodeKey,
      SirixDeweyID id);
}
