package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.brackit.xquery.atomic.QNm;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.TextNode;

import com.google.common.base.Optional;

/**
 * Node factory for creating nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface NodeFactory {
	/**
	 * Create a {@link PathNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param leftSibKey
	 *          left sibling key
	 * @param rightSibKey
	 *          right sibling key
	 * @param hash
	 *          hash value associated with the node
	 * @param name
	 *          {@link QName} of the node
	 * @param pathNodeKey
	 *          path node key of node
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	PathNode createPathNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final long rightSibKey,
			final long hash, final QNm name, final Kind kind,
			final @Nonnegative int level) throws SirixIOException;

	/**
	 * Create a {@link PINode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param leftSibKey
	 *          left sibling key
	 * @param rightSibKey
	 *          right sibling key
	 * @param target
	 *          target of the processing instruction
	 * @param content
	 *          content of the processing instruction
	 * @param isCompressed
	 *          determines if the value is compressed or not
	 * @param pathNodeKey
	 *          path node key of node
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	PINode createPINode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final QNm target, final byte[] content, final boolean isCompressed,
			final @Nonnegative long pathNodeKey, final Optional<SirixDeweyID> id)
			throws SirixIOException;

	/**
	 * Create a {@link CommentNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param leftSibKey
	 *          left sibling key
	 * @param rightSibKey
	 *          right sibling key
	 * @param value
	 *          value of the node
	 * @param isCompressed
	 *          determines if the value is compressed or not
	 * @param id
	 *          an optional dewey ID
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	CommentNode createCommentNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			@Nonnull byte[] value, final boolean isCompressed,
			final Optional<SirixDeweyID> id) throws SirixIOException;

	/**
	 * Create an {@link ElementNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param leftSibKey
	 *          left sibling key
	 * @param rightSibKey
	 *          right sibling key
	 * @param hash
	 *          hash value associated with the node
	 * @param name
	 *          {@link QNm} of the node
	 * @param pPCR
	 *          path class record of node
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	ElementNode createElementNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final long hash, final QNm name, final @Nonnegative long pathNodeKey,
			final Optional<SirixDeweyID> id) throws SirixIOException;

	/**
	 * Create a {@link TextNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param leftSibKey
	 *          left sibling key
	 * @param rightSibKey
	 *          right sibling key
	 * @param value
	 *          value of the node
	 * @param isCompressed
	 *          determines if the value should be compressed or not
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	TextNode createTextNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final byte[] value, final boolean isCompressed,
			final Optional<SirixDeweyID> id) throws SirixIOException;

	/**
	 * Create an {@link AttributeNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param name
	 *          the {@link QNm} of the attribute
	 * @param pathNodeKey
	 *          the path class record
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	AttributeNode createAttributeNode(final @Nonnegative long parentKey,
			final QNm name, final byte[] value, final @Nonnegative long pathNodeKey,
			final Optional<SirixDeweyID> id) throws SirixIOException;

	/**
	 * Create a {@link NamespaceNode}.
	 * 
	 * @param parentKey
	 *          parent node key
	 * @param name
	 *          the {@link QNm} of the namespace
	 * @param pathNodeKey
	 *          the path class record
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NamespaceNode createNamespaceNode(final @Nonnegative long parentKey,
			final QNm name, final @Nonnegative long pathNodeKey,
			final Optional<SirixDeweyID> id) throws SirixIOException;
}
