package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.exception.SirixIOException;
import org.sirix.index.path.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.Kind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.TextNode;

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
	 * @param pParentKey
	 *          parent node key
	 * @param pLeftSibKey
	 *          left sibling key
	 * @param pRightSibKey
	 *          right sibling key
	 * @param pHash
	 *          hash value associated with the node
	 * @param pName
	 *          {@link QName} of the node
	 * @param pPCR
	 *          path class record of node
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	PathNode createPathNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey, final long pRightSibKey,
			final long pHash, final @Nonnull QName pName, final @Nonnull Kind pKind,
			final @Nonnegative int pLevel)
			throws SirixIOException;

	/**
	 * Create a {@link PINode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pLeftSibKey
	 *          left sibling key
	 * @param pRightSibKey
	 *          right sibling key
	 * @param pTarget
	 *          target of the processing instruction
	 * @param pContent
	 *          content of the processing instruction
	 * @param pIsCompressed
	 *          determines if the value is compressed or not
	 * @param pPCR
	 *          path class record of node
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	PINode createPINode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final @Nonnull QName pTarget,
			final @Nonnull byte[] pContent, final boolean pIsCompressed,
			final @Nonnegative long pPathNodeKey)
			throws SirixIOException;

	/**
	 * Create a {@link CommentNode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pLeftSibKey
	 *          left sibling key
	 * @param pRightSibKey
	 *          right sibling key
	 * @param pValue
	 *          value of the node
	 * @param pIsCompressed
	 *          determines if the value is compressed or not
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	CommentNode createCommentNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, @Nonnull byte[] pValue,
			final boolean pIsCompressed)
			throws SirixIOException;

	/**
	 * Create an {@link ElementNode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pLeftSibKey
	 *          left sibling key
	 * @param pRightSibKey
	 *          right sibling key
	 * @param pHash
	 *          hash value associated with the node
	 * @param pName
	 *          {@link QName} of the node
	 * @param pPCR
	 *          path class record of node
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	ElementNode createElementNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final long pHash,
			final @Nonnull QName pName, final @Nonnegative long pPathNodeKey) throws SirixIOException;

	/**
	 * Create a {@link TextNode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pLeftSibKey
	 *          left sibling key
	 * @param pRightSibKey
	 *          right sibling key
	 * @param pValue
	 *          value of the node
	 * @param pIsCompressed
	 *          determines if the value should be compressed or not
	 * @param pSiblingPos
	 *          sibling position
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	TextNode createTextNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final @Nonnull byte[] pValue,
			final boolean pIsCompressed)
			throws SirixIOException;

	/**
	 * Create an {@link AttributeNode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pName
	 *          the {@link QName} of the attribute
	 * @param pPCR
	 *          the path class record
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	AttributeNode createAttributeNode(final @Nonnegative long pParentKey,
			final @Nonnull QName pName, final @Nonnull byte[] pValue,
			final @Nonnegative long pPathNodeKey) throws SirixIOException;

	/**
	 * Create a {@link NamespaceNode}.
	 * 
	 * @param pParentKey
	 *          parent node key
	 * @param pUriKey
	 *          the URI key
	 * @param pPrefixKey
	 *          the prefix key
	 * @param pPCR
	 *          the path class record
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NamespaceNode createNamespaceNode(final @Nonnegative long pParentKey,
			final int pUriKey, final int pPrefixKey,
			final @Nonnegative long pPathNodeKey) throws SirixIOException;
}
