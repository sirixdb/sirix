package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.exception.SirixIOException;
import org.sirix.index.path.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.TextNode;

public interface INodeFactory {
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
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	PathNode createPathNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey, final long pRightSibKey,
			final long pHash, @Nonnull final QName pName, @Nonnull final EKind pKind,
			@Nonnegative final int pLevel) throws SirixIOException;

	/**
	 * Create an {@link ElementNode}.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	ElementNode createElementNode() throws SirixIOException;

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
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	ElementNode createElementNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey,
			@Nonnegative final long pRightSibKey, final long pHash,
			@Nonnull final QName pName, @Nonnegative final long pPathNodeKey)
			throws SirixIOException;

	/**
	 * Create a {@link TextNode}.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	TextNode createTextNode() throws SirixIOException;

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
	 * @return the created node
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	TextNode createTextNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey,
			@Nonnegative final long pRightSibKey, @Nonnull final byte[] pValue,
			final boolean pIsCompressed) throws SirixIOException;

	/**
	 * Create an {@link AttributeNode}.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	AttributeNode createAttributeNode() throws SirixIOException;

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
	AttributeNode createAttributeNode(@Nonnegative final long pParentKey,
			@Nonnull final QName pName, @Nonnull final byte[] pValue,
			@Nonnegative final long pPathNodeKey) throws SirixIOException;

	/**
	 * Create an {@link NamespaceNode}.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NamespaceNode createNamespaceNode() throws SirixIOException;

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
	NamespaceNode createNamespaceNode(@Nonnegative final long pParentKey,
			final int pUriKey, final int pPrefixKey,
			@Nonnegative final long pPathNodeKey) throws SirixIOException;
	
	/**
	 * Create a {@link DocumentRootNode}.
	 * 
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	DocumentRootNode createDocumentNode() throws SirixIOException;
}
