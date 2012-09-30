package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.zip.Deflater;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeFactory;
import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.TextNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.page.EPage;
import org.sirix.settings.EFixed;
import org.sirix.settings.IConstants;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import com.google.common.collect.HashBiMap;

/**
 * Node factory to create nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class NodeFactory implements INodeFactory {

	/** {@link IPageWriteTrx} implementation. */
	private final IPageWriteTrx mPageWriteTrx;

	/**
	 * Constructor.
	 * 
	 * @param pPageWriteTrx
	 *          {@link IPageWriteTrx} implementation
	 */
	public NodeFactory(final @Nonnull IPageWriteTrx pPageWriteTrx) {
		mPageWriteTrx = checkNotNull(pPageWriteTrx);
	}

	@Override
	public PathNode createPathNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey, final long pRightSibKey,
			final long pHash, @Nonnull final QName pName, @Nonnull final EKind pKind,
			final @Nonnegative int pLevel) throws SirixIOException {
		final int nameKey = pKind == EKind.NAMESPACE ? NamePageHash
				.generateHashForString(pName.getPrefix()) : NamePageHash
				.generateHashForString(Utils.buildName(pName));
		final int uriKey = NamePageHash.generateHashForString(pName
				.getNamespaceURI());

		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxPathNodeKey() + 1, pParentKey, 0,
				revision);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				EFixed.NULL_NODE_KEY.getStandardProperty(), pRightSibKey, pLeftSibKey,
				0, 0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, 0);

		return (PathNode) mPageWriteTrx.createNode(new PathNode(nodeDel, structDel,
				nameDel, pKind, 1, pLevel), EPage.PATHSUMMARYPAGE);
	}

	@Override
	public ElementNode createElementNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final long pHash,
			@Nonnull final QName pName, final @Nonnegative long pPathNodeKey)
			throws SirixIOException {
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(pName),
				EKind.ELEMENT);
		final int uriKey = mPageWriteTrx.createNameKey(pName.getNamespaceURI(),
				EKind.NAMESPACE);

		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				EFixed.NULL_NODE_KEY.getStandardProperty(), pRightSibKey, pLeftSibKey,
				0, 0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pPathNodeKey);

		return (ElementNode) mPageWriteTrx.createNode(new ElementNode(structDel,
				nameDel, new ArrayList<Long>(), HashBiMap.<Integer, Long> create(),
				new ArrayList<Long>()), EPage.NODEPAGE);
	}

	@Override
	public TextNode createTextNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, @Nonnull final byte[] pValue,
			final boolean pIsCompressed) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final boolean compression = pIsCompressed && pValue.length > 10;
		final byte[] value = compression ? Compression.compress(pValue,
				Deflater.HUFFMAN_ONLY) : pValue;
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, value,
				compression);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				EFixed.NULL_NODE_KEY.getStandardProperty(), pRightSibKey, pLeftSibKey,
				0, 0);
		return (TextNode) mPageWriteTrx.createNode(new TextNode(valDel, structDel),
				EPage.NODEPAGE);
	}

	@Override
	public AttributeNode createAttributeNode(final @Nonnegative long pParentKey,
			@Nonnull final QName pName, @Nonnull final byte[] pValue,
			final @Nonnegative long pPathNodeKey) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(pName),
				EKind.ATTRIBUTE);
		final int uriKey = mPageWriteTrx.createNameKey(pName.getNamespaceURI(),
				EKind.NAMESPACE);
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pPathNodeKey);
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, pValue, false);

		return (AttributeNode) mPageWriteTrx.createNode(new AttributeNode(nodeDel,
				nameDel, valDel), EPage.NODEPAGE);
	}

	@Override
	public NamespaceNode createNamespaceNode(final @Nonnegative long pParentKey,
			final int pUriKey, final int pPrefixKey,
			final @Nonnegative long pPathNodeKey) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, pPrefixKey,
				pUriKey, pPathNodeKey);

		return (NamespaceNode) mPageWriteTrx.createNode(new NamespaceNode(nodeDel,
				nameDel), EPage.NODEPAGE);
	}

	@Override
	public ElementNode createElementNode() throws SirixIOException {
		return createElementNode(0, 0, 0, 0, new QName(""), 0);
	}

	@Override
	public TextNode createTextNode() throws SirixIOException {
		return createTextNode(0, 0, 0, "".getBytes(IConstants.DEFAULT_ENCODING),
				false);
	}

	@Override
	public AttributeNode createAttributeNode() throws SirixIOException {
		return null;
	}

	@Override
	public NamespaceNode createNamespaceNode() throws SirixIOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DocumentRootNode createDocumentNode() throws SirixIOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PINode createPINode() throws SirixIOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PINode createPINode(@Nonnegative long pParentKey,
			@Nonnegative long pLeftSibKey, @Nonnegative long pRightSibKey,
			@Nonnull QName pTarget, @Nonnull byte[] pContent, boolean pIsCompressed,
			@Nonnegative long pPathNodeKey) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(pTarget),
				EKind.PROCESSING);
		final int uriKey = mPageWriteTrx.createNameKey(pTarget.getNamespaceURI(),
				EKind.NAMESPACE);
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				EFixed.NULL_NODE_KEY.getStandardProperty(), pRightSibKey, pLeftSibKey,
				0, 0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pPathNodeKey);
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, pContent, false);

		return (PINode) mPageWriteTrx.createNode(new PINode(structDel, nameDel,
				valDel), EPage.NODEPAGE);
	}

	@Override
	public CommentNode createCommentNode() throws SirixIOException {
		return createCommentNode(0l, 0l, 0l,
				"".getBytes(IConstants.DEFAULT_ENCODING), false);
	}

	@Override
	public CommentNode createCommentNode(@Nonnegative long pParentKey,
			@Nonnegative long pLeftSibKey, @Nonnegative long pRightSibKey,
			@Nonnull byte[] pValue, boolean pIsCompressed) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final boolean compression = pIsCompressed && pValue.length > 10;
		final byte[] value = compression ? Compression.compress(pValue,
				Deflater.HUFFMAN_ONLY) : pValue;
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, value,
				compression);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				EFixed.NULL_NODE_KEY.getStandardProperty(), pRightSibKey, pLeftSibKey,
				0, 0);
		return (CommentNode) mPageWriteTrx.createNode(new CommentNode(valDel,
				structDel), EPage.NODEPAGE);
	}
}
