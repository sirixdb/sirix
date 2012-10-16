package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.zip.Deflater;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.NodeFactory;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.CommentNode;
import org.sirix.node.Kind;
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
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import com.google.common.collect.HashBiMap;

/**
 * Node factory to create nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class NodeFactoryImpl implements NodeFactory {

	/** {@link PageWriteTrx} implementation. */
	private final PageWriteTrx mPageWriteTrx;

	/**
	 * Constructor.
	 * 
	 * @param pPageWriteTrx
	 *          {@link PageWriteTrx} implementation
	 */
	public NodeFactoryImpl(final @Nonnull PageWriteTrx pPageWriteTrx) {
		mPageWriteTrx = checkNotNull(pPageWriteTrx);
	}

	@Override
	public PathNode createPathNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey, final long pRightSibKey,
			final long pHash, @Nonnull final QName pName, @Nonnull final Kind pKind,
			final @Nonnegative int pLevel)
			throws SirixIOException {
		final int nameKey = pKind == Kind.NAMESPACE ? NamePageHash
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
			@Nonnull final QName pName, final @Nonnegative long pPathNodeKey) throws SirixIOException {
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(pName),
				Kind.ELEMENT);
		final int uriKey = mPageWriteTrx.createNameKey(pName.getNamespaceURI(),
				Kind.NAMESPACE);

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
			final boolean pIsCompressed)
			throws SirixIOException {
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
				Kind.ATTRIBUTE);
		final int uriKey = mPageWriteTrx.createNameKey(pName.getNamespaceURI(),
				Kind.NAMESPACE);
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
	public PINode createPINode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final @Nonnull QName pTarget,
			final @Nonnull byte[] pContent, final boolean pIsCompressed,
			final @Nonnegative long pPathNodeKey)
			throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(pTarget),
				Kind.PROCESSING);
		final int uriKey = mPageWriteTrx.createNameKey(pTarget.getNamespaceURI(),
				Kind.NAMESPACE);
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
	public CommentNode createCommentNode(final @Nonnegative long pParentKey,
			final @Nonnegative long pLeftSibKey,
			final @Nonnegative long pRightSibKey, final @Nonnull byte[] pValue,
			final boolean pIsCompressed)
			throws SirixIOException {
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
