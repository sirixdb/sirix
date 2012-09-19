package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.zip.Deflater;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeFactory;
import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.TTIOException;
import org.sirix.index.path.PathNode;
import org.sirix.node.AttributeNode;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.NamespaceNode;
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
	public PathNode createPathNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey, final long pRightSibKey,
			final long pHash, @Nonnull final QName pName, @Nonnull final EKind pKind,
			@Nonnegative final int pLevel) throws TTIOException {
		final int nameKey = pKind == EKind.NAMESPACE ? NamePageHash
				.generateHashForString(pName.getPrefix()) : NamePageHash
				.generateHashForString(PageWriteTrx.buildName(pName));
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
	public ElementNode createElementNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey,
			@Nonnegative final long pRightSibKey, final long pHash,
			@Nonnull final QName pName, @Nonnegative final long pPathNodeKey)
			throws TTIOException {
		final int nameKey = mPageWriteTrx.createNameKey(
				PageWriteTrx.buildName(pName), EKind.ELEMENT);
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

		return (ElementNode) mPageWriteTrx.createNode(
				new ElementNode(nodeDel, structDel, nameDel, new ArrayList<Long>(),
						HashBiMap.<Integer, Long> create(), new ArrayList<Long>()),
				EPage.NODEPAGE);
	}

	@Override
	public TextNode createTextNode(@Nonnegative final long pParentKey,
			@Nonnegative final long pLeftSibKey,
			@Nonnegative final long pRightSibKey, @Nonnull final byte[] pValue,
			final boolean pIsCompressed) throws TTIOException {
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
		return (TextNode) mPageWriteTrx.createNode(new TextNode(nodeDel, valDel,
				structDel), EPage.NODEPAGE);
	}

	@Override
	public AttributeNode createAttributeNode(@Nonnegative final long pParentKey,
			@Nonnull final QName pName, @Nonnull final byte[] pValue,
			@Nonnegative final long pPathNodeKey) throws TTIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(
				PageWriteTrx.buildName(pName), EKind.ATTRIBUTE);
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
	public NamespaceNode createNamespaceNode(@Nonnegative final long pParentKey,
			final int pUriKey, final int pPrefixKey,
			@Nonnegative final long pPathNodeKey) throws TTIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, pParentKey, 0,
				revision);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, pPrefixKey,
				pUriKey, pPathNodeKey);

		return (NamespaceNode) mPageWriteTrx.createNode(new NamespaceNode(nodeDel,
				nameDel), EPage.NODEPAGE);
	}
}
