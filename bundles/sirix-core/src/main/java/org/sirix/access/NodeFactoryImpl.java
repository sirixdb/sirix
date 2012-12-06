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
import org.sirix.node.ElementNode;
import org.sirix.node.Kind;
import org.sirix.node.NamespaceNode;
import org.sirix.node.PINode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.TextNode;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import com.google.common.base.Optional;
import com.google.common.collect.HashBiMap;

/**
 * Node factory to create nodes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class NodeFactoryImpl implements NodeFactory {

	/** {@link PageWriteTrx} implementation. */
	private final PageWriteTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

	/**
	 * Constructor.
	 * 
	 * @param pageWriteTrx
	 *          {@link PageWriteTrx} implementation
	 * @throws SirixIOException
	 *           if an I/O exception occured due to name key creation
	 */
	public NodeFactoryImpl(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx)
			throws SirixIOException {
		mPageWriteTrx = checkNotNull(pageWriteTrx);
		mPageWriteTrx.createNameKey("xs:untyped", Kind.ATTRIBUTE);
		mPageWriteTrx.createNameKey("xs:untyped", Kind.NAMESPACE);
		mPageWriteTrx.createNameKey("xs:untyped", Kind.ELEMENT);
		mPageWriteTrx.createNameKey("xs:untyped", Kind.PROCESSING_INSTRUCTION);
	}

	@Override
	public PathNode createPathNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final long rightSibKey,
			final long hash, @Nonnull final QName name, @Nonnull final Kind kind,
			final @Nonnegative int level) throws SirixIOException {
		final int nameKey = kind == Kind.NAMESPACE ? NamePageHash
				.generateHashForString(name.getPrefix()) : NamePageHash
				.generateHashForString(Utils.buildName(name));
		final int uriKey = NamePageHash.generateHashForString(name
				.getNamespaceURI());

		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxPathNodeKey() + 1, parentKey, 0,
				revision, Optional.<SirixDeweyID> absent());
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0,
				0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, 0);

		return (PathNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
				new PathNode(nodeDel, structDel, nameDel, kind, 1, level),
				PageKind.PATHSUMMARYPAGE, Optional.<UnorderedKeyValuePage> absent());
	}

	@Override
	public ElementNode createElementNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final long hash, @Nonnull final QName name,
			final @Nonnegative long pathNodeKey,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(name),
				Kind.ELEMENT);
		final int uriKey = mPageWriteTrx.createNameKey(name.getNamespaceURI(),
				Kind.NAMESPACE);

		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0,
				0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pathNodeKey);

		return (ElementNode) mPageWriteTrx.createEntry(
				nodeDel.getNodeKey(),
				new ElementNode(structDel, nameDel, new ArrayList<Long>(), HashBiMap
						.<Integer, Long> create(), new ArrayList<Long>()),
				PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
	}

	@Override
	public TextNode createTextNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			@Nonnull final byte[] value, final boolean isCompressed,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final boolean compression = isCompressed && value.length > 10;
		final byte[] compressedValue = compression ? Compression.compress(value,
				Deflater.HUFFMAN_ONLY) : value;
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel,
				compressedValue, compression);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0,
				0);
		return (TextNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
				new TextNode(valDel, structDel), PageKind.NODEPAGE,
				Optional.<UnorderedKeyValuePage> absent());
	}

	@Override
	public AttributeNode createAttributeNode(final @Nonnegative long parentKey,
			@Nonnull final QName name, @Nonnull final byte[] value,
			final @Nonnegative long pathNodeKey,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(name),
				Kind.ATTRIBUTE);
		final int uriKey = mPageWriteTrx.createNameKey(name.getNamespaceURI(),
				Kind.NAMESPACE);
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pathNodeKey);
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, value, false);

		return (AttributeNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
				new AttributeNode(nodeDel, nameDel, valDel), PageKind.NODEPAGE,
				Optional.<UnorderedKeyValuePage> absent());
	}

	@Override
	public NamespaceNode createNamespaceNode(final @Nonnegative long parentKey,
			final int uriKey, final int prefixKey,
			final @Nonnegative long pathNodeKey,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, prefixKey,
				uriKey, pathNodeKey);

		return (NamespaceNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
				new NamespaceNode(nodeDel, nameDel), PageKind.NODEPAGE,
				Optional.<UnorderedKeyValuePage> absent());
	}

	@Override
	public PINode createPINode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final @Nonnull QName target, final @Nonnull byte[] content,
			final boolean isCompressed, final @Nonnegative long pathNodeKey,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final int nameKey = mPageWriteTrx.createNameKey(Utils.buildName(target),
				Kind.PROCESSING_INSTRUCTION);
		final int uriKey = mPageWriteTrx.createNameKey(target.getNamespaceURI(),
				Kind.NAMESPACE);
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0,
				0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, nameKey,
				uriKey, pathNodeKey);
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, content, false);

		return (PINode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new PINode(
				structDel, nameDel, valDel), PageKind.NODEPAGE, Optional
				.<UnorderedKeyValuePage> absent());
	}

	@Override
	public CommentNode createCommentNode(final @Nonnegative long parentKey,
			final @Nonnegative long leftSibKey, final @Nonnegative long rightSibKey,
			final @Nonnull byte[] value, final boolean isCompressed,
			final @Nonnull Optional<SirixDeweyID> id) throws SirixIOException {
		final long revision = mPageWriteTrx.getRevisionNumber();
		final NodeDelegate nodeDel = new NodeDelegate(mPageWriteTrx
				.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0,
				revision, id);
		final boolean compression = isCompressed && value.length > 10;
		final byte[] compressedValue = compression ? Compression.compress(value,
				Deflater.HUFFMAN_ONLY) : value;
		final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel,
				compressedValue, compression);
		final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0,
				0);
		return (CommentNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
				new CommentNode(valDel, structDel), PageKind.NODEPAGE,
				Optional.<UnorderedKeyValuePage> absent());
	}
}
