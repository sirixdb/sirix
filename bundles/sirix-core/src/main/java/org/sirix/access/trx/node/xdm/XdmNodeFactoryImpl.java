package org.sirix.access.trx.node.xdm;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayList;
import java.util.zip.Deflater;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.node.xdm.AttributeNode;
import org.sirix.node.xdm.CommentNode;
import org.sirix.node.xdm.ElementNode;
import org.sirix.node.xdm.NamespaceNode;
import org.sirix.node.xdm.PINode;
import org.sirix.node.xdm.TextNode;
import org.sirix.page.PageKind;
import org.sirix.page.PathSummaryPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;
import com.google.common.collect.HashBiMap;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 *
 */
final class XdmNodeFactoryImpl implements XdmNodeFactory {

  /** {@link PageTrx} implementation. */
  private final PageTrx<Long, Record, UnorderedKeyValuePage> mPageWriteTrx;

  /**
   * Constructor.
   *
   * @param pageWriteTrx {@link PageTrx} implementation
   * @throws SirixIOException if an I/O exception occured due to name key creation
   */
  XdmNodeFactoryImpl(final PageTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx) {
    mPageWriteTrx = checkNotNull(pageWriteTrx);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ATTRIBUTE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.NAMESPACE);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.ELEMENT);
    mPageWriteTrx.createNameKey("xs:untyped", Kind.PROCESSING_INSTRUCTION);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final long rightSibKey, @Nonnull final QNm name, @Nonnull final Kind kind, final @Nonnegative int level) {
    final int uriKey = NamePageHash.generateHashForString(name.getNamespaceURI());
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(((PathSummaryPage) mPageWriteTrx.getActualRevisionRootPage()
                                                                                  .getPathSummaryPageReference()
                                                                                  .getPage()).getMaxNodeKey(0)
        + 1, parentKey, 0, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return (PathNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new PathNode(nodeDel, structDel, nameDel, kind, 1, level), PageKind.PATHSUMMARYPAGE, 0);
  }

  @Override
  public ElementNode createElementNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, @Nonnull final QNm name, final @Nonnegative long pathNodeKey,
      final SirixDeweyID id) {
    final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
        ? mPageWriteTrx.createNameKey(name.getNamespaceURI(), Kind.NAMESPACE)
        : -1;
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? mPageWriteTrx.createNameKey(name.getPrefix(), Kind.ELEMENT)
        : -1;
    final int localNameKey = mPageWriteTrx.createNameKey(name.getLocalName(), Kind.ELEMENT);

    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);

    return (ElementNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new ElementNode(structDel, nameDel, new ArrayList<>(), HashBiMap.create(), new ArrayList<>(), name),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public TextNode createTextNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, @Nonnull final byte[] value, final boolean isCompressed,
      final SirixDeweyID id) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (TextNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new TextNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public AttributeNode createAttributeNode(final @Nonnegative long parentKey, @Nonnull final QNm name,
      @Nonnull final byte[] value, final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final int uriKey = mPageWriteTrx.createNameKey(name.getNamespaceURI(), Kind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? mPageWriteTrx.createNameKey(name.getPrefix(), Kind.ATTRIBUTE)
        : -1;
    final int localNameKey = mPageWriteTrx.createNameKey(name.getLocalName(), Kind.ATTRIBUTE);

    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, value, false);

    return (AttributeNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new AttributeNode(nodeDel, nameDel, valDel, name), PageKind.RECORDPAGE, -1);
  }

  @Override
  public NamespaceNode createNamespaceNode(final @Nonnegative long parentKey, final QNm name,
      final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);

    final int uriKey = mPageWriteTrx.createNameKey(name.getNamespaceURI(), Kind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? mPageWriteTrx.createNameKey(name.getPrefix(), Kind.NAMESPACE)
        : -1;

    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, -1, pathNodeKey);

    return (NamespaceNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new NamespaceNode(nodeDel, nameDel, name),
        PageKind.RECORDPAGE, -1);
  }

  @Override
  public PINode createPINode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, final QNm target, final byte[] content, final boolean isCompressed,
      final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = mPageWriteTrx.getRevisionNumber();

    final int prefixKey = target.getPrefix() != null && !target.getPrefix().isEmpty()
        ? mPageWriteTrx.createNameKey(target.getPrefix(), Kind.PROCESSING_INSTRUCTION)
        : -1;
    final int localNameKey = mPageWriteTrx.createNameKey(target.getLocalName(), Kind.PROCESSING_INSTRUCTION);
    final int uriKey = mPageWriteTrx.createNameKey(target.getNamespaceURI(), Kind.NAMESPACE);
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, content, false);

    return (PINode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(),
        new PINode(structDel, nameDel, valDel, mPageWriteTrx), PageKind.RECORDPAGE, -1);
  }

  @Override
  public CommentNode createCommentNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long revision = mPageWriteTrx.getRevisionNumber();
    final NodeDelegate nodeDel =
        new NodeDelegate(mPageWriteTrx.getActualRevisionRootPage().getMaxNodeKey() + 1, parentKey, 0, revision, id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;
    final ValNodeDelegate valDel = new ValNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return (CommentNode) mPageWriteTrx.createEntry(nodeDel.getNodeKey(), new CommentNode(valDel, structDel),
        PageKind.RECORDPAGE, -1);
  }
}
