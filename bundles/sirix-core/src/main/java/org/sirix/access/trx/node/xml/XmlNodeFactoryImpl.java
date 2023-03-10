package org.sirix.access.trx.node.xml;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.hashing.LongHashFunction;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageTrx;
import org.sirix.index.IndexType;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.xml.*;
import org.sirix.page.PathSummaryPage;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class XmlNodeFactoryImpl implements XmlNodeFactory {

  /**
   * {@link PageTrx} implementation.
   */
  private final PageTrx pageTrx;

  /**
   * The hash function used for hashing nodes.
   */
  private final LongHashFunction hashFunction;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param hashFunction the hash function used for hashing nodes
   * @param pageWriteTrx {@link PageTrx} implementation
   */
  XmlNodeFactoryImpl(final LongHashFunction hashFunction, final PageTrx pageWriteTrx) {
    this.pageTrx = requireNonNull(pageWriteTrx);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ATTRIBUTE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.NAMESPACE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ELEMENT);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.PROCESSING_INSTRUCTION);
    this.hashFunction = requireNonNull(hashFunction);
    this.revisionNumber = pageWriteTrx.getRevisionNumber();
  }

  @Override
  public PathNode createPathNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final long rightSibKey, @NonNull final QNm name, @NonNull final NodeKind kind, final @NonNegative int level) {
    final int uriKey = NamePageHash.generateHashForString(name.getNamespaceURI());
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) pageTrx.getActualRevisionRootPage().getPathSummaryPageReference().getPage()).getMaxNodeKey(0)
            + 1, parentKey, hashFunction, Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level),
                                IndexType.PATH_SUMMARY,
                                0);
  }

  @Override
  public ElementNode createElementNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, @NonNull final QNm name, final @NonNegative long pathNodeKey,
      final SirixDeweyID id) {
    final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
        ? pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
        : -1;
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.ELEMENT) : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ELEMENT);

    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);

    return pageTrx.createRecord(new ElementNode(structDel, nameDel, new LongArrayList(), new LongArrayList(), name),
                                IndexType.DOCUMENT,
                                -1);
  }

  @Override
  public TextNode createTextNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return pageTrx.createRecord(new TextNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public AttributeNode createAttributeNode(final @NonNegative long parentKey, final @NonNull QNm name,
      final byte[] value, final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.ATTRIBUTE) : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ATTRIBUTE);

    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, value, false);

    return pageTrx.createRecord(new AttributeNode(nodeDel, nameDel, valDel, name), IndexType.DOCUMENT, -1);
  }

  @Override
  public NamespaceNode createNamespaceNode(final @NonNegative long parentKey, final QNm name,
      final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);

    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.NAMESPACE) : -1;

    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, -1, pathNodeKey);

    return pageTrx.createRecord(new NamespaceNode(nodeDel, nameDel, name), IndexType.DOCUMENT, -1);
  }

  @Override
  public PINode createPINode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final QNm target, final byte[] content, final boolean isCompressed,
      final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int prefixKey =
        target.getPrefix() != null && !target.getPrefix().isEmpty()
            ? pageTrx.createNameKey(target.getPrefix(),
                                    NodeKind.PROCESSING_INSTRUCTION)
            : -1;
    final int localNameKey = pageTrx.createNameKey(target.getLocalName(), NodeKind.PROCESSING_INSTRUCTION);
    final int uriKey = pageTrx.createNameKey(target.getNamespaceURI(), NodeKind.NAMESPACE);
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, content, false);

    return pageTrx.createRecord(new PINode(structDel, nameDel, valDel, pageTrx), IndexType.DOCUMENT, -1);
  }

  @Override
  public CommentNode createCommentNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final NodeDelegate nodeDel =
        new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1,
                         parentKey,
                         hashFunction,
                         Constants.NULL_REVISION_NUMBER,
                         revisionNumber,
                         id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return pageTrx.createRecord(new CommentNode(valDel, structDel), IndexType.DOCUMENT, -1);
  }
}
