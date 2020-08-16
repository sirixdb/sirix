package org.sirix.access.trx.node.xml;

import com.google.common.collect.HashBiMap;
import com.google.common.hash.HashFunction;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageTrx;
import org.sirix.index.path.summary.PathNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.xml.*;
import org.sirix.page.PageKind;
import org.sirix.page.PathSummaryPage;
import org.sirix.settings.Fixed;
import org.sirix.utils.Compression;
import org.sirix.utils.NamePageHash;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.zip.Deflater;

import static com.google.common.base.Preconditions.checkNotNull;

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
  private final HashFunction hashFunction;

  /**
   * Constructor.
   *
   * @param hashFunction the hash function used for hashing nodes
   * @param pageWriteTrx {@link PageTrx} implementation
   */
  XmlNodeFactoryImpl(final HashFunction hashFunction, final PageTrx pageWriteTrx) {
    this.pageTrx = checkNotNull(pageWriteTrx);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ATTRIBUTE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.NAMESPACE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ELEMENT);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.PROCESSING_INSTRUCTION);
    this.hashFunction = checkNotNull(hashFunction);
  }

  @Override
  public PathNode createPathNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final long rightSibKey, @Nonnull final QNm name, @Nonnull final NodeKind kind, final @Nonnegative int level) {
    final int uriKey = NamePageHash.generateHashForString(name.getNamespaceURI());
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(
        ((PathSummaryPage) pageTrx.getActualRevisionRootPage().getPathSummaryPageReference().getPage()).getMaxNodeKey(0)
            + 1, parentKey, hashFunction, null, revision, null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level),
                                PageKind.PATHSUMMARYPAGE,
                                0);
  }

  @Override
  public ElementNode createElementNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, @Nonnull final QNm name, final @Nonnegative long pathNodeKey,
      final SirixDeweyID id) {
    final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
        ? pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
        : -1;
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.ELEMENT) : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ELEMENT);

    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new ElementNode(structDel,
                                               nameDel,
                                               new ArrayList<>(),
                                               HashBiMap.create(),
                                               new ArrayList<>(),
                                               name),
                                PageKind.RECORDPAGE,
                                -1);
  }

  @Override
  public TextNode createTextNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, @Nonnull final byte[] value, final boolean isCompressed,
      final SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new TextNode(valDel, structDel), PageKind.RECORDPAGE, -1);
  }

  @Override
  public AttributeNode createAttributeNode(final @Nonnegative long parentKey, @Nonnull final QNm name,
      @Nonnull final byte[] value, final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.ATTRIBUTE) : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ATTRIBUTE);

    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, value, false);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new AttributeNode(nodeDel, nameDel, valDel, name),
                                PageKind.RECORDPAGE,
                                -1);
  }

  @Override
  public NamespaceNode createNamespaceNode(final @Nonnegative long parentKey, final QNm name,
      final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);

    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.NAMESPACE) : -1;

    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, -1, pathNodeKey);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new NamespaceNode(nodeDel, nameDel, name),
                                PageKind.RECORDPAGE,
                                -1);
  }

  @Override
  public PINode createPINode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, final QNm target, final byte[] content, final boolean isCompressed,
      final @Nonnegative long pathNodeKey, final SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();

    final int prefixKey =
        target.getPrefix() != null && !target.getPrefix().isEmpty()
            ? pageTrx.createNameKey(target.getPrefix(),
                                    NodeKind.PROCESSING_INSTRUCTION)
            : -1;
    final int localNameKey = pageTrx.createNameKey(target.getLocalName(), NodeKind.PROCESSING_INSTRUCTION);
    final int uriKey = pageTrx.createNameKey(target.getNamespaceURI(), NodeKind.NAMESPACE);
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localNameKey, pathNodeKey);
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, content, false);

    return pageTrx.createRecord(nodeDel.getNodeKey(),
                                new PINode(structDel, nameDel, valDel, pageTrx),
                                PageKind.RECORDPAGE,
                                -1);
  }

  @Override
  public CommentNode createCommentNode(final @Nonnegative long parentKey, final @Nonnegative long leftSibKey,
      final @Nonnegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long revision = pageTrx.getRevisionNumber();
    final NodeDelegate nodeDel = new NodeDelegate(pageTrx.getActualRevisionRootPage().getMaxNodeKey() + 1,
                                                  parentKey,
                                                  hashFunction,
                                                  null,
                                                  revision,
                                                  id);
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    final ValueNodeDelegate valDel = new ValueNodeDelegate(nodeDel, compressedValue, compression);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    return pageTrx.createRecord(nodeDel.getNodeKey(), new CommentNode(valDel, structDel), PageKind.RECORDPAGE, -1);
  }
}
