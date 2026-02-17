package io.sirix.access.trx.node.xml;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class XmlNodeFactoryImpl implements XmlNodeFactory {

  /**
   * {@link StorageEngineWriter} implementation.
   */
  private final StorageEngineWriter pageTrx;

  /**
   * The hash function used for hashing nodes.
   */
  private final LongHashFunction hashFunction;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Transaction-local reusable proxies for non-structural XML hot paths.
   */
  private final AttributeNode reusableAttributeNode;
  private final NamespaceNode reusableNamespaceNode;
  private final PINode reusablePINode;
  private final TextNode reusableTextNode;
  private final CommentNode reusableCommentNode;
  private final LongArrayList reusableElementAttributeKeys;
  private final LongArrayList reusableElementNamespaceKeys;
  private final ElementNode reusableElementNode;

  /**
   * Constructor.
   *
   * @param hashFunction the hash function used for hashing nodes
   * @param pageWriteTrx {@link StorageEngineWriter} implementation
   */
  XmlNodeFactoryImpl(final LongHashFunction hashFunction, final StorageEngineWriter pageWriteTrx) {
    this.pageTrx = requireNonNull(pageWriteTrx);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ATTRIBUTE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.NAMESPACE);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.ELEMENT);
    this.pageTrx.createNameKey("xs:untyped", NodeKind.PROCESSING_INSTRUCTION);
    this.hashFunction = requireNonNull(hashFunction);
    this.revisionNumber = pageWriteTrx.getRevisionNumber();
    this.reusableElementAttributeKeys = new LongArrayList();
    this.reusableElementNamespaceKeys = new LongArrayList();
    this.reusableElementNode = new ElementNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, 0, -1, -1, -1,
        hashFunction, (SirixDeweyID) null, reusableElementAttributeKeys, reusableElementNamespaceKeys, new QNm(""));
    this.reusableAttributeNode = new AttributeNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, -1, -1, -1,
        0, new byte[0], hashFunction, (SirixDeweyID) null, new QNm(""));
    this.reusableNamespaceNode = new NamespaceNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, -1, -1, -1,
        0, hashFunction, (SirixDeweyID) null, new QNm(""));
    this.reusablePINode = new PINode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, 0, -1, -1, -1,
        new byte[0], false, hashFunction, (SirixDeweyID) null, new QNm(""));
    this.reusableTextNode =
        new TextNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, new byte[0], false, hashFunction, (SirixDeweyID) null);
    this.reusableCommentNode =
        new CommentNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, new byte[0], false, hashFunction, (SirixDeweyID) null);
  }

  private long nextNodeKey() {
    return pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
  }

  private AttributeNode bindAttributeNode(final long nodeKey, final long parentKey, final QNm name, final byte[] value,
      final long pathNodeKey, final int prefixKey, final int localNameKey, final int uriKey, final SirixDeweyID id) {
    final AttributeNode node = reusableAttributeNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPathNodeKey(pathNodeKey);
    node.setPrefixKey(prefixKey);
    node.setLocalNameKey(localNameKey);
    node.setURIKey(uriKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRawValue(value);
    node.setHash(0);
    node.setName(name);
    node.setDeweyID(id);
    return node;
  }

  private NamespaceNode bindNamespaceNode(final long nodeKey, final long parentKey, final QNm name,
      final long pathNodeKey, final int prefixKey, final int uriKey, final SirixDeweyID id) {
    final NamespaceNode node = reusableNamespaceNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPathNodeKey(pathNodeKey);
    node.setPrefixKey(prefixKey);
    node.setLocalNameKey(-1);
    node.setURIKey(uriKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setName(name);
    node.setDeweyID(id);
    return node;
  }

  private PINode bindPINode(final long nodeKey, final long parentKey, final long leftSibKey, final long rightSibKey,
      final QNm target, final byte[] content, final long pathNodeKey, final int prefixKey, final int localNameKey,
      final int uriKey, final boolean isCompressed, final SirixDeweyID id) {
    final PINode node = reusablePINode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setFirstChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setLastChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setChildCount(0);
    node.setDescendantCount(0);
    node.setPathNodeKey(pathNodeKey);
    node.setPrefixKey(prefixKey);
    node.setLocalNameKey(localNameKey);
    node.setURIKey(uriKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRawValue(content);
    node.setCompressed(isCompressed);
    node.setHash(0);
    node.setName(target);
    node.setDeweyID(id);
    return node;
  }

  private ElementNode bindElementNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final QNm name, final long pathNodeKey, final int prefixKey, final int localNameKey,
      final int uriKey, final SirixDeweyID id) {
    reusableElementAttributeKeys.clear();
    reusableElementNamespaceKeys.clear();
    final ElementNode node = reusableElementNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setFirstChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setLastChildKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setChildCount(0);
    node.setDescendantCount(0);
    node.setPathNodeKey(pathNodeKey);
    node.setPrefixKey(prefixKey);
    node.setLocalNameKey(localNameKey);
    node.setURIKey(uriKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setHash(0);
    node.setName(name);
    node.setDeweyID(id);
    return node;
  }

  private TextNode bindTextNode(final long nodeKey, final long parentKey, final long leftSibKey, final long rightSibKey,
      final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final TextNode node = reusableTextNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setRawValue(value);
    node.setCompressed(isCompressed);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
  }

  private CommentNode bindCommentNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final CommentNode node = reusableCommentNode;
    node.setNodeKey(nodeKey);
    node.setParentKey(parentKey);
    node.setPreviousRevision(Constants.NULL_REVISION_NUMBER);
    node.setLastModifiedRevision(revisionNumber);
    node.setRightSiblingKey(rightSibKey);
    node.setLeftSiblingKey(leftSibKey);
    node.setRawValue(value);
    node.setCompressed(isCompressed);
    node.setHash(0);
    node.setDeweyID(id);
    return node;
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

    // CRITICAL FIX: Use accessor method instead of direct .getPage() call
    // After TIL.put(), PageReference.getPage() returns null
    // Must use pageTrx.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage = pageTrx.getPathSummaryPage(pageTrx.getActualRevisionRootPage());
    final NodeDelegate nodeDel = new NodeDelegate(pathSummaryPage.getMaxNodeKey(0) + 1, parentKey, hashFunction,
        Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level), IndexType.PATH_SUMMARY,
        0);
  }

  @Override
  public ElementNode createElementNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, @NonNull final QNm name, final @NonNegative long pathNodeKey,
      final SirixDeweyID id) {
    final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
        ? pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
        : -1;
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? pageTrx.createNameKey(name.getPrefix(), NodeKind.ELEMENT)
        : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ELEMENT);
    final long nodeKey = nextNodeKey();
    final ElementNode node = bindElementNode(nodeKey, parentKey, leftSibKey, rightSibKey, name, pathNodeKey, prefixKey,
        localNameKey, uriKey, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public TextNode createTextNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long nodeKey = nextNodeKey();

    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;

    final TextNode node = bindTextNode(nodeKey, parentKey, leftSibKey, rightSibKey, compressedValue, compression, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public AttributeNode createAttributeNode(final @NonNegative long parentKey, final @NonNull QNm name,
      final byte[] value, final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? pageTrx.createNameKey(name.getPrefix(), NodeKind.ATTRIBUTE)
        : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ATTRIBUTE);
    final long nodeKey = nextNodeKey();
    final AttributeNode node =
        bindAttributeNode(nodeKey, parentKey, name, value, pathNodeKey, prefixKey, localNameKey, uriKey, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NamespaceNode createNamespaceNode(final @NonNegative long parentKey, final QNm name,
      final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? pageTrx.createNameKey(name.getPrefix(), NodeKind.NAMESPACE)
        : -1;

    final long nodeKey = nextNodeKey();
    final NamespaceNode node = bindNamespaceNode(nodeKey, parentKey, name, pathNodeKey, prefixKey, uriKey, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public PINode createPINode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final QNm target, final byte[] content, final boolean isCompressed,
      final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int prefixKey = target.getPrefix() != null && !target.getPrefix().isEmpty()
        ? pageTrx.createNameKey(target.getPrefix(), NodeKind.PROCESSING_INSTRUCTION)
        : -1;
    final int localNameKey = pageTrx.createNameKey(target.getLocalName(), NodeKind.PROCESSING_INSTRUCTION);
    final int uriKey = pageTrx.createNameKey(target.getNamespaceURI(), NodeKind.NAMESPACE);
    final long nodeKey = nextNodeKey();
    final boolean compression = isCompressed && content.length > 10;
    final byte[] compressedContent = compression
        ? Compression.compress(content, Deflater.HUFFMAN_ONLY)
        : content;
    final PINode node = bindPINode(nodeKey, parentKey, leftSibKey, rightSibKey, target, compressedContent, pathNodeKey,
        prefixKey, localNameKey, uriKey, compression, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public CommentNode createCommentNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long nodeKey = nextNodeKey();

    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;

    final CommentNode node =
        bindCommentNode(nodeKey, parentKey, leftSibKey, rightSibKey, compressedValue, compression, id);
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }
}
