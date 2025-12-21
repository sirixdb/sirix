package io.sirix.access.trx.node.xml;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.xml.*;
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
    final NodeDelegate nodeDel = new NodeDelegate(
        pathSummaryPage.getMaxNodeKey(0) + 1, 
        parentKey, hashFunction, Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
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
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Create ElementNode with primitive fields
    var node = new ElementNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,      // previousRevision
        revisionNumber,                       // lastModifiedRevision
        rightSibKey,
        leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(),  // firstChild
        Fixed.NULL_NODE_KEY.getStandardProperty(),  // lastChild
        0,                                    // childCount
        0,                                    // descendantCount
        0,                                    // hash
        pathNodeKey,
        prefixKey,
        localNameKey,
        uriKey,
        config.nodeHashFunction,
        id,
        new LongArrayList(),                  // attributeKeys
        new LongArrayList(),                  // namespaceKeys
        name);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public TextNode createTextNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    
    // Create TextNode with primitive fields
    var node = new TextNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,       // previousRevision
        revisionNumber,                        // lastModifiedRevision
        rightSibKey,
        leftSibKey,
        0,                                     // hash
        compressedValue,
        compression,
        hashFunction,
        id);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public AttributeNode createAttributeNode(final @NonNegative long parentKey, final @NonNull QNm name,
      final byte[] value, final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.ATTRIBUTE) : -1;
    final int localNameKey = pageTrx.createNameKey(name.getLocalName(), NodeKind.ATTRIBUTE);
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    // Create AttributeNode with primitive fields
    var node = new AttributeNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,              // previousRevision
        revisionNumber,                               // lastModifiedRevision
        pathNodeKey,
        prefixKey,
        localNameKey,
        uriKey,
        0,                                            // hash
        value,
        hashFunction,
        id,
        name);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NamespaceNode createNamespaceNode(final @NonNegative long parentKey, final QNm name,
      final @NonNegative long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = pageTrx.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey =
        name.getPrefix() != null && !name.getPrefix().isEmpty() ? pageTrx.createNameKey(name.getPrefix(),
                                                                                        NodeKind.NAMESPACE) : -1;

    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    // Allocate MemorySegment and write all fields matching NamespaceNode.CORE_LAYOUT order
    // Create NamespaceNode with primitive fields
    var node = new NamespaceNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,              // previousRevision
        revisionNumber,                               // lastModifiedRevision
        pathNodeKey,
        prefixKey,
        -1,                                           // localNameKey (not used for namespaces)
        uriKey,
        0,                                            // hash
        hashFunction,
        id,
        name);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
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
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    // Create PINode with primitive fields
    var node = new PINode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,                          // previousRevision
        revisionNumber,                                           // lastModifiedRevision
        rightSibKey,
        leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(),               // firstChild
        Fixed.NULL_NODE_KEY.getStandardProperty(),               // lastChild
        0,                                                        // childCount
        0,                                                        // descendantCount
        0,                                                        // hash
        pathNodeKey,
        prefixKey,
        localNameKey,
        uriKey,
        content,
        false,                                                    // isCompressed
        hashFunction,
        id,
        target);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public CommentNode createCommentNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    
    // Create CommentNode with primitive fields
    var node = new CommentNode(
        nodeKey,
        parentKey,
        Constants.NULL_REVISION_NUMBER,       // previousRevision
        revisionNumber,                        // lastModifiedRevision
        rightSibKey,
        leftSibKey,
        0,                                     // hash
        compressedValue,
        compression,
        hashFunction,
        id);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }
}
