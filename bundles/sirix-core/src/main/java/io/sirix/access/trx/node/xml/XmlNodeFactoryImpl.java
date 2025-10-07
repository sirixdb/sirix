package io.sirix.access.trx.node.xml;

import io.sirix.api.PageTrx;
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
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    // Allocate MemorySegment and write all fields matching ElementNode.CORE_LAYOUT order
    final var config = pageTrx.getResourceSession().getResourceConfig();
    final var data = io.sirix.node.Bytes.elasticHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);                      // offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);  // offset 8
    data.writeInt(revisionNumber);                  // offset 12
    
    // Write StructNode fields (32 bytes)
    data.writeLong(rightSibKey);                                         // offset 16
    data.writeLong(leftSibKey);                                          // offset 24
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty());           // offset 32 (firstChild)
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty());           // offset 40 (lastChild)
    
    // Write NameNode fields (20 bytes)
    data.writeLong(pathNodeKey);                    // offset 48
    data.writeInt(prefixKey);                       // offset 56
    data.writeInt(localNameKey);                    // offset 60
    data.writeInt(uriKey);                          // offset 64
    
    // Write optional fields
    if (config.storeChildCount()) {
      data.writeLong(0); // childCount = 0
    }
    if (config.hashType != io.sirix.access.trx.node.HashType.NONE) {
      data.writeLong(0); // hash placeholder
      data.writeLong(0); // descendantCount = 0
    }
    
    // Create ElementNode from MemorySegment
    var segment = (java.lang.foreign.MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ElementNode(segment, nodeKey, id, config, new LongArrayList(), new LongArrayList(), name);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
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
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;

    // Allocate MemorySegment and write all fields matching AttributeNode.CORE_LAYOUT order
    final var data = io.sirix.node.Bytes.elasticHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);                      // offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);  // offset 8
    data.writeInt(revisionNumber);                  // offset 12
    
    // Write NameNode fields (20 bytes)
    data.writeLong(pathNodeKey);                    // offset 16
    data.writeInt(prefixKey);                       // offset 24
    data.writeInt(localNameKey);                    // offset 28
    data.writeInt(uriKey);                          // offset 32
    
    // Create AttributeNode from MemorySegment
    var segment = (java.lang.foreign.MemorySegment) data.asBytesIn().getUnderlying();
    var node = new AttributeNode(segment, nodeKey, id, value, false, name);

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
    final var data = io.sirix.node.Bytes.elasticHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);                      // offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);  // offset 8
    data.writeInt(revisionNumber);                  // offset 12
    
    // Write NameNode fields (20 bytes)
    data.writeLong(pathNodeKey);                    // offset 16
    data.writeInt(prefixKey);                       // offset 24
    data.writeInt(-1);                              // offset 28 (localNameKey - not used for namespaces)
    data.writeInt(uriKey);                          // offset 32
    
    // Create NamespaceNode from MemorySegment
    var segment = (java.lang.foreign.MemorySegment) data.asBytesIn().getUnderlying();
    var node = new NamespaceNode(segment, nodeKey, id, hashFunction, name);

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

    return pageTrx.createRecord(new PINode(structDel, nameDel, valDel), IndexType.DOCUMENT, -1);
  }

  @Override
  public CommentNode createCommentNode(final @NonNegative long parentKey, final @NonNegative long leftSibKey,
      final @NonNegative long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Allocate MemorySegment and write all fields matching CommentNode.CORE_LAYOUT order
    final var data = io.sirix.node.Bytes.elasticHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);                      // offset 0
    data.writeInt(Constants.NULL_REVISION_NUMBER);  // offset 8
    data.writeInt(revisionNumber);                  // offset 12
    
    // Write sibling keys (16 bytes)
    data.writeLong(rightSibKey);                    // offset 16
    data.writeLong(leftSibKey);                     // offset 24
    
    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression ? Compression.compress(value, Deflater.HUFFMAN_ONLY) : value;
    
    // Create CommentNode from MemorySegment
    var segment = (java.lang.foreign.MemorySegment) data.asBytesIn().getUnderlying();
    var node = new CommentNode(segment, nodeKey, id, hashFunction, compressedValue, compression);

    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }
}
