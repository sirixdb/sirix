package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.PageTrx;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.json.*;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;
import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /**
   * Hash function used to hash nodes.
   */
  private final LongHashFunction hashFunction;

  /**
   * {@link PageTrx} implementation.
   */
  private final PageTrx pageTrx;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param pageTrx      {@link PageTrx} implementation
   */
  JsonNodeFactoryImpl(final LongHashFunction hashFunction, final PageTrx pageTrx) {
    this.hashFunction = requireNonNull(hashFunction);
    this.pageTrx = requireNonNull(pageTrx);
    this.revisionNumber = pageTrx.getRevisionNumber();
  }

  @Override
  public PathNode createPathNode(final @NonNegative long parentKey, final long leftSibKey, final long rightSibKey,
      @NonNull final QNm name, @NonNull final NodeKind kind, final @NonNegative int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
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
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel,
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                Fixed.NULL_NODE_KEY.getStandardProperty(),
                                                                rightSibKey,
                                                                leftSibKey,
                                                                0,
                                                                0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return pageTrx.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level),
                                IndexType.PATH_SUMMARY,
                                0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Allocate MemorySegment and write all fields
    final var config = pageTrx.getResourceSession().getResourceConfig();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode fields matching ArrayNode.CORE_LAYOUT order:
    // pathNodeKey FIRST, then siblings, then children
    data.writeLong(pathNodeKey);      // offset 16
    data.writeLong(rightSibKey);      // offset 24
    data.writeLong(leftSibKey);       // offset 32
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // firstChild, offset 40
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // lastChild, offset 48
    
    // Write optional fields at the end
    if (config.storeChildCount()) {
      data.writeLong(0); // childCount
    }
    if (config.hashType != io.sirix.access.trx.node.HashType.NONE) {
      data.writeLong(0); // hash placeholder
      data.writeLong(0); // descendantCount
    }
    
    // Create ArrayNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ArrayNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Allocate MemorySegment and write all fields
    final var config = pageTrx.getResourceSession().getResourceConfig();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode fields
    data.writeLong(rightSibKey);
    data.writeLong(leftSibKey);
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // firstChild
    data.writeLong(Fixed.NULL_NODE_KEY.getStandardProperty()); // lastChild
    
    // Write optional fields at the end
    if (config.storeChildCount()) {
      data.writeLong(0); // childCount
    }
    if (config.hashType != io.sirix.access.trx.node.HashType.NONE) {
      data.writeLong(0); // hash placeholder
      data.writeLong(0); // descendantCount
    }
    
    // Create ObjectNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ObjectNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write all fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode sibling fields (value nodes don't have children)
    data.writeLong(rightSibKey);
    data.writeLong(leftSibKey);
    
    // Create NullNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new NullNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = pageTrx.createNameKey(name, NodeKind.OBJECT_KEY);
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    
    // Allocate MemorySegment and write all fields
    final var config = pageTrx.getResourceSession().getResourceConfig();
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write ObjectKeyNode fields matching CORE_LAYOUT order:
    // All longs first for alignment, then nameKey (int) at the end
    data.writeLong(pathNodeKey);      // offset 16
    data.writeLong(rightSibKey);      // offset 24
    data.writeLong(leftSibKey);       // offset 32
    data.writeLong(objectValueKey);   // offset 40 (firstChild)
    data.writeInt(localNameKey);      // offset 48
    
    // Write hash and descendant count at the end (if needed)
    if (config.hashType != io.sirix.access.trx.node.HashType.NONE) {
      data.writeInt(0); // 4-byte padding to align hash to 8-byte boundary
      data.writeLong(0); // hash placeholder
      data.writeLong(0); // descendantCount
    }
    
    // Create ObjectKeyNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ObjectKeyNode(segment, nodeKey, id, config);
    
    // Set name for later retrieval (cached, not in segment)
    node.setName(name);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write all fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode sibling fields (16 bytes)
    data.writeLong(rightSibKey);
    data.writeLong(leftSibKey);
    
    // Write optional fields (fixed-length) if present (skip childCount and descendantCount - value nodes are always leaf nodes with 0 descendants)
    if (config.hashType != HashType.NONE) {
      data.writeLong(0); // Hash (placeholder, computed on-demand)
    }
    
    // Write variable-length value at the end
    data.writeStopBit(value.length);
    data.write(value);
    
    // Create StringNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new StringNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write all fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode sibling fields (value nodes don't have children)
    data.writeLong(rightSibKey);
    data.writeLong(leftSibKey);
    
    // Write boolean value
    data.writeBoolean(boolValue);
    
    // Create BooleanNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new BooleanNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write all fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write StructNode sibling fields (16 bytes)
    data.writeLong(rightSibKey);
    data.writeLong(leftSibKey);
    
    // Write optional fields (fixed-length) if present (skip childCount and descendantCount - value nodes are always leaf nodes with 0 descendants)
    if (config.hashType != HashType.NONE) {
      data.writeLong(0); // Hash (placeholder, computed on-demand)
    }
    
    // Write variable-length number at the end
    NodeKind.serializeNumber(value, data);
    
    // Create NumberNode from MemorySegment
    MemorySegment segment = (MemorySegment) data.getDestination();
    var node = new NumberNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields only (16 bytes)
    // Object* value nodes are leaf nodes - no siblings, children, childCount, or hash
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Null value has no additional data beyond NodeDelegate
    
    // Create ObjectNullNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ObjectNullNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write optional fields (fixed-length) if present (skip childCount and descendantCount - value nodes are always leaf nodes with 0 descendants)
    if (config.hashType != HashType.NONE) {
      data.writeLong(0); // Hash (placeholder, computed on-demand)
    }
    
    // Write variable-length value at the end
    data.writeStopBit(value.length);
    data.write(value);
    
    // Create ObjectStringNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ObjectStringNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields only (16 bytes)
    // Object* value nodes are leaf nodes - no siblings, children, childCount, or hash
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write boolean value
    data.writeBoolean(boolValue);
    
    // Create ObjectBooleanNode from MemorySegment
    var segment = (MemorySegment) data.asBytesIn().getUnderlying();
    var node = new ObjectBooleanNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    final long nodeKey = pageTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
    final var config = pageTrx.getResourceSession().getResourceConfig();
    
    // Allocate MemorySegment and write fields
    final BytesOut<?> data = Bytes.elasticOffHeapByteBuffer();
    
    // Write NodeDelegate fields (16 bytes)
    data.writeLong(parentKey);
    data.writeInt(Constants.NULL_REVISION_NUMBER);
    data.writeInt(revisionNumber);
    
    // Write optional fields (fixed-length) if present (skip childCount and descendantCount - value nodes are always leaf nodes with 0 descendants)
    if (config.hashType != HashType.NONE) {
      data.writeLong(0); // Hash (placeholder, computed on-demand)
    }
    
    // Write variable-length number at the end
    NodeKind.serializeNumber(value, data);
    
    // Create ObjectNumberNode from MemorySegment
    MemorySegment segment = (MemorySegment) data.getDestination();
    var node = new ObjectNumberNode(segment, nodeKey, id, config);
    
    return pageTrx.createRecord(node, IndexType.DOCUMENT, -1);
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return pageTrx.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }
}
