package io.sirix.access.trx.node.xml;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;

import java.lang.foreign.MemorySegment;
import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class XmlNodeFactoryImpl implements XmlNodeFactory {

  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  /**
   * {@link StorageEngineWriter} implementation.
   */
  private final StorageEngineWriter storageEngineWriter;

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
  private final XmlDocumentRootNode reusableXmlDocumentRootNode;

  /**
   * Constructor.
   *
   * @param hashFunction the hash function used for hashing nodes
   * @param storageEngineWriter {@link StorageEngineWriter} implementation
   */
  XmlNodeFactoryImpl(final LongHashFunction hashFunction, final StorageEngineWriter storageEngineWriter) {
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    this.storageEngineWriter.createNameKey("xs:untyped", NodeKind.ATTRIBUTE);
    this.storageEngineWriter.createNameKey("xs:untyped", NodeKind.NAMESPACE);
    this.storageEngineWriter.createNameKey("xs:untyped", NodeKind.ELEMENT);
    this.storageEngineWriter.createNameKey("xs:untyped", NodeKind.PROCESSING_INSTRUCTION);
    this.hashFunction = requireNonNull(hashFunction);
    this.revisionNumber = storageEngineWriter.getRevisionNumber();
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
    this.reusableXmlDocumentRootNode = new XmlDocumentRootNode(0, hashFunction);

    // Mark all singletons as write singletons so setRecord skips records[] storage.
    reusableElementNode.setWriteSingleton(true);
    reusableAttributeNode.setWriteSingleton(true);
    reusableNamespaceNode.setWriteSingleton(true);
    reusablePINode.setWriteSingleton(true);
    reusableTextNode.setWriteSingleton(true);
    reusableCommentNode.setWriteSingleton(true);
    reusableXmlDocumentRootNode.setWriteSingleton(true);
  }

  @Override
  public PathNode createPathNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final QNm name, final NodeKind kind, final int level) {
    final int uriKey = NamePageHash.generateHashForString(name.getNamespaceURI());
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? NamePageHash.generateHashForString(name.getLocalName())
        : -1;

    // CRITICAL FIX: Use accessor method instead of direct .getPage() call
    // After TIL.put(), PageReference.getPage() returns null
    // Must use storageEngineWriter.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage = storageEngineWriter.getPathSummaryPage(storageEngineWriter.getActualRevisionRootPage());
    final NodeDelegate nodeDel = new NodeDelegate(pathSummaryPage.getMaxNodeKey(0) + 1, parentKey, hashFunction,
        Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel =
        new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return storageEngineWriter.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level), IndexType.PATH_SUMMARY,
        0);
  }

  @Override
  public ElementNode createElementNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final QNm name, final long pathNodeKey,
      final SirixDeweyID id) {
    final int uriKey = name.getNamespaceURI() != null && !name.getNamespaceURI().isEmpty()
        ? storageEngineWriter.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE)
        : -1;
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? storageEngineWriter.createNameKey(name.getPrefix(), NodeKind.ELEMENT)
        : -1;
    final int localNameKey = storageEngineWriter.createNameKey(name.getLocalName(), NodeKind.ELEMENT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableElementNode.estimateSerializedSize(), deweyIdLen);
    reusableElementAttributeKeys.clear();
    reusableElementNamespaceKeys.clear();
    final int recordBytes = ElementNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableElementNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        NULL_KEY, NULL_KEY, pathNodeKey, prefixKey, localNameKey, uriKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0);
    kvl.completeDirectWrite(NodeKind.ELEMENT.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableElementNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableElementNode.setOwnerPage(kvl);
    reusableElementNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    reusableElementNode.setName(name);
    return reusableElementNode;
  }

  @Override
  public TextNode createTextNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;

    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        55 + compressedValue.length, deweyIdLen);
    final int recordBytes = TextNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableTextNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, compressedValue, compression);
    kvl.completeDirectWrite(NodeKind.TEXT.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableTextNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableTextNode.setOwnerPage(kvl);
    reusableTextNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableTextNode;
  }

  @Override
  public AttributeNode createAttributeNode(final long parentKey, final QNm name,
      final byte[] value, final long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = storageEngineWriter.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? storageEngineWriter.createNameKey(name.getPrefix(), NodeKind.ATTRIBUTE)
        : -1;
    final int localNameKey = storageEngineWriter.createNameKey(name.getLocalName(), NodeKind.ATTRIBUTE);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        64 + value.length, deweyIdLen);
    final int recordBytes = AttributeNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableAttributeNode.getHeapOffsets(), nodeKey, parentKey, pathNodeKey,
        prefixKey, localNameKey, uriKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, value);
    kvl.completeDirectWrite(NodeKind.ATTRIBUTE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableAttributeNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableAttributeNode.setOwnerPage(kvl);
    reusableAttributeNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    reusableAttributeNode.setName(name);
    return reusableAttributeNode;
  }

  @Override
  public NamespaceNode createNamespaceNode(final long parentKey, final QNm name,
      final long pathNodeKey, final SirixDeweyID id) {
    final int uriKey = storageEngineWriter.createNameKey(name.getNamespaceURI(), NodeKind.NAMESPACE);
    final int prefixKey = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? storageEngineWriter.createNameKey(name.getPrefix(), NodeKind.NAMESPACE)
        : -1;

    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableNamespaceNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = NamespaceNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableNamespaceNode.getHeapOffsets(), nodeKey, parentKey, pathNodeKey,
        prefixKey, -1, uriKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0);
    kvl.completeDirectWrite(NodeKind.NAMESPACE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableNamespaceNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableNamespaceNode.setOwnerPage(kvl);
    reusableNamespaceNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    reusableNamespaceNode.setName(name);
    return reusableNamespaceNode;
  }

  @Override
  public PINode createPINode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final QNm target, final byte[] content, final boolean isCompressed,
      final long pathNodeKey, final SirixDeweyID id) {
    final int prefixKey = target.getPrefix() != null && !target.getPrefix().isEmpty()
        ? storageEngineWriter.createNameKey(target.getPrefix(), NodeKind.PROCESSING_INSTRUCTION)
        : -1;
    final int localNameKey = storageEngineWriter.createNameKey(target.getLocalName(), NodeKind.PROCESSING_INSTRUCTION);
    final int uriKey = storageEngineWriter.createNameKey(target.getNamespaceURI(), NodeKind.NAMESPACE);
    final boolean compression = isCompressed && content.length > 10;
    final byte[] compressedContent = compression
        ? Compression.compress(content, Deflater.HUFFMAN_ONLY)
        : content;
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        64 + compressedContent.length, deweyIdLen);
    final int recordBytes = PINode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusablePINode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        NULL_KEY, NULL_KEY, pathNodeKey, prefixKey, localNameKey, uriKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0,
        compressedContent, compression);
    kvl.completeDirectWrite(NodeKind.PROCESSING_INSTRUCTION.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusablePINode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusablePINode.setOwnerPage(kvl);
    reusablePINode.setDeweyIDAfterCreation(id, deweyIdBytes);
    reusablePINode.setName(target);
    return reusablePINode;
  }

  @Override
  public CommentNode createCommentNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final byte[] value, final boolean isCompressed, final SirixDeweyID id) {
    // Compress value if needed
    final boolean compression = isCompressed && value.length > 10;
    final byte[] compressedValue = compression
        ? Compression.compress(value, Deflater.HUFFMAN_ONLY)
        : value;

    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        55 + compressedValue.length, deweyIdLen);
    final int recordBytes = CommentNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableCommentNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, compressedValue, compression);
    kvl.completeDirectWrite(NodeKind.COMMENT.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableCommentNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableCommentNode.setOwnerPage(kvl);
    reusableCommentNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableCommentNode;
  }

  /**
   * Bind the correct write singleton to a slotted page slot for zero-allocation modification.
   * Reads the nodeKindId from the page directory, selects the matching singleton, binds to the
   * slot, and propagates DeweyID.
   *
   * @param page    the KeyValueLeafPage containing the slotted page
   * @param offset  the slot index (0-1023)
   * @param nodeKey the record key
   * @return the bound write singleton, or null if the slot is not an XML node type
   */
  DataRecord bindWriteSingleton(final KeyValueLeafPage page, final int offset, final long nodeKey) {
    final MemorySegment slottedPage = page.getSlottedPage();
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, offset)) {
      return null;
    }
    final int nodeKindId = PageLayout.getDirNodeKindId(slottedPage, offset);
    final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, offset);
    final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
    final byte[] deweyIdBytes = page.getDeweyIdAsByteArray(offset);

    // Concrete-type switch eliminates 3 itable stubs per bind (bind, setDeweyIDBytes, setOwnerPage).
    // Each case is monomorphic — JVM can inline directly.
    // setDeweyIDBytes stores raw bytes lazily (no SirixDeweyID parsing).
    // No setOwnerPage(null) needed — setDeweyIDBytes doesn't trigger resize.
    return switch (nodeKindId) {
      case 1 -> { // ELEMENT
        reusableElementNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableElementNode.setDeweyIDBytes(deweyIdBytes);
        reusableElementNode.setOwnerPage(page);
        yield reusableElementNode;
      }
      case 2 -> { // ATTRIBUTE
        reusableAttributeNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableAttributeNode.setDeweyIDBytes(deweyIdBytes);
        reusableAttributeNode.setOwnerPage(page);
        yield reusableAttributeNode;
      }
      case 3 -> { // TEXT
        reusableTextNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableTextNode.setDeweyIDBytes(deweyIdBytes);
        reusableTextNode.setOwnerPage(page);
        yield reusableTextNode;
      }
      case 7 -> { // PROCESSING_INSTRUCTION
        reusablePINode.bind(slottedPage, recordBase, nodeKey, offset);
        reusablePINode.setDeweyIDBytes(deweyIdBytes);
        reusablePINode.setOwnerPage(page);
        yield reusablePINode;
      }
      case 8 -> { // COMMENT
        reusableCommentNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableCommentNode.setDeweyIDBytes(deweyIdBytes);
        reusableCommentNode.setOwnerPage(page);
        yield reusableCommentNode;
      }
      case 9 -> { // XML_DOCUMENT
        reusableXmlDocumentRootNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableXmlDocumentRootNode.setDeweyIDBytes(deweyIdBytes);
        reusableXmlDocumentRootNode.setOwnerPage(page);
        yield reusableXmlDocumentRootNode;
      }
      case 13 -> { // NAMESPACE
        reusableNamespaceNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableNamespaceNode.setDeweyIDBytes(deweyIdBytes);
        reusableNamespaceNode.setOwnerPage(page);
        yield reusableNamespaceNode;
      }
      default -> null;
    };
  }
}
