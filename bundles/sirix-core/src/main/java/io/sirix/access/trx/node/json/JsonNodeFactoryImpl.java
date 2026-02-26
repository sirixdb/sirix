package io.sirix.access.trx.node.json;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;

import static java.util.Objects.requireNonNull;

/**
 * Node factory to create nodes.
 *
 * @author Johannes Lichtenberger
 */
final class JsonNodeFactoryImpl implements JsonNodeFactory {

  /** Cached null node key constant — avoids enum method call in hot path. */
  private static final long NULL_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();

  /**
   * Hash function used to hash nodes.
   */
  private final LongHashFunction hashFunction;

  /**
   * {@link StorageEngineWriter} implementation.
   */
  private final StorageEngineWriter storageEngineWriter;

  /**
   * The current revision number.
   */
  private final int revisionNumber;

  /**
   * Transaction-local reusable proxies for selected hot-path JSON node kinds.
   */
  private final ObjectNode reusableObjectNode;
  private final ArrayNode reusableArrayNode;
  private final ObjectKeyNode reusableObjectKeyNode;
  private final NullNode reusableNullNode;
  private final BooleanNode reusableBooleanNode;
  private final NumberNode reusableNumberNode;
  private final StringNode reusableStringNode;
  private final ObjectNullNode reusableObjectNullNode;
  private final ObjectBooleanNode reusableObjectBooleanNode;
  private final ObjectNumberNode reusableObjectNumberNode;
  private final ObjectStringNode reusableObjectStringNode;
  private final JsonDocumentRootNode reusableJsonDocumentRootNode;

  /**
   * Constructor.
   *
   * @param hashFunction hash function used to hash nodes
   * @param storageEngineWriter {@link StorageEngineWriter} implementation
   */
  JsonNodeFactoryImpl(final LongHashFunction hashFunction, final StorageEngineWriter storageEngineWriter) {
    this.hashFunction = requireNonNull(hashFunction);
    this.storageEngineWriter = requireNonNull(storageEngineWriter);
    this.revisionNumber = storageEngineWriter.getRevisionNumber();
    this.reusableObjectNode =
        new ObjectNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableArrayNode = new ArrayNode(0, 0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction,
        (SirixDeweyID) null);
    this.reusableObjectKeyNode = new ObjectKeyNode(0, 0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableNullNode =
        new NullNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, hashFunction, (SirixDeweyID) null);
    this.reusableBooleanNode =
        new BooleanNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, false, hashFunction, (SirixDeweyID) null);
    this.reusableNumberNode =
        new NumberNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0, hashFunction, (SirixDeweyID) null);
    this.reusableStringNode =
        new StringNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, Fixed.NULL_NODE_KEY.getStandardProperty(),
            Fixed.NULL_NODE_KEY.getStandardProperty(), 0, new byte[0], hashFunction, (SirixDeweyID) null, false, null);
    this.reusableObjectNullNode =
        new ObjectNullNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, hashFunction, (SirixDeweyID) null);
    this.reusableObjectBooleanNode = new ObjectBooleanNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0,
        false, hashFunction, (SirixDeweyID) null);
    this.reusableObjectNumberNode = new ObjectNumberNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0,
        hashFunction, (SirixDeweyID) null);
    this.reusableObjectStringNode = new ObjectStringNode(0, 0, Constants.NULL_REVISION_NUMBER, revisionNumber, 0,
        new byte[0], hashFunction, (SirixDeweyID) null, false, null);

    this.reusableJsonDocumentRootNode = new JsonDocumentRootNode(0, hashFunction);

    // Mark all singletons as write singletons so setRecord skips records[] storage.
    reusableJsonDocumentRootNode.setWriteSingleton(true);
    reusableObjectNode.setWriteSingleton(true);
    reusableArrayNode.setWriteSingleton(true);
    reusableObjectKeyNode.setWriteSingleton(true);
    reusableNullNode.setWriteSingleton(true);
    reusableBooleanNode.setWriteSingleton(true);
    reusableNumberNode.setWriteSingleton(true);
    reusableStringNode.setWriteSingleton(true);
    reusableObjectNullNode.setWriteSingleton(true);
    reusableObjectBooleanNode.setWriteSingleton(true);
    reusableObjectNumberNode.setWriteSingleton(true);
    reusableObjectStringNode.setWriteSingleton(true);
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
    // Must use storageEngineWriter.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage = storageEngineWriter.getPathSummaryPage(storageEngineWriter.getActualRevisionRootPage());
    final NodeDelegate nodeDel = new NodeDelegate(pathSummaryPage.getMaxNodeKey(0) + 1, parentKey, hashFunction,
        Constants.NULL_REVISION_NUMBER, revisionNumber, (SirixDeweyID) null);
    final StructNodeDelegate structDel = new StructNodeDelegate(nodeDel, Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(), rightSibKey, leftSibKey, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(nodeDel, uriKey, prefixKey, localName, 0);

    return storageEngineWriter.createRecord(new PathNode(name, nodeDel, structDel, nameDel, kind, 1, level), IndexType.PATH_SUMMARY,
        0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableArrayNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ArrayNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableArrayNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        NULL_KEY, NULL_KEY, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0);
    kvl.completeDirectWrite(NodeKind.ARRAY.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableArrayNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableArrayNode.setOwnerPage(kvl);
    reusableArrayNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableArrayNode;
  }

  @Override
  public ObjectNode createJsonObjectNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableObjectNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ObjectNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        NULL_KEY, NULL_KEY,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNode.setOwnerPage(kvl);
    reusableObjectNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNode;
  }

  @Override
  public NullNode createJsonNullNode(long parentKey, long leftSibKey, long rightSibKey, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableNullNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = NullNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableNullNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0);
    kvl.completeDirectWrite(NodeKind.NULL_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableNullNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableNullNode.setOwnerPage(kvl);
    reusableNullNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableNullNode;
  }

  @Override
  public ObjectKeyNode createJsonObjectKeyNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      String name, long objectValueKey, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_KEY);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableObjectKeyNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ObjectKeyNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectKeyNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        objectValueKey, localNameKey, pathNodeKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT_KEY.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectKeyNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectKeyNode.setOwnerPage(kvl);
    reusableObjectKeyNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectKeyNode;
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        64 + value.length, deweyIdLen);
    final int recordBytes = StringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableStringNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, false);
    kvl.completeDirectWrite(NodeKind.STRING_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableStringNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableStringNode.setOwnerPage(kvl);
    reusableStringNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableStringNode;
  }

  @Override
  public BooleanNode createJsonBooleanNode(long parentKey, long leftSibKey, long rightSibKey, boolean boolValue,
      SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableBooleanNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = BooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableBooleanNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, boolValue, 0);
    kvl.completeDirectWrite(NodeKind.BOOLEAN_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableBooleanNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableBooleanNode.setOwnerPage(kvl);
    reusableBooleanNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableBooleanNode;
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableNumberNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = NumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.NUMBER_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableNumberNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableNumberNode.setOwnerPage(kvl);
    reusableNumberNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableNumberNode;
  }

  @Override
  public ObjectNullNode createJsonObjectNullNode(long parentKey, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableObjectNullNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ObjectNullNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNullNode.getHeapOffsets(), nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT_NULL_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNullNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNullNode.setOwnerPage(kvl);
    reusableObjectNullNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNullNode;
  }

  @Override
  public ObjectStringNode createJsonObjectStringNode(long parentKey, byte[] value, boolean doCompress,
      SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        64 + value.length, deweyIdLen);
    final int recordBytes = ObjectStringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectStringNode.getHeapOffsets(), nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, false);
    kvl.completeDirectWrite(NodeKind.OBJECT_STRING_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectStringNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectStringNode.setOwnerPage(kvl);
    reusableObjectStringNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectStringNode;
  }

  @Override
  public ObjectBooleanNode createJsonObjectBooleanNode(long parentKey, boolean boolValue, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableObjectBooleanNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ObjectBooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectBooleanNode.getHeapOffsets(), nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, boolValue, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT_BOOLEAN_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectBooleanNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectBooleanNode.setOwnerPage(kvl);
    reusableObjectBooleanNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectBooleanNode;
  }

  @Override
  public ObjectNumberNode createJsonObjectNumberNode(long parentKey, Number value, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored()) ? id.toBytes() : null;
    final int deweyIdLen = deweyIdBytes != null ? deweyIdBytes.length : 0;
    final long absOffset = kvl.prepareHeapForDirectWrite(
        reusableObjectNumberNode.estimateSerializedSize(), deweyIdLen);
    final int recordBytes = ObjectNumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNumberNode.getHeapOffsets(), nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NUMBER_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNumberNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNumberNode.setOwnerPage(kvl);
    reusableObjectNumberNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNumberNode;
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, @NonNull SirixDeweyID id) {
    return storageEngineWriter.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }

  /**
   * Bind the correct write singleton to a slotted page slot for zero-allocation modification.
   * Reads the nodeKindId from the page directory, selects the matching singleton, unbinds if
   * currently bound elsewhere, binds to the slot, and propagates DeweyID.
   *
   * @param page    the KeyValueLeafPage containing the slotted page
   * @param offset  the slot index (0-1023)
   * @param nodeKey the record key
   * @return the bound write singleton, or null if the slot is not a JSON node type
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
      case 24 -> { // OBJECT
        reusableObjectNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNode.setOwnerPage(page);
        yield reusableObjectNode;
      }
      case 25 -> { // ARRAY
        reusableArrayNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableArrayNode.setDeweyIDBytes(deweyIdBytes);
        reusableArrayNode.setOwnerPage(page);
        yield reusableArrayNode;
      }
      case 26 -> { // OBJECT_KEY
        reusableObjectKeyNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectKeyNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectKeyNode.setOwnerPage(page);
        yield reusableObjectKeyNode;
      }
      case 27 -> { // BOOLEAN_VALUE
        reusableBooleanNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableBooleanNode.setDeweyIDBytes(deweyIdBytes);
        reusableBooleanNode.setOwnerPage(page);
        yield reusableBooleanNode;
      }
      case 28 -> { // NUMBER_VALUE
        reusableNumberNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableNumberNode.setDeweyIDBytes(deweyIdBytes);
        reusableNumberNode.setOwnerPage(page);
        yield reusableNumberNode;
      }
      case 29 -> { // NULL_VALUE
        reusableNullNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableNullNode.setDeweyIDBytes(deweyIdBytes);
        reusableNullNode.setOwnerPage(page);
        yield reusableNullNode;
      }
      case 30 -> { // STRING_VALUE
        reusableStringNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableStringNode.setDeweyIDBytes(deweyIdBytes);
        reusableStringNode.setOwnerPage(page);
        reusableStringNode.setFsstSymbolTable(page.getFsstSymbolTable());
        yield reusableStringNode;
      }
      case 31 -> { // JSON_DOCUMENT
        reusableJsonDocumentRootNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableJsonDocumentRootNode.setDeweyIDBytes(deweyIdBytes);
        reusableJsonDocumentRootNode.setOwnerPage(page);
        yield reusableJsonDocumentRootNode;
      }
      case 40 -> { // OBJECT_STRING_VALUE
        reusableObjectStringNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectStringNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectStringNode.setOwnerPage(page);
        reusableObjectStringNode.setFsstSymbolTable(page.getFsstSymbolTable());
        yield reusableObjectStringNode;
      }
      case 41 -> { // OBJECT_BOOLEAN_VALUE
        reusableObjectBooleanNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectBooleanNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectBooleanNode.setOwnerPage(page);
        yield reusableObjectBooleanNode;
      }
      case 42 -> { // OBJECT_NUMBER_VALUE
        reusableObjectNumberNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNumberNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNumberNode.setOwnerPage(page);
        yield reusableObjectNumberNode;
      }
      case 43 -> { // OBJECT_NULL_VALUE
        reusableObjectNullNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNullNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNullNode.setOwnerPage(page);
        yield reusableObjectNullNode;
      }
      default -> null;
    };
  }
}
