package io.sirix.access.trx.node.json;

import io.sirix.access.ResourceConfiguration;
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
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
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
import io.sirix.settings.StringCompressionType;
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

  private long nextNodeKey() {
    return storageEngineWriter.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex() + 1;
  }

  private ObjectNode bindObjectNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final SirixDeweyID id) {
    reusableObjectNode.clearBinding();
    reusableObjectNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, id);
    return reusableObjectNode;
  }

  private ArrayNode bindArrayNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final SirixDeweyID id) {
    reusableArrayNode.clearBinding();
    reusableArrayNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Fixed.NULL_NODE_KEY.getStandardProperty(), Fixed.NULL_NODE_KEY.getStandardProperty(),
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, pathNodeKey, id);
    return reusableArrayNode;
  }

  private ObjectKeyNode bindObjectKeyNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final int nameKey, final String name, final long objectValueKey,
      final SirixDeweyID id) {
    reusableObjectKeyNode.clearBinding();
    reusableObjectKeyNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        objectValueKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0,
        pathNodeKey, nameKey, name, id);
    return reusableObjectKeyNode;
  }

  private NullNode bindNullNode(final long nodeKey, final long parentKey, final long leftSibKey, final long rightSibKey,
      final SirixDeweyID id) {
    reusableNullNode.clearBinding();
    reusableNullNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, id);
    return reusableNullNode;
  }

  private BooleanNode bindBooleanNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final boolean boolValue, final SirixDeweyID id) {
    reusableBooleanNode.clearBinding();
    reusableBooleanNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, boolValue, id);
    return reusableBooleanNode;
  }

  private NumberNode bindNumberNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final Number value, final SirixDeweyID id) {
    reusableNumberNode.clearBinding();
    reusableNumberNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, id);
    return reusableNumberNode;
  }

  private StringNode bindStringNode(final long nodeKey, final long parentKey, final long leftSibKey,
      final long rightSibKey, final byte[] value, final boolean isCompressed, final byte[] fsstSymbolTable,
      final SirixDeweyID id) {
    reusableStringNode.clearBinding();
    reusableStringNode.initForCreation(nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, isCompressed, fsstSymbolTable, id);
    return reusableStringNode;
  }

  private ObjectNullNode bindObjectNullNode(final long nodeKey, final long parentKey, final SirixDeweyID id) {
    reusableObjectNullNode.clearBinding();
    reusableObjectNullNode.initForCreation(nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, id);
    return reusableObjectNullNode;
  }

  private ObjectBooleanNode bindObjectBooleanNode(final long nodeKey, final long parentKey, final boolean boolValue,
      final SirixDeweyID id) {
    reusableObjectBooleanNode.clearBinding();
    reusableObjectBooleanNode.initForCreation(nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, boolValue, id);
    return reusableObjectBooleanNode;
  }

  private ObjectNumberNode bindObjectNumberNode(final long nodeKey, final long parentKey, final Number value,
      final SirixDeweyID id) {
    reusableObjectNumberNode.clearBinding();
    reusableObjectNumberNode.initForCreation(nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, id);
    return reusableObjectNumberNode;
  }

  private ObjectStringNode bindObjectStringNode(final long nodeKey, final long parentKey, final byte[] value,
      final boolean isCompressed, final byte[] fsstSymbolTable, final SirixDeweyID id) {
    reusableObjectStringNode.clearBinding();
    reusableObjectStringNode.initForCreation(nodeKey, parentKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, isCompressed, fsstSymbolTable, id);
    return reusableObjectStringNode;
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
    kvl.completeDirectWrite(reusableArrayNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableNullNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectKeyNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableStringNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableBooleanNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableNumberNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectNullNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectStringNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectBooleanNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    kvl.completeDirectWrite(reusableObjectNumberNode, nodeKey, slotOffset, recordBytes, deweyIdBytes);
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
    final FlyweightNode singleton = selectSingleton(nodeKindId);
    if (singleton == null) {
      return null;
    }
    // Skip isBound()/unbind() — bind() overwrites all fields unconditionally,
    // and we avoid 2 megamorphic interface calls (itable stubs) per bind.
    final int heapOffset = PageLayout.getDirHeapOffset(slottedPage, offset);
    final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);
    singleton.bind(slottedPage, recordBase, nodeKey, offset);
    singleton.setOwnerPage(null); // Clear STALE owner from previous binding

    // Propagate DeweyID — safe because ownerPage is null (no write-through).
    // MUST always set — null clears stale DeweyID from previous singleton reuse.
    if (singleton instanceof Node node) {
      final byte[] deweyIdBytes = page.getDeweyIdAsByteArray(offset);
      node.setDeweyID(deweyIdBytes != null ? new SirixDeweyID(deweyIdBytes) : null);
    }

    singleton.setOwnerPage(page); // Set CORRECT owner LAST

    // Propagate FSST symbol table for string singletons
    final byte[] fsstTable = page.getFsstSymbolTable();
    if (singleton instanceof StringNode sn) {
      sn.setFsstSymbolTable(fsstTable);
    } else if (singleton instanceof ObjectStringNode osn) {
      osn.setFsstSymbolTable(fsstTable);
    }

    return (DataRecord) singleton;
  }

  /**
   * Select the factory write singleton for a given nodeKindId.
   *
   * @param nodeKindId the node kind ID from the page directory
   * @return the singleton, or null if not a managed JSON type
   */
  private FlyweightNode selectSingleton(final int nodeKindId) {
    return switch (nodeKindId) {
      case 24 -> reusableObjectNode;           // OBJECT
      case 25 -> reusableArrayNode;            // ARRAY
      case 26 -> reusableObjectKeyNode;        // OBJECT_KEY
      case 27 -> reusableBooleanNode;          // BOOLEAN_VALUE
      case 28 -> reusableNumberNode;           // NUMBER_VALUE
      case 29 -> reusableNullNode;             // NULL_VALUE
      case 30 -> reusableStringNode;           // STRING_VALUE
      case 31 -> reusableJsonDocumentRootNode;  // JSON_DOCUMENT
      case 40 -> reusableObjectStringNode;     // OBJECT_STRING_VALUE
      case 41 -> reusableObjectBooleanNode;    // OBJECT_BOOLEAN_VALUE
      case 42 -> reusableObjectNumberNode;     // OBJECT_NUMBER_VALUE
      case 43 -> reusableObjectNullNode;       // OBJECT_NULL_VALUE
      default -> null;                         // Not a JSON type or not managed
    };
  }
}
