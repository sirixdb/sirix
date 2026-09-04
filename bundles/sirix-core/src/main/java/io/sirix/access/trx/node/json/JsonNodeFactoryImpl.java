package io.sirix.access.trx.node.json;

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathNode;
import io.sirix.node.DeweyIDNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PathSummaryPage;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import io.brackit.query.atomic.QNm;

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

  private static final byte BOXED_NUMBER = 0;
  private static final byte INT_NUMBER = 1;
  private static final byte LONG_NUMBER = 2;

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
  private final NullNode reusableNullNode;
  private final BooleanNode reusableBooleanNode;
  private final NumberNode reusableNumberNode;
  private final StringNode reusableStringNode;
  private final JsonDocumentRootNode reusableJsonDocumentRootNode;
  private final ObjectNamedBooleanNode reusableObjectNamedBooleanNode;
  private final ObjectNamedNumberNode reusableObjectNamedNumberNode;
  private final ObjectNamedStringNode reusableObjectNamedStringNode;
  private final ObjectNamedNullNode reusableObjectNamedNullNode;
  private final ObjectNamedObjectNode reusableObjectNamedObjectNode;
  private final ObjectNamedArrayNode reusableObjectNamedArrayNode;

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
    this.reusableObjectNamedBooleanNode = new ObjectNamedBooleanNode(0, hashFunction);
    this.reusableObjectNamedNumberNode = new ObjectNamedNumberNode(0, hashFunction);
    this.reusableObjectNamedStringNode = new ObjectNamedStringNode(0, hashFunction);
    this.reusableObjectNamedNullNode = new ObjectNamedNullNode(0, hashFunction);
    this.reusableObjectNamedObjectNode = new ObjectNamedObjectNode(0, hashFunction);
    this.reusableObjectNamedArrayNode = new ObjectNamedArrayNode(0, hashFunction);

    this.reusableJsonDocumentRootNode = new JsonDocumentRootNode(0, hashFunction);

    // Mark all singletons as write singletons so setRecord skips records[] storage.
    reusableJsonDocumentRootNode.setWriteSingleton(true);
    reusableObjectNode.setWriteSingleton(true);
    reusableArrayNode.setWriteSingleton(true);
    reusableNullNode.setWriteSingleton(true);
    reusableBooleanNode.setWriteSingleton(true);
    reusableNumberNode.setWriteSingleton(true);
    reusableStringNode.setWriteSingleton(true);
    reusableObjectNamedBooleanNode.setWriteSingleton(true);
    reusableObjectNamedNumberNode.setWriteSingleton(true);
    reusableObjectNamedStringNode.setWriteSingleton(true);
    reusableObjectNamedNullNode.setWriteSingleton(true);
    reusableObjectNamedObjectNode.setWriteSingleton(true);
    reusableObjectNamedArrayNode.setWriteSingleton(true);
  }

  @Override
  public PathNode createPathNode(final long parentKey, final long leftSibKey, final long rightSibKey, final QNm name,
      final NodeKind kind, final int level) {
    final int uriKey = -1;
    final int prefixKey = -1;
    // Resolve through the name dictionary, NOT NamePageHash.generateHashForString: the dictionary
    // probes past hash collisions, so "Aa" and "BB" (both hash 2112) own 2112 and 2113, while the
    // bare hash gave BOTH path nodes 2112. A path node reloaded from disk resolves its name from
    // this key, so the collision-losing path reported the OTHER name -- putting it in the wrong
    // bucket of the name mapping and making PathSummaryReader.match() miss it entirely.
    //
    // keyForName rather than createNameKey: a path node is one per distinct path, not a record, so
    // it must not be counted as an occurrence of the name.
    final int localName = name.getLocalName() != null && !name.getLocalName().isEmpty()
        ? storageEngineWriter.keyForName(name.getLocalName(), kind)
        : -1;

    // CRITICAL FIX: Use accessor method instead of direct .getPage() call
    // After TIL.put(), PageReference.getPage() returns null
    // Must use storageEngineWriter.getPathSummaryPage() which handles TIL lookups
    final PathSummaryPage pathSummaryPage =
        storageEngineWriter.getPathSummaryPage(storageEngineWriter.getActualRevisionRootPage());
    final long nodeKey = pathSummaryPage.getMaxNodeKey(0) + 1;
    final long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();

    return storageEngineWriter.createRecord(
        new PathNode(name, kind, 1, level, nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
            (SirixDeweyID) null, nullKey, nullKey, rightSibKey, leftSibKey, 0L, 0L, uriKey, prefixKey, localName, 0L),
        IndexType.PATH_SUMMARY, 0);
  }

  @Override
  public ArrayNode createJsonArrayNode(long parentKey, long leftSibKey, long rightSibKey, long pathNodeKey,
      SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableArrayNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ArrayNode node = new ArrayNode(nodeKey, parentKey, pathNodeKey, Constants.NULL_REVISION_NUMBER,
          revisionNumber, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ArrayNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableArrayNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0);
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
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableObjectNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ObjectNode node = new ObjectNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, NULL_KEY, NULL_KEY, 0, 0, 0, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY,
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
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableNullNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final NullNode node = new NullNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, 0, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = NullNode.writeNewRecord(kvl.getSlottedPage(), absOffset, reusableNullNode.getHeapOffsets(),
        nodeKey, parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber);
    kvl.completeDirectWrite(NodeKind.NULL_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableNullNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableNullNode.setOwnerPage(kvl);
    reusableNullNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableNullNode;
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value,
      boolean doCompress, SirixDeweyID id) {
    return createJsonStringNode(parentKey, leftSibKey, rightSibKey, value, 0, value.length, doCompress, id);
  }

  @Override
  public StringNode createJsonStringNode(long parentKey, long leftSibKey, long rightSibKey, byte[] value, int valueOff,
      int valueLen, boolean doCompress, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    // Insert-time FSST: this direct-to-heap path is where JSON string bytes actually land, so
    // this is where the encode must ride — the value is in hand and the record is being
    // serialized anyway. Null means "store raw" for any reason (disabled, no table yet, too
    // small, not smaller, page bound to a different table); the commit-time pass remains the
    // bootstrap and fallback.
    final byte[] encodedValue = storageEngineWriter.encodeStringValueForInsert(kvl, value, valueOff, valueLen);
    final byte[] effValue = encodedValue != null
        ? encodedValue
        : value;
    final int effOff = encodedValue != null
        ? 0
        : valueOff;
    final int effLen = encodedValue != null
        ? encodedValue.length
        : valueLen;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(55 + effLen, deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      // Large value (#1076): does not fit into the slotted page — store as a heap node so the
      // page diverts it to an OverflowPage at commit time. Stored raw: overflow payloads are
      // written verbatim and carry no page-level table claim.
      final byte[] valueCopy = new byte[valueLen];
      System.arraycopy(value, valueOff, valueCopy, 0, valueLen);
      final StringNode node = new StringNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, 0, valueCopy, hashFunction, id, false, null);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = StringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableStringNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey,
        Constants.NULL_REVISION_NUMBER, revisionNumber, effValue, effOff, effLen, encodedValue != null);
    kvl.completeDirectWrite(NodeKind.STRING_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableStringNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableStringNode.setOwnerPage(kvl);
    // The page's table (bound by the encode above when it engaged) — in-transaction reads of
    // this node decode through it; null clears any stale table from the singleton's last use.
    reusableStringNode.setFsstSymbolTable(kvl.getFsstSymbolTable());
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
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableBooleanNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final BooleanNode node = new BooleanNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, 0, boolValue, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes =
        BooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset, reusableBooleanNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, boolValue);
    kvl.completeDirectWrite(NodeKind.BOOLEAN_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableBooleanNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableBooleanNode.setOwnerPage(kvl);
    reusableBooleanNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableBooleanNode;
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number value,
      SirixDeweyID id) {
    return createJsonNumberNode(parentKey, leftSibKey, rightSibKey, requireNonNull(value), BOXED_NUMBER, 0L, id);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, int value,
      SirixDeweyID id) {
    return createJsonNumberNode(parentKey, leftSibKey, rightSibKey, null, INT_NUMBER, value, id);
  }

  @Override
  public NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, long value,
      SirixDeweyID id) {
    return createJsonNumberNode(parentKey, leftSibKey, rightSibKey, null, LONG_NUMBER, value, id);
  }

  private NumberNode createJsonNumberNode(long parentKey, long leftSibKey, long rightSibKey, Number fallbackValue,
      byte primitiveType, long primitiveValue, SirixDeweyID id) {
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final int estimatedSize = switch (primitiveType) {
      case BOXED_NUMBER -> NumberNode.estimateSerializedSize(fallbackValue);
      case INT_NUMBER -> NumberNode.estimateSerializedIntSize((int) primitiveValue);
      case LONG_NUMBER -> NumberNode.estimateSerializedLongSize(primitiveValue);
      default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
    };
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(estimatedSize, deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final Number materializedValue = switch (primitiveType) {
        case BOXED_NUMBER -> fallbackValue;
        case INT_NUMBER -> (int) primitiveValue;
        case LONG_NUMBER -> primitiveValue;
        default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
      };
      final NumberNode node = new NumberNode(nodeKey, parentKey, Constants.NULL_REVISION_NUMBER, revisionNumber,
          rightSibKey, leftSibKey, 0, materializedValue, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = switch (primitiveType) {
      case BOXED_NUMBER ->
        NumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset, reusableNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, fallbackValue);
      case INT_NUMBER ->
        NumberNode.writeNewIntRecord(kvl.getSlottedPage(), absOffset, reusableNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, (int) primitiveValue);
      case LONG_NUMBER ->
        NumberNode.writeNewLongRecord(kvl.getSlottedPage(), absOffset, reusableNumberNode.getHeapOffsets(), nodeKey,
            parentKey, rightSibKey, leftSibKey, Constants.NULL_REVISION_NUMBER, revisionNumber, primitiveValue);
      default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
    };
    kvl.completeDirectWrite(NodeKind.NUMBER_VALUE.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableNumberNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableNumberNode.setOwnerPage(kvl);
    reusableNumberNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableNumberNode;
  }

  @Override
  public ObjectNamedBooleanNode createJsonObjectNamedBooleanNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, boolean value, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableObjectNamedBooleanNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ObjectNamedBooleanNode node = new ObjectNamedBooleanNode(nodeKey, parentKey, rightSibKey, leftSibKey,
          localNameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNamedBooleanNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNamedBooleanNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, value);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_BOOLEAN.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedBooleanNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedBooleanNode.setOwnerPage(kvl);
    reusableObjectNamedBooleanNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedBooleanNode;
  }

  @Override
  public ObjectNamedNumberNode createJsonObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, Number value, SirixDeweyID id) {
    return createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, requireNonNull(value),
        BOXED_NUMBER, 0L, id);
  }

  @Override
  public ObjectNamedNumberNode createJsonObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, int value, SirixDeweyID id) {
    return createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, null, INT_NUMBER,
        value, id);
  }

  @Override
  public ObjectNamedNumberNode createJsonObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, long value, SirixDeweyID id) {
    return createJsonObjectNamedNumberNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, null, LONG_NUMBER,
        value, id);
  }

  private ObjectNamedNumberNode createJsonObjectNamedNumberNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, Number fallbackValue, byte primitiveType, long primitiveValue, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final int estimatedSize = switch (primitiveType) {
      case BOXED_NUMBER -> ObjectNamedNumberNode.estimateSerializedSize(fallbackValue);
      case INT_NUMBER -> ObjectNamedNumberNode.estimateSerializedIntSize((int) primitiveValue);
      case LONG_NUMBER -> ObjectNamedNumberNode.estimateSerializedLongSize(primitiveValue);
      default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
    };
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(estimatedSize, deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final Number materializedValue = switch (primitiveType) {
        case BOXED_NUMBER -> fallbackValue;
        case INT_NUMBER -> (int) primitiveValue;
        case LONG_NUMBER -> primitiveValue;
        default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
      };
      final ObjectNamedNumberNode node =
          new ObjectNamedNumberNode(nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey, pathNodeKey,
              Constants.NULL_REVISION_NUMBER, revisionNumber, 0, materializedValue, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = switch (primitiveType) {
      case BOXED_NUMBER -> ObjectNamedNumberNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
          reusableObjectNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, fallbackValue);
      case INT_NUMBER -> ObjectNamedNumberNode.writeNewIntRecord(kvl.getSlottedPage(), absOffset,
          reusableObjectNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, (int) primitiveValue);
      case LONG_NUMBER -> ObjectNamedNumberNode.writeNewLongRecord(kvl.getSlottedPage(), absOffset,
          reusableObjectNamedNumberNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
          pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, primitiveValue);
      default -> throw new IllegalArgumentException("Unknown primitive number type: " + primitiveType);
    };
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NUMBER.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedNumberNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedNumberNode.setOwnerPage(kvl);
    reusableObjectNamedNumberNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedNumberNode;
  }

  @Override
  public ObjectNamedStringNode createJsonObjectNamedStringNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, byte[] value, SirixDeweyID id) {
    final int valueLen = value != null
        ? value.length
        : 0;
    return createJsonObjectNamedStringNode(parentKey, leftSibKey, rightSibKey, pathNodeKey, name, value, 0, valueLen,
        id);
  }

  @Override
  public ObjectNamedStringNode createJsonObjectNamedStringNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, byte[] value, int off, int len, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final int valueLen = value != null
        ? len
        : 0;
    // Insert-time FSST, same shape as createJsonStringNode: fused object-string values carry
    // nearly all string bytes on real JSON, so THIS is the hot path the encode has to ride.
    final byte[] encodedValue = storageEngineWriter.encodeStringValueForInsert(kvl, value, off, valueLen);
    final byte[] effValue = encodedValue != null
        ? encodedValue
        : value;
    final int effOff = encodedValue != null
        ? 0
        : off;
    final int effLen = encodedValue != null
        ? encodedValue.length
        : valueLen;
    final long absOffset = kvl.prepareHeapForDirectWriteOrOverflow(64 + effLen, deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      // Large value (#1076): does not fit into the slotted page — store as a heap node so the
      // page diverts it to an OverflowPage at commit time. Stored raw: overflow payloads are
      // written verbatim and carry no page-level table claim.
      final byte[] valueCopy = new byte[valueLen];
      if (valueLen > 0) {
        System.arraycopy(value, off, valueCopy, 0, valueLen);
      }
      final ObjectNamedStringNode node =
          new ObjectNamedStringNode(nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey, pathNodeKey,
              Constants.NULL_REVISION_NUMBER, revisionNumber, 0, valueCopy, hashFunction, id, false, null);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNamedStringNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNamedStringNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, effValue, effOff, effLen, encodedValue != null);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_STRING.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedStringNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedStringNode.setOwnerPage(kvl);
    // In-transaction reads of a compressed value decode through the page's table; null clears
    // any stale table from the singleton's last use.
    reusableObjectNamedStringNode.setFsstSymbolTable(kvl.getFsstSymbolTable());
    reusableObjectNamedStringNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedStringNode;
  }

  @Override
  public ObjectNamedNullNode createJsonObjectNamedNullNode(long parentKey, long leftSibKey, long rightSibKey,
      long pathNodeKey, String name, SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableObjectNamedNullNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ObjectNamedNullNode node = new ObjectNamedNullNode(nodeKey, parentKey, rightSibKey, leftSibKey,
          localNameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, hashFunction, id);
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNamedNullNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNamedNullNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, localNameKey,
        pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_NULL.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedNullNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedNullNode.setOwnerPage(kvl);
    reusableObjectNamedNullNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedNullNode;
  }

  @Override
  public ObjectNamedObjectNode createJsonObjectNamedObjectNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final String name, final SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableObjectNamedObjectNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ObjectNamedObjectNode node =
          new ObjectNamedObjectNode(nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY, localNameKey,
              pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, hashFunction, id);
      node.setName(new QNm(name));
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNamedObjectNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNamedObjectNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY,
        localNameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0L, 0L, 0L);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_OBJECT.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedObjectNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedObjectNode.setOwnerPage(kvl);
    reusableObjectNamedObjectNode.setName(new QNm(name));
    reusableObjectNamedObjectNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedObjectNode;
  }

  @Override
  public ObjectNamedArrayNode createJsonObjectNamedArrayNode(final long parentKey, final long leftSibKey,
      final long rightSibKey, final long pathNodeKey, final String name, final SirixDeweyID id) {
    final int localNameKey = storageEngineWriter.createNameKey(name, NodeKind.OBJECT_NAMED_OBJECT);
    storageEngineWriter.allocateForDocumentCreation();
    final KeyValueLeafPage kvl = storageEngineWriter.getAllocKvl();
    final long nodeKey = storageEngineWriter.getAllocNodeKey();
    final int slotOffset = storageEngineWriter.getAllocSlotOffset();
    final byte[] deweyIdBytes = (id != null && kvl.areDeweyIDsStored())
        ? id.toBytes()
        : null;
    final int deweyIdLen = deweyIdBytes != null
        ? deweyIdBytes.length
        : 0;
    final long absOffset =
        kvl.prepareHeapForDirectWriteOrOverflow(reusableObjectNamedArrayNode.estimateSerializedSize(), deweyIdLen);
    if (absOffset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
      final ObjectNamedArrayNode node =
          new ObjectNamedArrayNode(nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY, localNameKey,
              pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0, 0, 0, hashFunction, id);
      node.setName(new QNm(name));
      kvl.setRecord(node);
      return node;
    }
    final int recordBytes = ObjectNamedArrayNode.writeNewRecord(kvl.getSlottedPage(), absOffset,
        reusableObjectNamedArrayNode.getHeapOffsets(), nodeKey, parentKey, rightSibKey, leftSibKey, NULL_KEY, NULL_KEY,
        localNameKey, pathNodeKey, Constants.NULL_REVISION_NUMBER, revisionNumber, 0L, 0L, 0L);
    kvl.completeDirectWrite(NodeKind.OBJECT_NAMED_ARRAY.getId(), nodeKey, slotOffset, recordBytes, deweyIdBytes);
    reusableObjectNamedArrayNode.bind(kvl.getSlottedPage(), absOffset, nodeKey, slotOffset);
    reusableObjectNamedArrayNode.setOwnerPage(kvl);
    reusableObjectNamedArrayNode.setName(new QNm(name));
    reusableObjectNamedArrayNode.setDeweyIDAfterCreation(id, deweyIdBytes);
    return reusableObjectNamedArrayNode;
  }

  @Override
  public DeweyIDNode createDeweyIdNode(long nodeKey, SirixDeweyID id) {
    return storageEngineWriter.createRecord(new DeweyIDNode(nodeKey, id), IndexType.DEWEYID_TO_RECORDID, 0);
  }

  /**
   * Bind the correct write singleton to a slotted page slot for zero-allocation modification. Reads
   * the nodeKindId from the page directory, selects the matching singleton, unbinds if currently
   * bound elsewhere, binds to the slot, and propagates DeweyID.
   *
   * @param page the KeyValueLeafPage containing the slotted page
   * @param offset the slot index (0-1023)
   * @param nodeKey the record key
   * @return the bound write singleton, or null if the slot is not a JSON node type
   */
  DataRecord bindWriteSingleton(final KeyValueLeafPage page, final int offset, final long nodeKey) {
    final MemorySegment slottedPage = page.getSlottedPage();
    if (slottedPage == null || !PageLayout.isSlotPopulated(slottedPage, offset)) {
      return null;
    }
    // The write singleton binds straight onto the heap and the DeweyID trailer is read out of it,
    // so the slot's records have to be in the page before either happens.
    page.ensureChunkFor(offset);
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
      case 48 -> { // OBJECT_NAMED_BOOLEAN
        reusableObjectNamedBooleanNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedBooleanNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedBooleanNode.setOwnerPage(page);
        yield reusableObjectNamedBooleanNode;
      }
      case 49 -> { // OBJECT_NAMED_NUMBER
        reusableObjectNamedNumberNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedNumberNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedNumberNode.setOwnerPage(page);
        yield reusableObjectNamedNumberNode;
      }
      case 50 -> { // OBJECT_NAMED_STRING
        reusableObjectNamedStringNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedStringNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedStringNode.setOwnerPage(page);
        // Same as the STRING_VALUE case: insert-time encoding means fused values can sit
        // compressed on the heap mid-transaction, and decoding them needs the page's table.
        reusableObjectNamedStringNode.setFsstSymbolTable(page.getFsstSymbolTable());
        yield reusableObjectNamedStringNode;
      }
      case 51 -> { // OBJECT_NAMED_NULL
        reusableObjectNamedNullNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedNullNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedNullNode.setOwnerPage(page);
        yield reusableObjectNamedNullNode;
      }
      case 52 -> { // OBJECT_NAMED_OBJECT
        reusableObjectNamedObjectNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedObjectNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedObjectNode.setOwnerPage(page);
        yield reusableObjectNamedObjectNode;
      }
      case 53 -> { // OBJECT_NAMED_ARRAY
        reusableObjectNamedArrayNode.bind(slottedPage, recordBase, nodeKey, offset);
        reusableObjectNamedArrayNode.setDeweyIDBytes(deweyIdBytes);
        reusableObjectNamedArrayNode.setOwnerPage(page);
        yield reusableObjectNamedArrayNode;
      }
      default -> null;
    };
  }
}
