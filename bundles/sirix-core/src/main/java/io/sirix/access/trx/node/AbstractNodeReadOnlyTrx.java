package io.sirix.access.trx.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.User;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.api.*;
import io.sirix.cache.PageGuard;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.NullNode;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.service.xml.xpath.AtomicValue;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A skeletal implementation of a read-only node transaction.
 *
 * @param <T> the type of node cursor
 */
public abstract class AbstractNodeReadOnlyTrx<T extends NodeCursor & NodeReadOnlyTrx, W extends NodeTrx & NodeCursor, N extends ImmutableNode>
    implements InternalNodeReadOnlyTrx<N>, NodeCursor, NodeReadOnlyTrx {

  /**
   * ID of transaction.
   */
  protected final int id;

  /**
   * State of transaction including all cached stuff.
   */
  protected StorageEngineReader pageReadOnlyTrx;

  /**
   * The current node (used as fallback when flyweight mode is disabled).
   */
  private N currentNode;

  /**
   * Resource manager this write transaction is bound to.
   */
  protected final InternalResourceSession<T, W> resourceSession;

  /**
   * Tracks whether the transaction is closed.
   */
  private volatile boolean isClosed;

  /**
   * Read-transaction-exclusive item list.
   */
  protected final ItemList<AtomicValue> itemList;
  
  // ==================== FLYWEIGHT CURSOR STATE ====================
  // These fields enable zero-allocation navigation by reading directly from MemorySegment
  
  /**
   * The raw MemorySegment containing the current node's serialized data.
   * When in flyweight mode, getters read directly from this segment.
   */
  private MemorySegment currentSlot;
  
  /**
   * The current node's key (used for delta decoding).
   */
  private long currentNodeKey;
  
  /**
   * The current node's kind.
   */
  private NodeKind currentNodeKind;
  
  /**
   * The current node's DeweyID bytes (may be null if DeweyIDs not stored).
   */
  private byte[] currentDeweyId;
  
  /**
   * Page guard protecting the current page from eviction.
   * MUST be released when moving to a different node or closing the transaction.
   */
  private PageGuard currentPageGuard;
  
  /**
   * Whether the transaction is in flyweight mode (reading from currentSlot).
   * When false, falls back to using currentNode object.
   */
  private boolean flyweightMode = false;
  
  /**
   * Preallocated array for caching field offsets within currentSlot.
   * Indices are defined by FIELD_* constants.
   * This avoids re-parsing varints on each getter call.
   */
  protected final int[] cachedFieldOffsets = new int[16];
  
  // Field offset indices for cachedFieldOffsets array
  protected static final int FIELD_PARENT_KEY = 0;
  protected static final int FIELD_PREV_REVISION = 1;
  protected static final int FIELD_LAST_MOD_REVISION = 2;
  protected static final int FIELD_RIGHT_SIBLING_KEY = 3;
  protected static final int FIELD_LEFT_SIBLING_KEY = 4;
  protected static final int FIELD_FIRST_CHILD_KEY = 5;
  protected static final int FIELD_LAST_CHILD_KEY = 6;
  protected static final int FIELD_CHILD_COUNT = 7;
  protected static final int FIELD_DESCENDANT_COUNT = 8;
  protected static final int FIELD_HASH = 9;
  protected static final int FIELD_NAME_KEY = 10;
  protected static final int FIELD_PATH_NODE_KEY = 11;
  protected static final int FIELD_VALUE = 12;
  protected static final int FIELD_END = 13;  // End marker for offset validation
  
  /**
   * Resource configuration cached for hash type checks.
   */
  protected final ResourceConfiguration resourceConfig;

  /**
   * Constructor.
   *
   * @param trxId               the transaction ID
   * @param pageReadTransaction the underlying read-only page transaction
   * @param documentNode        the document root node
   * @param resourceSession     The resource manager for the current transaction
   * @param itemList            Read-transaction-exclusive item list.
   */
  protected AbstractNodeReadOnlyTrx(final @NonNegative int trxId, final @NonNull StorageEngineReader pageReadTransaction,
      final @NonNull N documentNode, final InternalResourceSession<T, W> resourceSession,
      final ItemList<AtomicValue> itemList) {
    this.itemList = itemList;
    this.resourceSession = requireNonNull(resourceSession);
    this.id = trxId;
    this.pageReadOnlyTrx = requireNonNull(pageReadTransaction);
    this.currentNode = requireNonNull(documentNode);
    this.isClosed = false;
    this.resourceConfig = resourceSession.getResourceConfig();
    
    // Initialize flyweight state from document node
    this.currentNodeKey = documentNode.getNodeKey();
    this.currentNodeKind = documentNode.getKind();
    this.flyweightMode = false;  // Start with object mode for document node
  }

  @Override
  public N getCurrentNode() {
    if (currentNode != null) {
      return currentNode;
    }
    
    if (flyweightMode && currentSlot != null) {
      // Create a snapshot by deserializing the node from the slot.
      // This ALLOCATES a new object (escape hatch for code that needs a stable reference).
      currentNode = deserializeToSnapshot();
    }
    
    return currentNode;
  }
  
  /**
   * Deserialize the current slot to a node object (snapshot).
   * This is the escape hatch for code that needs a stable node reference.
   * Called by getCurrentNode() when in flyweight mode.
   *
   * @return the deserialized node
   */
  @SuppressWarnings("unchecked")
  protected N deserializeToSnapshot() {
    // Use the same deserialization as normal read path
    var bytesIn = new io.sirix.node.MemorySegmentBytesIn(currentSlot);
    var record = resourceConfig.recordPersister.deserialize(bytesIn, currentNodeKey, currentDeweyId, resourceConfig);
    return (N) record;
  }

  @Override
  public void setCurrentNode(final @Nullable N currentNode) {
    assertNotClosed();
    this.currentNode = currentNode;
    
    if (currentNode != null) {
      // Disable flyweight mode - use the provided node object
      this.flyweightMode = false;
      this.currentNodeKey = currentNode.getNodeKey();
      this.currentNodeKind = currentNode.getKind();
      // Release page guard since we're not reading from slot anymore
      releaseCurrentPageGuard();
    }
  }

  @Override
  public boolean storeDeweyIDs() {
    return resourceSession.getResourceConfig().areDeweyIDsStored;
  }

  @Override
  public ResourceSession<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceSession() {
    return resourceSession;
  }

  @Override
  public Optional<User> getUser() {
    return pageReadOnlyTrx.getActualRevisionRootPage().getUser();
  }

  @Override
  public boolean moveToPrevious() {
    assertNotClosed();
    // Use flyweight getters to avoid node materialization
    if (hasLeftSibling()) {
      // Left sibling node.
      boolean leftSiblMove = moveTo(getLeftSiblingKey());
      // Now move down to rightmost descendant node if it has one.
      while (hasFirstChild()) {
        leftSiblMove = moveToLastChild();
      }
      return leftSiblMove;
    }
    // Parent node.
    return moveTo(getParentKey());
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    assertNotClosed();
    if (hasLeftSibling()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLeftSibling();
      final NodeKind leftSiblingKind = getKind();
      moveTo(savedNodeKey);
      return leftSiblingKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLeftSiblingKey() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY], currentNodeKey);
    }
    return getStructuralNode().getLeftSiblingKey();
  }

  @Override
  public boolean hasLeftSibling() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] >= 0) {
      return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasLeftSibling();
  }

  @Override
  public boolean moveToLeftSibling() {
    assertNotClosed();
    // Use flyweight getter if available to avoid node materialization
    if (!hasLeftSibling()) {
      return false;
    }
    return moveTo(getLeftSiblingKey());
  }

  @Override
  public int keyForName(final String name) {
    assertNotClosed();
    return NamePageHash.generateHashForString(name);
  }

  @Override
  public String nameForKey(final int key) {
    assertNotClosed();
    return pageReadOnlyTrx.getName(key, getKind());
  }

  @Override
  public long getPathNodeKey() {
    assertNotClosed();
    final ImmutableNode node = getCurrentNode();
    if (node instanceof NameNode) {
      return ((NameNode) node).getPathNodeKey();
    }
    if (node instanceof ObjectKeyNode objectKeyNode) {
      return objectKeyNode.getPathNodeKey();
    }
    if (node instanceof ArrayNode arrayNode) {
      return arrayNode.getPathNodeKey();
    }
    if (node.getKind() == NodeKind.XML_DOCUMENT || node.getKind() == NodeKind.JSON_DOCUMENT) {
      return 0;
    }
    return -1;
  }

  @Override
  public int getId() {
    assertNotClosed();
    return id;
  }

  @Override
  public int getRevisionNumber() {
    assertNotClosed();
    return pageReadOnlyTrx.getActualRevisionRootPage().getRevision();
  }

  @Override
  public Instant getRevisionTimestamp() {
    assertNotClosed();
    return Instant.ofEpochMilli(pageReadOnlyTrx.getActualRevisionRootPage().getRevisionTimestamp());
  }

  @Override
  public boolean moveToDocumentRoot() {
    assertNotClosed();
    return moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
  }

  @Override
  public boolean moveToParent() {
    assertNotClosed();
    return moveTo(getParentKey());
  }

  @Override
  public boolean moveToFirstChild() {
    assertNotClosed();
    // Use flyweight getter if available to avoid node materialization
    if (!hasFirstChild()) {
      return false;
    }
    return moveTo(getFirstChildKey());
  }

  @Override
  public boolean moveTo(final long nodeKey) {
    assertNotClosed();

    // Handle negative keys (item list) - fall back to object mode
    if (nodeKey < 0) {
      return moveToItemList(nodeKey);
    }
    
    // Try flyweight mode first
    if (pageReadOnlyTrx instanceof NodeStorageEngineReader reader) {
      return moveToFlyweight(nodeKey, reader);
    }
    
    // Fallback to traditional object mode
    return moveToLegacy(nodeKey);
  }
  
  /**
   * Toggle for flyweight mode. Set to true to enable zero-allocation optimization.
   */
  private static final boolean FLYWEIGHT_ENABLED = true;
  
  /**
   * Move to a node using flyweight mode (zero allocation).
   * Reads directly from MemorySegment without creating node objects.
   *
   * @param nodeKey the node key to move to
   * @param reader  the storage engine reader
   * @return true if the move was successful
   */
  private boolean moveToFlyweight(final long nodeKey, final NodeStorageEngineReader reader) {
    if (!FLYWEIGHT_ENABLED) {
      return moveToLegacy(nodeKey);
    }
    
    // Lookup slot directly without deserializing
    // NOTE: We acquire the new guard BEFORE releasing the old one to ensure
    // the transaction state remains valid if the lookup fails.
    var slotLocation = reader.lookupSlotWithGuard(nodeKey, IndexType.DOCUMENT, -1);
    if (slotLocation == null) {
      // Node not found - keep the current position unchanged
      return false;
    }
    
    // Read node kind from first byte
    MemorySegment data = slotLocation.data();
    byte kindByte = data.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0);
    NodeKind kind = NodeKind.getKind(kindByte);
    
    // Check for deleted node
    if (kind == NodeKind.DELETE) {
      // Release the newly acquired guard, keep current position unchanged
      slotLocation.guard().close();
      return false;
    }
    
    // Move succeeded - now release the previous page guard
    releaseCurrentPageGuard();
    
    // Update flyweight state (NO ALLOCATION)
    this.currentSlot = data;
    this.currentNodeKey = nodeKey;
    this.currentNodeKind = kind;
    this.currentDeweyId = slotLocation.page().getDeweyIdAsByteArray(slotLocation.offset());
    this.currentPageGuard = slotLocation.guard();
    this.flyweightMode = true;
    this.currentNode = null;  // Invalidate cached node object
    
    // Parse and cache field offsets for fast getter access
    parseFieldOffsets();
    
    return true;
  }
  
  /**
   * Move to an item in the item list (negative keys).
   * Falls back to object mode since item list uses objects.
   *
   * @param nodeKey the negative node key
   * @return true if the move was successful
   */
  private boolean moveToItemList(final long nodeKey) {
    if (itemList.size() > 0) {
      DataRecord item = itemList.getItem(nodeKey);
      if (item != null) {
        // Move succeeded - release previous page guard and switch to object mode
        releaseCurrentPageGuard();
        flyweightMode = false;
        //noinspection unchecked
        setCurrentNode((N) item);
        this.currentNodeKey = nodeKey;
        return true;
      }
    }
    // Item not found - keep the current position unchanged
    return false;
  }
  
  /**
   * Legacy object-based moveTo for when flyweight mode is not available.
   *
   * @param nodeKey the node key to move to
   * @return true if the move was successful
   */
  private boolean moveToLegacy(final long nodeKey) {
    DataRecord newNode;
    try {
      newNode = pageReadOnlyTrx.getRecord(nodeKey, IndexType.DOCUMENT, -1);
    } catch (final SirixIOException | UncheckedIOException | IllegalArgumentException e) {
      newNode = null;
    }

    if (newNode == null) {
      // Node not found - keep the current position unchanged
      return false;
    } else {
      // Move succeeded - release previous page guard and switch to object mode
      releaseCurrentPageGuard();
      flyweightMode = false;
      //noinspection unchecked
      setCurrentNode((N) newNode);
      this.currentNodeKey = nodeKey;
      return true;
    }
  }
  
  /**
   * Release the current page guard if one is held.
   * This allows the page to be evicted if needed.
   */
  protected void releaseCurrentPageGuard() {
    if (currentPageGuard != null) {
      currentPageGuard.close();
      currentPageGuard = null;
    }
  }
  
  /**
   * Parse the field offsets from the current slot for fast getter access.
   * This is called once per moveTo() and caches the byte offset of each field.
   * <p>
   * The slot format starts with:
   * - Byte 0: NodeKind byte
   * - Byte 1+: Node-specific fields (varints)
   * <p>
   * Field order varies by node kind, but common structural nodes follow:
   * - parentKey (delta varint)
   * - prevRev (signed varint)
   * - lastModRev (signed varint)
   * - [pathNodeKey for ARRAY] (delta varint)
   * - rightSiblingKey (delta varint)
   * - leftSiblingKey (delta varint)
   * - firstChildKey (delta varint) for structural nodes
   * - lastChildKey (delta varint) for structural nodes
   * - childCount (signed varint) if storeChildCount
   * - hash (8 bytes fixed) if hashType != NONE
   * - descendantCount (signed varint) if hashType != NONE
   */
  protected void parseFieldOffsets() {
    // Start after NodeKind byte
    int offset = 1;
    
    switch (currentNodeKind) {
      case OBJECT -> parseObjectNodeOffsets(offset);
      case ARRAY -> parseArrayNodeOffsets(offset);
      case OBJECT_KEY -> parseObjectKeyNodeOffsets(offset);
      // Non-object value nodes have siblings (used in arrays)
      case STRING_VALUE -> parseStringValueNodeOffsets(offset);
      case NUMBER_VALUE -> parseNumberValueNodeOffsets(offset);
      case BOOLEAN_VALUE -> parseBooleanValueNodeOffsets(offset);
      case NULL_VALUE -> parseNullValueNodeOffsets(offset);
      // Object value nodes have no siblings (single child of ObjectKeyNode)
      case OBJECT_STRING_VALUE -> parseObjectStringValueNodeOffsets(offset);
      case OBJECT_NUMBER_VALUE -> parseObjectNumberValueNodeOffsets(offset);
      case OBJECT_BOOLEAN_VALUE -> parseObjectBooleanValueNodeOffsets(offset);
      case OBJECT_NULL_VALUE -> parseObjectNullValueNodeOffsets(offset);
      case JSON_DOCUMENT -> {
        // JSON_DOCUMENT has special serialization - fall back to object mode
        // for simplicity. Document root is typically only visited once.
        java.util.Arrays.fill(cachedFieldOffsets, -1);
      }
      default -> {
        // For unsupported node kinds, set all offsets to -1 (use object fallback)
        java.util.Arrays.fill(cachedFieldOffsets, -1);
      }
    }
  }
  
  /**
   * Parse field offsets for OBJECT node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey,
   *         firstChildKey, lastChildKey, [childCount], [hash, descendantCount]
   */
  private void parseObjectNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    // Optional childCount
    if (resourceConfig.storeChildCount()) {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    }
    
    // Optional hash and descendant count
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;  // Fixed 8 bytes for hash
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    // OBJECT nodes don't have nameKey/pathNodeKey/value
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for ARRAY node.
   * Format: parentKey, prevRev, lastModRev, pathNodeKey, rightSiblingKey, leftSiblingKey,
   *         firstChildKey, lastChildKey, [childCount], [hash, descendantCount]
   */
  private void parseArrayNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.storeChildCount()) {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    }
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_KEY node.
   * Format: parentKey, prevRev, lastModRev, pathNodeKey, rightSiblingKey,
   *         leftSiblingKey, firstChildKey, nameKey, [hash, descendantCount]
   */
  private void parseObjectKeyNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_NAME_KEY] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
      offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
      cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    }
    
    // OBJECT_KEY has no lastChildKey, childCount
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for STRING_VALUE/OBJECT_STRING_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], valueLength, value
   */
  private void parseStringValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    // Value starts at current offset (length + bytes)
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // Leaf nodes don't have children
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;  // Value parsing done on demand
  }
  
  /**
   * Parse field offsets for NUMBER_VALUE/OBJECT_NUMBER_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], numberValue
   */
  private void parseNumberValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for BOOLEAN_VALUE/OBJECT_BOOLEAN_VALUE node.
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash], booleanValue
   */
  private void parseBooleanValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for NULL_VALUE node (used in arrays).
   * Format: parentKey, prevRev, lastModRev, rightSiblingKey, leftSiblingKey, [hash]
   */
  private void parseNullValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  // ==================== OBJECT_* VALUE NODES (no siblings) ====================
  
  /**
   * Parse field offsets for OBJECT_STRING_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], stringValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectStringValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_NUMBER_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], numberValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectNumberValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_BOOLEAN_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash], booleanValue
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectBooleanValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    cachedFieldOffsets[FIELD_VALUE] = offset;
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for OBJECT_NULL_VALUE node.
   * Format: parentKey, prevRev, lastModRev, [hash]
   * Note: No sibling keys - single child of ObjectKeyNode.
   */
  private void parseObjectNullValueNodeOffsets(int offset) {
    cachedFieldOffsets[FIELD_PARENT_KEY] = offset;
    offset += DeltaVarIntCodec.deltaLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_PREV_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    if (resourceConfig.hashType != HashType.NONE) {
      cachedFieldOffsets[FIELD_HASH] = offset;
      offset += 8;
    } else {
      cachedFieldOffsets[FIELD_HASH] = -1;
    }
    
    // No siblings for object value nodes
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }
  
  /**
   * Parse field offsets for JSON_DOCUMENT node.
   * Format: firstChildKey (varint), descendantCount (8 bytes)
   * Note: JSON_DOCUMENT has fixed parent, prevRev, lastModRev values, not serialized.
   */
  private void parseJsonDocumentNodeOffsets(int offset) {
    // JSON_DOCUMENT has fixed values for these, not serialized:
    // - parentKey = NULL_NODE_KEY
    // - prevRev = NULL_REVISION_NUMBER
    // - lastModRev = NULL_REVISION_NUMBER
    cachedFieldOffsets[FIELD_PARENT_KEY] = -1;  // Fixed value, not serialized
    cachedFieldOffsets[FIELD_PREV_REVISION] = -1;  // Fixed value
    cachedFieldOffsets[FIELD_LAST_MOD_REVISION] = -1;  // Fixed value
    
    // firstChildKey is a plain varint (not delta encoded)
    cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] = offset;
    offset += DeltaVarIntCodec.varintLength(currentSlot, offset);
    
    // In JSON_DOCUMENT, firstChildKey == lastChildKey
    cachedFieldOffsets[FIELD_LAST_CHILD_KEY] = -1;  // Same as firstChildKey
    
    // descendantCount is always stored as 8-byte long
    cachedFieldOffsets[FIELD_DESCENDANT_COUNT] = offset;
    offset += 8;
    
    // These fields don't exist for JSON_DOCUMENT
    cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_LEFT_SIBLING_KEY] = -1;
    cachedFieldOffsets[FIELD_CHILD_COUNT] = -1;
    cachedFieldOffsets[FIELD_HASH] = -1;
    cachedFieldOffsets[FIELD_NAME_KEY] = -1;
    cachedFieldOffsets[FIELD_PATH_NODE_KEY] = -1;
    cachedFieldOffsets[FIELD_VALUE] = -1;
    cachedFieldOffsets[FIELD_END] = offset;
  }

  @Override
  public boolean moveToRightSibling() {
    assertNotClosed();
    // Use flyweight getter if available to avoid node materialization
    if (!hasRightSibling()) {
      return false;
    }
    return moveTo(getRightSiblingKey());
  }

  @Override
  public long getNodeKey() {
    assertNotClosed();
    if (flyweightMode) {
      return currentNodeKey;
    }
    return getCurrentNode().getNodeKey();
  }

  @Override
  public long getHash() {
    assertNotClosed();
    if (flyweightMode) {
      if (cachedFieldOffsets[FIELD_HASH] >= 0) {
        // Read 8-byte hash directly from MemorySegment - ZERO ALLOCATION
        return DeltaVarIntCodec.readLongFromSegment(currentSlot, cachedFieldOffsets[FIELD_HASH]);
      }
      // Hash field not available in flyweight mode, deserialize node
      N node = getCurrentNode();
      return node != null ? node.getHash() : 0L;
    }
    return currentNode != null ? currentNode.getHash() : 0L;
  }

  @Override
  public NodeKind getKind() {
    assertNotClosed();
    if (flyweightMode) {
      return currentNodeKind;
    }
    return getCurrentNode().getKind();
  }

  /**
   * Make sure that the transaction is not yet closed when calling this method.
   */
  public void assertNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("Transaction is already closed.");
    }
  }

  /**
   * Get the {@link StorageEngineReader}.
   *
   * @return current {@link StorageEngineReader}
   */
  public StorageEngineReader getPageTransaction() {
    assertNotClosed();
    return pageReadOnlyTrx;
  }

  /**
   * Replace the current {@link NodeStorageEngineReader}.
   *
   * @param pageReadTransaction {@link NodeStorageEngineReader} instance
   */
  public final void setPageReadTransaction(@Nullable final StorageEngineReader pageReadTransaction) {
    assertNotClosed();
    pageReadOnlyTrx = pageReadTransaction;
  }

  @Override
  public final long getMaxNodeKey() {
    assertNotClosed();
    return pageReadOnlyTrx.getActualRevisionRootPage().getMaxNodeKeyInDocumentIndex();
  }

  /**
   * Retrieve the current node as a structural node.
   *
   * @return structural node instance of current node
   */
  public final StructNode getStructuralNode() {
    // In flyweight mode, materialize if needed
    N node = getCurrentNode();
    if (node instanceof StructNode structNode) {
      return structNode;
    }
    return new NullNode(node);
  }

  @Override
  public boolean moveToNextFollowing() {
    assertNotClosed();
    // Use flyweight getters to avoid node materialization
    while (!hasRightSibling() && hasParent()) {
      moveToParent();
    }
    return moveToRightSibling();
  }

  @Override
  public boolean hasNode(final @NonNegative long key) {
    assertNotClosed();
    // Save current position using flyweight-compatible getters
    final long savedNodeKey = getNodeKey();
    final boolean retVal = moveTo(key);
    // Restore to the saved position
    moveTo(savedNodeKey);
    return retVal;
  }

  @Override
  public boolean hasParent() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_PARENT_KEY] >= 0) {
      return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getCurrentNode().hasParent();
  }

  @Override
  public boolean hasFirstChild() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] >= 0) {
      return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public boolean hasRightSibling() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] >= 0) {
      return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return getStructuralNode().hasRightSibling();
  }

  @Override
  public long getRightSiblingKey() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_RIGHT_SIBLING_KEY], currentNodeKey);
    }
    return getStructuralNode().getRightSiblingKey();
  }

  @Override
  public long getFirstChildKey() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_FIRST_CHILD_KEY] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_FIRST_CHILD_KEY], currentNodeKey);
    }
    return getStructuralNode().getFirstChildKey();
  }

  @Override
  public long getParentKey() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_PARENT_KEY] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeDeltaFromSegment(currentSlot, cachedFieldOffsets[FIELD_PARENT_KEY], currentNodeKey);
    }
    // Fall back to getCurrentNode() which materializes if needed
    return getCurrentNode().getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    assertNotClosed();
    if (getParentKey() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return NodeKind.UNKNOWN;
    }
    // Save current position using flyweight-compatible getters
    final long savedNodeKey = getNodeKey();
    moveToParent();
    final NodeKind parentKind = getKind();
    moveTo(savedNodeKey);
    return parentKind;
  }

  @Override
  public boolean moveToNext() {
    assertNotClosed();
    // Use flyweight getter if available
    if (hasRightSibling()) {
      // Right sibling node.
      return moveTo(getRightSiblingKey());
    }
    // Next following node.
    return moveToNextFollowing();
  }

  @Override
  public boolean hasLastChild() {
    assertNotClosed();
    // Use flyweight getter - if it has a first child, it also has a last child
    return hasFirstChild();
  }

  @Override
  public NodeKind getLastChildKind() {
    assertNotClosed();
    if (hasLastChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLastChild();
      final NodeKind lastChildKind = getKind();
      moveTo(savedNodeKey);
      return lastChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getFirstChildKind() {
    assertNotClosed();
    if (hasFirstChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToFirstChild();
      final NodeKind firstChildKind = getKind();
      moveTo(savedNodeKey);
      return firstChildKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getLastChildKey() {
    assertNotClosed();
    if (hasLastChild()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToLastChild();
      final long lastChildNodeKey = getNodeKey();
      moveTo(savedNodeKey);
      return lastChildNodeKey;
    }
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_CHILD_COUNT] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_CHILD_COUNT]);
    }
    return getStructuralNode().getChildCount();
  }

  @Override
  public boolean hasChildren() {
    assertNotClosed();
    if (flyweightMode) {
      return hasFirstChild();
    }
    return getStructuralNode().hasFirstChild();
  }

  @Override
  public long getDescendantCount() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_DESCENDANT_COUNT] >= 0) {
      // Read directly from MemorySegment - ZERO ALLOCATION
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_DESCENDANT_COUNT]);
    }
    return getStructuralNode().getDescendantCount();
  }

  @Override
  public NodeKind getPathKind() {
    assertNotClosed();
    return NodeKind.UNKNOWN;
  }

  @Override
  public NodeKind getRightSiblingKind() {
    assertNotClosed();
    if (hasRightSibling()) {
      // Save current position using flyweight-compatible getters
      final long savedNodeKey = getNodeKey();
      moveToRightSibling();
      final NodeKind rightSiblingKind = getKind();
      moveTo(savedNodeKey);
      return rightSiblingKind;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  public StorageEngineReader getPageTrx() {
    assertNotClosed();
    return pageReadOnlyTrx;
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    assertNotClosed();
    return pageReadOnlyTrx.getCommitCredentials();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    assertNotClosed();
    if (flyweightMode) {
      if (currentDeweyId != null) {
        return new SirixDeweyID(currentDeweyId);
      }
      // DeweyID not stored in page, need to deserialize node
      N node = getCurrentNode();
      return node != null ? node.getDeweyID() : null;
    }
    return currentNode != null ? currentNode.getDeweyID() : null;
  }

  @Override
  public int getPreviousRevisionNumber() {
    assertNotClosed();
    if (flyweightMode && cachedFieldOffsets[FIELD_PREV_REVISION] >= 0) {
      return DeltaVarIntCodec.decodeSignedFromSegment(currentSlot, cachedFieldOffsets[FIELD_PREV_REVISION]);
    }
    return getCurrentNode().getPreviousRevisionNumber();
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public void close() {
    if (!isClosed) {
      // Release flyweight state and page guard FIRST to allow page eviction
      releaseCurrentPageGuard();
      currentSlot = null;
      flyweightMode = false;
      
      // Callback on session to make sure everything is cleaned up.
      resourceSession.closeReadTransaction(id);

      setPageReadTransaction(null);

      // Immediately release all references.
      pageReadOnlyTrx = null;
      currentNode = null;

      // Close state.
      isClosed = true;
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final AbstractNodeReadOnlyTrx<?, ?, ?> that = (AbstractNodeReadOnlyTrx<?, ?, ?>) o;
    return getNodeKey() == that.getNodeKey()
        && pageReadOnlyTrx.getRevisionNumber() == that.pageReadOnlyTrx.getRevisionNumber();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNodeKey(), pageReadOnlyTrx.getRevisionNumber());
  }
}
